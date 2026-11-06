#!/usr/bin/env python3
"""Opt-in dev-machine BEP 9 / BEP 52 magnet checks against libtorrent 2.1+."""
import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.parse

import libtorrent_v2 as wire
from libtorrent_discovery import Daemon, Tracker, install, keys, hellos
from transmission_smoke import control


def magnet_uri(lt, torrent, topic, tracker=None):
    hashes = lt.load_torrent_file(str(torrent)).ti.info_hashes()
    topics = []
    if topic in ("v1", "dual"):
        assert hashes.has_v1()
        topics.append("xt=urn:btih:" + hashes.v1.to_bytes().hex())
    if topic in ("v2", "dual"):
        topics.append("xt=urn:btmh:1220" + hashes.v2.to_bytes().hex())
    if tracker:
        topics.append("tr=" + urllib.parse.quote(tracker, safe=""))
    return "magnet:?" + "&".join(topics)


def layer_bytes(lt, torrent):
    return sum(len(value) for value in lt.bdecode(torrent.read_bytes())[b"piece layers"].values())


def assert_identity(lt, torrent, topic, row, daemon):
    hashes = lt.load_torrent_file(str(torrent)).ti.info_hashes()
    v1 = hashes.v1.to_bytes().hex() if hashes.has_v1() else None
    v2 = hashes.v2.to_bytes().hex()
    assert row["id"] == (v2 if topic == "v2" else v1), row
    assert row["v1"] == v1 and row["v2"] == v2 and row["metadata_ready"], row
    expected = 0 if v1 else layer_bytes(lt, torrent)
    assert row["hash_bytes"] == row["hash_total_bytes"] == expected, row
    assert not row["layers_pending"], row
    for alias in (v1, v2):
        if alias:
            assert daemon.row(alias)["id"] == row["id"]


def proof_after_restart(lt, args, torrent, daemon, source, relay):
    document = lt.bdecode(torrent.read_bytes())
    if relay.hash_requests:
        request = relay.hash_requests[0]
    else:
        piece_length = document[b"info"][b"piece length"]
        entry = next(e for e in wire.spill_files(document) if e["length"] > piece_length)
        base = (piece_length // wire.BLOCK).bit_length() - 1
        count = (entry["length"] + piece_length - 1) // piece_length
        blocks = (entry["length"] + wire.BLOCK - 1) // wire.BLOCK
        request = {"root": entry["root"].hex(), "base": base, "index": 0,
                   "length": min(512, 1 << (count - 1).bit_length()),
                   "proof": (blocks - 1).bit_length() - base - 1}
    entry = next(e for e in wire.spill_files(document) if e["root"] and e["root"].hex() == request["root"])
    geometry = tuple(request[k] for k in ("base", "index", "length", "proof"))
    key = keys(lt, torrent)[-1]
    expected, _ = wire.query_hash(source.address(args.host), key, entry, document[b"info"][b"piece length"], geometry, args.timeout)
    singletons = []
    if entry["length"] > 512 * document[b"info"][b"piece length"]:
        blocks = (entry["length"] + wire.BLOCK - 1) // wire.BLOCK
        height = (blocks - 1).bit_length()
        for index in (0, 17, (entry["length"] - 1) // document[b"info"][b"piece length"]):
            singleton = (request["base"], index, 1, height - request["base"] - 1)
            hashes, _ = wire.query_hash(source.address(args.host), key, entry, document[b"info"][b"piece length"], singleton, args.timeout)
            assert len(hashes) == (singleton[3] + 2) * 32
            singletons.append((singleton, hashes))
    source.stop()
    daemon.stop()
    daemon.start()
    torrent_id = control(daemon.socket, "list")["torrents"][0]["id"]
    row = daemon.wait(torrent_id, lambda r: r["state"] == "seeding", args.timeout)
    actual, trace = wire.query_hash(daemon.address(), key, entry, document[b"info"][b"piece length"], geometry, args.timeout)
    assert actual == expected, (actual.hex(), expected.hex())
    comparisons = []
    for singleton, oracle in singletons:
        hashes, _ = wire.query_hash(daemon.address(), key, entry, document[b"info"][b"piece length"], singleton, args.timeout)
        assert hashes == oracle, (singleton, hashes.hex(), oracle.hex())
        comparisons.append({"request": singleton, "nodes": len(hashes) // 32, "hashes": hashes.hex()})
    return {"row": row, "hashes": actual.hex(), "trace": trace, "singletons": comparisons, "source_discovery_disabled": True}


def magnet_case(lt, args, binary, root, torrent, topic, *, kill=False):
    root.mkdir()
    expected = install(lt, torrent, root / "seed")
    source = wire.Libtorrent(lt, root / "libtorrent-source", torrent, root / "seed", args.host)
    daemon = relay = tracker = reverse = leecher = None
    result = {"torrent": torrent.name, "topic": topic, "kill": kill, "status": "failed"}
    try:
        source.complete(args.timeout, seed=True)
        hybrid = lt.load_torrent_file(str(torrent)).ti.info_hashes().has_v1()
        accepted_key = keys(lt, torrent)[0] if hybrid and topic != "v2" else None
        relay = wire.Relay(args.host, source.address(args.host), root / "download-wire.jsonl", "spill",
                           allow_hash_requests=True, choke_for_hashes=not hybrid, force_v1=hybrid,
                           accepted_key=accepted_key)
        # A v1-only relay must not be advertised as a v2 peer for the same endpoint.
        tracker = Tracker(lt, "http", args.host,
                          {key: relay.address if accepted_key is None or key == accepted_key else None
                           for key in keys(lt, torrent)})
        daemon = Daemon(binary, root / "daemon", args.host, rate_limit=524288 if kill else 0)
        torrent_id = control(daemon.socket, "add", magnet=magnet_uri(lt, torrent, topic, tracker.url))["id"]
        daemon.wait(torrent_id, lambda r: r["metadata_ready"], args.timeout)
        if not hybrid:
            assert relay.hash_requests and any(r["source_choked"] for r in relay.hash_requests), relay.hash_requests
        if kill:
            partial = daemon.wait(torrent_id, lambda r: r["metadata_ready"] and r["verified_bytes"] >= 1048576, args.timeout)
            assert partial["verified_bytes"] < partial["total_bytes"], partial
            daemon.kill()
            daemon.start()
            result["killed"] = partial
        row = daemon.wait(torrent_id, lambda r: r["state"] == "seeding" and not r["layers_pending"], args.timeout)
        if not kill:
            assert_identity(lt, torrent, topic, row, daemon)
        else:
            assert row["metadata_ready"] and row["hash_total_bytes"] == 0, row
        result["download"] = row
        result["files"] = wire.verify_files(daemon.output, wire.spill_files(lt.bdecode(torrent.read_bytes())), expected)
        if hybrid:
            assert not relay.hash_requests and not relay.hash_replies
            assert not row["layers_pending"], row
        else:
            assert relay.hash_requests and relay.hash_replies
            assert all(r["length"] >= 2 for r in relay.hash_requests), relay.hash_requests
            assert any(r["source_choked"] for r in relay.hash_requests), relay.hash_requests
        result["choked_hash_requests"] = sum(r["source_choked"] for r in relay.hash_requests)
        control(daemon.socket, "set", id=torrent_id, values={"trackers": False, "download_limit": 0, "upload_limit": 0})
        daemon.wait(torrent_id, lambda r: r["state"] == "seeding", args.timeout)
        result["restart_proof"] = proof_after_restart(lt, args, torrent, daemon, source, relay)
        reverse = wire.Relay(args.host, daemon.address(), root / "upload-wire.jsonl", "libtorrent", allow_hash_requests=True)
        leecher = wire.Libtorrent(lt, root / "libtorrent-magnet", torrent, root / "fresh", args.host,
                                  magnet=magnet_uri(lt, torrent, topic))
        leecher.handle.connect_peer(reverse.address)
        result["upload"] = leecher.complete(args.timeout, seed=True)
        assert leecher.poll().has_metadata
        result["reverse_files"] = wire.verify_files(root / "fresh", wire.lt_files(lt, torrent), expected)
        result["status"] = "passed"
        return result
    finally:
        # Run every cleanup even when a relay's wire assertions fail.
        with ExitStack() as cleanup:
            cleanup.callback(lambda: (root / "case.json").write_text(json.dumps(result, indent=2)))
            if tracker:
                result["announces"] = tracker.records
                cleanup.callback(tracker.close)
            if reverse:
                result["reverse_requests"] = reverse.hash_requests
                result["reverse_handshakes"] = hellos(reverse)
                cleanup.callback(lambda: result.update(reverse_wire=reverse.stop()))
            if relay:
                result["handshakes"] = hellos(relay)
                result["requests"] = relay.hash_requests
                cleanup.callback(lambda: result.update(download_wire=relay.stop()))
            cleanup.callback(source.stop)
            if daemon: cleanup.callback(daemon.close)
            if leecher: cleanup.callback(leecher.stop)


def recovery_case(lt, args, binary, root, policy):
    """Constrain the first route, then let an independent honest peer connect."""
    root.mkdir()
    legacy = policy == "mixed"
    torrent = wire.FIXTURES / ("hybrid.torrent" if legacy else "v2.torrent")
    expected = install(lt, torrent, root / "seed")
    source = wire.Libtorrent(lt, root / "libtorrent", torrent, root / "seed", args.host)
    daemon = relay = good = tracker = honest_source = None
    result = {"type": policy, "status": "failed"}
    try:
        source.complete(args.timeout, seed=True)
        relay = wire.Relay(args.host, source.address(args.host), root / "initial-wire.jsonl", "spill",
                           allow_hash_requests=True, hash_policy=None if legacy else policy,
                           force_v1=legacy, accepted_key=keys(lt, torrent)[0] if legacy else None)
        tracker = Tracker(lt, "http", args.host, {key: relay.address for key in keys(lt, torrent)}, interval=1)
        daemon = Daemon(binary, root / "daemon", args.host)
        torrent_id = control(daemon.socket, "add", magnet=magnet_uri(lt, torrent, "v1" if legacy else "v2", tracker.url))["id"]
        if legacy:
            row = daemon.wait(torrent_id, lambda r: r["state"] == "seeding", args.timeout)
            assert not relay.hash_requests and not row["layers_pending"], row
            result["download"] = row
            result["files"] = wire.verify_files(daemon.output, wire.spill_files(lt.bdecode(torrent.read_bytes())), expected)
            result["status"] = "passed"
            return result
        wire.wait_for("initial hash request", lambda: relay.hash_requests, args.timeout)
        pending = daemon.row(torrent_id)
        assert pending["info_validated"] and not pending["metadata_ready"], pending
        assert not list(daemon.output.rglob("*.part"))
        honest_source = wire.Libtorrent(lt, root / "honest-libtorrent", torrent, root / "seed", args.host)
        honest_source.complete(args.timeout, seed=True)
        good = wire.Relay(args.host, daemon.address(), root / "honest-wire.jsonl", "libtorrent", allow_hash_requests=True)
        if policy == "stall":
            started = time.monotonic()
            wire.wait_for("stalled hash peer request timeout", lambda: "spill" in relay.closures, args.timeout)
            result["request_timeout_seconds"] = round(time.monotonic() - started, 3)
            assert result["request_timeout_seconds"] >= 25, result
        result["initial_wire"] = relay.stop()
        relay = None
        honest_source.handle.connect_peer(good.address)
        row = daemon.wait(torrent_id, lambda r: r["state"] == "seeding", args.timeout)
        result["download"] = row
        result["files"] = wire.verify_files(daemon.output, wire.spill_files(lt.bdecode(torrent.read_bytes())), expected)
        assert good.hash_replies
        result["status"] = "passed"
        return result
    finally:
        if daemon: daemon.close()
        source.stop()
        if relay:
            result["initial_handshakes"] = relay.handshakes
            result["initial_wire"] = relay.stop()
        if good: result["honest_wire"] = good.stop()
        if honest_source: honest_source.stop()
        if tracker:
            tracker.close()
            result["announces"] = tracker.records
        (root / "case.json").write_text(json.dumps(result, indent=2))


def acquisition_case(lt, args, binary, root):
    root.mkdir()
    torrent = wire.FIXTURES / "v2.torrent"
    expected = install(lt, torrent, root / "seed")
    source = wire.Libtorrent(lt, root / "libtorrent", torrent, root / "seed", args.host)
    daemon = relay = tracker = None
    result = {"type": "stop-kill-acquisition", "status": "failed"}
    try:
        source.complete(args.timeout, seed=True)
        relay = wire.Relay(args.host, source.address(args.host), root / "wire.jsonl", "spill", allow_hash_requests=True, hash_policy="stall")
        tracker = Tracker(lt, "http", args.host, {key: relay.address for key in keys(lt, torrent)}, interval=1)
        daemon = Daemon(binary, root / "daemon", args.host)
        torrent_id = control(daemon.socket, "add", magnet=magnet_uri(lt, torrent, "v2", tracker.url))["id"]
        result["pending"] = daemon.wait(torrent_id, lambda r: r["info_validated"] and not r["metadata_ready"], args.timeout)
        control(daemon.socket, "pause", id=torrent_id)
        daemon.wait(torrent_id, lambda r: r["state"] == "paused", args.timeout)
        assert not list(daemon.output.iterdir())
        control(daemon.socket, "resume", id=torrent_id)
        daemon.wait(torrent_id, lambda r: r["info_validated"] and not r["metadata_ready"], args.timeout)
        daemon.kill()
        daemon.start()
        result["after_kill"] = daemon.wait(torrent_id, lambda r: r["info_validated"] and not r["metadata_ready"], args.timeout)
        assert not list(daemon.output.iterdir())
        control(daemon.socket, "pause", id=torrent_id)
        daemon.wait(torrent_id, lambda r: r["state"] == "paused", args.timeout)
        relay.hash_policy = None
        control(daemon.socket, "resume", id=torrent_id)
        result["complete"] = daemon.wait(torrent_id, lambda r: r["state"] == "seeding", args.timeout)
        result["files"] = wire.verify_files(daemon.output, wire.spill_files(lt.bdecode(torrent.read_bytes())), expected)
        result["status"] = "passed"
        return result
    finally:
        if daemon: daemon.close()
        source.stop()
        if relay: result["wire"] = relay.stop()
        if tracker: tracker.close()
        (root / "case.json").write_text(json.dumps(result, indent=2))


def generated_torrent(lt, root, size, piece_length, hybrid=False):
    root.mkdir()
    wire.write_payload(root / "payload.bin", size)
    flags = 0 if hybrid else lt.create_torrent_flags_t.v2_only
    creator = lt.create_torrent([lt.create_file_entry("payload.bin", size)], piece_length, flags)
    lt.set_piece_hashes(creator, str(root))
    torrent = root / "source.torrent"
    torrent.write_bytes(lt.bencode(creator.generate()))
    return torrent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-network", action="store_true", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--scenario", choices=("magnets", "layers", "retry", "acquisition", "restart", "all"), default="all")
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--artifacts", type=Path)
    args = parser.parse_args()
    import libtorrent as lt
    assert tuple(map(int, lt.__version__.split(".")[:2])) >= (2, 1), lt.__version__
    build = ["cargo", "build", "--locked", "--bin", "spill"] + (["--release"] if args.release else [])
    subprocess.run(build, check=True)
    binary = Path("target", "release" if args.release else "debug", "spill").resolve()
    base = Path("target/interop")
    base.mkdir(exist_ok=True)
    root = args.artifacts.resolve() if args.artifacts else Path(tempfile.mkdtemp(prefix="lt-magnets-", dir=base)).resolve()
    if args.artifacts: root.mkdir()
    result = {"revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "modified_paths": subprocess.check_output(["git", "status", "--porcelain"], text=True).splitlines(),
              "libtorrent": lt.__version__, "host": args.host, "binary_sha256": wire.digest(binary),
              "helpers": {name: wire.digest(Path(__file__).with_name(name)) for name in
                          ("libtorrent_magnets.py", "libtorrent_discovery.py", "libtorrent_v2.py", "transmission_smoke.py")},
              "checks": [], "passed": False}
    print(f"libtorrent {lt.__version__}; artifacts: {root}", flush=True)
    try:
        if args.scenario in ("magnets", "all"):
            for name in ("v2.torrent", "v2-leaves.torrent", *wire.HYBRID_FIXTURES):
                for topic in (("v2",) if name.startswith("v2") else ("v1", "v2", "dual")):
                    print(f"magnets both ways: {name} {topic}", flush=True)
                    result["checks"].append(magnet_case(lt, args, binary, root / f"{name}-{topic}", wire.FIXTURES / name, topic))
        if args.scenario in ("retry", "all"):
            for policy in ("reject", "corrupt", "stall", "mixed"):
                print(f"acquisition retry: {policy}", flush=True)
                result["checks"].append(recovery_case(lt, args, binary, root / policy, policy))
        if args.scenario in ("acquisition", "all"):
            print("pause and SIGKILL during hash acquisition", flush=True)
            result["checks"].append(acquisition_case(lt, args, binary, root / "acquisition"))
        if args.scenario in ("restart", "all"):
            for hybrid in (False, True):
                torrent = generated_torrent(lt, root / ("generated-hybrid" if hybrid else "generated-v2"),
                                            8 * 1024 * 1024 + 123, 262144, hybrid)
                print(f"magnet partial SIGKILL/restart: hybrid={hybrid}", flush=True)
                result["checks"].append(magnet_case(lt, args, binary, root / f"restart-{hybrid}", torrent, "dual" if hybrid else "v2", kill=True))
        if args.scenario in ("layers", "all"):
            print("multi-range 1025-hash layer in both magnet directions", flush=True)
            torrent = generated_torrent(lt, root / "generated-layer", 16 * 1024 * 1024 + 123, 16384)
            result["checks"].append(magnet_case(lt, args, binary, root / "large-layer", torrent, "v2"))
            assert {request["index"] for request in result["checks"][-1]["requests"]} >= {0, 512, 1024}
        result["passed"] = True
    except Exception as error:
        result["error"] = repr(error)
        raise
    finally:
        (root / "result.json").write_text(json.dumps(result, indent=2))
    print(f"passed: {root / 'result.json'}", flush=True)


if __name__ == "__main__":
    main()
