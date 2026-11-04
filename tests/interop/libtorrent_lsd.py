#!/usr/bin/env python3
"""Dev-machine tier 2 LSD-only discovery, magnets and transfers with libtorrent 2.1+.

Uses numeric local LAN addresses (IPv4 or IPv6), no trackers, DHT bootstrap,
PEX, uTP or port mapping. Starts libtorrent first to check shared port 6771. Resumes spill's seed
for the reverse direction so a new announce can bootstrap the magnet. Installs nothing.
"""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile

import libtorrent_v2 as wire
from libtorrent_discovery import Daemon
from transmission_smoke import control


def case(lt, args, binary, root, kind):
    root.mkdir()
    payload = root / "seed"
    expected = wire.write_payload(payload / "x", 1024 * 1024 + 123)
    flags = {"v1": lt.create_torrent.v1_only,
             "v2": lt.create_torrent.v2_only,
             "hybrid": 0}[kind]
    creator = lt.create_torrent([lt.create_file_entry("x", 1024 * 1024 + 123)], 32768, flags)
    lt.set_piece_hashes(creator, str(payload))
    torrent = root / "source.torrent"
    torrent.write_bytes(lt.bencode(creator.generate()))
    hashes = lt.load_torrent_file(str(torrent)).ti.info_hashes()
    magnet = (f"magnet:?xt=urn:btih:{hashes.v1}" if hashes.has_v1()
              else f"magnet:?xt=urn:btmh:1220{hashes.v2}")
    listen = f"[{args.host}]" if ":" in args.host else args.host
    peers = args.peer_host or [args.host]
    with socket.socket(socket.AF_INET6 if ":" in peers[0] else socket.AF_INET) as reserve:
        reserve.bind((peers[0], 0))
        port = reserve.getsockname()[1]
    peer_listen = ",".join(f"[{peer}]:{port}" if ":" in peer else f"{peer}:{port}" for peer in peers)
    source = wire.Libtorrent(lt, root / "libtorrent-seed", torrent, payload, peers[0], lsd=True, listen=peer_listen)
    daemon = leecher = None
    try:
        source.complete(args.timeout, seed=True)
        # libtorrent has already bound 6771 under its normal socket options.
        daemon = Daemon(binary, root / "daemon", listen, trackers=False, dht=True,
                        bootstrap=(), pex=False, rate_limit=256 * 1024)
        torrent_id = control(daemon.socket, "add", magnet=magnet)["id"]
        seen = set()

        def downloaded(row):
            seen.update(p["source"] for p in (row.get("transfer") or {}).get("peers", []))
            source.poll()
            return row["state"] == "seeding"

        row = daemon.wait(torrent_id, downloaded, args.timeout)
        health = control(daemon.socket, "health")
        assert health["network"]["lsd"]["announces"] > 0, health
        assert seen & {"lsd", "metadata", "inbound"}, seen
        if "lsd" not in seen:
            assert health["network"]["inbound_tcp"]["total"] > 0, health
        assert wire.digest(daemon.output / "x") == expected
        source.stop()
        # A fresh registration announces after the magnet is listening, rather
        # than making each format wait five minutes for the regular cadence.
        control(daemon.socket, "pause", id=torrent_id)
        daemon.wait(torrent_id, lambda row: row["state"] == "paused", 15)
        leecher = wire.Libtorrent(lt, root / "libtorrent-download", torrent,
                                  root / "fresh", peers[0], lsd=True, magnet=magnet, listen=peer_listen)
        control(daemon.socket, "resume", id=torrent_id)
        reverse = leecher.complete(args.timeout, seed=True)
        assert wire.digest(root / "fresh" / "x") == expected
        assert any("spill" in client for client in reverse["clients"]), reverse
        return {"format": kind, "download": row, "upload": reverse, "sources": sorted(seen), "network": health["network"]}
    finally:
        if leecher:
            leecher.stop()
        source.stop()
        if daemon:
            daemon.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-network", action="store_true", required=True)
    parser.add_argument("--host", required=True, help="numeric IPv4 or IPv6 address of this machine's LAN interface")
    parser.add_argument("--peer-host", action="append", help="local libtorrent bind address; repeat for multiple addresses; defaults to --host")
    parser.add_argument("--format", choices=("v1", "v2", "hybrid", "all"), default="all")
    parser.add_argument("--timeout", type=float, default=420)
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--artifacts", type=Path, help="new directory")
    args = parser.parse_args()
    if os.environ.get("CI"):
        parser.error("tier 2 runs are dev-machine-only, never CI")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    for host in [args.host, *(args.peer_host or [])]:
        family = socket.AF_INET6 if ":" in host else socket.AF_INET
        try:
            socket.inet_pton(family, host)
            with socket.socket(family) as check:
                check.bind((host, 0))
        except OSError:
            parser.error("--host and --peer-host must be local numeric LAN addresses")
    import libtorrent as lt
    if tuple(int(v) for v in lt.__version__.split(".")[:2]) < (2, 1):
        parser.error("libtorrent 2.1+ is required")
    subprocess.run(["cargo", "build", "--locked"] + (["--release"] if args.release else []), check=True)
    binary = (Path("target") / ("release" if args.release else "debug") / "spill").resolve()
    parent = Path("target/interop").resolve()
    parent.mkdir(parents=True, exist_ok=True)
    root = args.artifacts.resolve() if args.artifacts else Path(tempfile.mkdtemp(prefix="libtorrent-lsd-", dir=parent))
    if args.artifacts:
        root.mkdir(parents=True)
    result = {"libtorrent": lt.__version__, "host": args.host, "peer_hosts": args.peer_host or [args.host],
              "binary_sha256": wire.digest(binary), "harness_sha256": wire.digest(Path(__file__)),
              "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "checks": []}
    print(f"libtorrent {lt.__version__}; artifacts: {root}", flush=True)
    try:
        for kind in ("v1", "v2", "hybrid") if args.format == "all" else (args.format,):
            print(f"LSD magnets and payload both ways: {kind}", flush=True)
            result["checks"].append(case(lt, args, binary, root / kind, kind))
        result["status"] = "passed"
    except Exception as error:
        result["status"], result["error"] = "failed", repr(error)
        raise
    finally:
        (root / "result.json").write_text(json.dumps(result, indent=2))
        print(f"{result['status']}: {root / 'result.json'}", flush=True)


if __name__ == "__main__":
    main()
