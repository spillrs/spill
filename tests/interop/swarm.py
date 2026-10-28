#!/usr/bin/env python3
"""Opt-in LAN peer-wire baseline: one Transmission seed and N networked leechers."""
import argparse
import base64
import hashlib
import http.server
import json
import pathlib
import shutil
import socket
import struct
import subprocess
import tempfile
import threading
import time
import urllib.parse

from transmission_smoke import (
    Engine, Transmission, bencode, byte_size, control, free_port, sha256_file,
    wait_for, write_config,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-network", action="store_true", required=True)
    parser.add_argument("--host", required=True, help="IPv4 address on your LAN interface")
    parser.add_argument("--leecher", choices=("spill", "transmission"), default="spill")
    parser.add_argument("--transport", choices=("utp", "tcp"), default="tcp")
    parser.add_argument("--leechers", type=int, default=8)
    parser.add_argument("--size", type=byte_size, default=256 * 1024**2, help="payload bytes per leecher")
    parser.add_argument("--piece-length", type=byte_size, default=1024**2)
    parser.add_argument("--timeout", type=float, default=300, help="seconds allowed for the swarm download")
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--daemon", type=pathlib.Path, default=pathlib.Path("target/release/spill"))
    parser.add_argument("--artifacts", type=pathlib.Path, help="fresh directory; otherwise beneath target/interop/")
    parser.add_argument("--allow-tmpfs", action="store_true")
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.leechers <= 32 or args.timeout <= 0:
        parser.error("use 1..32 leechers and a positive timeout")
    transmission_bin = shutil.which("transmission-daemon")
    if not transmission_bin:
        parser.error("install Transmission 4.1+ first; this script never installs packages")
    if args.release:
        subprocess.run(["cargo", "build", "--release", "--locked"], check=True)
        args.daemon = pathlib.Path("target/release/spill")
    parent = args.artifacts.resolve().parent if args.artifacts else pathlib.Path("target/interop").resolve()
    parent.mkdir(parents=True, exist_ok=True)
    filesystem = subprocess.check_output(["findmnt", "-n", "-o", "FSTYPE", "--target", str(parent)], text=True).strip()
    if filesystem in ("tmpfs", "ramfs") and not args.allow_tmpfs:
        parser.error("tmpfs requires --allow-tmpfs; use disk for comparisons")
    if args.artifacts:
        root = args.artifacts.resolve()
        root.mkdir(mode=0o700)
    else:
        root = pathlib.Path(tempfile.mkdtemp(prefix=f"swarm-{args.leecher}-", dir=parent))
    result = {
        "passed": False, "scenario": "swarm", "scenario_version": 1,
        "leecher": args.leecher, "transport": args.transport, "leechers": args.leechers, "bytes_per_leecher": args.size,
        "aggregate_bytes": args.size * args.leechers, "piece_length": args.piece_length,
        "host": args.host, "filesystem": filesystem, "artifacts": str(root),
        "transmission": subprocess.check_output([transmission_bin, "--version"], text=True, stderr=subprocess.STDOUT).strip(),
        "source_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "source_dirty": subprocess.run(["git", "diff", "--quiet"]).returncode != 0,
        "runner_sha256": sha256_file(pathlib.Path(__file__)),
        "daemon_sha256": sha256_file(args.daemon.resolve()) if args.leecher == "spill" else None,
        "samples": [], "tracker_exchanges": [], "peer_clients": [],
    }
    ports = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urllib.parse.urlsplit(self.path)
            if parsed.path != "/announce":
                self.send_error(404)
                return
            query = urllib.parse.parse_qs(parsed.query)
            port = int(query["port"][0])
            peers = b"".join(socket.inet_aton(args.host) + struct.pack("!H", peer) for peer in ports if peer != port)
            result["tracker_exchanges"].append({"port": port, "left": int(query["left"][0]), "event": query.get("event", [None])[0]})
            body = bencode({b"interval": 1, b"complete": 1, b"incomplete": args.leechers, b"peers": peers})
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *unused):
            pass

    server = http.server.ThreadingHTTPServer((args.host, 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    files, processes = [], []
    engine = Engine(args, root, root / "unused-config", files, processes)
    members = []
    clients = set()

    def launch_transmission(name, destination):
        config = root / name / "config"
        config.mkdir(parents=True)
        (config / "settings.json").write_text(json.dumps({
            "dht-enabled": False, "pex-enabled": False, "lpd-enabled": False,
            "port-forwarding-enabled": False, "utp-enabled": args.transport == "utp", "default-trackers": "",
            "rpc-host-whitelist-enabled": False, "ratio-limit-enabled": False,
            "speed-limit-up-enabled": False, "speed-limit-down-enabled": False,
            "alt-speed-enabled": False, "incomplete-dir-enabled": False,
            "peer-limit-global": 64, "peer-limit-per-torrent": 64,
        }))
        peer_port, rpc_port = free_port(args.host), free_port("127.0.0.1")
        engine.start([transmission_bin, "-f", "-g", str(config), "-w", str(destination),
                      "-p", str(rpc_port), "-P", str(peer_port), "-r", "127.0.0.1", "-i", args.host,
                      "-T", "-O", "-Y", "-M", "-B", "-et", "--preferred-transports", args.transport, "--log-level", "info"])
        client = Transmission(rpc_port)
        wait_for(f"{name} RPC", lambda: client.rpc("session-get"), 10)
        ports.append(peer_port)
        return client

    def torrent(client):
        values = client.rpc("torrent-get", fields=["percentDone", "status", "downloadedEver", "uploadedEver", "errorString", "peers"])["torrents"]
        if not values:
            return None
        value = values[0]
        assert not value["errorString"], value
        clients.update(peer["clientName"] for peer in value["peers"])
        return value

    try:
        seed_dir = root / "seed"
        seed_dir.mkdir()
        pattern = bytes(range(256)) * 4096
        pieces, digest = [], hashlib.sha256()
        with (seed_dir / "payload.bin").open("wb") as output:
            for offset in range(0, args.size, args.piece_length):
                length = min(args.piece_length, args.size - offset)
                block = (pattern * ((length + len(pattern) - 1) // len(pattern)))[:length]
                output.write(block)
                digest.update(block)
                pieces.append(hashlib.sha1(block).digest())
        digest = digest.hexdigest()
        info = {b"name": b"payload.bin", b"length": args.size, b"piece length": args.piece_length, b"pieces": b"".join(pieces)}
        meta = bencode({b"announce": f"http://{args.host}:{server.server_port}/announce".encode(), b"info": info})
        fixture = root / "fixture.torrent"
        fixture.write_bytes(meta)
        result["sha256"] = digest
        seed = launch_transmission("seed", seed_dir)
        seed.rpc("torrent-add", filename=str(fixture), **{"download-dir": str(seed_dir)})
        wait_for("seed verification", lambda: (value := torrent(seed)) and value["percentDone"] == 1 and value["status"] == 6, args.timeout)
        for index in range(args.leechers):
            name = f"leecher-{index:02}"
            destination = root / name / "downloads"
            destination.mkdir(parents=True)
            if args.leecher == "transmission":
                client = launch_transmission(name, destination)
                member = {"client": client}
            else:
                socket_path, config = root / name / "control.sock", root / name / "spill.toml"
                write_config(config, state=root / name / "state", destination=destination,
                             control_socket=socket_path, host=args.host, utp=args.transport == "utp")
                engine.start([str(args.daemon.resolve()), "serve", "--config", str(config)])
                process = engine.process
                def ready():
                    assert process.poll() is None, f"{name} exited: {process.returncode}"
                    return control(socket_path, "health")
                health = wait_for(f"{name} control", ready, 10)
                ports.append(int(health["listen"].rsplit(":", 1)[1]))
                member = {"socket": socket_path}
            members.append(member | {"destination": destination, "first_payload": None, "completed": None,
                                     "uploaded_bytes": 0, "peak_peers": 0, "utp_seen": False})
        started = previous = time.monotonic()
        for member in members:
            if args.leecher == "spill":
                member["id"] = control(member["socket"], "add", metainfo=base64.b64encode(meta).decode("ascii"))["id"]
            else:
                member["client"].rpc("torrent-add", filename=str(fixture), **{"download-dir": str(member["destination"])})
        peer_trace = (root / "peer-samples.jsonl").open("w")
        files.append(peer_trace)
        result["peer_trace"] = "peer-samples.jsonl"
        last_report = started
        while time.monotonic() - started < args.timeout:
            assert all(process.poll() is None for process in processes), "swarm process exited"
            seed_status = torrent(seed)
            peers = seed_status["peers"]
            connected, uploaded, downloaded = 0, 0, 0
            peer_snapshots = []
            for index, member in enumerate(members):
                if args.leecher == "spill":
                    row = control(member["socket"], "get", id=member["id"])
                    assert row["last_error"] is None, row
                    transfer = row["transfer"] or {}
                    member["utp_seen"] |= any(peer["transport"] == "utp" for peer in transfer.get("peers", []))
                    count = transfer.get("active_peers", 0)
                    received = transfer.get("downloaded_bytes", 0)
                    sent = transfer.get("uploaded_bytes", 0)
                    complete = row["state"] == "seeding" and transfer.get("ready", False)
                    if complete:
                        assert row["verified_bytes"] == args.size, row
                    peer_snapshots.append({"leecher": index, "state": row["state"],
                                           "downloaded_bytes": received,
                                           "writer_free_slots": transfer.get("writer_free_slots"),
                                           "upload_reads_in_flight": transfer.get("upload_reads_in_flight"),
                                           "upload_reads_finished": transfer.get("upload_reads_finished"),
                                           "peers": transfer.get("peers", [])})
                else:
                    value = torrent(member["client"])
                    count = len(value["peers"]) if value else 0
                    received = value["downloadedEver"] if value else 0
                    sent = value["uploadedEver"] if value else 0
                    complete = value is not None and value["percentDone"] == 1
                    peer_snapshots.append({"leecher": index, "downloaded_bytes": received,
                                           "peers": value["peers"] if value else []})
                if received and member["first_payload"] is None:
                    member["first_payload"] = previous - started
                if complete and member["completed"] is None:
                    member["completed"] = time.monotonic() - started
                member["uploaded_bytes"] = sent
                member["peak_peers"] = max(member["peak_peers"], count)
                connected += count
                downloaded += received
                uploaded += sent
            now = time.monotonic()
            rss = []
            for process in processes:
                status = pathlib.Path(f"/proc/{process.pid}/status").read_text()
                rss.append(int(next(line.split()[1] for line in status.splitlines() if line.startswith("VmRSS:"))))
            sample = {"seconds": round(now - started, 3), "completed": sum(member["completed"] is not None for member in members),
                      "downloaded_bytes": downloaded, "leecher_uploaded_bytes": uploaded,
                      "leecher_peer_connections": connected, "seed_peer_connections": len(peers),
                      "seed_choked_peers": sum(peer["peerIsChoked"] for peer in peers),
                      "seed_uploading_peers": sum(peer["rateToPeer"] > 0 for peer in peers),
                      "seed_rss_kib": rss[0], "leecher_total_rss_kib": sum(rss[1:])}
            result["samples"].append(sample)
            peer_trace.write(json.dumps({"seconds": sample["seconds"], "leechers": peer_snapshots}) + "\n")
            peer_trace.flush()
            if sample["completed"] == args.leechers:
                break
            if now - last_report >= 5:
                print(sample, flush=True)
                last_report = now
            previous = now
            time.sleep(.1)
        else:
            raise TimeoutError(f"swarm download: {result['samples'][-1]}")
        elapsed = time.monotonic() - started
        for member in members:
            assert sha256_file(member["destination"] / "payload.bin") == digest
        if args.leechers > 1:
            assert max(member["peak_peers"] for member in members) > 1, "no multi-peer connections observed"
        if args.leecher == "spill":
            if args.transport == "utp":
                assert all(member["utp_seen"] for member in members), "uTP was not observed for every leecher"
            assert any(client.startswith("Spill/") for client in clients), clients
            for member in members:
                control(member["socket"], "shutdown")
        for process in processes:
            if process.poll() is None:
                process.terminate()
            assert process.wait(timeout=60) == 0
        result.update({"passed": True, "all_sha256_match": True, "joined_shutdown": True,
                       "seconds": round(elapsed, 3), "aggregate_mb_per_second": round(args.size * args.leechers / elapsed / 1e6, 3),
                       "seed_uploaded_bytes": seed_status["uploadedEver"],
                       "leecher_uploaded_bytes": sum(member["uploaded_bytes"] for member in members),
                       "leechers_metrics": [{key: member[key] for key in ("first_payload", "completed", "uploaded_bytes", "peak_peers")} for member in members]})
        first = min(member["first_payload"] for member in members)
        result["payload_seconds"] = round(elapsed - first, 3)
        result["payload_aggregate_mb_per_second"] = round(args.size * args.leechers / (elapsed - first) / 1e6, 3)
        result["payload_timing"] = "first observed leecher payload counter to all completions; 100 ms sampling; Spill includes durability barrier, Transmission uses percentDone"
        if not args.keep:
            for directory in [seed_dir] + [member["destination"] for member in members]:
                shutil.rmtree(directory)
    except BaseException as error:
        result["error"] = repr(error)
        raise
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        result["process_metrics"] = engine.finish()
        result["peer_clients"] = sorted(clients)
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        for file in files:
            file.close()
        result["peak_leecher_total_rss_mib"] = round(max((sample["leecher_total_rss_kib"] for sample in result["samples"]), default=0) / 1024, 3)
        result["peak_seed_peer_connections"] = max((sample["seed_peer_connections"] for sample in result["samples"]), default=0)
        result["peak_leecher_peer_connections"] = max((sample["leecher_peer_connections"] for sample in result["samples"]), default=0)
        result["peak_seed_choked_peers"] = max((sample["seed_choked_peers"] for sample in result["samples"]), default=0)
        result["peak_seed_uploading_peers"] = max((sample["seed_uploading_peers"] for sample in result["samples"]), default=0)
        (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({key: value for key, value in result.items() if key not in ("samples", "tracker_exchanges")}, indent=2))


if __name__ == "__main__":
    main()
