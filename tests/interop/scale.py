#!/usr/bin/env python3
"""Opt-in disk baselines against the real daemon; only local HTTP sources are used."""
import argparse
import base64
import hashlib
import http.server
import json
import pathlib
import sqlite3
import socket
import subprocess
import tempfile
import threading
import time

from transmission_smoke import (
    Engine, bencode, control, sha256_file, wait_for, wait_for_control, write_config,
)

CHUNK = bytes(range(256)) * 1024


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=("multi-torrent", "many-files"))
    parser.add_argument("--allow-network", action="store_true", required=True)
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--daemon", type=pathlib.Path, default=pathlib.Path("target/release/spill"))
    parser.add_argument("--artifacts", type=pathlib.Path, help="fresh directory; otherwise beneath target/interop/")
    parser.add_argument("--allow-tmpfs", action="store_true")
    parser.add_argument("--keep", action="store_true", help="retain payloads and catalog entries after success")
    parser.add_argument("--files", type=int, default=60_001, help="many-files count, including one nonempty file (1..100000)")
    parser.add_argument("--soak-seconds", type=float, default=60, help="completed multi-torrent soak duration")
    parser.add_argument("--timeout", type=float, default=300, help="seconds per completion/restart stage")
    parser.add_argument("--piece-length", type=int, default=len(CHUNK), help="piece bytes for multi-torrent (multiple of 256 KiB, up to 16 MiB)")
    parser.add_argument("--dead-udp-tracker", action="store_true", help="include a local UDP tracker that receives requests but never replies")
    args = parser.parse_args()
    if not 1 <= args.files <= 100_000 or args.soak_seconds < 0 or args.timeout <= 0:
        parser.error("invalid file count, soak duration or timeout")
    if args.release:
        subprocess.run(["cargo", "build", "--release", "--locked"], check=True)
        args.daemon = pathlib.Path("target/release/spill")
    parent = args.artifacts.resolve().parent if args.artifacts else pathlib.Path("target/interop").resolve()
    parent.mkdir(parents=True, exist_ok=True)
    filesystem = subprocess.check_output(["findmnt", "-n", "-o", "FSTYPE", "--target", str(parent)], text=True).strip()
    if filesystem in ("tmpfs", "ramfs") and not args.allow_tmpfs:
        parser.error("tmpfs artifacts require --allow-tmpfs; use disk for comparisons")
    if args.artifacts:
        root = args.artifacts.resolve()
        root.mkdir(mode=0o700)
    else:
        root = pathlib.Path(tempfile.mkdtemp(prefix=f"{args.scenario}-", dir=parent))

    multi = args.scenario == "multi-torrent"
    if not len(CHUNK) <= args.piece_length <= 16 * 1024**2 or args.piece_length % len(CHUNK):
        parser.error("piece length must be a multiple of 256 KiB up to 16 MiB")
    if multi and 16 * 1024**2 % args.piece_length:
        parser.error("piece length must divide the 16 MiB fixtures")
    if not multi and args.piece_length != len(CHUNK):
        parser.error("--piece-length is only supported by multi-torrent")
    sizes = ({"large.bin": 256 * 1024**2} | {f"payload-{i:02}.bin": 16 * 1024**2 for i in range(32)}
             if multi else {"many-files/payload.bin": len(CHUNK)})
    downloads, seeds = (4, 33) if multi else (1, 1)
    limit = 4 * 1024**2 if multi else 0
    result = {
        "passed": False, "scenario": args.scenario, "scenario_version": 2,
        "source_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "source_dirty": subprocess.run(["git", "diff", "--quiet"]).returncode != 0,
        "runner_sha256": sha256_file(pathlib.Path(__file__)),
        "daemon_sha256": sha256_file(args.daemon.resolve()),
        "filesystem": filesystem, "artifacts": str(root),
        "torrents": len(sizes), "files": len(sizes) if multi else args.files,
        "bytes": sum(sizes.values()), "active_downloads": downloads, "active_seeds": seeds,
        "download_limit_per_torrent": limit,
        "piece_length": args.piece_length, "dead_udp_tracker": args.dead_udp_tracker,
        "completed_soak_seconds": args.soak_seconds if multi else 0,
        "samples": [],
    }
    requests = 0
    request_lock = threading.Lock()
    udp_sources = set()
    udp_transactions = set()
    udp_stop = threading.Event()
    blackhole = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) if args.dead_udp_tracker else None
    udp_thread = None
    if blackhole:
        blackhole.bind(("127.0.0.1", 0))
        blackhole.settimeout(.2)
        def discard_udp():
            while not udp_stop.is_set():
                try:
                    packet, source = blackhole.recvfrom(65536)
                    assert len(packet) >= 16
                    with request_lock:
                        udp_sources.add(source)
                        udp_transactions.add(packet[12:16])
                except socket.timeout:
                    pass
        udp_thread = threading.Thread(target=discard_udp, daemon=True)
        udp_thread.start()

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            nonlocal requests
            name = self.path.removeprefix("/")
            if name not in sizes:
                self.send_error(404)
                return
            start, end = map(int, self.headers["Range"].removeprefix("bytes=").split("-"))
            assert 0 <= start <= end < sizes[name]
            with request_lock:
                requests += 1
            offset = start % len(CHUNK)
            length = end - start + 1
            body = (CHUNK * ((offset + length + len(CHUNK) - 1) // len(CHUNK)))[offset:offset + length]
            self.send_response(206)
            self.send_header("Content-Length", str(length))
            self.send_header("Content-Range", f"bytes {start}-{end}/{sizes[name]}")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *unused):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    socket_path, config = root / "control.sock", root / "spill.toml"
    write_config(config, state=root / "state", destination=root / "downloads",
                 control_socket=socket_path, host="127.0.0.1", trackers=args.dead_udp_tracker, web_seeds=True)
    config.write_text(config.read_text().replace("active_downloads = 1", f"active_downloads = {downloads}")
                      .replace("active_seeds = 1", f"active_seeds = {seeds}")
                      .replace("download_limit = 0", f"download_limit = {limit}")
                      .replace("[defaults]", "peers = 4\ncache_bytes = 262144\nwrite_queue_bytes = 1048576\n\n[defaults]"))
    files, processes = [], []
    engine = Engine(args, root, config, files, processes)
    started = time.monotonic()

    def sample(stage):
        assert engine.process.poll() is None, f"daemon exited: {engine.process.returncode}"
        before = time.monotonic()
        rows = control(socket_path, "list")["torrents"]
        latency = time.monotonic() - before
        for row in rows:
            tracker_timeout = (args.dead_udp_tracker and row["state"] in ("downloading", "seeding")
                               and row["last_error"] == "timed out"
                               and any(t["error"] == "timed out" for t in row["transfer"]["trackers"]))
            assert row["last_error"] is None or tracker_timeout, (row["id"], row["last_error"])
        text = pathlib.Path(f"/proc/{engine.pid}/status").read_text()
        status = dict(line.split(":", 1) for line in text.splitlines() if ":" in line)
        states = {}
        for row in rows:
            states[row["state"]] = states.get(row["state"], 0) + 1
        value = {"seconds": round(time.monotonic() - started, 3), "stage": stage,
                 "rss_kib": int(status["VmRSS"].split()[0]),
                 "peak_rss_kib": int(status["VmHWM"].split()[0]),
                 "fds": len(list(pathlib.Path(f"/proc/{engine.pid}/fd").iterdir())),
                 "control_ms": round(latency * 1000, 3), "states": states,
                 "verified_bytes": sum(row["verified_bytes"] for row in rows)}
        result["samples"].append(value)
        if stage == "download":
            assert states.get("downloading", 0) <= downloads, states
        return rows

    def all_seeding(stage):
        deadline, last_report = time.monotonic() + args.timeout, 0
        while time.monotonic() < deadline:
            rows = sample(stage)
            if len(rows) == len(sizes) and all(row["state"] == "seeding" and row["transfer"]["ready"] for row in rows):
                assert all(row["verified_bytes"] == row["total_bytes"] for row in rows)
                return
            if time.monotonic() - last_report >= 10:
                print(stage, result["samples"][-1], flush=True)
                last_report = time.monotonic()
            time.sleep(.25)
        raise TimeoutError(f"{stage}: {result['samples'][-1]}")

    def shutdown():
        control(socket_path, "shutdown")
        assert engine.process.wait(timeout=args.timeout) == 0

    def checkpoint(torrent_id):
        def saved():
            with sqlite3.connect(root / "state/catalog.sqlite") as db:
                resume, complete = db.execute("SELECT resume,complete FROM torrents WHERE id=?", (torrent_id,)).fetchone()
            return (resume, complete) if complete and resume else None
        resume, complete = wait_for("complete checkpoint", saved, args.timeout)
        fingerprint_bytes = int.from_bytes(resume[32:36], "little")
        assert fingerprint_bytes == args.files * 40, fingerprint_bytes
        return {"resume_bytes": len(resume), "fingerprint_bytes": fingerprint_bytes, "complete": bool(complete)}

    try:
        engine.start()
        wait_for_control(engine, socket_path)
        ids = []
        piece_hash = hashlib.sha1(CHUNK * (args.piece_length // len(CHUNK))).digest()
        if multi:
            documents = []
            for name, size in sizes.items():
                info = {b"name": name.encode(), b"length": size, b"piece length": args.piece_length,
                        b"pieces": piece_hash * (size // args.piece_length)}
                documents.append((name, {b"info": info, b"url-list": f"http://127.0.0.1:{server.server_port}/{name}".encode()}))
        else:
            entries = [{b"path": [f"empty-{i:05}".encode()], b"length": 0} for i in range(args.files - 1)]
            entries.append({b"path": [b"payload.bin"], b"length": len(CHUNK)})
            info = {b"name": b"many-files", b"files": entries, b"piece length": len(CHUNK), b"pieces": piece_hash}
            documents = [("many-files", {b"info": info, b"url-list": f"http://127.0.0.1:{server.server_port}/".encode()})]
        for name, document in documents:
            if blackhole:
                document[b"announce"] = f"udp://127.0.0.1:{blackhole.getsockname()[1]}/announce".encode()
            meta = bencode(document)
            (root / f"{name}.torrent").write_bytes(meta)
            ids.append(control(socket_path, "add", metainfo=base64.b64encode(meta).decode("ascii"))["id"])
        all_seeding("download")
        result["download_seconds"] = round(time.monotonic() - started, 3)
        for name, size in sizes.items():
            expected = hashlib.sha256()
            for _ in range(size // len(CHUNK)):
                expected.update(CHUNK)
            assert sha256_file(root / "downloads" / name) == expected.hexdigest(), name
        result["all_sha256_match"] = True
        if not multi:
            entries = list((root / "downloads/many-files").iterdir())
            assert len(entries) == args.files
            assert all(path.stat().st_size == 0 for path in entries if path.name != "payload.bin")
            result["checkpoint"] = checkpoint(ids[0])
        before_requests = requests
        soak_started = time.monotonic()
        while multi and time.monotonic() - soak_started < args.soak_seconds:
            assert all(row["state"] == "seeding" for row in sample("completed-soak"))
            time.sleep(.5)
        assert requests == before_requests, (before_requests, requests)
        if blackhole:
            with request_lock:
                assert len(udp_sources) == 1, "UDP announces must share the session socket"
                assert len(udp_transactions) >= len(sizes), "UDP announces did not run independently"
                result["dead_udp_tracker_sources_before_restart"] = len(udp_sources)
                result["dead_udp_tracker_transactions_before_restart"] = len(udp_transactions)
        if not multi:
            control(socket_path, "pause", id=ids[0])
            control(socket_path, "set", id=ids[0], values={"web_seeds": False})
        shutdown()
        downloads = 0
        config.write_text(config.read_text().replace(f"active_downloads = {result['active_downloads']}", "active_downloads = 0"))
        restart_started = time.monotonic()
        engine.start()
        wait_for_control(engine, socket_path)
        if not multi:
            control(socket_path, "resume", id=ids[0])
        all_seeding("restart")
        result["restart_seconds"] = round(time.monotonic() - restart_started, 3)
        assert requests == before_requests, (before_requests, requests)
        result["restart_no_refetch"] = True
        if not multi:
            result["restart_checkpoint"] = checkpoint(ids[0])
        if not args.keep:
            for torrent_id in ids:
                control(socket_path, "remove", id=torrent_id, delete_payload=True)
            assert control(socket_path, "list")["torrents"] == []
            assert list((root / "downloads").iterdir()) == []
            result["payload_and_catalog_cleanup"] = True
        shutdown()
        result["joined_shutdown"] = True
        result["passed"] = True
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
        result["engine"] = engine.finish()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        if blackhole:
            udp_stop.set()
            udp_thread.join(timeout=5)
            blackhole.close()
            result["dead_udp_tracker_sources"] = len(udp_sources)
            result["dead_udp_tracker_transactions"] = len(udp_transactions)
        for file in files:
            file.close()
        result["seconds"] = round(time.monotonic() - started, 3)
        result["http_requests"] = requests
        result["peak_rss_mib"] = result["engine"]["peak_rss_mib"]
        result["peak_fds"] = max((s["fds"] for s in result["samples"]), default=0)
        latencies = sorted(s["control_ms"] for s in result["samples"])
        result["control_p95_ms"] = latencies[int(.95 * (len(latencies) - 1))] if latencies else None
        (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({k: v for k, v in result.items() if k != "samples"}, indent=2))


if __name__ == "__main__":
    main()
