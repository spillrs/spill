#!/usr/bin/env python
"""Opt-in LAN interop: Spill downloads from a local peer, then seeds back to it.

Requires Transmission 4.1+ (for --preferred-transports), Python 3.11+ and a built daemon.
No public tracker, DHT, PEX, LPD, NAT mapping or encryption is used.
Logs/results are retained; passing runs remove their generated payloads unless --keep.
"""
import argparse
import base64
import hashlib
import http.server
import json
import os
import pathlib
import re
import signal
import shutil
import socket
import sqlite3
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import urllib.parse


def bencode(value):
    if isinstance(value, int):
        return b"i" + str(value).encode() + b"e"
    if isinstance(value, bytes):
        return str(len(value)).encode() + b":" + value
    if isinstance(value, list):
        return b"l" + b"".join(bencode(item) for item in value) + b"e"
    return b"d" + b"".join(bencode(k) + bencode(v) for k, v in sorted(value.items())) + b"e"


def free_port(host):
    with socket.socket() as sock:
        sock.bind((host, 0))
        return sock.getsockname()[1]


def wait_for(description, fn, seconds=90, *, poll_interval=0.1):
    end = time.monotonic() + seconds
    last = None
    while time.monotonic() < end:
        try:
            last = fn()
            if last:
                return last
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(poll_interval)
    raise TimeoutError(f"{description}: {last!r}")


class Transmission:
    def __init__(self, port):
        self.url = f"http://127.0.0.1:{port}/transmission/rpc"
        self.session = ""
        self.clients = set()
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def rpc(self, method, **arguments):
        data = json.dumps({"method": method, "arguments": arguments}).encode()
        for _ in range(2):
            request = urllib.request.Request(self.url, data, {
                "X-Transmission-Session-Id": self.session,
                "Content-Type": "application/json",
            })
            try:
                with self.http.open(request, timeout=5) as response:
                    reply = json.load(response)
                if reply["result"] != "success":
                    raise RuntimeError(reply)
                return reply["arguments"]
            except urllib.error.HTTPError as error:
                if error.code != 409:
                    raise
                self.session = error.headers["X-Transmission-Session-Id"]
        raise RuntimeError("Transmission session negotiation failed")

    def torrent(self, pieces=False):
        fields = ["id", "percentDone", "status", "errorString", "downloadedEver", "uploadedEver", "hashString", "peersConnected", "peers"]
        if pieces:
            fields.append("pieces")
        result = self.rpc("torrent-get", fields=fields)
        for torrent in result["torrents"]:
            self.clients.update(peer["clientName"] for peer in torrent.pop("peers"))
        return result["torrents"][0] if result["torrents"] else None


def read_exact(sock, size):
    result = b""
    while len(result) < size:
        chunk = sock.recv(size - len(result))
        if not chunk:
            raise ConnectionError("short control reply")
        result += chunk
    return result


class ControlError(RuntimeError):
    def __init__(self, response):
        self.code = response["code"]
        super().__init__(response.get("error") or response)


def control(path, op, **arguments):
    """The daemon's public newline-delimited JSON protocol, without adapters."""
    with socket.socket(socket.AF_UNIX) as sock:
        sock.settimeout(60 if op in ("add", "set", "pause", "remove", "shutdown") else 5)
        sock.connect(str(path))
        sock.sendall(json.dumps({"op": op, **arguments}).encode() + b"\n")
        limit = 64 * 1024 * 1024
        with sock.makefile("rb") as stream:
            data = stream.readline(limit + 1)
        if len(data) > limit or not data.endswith(b"\n"):
            raise RuntimeError("oversized or incomplete control reply")
        result = json.loads(data)
        if result["code"] != 0:
            raise ControlError(result)
        return result["data"]


def write_config(path, *, state, destination, control_socket, host, trackers=True,
                 dht=False, pex=False, web_seeds=False, bootstrap=(), utp=False, rate_limit=0):
    values = {"state_dir": str(state), "download_dir": str(destination),
              "control_socket": str(control_socket), "listen": f"{host}:0",
              "allow_local_network": True, "port_mapping": False, "active_downloads": 1, "active_seeds": 1,
              "dht_bootstrap": list(bootstrap), "utp": utp}
    defaults = {"trackers": trackers, "dht": dht, "pex": pex, "web_seeds": web_seeds,
                "download_limit": rate_limit, "upload_limit": rate_limit, "seed_ratio": 0.0, "seed_time": 0,
                "file_priorities": []}
    path.write_text("\n".join(f"{key} = {json.dumps(value, ensure_ascii=False)}" for key, value in values.items())
                    + "\n\n[defaults]\n"
                    + "\n".join(f"{key} = {json.dumps(value, ensure_ascii=False)}" for key, value in defaults.items()) + "\n")


def wait_for_control(engine, path):
    def ready():
        if engine.process.poll() is not None:
            log = engine.root / f"engine{len(engine.runs) - 1 or ''}.log"
            raise RuntimeError(f"Spill exited with {engine.process.returncode}: {log.read_text()[-4000:]}")
        return control(path, "health")
    return wait_for("engine control", ready, 10)


def torrent_status(path, torrent_id):
    row = control(path, "get", id=torrent_id)
    if row["last_error"]:
        raise RuntimeError(row)
    return row


def log_transport(root, result, logs, direction, started, row, seen):
    transfer = row["transfer"] or {}
    live = sorted({peer["transport"] for peer in transfer.get("peers", [])})
    seen.update(live)
    count = transfer.get("utp_retransmits") or 0
    state = logs.setdefault(direction, {"logged": -1, "previous": count, "carry": 0, "baseline": count})
    if count < state["previous"]:
        state["carry"] += state["previous"]
    state["previous"] = count
    sample = {"direction": direction, "elapsed_seconds": round(time.monotonic() - started, 3),
              "transports": sorted(seen), "live_transports": live,
              "spill_utp_retransmits": state["carry"] + count - state["baseline"],
              "owner_utp_retransmits": count,
              "verified_bytes": row["verified_bytes"], "state": row["state"],
              "downloaded_bytes": transfer.get("downloaded_bytes", 0),
              "uploaded_bytes": transfer.get("uploaded_bytes", 0)}
    result.setdefault("directions", {})[direction] = sample
    if sample["elapsed_seconds"] >= state["logged"] + 1:
        with (root / "transport-samples.jsonl").open("a") as output:
            output.write(json.dumps(sample) + "\n")
        state["logged"] = sample["elapsed_seconds"]
    return sample


def catalog_checkpoint(root, torrent_id):
    with sqlite3.connect(root / "state" / "catalog.sqlite") as db:
        row = db.execute("SELECT resume,uploaded,seed_seconds,complete FROM torrents WHERE id=?", (torrent_id,)).fetchone()
    return dict(zip(("resume", "uploaded", "seed_seconds", "complete"), row))


class Tracker:
    def __init__(self, host, peer_port):
        self.host = host
        self.peer_port = peer_port
        self.engine_port = None
        self.exchanges = []
        self.errors = []
        self.stopped = threading.Event()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((host, 0))
        self.sock.settimeout(0.1)
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    @property
    def url(self):
        return f"udp://{self.host}:{self.sock.getsockname()[1]}/announce"

    def run(self):
        while not self.stopped.is_set():
            try:
                packet, source = self.sock.recvfrom(4096)
                if len(packet) < 16:
                    continue
                action, tx = struct.unpack_from("!II", packet, 8)
                if action == 0:
                    reply = struct.pack("!IIQ", 0, tx, 17)
                elif action == 1 and len(packet) == 98:
                    port, = struct.unpack_from("!H", packet, 96)
                    target = self.engine_port if port == self.peer_port else self.peer_port
                    peers = socket.inet_aton(self.host) + struct.pack("!H", target) if target else b""
                    reply = struct.pack("!IIIII", 1, tx, 1, 1, 1) + peers
                    self.exchanges.append({"port": port, "target": target, "left": struct.unpack_from("!Q", packet, 64)[0]})
                else:
                    continue
                self.sock.sendto(reply, source)
            except socket.timeout:
                continue
            except OSError as error:
                if not self.stopped.is_set():
                    self.errors.append(str(error))
                return

    def close(self):
        self.stopped.set()
        self.thread.join(timeout=2)
        self.sock.close()
        if self.thread.is_alive() or self.errors:
            raise RuntimeError(f"tracker cleanup: {self.errors}")


class HttpTracker:
    def __init__(self, host, peer_port):
        self.host, self.peer_port = host, peer_port
        self.engine_port = None
        self.exchanges = []
        tracker = self
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                parsed = urllib.parse.urlsplit(self.path)
                if parsed.path != "/announce":
                    self.send_error(404)
                    return
                query = urllib.parse.parse_qs(parsed.query)
                port = int(query["port"][0])
                target = tracker.engine_port if port == tracker.peer_port else tracker.peer_port
                peers = socket.inet_aton(host) + struct.pack("!H", target) if target else b""
                tracker.exchanges.append({"port": port, "target": target, "left": int(query["left"][0])})
                body = bencode({b"interval": 1, b"complete": 1, b"incomplete": 1, b"peers": peers})
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass
        self.server = http.server.HTTPServer((host, 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self):
        return f"http://{self.host}:{self.server.server_port}/announce"

    def close(self):
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()
        assert not self.thread.is_alive()


def byte_size(value):
    match = re.fullmatch(r"(\d+)(KiB|MiB|GiB)?", value)
    if not match:
        raise argparse.ArgumentTypeError("use bytes or an integer with KiB/MiB/GiB")
    size = int(match[1]) * {None: 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}[match[2]]
    if size <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return size


def sha256_file(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def process_environment(root):
    temporary = root / "runtime-tmp"
    temporary.mkdir(mode=0o700, exist_ok=True)
    return {**os.environ, "TMPDIR": str(temporary)}


class Engine:
    """Own daemon runs and sample peak memory, including across SIGKILL."""
    def __init__(self, args, root, config, files, processes):
        self.args, self.root, self.config = args, root, config
        self.files, self.processes = files, processes
        self.runs = []
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self.sample, daemon=True)
        self.thread.start()

    def sample(self):
        while not self.stopped.wait(0.02):
            for run in self.runs.copy():
                try:
                    text = pathlib.Path(f"/proc/{run['pid']}/status").read_text()
                    match = re.search(r"^VmHWM:\s+(\d+)", text, re.M)
                    if match:
                        run["peak_rss_kib"] = max(run["peak_rss_kib"], int(match[1]))
                except FileNotFoundError:
                    pass

    def start(self, command=None):
        index = len(self.runs)
        log = (self.root / f"engine{index or ''}.log").open("w")
        self.files.append(log)
        command = command or [str(self.args.daemon.resolve()), "serve", "--config", str(self.config)]
        self.process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=process_environment(self.root))
        self.processes.append(self.process)
        self.pid = self.process.pid
        self.runs.append({"pid": self.pid, "peak_rss_kib": 0})

    def kill(self):
        os.kill(self.pid, signal.SIGKILL)
        self.process.wait(timeout=15)

    def finish(self):
        self.stopped.set()
        self.thread.join()
        return {"runs": self.runs,
                "peak_rss_mib": round(max((r["peak_rss_kib"] for r in self.runs), default=0) / 1024, 3)}


def restart_check(args, root, config, socket_path, local_id, payload, digest, touch=False):
    restart_root = root / ("restart-mtime" if touch else "restart-complete")
    restart_root.mkdir()
    if touch:
        stat = payload.stat()
        os.utime(payload, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
    files, processes = [], []
    engine = Engine(args, restart_root, config, files, processes)
    try:
        start = time.monotonic()
        engine.start()
        wait_for_control(engine, socket_path)
        def ready():
            row = torrent_status(socket_path, local_id)
            return row if row["state"] == "seeding" and (row["transfer"] or {}).get("ready") else None
        row = wait_for("restart ready", ready, args.timeout)
        elapsed = time.monotonic() - start
        transfer = row["transfer"]
        assert transfer["downloaded_bytes"] == transfer["uploaded_bytes"] == 0, row
        assert transfer["active_peers"] == 0, row
        assert all(not row["settings"][source] for source in ("trackers", "dht", "pex", "web_seeds"))
        assert row["verified_bytes"] == args.size and sha256_file(payload) == digest
        control(socket_path, "shutdown")
        assert engine.process.wait(timeout=15) == 0
        result = {"seconds": round(elapsed, 3),
                  "timing_status": "untraced",
                  "source_discovery_disabled": True, "active_peers_at_ready": transfer["active_peers"],
                  "checksum_matches": True}
    finally:
        if processes and engine.process.poll() is None:
            try:
                control(socket_path, "shutdown")
                engine.process.wait(timeout=15)
            except (OSError, RuntimeError, subprocess.TimeoutExpired):
                engine.kill()
        metrics = engine.finish()
        (restart_root / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
        for file in files:
            file.close()
    return result


def reference_run(args, root, transmission_bin, settings, torrent, tracker, digest):
    """Same source/fixture/disk; RPC counters sampled at 10 ms resolution."""
    root = root / "reference"
    root.mkdir()
    destination, config = root / "download", root / "config"
    destination.mkdir()
    config.mkdir()
    shutil.copyfile(settings / "settings.json", config / "settings.json")
    peer_port, rpc_port = free_port(args.host), free_port("127.0.0.1")
    tracker.engine_port = peer_port
    files, processes = [], []
    engine = Engine(args, root, config, files, processes)
    try:
        engine.start([transmission_bin, "-f", "-g", str(config), "-w", str(destination),
                      "-p", str(rpc_port), "-P", str(peer_port), "-r", "127.0.0.1",
                      "-i", args.host, "-T", "-O", "-Y", "-M", "-B", "-et",
                      "--preferred-transports", args.transport, "--log-level", "debug"])
        client = Transmission(rpc_port)
        wait_for("reference RPC", lambda: client.rpc("session-get"), 10)
        start = previous = time.monotonic()
        first = None
        client.rpc("torrent-add", filename=str(torrent), **{"download-dir": str(destination)})
        while time.monotonic() - start < args.timeout:
            value = client.torrent()
            now = time.monotonic()
            if value and value["downloadedEver"] and first is None:
                first = previous
            if value and value["percentDone"] == 1:
                break
            previous = now
            time.sleep(0.01)
        else:
            raise TimeoutError("Transmission reference download")
        assert first is not None
        elapsed, payload = now - start, now - first
        assert sha256_file(destination / "payload.bin") == digest
        os.kill(engine.pid, signal.SIGTERM)
        assert engine.process.wait(timeout=60) == 0
        return transfer_metrics(args, elapsed, payload) | {
            "payload_timing": "RPC counter sampling, 10 ms interval", "checksum_matches": True}
    finally:
        if processes and engine.process.poll() is None:
            os.kill(engine.pid, signal.SIGTERM)
            try:
                engine.process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                engine.kill()
        metrics = engine.finish()
        (root / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
        for file in files:
            file.close()


def download(binary, source, destination, root, host, *, run_seconds, stall_seconds=60,
             dht=False, bootstrap=(), trackers=False, web_seeds=False, processes=None,
             total_seconds=None, utp=False, rate_limit=0):
    """Observe a bounded real serve/ctl download and require joined shutdown."""
    root.mkdir(mode=0o700)
    socket = root / "control.sock"
    config = root / "spill.toml"
    write_config(config, state=root / "state", destination=destination, control_socket=socket,
                 host=host, dht=dht, bootstrap=bootstrap, trackers=trackers, web_seeds=web_seeds, utp=utp, rate_limit=rate_limit)
    command = [str(pathlib.Path(binary).resolve()), "serve", "--config", str(config)]
    started = time.monotonic()
    total_seconds = total_seconds if total_seconds is not None else run_seconds + 15
    with (root / "process.log").open("w") as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                   env=process_environment(root))
        if processes is not None:
            processes.append(process)
        try:
            wait_for("product control", lambda: control(socket, "list"), 10)
            ctl = [str(pathlib.Path(binary).resolve()), "ctl", "--json", "--socket", str(socket)]
            last_progress = started
            added = subprocess.run([*ctl, "add", str(source), "--output", str(destination)],
                                   capture_output=True, text=True, check=True,
                                   timeout=max(0.001, run_seconds - (time.monotonic() - started)),
                                   env=process_environment(root))
            torrent_id = json.loads(added.stdout)["id"]
            verified = 0
            seen_transports = set()
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f"product exited early: {process.returncode}")
                status = torrent_status(socket, torrent_id)
                seen_transports.update(peer["transport"] for peer in (status["transfer"] or {}).get("peers", []))
                now = time.monotonic()
                if now - started >= run_seconds:
                    raise TimeoutError("product download reached harness run deadline")
                if status["verified_bytes"] > verified:
                    verified = status["verified_bytes"]
                    last_progress = now
                if status["state"] == "seeding":
                    assert status["transfer"]["completed"]
                    if utp:
                        assert "utp" in seen_transports, "no uTP peer observed; use a larger fixture if it finished before diagnostics were sampled"
                    return status
                if now - last_progress >= stall_seconds:
                    raise TimeoutError("product download reached harness stall deadline")
                time.sleep(0.02)
        finally:
            if process.poll() is None:
                try:
                    control(socket, "shutdown")
                    remaining = total_seconds - (time.monotonic() - started)
                    assert remaining > 0, "product exceeded joined-process deadline"
                    assert process.wait(timeout=min(15, remaining)) == 0
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=5)
            assert process.returncode == 0
            assert time.monotonic() - started <= total_seconds, "product exceeded joined-process deadline"


def transfer_metrics(args, elapsed, payload=None):
    """Report wall-clock timings and optional sampled payload-counter timings."""
    return {"timing_status": "untraced",
            "seconds": round(elapsed, 3),
            "mb_per_second": round(args.size / elapsed / 1e6, 3),
            "payload_seconds": None if payload is None else round(payload, 3),
            "payload_mb_per_second": None if payload is None else round(args.size / payload / 1e6, 3)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-network", action="store_true", required=True)
    parser.add_argument("--host", required=True, help="IPv4 address on your own LAN interface")
    parser.add_argument("--daemon", type=pathlib.Path, default=pathlib.Path("target/debug/spill"))
    parser.add_argument("--artifacts", type=pathlib.Path, help="new directory; defaults beneath target/interop/")
    parser.add_argument("--allow-tmpfs", action="store_true", help="permit tmpfs runs (not disk-performance measurements)")
    parser.add_argument("--keep", action="store_true", help="retain generated payloads after success")
    parser.add_argument("--reference", action="store_true", help="also time Transmission -> Transmission; reports measurements without performance thresholds")
    parser.add_argument("--magnet", action="store_true", help="tracker-only magnet through daemon control; catalog restart/retry check")
    parser.add_argument("--http-tracker", action="store_true", help="use a local HTTP tracker instead of UDP")
    parser.add_argument("--private", action="store_true", help="private metainfo; requires --http-tracker and forbids --magnet")
    parser.add_argument("--restart-check", action="store_true", help="check completed-payload and mtime-touch recovery without source discovery")
    parser.add_argument("--size", type=byte_size, default=2 * 1024**2, help="bytes or KiB/MiB/GiB (default: 2MiB)")
    parser.add_argument("--piece-length", type=byte_size, default=32768)
    parser.add_argument("--release", action="store_true", help="build and use target/release/spill")
    parser.add_argument("--transport", choices=("utp", "tcp"), default="tcp", help="preferred peer transport (default: tcp); uTP runs require observed uTP peers")
    parser.add_argument("--rate-limit", type=byte_size, default=0, help="Spill payload bytes/s in each direction; 0 is unlimited (use 1MiB for small uTP fixtures)")
    parser.add_argument("--download-only", action="store_true", help="only run Transmission -> Spill; skip reverse seeding")
    parser.add_argument("--kill-at", type=float, help="SIGKILL at this verified fraction, then restart")
    parser.add_argument("--timeout", type=float, default=60, help="seconds allowed per transfer (default: 60)")
    args = parser.parse_args()
    if args.reference and args.rate_limit:
        parser.error("--reference requires unlimited --rate-limit 0 for comparable runs")
    if args.private and (not args.http_tracker or args.magnet):
        parser.error("--private requires --http-tracker and cannot use --magnet")
    if args.kill_at is not None and not 0 < args.kill_at < 1:
        parser.error("--kill-at must be between zero and one")
    if args.release:
        subprocess.run(["cargo", "build", "--release", "--locked"], check=True)
        args.daemon = pathlib.Path("target/release/spill")
    transmission_bin = shutil.which("transmission-daemon")
    if not transmission_bin:
        parser.error("install transmission-daemon first; this script never installs packages")
    parent = args.artifacts.resolve().parent if args.artifacts else pathlib.Path("target/interop").resolve()
    parent.mkdir(parents=True, exist_ok=True)
    filesystem = subprocess.check_output(["findmnt", "-n", "-o", "FSTYPE", "--target", str(parent)], text=True).strip()
    if filesystem in ("tmpfs", "ramfs") and not args.allow_tmpfs:
        parser.error("tmpfs artifacts require --allow-tmpfs (not disk-performance measurements)")
    root = args.artifacts.resolve() if args.artifacts else pathlib.Path(tempfile.mkdtemp(prefix="run-", dir=parent))
    if args.artifacts:
        root.mkdir(mode=0o700)
    seed = root / "transmission-seed"
    fresh = root / "transmission-download"
    settings = root / "transmission-config"
    for path in (seed, fresh, settings):
        path.mkdir(mode=0o700)
    # Generate and hash piece by piece: the harness must not buffer a GiB.
    pieces = []
    pattern = bytes(range(256)) * 4096
    digest_state = hashlib.sha256()
    with (seed / "payload.bin").open("wb") as output:
        for offset in range(0, args.size, args.piece_length):
            length = min(args.piece_length, args.size - offset)
            piece = (pattern * ((length + len(pattern) - 1) // len(pattern)))[:length]
            output.write(piece)
            digest_state.update(piece)
            pieces.append(hashlib.sha1(piece).digest())
    digest = digest_state.hexdigest()
    peer_port, rpc_port = free_port(args.host), free_port("127.0.0.1")
    tracker = (HttpTracker if args.http_tracker else Tracker)(args.host, peer_port)
    info = {b"length": args.size, b"name": b"payload.bin", b"piece length": args.piece_length,
            b"pieces": b"".join(pieces)}
    if args.private:
        info[b"private"] = 1
    info_hash = hashlib.sha1(bencode(info)).hexdigest()
    torrent = root / "fixture.torrent"
    torrent.write_bytes(bencode({b"announce": tracker.url.encode(), b"info": info}))
    (settings / "settings.json").write_text(json.dumps({
        "dht-enabled": False, "pex-enabled": False, "lpd-enabled": False,
        "port-forwarding-enabled": False, "utp-enabled": args.transport == "utp",
        "default-trackers": "", "rpc-host-whitelist-enabled": False,
        "ratio-limit-enabled": False, "speed-limit-up-enabled": False,
        "speed-limit-down-enabled": False, "alt-speed-enabled": False,
        "incomplete-dir-enabled": False,
    }))
    socket_path = root / "control.sock"
    config = root / "spill.toml"
    write_config(config, state=root / "state", destination=root / "downloads",
                 control_socket=socket_path, host=args.host, utp=args.transport == "utp", rate_limit=args.rate_limit)
    transmission = Transmission(rpc_port)
    processes = []
    files = []
    result = {"transmission": subprocess.check_output([transmission_bin, "--version"], text=True, stderr=subprocess.STDOUT).strip(),
              "host": args.host, "bytes": args.size, "info_hash": info_hash, "sha256": digest,
              "piece_length": args.piece_length, "artifacts": str(root), "release": args.release,
              "download_only": args.download_only, "transport": args.transport, "rate_limit": args.rate_limit, "filesystem": filesystem,
              "timing_status": "untraced"}
    engine = Engine(args, root, config, files, processes)
    transport_logs = {}
    try:
        for name, command in [
            ("transmission", [transmission_bin, "-f", "-g", str(settings), "-w", str(seed), "-p", str(rpc_port), "-P", str(peer_port), "-r", "127.0.0.1", "-i", args.host, "-T", "-O", "-Y", "-M", "-B", "-et", "--preferred-transports", args.transport, "--log-level", "debug"]),
        ]:
            log = (root / f"{name}.log").open("w")
            files.append(log)
            processes.append(subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=process_environment(root)))
        engine.start()
        wait_for("Transmission RPC", lambda: transmission.rpc("session-get"), 10)
        session = transmission.rpc("session-get")
        for key in ("dht-enabled", "pex-enabled", "lpd-enabled", "port-forwarding-enabled",
                    "speed-limit-up-enabled", "speed-limit-down-enabled", "alt-speed-enabled"):
            assert session[key] is False, (key, session[key])
        health = wait_for_control(engine, socket_path)
        tracker.engine_port = int(health["listen"].rsplit(":", 1)[1])
        transmission.rpc("torrent-add", filename=str(torrent), **{"download-dir": str(seed)})
        wait_for("Transmission seed verification", lambda: (t := transmission.torrent()) and t["percentDone"] == 1 and t["status"] == 6)
        start = time.monotonic()
        magnet = f"magnet:?xt=urn:btih:{info_hash}&tr={urllib.parse.quote(tracker.url, safe='')}"
        add_input = {"magnet": magnet, "metainfo": None, "output_dir": None} if args.magnet else {
            "metainfo": base64.b64encode(torrent.read_bytes()).decode("ascii"), "magnet": None, "output_dir": None}
        if args.magnet:
            reply = subprocess.run([str(args.daemon.resolve()), "ctl", "--json", "--socket", str(socket_path), "add", magnet],
                                   capture_output=True, text=True, check=True, timeout=args.timeout + 15,
                                   env=process_environment(root))
            added = json.loads(reply.stdout)
        else:
            added = control(socket_path, "add", **add_input)
        local_id = added["id"]
        killed = False
        last_download_report = previous_sample = start
        first_payload = None
        download_transports = set()
        def ready():
            nonlocal killed, last_download_report, previous_sample, first_payload
            row = torrent_status(socket_path, local_id)
            transfer = row["transfer"] or {}
            sample = log_transport(root, result, transport_logs, "transmission_to_serve", start, row, download_transports)
            now = time.monotonic()
            if transfer.get("downloaded_bytes", 0) and first_payload is None:
                first_payload = previous_sample
            previous_sample = now
            if now - last_download_report >= 5:
                print(f"Transmission -> Spill: {row['verified_bytes']} / {args.size} bytes verified; "
                      f"{sample['elapsed_seconds']}s; transport={sample['transports']}; "
                      f"Spill retransmits={sample['spill_utp_retransmits']}", flush=True)
                last_download_report = now
            payload = root / "downloads" / "payload.bin"
            part = payload.with_name("payload.bin.part")
            if part.exists():
                # Completion can race the published status, but a single file
                # must never have both partial and final names.
                assert not payload.exists() or not part.exists()
                result["partial_name_seen"] = True
            if args.kill_at is not None and not killed and row["verified_bytes"] >= args.size * args.kill_at:
                assert row["verified_bytes"] < args.size, "download completed before kill threshold was sampled; use a larger fixture"
                killed = True
                result["kill_restart"] = {"verified_bytes_at_kill": row["verified_bytes"],
                                          "seconds_at_kill": round(now - start, 3)}
                engine.kill()
                assert part.exists() and not payload.exists()
                checkpoint = catalog_checkpoint(root, local_id)
                result["kill_restart"].update({"partial_name_after_kill": True,
                                               "resume_present_at_kill": bool(checkpoint["resume"])})
                engine.start()
                health = wait_for_control(engine, socket_path)
                tracker.engine_port = int(health["listen"].rsplit(":", 1)[1])
                return None
            return row if row["state"] == "seeding" and transfer.get("ready") else None
        # Crash injection needs a finer observation window on fast local disks;
        # keep timing/reference runs and the 45–65% acceptance target unchanged.
        downloaded = wait_for("Transmission -> serve durable download", ready, args.timeout,
                              poll_interval=0.01)
        elapsed = time.monotonic() - start
        if args.transport == "utp":
            assert "utp" in download_transports, "no uTP peer observed; use a larger fixture if it finished before diagnostics were sampled"
        result["download_transports"] = sorted(download_transports)
        if args.kill_at is not None:
            assert killed, "download completed before kill threshold was sampled"
        payload, = (root / "downloads").glob("payload.bin")
        assert sha256_file(payload) == digest
        assert not payload.with_name("payload.bin.part").exists()
        result["payload_layout"] = "partial-then-final"
        result["final_name_only"] = True
        assert downloaded["verified_bytes"] == args.size, downloaded
        payload_seconds = start + elapsed - first_payload if first_payload is not None and not killed else None
        result["transmission_to_serve"] = transfer_metrics(args, elapsed, payload_seconds) | {
            "payload_timing": "status counter sampling, 10 ms interval; not an exact first-block timestamp",
            "checksum_matches": True, "runtime": downloaded}
        seeded = transmission.torrent()
        result["transmission_to_serve"]["torrent"] = seeded
        if killed:
            # All payload the seed sent beyond one fixture is a conservative
            # bound: it also includes duplicates and data left in TCP at SIGKILL.
            time.sleep(1)
            extra = max(0, transmission.torrent()["uploadedEver"] - args.size)
            # Conservative queue + piece/peer bound using the engine Profile's
            # peer cap, not an invented observed connection counter.
            bound = 16 * 1024**2 + args.piece_length * 32
            assert extra <= bound, (extra, bound)
            result["kill_restart"].update({"extra_source_payload_bytes": extra, "redownload_bound_bytes": bound, "checksum_matches": True})
        if args.magnet:
            with sqlite3.connect(root / "state" / "catalog.sqlite") as db:
                stored, = db.execute("SELECT metainfo FROM torrents WHERE id=?", (local_id,)).fetchone()
            assert bencode(info) in stored, "catalog must retain the exact verified info bytes"
            resolutions = sum(exchange["left"] == 1 for exchange in tracker.exchanges)
            assert resolutions > 0
            control(socket_path, "shutdown")
            assert engine.process.wait(timeout=15) == 0
            engine.start()
            health = wait_for_control(engine, socket_path)
            tracker.engine_port = int(health["listen"].rsplit(":", 1)[1])
            wait_for("magnet catalog restart", lambda: torrent_status(socket_path, local_id)["state"] == "seeding", args.timeout)
            try:
                control(socket_path, "add", **add_input)
            except ControlError as error:
                assert error.code == 2 and "already exists" in str(error), error
            else:
                raise AssertionError("duplicate add must be rejected")
            assert sum(exchange["left"] == 1 for exchange in tracker.exchanges) == resolutions
            result["magnet_catalog_restart"] = {"metainfo_retained": True, "refetched": False, "duplicate_rejected": True}
            destination = root / "magnet-download"
            download(args.daemon, magnet, destination, root / "magnet-product", args.host,
                     run_seconds=int(args.timeout), trackers=True, utp=args.transport == "utp", rate_limit=args.rate_limit)
            cli_payload, = destination.glob("payload.bin")
            assert sha256_file(cli_payload) == digest
            result["magnet_cli"] = {"checksum_matches": True}
        if not args.download_only:
            old_id = seeded["id"]
            transmission.rpc("torrent-remove", ids=[old_id], **{"delete-local-data": False})
            transmission.rpc("torrent-add", filename=str(torrent), **{"download-dir": str(fresh)})
            start = time.monotonic()
            last_bytes, last_progress, last_report = 0, start, start
            upload_transports = set()
            def upload_ready():
                nonlocal last_bytes, last_progress, last_report
                value = transmission.torrent()
                row = torrent_status(socket_path, local_id)
                sample = log_transport(root, result, transport_logs, "serve_to_transmission", start, row, upload_transports)
                if not value:
                    return None
                now = time.monotonic()
                if value["downloadedEver"] > last_bytes:
                    last_bytes, last_progress = value["downloadedEver"], now
                if now - last_report >= 5:
                    print(f"serve -> Transmission: {value['percentDone']:.1%}, {last_bytes} bytes received; "
                          f"{sample['elapsed_seconds']}s; transport={sample['transports']}; "
                          f"Spill retransmits={sample['spill_utp_retransmits']}", flush=True)
                    last_report = now
                if now - last_progress > 20:
                    raise RuntimeError(f"serve -> Transmission: no progress for 20s; {value}")
                return value if value["percentDone"] == 1 else None
            complete = wait_for("serve -> Transmission download", upload_ready, args.timeout)
            if args.transport == "utp":
                assert "utp" in upload_transports, "no uTP peer observed on reverse transfer; use a larger fixture"
            result["upload_transports"] = sorted(upload_transports)
            elapsed = time.monotonic() - start
            assert sha256_file(fresh / "payload.bin") == digest
            assert complete["hashString"] == info_hash
            result["serve_to_transmission"] = transfer_metrics(args, elapsed) | {
                "checksum_matches": True, "torrent": complete,
                "runtime": torrent_status(socket_path, local_id)}
        result["peer_clients"] = sorted(transmission.clients)
        if not args.download_only:
            assert any(name.startswith("Spill/") for name in transmission.clients), result["peer_clients"]
        if args.restart_check:
            # Recovery checks must not fetch missing data from the source.
            control(socket_path, "set", id=local_id, values={source: False for source in ("trackers", "dht", "pex", "web_seeds")})
        control(socket_path, "shutdown")
        assert engine.process.wait(timeout=15) == 0
        if args.reference:
            result["transmission_to_transmission"] = reference_run(
                args, root, transmission_bin, settings, torrent, tracker, digest)
            result["transmission_to_transmission"]["engine"] = json.loads((root / "reference" / "metrics.json").read_text())
        # SIGTERM exits the foreground daemon as well as closing its RPC session.
        processes[0].terminate()
        assert processes[0].wait(timeout=60) == 0
        if args.restart_check:
            result["restart_complete"] = restart_check(args, root, config, socket_path, local_id, payload, digest)
            result["restart_mtime"] = restart_check(args, root, config, socket_path, local_id, payload, digest, touch=True)
        # Exit 0 follows the daemon's joined owner/session shutdown. Internal
        # tracing events are no longer part of the product's control contract.
        result["joined_shutdown"] = True
        result["tracker_announces"] = tracker.exchanges
        result["passed"] = True
    except BaseException as error:
        result["passed"] = False
        result["error"] = repr(error)
        raise
    finally:
        if engine.runs and engine.process.poll() is None:
            try:
                control(socket_path, "shutdown")
                engine.process.wait(timeout=10)
            except (OSError, RuntimeError, subprocess.TimeoutExpired):
                engine.kill()
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        result["engine"] = engine.finish()
        result["tracker_announces"] = tracker.exchanges
        (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print("| Metric | Value |\n|---|---|")
        for key in ("bytes", "piece_length", "timing_status", "peer_clients", "passed", "error"):
            if key in result:
                print(f"| {key} | {result[key]} |")
        for key in ("restart_complete", "restart_mtime", "kill_restart", "payload_layout", "partial_name_seen", "final_name_only", "magnet_catalog_restart", "magnet_cli"):
            if key in result:
                print(f"| {key} | {result[key]} |")
        for direction in ("transmission_to_serve", "serve_to_transmission", "transmission_to_transmission"):
            if direction in result:
                metrics = result[direction]
                print(f"| {direction} | {metrics['seconds']} s; {metrics['mb_per_second']} MB/s; SHA-256 matches |")
                if metrics.get("payload_seconds") is not None:
                    print(f"| Download: observed payload counter through durable seeding | {metrics['payload_seconds']} s; {metrics['payload_mb_per_second']} MB/s (sampled) |")
        print(f"| Peak spill RSS | {result['engine']['peak_rss_mib']} MiB |")
        tracker.close()
        for file in files:
            file.close()
        if result.get("passed") and not args.keep:
            for directory in (seed, fresh, root / "downloads", root / "reference" / "download", root / "magnet-download"):
                if directory.exists():
                    shutil.rmtree(directory)
            torrent.unlink(missing_ok=True)
            for temporary in list(root.rglob("runtime-tmp")):
                shutil.rmtree(temporary)
        print(f"Artifacts: {root}")


if __name__ == "__main__":
    sys.exit(main())
