#!/usr/bin/env python3
"""Dev-machine tier 2 discovery and daemon lifecycle checks against libtorrent.

Only this machine's LAN address is used. No public DHT bootstrap, tracker, LPD,
uTP, encryption or port mapping. Requires libtorrent 2.1+; installs nothing.
"""
import argparse
import base64
import hashlib
import http.server
import json
import math
import os
import select
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse

import libtorrent_v2 as wire
from transmission_smoke import control, ControlError, write_config, read_exact


class Daemon:
    def __init__(self, binary, root, host, **settings):
        root.mkdir()
        self.binary, self.root = binary, root
        self.output, self.socket = root / "payload", root / "c.sock"
        self.config = root / "config.toml"
        write_config(self.config, state=root / "state", destination=self.output,
                     control_socket=self.socket, host=host, **settings)
        self.process = None
        self.runs = 0
        self.samples = (root / "observations.jsonl").open("w")
        self.start()

    def start(self):
        with (self.root / f"daemon-{self.runs}.log").open("w") as log:
            self.process = subprocess.Popen([str(self.binary), "serve", "--config", str(self.config)],
                                            stdout=log, stderr=subprocess.STDOUT)
        self.runs += 1
        def ready():
            assert self.process.poll() is None, self.root
            try:
                return control(self.socket, "health")
            except (OSError, ValueError):
                return None
        wire.wait_for("daemon control", ready, 10)

    def row(self, torrent_id):
        assert self.process.poll() is None, self.root
        row = control(self.socket, "get", id=torrent_id)
        self.samples.write(json.dumps({"time": time.time(), "row": row}) + "\n")
        self.samples.flush()
        assert not row["last_error"], row
        return row

    def wait(self, torrent_id, predicate, timeout):
        def poll():
            row = self.row(torrent_id)
            return row if predicate(row) else None
        return wire.wait_for(f"daemon progress: {self.root}", poll, timeout)

    def add(self, torrent):
        return control(self.socket, "add", metainfo=base64.b64encode(torrent.read_bytes()).decode())["id"]

    def address(self):
        host, port = control(self.socket, "health")["listen"].rsplit(":", 1)
        return host, int(port)

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            try:
                control(self.socket, "shutdown")
                assert self.process.wait(timeout=15) == 0, self.root
            finally:
                if self.process.poll() is None:
                    self.process.kill()
                    self.process.wait(timeout=10)

    def kill(self):
        self.process.kill()
        self.process.wait(timeout=10)

    def close(self):
        self.stop()
        self.samples.close()


class Tracker:
    def __init__(self, lt, kind, host, routes, *, interval=30):
        self.lt, self.kind, self.host, self.routes = lt, kind, host, routes
        self.interval = interval
        self.records, self.errors = [], []
        self.done = threading.Event()
        if kind == "http":
            tracker = self
            class Handler(http.server.BaseHTTPRequestHandler):
                def do_GET(self):
                    fields = dict(p.split("=", 1) for p in urllib.parse.urlsplit(self.path).query.split("&"))
                    key = urllib.parse.unquote_to_bytes(fields["info_hash"])
                    event = fields.get("event", "none")
                    peers = tracker.reply(key, event, int(fields["downloaded"]), int(fields["uploaded"]), int(fields["left"]),
                                          urllib.parse.unquote_to_bytes(fields["peer_id"]).startswith(b"-sP"))
                    body = lt.bencode({b"interval": tracker.interval, b"peers": peers})
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

                def log_message(self, *_):
                    pass
            self.server = http.server.ThreadingHTTPServer((host, 0), Handler)
            self.server.daemon_threads = True
            self.port = self.server.server_port
            self.thread = threading.Thread(target=self.server.serve_forever)
        else:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.server.bind((host, 0))
            self.server.settimeout(0.2)
            self.port = self.server.getsockname()[1]
            self.thread = threading.Thread(target=self.udp)
        self.thread.start()

    @property
    def url(self):
        return f"{self.kind}://{self.host}:{self.port}/announce"

    def reply(self, key, event, downloaded, uploaded, left, spill):
        assert key in self.routes, key.hex()
        peer = self.routes[key]
        self.records.append(dict(time=time.time(), swarm=key.hex(), event=event,
                                 downloaded=downloaded, uploaded=uploaded, left=left, peer=peer, spill=spill))
        return socket.inet_aton(peer[0]) + struct.pack("!H", peer[1]) if peer else b""

    def udp(self):
        try:
            while not self.done.is_set():
                try:
                    packet, address = self.server.recvfrom(4096)
                except socket.timeout:
                    continue
                action, tx = struct.unpack_from("!II", packet, 8)
                if action == 0:
                    reply = struct.pack("!IIQ", 0, tx, 17)
                else:
                    assert action == 1 and len(packet) >= 98, packet.hex()
                    downloaded, left, uploaded, event = struct.unpack_from("!QQQI", packet, 56)
                    peers = self.reply(packet[16:36], {0: "none", 1: "completed", 2: "started", 3: "stopped", 4: "paused"}[event],
                                       downloaded, uploaded, left, packet[36:39] == b"-sP")
                    reply = struct.pack("!IIIII", 1, tx, self.interval, 1, 1) + peers
                self.server.sendto(reply, address)
        except Exception as error:
            self.errors.append(repr(error))

    def close(self):
        self.done.set()
        if self.kind == "http":
            self.server.shutdown()
            self.thread.join(timeout=5)
            self.server.server_close()
        else:
            self.thread.join(timeout=5)
            self.server.close()
        assert not self.thread.is_alive() and not self.errors, self.errors


class DhtRelay:
    """Keep libtorrent as the DHT server/oracle, retaining daemon KRPC evidence."""
    def __init__(self, lt, host, target, node_id):
        self.lt, self.target, self.node_id = lt, target, node_id
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((host, 0))
        self.sock.settimeout(0.2)
        self.address = self.sock.getsockname()
        self.router = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.router.bind((host, 0))
        self.bootstrap = self.router.getsockname()
        self.done = threading.Event()
        self.records, self.errors = [], []
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def run(self):
        client = None
        try:
            while not self.done.is_set():
                readable, _, _ = select.select([self.sock, self.router], [], [], 0.2)
                if not readable:
                    continue
                incoming = readable[0]
                packet, source = incoming.recvfrom(65535)
                message = self.lt.bdecode(packet)
                if incoming is self.router:
                    assert message[b"q"] == b"find_node", message
                    contact = self.node_id + socket.inet_aton(self.address[0]) + struct.pack("!H", self.address[1])
                    reply = {b"t": message[b"t"], b"y": b"r", b"r": {b"id": b"\x08" * 20, b"nodes": contact}}
                    self.router.sendto(self.lt.bencode(reply), source)
                    continue
                if source == self.target:
                    assert client is not None
                    destination = client
                else:
                    client = source
                    destination = self.target
                    if message[b"y"] == b"q":
                        args = message[b"a"]
                        self.records.append({"query": message[b"q"].decode(),
                                             "swarm": args.get(b"info_hash", b"").hex(),
                                             "port": args.get(b"port")})
                self.sock.sendto(packet, destination)
        except Exception as error:
            self.errors.append(repr(error))

    def close(self):
        self.done.set()
        self.thread.join(timeout=5)
        self.sock.close()
        self.router.close()
        assert not self.thread.is_alive() and not self.errors, self.errors


def register_dht(lt, host, node, key, peer):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind((host, 0))
        sock.settimeout(10)
        def request(method, args, tx):
            args[b"id"] = b"\x07" * 20
            sock.sendto(lt.bencode({b"t": tx, b"y": b"q", b"q": method, b"a": args, b"ro": 1}), node)
            message = lt.bdecode(sock.recv(65535))
            assert message[b"t"] == tx and message[b"y"] == b"r", message
            return message[b"r"]
        token = request(b"get_peers", {b"info_hash": key}, b"register-get")[b"token"]
        request(b"announce_peer", {b"info_hash": key, b"token": token, b"port": peer[1]}, b"register-put")
        found = request(b"get_peers", {b"info_hash": key}, b"register-check")
        compact = socket.inet_aton(peer[0]) + struct.pack("!H", peer[1])
        assert compact in found[b"values"], found
        return found[b"id"]


def keys(lt, torrent):
    hashes = lt.load_torrent_file(str(torrent)).ti.info_hashes()
    return ([hashes.v1.to_bytes()] if hashes.has_v1() else []) + [hashes.v2.to_bytes()[:20]]


def install(lt, torrent, output):
    expected = {}
    for entry in wire.lt_files(lt, torrent):
        expected[entry["length"]] = wire.write_payload(output / entry["path"], entry["length"])
    return expected


def tracked(lt, torrent, url, path):
    document = lt.bdecode(torrent.read_bytes())
    document.pop(b"announce-list", None)
    document[b"announce"] = url.encode()
    path.write_bytes(lt.bencode(document))
    assert lt.bencode(document[b"info"]) == lt.bencode(lt.bdecode(torrent.read_bytes())[b"info"])
    return path


def check_identity(lt, torrent, row, daemon):
    hashes = lt.load_torrent_file(str(torrent)).ti.info_hashes()
    v1 = hashes.v1.to_bytes().hex() if hashes.has_v1() else None
    v2 = hashes.v2.to_bytes().hex()
    assert row["id"] == (v1 or v2) and row["v1"] == v1 and row["v2"] == v2, row
    assert row["format"] == ("hybrid" if v1 else "v2") and row["metadata_ready"], row
    for alias in (v1, v2):
        if alias:
            assert daemon.row(alias)["id"] == row["id"]
    try:
        daemon.add(torrent)
    except ControlError as error:
        assert error.code == 2 and "already exists" in str(error), error
    else:
        raise AssertionError("duplicate accepted")
    return v2


def hellos(relay):
    return [{"from": h["from"], "key": bytes.fromhex(h["handshake"])[28:48].hex(),
             "upgrade": bool(bytes.fromhex(h["handshake"])[27] & 0x10)} for h in relay.handshakes]


def discovery_case(lt, args, binary, root, torrent, method, mode):
    root.mkdir()
    swarm_keys = keys(lt, torrent)
    key = swarm_keys[0] if mode == "v1" else swarm_keys[-1]
    expected = install(lt, torrent, root / "seed")
    source = wire.Libtorrent(lt, root / "libtorrent", torrent, root / "seed", args.host, dht=method == "dht")
    daemon = tracker = dht = relay = reverse = leecher = None
    result = {"method": method, "mode": mode, "torrent": torrent.name, "swarm": key.hex()}
    try:
        source.complete(args.timeout, seed=True)
        relay = wire.Relay(args.host, source.address(args.host), root / "download-wire.jsonl", "spill", force_v1=mode == "v1")
        if method == "dht":
            node_id = register_dht(lt, args.host, source.address(args.host), key, relay.address)
            dht = DhtRelay(lt, args.host, source.address(args.host), node_id)
            options = dict(trackers=False, dht=True, bootstrap=[f"{dht.bootstrap[0]}:{dht.bootstrap[1]}"])
            added = torrent
        else:
            tracker = Tracker(lt, method, args.host, {k: relay.address if k == key else None for k in swarm_keys})
            added = tracked(lt, torrent, tracker.url, root / "source.torrent")
            options = dict(trackers=True)
        daemon = Daemon(binary, root / "daemon", args.host, **options)
        torrent_id = daemon.add(added)
        row = daemon.wait(torrent_id, lambda r: r["state"] == "seeding", args.timeout)
        alias = check_identity(lt, torrent, row, daemon)
        result["download"] = row
        result["files"] = wire.verify_files(daemon.output, wire.spill_files(lt.bdecode(torrent.read_bytes())), expected)
        assert any(h["from"] == "spill" and h["key"] == key.hex() for h in hellos(relay)), hellos(relay)
        if method == "dht":
            daemon.wait(alias, lambda r: len(r["transfer"]["dht"]) == len(swarm_keys)
                        and all(s["announced"] > 0 for s in r["transfer"]["dht"]), args.timeout)
            for k in swarm_keys:
                assert any(r["query"] == "announce_peer" and r["swarm"] == k.hex() for r in dht.records), dht.records
        else:
            daemon.wait(alias, lambda r: len(r["completed_announces"]) == len(swarm_keys)
                        and all(s["delivered"] for s in r["completed_announces"]), args.timeout)
        control(daemon.socket, "pause", id=alias)
        daemon.stop()
        daemon.start()
        assert daemon.row(alias)["state"] == "paused"
        control(daemon.socket, "resume", id=alias)
        resumed = daemon.wait(alias, lambda r: r["state"] == "seeding", args.timeout)
        assert resumed["transfer"]["downloaded_bytes"] == 0, resumed
        control(daemon.socket, "recheck", id=alias)
        daemon.wait(alias, lambda r: r["state"] == "paused" and r["completed"], args.timeout)
        control(daemon.socket, "resume", id=alias)
        daemon.wait(alias, lambda r: r["state"] == "seeding", args.timeout)
        if method != "dht":
            control(daemon.socket, "set", id=alias, values={"trackers": False})
            daemon.wait(alias, lambda r: r["state"] == "seeding", args.timeout)
            source.stop()
            reverse = wire.Relay(args.host, daemon.address(), root / "upload-wire.jsonl", "libtorrent", force_v1=mode == "v1")
            tracker.routes = {k: reverse.address if k == key else None for k in swarm_keys}
            leecher = wire.Libtorrent(lt, root / "libtorrent-download", added, root / "fresh", args.host)
            leecher.handle.replace_trackers([{"url": tracker.url}])
            leecher.handle.force_reannounce()
            result["upload"] = leecher.complete(args.timeout)
            result["reverse_files"] = wire.verify_files(root / "fresh", wire.lt_files(lt, torrent), expected)
            result["reverse_handshakes"] = hellos(reverse)
            assert any(h["key"] == (swarm_keys[0] if mode == "v1" else swarm_keys[-1]).hex()
                       for h in result["reverse_handshakes"]), result["reverse_handshakes"]
            leecher.stop()
        else:
            control(daemon.socket, "set", id=alias, values={"dht": False})
            daemon.wait(alias, lambda r: r["state"] == "seeding", args.timeout)
            source.handle.pause()
            before = source.poll().total_payload_upload
            leecher = wire.Libtorrent(lt, root / "libtorrent-download", torrent, root / "fresh", args.host, dht=True)
            leecher.session.add_dht_node(source.address(args.host))
            leecher.handle.unset_flags(lt.torrent_flags.disable_dht)
            leecher.handle.force_dht_announce()
            result["upload"] = leecher.complete(args.timeout)
            result["daemon_upload"] = daemon.wait(alias, lambda r: r["transfer"]["uploaded_bytes"] > 0, args.timeout)
            assert source.poll().total_payload_upload == before
            result["reverse_files"] = wire.verify_files(root / "fresh", wire.lt_files(lt, torrent), expected)
            leecher.stop()
        control(daemon.socket, "remove", id=alias, delete_payload=True)
        assert control(daemon.socket, "list")["torrents"] == []
        if tracker:
            for k in swarm_keys:
                events = [r["event"] for r in tracker.records if r["spill"] and r["swarm"] == k.hex()]
                assert events.count("completed") == 1 and "started" in events and "stopped" in events, events
        result["status"] = "passed"
        return result
    finally:
        if leecher: leecher.stop()
        if daemon: daemon.close()
        source.stop()
        if relay:
            result["handshakes"] = hellos(relay)
            result["wire"] = relay.stop()
        if reverse: result["reverse_wire"] = reverse.stop()
        if tracker:
            tracker.close()
            result["announces"] = tracker.records
        if dht:
            dht.close()
            result["krpc"] = dht.records
        (root / "case.json").write_text(json.dumps(result, indent=2))


class LegacySeed:
    def __init__(self, lt, host, torrent):
        info = lt.bdecode(torrent.read_bytes())[b"info"]
        self.key = hashlib.sha1(lt.bencode(info)).digest()
        files = info[b"files"]
        self.payload = b"".join(bytes(f[b"length"]) if b"p" in f.get(b"attr", b"")
                                else bytes(i % 251 for i in range(f[b"length"])) for f in files)
        self.piece_length = info[b"piece length"]
        self.listener = socket.create_server((host, 0))
        self.listener.settimeout(0.2)
        self.address = self.listener.getsockname()
        self.done = threading.Event()
        self.handshakes, self.errors, self.workers = [], [], []
        self.uploaded = 0
        self.thread = threading.Thread(target=self.accept)
        self.thread.start()

    def accept(self):
        while not self.done.is_set():
            try:
                peer, _ = self.listener.accept()
            except socket.timeout:
                continue
            worker = threading.Thread(target=self.serve, args=(peer,))
            self.workers.append(worker)
            worker.start()

    def serve(self, peer):
        try:
            peer.settimeout(1)
            hello = bytearray(read_exact(peer, 68))
            self.handshakes.append({"key": hello[28:48].hex(), "upgrade": bool(hello[27] & 0x10),
                                    "spill": hello[48:51] == b"-sP"})
            assert hello[28:48] == self.key, self.handshakes
            hello[20:28] = bytes(8)
            hello[48:68] = b"legacy-seed".ljust(20, b"-")
            peer.sendall(hello)
            pieces = math.ceil(len(self.payload) / self.piece_length)
            bits = bytearray(math.ceil(pieces / 8))
            for i in range(pieces): bits[i // 8] |= 0x80 >> (i % 8)
            wire.send(peer, 5, bytes(bits))
            wire.send(peer, 1)
            while not self.done.is_set():
                try:
                    body = wire.receive(peer)
                except socket.timeout:
                    continue
                if body and body[0] == 6:
                    index, begin, length = struct.unpack("!III", body[1:])
                    offset = index * self.piece_length + begin
                    wire.send(peer, 7, body[1:9] + self.payload[offset:offset + length])
                    self.uploaded += length
        except (ConnectionError, BrokenPipeError, ConnectionResetError):
            pass
        except Exception as error:
            self.errors.append(repr(error))
        finally:
            peer.close()

    def close(self):
        self.done.set()
        self.thread.join(timeout=5)
        self.listener.close()
        for worker in self.workers:
            worker.join(timeout=5)
            assert not worker.is_alive()
        assert not self.thread.is_alive() and not self.errors, self.errors


def pex_case(lt, args, binary, root):
    root.mkdir()
    torrent = wire.FIXTURES / "hybrid.torrent"
    expected = install(lt, torrent, root / "seed")
    source = wire.Libtorrent(lt, root / "libtorrent", torrent, root / "seed", args.host, pex=True)
    legacy = LegacySeed(lt, args.host, torrent)
    daemon = tracker = relay = None
    result = {"type": "libtorrent-pex-v1-only-contact"}
    try:
        source.complete(args.timeout, seed=True)
        source.handle.connect_peer(legacy.address)
        wire.wait_for("libtorrent has a legacy peer", lambda: (source.poll(), legacy.handshakes)[1], args.timeout)
        relay = wire.Relay(args.host, source.address(args.host), root / "pex-wire.jsonl", "spill",
                           withhold_pieces=True, choke_source=True)
        swarm_keys = keys(lt, torrent)
        tracker = Tracker(lt, "http", args.host, {k: relay.address if k == swarm_keys[0] else None for k in swarm_keys})
        added = tracked(lt, torrent, tracker.url, root / "source.torrent")
        daemon = Daemon(binary, root / "daemon", args.host, trackers=True, pex=True)
        torrent_id = daemon.add(added)
        result["download"] = daemon.wait(torrent_id, lambda r: r["state"] == "seeding", args.timeout)
        result["files"] = wire.verify_files(daemon.output, wire.spill_files(lt.bdecode(torrent.read_bytes())), expected)
        assert any(h["from"] == "libtorrent" and h["key"] == swarm_keys[-1].hex() for h in hellos(relay)), hellos(relay)
        assert any(h["spill"] and h["upgrade"] and h["key"] == swarm_keys[0].hex() for h in legacy.handshakes), legacy.handshakes
        assert legacy.uploaded > 0
        compact = socket.inet_aton(legacy.address[0]) + struct.pack("!H", legacy.address[1])
        def pex_directions():
            directions = set()
            for line in (root / "pex-wire.jsonl").read_text().splitlines():
                event = json.loads(line)
                if event.get("id") != 20 or not event.get("extension"):
                    continue
                message = lt.bdecode(bytes.fromhex(event["extension_payload"]))
                if compact in [message.get(b"added", b"")[i:i+6] for i in range(0, len(message.get(b"added", b"")), 6)]:
                    directions.add(event["from"])
            return sorted(directions)
        wire.wait_for("actual PEX contact in both directions across wire modes",
                      lambda: pex_directions() == ["libtorrent", "spill"], args.timeout)
        result["pex_directions"] = pex_directions()
        result["legacy_handshakes"] = legacy.handshakes
        result["legacy_uploaded"] = legacy.uploaded
        result["status"] = "passed"
        return result
    finally:
        if daemon: daemon.close()
        source.stop()
        result["legacy_handshakes"] = legacy.handshakes
        result["legacy_uploaded"] = legacy.uploaded
        legacy.close()
        if relay:
            result["handshakes"] = hellos(relay)
            result["wire"] = relay.stop()
        if tracker:
            tracker.close()
            result["announces"] = tracker.records
        (root / "case.json").write_text(json.dumps(result, indent=2))


def restart_case(lt, args, binary, root, hybrid):
    root.mkdir()
    source_root = root / "seed"
    wire.write_payload(source_root / "restart.bin", 8 * 1024 * 1024 + 123)
    flags = 0 if hybrid else lt.create_torrent_flags_t.v2_only
    creator = lt.create_torrent([lt.create_file_entry("restart.bin", 8 * 1024 * 1024 + 123)], 262144, flags)
    lt.set_piece_hashes(creator, str(source_root))
    torrent = root / "restart.torrent"
    torrent.write_bytes(lt.bencode(creator.generate()))
    source = wire.Libtorrent(lt, root / "libtorrent", torrent, source_root, args.host)
    daemon = tracker = relay = None
    result = {"type": "daemon-kill-restart", "format": "hybrid" if hybrid else "v2"}
    try:
        source.complete(args.timeout, seed=True)
        relay = wire.Relay(args.host, source.address(args.host), root / "wire.jsonl", "spill")
        swarm_keys = keys(lt, torrent)
        tracker = Tracker(lt, "http", args.host, {k: relay.address for k in swarm_keys})
        added = tracked(lt, torrent, tracker.url, root / "source.torrent")
        daemon = Daemon(binary, root / "daemon", args.host, trackers=True, rate_limit=1024 * 1024)
        torrent_id = daemon.add(added)
        result["before_kill"] = daemon.wait(torrent_id, lambda r: 0 < r["verified_bytes"] < r["total_bytes"], args.timeout)
        daemon.kill()
        daemon.start()
        result["recovered"] = daemon.wait(torrent_id, lambda r: r["state"] == "seeding", args.timeout)
        alias = check_identity(lt, torrent, result["recovered"], daemon)
        entries = wire.spill_files(lt.bdecode(torrent.read_bytes()))
        expected = {entries[0]["length"]: wire.digest(source_root / "restart.bin")}
        result["files"] = wire.verify_files(daemon.output, entries, expected)
        height = ((entries[0]["length"] + wire.BLOCK - 1) // wire.BLOCK - 1).bit_length()
        request = (4, 0, 2, height - 4 - 1)
        oracle, oracle_trace = wire.query_hash(source.address(args.host), swarm_keys[-1], entries[0], 262144, request, args.timeout)
        source.stop()
        control(daemon.socket, "set", id=alias, values={"trackers": False, "dht": False, "pex": False})
        daemon.wait(alias, lambda r: r["state"] == "seeding", args.timeout)
        daemon.stop()
        daemon.start()
        result["seed_restart"] = daemon.wait(alias, lambda r: r["state"] == "seeding", args.timeout)
        assert result["seed_restart"]["transfer"]["downloaded_bytes"] == 0
        hashes, trace = wire.query_hash(daemon.address(), swarm_keys[-1], entries[0], 262144, request, args.timeout)
        assert hashes == oracle, (hashes.hex(), oracle.hex())
        result["proof"] = {"request": request, "hashes": hashes.hex(), "libtorrent": oracle_trace, "spill": trace}
        control(daemon.socket, "recheck", id=alias)
        daemon.wait(alias, lambda r: r["state"] == "paused" and r["completed"], args.timeout)
        control(daemon.socket, "remove", id=alias, delete_payload=True)
        result["status"] = "passed"
        return result
    finally:
        if daemon: daemon.close()
        source.stop()
        if relay: result["wire"] = relay.stop()
        if tracker:
            tracker.close()
            result["announces"] = tracker.records
        (root / "case.json").write_text(json.dumps(result, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-network", action="store_true", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--scenario", choices=("trackers", "dht", "pex", "restart", "all"), default="all")
    parser.add_argument("--timeout", type=float, default=90)
    parser.add_argument("--artifacts", type=Path)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if os.environ.get("CI"):
        parser.error("tier 2 runs are dev-machine-only, never CI")
    socket.inet_pton(socket.AF_INET, args.host)
    with socket.socket() as check: check.bind((args.host, 0))
    import libtorrent as lt
    subprocess.run(["cargo", "build", "--locked"] + (["--release"] if args.release else []), check=True)
    binary = (Path("target") / ("release" if args.release else "debug") / "spill").resolve()
    parent = Path("target/interop").resolve()
    parent.mkdir(parents=True, exist_ok=True)
    root = args.artifacts.resolve() if args.artifacts else Path(tempfile.mkdtemp(prefix="lt-discovery-", dir=parent))
    if args.artifacts: root.mkdir(parents=True)
    result = {"revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"])),
              "host": args.host, "libtorrent": lt.__version__, "python": sys.version,
              "binary_sha256": wire.digest(binary), "harness_sha256": wire.digest(Path(__file__)),
              "shared_harness_sha256": wire.digest(Path(wire.__file__)),
              "control_helper_sha256": wire.digest(Path(__file__).with_name("transmission_smoke.py")),
              "modified_paths": subprocess.check_output(["git", "status", "--porcelain"], text=True).splitlines(),
              "scenario": args.scenario, "checks": []}
    print(f"libtorrent {lt.__version__}; artifacts: {root}", flush=True)
    try:
        methods = ["http", "udp", "dht"] if args.scenario == "all" else ([args.scenario] if args.scenario == "dht" else ["http", "udp"] if args.scenario == "trackers" else [])
        for method in methods:
            fixtures = [("v2.torrent", ["v2"])] + [(name, ["v1", "v2"]) for name in wire.HYBRID_FIXTURES]
            for fixture, modes in fixtures:
                for mode in modes:
                    print(f"{method}: {fixture} via {mode}", flush=True)
                    result["checks"].append(discovery_case(lt, args, binary, root / f"{method}-{fixture.removesuffix('.torrent')}-{mode}", wire.FIXTURES / fixture, method, mode))
        if args.scenario in ("pex", "all"):
            print("libtorrent PEX advertises a v1-only peer across an upgraded connection", flush=True)
            result["checks"].append(pex_case(lt, args, binary, root / "pex"))
        if args.scenario in ("restart", "all"):
            for hybrid in (False, True):
                print(f"daemon SIGKILL/restart and proof serving: hybrid={hybrid}", flush=True)
                result["checks"].append(restart_case(lt, args, binary, root / ("restart-hybrid" if hybrid else "restart-v2"), hybrid))
        result["status"] = "passed"
    except Exception as error:
        result["status"], result["error"] = "failed", repr(error)
        raise
    finally:
        (root / "result.json").write_text(json.dumps(result, indent=2))
        print(f"{result.get('status')}: {root / 'result.json'}", flush=True)


if __name__ == "__main__":
    main()
