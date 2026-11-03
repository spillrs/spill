#!/usr/bin/env python3
"""Opt-in v2 direct-peer interop through spill's public engine and libtorrent.

Requires system libtorrent-rasterbar Python bindings (2.1+); never installs packages.
No trackers, DHT, PEX, LPD, port mapping, uTP or encryption are enabled.
"""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import selectors
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time

BLOCK = 16384
FIXTURES = Path(__file__).resolve().parents[1] / "protocol" / "fixtures"
FIXTURE_NAMES = ["v2.torrent", "v2-leaves.torrent", "v2-large.torrent", "v2-copies.torrent",
                 "v2-nested.torrent", "v2-single.torrent", "v2-empty.torrent"]
HYBRID_FIXTURES = ["hybrid.torrent", "libtorrent-hybrid.torrent"]


def write_payload(path, length):
    path.parent.mkdir(parents=True, exist_ok=True)
    pattern = bytes(range(251)) * 8192
    digest = hashlib.sha256()
    with path.open("wb") as output:
        while length:
            data = pattern[:min(length, len(pattern))]
            output.write(data)
            digest.update(data)
            length -= len(data)
    return digest.hexdigest()


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def spill_files(document):
    info = document[b"info"]
    tree = info[b"file tree"]
    single = len(tree) == 1 and b"" in next(iter(tree.values()))
    entries = []

    def walk(node, parts):
        if b"" in node:
            props = node[b""]
            entries.append({"path": "/".join(p.decode() for p in parts),
                            "length": props[b"length"], "root": props.get(b"pieces root")})
        else:
            for name, child in sorted(node.items()):
                walk(child, parts + [name])
    walk(tree, [] if single else [info[b"name"]])
    offset = 0
    for entry in entries:
        entry["offset"] = offset
        if entry["length"]:
            offset += ((entry["length"] + info[b"piece length"] - 1)
                       // info[b"piece length"]) * info[b"piece length"]
    return entries


def wait_for(description, predicate, timeout):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise TimeoutError(description)


class Engine:
    def __init__(self, binary, root, torrent, output, host, *, seed=False, peers=(),
                 upload_limit=0, download_limit=0, priorities=(), web_seeds=False):
        self.binary, self.root, self.output = binary, root, output
        root.mkdir()
        output.mkdir(parents=True, exist_ok=True)
        self.config = dict(torrent=str(torrent), output=str(output),
                           recovery=str(root / "recovery"), stop=str(root / "stop"),
                           listen=f"{host}:0", peers=[f"{h}:{p}" for h, p in peers],
                           mode="seed" if seed else "download", upload_limit=upload_limit,
                           download_limit=download_limit, file_priorities=list(priorities),
                           web_seeds=web_seeds)
        self.runs = 0
        self.process = None
        self.reader = None
        self.latest = {}
        self.start()

    def start(self):
        self.root.joinpath("stop").unlink(missing_ok=True)
        path = self.root / f"config-{self.runs}.json"
        path.write_text(json.dumps(self.config, indent=2))
        log = self.root / f"status-{self.runs}.jsonl"
        with log.open("w") as stdout, (self.root / f"stderr-{self.runs}.log").open("w") as stderr:
            self.process = subprocess.Popen([str(self.binary), str(path)], stdout=stdout, stderr=stderr)
        self.reader = log.open()
        self.latest = {}
        self.runs += 1

    def snapshot(self):
        for line in self.reader:
            value = json.loads(line)
            if value["event"] == "outcome":
                if value["error"]:
                    raise RuntimeError(value)
            else:
                self.latest = value
        if self.process.poll() is not None:
            raise RuntimeError(f"engine exited {self.process.returncode}: {self.root}")
        return self.latest

    def ready(self, timeout):
        wait_for(f"engine ready: {self.root}", lambda: self.snapshot().get("ready"), timeout)
        host, port = self.latest["listen"].rsplit(":", 1)
        return host, int(port)

    def complete(self, timeout):
        wait_for(f"engine completion: {self.root}", lambda: self.snapshot().get("completed"), timeout)
        assert self.latest["hash_failed_pieces"] == 0, self.latest
        return self.latest.copy()

    def recheck(self, timeout):
        self.stop()
        mode = self.config["mode"]
        self.config["mode"] = "recheck"
        self.start()
        code = self.process.wait(timeout=timeout)
        rows = [json.loads(line) for line in self.reader]
        self.reader.close()
        self.process = None
        self.config["mode"] = mode
        outcome = next(row for row in rows if row["event"] == "outcome")
        assert code == 0 and outcome["recheck_finished"] and outcome["full_complete"], outcome
        return outcome

    def kill(self):
        self.process.kill()
        self.process.wait(timeout=10)
        self.reader.close()
        self.process = None

    def stop(self):
        if self.process is not None:
            self.root.joinpath("stop").touch()
            try:
                code = self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                raise
            finally:
                self.reader.close()
                self.process = None
            if code:
                raise RuntimeError(f"engine cleanup failed ({code}): {self.root}")


class Libtorrent:
    def __init__(self, lt, root, torrent, output, host, *, selected=None, expected_corruption=False,
                 dht=False, pex=False, magnet=None):
        self.lt, self.root = lt, root
        self.expected_corruption = expected_corruption
        self.hash_failures = 0
        self.cache_flushed = False
        self.disconnects = []
        root.mkdir()
        output.mkdir(parents=True, exist_ok=True)
        self.log = (root / "alerts.jsonl").open("w")
        self.session = lt.session({
            "listen_interfaces": f"{host}:0", "enable_dht": dht, "enable_lsd": False,
            "dht_bootstrap_nodes": "", "dht_ignore_dark_internet": False,
            "dht_restrict_routing_ips": False, "dht_restrict_search_ips": False,
            "dht_block_ratelimit": 1000, "close_redundant_connections": not pex,
            "allow_multiple_connections_per_ip": dht or pex,
            "enable_upnp": False, "enable_natpmp": False,
            "enable_outgoing_utp": False, "enable_incoming_utp": False,
            "out_enc_policy": int(lt.enc_policy.pe_disabled),
            "in_enc_policy": int(lt.enc_policy.pe_disabled),
            "alert_mask": int(lt.alert_category.all),
        })
        params = lt.parse_magnet_uri(magnet) if magnet else lt.load_torrent_file(str(torrent))
        params.save_path = str(output)
        params.flags |= (lt.torrent_flags.paused | lt.torrent_flags.disable_dht
                         | lt.torrent_flags.disable_lsd)
        if pex:
            params.flags &= ~lt.torrent_flags.disable_pex
        else:
            params.flags |= lt.torrent_flags.disable_pex
        params.flags &= ~lt.torrent_flags.auto_managed
        self.handle = self.session.add_torrent(params)
        self.handle.replace_trackers([])
        if selected is not None:
            storage = params.ti.layout()
            self.handle.prioritize_files([int(storage.file_path(i).endswith('/' + selected))
                                          for i in range(storage.num_files())])
        self.handle.resume()
        self.clients = set()

    def alerts(self):
        alerts = self.session.pop_alerts()
        for alert in alerts:
            if isinstance(alert, self.lt.cache_flushed_alert):
                self.cache_flushed = True
            record = {"time": time.time(), "type": type(alert).__name__, "message": alert.message()}
            if isinstance(alert, (self.lt.peer_disconnected_alert, self.lt.peer_error_alert)):
                record["error"] = alert.error.message()
                record["error_value"] = alert.error.value()
                self.disconnects.append(record)
            self.log.write(json.dumps(record) + "\n")
        self.log.flush()
        return alerts

    def poll(self):
        for alert in self.alerts():
            if isinstance(alert, self.lt.hash_failed_alert):
                self.hash_failures += 1
                if not self.expected_corruption:
                    raise RuntimeError(alert.message())
            if isinstance(alert, (self.lt.torrent_error_alert, self.lt.file_error_alert)):
                raise RuntimeError(alert.message())
        for peer in self.handle.get_peer_info():
            self.clients.add(peer.client.decode("utf-8", "replace"))
        status = self.handle.status()
        if status.errc.value():
            raise RuntimeError(status.errc.message())
        return status

    def complete(self, timeout, *, seed=False):
        wait_for(f"libtorrent completion: {self.root}",
                 lambda: (self.poll().is_seeding if seed else self.poll().is_finished), timeout)
        # Finished may precede disk writes, even creation of a one-byte file.
        self.cache_flushed = False
        self.handle.flush_cache()
        wait_for(f"libtorrent payload writes: {self.root}", lambda: (self.poll(), self.cache_flushed)[1], timeout)
        status = self.poll()
        return {"downloaded": status.total_payload_download, "uploaded": status.total_payload_upload,
                "clients": sorted(self.clients), "seeding": status.is_seeding}

    def address(self, host):
        port = wait_for("libtorrent listen", self.session.listen_port, 10)
        return host, port

    def stop(self):
        if self.session is not None:
            self.handle.pause()
            self.session.remove_torrent(self.handle)
            wait_for("libtorrent removal", lambda: not self.handle.is_valid(), 10)
            self.alerts()
            self.session = None
            self.log.close()


def frame_event(body):
    event = {"id": body[0] if body else None, "bytes": len(body)}
    if body and body[0] in (6, 8, 16):
        event.update(zip(("piece", "begin", "length"), struct.unpack("!III", body[1:])))
    elif body and body[0] == 7:
        event.update(zip(("piece", "begin"), struct.unpack("!II", body[1:9])))
        event["length"] = len(body) - 9
    elif body and body[0] == 20:
        event["extension"] = body[1]
        event["extension_payload"] = body[2:].hex()
    elif body and body[0] in (21, 22, 23):
        event["root"] = body[1:33].hex()
        event.update(zip(("base", "index", "length", "proof"), struct.unpack("!IIII", body[33:49])))
        event["hashes"] = body[49:].hex()
    return event


class Relay:
    """Plaintext TCP relay retaining frame headers and hash nodes, never payloads."""
    def __init__(self, host, target, path, initiator, *, withhold_pieces=False, force_v1=False,
                 choke_source=False, allow_hash_requests=False, hash_policy=None, accepted_key=None,
                 choke_for_hashes=False):
        self.target, self.initiator = target, initiator
        self.withhold_pieces = withhold_pieces
        self.choke_source = choke_source
        self.force_v1 = force_v1
        self.allow_hash_requests = allow_hash_requests
        self.hash_policy = hash_policy
        self.accepted_key = accepted_key
        self.choke_for_hashes = choke_for_hashes
        self.handshakes = []
        self.hash_requests = []
        self.hash_replies = []
        self.hash_rejects = []
        self.closures = []
        self.listener = socket.create_server((host, 0))
        self.listener.settimeout(0.2)
        self.address = self.listener.getsockname()
        self.log = path.open("w")
        self.lock = threading.Lock()
        self.done = threading.Event()
        self.workers, self.errors = [], []
        self.counts = {"spill": {}, "libtorrent": {}}
        self.thread = threading.Thread(target=self.accept)
        self.thread.start()

    def record(self, direction, event):
        with self.lock:
            if "handshake" in event:
                self.handshakes.append({"from": direction, **event})
            if event.get("closed"):
                self.closures.append(direction)
            if "id" in event:
                counts = self.counts[direction]
                name = str(event["id"])
                counts[name] = counts.get(name, 0) + 1
                if event["id"] == 21:
                    self.hash_requests.append(event.copy())
                if event["id"] == 22:
                    self.hash_replies.append(event.copy())
                if event["id"] == 23:
                    self.hash_rejects.append(event.copy())
            self.log.write(json.dumps({"time": time.time(), "from": direction, **event}) + "\n")
            self.log.flush()

    def accept(self):
        while not self.done.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                if not self.done.is_set():
                    raise
                break
            worker = threading.Thread(target=self.forward, args=(client,))
            self.workers.append(worker)
            worker.start()

    def forward(self, client):
        remote = None
        try:
            remote = socket.create_connection(self.target, timeout=10)
            client.settimeout(10)
            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            remote.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            selector = selectors.DefaultSelector()
            other = "libtorrent" if self.initiator == "spill" else "spill"
            selector.register(client, selectors.EVENT_READ, (remote, self.initiator))
            selector.register(remote, selectors.EVENT_READ, (client, other))
            buffers, handshakes = {client: bytearray(), remote: bytearray()}, set()
            source_choked = True
            waiting_hash = None
            with selector:
                while not self.done.is_set():
                    for key, _ in selector.select(0.2):
                        source = key.fileobj
                        destination, direction = key.data
                        data = source.recv(65536)
                        if not data:
                            self.record(direction, {"closed": True})
                            return
                        buf = buffers[source]
                        buf.extend(data)
                        if source not in handshakes and len(buf) >= 68:
                            assert buf[:20] == b"\x13BitTorrent protocol", "non-plaintext handshake"
                            hello = bytearray(buf[:68])
                            if direction == self.initiator and self.accepted_key is not None and hello[28:48] != self.accepted_key:
                                self.record(direction, {"handshake": hello.hex(), "rejected_key": True})
                                return
                            if self.force_v1:
                                hello[27] &= ~0x10
                            destination.sendall(hello)
                            self.record(direction, {"handshake": hello.hex()})
                            del buf[:68]
                            handshakes.add(source)
                        while source in handshakes and len(buf) >= 4:
                            size = int.from_bytes(buf[:4], "big")
                            assert size <= 1024 * 1024, size
                            if len(buf) < size + 4:
                                break
                            body = bytes(buf[4:4 + size])
                            if self.choke_source and direction == "libtorrent" and body == b"\x01":
                                destination.sendall(b"\x00\x00\x00\x01\x00")
                                self.record(direction, {**frame_event(body), "forwarded_as": 0})
                                del buf[:4 + size]
                                continue
                            if direction == other and body in (b"\x00", b"\x01"):
                                source_choked = body == b"\x00"
                                if source_choked and waiting_hash is not None:
                                    packet, event = waiting_hash
                                    source.sendall(packet)
                                    self.record(self.initiator, {**event, "source_choked": True})
                                    waiting_hash = None
                            if body and body[0] == 21 and direction == self.initiator:
                                event = {**frame_event(body), "source_choked": source_choked}
                                if self.choke_for_hashes and not source_choked:
                                    assert waiting_hash is None
                                    send(destination, 2)
                                    send(destination, 3)
                                    self.record(direction, {"id": 2, "injected": "request-native-choke"})
                                    self.record(direction, {"id": 3, "injected": "request-native-choke"})
                                    waiting_hash = (bytes(buf[:4 + size]), event)
                                    del buf[:4 + size]
                                    continue
                                if self.hash_policy in ("stall", "reject"):
                                    self.record(direction, {**event, "injected": self.hash_policy})
                                    if self.hash_policy == "reject":
                                        send(source, 23, body[1:49])
                                    del buf[:4 + size]
                                    continue
                                self.record(direction, event)
                                destination.sendall(buf[:4 + size])
                                del buf[:4 + size]
                                continue
                            if body and body[0] == 22 and self.hash_policy == "corrupt":
                                corrupted = bytearray(body)
                                corrupted[49] ^= 1
                                send(destination, 22, corrupted[1:])
                                self.record(direction, {**frame_event(bytes(corrupted)), "injected": "corrupt"})
                                del buf[:4 + size]
                                continue
                            if body and body[0] == 7 and self.withhold_pieces:
                                self.record(direction, {**frame_event(body), "withheld_retry": True})
                                del buf[:4 + size]
                                continue
                            destination.sendall(buf[:4 + size])
                            self.record(direction, frame_event(body))
                            if body and body[0] == 23 and self.hash_policy == "close-after-reject":
                                return
                            del buf[:4 + size]
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as error:
            if not self.done.is_set():
                self.errors.append(repr(error))
        finally:
            client.close()
            if remote is not None:
                remote.close()

    def stop(self):
        self.done.set()
        self.listener.close()
        self.thread.join()
        for worker in self.workers:
            worker.join(timeout=12)
            assert not worker.is_alive(), "relay failed to join"
        self.log.close()
        assert not self.errors, self.errors
        if not self.allow_hash_requests:
            assert not self.counts["spill"].get("21", 0), "spill sent an outbound hash request"
        return self.counts


def receive(peer):
    def exact(length):
        data = bytearray()
        while len(data) < length:
            chunk = peer.recv(length - len(data))
            if not chunk:
                raise ConnectionError("peer closed")
            data.extend(chunk)
        return bytes(data)
    size = int.from_bytes(exact(4), "big")
    assert size <= 1024 * 1024, size
    return exact(size)


def send(peer, message, body=b""):
    peer.sendall(struct.pack("!IB", len(body) + 1, message) + body)


def raw_peer(address, key, timeout, *, fast=False):
    peer = socket.create_connection(address, timeout=timeout)
    peer.settimeout(timeout)
    peer.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    peer.sendall(b"\x13BitTorrent protocol" + bytes(7) + bytes([4 if fast else 0]) + key + b"-SI0001-" + os.urandom(12))
    hello = bytearray()
    while len(hello) < 68:
        data = peer.recv(68 - len(hello))
        if not data:
            raise ConnectionError("handshake closed")
        hello.extend(data)
    assert hello[:20] == b"\x13BitTorrent protocol" and hello[28:48] == key, hello.hex()
    return peer


def await_id(peer, wanted, trace):
    while True:
        body = receive(peer)
        trace.append(frame_event(body))
        if body and body[0] in wanted:
            return body


def query_hash(address, key, entry, piece_length, request, timeout, *, choke=False):
    header = entry["root"] + struct.pack("!IIII", *request)
    trace = []
    with raw_peer(address, key, timeout) as peer:
        send(peer, 2)
        await_id(peer, (1,), trace)
        piece = entry["offset"] // piece_length + (request[1] * BLOCK // piece_length if request[0] == 0 else 0)
        relative = piece - entry["offset"] // piece_length
        length = min(BLOCK, entry["length"] - relative * piece_length)
        send(peer, 6, struct.pack("!III", piece, 0, length))
        block = await_id(peer, (7, 16), trace)
        assert block[0] == 7, trace
        send(peer, 21, header)
        if choke:
            send(peer, 3)
        reply = await_id(peer, (22, 23), trace)
        assert reply[1:49] == header, trace
        assert reply[0] == 22, trace
        return reply[49:], trace


def verify_files(root, entries, expected, selected=None):
    verified = {}
    for entry in entries:
        if selected is not None and not entry["path"].endswith('/' + selected):
            continue
        path = root / entry["path"]
        actual = digest(path)
        assert path.stat().st_size == entry["length"] and actual == expected[entry["length"]], path
        verified[entry["path"]] = actual
    return verified


def lt_files(lt, torrent):
    files = lt.load_torrent_file(str(torrent)).ti.layout()
    return [{"path": files.file_path(i), "length": files.file_size(i)}
            for i in range(files.num_files())
            if not int(files.file_flags(i)) & int(lt.file_flags_t.flag_pad_file)]


def transfer_case(lt, args, binary, root, torrent, *, selected=None, restart=False, force_v1=False):
    root.mkdir()
    document = lt.bdecode(torrent.read_bytes())
    entries, native = spill_files(document), lt_files(lt, torrent)
    seed, target, fresh = root / "lt-payload", root / "spill-payload", root / "lt-fresh"
    expected = {}
    for entry in native:
        expected[entry["length"]] = write_payload(seed / entry["path"], entry["length"])
    source = Libtorrent(lt, root / "lt-seed", torrent, seed, args.host)
    engine = relay = leecher = reverse = None
    result = {"torrent": torrent.name, "selected": selected, "restart": restart,
              "placement": {"spill": [e["path"] for e in entries], "libtorrent": [e["path"] for e in native]}}
    try:
        source.complete(args.timeout, seed=True)
        relay = Relay(args.host, source.address(args.host), root / "download-wire.jsonl", "spill", force_v1=force_v1)
        priorities = []
        if selected is not None:
            # File indices include virtual gaps, as exposed by the public layout.
            for index, entry in enumerate(entries):
                priorities.append("normal" if entry["path"].endswith('/' + selected) else "skip")
                if entry["length"] % document[b"info"][b"piece length"] and index < len(entries) - 1:
                    priorities.append("skip")
        engine = Engine(binary, root / "engine", torrent, target, args.host,
                        peers=[relay.address], priorities=priorities,
                        download_limit=1024 * 1024 if restart else 0)
        engine.ready(args.timeout)
        if restart:
            wait_for("partially verified payload before SIGKILL",
                     lambda: 0 < engine.snapshot().get("verified_pieces", 0) and not engine.latest["completed"], args.timeout)
            result["before_kill"] = engine.latest.copy()
            engine.kill()
            engine.start()
            engine.ready(args.timeout)
            result["after_restart"] = engine.latest.copy()
        result["download"] = engine.complete(args.timeout)
        result["spill_files"] = verify_files(target, entries, expected, selected)
        result["download_wire"] = relay.stop()
        result["download_handshakes"] = relay.handshakes
        relay = None
        source.stop()
        if restart:
            engine.stop()
            engine.config["peers"] = []
            engine.start()
            engine.ready(args.timeout)
            result["seed_restart"] = engine.complete(args.timeout)
            assert result["seed_restart"]["received_bytes"] == 0
        address = engine.latest["listen"].rsplit(":", 1)
        reverse = Relay(args.host, (address[0], int(address[1])), root / "upload-wire.jsonl", "libtorrent", force_v1=force_v1)
        leecher = Libtorrent(lt, root / "lt-download", torrent, fresh, args.host, selected=selected)
        leecher.handle.connect_peer(reverse.address)
        result["upload"] = leecher.complete(args.timeout)
        result["libtorrent_files"] = verify_files(fresh, native, expected, selected)
        result["upload_wire"] = reverse.stop()
        result["upload_handshakes"] = reverse.handshakes
        reverse = None
        if restart:
            leecher.stop()
            result["offline_recheck"] = engine.recheck(args.timeout)
        return result
    finally:
        (root / "transfer.json").write_text(json.dumps(result, indent=2))
        if leecher is not None:
            leecher.stop()
        source.stop()
        if engine is not None:
            engine.stop()
        if relay is not None:
            relay.stop()
        if reverse is not None:
            reverse.stop()


def proof_case(lt, args, binary, root, torrent):
    root.mkdir()
    document = lt.bdecode(torrent.read_bytes())
    entries = spill_files(document)
    piece_length = document[b"info"][b"piece length"]
    seed, source_data = root / "spill-payload", root / "lt-payload"
    for entry in entries:
        write_payload(seed / entry["path"], entry["length"])
    for entry in lt_files(lt, torrent):
        write_payload(source_data / entry["path"], entry["length"])
    source = Libtorrent(lt, root / "lt-seed", torrent, source_data, args.host)
    engine = None
    result = []
    try:
        source.complete(args.timeout, seed=True)
        engine = Engine(binary, root / "engine", torrent, seed, args.host, seed=True)
        address = engine.ready(args.timeout)
        key = source.handle.info_hashes().v2.to_bytes()[:20]
        for entry in entries:
            if entry["length"] <= BLOCK:
                continue
            # ceil(log2(ceil(file_length / BLOCK)))
            height = ((entry["length"] + BLOCK - 1) // BLOCK - 1).bit_length()
            piece_height = (min((entry["length"] + BLOCK - 1) // BLOCK, piece_length // BLOCK) - 1).bit_length()
            requests = set()
            for base in {0, piece_height}:
                if base >= height:
                    continue
                width = 1 << (height - base)
                for length in (2, 4, 512):
                    if length <= width and (base != 0 or piece_height == 0 or length <= 1 << piece_height):
                        requests.add((base, 0, length, height - base - 1))
                        if width > length:
                            # The final aligned range may include absent leaves/pieces.
                            index = ((entry["length"] - 1) // (BLOCK << base) // length) * length
                            requests.add((base, index, length, height - base - 1))
            for request in sorted(requests):
                oracle, lt_trace = query_hash(source.address(args.host), key, entry, piece_length, request, args.timeout)
                actual, spill_trace = query_hash(address, key, entry, piece_length, request, args.timeout)
                assert actual == oracle, {"request": request, "libtorrent": oracle.hex(), "spill": actual.hex()}
                result.append({"file": entry["path"], "root": entry["root"].hex(), "request": request, "hash_count": len(actual) // 32,
                               "hashes": actual.hex(), "libtorrent_trace": lt_trace, "spill_trace": spill_trace})
                (root / "proofs.json").write_text(json.dumps(result, indent=2))
        assert result, "fixture provides no hash ranges"
        return result
    finally:
        source.stop()
        if engine is not None:
            engine.stop()


def policy_case(lt, args, binary, root):
    root.mkdir()
    torrent = FIXTURES / "v2.torrent"
    document = lt.bdecode(torrent.read_bytes())
    entries = spill_files(document)
    entry = next(e for e in entries if e["path"].endswith("/odd"))
    piece_length = document[b"info"][b"piece length"]
    request = (0, 0, 2, 2)
    header = entry["root"] + struct.pack("!IIII", *request)
    source_data, seed = root / "lt-payload", root / "spill-payload"
    expected = {}
    for file in lt_files(lt, torrent):
        expected[file["length"]] = write_payload(source_data / file["path"], file["length"])
    for file in entries:
        write_payload(seed / file["path"], file["length"])
    source = Libtorrent(lt, root / "lt-seed", torrent, source_data, args.host)
    engine = None
    result = {}
    try:
        source.complete(args.timeout, seed=True)
        engine = Engine(binary, root / "engine", torrent, seed, args.host,
                        seed=True, upload_limit=32768)
        address = engine.ready(args.timeout)
        key = bytes.fromhex(engine.latest["identity"])[:20]
        for name, endpoint in [("libtorrent", source.address(args.host)), ("spill", address)]:
            trace = []
            with raw_peer(endpoint, key, args.timeout) as peer:
                send(peer, 21, header)
                reply = await_id(peer, (22, 23), trace)
                result[name + "_initial_leaf_reply"] = reply[0]
                if name == "spill":
                    assert reply[0] == 23, trace
            hashes, trace = query_hash(endpoint, key, entry, piece_length, request, args.timeout, choke=True)
            result[name + "_queued_proof"] = {"hashes": hashes.hex(), "trace": trace}
            if name == "spill":
                assert any(e.get("id") == 0 for e in trace), "proof was not observed across a choke"
        assert result["libtorrent_queued_proof"]["hashes"] == result["spill_queued_proof"]["hashes"]
        engine.stop()
        engine.config["upload_limit"] = 0
        engine.start()
        address = engine.ready(args.timeout)
        result["request_duties"], result["violations"] = [], []
        for name, endpoint in [("libtorrent", source.address(args.host)), ("spill", address)]:
            for fast in (False, True):
                trace = []
                with raw_peer(endpoint, key, args.timeout, fast=fast) as peer:
                    piece = entry["offset"] // piece_length
                    send(peer, 6, struct.pack("!III", piece, 0, BLOCK))
                    initial = await_id(peer, (7, 16), trace)[0]
                    send(peer, 2)
                    await_id(peer, (1,), trace)
                    requests = []
                    for file in entries:
                        for offset in range(0, file["length"], BLOCK):
                            index = (file["offset"] + offset) // piece_length
                            requests.append(struct.pack("!IBIII", 13, 6, index, offset % piece_length,
                                                        min(BLOCK, file["length"] - offset)))
                    peer.sendall(b"".join(requests) + struct.pack("!IB", 1, 3))
                    choked, answers, rejects = False, 0, 0
                    while answers < len(requests):
                        (root / f"duties-{name}-{fast}.json").write_text(json.dumps({"client": name, "fast": fast, "wire_key": key.hex(), "answers": answers, "requests": [frame_event(r[4:]) for r in requests], "trace": trace}, indent=2))
                        try:
                            body = receive(peer)
                        except socket.timeout:
                            result["violations"].append({"client": name, "fast": fast, "unanswered_after_choke": len(requests) - answers, "timeout": args.timeout})
                            break
                        trace.append(frame_event(body))
                        if body and body[0] == 0:
                            choked = True
                        if body and body[0] in (7, 16):
                            answers += 1
                            if body[0] == 16:
                                assert choked, {"client": name, "fast": fast, "trace": trace}
                                rejects += 1
                    result["request_duties"].append({"client": name, "fast": fast, "initial_answer": initial,
                                                       "pending_rejects": rejects, "trace": trace})
        # An unsolicited response must not be accepted as download authority.
        for name, endpoint in [("libtorrent", source.address(args.host)), ("spill", address)]:
            trace = []
            with raw_peer(endpoint, key, 2) as peer:
                send(peer, 7, struct.pack("!II", 0, 0) + b"\0")
                try:
                    while True:
                        trace.append(frame_event(receive(peer)))
                except (ConnectionError, ConnectionResetError):
                    result[name + "_unsolicited_disconnect"] = True
                except socket.timeout:
                    result[name + "_unsolicited_disconnect"] = False
            result[name + "_unsolicited_trace"] = trace
        assert result["spill_unsolicited_disconnect"], result
        # Rejected work must not take upload credit away from a real client.
        result["choked_budget_isolation"] = []
        for limit in (0, 16384):
            engine.stop()
            engine.config["upload_limit"] = limit
            engine.start()
            address = engine.ready(args.timeout)
            client = relay = None
            try:
                with raw_peer(address, key, args.timeout) as peer:
                    trace = []
                    for _ in range(3):
                        send(peer, 21, header)
                        assert await_id(peer, (22, 23), trace)[0] == 23, trace
                    relay = Relay(args.host, address, root / f"isolation-{limit}-wire.jsonl", "libtorrent")
                    destination = root / f"lt-fresh-{limit}"
                    client = Libtorrent(lt, root / f"lt-download-{limit}", torrent, destination, args.host)
                    client.handle.connect_peer(relay.address)
                    started = time.monotonic()
                    transfer = client.complete(args.timeout)
                    result["choked_budget_isolation"].append({"upload_limit": limit,
                        "seconds": time.monotonic() - started, "transfer": transfer,
                        "files": verify_files(destination, lt_files(lt, torrent), expected),
                        "wire": relay.stop()})
                    relay = None
            finally:
                if client is not None:
                    client.stop()
                if relay is not None:
                    relay.stop()
        result["known_divergences"] = []
        if lt.__version__ == "2.1.2.0":
            known = [v for v in result["violations"] if v["client"] == "libtorrent" and not v["fast"]]
            assert len(known) == 1 and known[0]["unanswered_after_choke"] == 8, {"known_divergence_changed": result["request_duties"]}
            result["known_divergences"].append({"version": "2.1.2.0", "observation": known[0],
                "source_tag": "v2.1.2", "source_commit": "6da363d2994f17c0b3c0450d124cf73a31a73847",
                "sources": ["https://github.com/arvidn/libtorrent/blob/v2.1.2/src/bt_peer_connection.cpp#L389",
                            "https://github.com/arvidn/libtorrent/blob/v2.1.2/src/peer_connection.cpp#L3821",
                            "https://github.com/arvidn/libtorrent/blob/v2.1.2/src/peer_connection.cpp#L3304"],
                "cause": "bt_peer_connection::write_reject_request returns on !m_supports_fast; send_choke and incoming_cancel erase requests then call it"})
            result["violations"].remove(known[0])
        result["status"] = "failed" if result["violations"] else "passed"
        return result
    finally:
        (root / "policies.json").write_text(json.dumps(result, indent=2))
        source.stop()
        if engine is not None:
            engine.stop()


def corruption_case(lt, args, binary, root):
    root.mkdir()
    size, piece_length = 65536, 65536
    seeds = [root / f"payload-{i}" for i in range(2)]
    expected = [write_payload(seed / "payload.bin", size) for seed in seeds]
    creator = lt.create_torrent([lt.create_file_entry("payload.bin", size)], piece_length,
                                lt.create_torrent_flags_t.v2_only)
    lt.set_piece_hashes(creator, str(seeds[0]))
    document = creator.generate()
    torrent = root / "corruption.torrent"
    torrent.write_bytes(lt.bencode(document))
    entry = spill_files(document)[0]
    engines, relays = [], []
    client = None
    result = {}
    try:
        for i, seed in enumerate(seeds):
            engine = Engine(binary, root / f"engine-{i}", torrent, seed, args.host, seed=True)
            engines.append(engine)
            address = engine.ready(args.timeout)
            with (seed / "payload.bin").open("r+b") as payload:
                payload.write(b"\x01")
            assert digest(seed / "payload.bin") != expected[i]
            relays.append(Relay(args.host, address, root / f"corrupt-{i}-wire.jsonl",
                                "libtorrent", withhold_pieces=i == 1,
                                hash_policy="close-after-reject" if i == 1 else None))
        destination = root / "lt-download"
        client = Libtorrent(lt, root / "lt-client", torrent, destination, args.host, expected_corruption=True)
        client.session.apply_settings({"allow_multiple_connections_per_ip": True})
        client.handle.connect_peer(relays[0].address)
        def failed_piece():
            client.poll()
            return client.hash_failures > 0 and any(d["error"] == "too many corrupt pieces" for d in client.disconnects)
        wait_for("libtorrent rejects the corrupt payload from its single source", failed_piece, args.timeout)
        # The first source is banned before leaf lookup. Hold retry payload so
        # the second changed seed's hash reject is observed independently. Close
        # that probe after its reject to prevent libtorrent's immediate retry loop.
        client.handle.connect_peer(relays[1].address)
        def rejected_range():
            client.poll()
            return any(r["base"] == 0 for relay in relays for r in relay.hash_rejects)
        wait_for("changed-seed leaf requests receive hash rejects", rejected_range, args.timeout)
        client.handle.pause()
        fields = ("root", "base", "index", "length", "proof")
        status = client.poll()
        assert client.hash_failures > 0 and status.num_pieces == 0 and not status.is_finished, "corrupt content was accepted"
        result["changed_seed_status"] = [engine.snapshot().copy() for engine in engines]
        assert all(row["phase"] == "Seeding" for row in result["changed_seed_status"]), result["changed_seed_status"]
        for engine in engines:
            engine.stop()
        result["corrupt_wire"] = [relay.stop() for relay in relays]
        requests = [r for relay in relays for r in relay.hash_requests if r["base"] == 0]
        rejects = [r for relay in relays for r in relay.hash_rejects if r["base"] == 0]
        replies = [r for relay in relays for r in relay.hash_replies if r["base"] == 0]
        assert requests and not replies, {"requests": requests, "hashes": replies}
        request_counts = Counter(tuple(r[k] for k in fields) for r in requests)
        reject_counts = Counter(tuple(r[k] for k in fields) for r in rejects)
        assert request_counts == reject_counts, {"requests": request_counts, "rejects": reject_counts}
        assert all(r["root"] == entry["root"].hex() and r["index"] == 0 and r["length"] == 4 for r in requests), requests
        result.update(hash_failures=client.hash_failures, corrupt_stage_verified_pieces=status.num_pieces,
                      natural_leaf_requests=requests, leaf_rejects=rejects,
                      leaf_hash_replies=replies, disconnects=list(client.disconnects),
                      expected_root=entry["root"].hex())
        relays.clear()
        client.handle.resume()
        honest = root / "honest-payload"
        write_payload(honest / "payload.bin", size)
        engine = Engine(binary, root / "honest-engine", torrent, honest, args.host, seed=True)
        engines.append(engine)
        address = engine.ready(args.timeout)
        relay = Relay(args.host, address, root / "honest-wire.jsonl", "libtorrent")
        relays.append(relay)
        client.handle.connect_peer(relay.address)
        result["recovered_with_honest_peer"] = client.complete(args.timeout)
        assert digest(destination / "payload.bin") == expected[0]
        result["checksum_matches"] = True
        return result
    finally:
        if client is not None:
            client.stop()
        for engine in engines:
            engine.stop()
        for relay in relays:
            relay.stop()
        (root / "corruption.json").write_text(json.dumps(result, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-network", action="store_true", required=True)
    parser.add_argument("--host", required=True, help="IPv4 address of this dev machine's LAN interface")
    parser.add_argument("--scenario", choices=("smoke", "restart", "proofs", "policy", "corruption", "hybrid", "all"), default="all")
    parser.add_argument("--fixture", action="append", choices=FIXTURE_NAMES + HYBRID_FIXTURES, help="committed fixture name for smoke/proofs/all; repeat to select a matrix")
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--artifacts", type=Path, help="new directory, never an existing state directory")
    parser.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args()
    if os.environ.get("CI"):
        parser.error("tier 2 runs are dev-machine-only, never CI")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.fixture and args.scenario in ("restart", "policy", "corruption", "hybrid"):
        parser.error("--fixture applies to smoke/proofs/all")
    try:
        socket.inet_pton(socket.AF_INET, args.host)
    except OSError:
        parser.error("--host must be a numeric LAN IPv4 address")
    with socket.socket() as check:
        check.bind((args.host, 0))
    import libtorrent as lt
    subprocess.run(["cargo", "build", "--locked", "--example", "interop-engine"] + (["--release"] if args.release else []), check=True)
    binary = Path("target") / ("release" if args.release else "debug") / "examples" / "interop-engine"
    binary = binary.resolve()
    parent = Path("target/interop").resolve()
    parent.mkdir(parents=True, exist_ok=True)
    root = args.artifacts.resolve() if args.artifacts else Path(tempfile.mkdtemp(prefix="libtorrent-v2-", dir=parent))
    if args.artifacts:
        root.mkdir(parents=True)
    result = {"libtorrent": lt.__version__, "binding": lt.__file__, "host": args.host,
              "python": sys.version, "platform": sys.platform,
              "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"])),
              "binary_sha256": digest(binary), "harness_sha256": digest(Path(__file__)),
              "runner_source_sha256": digest(Path(__file__).with_name("engine.rs")),
              "scenario": args.scenario, "checks": []}
    print(f"libtorrent {lt.__version__}; artifacts: {root}", flush=True)
    try:
        fixtures = [] if args.scenario in ("restart", "policy", "corruption", "hybrid") else (args.fixture or FIXTURE_NAMES)
        for name in fixtures:
            torrent = FIXTURES / name
            if name == "v2-empty.torrent":
                try:
                    lt.load_torrent_buffer(torrent.read_bytes())
                except RuntimeError as error:
                    result["checks"].append({"type": "empty-metainfo", "torrent": name,
                                              "status": "unsupported", "error": str(error)})
                    print(f"unsupported by libtorrent: {name}: {error}", flush=True)
                    continue
            if args.scenario in ("smoke", "all"):
                print(f"transfer both ways: {name}", flush=True)
                case = transfer_case(lt, args, binary, root / (name + "-transfer"), torrent,
                                     selected="b" if name == "v2-copies.torrent" else None)
                result["checks"].append({"type": "transfer", **case})
            if args.scenario in ("proofs", "all") and name not in ("v2-single.torrent", "v2-empty.torrent", "v2-nested.torrent"):
                print(f"compare libtorrent hash replies: {name}", flush=True)
                proofs = proof_case(lt, args, binary, root / (name + "-proofs"), torrent)
                result["checks"].append({"type": "proofs", "torrent": name, "ranges": len(proofs)})
        if args.scenario in ("hybrid", "all"):
            for mode in ("v1", "v2"):
                for name in HYBRID_FIXTURES:
                    torrent = FIXTURES / name
                    hashes = lt.load_torrent_file(str(torrent)).ti.info_hashes()
                    v1, v2 = hashes.v1.to_bytes(), hashes.v2.to_bytes()[:20]
                    print(f"hybrid both ways in {mode} mode: {name}", flush=True)
                    case = transfer_case(lt, args, binary, root / f"{name}-{mode}", torrent, force_v1=mode == "v1")
                    for direction in ("download_handshakes", "upload_handshakes"):
                        hellos = [bytes.fromhex(h["handshake"]) for h in case[direction]]
                        assert len(hellos) >= 2, case[direction]
                        if mode == "v1":
                            assert all(h[28:48] == v1 for h in hellos), case[direction]
                        else:
                            assert any(h[28:48] == v2 for h in hellos), case[direction]
                    result["checks"].append({"type": "hybrid-transfer", "wire_mode": mode, **case})
                    if mode == "v1":
                        print(f"hybrid hash serving: {name}", flush=True)
                        proofs = proof_case(lt, args, binary, root / f"{name}-proofs", torrent)
                        result["checks"].append({"type": "hybrid-proofs", "torrent": name, "ranges": len(proofs)})
        if args.scenario in ("policy", "all"):
            print("queued proofs, unsolicited pieces and choked-peer budget isolation", flush=True)
            result["checks"].append({"type": "policy", **policy_case(lt, args, binary, root / "policy")})
        if args.scenario in ("corruption", "all"):
            print("post-verification payload mutation and leaf hash rejects", flush=True)
            result["checks"].append({"type": "corruption", **corruption_case(lt, args, binary, root / "corruption")})
        if args.scenario in ("restart", "all"):
            seed = root / "generated-source"
            write_payload(seed / "restart.bin", 8 * 1024 * 1024 + 123)
            files = [lt.create_file_entry("restart.bin", 8 * 1024 * 1024 + 123)]
            creator = lt.create_torrent(files, 262144, lt.create_torrent_flags_t.v2_only)
            lt.set_piece_hashes(creator, str(seed))
            torrent = root / "restart.torrent"
            torrent.write_bytes(lt.bencode(creator.generate()))
            print("SIGKILL partial download; restart download and seed", flush=True)
            result["checks"].append({"type": "restart", **transfer_case(lt, args, binary, root / "restart", torrent, restart=True)})
        result["status"] = "failed" if any(c.get("status") == "failed" for c in result["checks"]) else "passed"
    except Exception as error:
        result["status"], result["error"] = "failed", repr(error)
        raise
    finally:
        (root / "result.json").write_text(json.dumps(result, indent=2))
        print(f"{result.get('status')}: {root / 'result.json'}", flush=True)
    if result["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
