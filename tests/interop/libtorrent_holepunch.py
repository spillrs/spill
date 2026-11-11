#!/usr/bin/env python3
"""Dev-machine tier 2 BEP 55 checks against libtorrent, without root or pcaps.

Uses an explicit LAN IPv4 address, scripted extension peers and an in-process
UDP filter. Checks spill as initiator, completed-seed recipient and introducer.
The filter requires fresh SYNs from both payload clients before forwarding.
This tests client interoperability, not a real router's NAT behaviour.
"""
import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import select
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time

import libtorrent_v2 as wire
from libtorrent_discovery import Daemon
from transmission_smoke import control, read_exact


class Peer:
    def __init__(self, lt, address, key, host, timeout, port=None):
        self.lt = lt
        self.socket = socket.create_connection(address, timeout=timeout, source_address=(host, 0))
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.socket.sendall(b"\x13BitTorrent protocol" + bytes([0, 0, 0, 0, 0, 16, 0, 0])
                            + key + b"-IH0001-" + os.urandom(12))
        hello = read_exact(self.socket, 68)
        assert hello[28:48] == key and hello[25] & 16, hello.hex()
        self.address = host, port or self.socket.getsockname()[1]
        self.extended(0, lt.bencode({b"m": {b"ut_holepunch": 7, b"ut_metadata": 1, b"ut_pex": 2},
                                    b"p": self.address[1]}))
        while True:
            frame = wire.receive(self.socket)
            if frame[:2] == b"\x14\x00":
                self.ids = lt.bdecode(frame[2:])[b"m"]
                break
        assert self.ids.get(b"ut_holepunch", 0), "client does not advertise BEP 55"
        # An ordered metadata reply confirms the remote processed our mapping.
        self.extended(self.ids[b"ut_metadata"], lt.bencode({b"msg_type": 0, b"piece": 0}))
        while wire.receive(self.socket)[:2] != b"\x14\x01":
            pass

    def extended(self, extension, payload):
        wire.send(self.socket, 20, bytes([extension]) + payload)

    def pex(self, target):
        compact = socket.inet_aton(target[0]) + struct.pack("!H", target[1])
        self.extended(self.ids[b"ut_pex"], self.lt.bencode({b"added": compact, b"added.f": b"\x0c"}))

    def punch(self, kind, target):
        self.extended(self.ids[b"ut_holepunch"], bytes([kind, 0]) + socket.inet_aton(target[0])
                      + struct.pack("!HI", target[1], 0))

    def receive(self):
        while True:
            frame = wire.receive(self.socket)
            if frame[:2] != b"\x14\x07":
                continue
            payload = frame[2:]
            assert payload[1] == 0 and len(payload) in (8, 12), payload.hex()
            return payload[0], (socket.inet_ntoa(payload[2:6]), struct.unpack_from("!H", payload, 6)[0])

    def close(self):
        self.socket.close()


class Filter:
    def __init__(self, host, a, b):
        self.local = [b, a]
        self.sockets, self.guards = [], []
        self.done, self.activate, self.activated = threading.Event(), threading.Event(), threading.Event()
        self.lock = threading.Lock()
        self.syns, self.errors = [None, None], []
        for _ in range(2):
            # Reserve TCP too, without listening: an ordinary TCP fallback must
            # fail rather than accidentally reach another service on this port.
            for attempt in range(32):
                tcp, udp = socket.socket(), socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                tcp.bind((host, 0))
                try:
                    udp.bind(tcp.getsockname())
                    break
                except OSError:
                    tcp.close()
                    udp.close()
                    if attempt == 31:
                        raise
            udp.setblocking(False)
            self.sockets.append(udp)
            self.guards.append(tcp)
        self.public = [s.getsockname() for s in self.sockets]
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def run(self):
        try:
            active = False
            while not self.done.is_set():
                if self.activate.is_set() and not active:
                    for sock in self.sockets:
                        while True:
                            try:
                                sock.recvfrom(65536)
                            except BlockingIOError:
                                break
                    active = True
                    self.activated.set()
                readable, _, _ = select.select(self.sockets, [], [], 0.05)
                for sock in readable:
                    payload, sender = sock.recvfrom(65536)
                    direction = self.sockets.index(sock)
                    if sender != self.local[direction]:
                        continue
                    if not active:
                        continue
                    with self.lock:
                        if len(payload) >= 20 and payload[0] == 0x41 and self.syns[direction] is None:
                            self.syns[direction] = time.monotonic()
                        forward = all(self.syns)
                    if forward:
                        self.sockets[1 - direction].sendto(payload, self.local[1 - direction])
        except Exception as error:
            self.errors.append(repr(error))

    def open(self):
        self.activate.set()
        assert self.activated.wait(2), self.errors

    def delays(self, started):
        with self.lock:
            assert all(self.syns), self.syns
            return {"a_ms": (self.syns[1] - started) * 1000, "b_ms": (self.syns[0] - started) * 1000}

    def close(self):
        self.done.set()
        self.thread.join(timeout=5)
        for sock in self.sockets + self.guards:
            sock.close()
        assert not self.thread.is_alive() and not self.errors, self.errors


def libtorrent(lt, args, root, torrent, output, *, pex=True):
    peer = wire.Libtorrent(lt, root, torrent, output, args.host, pex=pex)
    peer.session.apply_settings({"enable_outgoing_utp": True, "enable_incoming_utp": True,
                                 "enable_outgoing_tcp": False, "allow_multiple_connections_per_ip": True,
                                 "close_redundant_connections": False})
    return peer


def transfer(lt, args, binary, root, torrent, payload, key, role):
    root.mkdir()
    with contextlib.ExitStack() as cleanup:
        # Keep the transfer alive across spill's diagnostic sampling interval.
        daemon = Daemon(binary, root / "daemon", args.host, trackers=False, pex=True, utp=True,
                        download_limit=128 * 1024)
        cleanup.callback(daemon.close)
        lt_output = root / "lt-payload"
        lt_output.mkdir()
        if role == "initiator":
            (lt_output / "payload.bin").write_bytes(payload)
        else:
            daemon.output.mkdir(parents=True, exist_ok=True)
            (daemon.output / "payload.bin").write_bytes(payload)
        peer = libtorrent(lt, args, root / "libtorrent", torrent, lt_output)
        cleanup.callback(peer.stop)
        if role == "initiator":
            peer.complete(args.timeout, seed=True)
        else:
            peer.handle.set_download_limit(128 * 1024)
        torrent_id = daemon.add(torrent)
        daemon.wait(torrent_id, lambda r: r["transfer"] and r["transfer"]["ready"]
                    and (role == "initiator" or r["state"] == "seeding"), args.timeout)
        a, b = (daemon.address(), peer.address(args.host)) if role == "initiator" else (peer.address(args.host), daemon.address())
        filtering = Filter(args.host, a, b)
        cleanup.callback(filtering.close)
        relay_a = Peer(lt, a, key, args.host, args.timeout)
        cleanup.callback(relay_a.close)
        relay_b = Peer(lt, b, key, args.host, args.timeout)
        cleanup.callback(relay_b.close)
        relay_a.pex(filtering.public[1])
        assert relay_a.receive() == (0, filtering.public[1]), "expected initiator rendezvous"
        filtering.open()
        started = time.monotonic()
        relay_a.punch(1, filtering.public[1])
        relay_b.punch(1, filtering.public[0])
        remote = filtering.public[1 if role == "initiator" else 0]
        endpoint = f"{remote[0]}:{remote[1]}"
        transport = daemon.wait(torrent_id, lambda r: any(p["transport"] == "utp" and p["detail"]["established"]
                                and p["endpoint"] == endpoint for p in r["transfer"]["peers"]), args.timeout)
        if role == "initiator":
            row = daemon.wait(torrent_id, lambda r: r["state"] == "seeding", args.timeout)
            output = daemon.output / "payload.bin"
        else:
            peer.complete(args.timeout)
            row = daemon.row(torrent_id)
            output = lt_output / "payload.bin"
        expected = hashlib.sha256(payload).hexdigest()
        wire.wait_for("verified direct payload", lambda: output.exists() and wire.digest(output) == expected, args.timeout)
        assert row["transfer"]["hash_failed_pieces"] == 0, row
        return {"role": role, "sha256": expected, "transport": "utp",
                "connect_to_syn_ms": filtering.delays(started), "transport_sample": transport, "daemon": row}


def relay(lt, args, binary, root, torrent, key):
    root.mkdir()
    with contextlib.ExitStack() as cleanup:
        daemon = Daemon(binary, root / "daemon", args.host, trackers=False, pex=True, utp=True)
        cleanup.callback(daemon.close)
        torrent_id = daemon.add(torrent)
        control(daemon.socket, "set", id=torrent_id, values={"file_priorities": ["skip"]})
        daemon.wait(torrent_id, lambda r: r["state"] == "seeding" and r["transfer"]["ready"], args.timeout)
        output = root / "target-payload"
        output.mkdir()
        # Keep the target incomplete: spill retires seed-to-seed connections.
        # Disable target PEX so it cannot independently discover the probe;
        # its SYN must be caused by spill's connect message.
        target = libtorrent(lt, args, root / "libtorrent", torrent, output, pex=False)
        cleanup.callback(target.stop)
        target.handle.connect_peer(daemon.address())
        daemon.wait(torrent_id, lambda r: any(p["detail"]["established"] and p["transport"] == "utp"
                    for p in r["transfer"]["peers"]), args.timeout)
        udp = cleanup.enter_context(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
        udp.bind((args.host, 0))
        udp.settimeout(args.timeout)
        requester = Peer(lt, daemon.address(), key, args.host, args.timeout, port=udp.getsockname()[1])
        cleanup.callback(requester.close)
        requester.punch(0, target.address(args.host))
        assert requester.receive() == (1, target.address(args.host))
        packet, sender = udp.recvfrom(1500)
        assert len(packet) >= 20 and packet[0] == 0x41 and sender == target.address(args.host), (sender, packet.hex())
        row = daemon.row(torrent_id)
        assert row["transfer"]["downloaded_bytes"] == 0, row
        return {"role": "relay", "libtorrent_received_connect": True, "observed_syn": True, "daemon": row}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-network", action="store_true", required=True)
    parser.add_argument("--host", required=True, help="this dev machine's LAN IPv4 address")
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--role", choices=("initiator", "target", "relay", "all"), default="all")
    parser.add_argument("--timeout", type=float, default=90)
    parser.add_argument("--artifacts", type=Path)
    args = parser.parse_args()
    if os.environ.get("CI"):
        parser.error("tier 2 is dev-machine-only, never CI")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    socket.inet_pton(socket.AF_INET, args.host)
    with socket.socket() as check:
        check.bind((args.host, 0))
    import libtorrent as lt
    subprocess.run(["cargo", "build", "--locked"] + (["--release"] if args.release else []), check=True)
    binary = (Path("target") / ("release" if args.release else "debug") / "spill").resolve()
    parent = Path("target/interop").resolve()
    parent.mkdir(parents=True, exist_ok=True)
    root = args.artifacts.resolve() if args.artifacts else Path(tempfile.mkdtemp(prefix="lt-holepunch-", dir=parent))
    if args.artifacts:
        root.mkdir(parents=True)
    payload = bytes(range(251)) * 4096
    pieces = b"".join(hashlib.sha1(payload[i:i + 32768]).digest() for i in range(0, len(payload), 32768))
    info = {b"length": len(payload), b"name": b"payload.bin", b"piece length": 32768, b"pieces": pieces}
    torrent = root / "payload.torrent"
    torrent.write_bytes(lt.bencode({b"info": info}))
    key = hashlib.sha1(lt.bencode(info)).digest()
    result = {"revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"])),
              "host": args.host, "libtorrent": lt.__version__, "checks": []}
    print(f"libtorrent {lt.__version__}; artifacts: {root}", flush=True)
    try:
        for role in ("initiator", "target", "relay") if args.role == "all" else (args.role,):
            result["checks"].append(relay(lt, args, binary, root / role, torrent, key) if role == "relay"
                                    else transfer(lt, args, binary, root / role, torrent, payload, key, role))
        result["status"] = "passed"
    except Exception as error:
        result.update(status="failed", error=repr(error))
        raise
    finally:
        (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(f"{result.get('status')}: {root / 'result.json'}", flush=True)


if __name__ == "__main__":
    main()
