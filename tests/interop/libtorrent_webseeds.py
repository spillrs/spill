#!/usr/bin/env python3
"""Opt-in BEP 19 v2/hybrid URL and swarm checks against libtorrent (dev machine only)."""
import argparse
import http.server
import json
from pathlib import Path
import re
import subprocess
import tempfile
import threading
import time
from urllib.parse import quote, unquote

from libtorrent_v2 import (Engine, FIXTURES, Libtorrent, digest, spill_files,
                          write_payload)


def case(lt, args, binary, root, fixture):
    document = lt.bdecode((FIXTURES / fixture).read_bytes())
    info = document[b"info"]
    # Keep raw-byte file ordering unchanged in both views.
    tree = info[b"file tree"]
    tree[b"odd \xc3\xa9 space"] = tree.pop(b"odd")
    for entry in info.get(b"files", []):
        entry[b"path"] = [b"odd \xc3\xa9 space" if p == b"odd" else p
                          for p in entry[b"path"]]
    entries = spill_files(document)
    files, expected = {}, {}
    for entry in entries:
        path = root / "http" / entry["path"]
        expected[entry["path"]] = write_payload(path, entry["length"])
        files["/" + quote(entry["path"], safe="/")] = path
    records, lock = [], threading.Lock()
    mixed_sources = threading.Event()

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            path = files.get(quote(unquote(self.path), safe="/"))
            match = re.fullmatch(r"bytes=(\d+)-(\d+)", self.headers.get("Range", ""))
            with lock:
                records.append({"path": self.path, "range": self.headers.get("Range"),
                                "agent": self.headers.get("User-Agent")})
            if path is None or match is None:
                self.send_error(404)
                return
            start, end = map(int, match.groups())
            if not 0 <= start <= end < path.stat().st_size:
                self.send_error(416)
                return
            with path.open("rb") as source:
                source.seek(start)
                body = source.read(end - start + 1)
            # Give the peer time to contribute during the mixed-source run.
            if mixed_sources.is_set():
                time.sleep(0.1)
            self.send_response(206)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Range", f"bytes {start}-{end}/{path.stat().st_size}")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    server = http.server.ThreadingHTTPServer((args.host, 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    torrent = root / "webseed.torrent"
    document[b"url-list"] = [f"http://{args.host}:{server.server_port}/".encode()]
    document.pop(b"announce", None)
    document.pop(b"announce-list", None)
    torrent.write_bytes(lt.bencode(document))
    engine = client = None
    results = {}
    try:
        engine = Engine(binary, root / "spill-http", torrent, root / "spill-output",
                        args.host, web_seeds=True)
        results["spill_http"] = engine.complete(args.timeout)
        engine.stop()
        engine = None
        spill_requests = list(records)
        records.clear()
        client = Libtorrent(lt, root / "lt-http", torrent, root / "lt-output", args.host)
        results["libtorrent_http"] = client.complete(args.timeout, seed=True)
        lt_requests = list(records)
        records.clear()
        # Keep the HTTP source attached to libtorrent, but require spill to use
        # the swarm here so a fast mirror cannot mask a broken peer transfer.
        engine = Engine(binary, root / "spill-swarm", torrent, root / "swarm-output",
                        args.host, peers=[client.address(args.host)])
        results["swarm"] = engine.complete(args.timeout)
        results["swarm_uploaded"] = client.poll().total_payload_upload
        assert results["swarm_uploaded"] > 0
        engine.stop()
        engine = None
        swarm_requests = list(records)
        records.clear()
        # The tiny fixtures fit entirely in the HTTP source's request window.
        # Use a larger same-format torrent so both sources have unassigned work.
        mixed_length = 2 * 1024 * 1024 + 123
        mixed_checksum = write_payload(root / "http" / "mixed.bin", mixed_length)
        files["/mixed.bin"] = root / "http" / "mixed.bin"
        flags = 0 if b"pieces" in info else lt.create_torrent_flags_t.v2_only
        creator = lt.create_torrent([lt.create_file_entry("mixed.bin", mixed_length)],
                                    info[b"piece length"], flags)
        lt.set_piece_hashes(creator, str(root / "http"))
        mixed_document = creator.generate()
        mixed_document[b"url-list"] = document[b"url-list"]
        mixed_torrent = root / "mixed.torrent"
        mixed_torrent.write_bytes(lt.bencode(mixed_document))
        client.stop()
        client = Libtorrent(lt, root / "lt-mixed", mixed_torrent, root / "http", args.host)
        client.complete(args.timeout, seed=True)
        records.clear()
        mixed_sources.set()
        uploaded_before = client.poll().total_payload_upload
        engine = Engine(binary, root / "spill-mixed", mixed_torrent, root / "mixed-output",
                        args.host, peers=[client.address(args.host)], web_seeds=True)
        results["mixed"] = engine.complete(args.timeout)
        engine.stop()
        engine = None
        results["mixed_uploaded"] = client.poll().total_payload_upload - uploaded_before
        mixed_requests = list(records)
        assert mixed_requests, "mixed-source run made no HTTP requests"
        assert results["mixed_uploaded"] > 0, "mixed-source run received no peer payload"
        assert results["mixed_uploaded"] < results["mixed"]["received_bytes"], \
            "mixed-source run received no HTTP payload"
        assert digest(root / "mixed-output" / "mixed.bin") == mixed_checksum
        results["mixed_fixture"] = {"length": mixed_length, "piece_length": info[b"piece length"],
                                    "hybrid": b"pieces" in info, "sha256": mixed_checksum}
        for output in ["spill-output", "lt-output", "swarm-output"]:
            for name, checksum in expected.items():
                assert digest(root / output / name) == checksum, (output, name)
        for requests in [spill_requests, lt_requests, swarm_requests, mixed_requests]:
            for request in requests:
                canonical_path = quote(unquote(request["path"]), safe="/")
                assert canonical_path in files, request
                assert request["range"], f"HTTP request missing Range header: {request}"
                match = re.fullmatch(r"bytes=(\d+)-(\d+)", request["range"])
                assert match is not None, f"HTTP request has invalid Range header: {request}"
                start, end = map(int, match.groups())
                assert 0 <= start <= end < files[canonical_path].stat().st_size, request
        # libtorrent uses lowercase percent escapes; both name the same file.
        awkward = "/vectors/odd%20%C3%A9%20space"
        assert awkward in {r["path"] for r in spill_requests}, spill_requests
        assert awkward in {quote(unquote(r["path"]), safe="/") for r in lt_requests}, lt_requests
        results["requests"] = {"spill": spill_requests, "libtorrent": lt_requests,
                               "swarm": swarm_requests, "mixed": mixed_requests}
        return results
    finally:
        if engine is not None:
            engine.stop()
        if client is not None:
            client.stop()
        server.shutdown()
        server.server_close()
        thread.join()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-network", action="store_true")
    parser.add_argument("--host", required=True, help="dev machine LAN IPv4 address")
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--release", action="store_true")
    args = parser.parse_args()
    if not args.allow_network:
        parser.error("requires --allow-network; never run in CI/cloud sessions")
    import libtorrent as lt
    subprocess.run(["cargo", "build", "--locked", "--example", "interop-engine"]
                   + (["--release"] if args.release else []), check=True)
    binary = Path("target", "release" if args.release else "debug", "examples", "interop-engine").resolve()
    parent = Path("target/interop")
    parent.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="lt-webseeds-", dir=parent)).resolve()
    result = {"libtorrent": lt.__version__, "host": args.host,
              "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "modified_paths": subprocess.check_output(["git", "status", "--porcelain"], text=True).splitlines(),
              "binary_sha256": digest(binary), "harness_sha256": digest(Path(__file__)),
              "status": "failed", "cases": {}}
    try:
        for fixture in ["v2.torrent", "v2-leaves.torrent", "hybrid.torrent",
                        "libtorrent-hybrid.torrent"]:
            directory = root / fixture.removesuffix(".torrent")
            directory.mkdir()
            result["cases"][fixture] = case(lt, args, binary, directory, fixture)
        result["status"] = "passed"
    finally:
        (root / "result.json").write_text(json.dumps(result, indent=2))
        print(root / "result.json")


if __name__ == "__main__":
    main()
