#!/usr/bin/env python3
"""Opt-in local comparison of hybrid padding names and Transmission acceptance."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

from libtorrent_v2 import digest, write_payload
from transmission_smoke import Transmission, free_port, process_environment, wait_for


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-network", action="store_true", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--artifacts", type=Path)
    args = parser.parse_args()
    import libtorrent as lt
    if args.release:
        subprocess.run(["cargo", "build", "--release", "--locked"], check=True)
    binary = Path("target/release/spill" if args.release else "target/debug/spill").resolve()
    transmission = shutil.which("transmission-daemon")
    if not transmission:
        parser.error("transmission-daemon is required; this harness never installs packages")
    parent = Path("target/interop").resolve()
    parent.mkdir(parents=True, exist_ok=True)
    root = args.artifacts.resolve() if args.artifacts else Path(tempfile.mkdtemp(prefix="lt-padding-", dir=parent))
    if args.artifacts:
        root.mkdir()
    source = root / "payload" / "copies"
    for name in ("a", "b", "c"):
        write_payload(source / name, 65659)
    entries = [lt.create_file_entry(f"copies/{name}", 65659) for name in ("a", "b", "c")]
    # Neither format-only flag is set: both v1 and v2 commitments are generated.
    creator = lt.create_torrent(entries, 32768, 0)
    lt.set_piece_hashes(creator, str(source.parent))
    native = root / "libtorrent-hybrid.torrent"
    native.write_bytes(lt.bencode(creator.generate()))
    generated = root / "spill-hybrid.torrent"
    create_command = [str(binary), "--json", "create", str(source), "--format", "hybrid", "-d", "-l", "15", "-o", str(generated)]
    created = json.loads(subprocess.check_output(create_command, text=True))
    result = {"revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "libtorrent": lt.__version__, "transmission": subprocess.check_output([transmission, "--version"], text=True, stderr=subprocess.STDOUT).strip(),
              "host": args.host, "binary_sha256": digest(binary), "harness_sha256": digest(Path(__file__)),
              "command": [sys.executable, *sys.argv], "create_command": create_command,
              "libtorrent_creator_flags": 0, "piece_length": 32768, "file_lengths": [65659] * 3,
              "spill_created": created, "cases": {}, "status": "failed"}
    try:
        for name, torrent in (("spill", generated), ("libtorrent", native)):
            info = lt.bdecode(torrent.read_bytes())[b"info"]
            assert info[b"meta version"] == 2 and b"pieces" in info and b"files" in info
            files = [{"length": entry[b"length"], "path": [part.decode() for part in entry[b"path"]],
                      "attr": entry.get(b"attr", b"").decode()} for entry in info[b"files"]]
            paths = ["/".join(entry["path"]) for entry in files if "p" in entry["attr"]]
            (root / f"{name}-files.json").write_text(json.dumps(files, indent=2) + "\n")
            print(name, json.dumps(files), flush=True)
            case = {"torrent": str(torrent), "files": files, "padding_paths": paths,
                    "info_hash_v1": hashlib.sha1(lt.bencode(info)).hexdigest(),
                    "padding_names_match_lengths": all(entry["path"] == [".pad", str(entry["length"])] for entry in files if "p" in entry["attr"])}
            result["cases"][name] = case
            settings = root / f"{name}-config"
            settings.mkdir()
            (settings / "settings.json").write_text(json.dumps({
                "dht-enabled": False, "pex-enabled": False, "lpd-enabled": False,
                "port-forwarding-enabled": False, "utp-enabled": False, "default-trackers": "",
                "rpc-host-whitelist-enabled": False,
            }))
            rpc, peer = free_port("127.0.0.1"), free_port(args.host)
            command = [transmission, "-f", "-g", str(settings), "-p", str(rpc), "-P", str(peer),
                       "-r", "127.0.0.1", "-i", args.host, "-T", "-O", "-Y", "-M", "-B", "-et",
                       "--preferred-transports", "tcp", "--log-level", "debug"]
            log_path = root / f"{name}-transmission.log"
            with log_path.open("w") as log:
                process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                           env=process_environment(root, args.host))
                try:
                    client = Transmission(rpc)
                    wait_for("Transmission RPC", lambda: client.rpc("session-get"), 10)
                    request = {"filename": str(torrent), "paused": True, "download-dir": str(source.parent)}
                    case["rpc_request"] = {"method": "torrent-add", "arguments": request}
                    try:
                        case["rpc_reply"] = client.rpc("torrent-add", **request)
                        case["accepted"] = True
                    except RuntimeError as error:
                        case["accepted"], case["rpc_error"] = False, str(error)
                finally:
                    process.terminate()
                    process.wait(timeout=20)
            case["transmission_command"] = command
            case["transmission_log"] = str(log_path)
            case["duplicate_path_lines"] = [line for line in log_path.read_text(errors="replace").splitlines() if "duplicate path" in line]
            print(name, "accepted:", case["accepted"], case["duplicate_path_lines"], flush=True)
        repeated = {name: sorted(path for path, count in Counter(case["padding_paths"]).items() if count > 1)
                    for name, case in result["cases"].items()}
        result["repeated_paths"] = repeated
        result["same_repeated_padding_names"] = bool(repeated["spill"]) and repeated["spill"] == repeated["libtorrent"]
        result["status"] = "confirmed" if (result["same_repeated_padding_names"]
            and all(case["padding_names_match_lengths"] and not case["accepted"] and case["duplicate_path_lines"]
                    for case in result["cases"].values())) else "different"
    finally:
        (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(result["status"], root / "result.json", flush=True)
    return 0 if result["status"] == "confirmed" else 1


if __name__ == "__main__":
    sys.exit(main())
