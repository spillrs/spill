# BEP 52 vectors

`v2.torrent` and `hybrid.torrent` were generated with the BEP 52 reference
creator, not spill. They contain identical deterministic payloads and use
32 KiB pieces. Payload byte `i` is `i % 251`, starting again at zero per file:

| Path | Bytes |
| --- | ---: |
| `block` | 16384 |
| `empty` | 0 |
| `odd` | 65659 |
| `one` | 1 |
| `piece` | 32768 |

The fixtures pin original-info identities, short-file tree heights, a short
final leaf, an odd piece count, piece-layer padding and hybrid padding. Test
payloads are generated in memory; no payload files are required.

## Reproduce

Requires Python 3 only. From the repository root:

```sh
curl -fsSL https://raw.githubusercontent.com/bittorrent/bittorrent.org/master/beps/bep_0052_torrent_creator.py -o /tmp/bep52_creator.py
python3 - <<'PY'
import hashlib
import importlib.util
from pathlib import Path
import tempfile

source = Path('/tmp/bep52_creator.py')
assert hashlib.sha256(source.read_bytes()).hexdigest() == 'd28ede84d078ad214558f23d2381efbc1465bd3c9ad175b4839477910ba6546e'
spec = importlib.util.spec_from_file_location('creator', source)
creator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(creator)
with tempfile.TemporaryDirectory() as temporary:
    root = Path(temporary) / 'vectors'
    root.mkdir()
    for name, length in [('empty', 0), ('one', 1), ('block', 16384),
                         ('piece', 32768), ('odd', 65659)]:
        (root / name).write_bytes(bytes(i % 251 for i in range(length)))
    for name, hybrid in [('v2', False), ('hybrid', True)]:
        torrent = creator.Torrent(str(root), 32768)
        wire = creator.encode(torrent.create('http://example.invalid/announce', hybrid=hybrid))
        (Path('tests/protocol/fixtures') / (name + '.torrent')).write_bytes(wire)
        print(name, torrent.info_hash_v1(), torrent.info_hash_v2())
PY
```

The source digest pins the reference implementation if the upstream URL changes.
These protocol tests never contact the tracker or run the generator.

## Independent libtorrent hybrid

`libtorrent-hybrid.torrent` was generated with the Python bindings for
libtorrent 2.0.11 (`lt.version == '2.0.11.0'`). It uses the same files plus
`tail`, 123 bytes with the same payload pattern. Libtorrent pads that final file
to the piece boundary and repeats an earlier padding filename. The fixture
protects both trailing-padding handling and padding-name independence.

Reproduce using a Python interpreter with those bindings available:

```python
import hashlib
import libtorrent as lt
from pathlib import Path
import tempfile

assert lt.version == '2.0.11.0'
with tempfile.TemporaryDirectory() as temporary:
    base = Path(temporary)
    (base / 'vectors').mkdir()
    files = lt.file_storage()
    for name, length in [('block', 16384), ('empty', 0), ('odd', 65659),
                         ('one', 1), ('piece', 32768), ('tail', 123)]:
        (base / 'vectors' / name).write_bytes(bytes(i % 251 for i in range(length)))
        files.add_file('vectors/' + name, length)
    torrent = lt.create_torrent(files, 32768)
    torrent.add_tracker('http://example.invalid/announce')
    lt.set_piece_hashes(torrent, temporary)
    document = torrent.generate()
    document.pop(b'creation date', None)
    document.pop(b'created by', None)
    wire = lt.bencode(document)
    Path('tests/protocol/fixtures/libtorrent-hybrid.torrent').write_bytes(wire)
    info = lt.bencode(document[b'info'])
    print(hashlib.sha1(info).hexdigest(), hashlib.sha256(info).hexdigest())
```

No libtorrent runtime or build dependency is needed to run spill or its tests.

## Direct-peer lifecycle vectors

Additional v2-only fixtures use the same pinned reference creator and byte pattern:

| Fixture | Files (bytes) | Piece length | SHA-256 infohash |
| --- | --- | ---: | --- |
| `v2-leaves.torrent` | Same `vectors` tree as above | 16384 | `72ac7438b4b06d3ed91f66f1fd592699f467304e084d8c36d83957d01de532ff` |
| `v2-large.torrent` | Single `large` (16777339) | 16777216 | `b5b966d2a5f2e8bf6c83387862baf1515bc4c46a5ca0aed6ac96e45a8d4c64b9` |
| `v2-nested.torrent` | `nested/dir/file` (1) | 32768 | `f50efbe0ab408011383a07794c293106f53bc66af722898636298a7d1e86f142` |
| `v2-single.torrent` | Single `single` (1) | 32768 | `5191b84376701e8b06b92b7572c78c6a879cb9b878d0bd86f325bc3bef68dcab` |
| `v2-empty.torrent` | `empty/a` (0), `empty/b` (0) | 32768 | `4c928ef499bb9cef0aca72b649ddf14a50cd032311aa8683e63196948d2dec15` |
| `v2-copies.torrent` | Identical `copies/a`, `copies/b`, `copies/c` (65659 each) | 32768 | `7b42148a622a20bb8935da312b421d4e7bd7f5fa233e0358dae950fdc7f830fc` |

After loading `creator` as above, reproduce with:

```python
with tempfile.TemporaryDirectory() as temporary:
    root = Path(temporary)
    for name, files, piece_length in [
        ('v2-leaves', [('vectors/block', 16384), ('vectors/empty', 0),
                       ('vectors/odd', 65659), ('vectors/one', 1),
                       ('vectors/piece', 32768)], 16384),
        ('v2-large', [('large', 16 * 1024 * 1024 + 123)], 16 * 1024 * 1024),
        ('v2-nested', [('nested/dir/file', 1)], 32768),
        ('v2-single', [('single', 1)], 32768),
        ('v2-empty', [('empty/a', 0), ('empty/b', 0)], 32768),
        ('v2-copies', [('copies/a', 65659), ('copies/b', 65659), ('copies/c', 65659)], 32768),
    ]:
        for path, length in files:
            file = root / path
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_bytes((bytes(range(251)) * (length // 251 + 1))[:length])
        torrent = creator.Torrent(str(root / files[0][0].split('/')[0]), piece_length)
        wire = creator.encode(torrent.create('http://example.invalid/announce', hybrid=False))
        (Path('tests/protocol/fixtures') / (name + '.torrent')).write_bytes(wire)
        print(name, torrent.info_hash_v2())
```

The large piece exceeds the engine's live-block retention budget, exercising
streaming read-back verification without a checked-in payload file.
