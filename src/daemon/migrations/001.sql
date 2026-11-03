CREATE TABLE torrents (
    id TEXT PRIMARY KEY, metainfo BLOB, magnet TEXT, output_dir TEXT NOT NULL,
    intent TEXT NOT NULL, settings TEXT NOT NULL CHECK(json_valid(settings)),
    resume BLOB NOT NULL, downloaded INTEGER NOT NULL DEFAULT 0,
    hash_failed_bytes INTEGER NOT NULL DEFAULT 0,
    hash_failed_pieces INTEGER NOT NULL DEFAULT 0,
    uploaded INTEGER NOT NULL DEFAULT 0, added_at INTEGER NOT NULL,
    completed_at INTEGER, last_download_at INTEGER, last_upload_at INTEGER,
    seed_seconds REAL NOT NULL DEFAULT 0, complete INTEGER NOT NULL DEFAULT 0,
    completed_announces TEXT NOT NULL CHECK(json_valid(completed_announces)),
    v1 TEXT UNIQUE, v2 TEXT UNIQUE,
    created_files BLOB, last_error TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0, retry_at INTEGER NOT NULL DEFAULT 0,
    CHECK(metainfo IS NOT NULL OR magnet IS NOT NULL),
    CHECK(v1 IS NOT NULL OR v2 IS NOT NULL));
CREATE TABLE dht (singleton INTEGER PRIMARY KEY CHECK(singleton=1), node_id BLOB NOT NULL, nodes BLOB NOT NULL);
