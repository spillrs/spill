//! Pre-beta catalog. No legacy schema migrations or removal receipts.
use engine::{provisioning::DestinationBinding, settings::TorrentSettings};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("catalog: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("catalog data: {0}")]
    Json(#[from] serde_json::Error),
    #[error("catalog payload: {0}")]
    Payload(#[from] std::io::Error),
    #[error("unsupported catalog version {0}; use a fresh state directory")]
    Version(i64),
}
type Result<T> = std::result::Result<T, Error>;
#[derive(Clone)]
pub struct Catalog(Arc<Mutex<State>>);
struct State {
    connection: Connection,
    rows: Vec<Arc<Row>>,
}
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Checkpoint {
    pub resume: Vec<u8>,
    pub uploaded: u64,
    pub seed_seconds: f64,
    pub complete: bool,
    pub completed_announce: Option<bool>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Intent {
    Run,
    Paused,
    Recheck,
    RemoveKeep,
    RemoveDelete,
}
#[derive(Clone)]
pub struct Row {
    pub id: String,
    pub metainfo: Option<Vec<u8>>,
    pub parsed: Option<Arc<protocol::metainfo::OwnedMetainfo>>,
    pub magnet: Option<String>,
    pub output_dir: PathBuf,
    pub intent: Intent,
    pub settings: TorrentSettings,
    pub checkpoint: Checkpoint,
    pub created_files: Option<DestinationBinding>,
    pub last_error: Option<String>,
    pub retry_count: u32,
    pub retry_at: u64,
}
impl Row {
    pub fn meta(&self) -> std::io::Result<&protocol::metainfo::OwnedMetainfo> {
        self.parsed.as_deref().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "metainfo is missing or invalid",
            )
        })
    }
}
impl Catalog {
    pub fn open(path: &Path) -> Result<Self> {
        let c = Connection::open(path)?;
        c.busy_timeout(std::time::Duration::from_secs(5))?;
        c.execute_batch("PRAGMA synchronous=NORMAL; PRAGMA journal_mode=WAL;")?;
        // Keep the WAL on close; normal auto-checkpointing bounds its growth.
        // NORMAL preserves database integrity across power loss, but the latest
        // commits need not survive it (our original catalog durability policy).
        c.set_db_config(
            rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
            true,
        )?;
        let version: i64 = c.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version == 0 {
            let tables: i64 = c.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                [],
                |r| r.get(0),
            )?;
            if tables != 0 {
                return Err(Error::Version(version));
            }
            c.execute_batch("BEGIN;
                CREATE TABLE torrents (
                    id TEXT PRIMARY KEY, metainfo BLOB, magnet TEXT, output_dir TEXT NOT NULL,
                    intent TEXT NOT NULL, settings TEXT NOT NULL CHECK(json_valid(settings)),
                    resume BLOB NOT NULL, uploaded INTEGER NOT NULL DEFAULT 0,
                    seed_seconds REAL NOT NULL DEFAULT 0, complete INTEGER NOT NULL DEFAULT 0,
                    completed_announce INTEGER,
                    created_files BLOB, last_error TEXT,
                    retry_count INTEGER NOT NULL DEFAULT 0, retry_at INTEGER NOT NULL DEFAULT 0,
                    CHECK(metainfo IS NOT NULL OR magnet IS NOT NULL));
                CREATE TABLE dht (singleton INTEGER PRIMARY KEY CHECK(singleton=1), node_id BLOB NOT NULL, nodes BLOB NOT NULL);
                PRAGMA user_version=3; COMMIT;")?;
        } else if version != 3 {
            return Err(Error::Version(version));
        }
        c.prepare("SELECT settings, resume, uploaded, seed_seconds, complete, created_files, retry_at FROM torrents LIMIT 0")?;
        let rows = Self::read_rows(&c)?.into_iter().map(Arc::new).collect();
        Ok(Self(Arc::new(Mutex::new(State {
            connection: c,
            rows,
        }))))
    }
    pub fn rows(&self) -> Result<Vec<Arc<Row>>> {
        Ok(self.0.lock().unwrap().rows.clone())
    }
    fn read_rows(c: &Connection) -> Result<Vec<Row>> {
        let mut query = c.prepare("SELECT id,metainfo,magnet,output_dir,intent,settings,resume,created_files,last_error,retry_count,retry_at,uploaded,seed_seconds,complete,completed_announce FROM torrents ORDER BY rowid")?;
        let raw = query.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<Vec<u8>>>(1)?,
                r.get(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, Vec<u8>>(6)?,
                r.get::<_, Option<Vec<u8>>>(7)?,
                r.get(8)?,
                r.get(9)?,
                r.get(10)?,
                r.get(11)?,
                r.get(12)?,
                r.get(13)?,
                r.get(14)?,
            ))
        })?;
        raw.map(|r| {
            let (
                id,
                metainfo,
                magnet,
                output_dir,
                intent,
                settings,
                resume,
                created_files,
                last_error,
                retry_count,
                retry_at,
                uploaded,
                seed_seconds,
                complete,
                completed_announce,
            ) = r?;
            let parsed = metainfo
                .as_deref()
                .and_then(|bytes| engine::profile::Profile::default().parse_owned(bytes).ok())
                .map(Arc::new);
            let created_files = created_files
                .map(|bytes| serde_json::from_slice(&bytes))
                .transpose()?;
            Ok(Row {
                id,
                metainfo,
                parsed,
                magnet,
                output_dir: output_dir.into(),
                intent: serde_json::from_str(&intent)?,
                settings: serde_json::from_str(&settings)?,
                checkpoint: Checkpoint {
                    resume,
                    uploaded,
                    seed_seconds,
                    complete,
                    completed_announce,
                },
                created_files,
                last_error,
                retry_count,
                retry_at,
            })
        })
        .collect()
    }
    pub fn row(&self, id: &str) -> Result<Option<Arc<Row>>> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .rows
            .iter()
            .find(|r| r.id == id)
            .cloned())
    }
    pub fn insert(&self, row: &Row) -> Result<()> {
        let mut state = self.0.lock().unwrap();
        state.connection.execute("INSERT INTO torrents(id,metainfo,magnet,output_dir,intent,settings,resume,uploaded,seed_seconds,complete,completed_announce) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![row.id,row.metainfo,row.magnet,row.output_dir.to_string_lossy(),serde_json::to_string(&row.intent)?,
                serde_json::to_string(&row.settings)?,row.checkpoint.resume,row.checkpoint.uploaded,
                row.checkpoint.seed_seconds,row.checkpoint.complete,row.checkpoint.completed_announce])?;
        state.rows.push(Arc::new(row.clone()));
        Ok(())
    }
    // Mutate a copy and publish only after the SQL write succeeds. All catalog
    // clones (including checkpoint callbacks) share the same lock and cache.
    fn update(
        &self,
        id: &str,
        change: impl FnOnce(&Connection, &mut Row) -> Result<()>,
    ) -> Result<()> {
        let mut state = self.0.lock().unwrap();
        let index = state
            .rows
            .iter()
            .position(|r| r.id == id)
            .ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        let mut row = state.rows[index].as_ref().clone();
        change(&state.connection, &mut row)?;
        state.rows[index] = Arc::new(row);
        Ok(())
    }
    pub fn intent(&self, id: &str, intent: Intent) -> Result<()> {
        self.update(id, |c, row| {
            c.execute("UPDATE torrents SET intent=?2,last_error=NULL,retry_count=0,retry_at=0 WHERE id=?1",
                params![id, serde_json::to_string(&intent)?])?;
            row.intent = intent;
            row.last_error = None;
            row.retry_count = 0;
            row.retry_at = 0;
            Ok(())
        })
    }
    pub fn settings(&self, id: &str, settings: &TorrentSettings) -> Result<()> {
        self.update(id, |c, row| {
            c.execute("UPDATE torrents SET settings=?2,last_error=NULL,retry_count=0,retry_at=0 WHERE id=?1",
                params![id, serde_json::to_string(settings)?])?;
            row.settings = settings.clone();
            row.last_error = None;
            row.retry_count = 0;
            row.retry_at = 0;
            Ok(())
        })
    }
    pub fn recovered(&self, id: &str) -> Result<()> {
        self.update(id, |c, row| {
            if row.last_error.is_some() {
                c.execute(
                    "UPDATE torrents SET last_error=NULL,retry_count=0,retry_at=0 WHERE id=?1",
                    [id],
                )?;
                row.last_error = None;
                row.retry_count = 0;
                row.retry_at = 0;
            }
            Ok(())
        })
    }
    pub fn resolved(&self, id: &str, meta: protocol::metainfo::OwnedMetainfo) -> Result<()> {
        self.update(id, |c, row| {
            let bytes = meta.source_bytes();
            if row.metainfo.as_deref() != Some(bytes) {
                c.execute(
                    "UPDATE torrents SET metainfo=?2 WHERE id=?1",
                    params![id, bytes],
                )?;
                row.metainfo = Some(bytes.to_vec());
            }
            row.parsed = Some(Arc::new(meta));
            Ok(())
        })
    }
    pub fn bind(&self, id: &str, binding: &DestinationBinding) -> Result<()> {
        self.update(id, |c, row| {
            if row.created_files.as_ref() != Some(binding) {
                c.execute(
                    "UPDATE torrents SET created_files=?2 WHERE id=?1",
                    params![id, serde_json::to_vec(binding)?],
                )?;
                row.created_files = Some(binding.clone());
            }
            Ok(())
        })
    }
    pub fn checkpoint(&self, id: &str, update: impl FnOnce(&mut Checkpoint)) -> Result<()> {
        self.update(id, |c, row| {
            let previous = row.checkpoint.clone();
            update(&mut row.checkpoint);
            if row.checkpoint != previous {
                let tx = c.unchecked_transaction()?;
                if row.checkpoint.completed_announce != previous.completed_announce {
                    tx.execute(
                        "UPDATE torrents SET completed_announce=?2 WHERE id=?1",
                        params![id, row.checkpoint.completed_announce],
                    )?;
                }
                // Accounting-only updates must not bind or serialize the resume blob.
                if row.checkpoint.resume != previous.resume {
                    tx.execute(
                        "UPDATE torrents SET resume=?2 WHERE id=?1",
                        params![id, row.checkpoint.resume],
                    )?;
                }
                if row.checkpoint.uploaded != previous.uploaded
                    || row.checkpoint.seed_seconds != previous.seed_seconds
                    || row.checkpoint.complete != previous.complete
                {
                    tx.execute(
                        "UPDATE torrents SET uploaded=?2,seed_seconds=?3,complete=?4 WHERE id=?1",
                        params![
                            id,
                            row.checkpoint.uploaded,
                            row.checkpoint.seed_seconds,
                            row.checkpoint.complete
                        ],
                    )?;
                }
                tx.commit()?;
            }
            Ok(())
        })
    }
    pub fn failure(&self, id: &str, error: &str) -> Result<()> {
        self.update(id, |c, row| {
            row.retry_count = row.retry_count.saturating_add(1);
            let delay = (5u64.saturating_mul(1u64 << row.retry_count.min(9))).min(1800);
            row.retry_at = now() + delay;
            row.last_error = Some(error.into());
            c.execute(
                "UPDATE torrents SET last_error=?2,retry_count=?3,retry_at=?4 WHERE id=?1",
                params![id, error, row.retry_count, row.retry_at],
            )?;
            Ok(())
        })
    }
    pub fn delete(&self, id: &str) -> Result<()> {
        let mut state = self.0.lock().unwrap();
        state
            .connection
            .execute("DELETE FROM torrents WHERE id=?1", [id])?;
        state.rows.retain(|r| r.id != id);
        Ok(())
    }
    pub fn load_dht(&self) -> Result<Option<engine::session::dht::SavedState>> {
        let c = self.0.lock().unwrap();
        let saved: Option<(Vec<u8>, Vec<u8>)> = c
            .connection
            .query_row("SELECT node_id,nodes FROM dht WHERE singleton=1", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .optional()?;
        Ok(saved.and_then(|(id, nodes)| {
            if nodes.len() % 26 != 0 || nodes.len() > engine::session::dht::MAX_SAVED_NODES * 26 {
                return None;
            }
            Some(engine::session::dht::SavedState {
                id: protocol::dht::NodeId(id.try_into().ok()?),
                nodes: nodes
                    .as_chunks::<26>()
                    .0
                    .iter()
                    .map(|n| protocol::dht::krpc::NodeV4 {
                        id: protocol::dht::NodeId(n[..20].try_into().unwrap()),
                        address: std::net::SocketAddrV4::new(
                            std::net::Ipv4Addr::new(n[20], n[21], n[22], n[23]),
                            u16::from_be_bytes([n[24], n[25]]),
                        ),
                    })
                    .collect(),
            })
        }))
    }
    pub fn save_dht(&self, state: engine::session::dht::SavedState) -> Result<()> {
        let nodes: Vec<u8> = state
            .nodes
            .iter()
            .take(engine::session::dht::MAX_SAVED_NODES)
            .flat_map(protocol::dht::krpc::NodeV4::encode)
            .collect();
        self.0.lock().unwrap().connection.execute("INSERT INTO dht VALUES(1,?1,?2) ON CONFLICT(singleton) DO UPDATE SET node_id=excluded.node_id,nodes=excluded.nodes WHERE node_id IS NOT excluded.node_id OR nodes IS NOT excluded.nodes", params![state.id.0.as_slice(),nodes])?;
        Ok(())
    }
}
pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
