//! Single-user catalog with transactional startup migrations.
use crate::{encoding, settings::Settings};
use rusqlite::{
    Connection, OptionalExtension, ToSql, params,
    types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef},
};
use serde::{Deserialize, Serialize};
use spill::{
    engine::{profile::AdmissionError, provisioning::DestinationBinding},
    protocol::identity::{InfoHashV1, InfoHashV2, TorrentIdentity},
};
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
    #[error("torrent already exists")]
    Duplicate,
    #[error(
        "unsupported catalog version {0}; use a compatible spill version or a fresh state directory"
    )]
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
    pub downloaded: u64,
    pub hash_failed_bytes: u64,
    pub hash_failed_pieces: u64,
    pub uploaded: u64,
    pub completed_at: Option<u64>,
    pub last_download_at: Option<u64>,
    pub last_upload_at: Option<u64>,
    pub seed_seconds: f64,
    pub complete: bool,
    pub completed_announces: Vec<([u8; 20], bool)>,
}
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Intent {
    Run,
    Paused,
    Recheck,
    RemoveKeep,
    RemoveDelete,
}
impl ToSql for Intent {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match self {
            Self::Run => "run",
            Self::Paused => "paused",
            Self::Recheck => "recheck",
            Self::RemoveKeep => "remove_keep",
            Self::RemoveDelete => "remove_delete",
        }
        .into())
    }
}
impl FromSql for Intent {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value.as_str()? {
            "run" => Ok(Self::Run),
            "paused" => Ok(Self::Paused),
            "recheck" => Ok(Self::Recheck),
            "remove_keep" => Ok(Self::RemoveKeep),
            "remove_delete" => Ok(Self::RemoveDelete),
            text => Err(FromSqlError::Other(
                format!("invalid torrent intent: {text}").into(),
            )),
        }
    }
}
#[derive(Clone)]
pub struct Row {
    pub id: String,
    pub identity: TorrentIdentity,
    pub metainfo: Option<Vec<u8>>,
    pub parsed: Option<Arc<spill::protocol::metainfo::OwnedMetainfo>>,
    pub magnet: Option<String>,
    pub output_dir: PathBuf,
    pub intent: Intent,
    pub settings: Settings,
    pub checkpoint: Checkpoint,
    pub created_files: Option<DestinationBinding>,
    pub last_error: Option<String>,
    pub retry_count: u32,
    pub retry_at: u64,
    pub added_at: u64,
}
impl Row {
    pub fn hashes(&self) -> impl Iterator<Item = String> {
        [
            self.identity.v1().map(|hash| hash.to_string()),
            self.identity.v2().map(|hash| hash.to_string()),
        ]
        .into_iter()
        .flatten()
    }
    pub fn meta(&self) -> std::io::Result<&spill::protocol::metainfo::OwnedMetainfo> {
        self.parsed.as_deref().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "metainfo is missing or invalid",
            )
        })
    }
}
// Append migrations; published entries must stay unchanged.
const MIGRATIONS: &[&str] = &[
    include_str!("migrations/001.sql"),
    include_str!("migrations/002.sql"),
];

fn migrate(c: &mut Connection) -> Result<()> {
    let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let version: i64 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if !(0..=MIGRATIONS.len() as i64).contains(&version) {
        return Err(Error::Version(version));
    }
    if version == 0 {
        let tables: i64 = tx.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            [],
            |r| r.get(0),
        )?;
        if tables != 0 {
            return Err(Error::Version(version));
        }
    }
    for (index, sql) in MIGRATIONS.iter().enumerate().skip(version as usize) {
        if index == 1 {
            migrate_text_records(&tx)?;
        }
        tx.execute_batch(sql)?;
        tx.pragma_update(None, "user_version", index as i64 + 1)?;
    }
    tx.commit()?;
    Ok(())
}

fn migrate_text_records(c: &Connection) -> Result<()> {
    #[derive(Deserialize)]
    struct LegacyBinding {
        read_only: bool,
        paths: Vec<Vec<u8>>,
    }
    let mut query = c.prepare("SELECT id,created_files,completed_announces FROM torrents")?;
    let rows = query
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<Vec<u8>>>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(query);
    for (id, binding, announces) in rows {
        let binding = binding
            .map(|bytes| -> Result<String> {
                let binding: LegacyBinding = serde_json::from_slice(&bytes)?;
                let paths = binding
                    .paths
                    .into_iter()
                    .map(String::from_utf8)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
                Ok(serde_json::to_string(&serde_json::json!({
                    "read_only": binding.read_only, "paths": paths,
                }))?)
            })
            .transpose()?;
        let announces: Vec<([u8; 20], bool)> = serde_json::from_str(&announces)?;
        c.execute(
            "UPDATE torrents SET created_files=?2,completed_announces=?3 WHERE id=?1",
            params![id, binding, encoding::encode_records(&announces)?],
        )?;
    }
    let nodes: Option<Vec<u8>> = c
        .query_row("SELECT nodes FROM dht WHERE singleton=1", [], |r| r.get(0))
        .optional()?;
    if let Some(nodes) = nodes {
        match serde_json::from_slice::<Vec<([u8; 20], std::net::SocketAddr)>>(&nodes) {
            Ok(nodes) => {
                c.execute(
                    "UPDATE dht SET nodes=?1 WHERE singleton=1",
                    [encoding::encode_records(&nodes)?],
                )?;
            }
            Err(_) => {
                // DHT contacts are a disposable cache, unlike payload authority.
                c.execute("DELETE FROM dht", [])?;
            }
        }
    }
    Ok(())
}

impl Catalog {
    pub fn open(path: &Path) -> Result<Self> {
        let mut c = Connection::open(path)?;
        c.busy_timeout(std::time::Duration::from_secs(5))?;
        c.execute_batch("PRAGMA synchronous=NORMAL; PRAGMA journal_mode=WAL;")?;
        // Keep the WAL on close; normal auto-checkpointing bounds its growth.
        // NORMAL preserves database integrity across power loss, but the latest
        // commits need not survive it (our original catalog durability policy).
        c.set_db_config(
            rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
            true,
        )?;
        migrate(&mut c)?;
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
        let mut query = c.prepare("SELECT id,metainfo,magnet,output_dir,intent,settings,resume,created_files,last_error,retry_count,retry_at,uploaded,seed_seconds,complete,completed_announces,downloaded,added_at,completed_at,last_download_at,last_upload_at,hash_failed_bytes,hash_failed_pieces,v1,v2 FROM torrents ORDER BY rowid")?;
        let raw = query.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<Vec<u8>>>(1)?,
                r.get(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, Intent>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, Vec<u8>>(6)?,
                r.get::<_, Option<String>>(7)?,
                r.get(8)?,
                r.get(9)?,
                r.get(10)?,
                r.get(11)?,
                r.get(12)?,
                r.get(13)?,
                r.get::<_, String>(14)?,
                r.get(15)?,
                r.get(16)?,
                r.get(17)?,
                r.get(18)?,
                r.get(19)?,
                r.get(20)?,
                r.get(21)?,
                r.get::<_, Option<String>>(22)?,
                r.get::<_, Option<String>>(23)?,
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
                completed_announces,
                downloaded,
                added_at,
                completed_at,
                last_download_at,
                last_upload_at,
                hash_failed_bytes,
                hash_failed_pieces,
                v1,
                v2,
            ) = r?;
            let parsed = metainfo
                .as_deref()
                .and_then(|bytes| {
                    spill::engine::profile::Profile::default()
                        .parse_owned(bytes)
                        .ok()
                })
                .map(Arc::new);
            let created_files = created_files
                .map(|text| serde_json::from_str(&text))
                .transpose()?;
            let v1 = v1
                .as_deref()
                .map(encoding::decode_key)
                .transpose()?
                .map(InfoHashV1::from_bytes);
            let v2 = v2
                .as_deref()
                .map(encoding::decode_key)
                .transpose()?
                .map(InfoHashV2::from_bytes);
            let identity = match (v1, v2) {
                (Some(v1), Some(v2)) => TorrentIdentity::Hybrid(v1, v2),
                (Some(v1), None) => TorrentIdentity::V1(v1),
                (None, Some(v2)) => TorrentIdentity::V2(v2),
                (None, None) => unreachable!("catalog identity"),
            };
            Ok(Row {
                id,
                identity,
                metainfo,
                parsed,
                magnet,
                output_dir: output_dir.into(),
                intent,
                settings: serde_json::from_str(&settings)?,
                checkpoint: Checkpoint {
                    resume,
                    downloaded,
                    hash_failed_bytes,
                    hash_failed_pieces,
                    uploaded,
                    completed_at,
                    last_download_at,
                    last_upload_at,
                    seed_seconds,
                    complete,
                    completed_announces: encoding::decode_records(&completed_announces)?,
                },
                created_files,
                last_error,
                retry_count,
                retry_at,
                added_at,
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
            .find(|r| r.hashes().any(|hash| hash == id))
            .cloned())
    }
    pub fn insert(&self, row: &Row) -> Result<()> {
        let mut state = self.0.lock().unwrap();
        state.connection.execute("INSERT INTO torrents(id,metainfo,magnet,output_dir,intent,settings,resume,uploaded,seed_seconds,complete,completed_announces,downloaded,added_at,completed_at,last_download_at,last_upload_at,hash_failed_bytes,hash_failed_pieces,v1,v2) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)",
            params![row.id,row.metainfo,row.magnet,row.output_dir.to_string_lossy(),row.intent,
                serde_json::to_string(&row.settings)?,row.checkpoint.resume,row.checkpoint.uploaded,
                row.checkpoint.seed_seconds,row.checkpoint.complete,encoding::encode_records(&row.checkpoint.completed_announces)?,
                row.checkpoint.downloaded,row.added_at,row.checkpoint.completed_at,
                row.checkpoint.last_download_at,row.checkpoint.last_upload_at,
                row.checkpoint.hash_failed_bytes,row.checkpoint.hash_failed_pieces,
                row.identity.v1().map(|hash| hash.to_string()),row.identity.v2().map(|hash| hash.to_string())])?;
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
                params![id, intent])?;
            row.intent = intent;
            row.last_error = None;
            row.retry_count = 0;
            row.retry_at = 0;
            Ok(())
        })
    }
    pub fn settings(&self, id: &str, settings: &Settings) -> Result<()> {
        self.update(id, |c, row| {
            if row.settings.file_priorities != settings.file_priorities {
                row.checkpoint.complete = false;
            }
            c.execute("UPDATE torrents SET settings=?2,last_error=NULL,retry_count=0,retry_at=0,complete=?3 WHERE id=?1",
                params![id, serde_json::to_string(settings)?, row.checkpoint.complete])?;
            row.settings = settings.clone();
            row.last_error = None;
            row.retry_count = 0;
            row.retry_at = 0;
            Ok(())
        })
    }
    pub fn selection(&self, id: &str, settings: &Settings, magnet: Option<&str>) -> Result<()> {
        self.update(id, |c, row| {
            row.settings = settings.clone();
            row.magnet = magnet.map(str::to_owned);
            row.checkpoint.complete = false;
            row.last_error = None;
            row.retry_count = 0;
            row.retry_at = 0;
            c.execute("UPDATE torrents SET settings=?2,magnet=?3,complete=0,last_error=NULL,retry_count=0,retry_at=0 WHERE id=?1",
                params![id, serde_json::to_string(&row.settings)?, row.magnet])?;
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
                tracing::info!(torrent_id = id, "torrent_recovered");
                row.last_error = None;
                row.retry_count = 0;
                row.retry_at = 0;
            }
            Ok(())
        })
    }
    pub fn resolved(&self, id: &str, meta: spill::protocol::metainfo::OwnedMetainfo) -> Result<()> {
        self.update(id, |c, row| {
            if row.metainfo.is_none()
                && row.settings.file_priorities.is_empty()
                && let Some(uri) = &row.magnet
            {
                let magnet = spill::protocol::magnet::Magnet::parse(uri, Default::default())
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
                if let Some(ranges) = magnet.select_only() {
                    row.settings.file_priorities =
                        spill::engine::selection::FileSelection::select_only(meta.layout(), ranges)
                            .file_priorities()
                            .to_vec();
                }
            }
            let tx = c.unchecked_transaction()?;
            learn_identity(&tx, row, meta.info_hash())?;
            let bytes = meta.source_bytes();
            tx.execute(
                "UPDATE torrents SET metainfo=?2,settings=?3 WHERE id=?1",
                params![id, bytes, serde_json::to_string(&row.settings)?],
            )?;
            tx.commit()?;
            row.metainfo = Some(bytes.to_vec());
            row.parsed = Some(Arc::new(meta));
            Ok(())
        })
    }
    pub fn bind(&self, id: &str, binding: &DestinationBinding) -> Result<()> {
        self.update(id, |c, row| {
            if row.created_files.as_ref() != Some(binding) {
                c.execute(
                    "UPDATE torrents SET created_files=?2 WHERE id=?1",
                    params![id, serde_json::to_string(binding)?],
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
            if row.checkpoint.complete && !previous.complete {
                row.checkpoint.completed_at = Some(now());
            }
            if row.checkpoint != previous {
                let tx = c.unchecked_transaction()?;
                if row.checkpoint.completed_announces != previous.completed_announces {
                    tx.execute(
                        "UPDATE torrents SET completed_announces=?2 WHERE id=?1",
                        params![id, encoding::encode_records(&row.checkpoint.completed_announces)?],
                    )?;
                }
                // Accounting-only updates must not bind or serialize the resume blob.
                if row.checkpoint.resume != previous.resume {
                    tx.execute(
                        "UPDATE torrents SET resume=?2 WHERE id=?1",
                        params![id, row.checkpoint.resume],
                    )?;
                }
                if row.checkpoint.hash_failed_bytes != previous.hash_failed_bytes
                    || row.checkpoint.hash_failed_pieces != previous.hash_failed_pieces
                    || row.checkpoint.downloaded != previous.downloaded
                    || row.checkpoint.completed_at != previous.completed_at
                    || row.checkpoint.last_download_at != previous.last_download_at
                    || row.checkpoint.last_upload_at != previous.last_upload_at
                    || row.checkpoint.uploaded != previous.uploaded
                    || row.checkpoint.seed_seconds != previous.seed_seconds
                    || row.checkpoint.complete != previous.complete
                {
                    tx.execute(
                        "UPDATE torrents SET uploaded=?2,seed_seconds=?3,complete=?4,downloaded=?5,completed_at=?6,last_download_at=?7,last_upload_at=?8,hash_failed_bytes=?9,hash_failed_pieces=?10 WHERE id=?1",
                        params![
                            id,
                            row.checkpoint.uploaded,
                            row.checkpoint.seed_seconds,
                            row.checkpoint.complete,
                            row.checkpoint.downloaded,
                            row.checkpoint.completed_at,
                            row.checkpoint.last_download_at,
                            row.checkpoint.last_upload_at,
                            row.checkpoint.hash_failed_bytes,
                            row.checkpoint.hash_failed_pieces
                        ],
                    )?;
                }
                tx.commit()?;
            }
            Ok(())
        })
    }
    pub fn failure(&self, id: &str, error: &std::io::Error) -> Result<()> {
        self.update(id, |c, row| {
            if AdmissionError::of(error).is_some() {
                row.intent = Intent::Paused;
                row.retry_count = 0;
                row.retry_at = 0;
            } else {
                row.retry_count = row.retry_count.saturating_add(1);
                let delay = (5u64.saturating_mul(1u64 << row.retry_count.min(9))).min(1800);
                row.retry_at = now() + delay;
            }
            let message = error.to_string();
            let changed = row.last_error.as_ref() != Some(&message);
            row.last_error = Some(message);
            c.execute(
                "UPDATE torrents SET last_error=?2,retry_count=?3,retry_at=?4,intent=?5 WHERE id=?1",
                params![id, row.last_error, row.retry_count, row.retry_at, row.intent],
            )?;
            if changed {
                tracing::warn!(torrent_id = id, %error, retry_at = row.retry_at, "torrent_failed");
            }
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
    pub fn load_dht(&self) -> Result<Option<spill::engine::session::dht::SavedState>> {
        let c = self.0.lock().unwrap();
        let saved: Option<(Vec<u8>, String)> = c
            .connection
            .query_row("SELECT node_id,nodes FROM dht WHERE singleton=1", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .optional()?;
        Ok(saved.and_then(|(id, nodes)| {
            let nodes: Vec<([u8; 20], std::net::SocketAddr)> =
                encoding::decode_records(&nodes).ok()?;
            let saved = spill::engine::session::dht::SavedState {
                id: spill::protocol::dht::NodeId(id.try_into().ok()?),
                nodes: nodes
                    .into_iter()
                    .map(|(id, address)| spill::protocol::dht::Contact {
                        id: spill::protocol::dht::NodeId(id),
                        address,
                    })
                    .collect(),
            };
            saved.validate().ok()?;
            Some(saved)
        }))
    }
    pub fn save_dht(&self, state: spill::engine::session::dht::SavedState) -> Result<()> {
        let nodes = encoding::encode_records(
            &state
                .nodes
                .iter()
                .map(|n| (n.id.0, n.address))
                .collect::<Vec<_>>(),
        )?;
        self.0.lock().unwrap().connection.execute("INSERT INTO dht VALUES(1,?1,?2) ON CONFLICT(singleton) DO UPDATE SET node_id=excluded.node_id,nodes=excluded.nodes WHERE node_id IS NOT excluded.node_id OR nodes IS NOT excluded.nodes", params![state.id.0.as_slice(),nodes])?;
        Ok(())
    }
}
fn learn_identity(c: &Connection, row: &mut Row, identity: TorrentIdentity) -> Result<()> {
    let v1 = identity.v1().map(|hash| hash.to_string());
    let v2 = identity.v2().map(|hash| hash.to_string());
    if row.identity == identity {
        return Ok(());
    }
    let duplicate: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM torrents WHERE id!=?1 AND (v1=?2 OR v2=?3))",
        params![row.id, v1, v2],
        |r| r.get(0),
    )?;
    if duplicate {
        return Err(Error::Duplicate);
    }
    c.execute(
        "UPDATE torrents SET v1=?2,v2=?3 WHERE id=?1",
        params![row.id, v1, v2],
    )?;
    row.identity = identity;
    Ok(())
}

pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
