//! One in-process dispatcher shared by Unix control and HTTP.
use crate::settings::Settings;
use crate::{
    catalog::{Catalog, Checkpoint, Intent, Row},
    supervisor::Supervisor,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use spill::engine::selection::FileSelection;
use std::{io, path::PathBuf, sync::Arc};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::Mutex,
};

mod metainfo_base64 {
    use base64::{Engine, prelude::BASE64_STANDARD};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        bytes: &Option<Vec<u8>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        bytes
            .as_ref()
            .map(|bytes| BASE64_STANDARD.encode(bytes))
            .serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|text| {
                BASE64_STANDARD
                    .decode(text)
                    .map_err(serde::de::Error::custom)
            })
            .transpose()
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Add {
        #[serde(default, with = "metainfo_base64")]
        metainfo: Option<Vec<u8>>,
        magnet: Option<String>,
        url: Option<String>,
        output_dir: Option<PathBuf>,
    },
    Pause {
        id: String,
    },
    Resume {
        id: String,
    },
    Remove {
        id: String,
        #[serde(default)]
        delete_payload: bool,
    },
    Recheck {
        id: String,
    },
    Reannounce {
        id: String,
    },
    Get {
        id: String,
    },
    Set {
        id: String,
        values: Map<String, Value>,
    },
    List,
    Status,
    Files {
        id: String,
    },
    Health,
    Shutdown,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub code: u8,
    pub data: Value,
    pub error: Option<String>,
}
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Invalid(String),
    #[error("torrent not found: {0}")]
    Missing(String),
    #[error(transparent)]
    Catalog(#[from] crate::catalog::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
impl Response {
    fn from_result(result: Result<Value, Error>) -> Self {
        match result {
            Ok(data) => Self {
                code: 0,
                data,
                error: None,
            },
            Err(e) => Self {
                code: match e {
                    Error::Missing(_) => 3,
                    Error::Invalid(_) | Error::Json(_) => 2,
                    Error::Io(ref error)
                        if matches!(
                            error.kind(),
                            io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData
                        ) =>
                    {
                        2
                    }
                    _ => 1,
                },
                data: Value::Null,
                error: Some(e.to_string()),
            },
        }
    }
}
#[derive(Clone)]
pub struct Control(pub Arc<Mutex<Supervisor>>);
fn find(db: &Catalog, id: &str) -> Result<Arc<Row>, Error> {
    let rows: Vec<_> = db
        .rows()?
        .into_iter()
        .filter(|r| r.id.starts_with(id))
        .collect();
    match rows.len() {
        0 => Err(Error::Missing(id.into())),
        1 => Ok(rows.into_iter().next().unwrap()),
        _ => Err(Error::Invalid("ambiguous ID".into())),
    }
}
impl Control {
    pub async fn handle(&self, request: Request) -> Response {
        Response::from_result(self.dispatch(request).await)
    }
    async fn dispatch(&self, mut request: Request) -> Result<Value, Error> {
        if let Request::Add {
            metainfo,
            magnet,
            url,
            output_dir,
        } = &mut request
        {
            if usize::from(metainfo.is_some())
                + usize::from(magnet.is_some())
                + usize::from(url.is_some())
                != 1
            {
                return Err(Error::Invalid(
                    "provide exactly one of metainfo, magnet, or url".into(),
                ));
            }
            if output_dir.as_ref().is_some_and(|path| !path.is_absolute()) {
                return Err(Error::Invalid("output_dir must be absolute".into()));
            }
            if let Some(source) = url.take() {
                let mut shutdown = self.0.lock().await.shutdown.subscribe();
                if *shutdown.borrow() {
                    return Err(Error::Invalid("daemon is shutting down".into()));
                }
                // Network I/O must not hold up other control requests.
                *metainfo = Some(tokio::select! {
                    result = crate::metainfo_url::fetch(&source) => result?,
                    _ = shutdown.changed() => return Err(Error::Invalid("daemon is shutting down".into())),
                });
            }
        }
        let mut supervisor = self.0.lock().await;
        if *supervisor.shutdown.borrow() && !matches!(request, Request::Health | Request::Status) {
            return Err(Error::Invalid("daemon is shutting down".into()));
        }
        match request {
            Request::Health => Ok(supervisor.health()),
            Request::Shutdown => {
                supervisor.shutdown.send_replace(true);
                Ok(json!({"stopping":true}))
            }
            Request::List | Request::Status => {
                let rows = supervisor.db.rows()?;
                Ok(
                    json!({"health":supervisor.health(),"torrents":rows.iter().map(|r|supervisor.view(r)).collect::<Vec<_>>()}),
                )
            }
            Request::Add {
                metainfo,
                magnet,
                url: _,
                output_dir,
            } => {
                let parsed = Default::default();
                let mut row = Row {
                    id: String::new(),
                    metainfo,
                    parsed,
                    magnet,
                    output_dir: output_dir
                        .unwrap_or_else(|| supervisor.config.download_dir.clone()),
                    intent: Intent::Run,
                    settings: supervisor.config.defaults.clone(),
                    checkpoint: Checkpoint::default(),
                    created_files: None,
                    last_error: None,
                    retry_count: 0,
                    retry_at: 0,
                    added_at: crate::catalog::now(),
                };
                let id = match (&row.metainfo, &row.magnet) {
                    (Some(bytes), None) if bytes.len() <= spill::engine::profile::MAX_META => {
                        let meta = supervisor
                            .config
                            .profile()
                            .parse_owned(bytes)
                            .map_err(io::Error::from)?;
                        if meta.is_private() {
                            row.settings.dht = false;
                            row.settings.pex = false;
                        }
                        let id = meta.info_hash().to_string();
                        row.parsed = Some(Arc::new(meta));
                        id
                    }
                    (None, Some(uri)) => {
                        spill::protocol::magnet::Magnet::parse(uri, Default::default())
                            .map_err(|e| Error::Invalid(e.to_string()))?
                            .info_hash()
                            .to_string()
                    }
                    (Some(_), None) => {
                        return Err(Error::Invalid("metainfo exceeds 16 MiB".into()));
                    }
                    _ => unreachable!("add source validated before dispatch"),
                };
                if supervisor.db.row(&id)?.is_some() {
                    return Err(Error::Invalid("torrent already exists".into()));
                }
                std::fs::create_dir_all(&row.output_dir)?;
                row.id = id.clone();
                supervisor.db.insert(&row)?;
                supervisor.reconcile().await?;
                Ok(json!({"id":id}))
            }
            Request::Reannounce { id } => {
                let row = find(&supervisor.db, &id)?;
                if !row.settings.trackers {
                    return Err(Error::Invalid("trackers are disabled".into()));
                }
                supervisor.reannounce(row.meta()?.info_hash()).await?;
                Ok(json!({}))
            }
            Request::Get { id } => {
                let row = find(&supervisor.db, &id)?;
                Ok(supervisor.view(&row))
            }
            Request::Set { id, values } => {
                let row = find(&supervisor.db, &id)?;
                let mut settings = serde_json::to_value(&row.settings)?;
                settings.as_object_mut().unwrap().extend(values);
                let settings: Settings = serde_json::from_value(settings)?;
                settings.validate()?;
                if row.parsed.as_ref().is_some_and(|meta| meta.is_private())
                    && (settings.dht || settings.pex)
                {
                    return Err(Error::Invalid(
                        "DHT and PEX are disabled for private torrents".into(),
                    ));
                }
                if settings.dht && !supervisor.config.listen.is_ipv4() {
                    return Err(Error::Invalid("DHT requires an IPv4 listen address".into()));
                }
                if !settings.file_priorities.is_empty() {
                    let _ = row.metainfo.as_deref().ok_or_else(|| {
                        Error::Invalid("wait for metadata before selecting files".into())
                    })?;
                    let meta = row.meta()?;
                    FileSelection::new(meta.layout(), &settings.file_priorities)?;
                }
                supervisor.stop(&row.id).await?;
                supervisor.db.settings(&row.id, &settings)?;
                supervisor.reconcile().await?;
                Ok(serde_json::to_value(settings)?)
            }
            Request::Files { id } => {
                let row = find(&supervisor.db, &id)?;
                let _ = row
                    .metainfo
                    .as_deref()
                    .ok_or_else(|| Error::Invalid("metadata is not available".into()))?;
                let meta = row.meta()?;
                let priorities = if row.settings.file_priorities.is_empty() {
                    FileSelection::all(meta.layout())
                } else {
                    FileSelection::new(meta.layout(), &row.settings.file_priorities)?
                };
                Ok(
                    json!({"id":row.id,"files":meta.layout().files().iter().enumerate().map(|(index,f)|
                    json!({"index":index,"path":f.path().iter().map(|s|String::from_utf8_lossy(s)).collect::<Vec<_>>().join("/"),
                        "length":f.length(),"priority":priorities.file_priorities()[index]})).collect::<Vec<_>>()}),
                )
            }
            Request::Remove { id, delete_payload } => {
                let row = find(&supervisor.db, &id)?;
                supervisor.db.intent(
                    &row.id,
                    if delete_payload {
                        Intent::RemoveDelete
                    } else {
                        Intent::RemoveKeep
                    },
                )?;
                supervisor.remove(&row.id, delete_payload).await?;
                supervisor.reconcile().await?;
                Ok(json!({"removed":row.id,"delete_payload":delete_payload}))
            }
            action => {
                let (id, intent) = match action {
                    Request::Pause { id } => (id, Intent::Paused),
                    Request::Resume { id } => (id, Intent::Run),
                    Request::Recheck { id } => (id, Intent::Recheck),
                    _ => unreachable!(),
                };
                let row = find(&supervisor.db, &id)?;
                supervisor.db.intent(&row.id, intent.clone())?;
                supervisor.stop(&row.id).await?;
                supervisor.reconcile().await?;
                Ok(json!({"id":row.id,"intent":intent}))
            }
        }
    }
}
const MAX_REQUEST: usize = spill::engine::profile::MAX_META.div_ceil(3) * 4 + 4096;
pub async fn unix_client(stream: tokio::net::UnixStream, control: Control) -> io::Result<()> {
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    let mut bytes = Vec::new();
    // read_until is bounded by take, including clients that never send a newline.
    use tokio::io::AsyncReadExt;
    let length = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        (&mut read)
            .take(MAX_REQUEST as u64 + 1)
            .read_until(b'\n', &mut bytes),
    )
    .await
    .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
    let response = if length > MAX_REQUEST || bytes.last() != Some(&b'\n') {
        Response::from_result(Err(Error::Invalid("invalid or oversized request".into())))
    } else {
        match serde_json::from_slice(&bytes) {
            Ok(request) => control.handle(request).await,
            Err(e) => Response::from_result(Err(Error::Json(e))),
        }
    };
    let mut bytes = serde_json::to_vec(&response)?;
    bytes.push(b'\n');
    write.write_all(&bytes).await
}
