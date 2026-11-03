//! stderr belongs to the terminal/service manager; debug.log is append-only.
use serde::Deserialize;
use std::{
    fs::{File, OpenOptions},
    io::{self, IsTerminal, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, SyncSender},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tracing::{Level, Metadata, Subscriber};
use tracing_subscriber::{
    EnvFilter, Layer, Registry,
    fmt::{self, MakeWriter},
    layer::SubscriberExt,
};

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Config {
    pub(crate) console_filter: String,
    pub(crate) file_filter: String,
    pub(crate) file_enabled: bool,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            console_filter: "info".into(),
            file_filter: "warn,spill=debug".into(),
            file_enabled: true,
        }
    }
}

pub(crate) struct Logging {
    startup: Option<tracing::subscriber::DefaultGuard>,
    // Retain state ownership until queued writes have drained.
    _state_lock: Option<File>,
    worker: Option<(SyncSender<Message>, JoinHandle<()>)>,
}
impl Logging {
    pub(crate) fn init() -> Self {
        let startup = tracing::subscriber::set_default(Registry::default().with(console()));
        Self {
            startup: Some(startup),
            _state_lock: None,
            worker: None,
        }
    }
    pub(crate) fn configure(
        &mut self,
        config: &Config,
        state: &Path,
        lock: File,
    ) -> io::Result<()> {
        self._state_lock = Some(lock);
        let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| config.console_filter.clone());
        let console = console().with_filter(parse_filter(&filter)?);
        let file_filter = parse_filter(&config.file_filter)?;
        let outputs: Box<dyn Layer<Registry> + Send + Sync> = if config.file_enabled {
            let path = state.join("debug.log");
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .mode(0o600)
                .open(&path)
                .and_then(|file| {
                    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
                    Ok(file)
                })
                .map_err(|error| {
                    io::Error::other(format!(
                        "cannot initialize debug log {}: {error}",
                        path.display()
                    ))
                })?;
            let (sender, receiver) = mpsc::sync_channel(1024);
            let dropped = Arc::new(AtomicU64::new(0));
            let writer = Queue {
                sender: sender.clone(),
                dropped: dropped.clone(),
            };
            let worker = std::thread::Builder::new()
                .name("spill-logs".into())
                .spawn(move || {
                    // Never send writer failures back through the failing file output.
                    tracing::subscriber::with_default(
                        Registry::default().with(console_layer()),
                        || run_writer(file, receiver, dropped),
                    );
                })?;
            self.worker = Some((sender, worker));
            console
                .and_then(
                    fmt::layer()
                        .with_ansi(false)
                        .with_writer(writer)
                        .with_filter(file_filter),
                )
                .boxed()
        } else {
            console.boxed()
        };
        tracing::subscriber::set_global_default(Registry::default().with(outputs))
            .expect("daemon tracing subscriber");
        self.startup.take();
        Ok(())
    }
}
impl Drop for Logging {
    fn drop(&mut self) {
        if let Some((sender, worker)) = self.worker.take() {
            sender.send(Message::Stop).expect("log writer alive");
            worker.join().expect("log writer joined");
        }
    }
}
fn parse_filter(value: &str) -> io::Result<EnvFilter> {
    EnvFilter::builder()
        .with_regex(false)
        .parse(value)
        .map_err(io::Error::other)
}
fn console_layer<S>() -> impl Layer<S> + Send + Sync
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fmt::layer()
        .with_ansi(io::stderr().is_terminal())
        .with_writer(Console::new())
}
fn console() -> impl Layer<Registry> + Send + Sync {
    console_layer()
}

#[derive(Clone, Copy)]
struct Console {
    journal: bool,
}
impl Console {
    fn new() -> Self {
        // JOURNAL_STREAM is inherited; verify stderr still names that stream after redirection.
        let journal = std::env::var("JOURNAL_STREAM").ok().is_some_and(|stream| {
            std::fs::metadata("/dev/stderr")
                .is_ok_and(|m| stream == format!("{}:{}", m.dev(), m.ino()))
        });
        Self { journal }
    }
}
struct ConsoleWriter {
    prefix: Option<&'static [u8]>,
}
impl<'a> MakeWriter<'a> for Console {
    type Writer = ConsoleWriter;
    fn make_writer(&'a self) -> Self::Writer {
        ConsoleWriter {
            prefix: self.journal.then_some(b"<6>".as_slice()),
        }
    }
    fn make_writer_for(&'a self, metadata: &Metadata<'_>) -> Self::Writer {
        let prefix: &'static [u8] = match *metadata.level() {
            Level::ERROR => b"<3>",
            Level::WARN => b"<4>",
            Level::INFO => b"<6>",
            _ => b"<7>",
        };
        ConsoleWriter {
            prefix: self.journal.then_some(prefix),
        }
    }
}
impl Write for ConsoleWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut stderr = io::stderr().lock();
        if let Some(prefix) = self.prefix.take() {
            stderr.write_all(prefix)?;
        }
        stderr.write_all(bytes)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        io::stderr().flush()
    }
}

const EVENT_BYTES: usize = 16 * 1024;
enum Message {
    Event(Vec<u8>),
    Stop,
}
#[derive(Clone)]
struct Queue {
    sender: SyncSender<Message>,
    dropped: Arc<AtomicU64>,
}
struct EventWriter {
    queue: Queue,
    bytes: Vec<u8>,
}
impl<'a> MakeWriter<'a> for Queue {
    type Writer = EventWriter;
    fn make_writer(&'a self) -> Self::Writer {
        EventWriter {
            queue: self.clone(),
            bytes: Vec::new(),
        }
    }
}
impl Write for EventWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let remaining = EVENT_BYTES - self.bytes.len();
        self.bytes
            .extend_from_slice(&bytes[..bytes.len().min(remaining)]);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Drop for EventWriter {
    fn drop(&mut self) {
        if self.bytes.len() == EVENT_BYTES {
            let end = b" [truncated]\n";
            self.bytes.truncate(self.bytes.len() - end.len());
            if let Err(error) = std::str::from_utf8(&self.bytes) {
                self.bytes.truncate(error.valid_up_to());
            }
            self.bytes.extend_from_slice(end);
        }
        if self
            .queue
            .sender
            .try_send(Message::Event(std::mem::take(&mut self.bytes)))
            .is_err()
        {
            self.queue.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}
fn run_writer(mut file: File, receiver: mpsc::Receiver<Message>, dropped: Arc<AtomicU64>) {
    let mut failed = false;
    let mut reported = Instant::now();
    loop {
        let message = receiver.recv_timeout(Duration::from_secs(60));
        let stopping = matches!(
            message,
            Ok(Message::Stop) | Err(mpsc::RecvTimeoutError::Disconnected)
        );
        if stopping || reported.elapsed() >= Duration::from_secs(60) {
            let count = dropped.swap(0, Ordering::Relaxed);
            if count != 0 {
                tracing::warn!(count, "debug_log_events_dropped");
            }
            reported = Instant::now();
        }
        match message {
            Ok(Message::Event(bytes)) => match file.write_all(&bytes) {
                Ok(()) => {
                    if failed {
                        tracing::info!("debug_log_recovered");
                    }
                    failed = false;
                }
                Err(error) => {
                    if !failed {
                        tracing::error!(%error, "debug_log_write_failed");
                    }
                    failed = true;
                }
            },
            _ if stopping => break,
            _ => {}
        }
    }
}
