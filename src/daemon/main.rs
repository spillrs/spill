//! One user, one Linux and macOS daemon, one current-thread runtime.
mod catalog;
mod cli;
mod config;
mod control;
#[path = "../http.rs"]
mod http;
mod metainfo_url;
mod observation;
mod settings;
mod storage;
mod supervisor;
mod web;
use clap::Parser;
use std::{
    fs::OpenOptions,
    io,
    os::unix::fs::{FileTypeExt, PermissionsExt},
    process::ExitCode,
    sync::Arc,
    time::Duration,
};
use tokio::{sync::Mutex, task::JoinSet};

fn main() -> ExitCode {
    let cli = match cli::Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            let code = e.exit_code() as u8;
            let _ = e.print();
            return ExitCode::from(code);
        }
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let code = runtime.block_on(async {
        match cli.command {
            cli::Command::Serve { config } => match serve(config.as_deref()).await {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("{e}");
                    1
                }
            },
            cli::Command::Client(command) => match cli::run(command, cli.socket, cli.json).await {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("{e}");
                    e.code()
                }
            },
        }
    });
    ExitCode::from(code)
}
#[cfg(target_os = "macos")]
fn macos_file_limit() -> io::Result<u64> {
    // rustix has no sysctl wrapper; use the system utility without unsafe FFI.
    let output = std::process::Command::new("/usr/sbin/sysctl")
        .args(["-n", "kern.maxfilesperproc"])
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other(String::from_utf8_lossy(&output.stderr)));
    }
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .map_err(io::Error::other)
}
async fn interrupt() -> io::Result<()> {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! { result = tokio::signal::ctrl_c() => result, _ = term.recv() => Ok(()) }
}
async fn serve(path: Option<&std::path::Path>) -> io::Result<()> {
    let config = config::Config::load(path)?;
    let limit = rustix::process::getrlimit(rustix::process::Resource::Nofile);
    let target = limit.maximum.unwrap_or(1 << 20);
    // Darwin rejects an infinite soft limit even when the hard limit is infinite.
    #[cfg(target_os = "macos")]
    let target = target.min(macos_file_limit()?);
    if limit.current.is_some_and(|current| current < target) {
        let raised = rustix::process::setrlimit(
            rustix::process::Resource::Nofile,
            rustix::process::Rlimit {
                current: Some(target),
                maximum: limit.maximum,
            },
        );
        #[cfg(target_os = "macos")]
        raised.map_err(|error| {
            io::Error::other(format!(
                "cannot raise file-descriptor limit to {target}: {error}"
            ))
        })?;
        #[cfg(not(target_os = "macos"))]
        let _ = raised;
    }
    rustix::process::umask(rustix::fs::Mode::from_raw_mode(0o077));
    std::fs::create_dir_all(&config.state_dir)?;
    std::fs::set_permissions(&config.state_dir, std::fs::Permissions::from_mode(0o700))?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(config.state_dir.join("daemon.lock"))?;
    rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive)?;
    if let Some(parent) = config.control_socket.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if let Ok(metadata) = std::fs::symlink_metadata(&config.control_socket) {
        if !metadata.file_type().is_socket()
            || std::os::unix::net::UnixStream::connect(&config.control_socket).is_ok()
        {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "control socket already in use",
            ));
        }
        std::fs::remove_file(&config.control_socket)?;
    }
    let listener = tokio::net::UnixListener::bind(&config.control_socket)?;
    std::fs::set_permissions(
        &config.control_socket,
        std::fs::Permissions::from_mode(0o600),
    )?;
    let db = catalog::Catalog::open(&config.state_dir.join("catalog.sqlite"))
        .map_err(io::Error::other)?;
    let supervisor = supervisor::Supervisor::new(config.clone(), db).await?;
    let mut stopping = supervisor.shutdown.subscribe();
    let control = control::Control(Arc::new(Mutex::new(supervisor)));
    let web = if let Some(address) = config.web_listen {
        let listener = tokio::net::TcpListener::bind(address).await?;
        Some(tokio::spawn(web::serve(
            listener,
            control.clone(),
            stopping.clone(),
        )))
    } else {
        None
    };
    let mut clients = JoinSet::new();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let interrupted = interrupt();
    tokio::pin!(interrupted);
    let result: io::Result<()> = async {
        loop {
            tokio::select! {
                _ = stopping.changed() => break,
                result = &mut interrupted => { result?; break; }
                accepted = listener.accept(), if clients.len() < 64 => {
                    let (stream,_) = accepted?;
                    let control = control.clone();
                    clients.spawn(async move { control::unix_client(stream,control).await });
                }
                Some(result) = clients.join_next(), if !clients.is_empty() => { if let Err(e) = result { eprintln!("control client: {e}"); } }
                _ = tick.tick() => {
                    if let Err(e) = control.0.lock().await.reconcile().await {
                        eprintln!("reconcile: {e}");
                    }
                }
            }
        }
        Ok(())
    }.await;
    drop(listener);
    let joined = {
        let mut s = control.0.lock().await;
        s.shutdown.send_replace(true);
        s.joined_shutdown().await
    };
    if let Some(mut web) = web
        && tokio::time::timeout(Duration::from_secs(1), &mut web)
            .await
            .is_err()
    {
        web.abort();
        let _ = web.await;
    }
    // Let the shutdown requester write its reply before aborting idle clients.
    if tokio::time::timeout(Duration::from_secs(1), async {
        while let Some(result) = clients.join_next().await {
            if let Err(e) = result {
                eprintln!("control client: {e}");
            }
        }
    })
    .await
    .is_err()
    {
        clients.shutdown().await;
    }
    std::fs::remove_file(&config.control_socket)?;
    drop(lock);
    result.and(joined)
}
