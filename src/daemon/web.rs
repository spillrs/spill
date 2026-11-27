//! Loopback-only web frontend using the same dispatcher as the Unix socket.
use crate::control::{Control, Request, Response};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Query, State},
    http::{HeaderMap, StatusCode, Uri},
    response::{Html, IntoResponse},
    routing::{get, post},
};
use std::{
    io::{self, SeekFrom},
    path::PathBuf,
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    sync::watch,
};

static WEB: &[(&str, &str, &[u8])] = &[
    (
        "app.js",
        "text/javascript; charset=utf-8",
        include_bytes!("web/app.js"),
    ),
    (
        "details.js",
        "text/javascript; charset=utf-8",
        include_bytes!("web/details.js"),
    ),
    (
        "logs.js",
        "text/javascript; charset=utf-8",
        include_bytes!("web/logs.js"),
    ),
    (
        "style.css",
        "text/css; charset=utf-8",
        include_bytes!("web/style.css"),
    ),
    ("logo.svg", "image/svg+xml", include_bytes!("web/logo.svg")),
];
#[derive(Clone)]
struct App {
    control: Control,
    token: String,
}
fn authorized_host(headers: &HeaderMap) -> bool {
    let host = headers
        .get("host")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    // SSH forwarding preserves the browser's Host, including its local port.
    let (name, port) = if let Some(rest) = host.strip_prefix('[') {
        let Some((addr, after)) = rest.split_once(']') else {
            return false;
        };
        let port = match after {
            "" => None,
            rest => match rest.strip_prefix(':') {
                Some(port) => Some(port),
                None => return false,
            },
        };
        (addr, port)
    } else if let Some((name, port)) = host.rsplit_once(':') {
        (name, Some(port))
    } else {
        (host, None)
    };
    let valid_port = port.is_none_or(|p| {
        !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) && p.parse::<u16>().is_ok()
    });
    let valid_host = name.eq_ignore_ascii_case("localhost")
        || name
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback() && ip.is_ipv6() == host.starts_with('['));
    valid_host
        && valid_port
        && headers
            .get("origin")
            .is_none_or(|o| o.to_str().ok() == Some(format!("http://{host}").as_str()))
}
async fn index(State(app): State<App>, headers: HeaderMap) -> axum::response::Response {
    if !authorized_host(&headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let defaults = {
        let supervisor = app.control.0.lock().await;
        serde_json::json!({
            "settings": supervisor.config.defaults,
        })
    };
    let mut response = Html(
        include_str!("web/index.html")
            .replace("__TOKEN__", &app.token)
            .replace("__ADD_DEFAULTS__", &defaults.to_string()),
    )
    .into_response();
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    response
        .headers_mut()
        .insert("x-frame-options", "DENY".parse().unwrap());
    response
}
async fn asset(State(app): State<App>, headers: HeaderMap, uri: Uri) -> axum::response::Response {
    if !authorized_host(&headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    if uri.path() == "/index.html" {
        return index(State(app), headers).await;
    }
    let path = uri.path().trim_start_matches('/');
    let Some(&(_, content_type, source)) = WEB.iter().find(|&&(name, _, _)| name == path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    (
        [
            ("content-type", content_type),
            ("cache-control", "no-store"),
        ],
        source,
    )
        .into_response()
}
fn authorized(app: &App, headers: &HeaderMap) -> bool {
    authorized_host(headers)
        && headers.get("x-spill-token").and_then(|v| v.to_str().ok()) == Some(app.token.as_str())
}
#[derive(serde::Deserialize)]
struct Upload {
    output_dir: Option<PathBuf>,
    #[serde(default)]
    paused: bool,
    settings: Option<String>,
}
async fn upload(
    State(app): State<App>,
    Query(options): Query<Upload>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Response>, StatusCode> {
    if !authorized(&app, &headers) {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(Json(
        app.control
            .handle(Request::Add {
                metainfo: Some(body.to_vec()),
                magnet: None,
                url: None,
                output_dir: options.output_dir,
                paused: options.paused,
                settings: options
                    .settings
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|_| StatusCode::BAD_REQUEST)?
                    .unwrap_or_default(),
            })
            .await,
    ))
}
async fn api(
    State(app): State<App>,
    headers: HeaderMap,
    Json(request): Json<Request>,
) -> Result<Json<Response>, StatusCode> {
    if !authorized(&app, &headers) {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(Json(app.control.handle(request).await))
}
async fn logs(State(app): State<App>, headers: HeaderMap) -> axum::response::Response {
    if !authorized(&app, &headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let (enabled, path) = {
        let supervisor = app.control.0.lock().await;
        (
            supervisor.config.logging.file_enabled,
            supervisor.config.state_dir.join("debug.log"),
        )
    };
    let result = async {
        let (text, truncated) = if enabled {
            let mut file = tokio::fs::File::open(path).await?;
            let length = file.metadata().await?.len();
            let start = length - length.min(64 * 1024);
            file.seek(SeekFrom::Start(start)).await?;
            let mut bytes = Vec::new();
            file.take(length - start).read_to_end(&mut bytes).await?;
            let first = if start > 0 {
                bytes
                    .iter()
                    .position(|&b| b == b'\n')
                    .map_or(bytes.len(), |i| i + 1)
            } else {
                0
            };
            (
                String::from_utf8_lossy(&bytes[first..]).into_owned(),
                start > 0,
            )
        } else {
            (String::new(), false)
        };
        Ok(serde_json::json!({"enabled":enabled,"text":text,"truncated":truncated}))
    }
    .await;
    (
        [("cache-control", "no-store")],
        Json(Response::from_result(result)),
    )
        .into_response()
}
pub async fn serve(
    listener: tokio::net::TcpListener,
    control: Control,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut entropy = [0; 32];
    getrandom::fill(&mut entropy).map_err(io::Error::other)?;
    let token = crate::encoding::hex_key(entropy);
    let app = App { control, token };
    let routes = Router::new()
        .route("/", get(index))
        .route("/{*path}", get(asset))
        .route(
            "/api",
            post(api).layer(DefaultBodyLimit::max(2 * 1024 * 1024)),
        )
        .route(
            "/api/logs",
            post(logs).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/api/upload",
            post(upload).layer(DefaultBodyLimit::max(spill::engine::profile::MAX_META)),
        )
        .with_state(app);
    axum::serve(listener, routes)
        .with_graceful_shutdown(async move {
            if !*stop.borrow() {
                let _ = stop.changed().await;
            }
        })
        .await
}
