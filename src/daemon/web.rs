//! Loopback-only web frontend using the same dispatcher as the Unix socket.
use crate::control::{Control, Request, Response};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Query, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse},
    routing::{get, post},
};
use std::{io, net::SocketAddr, path::PathBuf};
use tokio::sync::watch;
#[derive(Clone)]
struct App {
    control: Control,
    token: String,
    port: u16,
}
fn authorized_host(headers: &HeaderMap, port: u16) -> bool {
    let host = headers
        .get("host")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    let allowed = [
        format!("127.0.0.1:{port}"),
        format!("[::1]:{port}"),
        format!("localhost:{port}"),
    ];
    allowed.iter().any(|h| h == host)
        && headers
            .get("origin")
            .is_none_or(|o| o.to_str().ok() == Some(format!("http://{host}").as_str()))
}
async fn index(State(app): State<App>, headers: HeaderMap) -> axum::response::Response {
    if !authorized_host(&headers, app.port) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let mut response =
        Html(include_str!("web/index.html").replace("__TOKEN__", &app.token)).into_response();
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    response
        .headers_mut()
        .insert("x-frame-options", "DENY".parse().unwrap());
    response
}
fn authorized(app: &App, headers: &HeaderMap) -> bool {
    authorized_host(headers, app.port)
        && headers.get("x-spill-token").and_then(|v| v.to_str().ok()) == Some(app.token.as_str())
}
#[derive(serde::Deserialize)]
struct Upload {
    output_dir: Option<PathBuf>,
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
                output_dir: options.output_dir,
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
pub async fn serve(
    listener: tokio::net::TcpListener,
    control: Control,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    use std::io::Read;
    let mut entropy = [0; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut entropy)?;
    let token = entropy.iter().map(|b| format!("{b:02x}")).collect();
    let address: SocketAddr = listener.local_addr()?;
    let app = App {
        control,
        token,
        port: address.port(),
    };
    let routes = Router::new()
        .route("/", get(index))
        .route("/api", post(api).layer(DefaultBodyLimit::max(64 * 1024)))
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
