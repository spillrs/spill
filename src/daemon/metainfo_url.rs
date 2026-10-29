//! Bounded HTTP ingestion before a torrent enters the catalog.
use reqwest::{Client, StatusCode, header};
use spill::engine::profile::MAX_META;
use std::{io, time::Duration};
use url::Url;

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn validate(url: &Url) -> io::Result<()> {
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(invalid(
            "torrent URL must use HTTP or HTTPS and have a host",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(invalid("torrent URL must not contain credentials"));
    }
    Ok(())
}

fn transport(error: reqwest::Error) -> io::Error {
    // Download URLs can contain private tracker passkeys or signed query strings.
    io::Error::other(error.without_url())
}

pub async fn fetch(source: &str) -> io::Result<Vec<u8>> {
    let mut url = Url::parse(source).map_err(|_| invalid("invalid torrent URL"))?;
    validate(&url)?;
    let client = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .build()
        .map_err(transport)?;
    tokio::time::timeout(Duration::from_secs(30), async {
        for redirects in 0..=5 {
            let mut response = client.get(url.clone()).send().await.map_err(transport)?;
            if matches!(
                response.status(),
                StatusCode::MOVED_PERMANENTLY
                    | StatusCode::FOUND
                    | StatusCode::SEE_OTHER
                    | StatusCode::TEMPORARY_REDIRECT
                    | StatusCode::PERMANENT_REDIRECT
            ) {
                if redirects == 5 {
                    return Err(invalid("torrent URL redirect limit exceeded"));
                }
                let location = response
                    .headers()
                    .get(header::LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .ok_or_else(|| invalid("invalid torrent URL redirect"))?;
                let next = url
                    .join(location)
                    .map_err(|_| invalid("invalid torrent URL redirect"))?;
                validate(&next)?;
                if url.scheme() == "https" && next.scheme() == "http" {
                    return Err(invalid("torrent URL redirect must not downgrade HTTPS"));
                }
                url = next;
                continue;
            }
            if !response.status().is_success() {
                return Err(io::Error::other(format!(
                    "torrent URL returned HTTP {}",
                    response.status()
                )));
            }
            if response
                .content_length()
                .is_some_and(|length| length > MAX_META as u64)
            {
                return Err(invalid("metainfo exceeds 16 MiB"));
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(transport)? {
                if bytes.len() + chunk.len() > MAX_META {
                    return Err(invalid("metainfo exceeds 16 MiB"));
                }
                bytes.extend_from_slice(&chunk);
            }
            return Ok(bytes);
        }
        unreachable!("bounded redirect loop")
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "torrent URL download timed out"))?
}
