//! Shared HTTP transport defaults for the engine and daemon URL ingestion.

pub(crate) fn client() -> reqwest::ClientBuilder {
    // Explicit configuration avoids process-global providers and system trust stores.
    let tls = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("ring supports default TLS versions")
    .with_root_certificates(rustls::RootCertStore::from_iter(
        webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
    ))
    .with_no_client_auth();
    reqwest::Client::builder()
        .tls_backend_preconfigured(tls)
        .http1_only()
        .no_proxy()
}
