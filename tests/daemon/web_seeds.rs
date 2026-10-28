use crate::common::{Daemon, Temp, WebSeed, metainfo};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn corrupt_piece_is_retried_from_another_web_seed() {
    let root = Temp::new();
    // Both mirrors return valid ranges, but whichever is used first corrupts
    // its body. A single source cannot reconnect and conceal a missing retry.
    let source = WebSeed::files(&[("/bad", b"abc"), ("/good", b"abc")], true).await;
    let urls = [format!("{}bad", source.url), format!("{}good", source.url)];
    let mut bytes = b"d8:url-listl".to_vec();
    for url in &urls {
        bytes.extend_from_slice(format!("{}:{url}", url.len()).as_bytes());
    }
    bytes.push(b'e');
    bytes.extend_from_slice(&metainfo(None)[1..]);
    let mut daemon = Daemon::start(&root.0, "web_seeds=true", None).await;
    let id = daemon.add(&bytes, None).await;
    let row = daemon.state(&id, "seeding").await;
    assert_eq!(row["verified_bytes"], 3);
    assert_eq!(std::fs::read(root.0.join("downloads/x")).unwrap(), b"abc");
    assert_eq!(source.hits.load(Ordering::Relaxed), 2);
    {
        let requests = source.requests.lock().unwrap();
        assert_ne!(
            requests[0].0, requests[1].0,
            "retry must use the other mirror"
        );
        assert!(
            requests
                .iter()
                .all(|(_, start, end)| (*start, *end) == (0, 2))
        );
    }
    daemon.stop().await;
}
