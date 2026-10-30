use serde_json::{Value, json};
use spill::engine::torrent::{TorrentPhase, TorrentStatus, telemetry};

pub fn phase(status: &TorrentStatus, recheck: bool) -> &'static str {
    match status.progress.phase {
        TorrentPhase::Starting => "starting",
        TorrentPhase::ResolvingMetadata => "metadata",
        TorrentPhase::PreparingDestination => "recovering",
        _ if recheck => "checking",
        TorrentPhase::Recovering => "recovering",
        _ if status.progress.completed => "seeding",
        _ => "downloading",
    }
}

pub fn transfer(s: &TorrentStatus) -> Value {
    let peers: Vec<_> = s
        .diagnostics
        .iter()
        .flat_map(|d| &d.peers)
        .map(peer)
        .collect();
    let trackers: Vec<_> = s.diagnostics.iter().flat_map(|d| &d.trackers).map(|t| json!({
        "url":t.url,"tier":t.tier,"slot":t.slot,"state":t.state,"exchanges":t.exchanges,
        "failures":t.failures,"last_reply_ago_ms":t.last_reply_ago_ms,"next_announce_ms":t.next_announce_ms,
        "seeders":t.seeders,"leechers":t.leechers,"peers_returned":t.peers_returned,"error":t.error,"warning":t.warning,
    })).collect();
    let web_seeds: Vec<_> = s
        .diagnostics
        .iter()
        .flat_map(|d| &d.web_seeds)
        .map(|w| {
            json!({
                "url":w.url,"state":w.state,"last_event_ago_ms":w.last_event_ago_ms,"error":w.error,
            })
        })
        .collect();
    let dht = s.diagnostics.as_ref().and_then(|d| d.dht.as_ref()).map(|d| json!({
        "lookup_pending":d.lookup_pending,"next_lookup_ms":d.next_lookup_ms,"last_lookup_ago_ms":d.last_lookup_ago_ms,
        "queried":d.queried,"announced":d.announced,"peers_returned":d.peers_returned,"error":d.error,
    }));
    json!({"ready":s.progress.ready,"completed":s.progress.completed,
        "active_peers":s.progress.active_peers,"downloaded_bytes":s.progress.processed_payload_bytes,
        "uploaded_bytes":s.progress.written_payload_bytes,
        "hash_failed_bytes":s.progress.hash_failed_bytes,"hash_failed_pieces":s.progress.hash_failed_pieces,
        "download_bytes_per_second":s.progress.download_bytes_per_second,"upload_bytes_per_second":s.progress.upload_bytes_per_second,
        "utp_retransmits":s.diagnostics.as_ref().map(|d|d.utp_retransmits),
        "pending_contacts":s.diagnostics.as_ref().map(|d|d.pending_contacts),
        "writer_free_slots":s.diagnostics.as_ref().map(|d|d.writer_free_slots),
        "upload_reads_in_flight":s.diagnostics.as_ref().map(|d|d.upload_reads_in_flight),
        "upload_reads_finished":s.diagnostics.as_ref().map(|d|d.upload_reads_finished),
        "peers":peers,"trackers":trackers,"web_seeds":web_seeds,"dht":dht})
}

fn peer(p: &telemetry::PeerStatus) -> Value {
    let d = &p.detail;
    json!({"connection":p.connection,"endpoint":p.endpoint,"client":p.client,"direction":p.direction,
        "transport":p.transport,"utp_retransmits":p.utp_retransmits,"source":p.source,"web_seed_url":p.web_seed_url,
        "download_bytes_per_second":p.download_bytes_per_second,"upload_bytes_per_second":p.upload_bytes_per_second,
        "detail":{"downloaded_bytes":d.downloaded_bytes,"uploaded_bytes":d.uploaded_bytes,"connection":d.connection,
            "established":d.established,"fast_extension":d.fast_extension,"remote_choking":d.remote_choking,
            "local_interested":d.local_interested,"remote_interested":d.remote_interested,"local_choking":d.local_choking,
            "upload_slot":d.upload_slot,"pending_upload_requests":d.pending_upload_requests,"has_needed_pieces":d.has_needed_pieces,
            "retiring":d.retiring,"output_blocked":d.output_blocked,"assigned_requests":d.assigned_requests,
            "authorized_requests":d.authorized_requests,"request_queue":d.request_queue,
            "ignored_frames_remaining":d.ignored_frames_remaining,"ignored_bytes_remaining":d.ignored_bytes_remaining}})
}
