#[allow(dead_code)]
#[path = "common/metadata_peer.rs"]
mod metadata_peer;
#[path = "common/payload.rs"]
mod payload;
#[path = "common/v2_peer.rs"]
mod v2_peer;

#[path = "engine/admission.rs"]
mod admission;
#[path = "engine/completion.rs"]
mod completion;
#[path = "engine/create.rs"]
mod create;
#[path = "engine/dht.rs"]
mod dht;
#[path = "engine/dht_ipv6.rs"]
mod dht_ipv6;
#[path = "engine/discovery_v2.rs"]
mod discovery_v2;
#[path = "engine/discovery_v2_policy.rs"]
mod discovery_v2_policy;
#[path = "engine/encryption.rs"]
mod encryption;
#[path = "engine/hybrid.rs"]
mod hybrid;
#[path = "engine/lifecycle.rs"]
mod lifecycle;
#[path = "engine/lsd.rs"]
mod lsd;
#[path = "engine/magnets_v2.rs"]
mod magnets_v2;
#[path = "engine/outcomes.rs"]
mod outcomes;
#[path = "engine/session.rs"]
mod session;
#[path = "engine/v2.rs"]
mod v2;
#[path = "engine/web_seeds.rs"]
mod web_seeds;
