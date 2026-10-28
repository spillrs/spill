//! Bounded saved-contact/router bootstrap; routers are lookup seeds, never routing entries.
use super::{Handle, NodeId, NodeV4, Query, SavedState, lookup::Requests};
use crate::session::dht::endpoint;
use std::{
    collections::{HashMap, HashSet},
    io,
    net::SocketAddrV4,
    time::Duration,
};

pub(super) fn needed(good: usize) -> bool {
    good < 128
}

pub(super) async fn run(
    handle: Handle,
    saved: SavedState,
    routers: Vec<String>,
    allow_local_network: bool,
) -> io::Result<()> {
    let mut first = true;
    let mut retry_delay = Duration::from_secs(30);
    let mut bootstrap_due = tokio::time::Instant::now();
    loop {
        let good_before = handle.stats().await?.good_nodes;
        // Contact loss after a network change must bypass growth backoff.
        if needed(good_before) && (good_before < 8 || tokio::time::Instant::now() >= bootstrap_due)
        {
            // Populate the densely split neighborhood around our own ID first.
            let target = handle.id();
            let seeds = if first {
                saved.nodes.clone()
            } else {
                handle.closest(target).await?
            };
            walk(&handle, target, seeds, vec![]).await?;
            if first && !saved.nodes.is_empty() {
                let good_nodes = handle.stats().await?.good_nodes;
                tracing::info!(source = "saved", good_nodes, "dht_bootstrap_finished");
            }
            if needed(handle.stats().await?.good_nodes) {
                // Pin DNS answers for this attempt, not for the process lifetime.
                // A failed DNS lookup must be retried on the next recovery cycle.
                let addresses = {
                    let mut addresses = Vec::new();
                    for router in &routers {
                        let (host, port) = endpoint(router)?;
                        if let Some(address) =
                            crate::network::resolve_v4(host, port, allow_local_network).await
                            && !addresses.contains(&address)
                        {
                            addresses.push(address);
                        }
                    }
                    handle.set_routers(addresses.clone()).await?;
                    addresses
                };
                let target = if first {
                    handle.id()
                } else {
                    NodeId(super::entropy()?)
                };
                // Also explore other keyspace regions, including in networks
                // with router bootstrap explicitly disabled.
                let seeds = handle.closest(target).await?;
                walk(&handle, target, seeds, addresses.clone()).await?;
                let good_nodes = handle.stats().await?.good_nodes;
                if first {
                    tracing::info!(
                        source = "routers",
                        routers = addresses.len(),
                        good_nodes,
                        "dht_bootstrap_finished"
                    );
                } else {
                    tracing::debug!(
                        routers = addresses.len(),
                        good_nodes,
                        "dht_bootstrap_recovery"
                    );
                }
            }
            let good_after = handle.stats().await?.good_nodes;
            retry_delay = if good_before < 8 || good_after > good_before {
                Duration::from_secs(30)
            } else {
                (retry_delay * 2).min(Duration::from_secs(15 * 60))
            };
            bootstrap_due = tokio::time::Instant::now() + retry_delay;
        }
        if first {
            handle.bootstrap_finished().await?;
        }
        for target in handle.refresh_targets().await? {
            let seeds = handle.closest(target).await?;
            walk(&handle, target, seeds, vec![]).await?;
        }
        first = false;
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}
async fn walk(
    handle: &Handle,
    target: NodeId,
    seeds: Vec<NodeV4>,
    routers: Vec<SocketAddrV4>,
) -> io::Result<()> {
    // Known contacts are distance-ordered. Query explicit router seeds first,
    // separately, so candidate truncation cannot silently discard them.
    let mut ids: HashMap<_, _> = seeds.iter().map(|node| (node.address, node.id)).collect();
    let mut queue: Vec<_> = seeds.into_iter().map(|node| node.address).collect();
    let mut routers = routers.into_iter();
    let mut asked = HashSet::new();
    let mut requests = Requests::default();
    loop {
        queue.sort_unstable_by_key(|address| {
            std::cmp::Reverse(super::routing::distance(ids[address], target))
        });
        if queue.len() > handle.lookup_queries {
            queue.drain(..queue.len() - handle.lookup_queries);
        }
        ids.retain(|address, _| queue.contains(address));
        while requests.can_start() && asked.len() < handle.lookup_queries {
            let Some(address) = routers.next().or_else(|| queue.pop()) else {
                break;
            };
            if address == handle.local_addr() || !asked.insert(address) {
                continue;
            }
            requests.push(handle.clone(), address, Query::FindNode(target));
        }
        if requests.is_empty() {
            break;
        }
        let Some((_, reply)) = requests.next().await else {
            continue;
        };
        if let Ok(reply) = reply {
            for node in reply.nodes {
                if node.id != handle.id()
                    && !asked.contains(&node.address)
                    && !queue.contains(&node.address)
                {
                    ids.insert(node.address, node.id);
                    queue.push(node.address);
                }
            }
        }
    }
    Ok(())
}
