//! Bounded saved-contact/router bootstrap; routers are lookup seeds, never routing entries.
use super::{Contact, Handle, NodeId, Query, SavedState, lookup::Requests};
use crate::engine::session::dht::endpoint;
use std::{
    collections::{HashMap, HashSet},
    io,
    net::SocketAddr,
    time::Duration,
};

pub(super) async fn run(
    handle: Handle,
    ipv6: bool,
    saved: SavedState,
    routers: Vec<String>,
    allow_local_network: bool,
) -> io::Result<()> {
    let mut first = true;
    let mut last_good = 0;
    let mut retry_delay = Duration::from_secs(30);
    let mut bootstrap_due = tokio::time::Instant::now();
    let mut identities = handle.identities.clone();
    'bootstrap: loop {
        identities.borrow_and_update();
        let (own, targets) = handle.refresh_targets(ipv6).await?;
        for target in targets {
            let seeds = handle.closest(ipv6, target).await?;
            walk(&handle, ipv6, own, target, seeds, vec![]).await?;
            if handle.id(ipv6) != own {
                continue 'bootstrap;
            }
        }
        let good_before = handle.stats(ipv6).await?.good_nodes;
        // Recover promptly after contact loss, but don't bypass backoff forever
        // on hosts where this address family has no route.
        let lost_contacts = good_before < 8 && last_good >= 8;
        if good_before < 128 && (lost_contacts || tokio::time::Instant::now() >= bootstrap_due) {
            let target = handle.id(ipv6);
            let seeds = if first {
                saved
                    .nodes
                    .iter()
                    .filter(|n| n.address.is_ipv6() == ipv6)
                    .copied()
                    .collect()
            } else {
                handle.closest(ipv6, target).await?
            };
            let mut replied = walk(&handle, ipv6, own, target, seeds, vec![]).await?;
            if handle.id(ipv6) != own {
                continue 'bootstrap;
            }
            if first && !saved.nodes.is_empty() {
                let good_nodes = handle.stats(ipv6).await?.good_nodes;
                tracing::debug!(source = "saved", ipv6, good_nodes, "dht_bootstrap_finished");
            }
            if handle.stats(ipv6).await?.good_nodes < 128 {
                // Pin DNS answers per attempt; recovery retries failed resolution.
                let mut addresses = Vec::new();
                for router in &routers {
                    let (host, port) = endpoint(router).expect("validated DHT router");
                    if let Some(address) =
                        crate::engine::network::resolve_dht(host, port, ipv6, allow_local_network)
                            .await
                        && !addresses.contains(&address)
                    {
                        addresses.push(address);
                    }
                }
                handle.set_routers(ipv6, addresses.clone()).await?;
                let target = if first {
                    handle.id(ipv6)
                } else {
                    NodeId(super::entropy()?)
                };
                let seeds = handle.closest(ipv6, target).await?;
                replied += walk(&handle, ipv6, own, target, seeds, addresses.clone()).await?;
                if handle.id(ipv6) != own {
                    continue 'bootstrap;
                }
                let good_nodes = handle.stats(ipv6).await?.good_nodes;
                if first {
                    tracing::debug!(
                        source = "routers",
                        ipv6,
                        routers = addresses.len(),
                        good_nodes,
                        "dht_bootstrap_finished"
                    );
                } else {
                    tracing::trace!(
                        ipv6,
                        routers = addresses.len(),
                        good_nodes,
                        "dht_bootstrap_recovery"
                    );
                }
            }
            let good_after = handle.stats(ipv6).await?.good_nodes;
            retry_delay = if good_after > good_before || good_before < 8 && replied > 0 {
                Duration::from_secs(30)
            } else {
                (retry_delay * 2).min(Duration::from_secs(15 * 60))
            };
            bootstrap_due = tokio::time::Instant::now() + retry_delay;
        }
        if first {
            handle.bootstrap_finished(ipv6).await?;
        }
        last_good = handle.stats(ipv6).await?.good_nodes;
        first = false;
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(30)) => {},
            changed = identities.changed() => changed.map_err(|_| super::stopped())?,
        }
    }
}
async fn walk(
    handle: &Handle,
    ipv6: bool,
    own: NodeId,
    target: NodeId,
    seeds: Vec<Contact>,
    routers: Vec<SocketAddr>,
) -> io::Result<usize> {
    // Router seeds must not be discarded by distance-based candidate truncation.
    let mut ids: HashMap<_, _> = seeds.iter().map(|node| (node.address, node.id)).collect();
    let mut queue: Vec<_> = seeds.into_iter().map(|node| node.address).collect();
    let mut routers = routers.into_iter();
    let mut asked = HashSet::new();
    let mut requests = Requests::default();
    let mut replied = 0;
    loop {
        // A new identity needs its own neighbourhood, not the previous walk's target.
        if handle.id(ipv6) != own {
            break;
        }
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
            replied += 1;
            for node in reply.nodes {
                if node.address.is_ipv6() == ipv6
                    && node.id != handle.id(ipv6)
                    && !asked.contains(&node.address)
                    && !queue.contains(&node.address)
                {
                    ids.insert(node.address, node.id);
                    queue.push(node.address);
                }
            }
        }
    }
    Ok(replied)
}
