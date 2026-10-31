use super::*;

impl Handle {
    pub(in crate::engine) async fn snapshot(&self) -> io::Result<SavedState> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Snapshot(reply))
            .await
            .map_err(|_| stopped())?;
        result.await.map_err(|_| stopped())
    }
    pub(super) async fn bootstrap_finished(&self, ipv6: bool) -> io::Result<()> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::BootstrapFinished(ipv6, reply))
            .await
            .map_err(|_| stopped())?;
        result.await.map_err(|_| stopped())
    }
    pub(super) async fn set_routers(
        &self,
        ipv6: bool,
        addresses: Vec<SocketAddr>,
    ) -> io::Result<()> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Routers(ipv6, addresses, reply))
            .await
            .map_err(|_| stopped())?;
        result.await.map_err(|_| stopped())
    }
    /// Bounded best-effort verification for peer-wire PORT hints, after initial bootstrap.
    pub(in crate::engine) fn try_ping(&self, address: SocketAddr) -> io::Result<()> {
        if !self.families[usize::from(address.is_ipv6())]
            || !valid_address(address, false)
            || !crate::engine::network::peer(address, self.allow_local_network)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid DHT contact",
            ));
        }
        self.commands
            .try_send(Command::Probe(address))
            .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "DHT command queue unavailable"))
    }
    pub(super) async fn closest(&self, ipv6: bool, target: NodeId) -> io::Result<Vec<Contact>> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Closest(ipv6, target, reply))
            .await
            .map_err(|_| stopped())?;
        result.await.map_err(|_| stopped())
    }
    pub(super) async fn local_peers(
        &self,
        ipv6: bool,
        hash: crate::protocol::identity::InfoHashV1,
    ) -> io::Result<Vec<SocketAddr>> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Peers(hash, ipv6, reply))
            .await
            .map_err(|_| stopped())?;
        result.await.map_err(|_| stopped())
    }
    pub(in crate::engine) fn local_addr(&self) -> SocketAddr {
        self.address
    }
    pub(in crate::engine) fn id(&self) -> NodeId {
        self.id
    }
    pub(super) async fn refresh_targets(&self, ipv6: bool) -> io::Result<Vec<NodeId>> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::RefreshTargets(ipv6, reply))
            .await
            .map_err(|_| stopped())?;
        result.await.map_err(|_| stopped())
    }
    // Local transaction pressure is not evidence that a remote node failed.
    pub(super) async fn request_wait(
        &self,
        address: SocketAddr,
        query: Query,
    ) -> io::Result<Reply> {
        loop {
            match self.request(address, query.clone()).await {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                result => return result,
            }
        }
    }
    pub(in crate::engine) async fn request(
        &self,
        address: SocketAddr,
        query: Query,
    ) -> io::Result<Reply> {
        if matches!(&query, Query::AnnouncePeer { port, token, .. } if *port == 0 || token.len() > 64)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid DHT announce",
            ));
        }
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Request {
                address,
                query,
                reply,
            })
            .await
            .map_err(|_| stopped())?;
        result.await.map_err(|_| stopped())?
    }
    pub(in crate::engine) async fn stats(&self, ipv6: bool) -> io::Result<Stats> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Stats(ipv6, reply))
            .await
            .map_err(|_| stopped())?;
        result.await.map_err(|_| stopped())
    }
}
