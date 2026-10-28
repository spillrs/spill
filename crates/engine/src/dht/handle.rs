use super::*;

impl Handle {
    pub(crate) async fn snapshot(&self) -> io::Result<SavedState> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Snapshot(reply))
            .await
            .map_err(|_| stopped())?;
        result.await.map_err(|_| stopped())
    }
    pub(super) async fn bootstrap_finished(&self) -> io::Result<()> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::BootstrapFinished(reply))
            .await
            .map_err(|_| stopped())?;
        result.await.map_err(|_| stopped())
    }
    pub(super) async fn set_routers(&self, addresses: Vec<SocketAddrV4>) -> io::Result<()> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Routers(addresses, reply))
            .await
            .map_err(|_| stopped())?;
        result.await.map_err(|_| stopped())
    }
    /// Bounded best-effort verification for peer-wire PORT hints, after initial bootstrap.
    pub(crate) fn try_ping(&self, address: SocketAddrV4) -> io::Result<()> {
        if !crate::network::peer(address.into(), self.allow_local_network) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid DHT contact",
            ));
        }
        self.commands
            .try_send(Command::Probe(address))
            .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "DHT command queue unavailable"))
    }
    pub(super) async fn closest(&self, target: NodeId) -> io::Result<Vec<NodeV4>> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Closest(target, reply))
            .await
            .map_err(|_| stopped())?;
        result.await.map_err(|_| stopped())
    }
    pub(super) async fn local_peers(
        &self,
        hash: protocol::identity::InfoHashV1,
    ) -> io::Result<Vec<SocketAddrV4>> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Peers(hash, reply))
            .await
            .map_err(|_| stopped())?;
        result.await.map_err(|_| stopped())
    }
    pub(crate) fn local_addr(&self) -> SocketAddrV4 {
        self.address
    }
    pub(crate) fn id(&self) -> NodeId {
        self.id
    }
    pub(super) async fn refresh_targets(&self) -> io::Result<Vec<NodeId>> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::RefreshTargets(reply))
            .await
            .map_err(|_| stopped())?;
        result.await.map_err(|_| stopped())
    }
    // Local transaction pressure is not evidence that a remote node failed.
    pub(super) async fn request_wait(
        &self,
        address: SocketAddrV4,
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
    pub(crate) async fn request(&self, address: SocketAddrV4, query: Query) -> io::Result<Reply> {
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
    pub(crate) async fn stats(&self) -> io::Result<Stats> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Stats(reply))
            .await
            .map_err(|_| stopped())?;
        result.await.map_err(|_| stopped())
    }
}
