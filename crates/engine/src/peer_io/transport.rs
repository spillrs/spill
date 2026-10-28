use super::*;

pub(super) async fn run_socket(
    socket: Stream,
    start: HandshakeStart,
    context: SocketContext,
    connection: ConnectionId,
    commands: mpsc::Receiver<Outbound>,
    events: &mpsc::Sender<PeerEvent>,
) -> io::Result<()> {
    match socket {
        Stream::Tcp(mut socket) => {
            // Control frames must not wait for Nagle between batches of blocks.
            socket.set_nodelay(true)?;
            handshake(&mut socket, start, &context.local, connection, events).await?;
            let (reader, writer) = socket.into_split();
            run_io_throttled(
                (reader, context.read_pause),
                writer,
                connection,
                commands,
                events,
                (
                    &context.written,
                    &context.bandwidth,
                    &context.capacity_changed,
                ),
            )
            .await
        }
        Stream::Utp(mut socket) => {
            handshake(&mut socket, start, &context.local, connection, events).await?;
            let (reader, writer) = socket.split();
            run_io_throttled(
                (reader, context.read_pause),
                writer,
                connection,
                commands,
                events,
                (
                    &context.written,
                    &context.bandwidth,
                    &context.capacity_changed,
                ),
            )
            .await
        }
    }
}
async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut S,
    start: HandshakeStart,
    local: &Handshake,
    connection: ConnectionId,
    events: &mpsc::Sender<PeerEvent>,
) -> io::Result<()> {
    let HandshakeStart { role, deadline } = start;
    if Instant::now() >= deadline {
        return Err(handshake_expired());
    }
    let remote = timeout_at(deadline, async {
        if matches!(role, Role::Outbound) {
            socket.write_all(&local.encode()).await?;
        }
        let mut bytes = [0; HANDSHAKE_LEN];
        socket.read_exact(&mut bytes).await?;
        let (remote, _) = Handshake::decode(&bytes).map_err(invalid_data)?;
        let reply = matches!(role, Role::Inbound);
        if remote.info_hash != local.info_hash {
            tracing::debug!(
                ?connection,
                reason = "info_hash_mismatch",
                "handshake_rejected"
            );
            return Err(protocol("handshake info hash mismatch"));
        }
        if remote.peer_id == local.peer_id {
            tracing::debug!(
                ?connection,
                reason = "self_connection",
                "handshake_rejected"
            );
            return Err(protocol("self connection"));
        }
        if reply {
            if Instant::now() >= deadline {
                return Err(handshake_expired());
            }
            socket.write_all(&local.encode()).await?;
        }
        Ok(remote)
    })
    .await
    .map_err(|_| {
        tracing::debug!(?connection, "handshake_deadline");
        handshake_expired()
    })??;
    if Instant::now() >= deadline {
        return Err(handshake_expired());
    }
    events
        .send(PeerEvent::Handshake {
            connection,
            reserved: remote.reserved,
            peer_id: remote.peer_id,
        })
        .await
        .map_err(|_| event_receiver_closed())?;
    Ok(())
}

// Both duplex futures drop together; there are no detached I/O subtasks.
pub(super) async fn run_io_throttled(
    reader: (impl AsyncRead + Unpin, watch::Receiver<bool>),
    writer: impl AsyncWrite + Unpin,
    connection: ConnectionId,
    commands: mpsc::Receiver<impl Into<Outbound> + Send>,
    events: &mpsc::Sender<PeerEvent>,
    output: (&PayloadWrites, &crate::bandwidth::Throttle, &Notify),
) -> io::Result<()> {
    tokio::select! {
        result = read_loop(reader.0, connection, events, reader.1) => result,
        result = write_loop_throttled(writer, commands, crate::profile::WRITE_TIMEOUT, crate::profile::KEEPALIVE_INTERVAL, output.0, output.1, output.2) => result,
    }.map_err(socket_failure)
}

pub(super) async fn read_loop(
    reader: impl AsyncRead + Unpin,
    connection: ConnectionId,
    events: &mpsc::Sender<PeerEvent>,
    mut read_pause: watch::Receiver<bool>,
) -> io::Result<()> {
    let capacity = (crate::profile::MAX_FRAME_LENGTH as usize + 4)
        .saturating_mul(4)
        .min(64 * 1024);
    let mut reader = BufReader::with_capacity(capacity, reader);
    let clock = tokio::time::sleep(crate::profile::IDLE_TIMEOUT);
    tokio::pin!(clock);
    loop {
        read_pause
            .wait_for(|paused| !paused)
            .await
            .map_err(|_| event_receiver_closed())?;
        let mut header = [0; 5];
        let idle = Instant::now() + crate::profile::IDLE_TIMEOUT;
        let count = on_clock(&mut clock, idle, reader.read(&mut header[..1])).await?;
        if Instant::now() >= idle {
            return Err(io_deadline());
        }
        if count == 0 {
            return Ok(());
        }
        let deadline = Instant::now() + crate::profile::FRAME_TIMEOUT;
        let frame = on_clock(
            &mut clock,
            deadline,
            read_frame_body(&mut reader, crate::profile::MAX_FRAME_LENGTH, header),
        )
        .await?;
        if Instant::now() >= deadline {
            return Err(io_deadline());
        }
        events
            .send(PeerEvent::Frame { connection, frame })
            .await
            .map_err(|_| event_receiver_closed())?;
    }
}
pub(super) async fn write_loop_throttled(
    mut writer: impl AsyncWrite + Unpin,
    mut commands: mpsc::Receiver<impl Into<Outbound> + Send>,
    duration: Duration,
    keepalive_interval: Duration,
    written: &PayloadWrites,
    bandwidth: &crate::bandwidth::Throttle,
    capacity_changed: &Notify,
) -> io::Result<()> {
    let clock = tokio::time::sleep(keepalive_interval);
    tokio::pin!(clock);
    let mut last_write = Instant::now();
    let mut batch = Vec::with_capacity(commands.max_capacity().min(64));
    loop {
        batch.clear();
        let keepalive = last_write + keepalive_interval;
        clock.as_mut().reset(keepalive);
        tokio::select! {
            biased;
            command = commands.recv() => match command {
                Some(bytes) => batch.push(bytes.into()),
                None => return Ok(()),
            },
            _ = &mut clock => {}
        }
        let mut size = batch.first().map_or(0, Outbound::len);
        while !batch.is_empty() && batch.len() < commands.max_capacity().min(64) && size < 64 * 1024
        {
            let Ok(bytes) = commands.try_recv() else {
                break;
            };
            let bytes = bytes.into();
            size += bytes.len();
            batch.push(bytes);
        }
        if batch.is_empty() {
            batch.push(Outbound::Encoded(vec![0; 4]));
        } else {
            capacity_changed.notify_one();
        }
        if bandwidth.limited(crate::bandwidth::Direction::Up) {
            for frame in &batch {
                let payload = (frame.len() - frame.payload_start()) as u32;
                if payload != 0 {
                    loop {
                        clock.as_mut().reset(last_write + keepalive_interval);
                        tokio::select! {
                            biased;
                            permit = bandwidth.acquire(crate::bandwidth::Direction::Up, payload) => {
                                permit.commit();
                                break;
                            }
                            _ = &mut clock => {
                                write_batch_counted(&mut writer, &[vec![0; 4].into()], written, duration, &mut clock).await?;
                                last_write = Instant::now();
                            }
                        }
                    }
                }
                write_batch_counted(
                    &mut writer,
                    std::slice::from_ref(frame),
                    written,
                    duration,
                    &mut clock,
                )
                .await?;
                last_write = Instant::now();
                capacity_changed.notify_one();
            }
        } else {
            write_batch_counted(&mut writer, &batch, written, duration, &mut clock).await?;
        }
        last_write = Instant::now();
        capacity_changed.notify_one();
    }
}

// Reuse one connection clock rather than allocating a Sleep for every frame or
// write. Callers check again after Ready; writes account accepted bytes first.
pub(super) async fn on_clock<T>(
    clock: &mut std::pin::Pin<&mut tokio::time::Sleep>,
    deadline: Instant,
    work: impl Future<Output = io::Result<T>>,
) -> io::Result<T> {
    clock.as_mut().reset(deadline);
    if Instant::now() >= deadline {
        return Err(io_deadline());
    }
    tokio::select! {
        biased;
        result = work => result,
        _ = clock.as_mut() => Err(io_deadline()),
    }
}

pub(super) async fn write_batch_counted(
    writer: &mut (impl AsyncWrite + Unpin),
    batch: &[Outbound],
    written: &PayloadWrites,
    duration: Duration,
    clock: &mut std::pin::Pin<&mut tokio::time::Sleep>,
) -> io::Result<()> {
    let payloads: Vec<_> = batch.iter().map(Outbound::payload_start).collect();
    let (mut frame, mut offset) = (0, 0);
    let mut deadline = Instant::now() + duration;
    while frame < batch.len() {
        let mut slices = Vec::with_capacity(2 * (batch.len() - frame));
        for (i, bytes) in batch[frame..].iter().enumerate() {
            let start = if i == 0 { offset } else { 0 };
            let header = bytes.header();
            if start < header.len() {
                slices.push(io::IoSlice::new(&header[start..]));
            }
            let payload = bytes.payload();
            let start = start.saturating_sub(header.len());
            if start < payload.len() {
                slices.push(io::IoSlice::new(&payload[start..]));
            }
        }
        let mut count = on_clock(clock, deadline, writer.write_vectored(&slices)).await?;
        if count == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        let expired = Instant::now() >= deadline;
        let previous_frame = frame;
        // Synchronous accounting before any subsequent await/error, including
        // partial vectored writes crossing multiple control/payload boundaries.
        while count != 0 {
            let accepted = count.min(batch[frame].len() - offset);
            let end = offset + accepted;
            let payload = end.saturating_sub(payloads[frame].max(offset));
            if payload != 0 {
                written.add(payload);
            }
            offset = end;
            count -= accepted;
            if offset == batch[frame].len() {
                frame += 1;
                offset = 0;
            }
        }
        if expired {
            return Err(io_deadline());
        }
        if frame != previous_frame {
            deadline = Instant::now() + duration;
        }
    }
    Ok(())
}

pub(super) async fn read_frame_body(
    reader: &mut (impl AsyncRead + Unpin),
    limit: u32,
    mut header: [u8; 5],
) -> io::Result<Frame> {
    reader.read_exact(&mut header[1..4]).await?;
    let prefix = Message::decode(&header[..4], limit);
    match prefix {
        Ok(_) => {
            return Ok(Frame {
                bytes: Arc::new(header[..4].to_vec()),
            });
        } // Keepalive.
        Err(protocol::peer::message::Error::Incomplete { .. }) => {}
        Err(error) => return Err(invalid_data(error)), // Oversize before body allocation.
    }
    reader.read_exact(&mut header[4..]).await?;
    match Message::decode(&header, limit) {
        Ok(_) | Err(protocol::peer::message::Error::Incomplete { .. }) => {}
        Err(error) => return Err(invalid_data(error)), // Known bad ID/length, early.
    }
    let length = usize::try_from(u32::from_be_bytes(header[..4].try_into().expect("prefix")))
        .ok()
        .and_then(|length| length.checked_add(4))
        .ok_or_else(|| invalid("frame size overflow"))?;
    let mut bytes = vec![0; length];
    bytes[..5].copy_from_slice(&header);
    reader.read_exact(&mut bytes[5..]).await?;
    Message::decode(&bytes, limit).map_err(invalid_data)?;
    Ok(Frame {
        bytes: Arc::new(bytes),
    })
}
