use super::*;

pub(super) fn change_deadline(
    deadlines: &mut BTreeMap<Instant, usize>,
    old: Option<Instant>,
    new: Option<Instant>,
) {
    if old == new {
        return;
    }
    if let Some(old) = old {
        let count = deadlines.get_mut(&old).expect("indexed deadline");
        *count -= 1;
        if *count == 0 {
            deadlines.remove(&old);
        }
    }
    if let Some(new) = new {
        *deadlines.entry(new).or_default() += 1;
    }
}
pub(super) fn set_pending(
    pending: &mut Pending,
    connection: Option<ConnectionId>,
    deadline: Option<Instant>,
    deadlines: &mut BTreeMap<Instant, usize>,
    not_live: &mut usize,
) {
    let was_waiting = pending.connection.is_none() || pending.deadline.is_none();
    let waiting = connection.is_none() || deadline.is_none();
    if was_waiting != waiting {
        if waiting {
            *not_live += 1;
        } else {
            *not_live -= 1;
        }
    }
    change_deadline(deadlines, pending.deadline, deadline);
    pending.connection = connection;
    pending.deadline = deadline;
}
pub(super) fn next_block(
    piece: &Piece,
    layout: &Layout<'_>,
    block_len: u32,
) -> Option<BlockRequest> {
    next_block_at(piece.index, piece.cursor, layout, block_len)
}
pub(super) fn next_block_at(
    index: u32,
    cursor: u32,
    layout: &Layout<'_>,
    block_len: u32,
) -> Option<BlockRequest> {
    let range = layout.piece_range(index).expect("piece index");
    let data = layout
        .segments(range.start + u64::from(cursor)..range.end)
        .expect("cursor")
        .find(|segment| segment.kind == FileKind::Data)?;
    Some(BlockRequest {
        index,
        begin: u32::try_from(data.torrent_offset - range.start).expect("piece offset"),
        length: u32::try_from(data.length.min(u64::from(block_len))).expect("block length"),
    })
}
