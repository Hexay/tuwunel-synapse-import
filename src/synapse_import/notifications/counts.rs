//! Each joined local user's unread notification and highlight counts, per
//! room and thread, as Synapse's sync would have reported them.

use ruma::{EventId, OwnedEventId, OwnedRoomId};
use tokio_postgres::Row;
use tuwunel_core::{Result, err};
use tuwunel_service::Services;

use super::super::{
	Report,
	source::{Source, user_id},
};

/// Synapse's `_get_unread_counts_by_receipt_txn` for every joined user and room
/// at once (see its comments for the reasoning). A thread is read up to its
/// own receipt where that is newer than the room's unthreaded one, which falls
/// back to the user's join. A summary still matching that receipt counts with
/// the push actions since the last roll-up; any other thread counts its push
/// actions after the receipt. Synapse adds the main timeline's post-roll-up
/// actions to whichever thread its loop saw last; this adds them to the main
/// timeline.
const COUNTS: &str = "
WITH receipts AS (
	SELECT * FROM receipts_linearized WHERE receipt_type IN ('m.read', 'm.read.private')
), unthreaded AS (
	SELECT m.user_id, m.room_id,
		COALESCE(
			(SELECT MAX(r.event_stream_ordering) FROM receipts r
			 WHERE r.user_id = m.user_id AND r.room_id = m.room_id AND r.thread_id IS NULL),
			e.stream_ordering
		) AS read_upto
	FROM local_current_membership m JOIN events e USING (event_id)
	WHERE m.membership = 'join'
), threaded AS (
	SELECT r.user_id, r.room_id, r.thread_id, MAX(r.event_stream_ordering) AS read_upto
	FROM receipts r JOIN unthreaded u USING (user_id, room_id)
	WHERE r.thread_id IS NOT NULL AND r.event_stream_ordering > u.read_upto
	GROUP BY r.user_id, r.room_id, r.thread_id
), summarised AS (
	SELECT s.user_id, s.room_id, s.thread_id, s.notif_count
	FROM event_push_summary s JOIN unthreaded u USING (user_id, room_id)
	LEFT JOIN threaded t USING (user_id, room_id, thread_id)
	WHERE (
		(s.last_receipt_stream_ordering IS NULL
			AND s.stream_ordering > COALESCE(t.read_upto, u.read_upto))
		OR s.last_receipt_stream_ordering = COALESCE(t.read_upto, u.read_upto)
	) AND (s.notif_count != 0 OR COALESCE(s.unread_count, 0) != 0)
), actions AS (
	SELECT a.user_id, a.room_id, a.thread_id,
		COUNT(*) FILTER (WHERE a.notif = 1
			AND a.stream_ordering > COALESCE(t.read_upto, u.read_upto)) AS unread,
		COUNT(*) FILTER (WHERE a.highlight = 1
			AND a.stream_ordering > COALESCE(t.read_upto, u.read_upto)) AS highlights,
		COUNT(*) FILTER (WHERE a.notif = 1 AND a.stream_ordering >
			(SELECT stream_ordering FROM event_push_summary_stream_ordering)) AS since_rollup
	FROM event_push_actions a JOIN unthreaded u USING (user_id, room_id)
	LEFT JOIN threaded t USING (user_id, room_id, thread_id)
	GROUP BY a.user_id, a.room_id, a.thread_id
)
SELECT user_id, room_id, COALESCE(thread_id, 'main'),
	CASE WHEN s.notif_count IS NOT NULL THEN s.notif_count + COALESCE(a.since_rollup, 0)
		ELSE a.unread END,
	COALESCE(a.highlights, 0)
FROM summarised s FULL JOIN actions a USING (user_id, room_id, thread_id)
ORDER BY user_id, room_id";

pub(super) async fn import(services: &Services, source: &Source, report: &mut Report) -> Result {
	for row in source.rows(COUNTS, &[]).await? {
		let counts = (count(&row, 3), count(&row, 4));
		if counts == (0, 0) {
			continue;
		}

		match import_counts(services, &row, counts) {
			| Ok(()) => Report::count(&mut report.unread_counts),
			| Err(e) => report.skip(format!(
				"{}'s unread counts in {}: {e}",
				row.get::<_, &str>(0),
				row.get::<_, &str>(1)
			)),
		}
	}

	Ok(())
}

fn import_counts(services: &Services, row: &Row, counts: (u64, u64)) -> Result {
	let user_id = user_id(row, 0)?;
	let room_id = OwnedRoomId::try_from(row.get::<_, &str>(1)).map_err(|e| err!("{e}"))?;
	let thread_root = thread_root(row.get(2))?;

	services
		.pusher
		.import_unread_counts(&user_id, &room_id, thread_root.as_deref(), counts);

	Ok(())
}

/// Synapse's thread ID, which names the main timeline `main`.
pub(super) fn thread_root(thread_id: &str) -> Result<Option<OwnedEventId>> {
	match thread_id {
		| "main" => Ok(None),
		| root => EventId::parse(root)
			.map(Some)
			.map_err(|e| err!("thread {root:?}: {e}")),
	}
}

fn count(row: &Row, idx: usize) -> u64 {
	row.get::<_, Option<i64>>(idx)
		.and_then(|n| u64::try_from(n).ok())
		.unwrap_or(0)
}
