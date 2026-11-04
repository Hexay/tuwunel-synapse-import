//! How far each local user had read each room and thread, from their latest
//! read receipts, as tuwunel's live receipt path records it: a room is read
//! to its newest unthreaded or main-timeline receipt, and a thread to its own
//! receipt while that is newer than the room's unthreaded one, which clears
//! thread stamps live.

use std::collections::BTreeMap;

use ruma::{EventId, OwnedEventId, OwnedRoomId, OwnedUserId};
use tuwunel_core::Result;
use tuwunel_matrix::PduCount;
use tuwunel_service::Services;

use super::{
	super::source::{Source, user_id},
	counts::thread_root,
};

/// Each user's latest receipt per room and Synapse thread ID ('' unthreaded).
const LATEST: &str = "
SELECT DISTINCT ON (user_id, room_id, thread) user_id, room_id, thread, event_id,
	event_stream_ordering
FROM (
	SELECT user_id, room_id, COALESCE(thread_id, '') AS thread, event_id, event_stream_ordering
	FROM receipts_linearized
	WHERE receipt_type IN ('m.read', 'm.read.private') AND user_id IN (SELECT name FROM users)
) r
ORDER BY user_id, room_id, thread, event_stream_ordering DESC NULLS LAST";

type Receipt = (i64, OwnedEventId);

#[derive(Default)]
struct Latest {
	unthreaded: Option<Receipt>,
	main: Option<Receipt>,
	threads: Vec<(OwnedEventId, Receipt)>,
}

pub(super) async fn import(services: &Services, source: &Source) -> Result {
	let mut latest: BTreeMap<(OwnedUserId, OwnedRoomId), Latest> = BTreeMap::new();

	// A row that doesn't parse names an invalid receipt, which receipts reported.
	for row in source.rows(LATEST, &[]).await? {
		let (Ok(user_id), Ok(room_id), Ok(event_id)) = (
			user_id(&row, 0),
			OwnedRoomId::try_from(row.get::<_, &str>(1)),
			OwnedEventId::try_from(row.get::<_, &str>(3)),
		) else {
			continue;
		};

		let receipt = (row.get::<_, Option<i64>>(4).unwrap_or(0), event_id);
		let entry = latest.entry((user_id, room_id)).or_default();
		match row.get::<_, &str>(2) {
			| "" => entry.unthreaded = Some(receipt),
			| "main" => entry.main = Some(receipt),
			| thread =>
				if let Ok(Some(root)) = thread_root(thread) {
					entry.threads.push((root, receipt));
				},
		}
	}

	for ((user_id, room_id), latest) in latest {
		let unthreaded_at = latest.unthreaded.as_ref().map(|(at, _)| *at);
		let room = latest
			.unthreaded
			.into_iter()
			.chain(latest.main)
			.max();
		if let Some((_, event_id)) = room {
			stamp(services, (&user_id, &room_id, None), &event_id).await;
		}

		for (root, (at, event_id)) in latest.threads {
			if unthreaded_at.is_none_or(|unthreaded| at > unthreaded) {
				stamp(services, (&user_id, &room_id, Some(&root)), &event_id).await;
			}
		}
	}

	Ok(())
}

type Key<'a> = (&'a OwnedUserId, &'a OwnedRoomId, Option<&'a EventId>);

async fn stamp(services: &Services, (user_id, room_id, root): Key<'_>, event_id: &EventId) {
	// A receipt for an event that wasn't imported was reported by receipts.
	if let Ok(PduCount::Normal(count)) = services.timeline.get_pdu_count(event_id).await {
		services
			.pusher
			.import_last_read(user_id, room_id, root, count);
	}
}
