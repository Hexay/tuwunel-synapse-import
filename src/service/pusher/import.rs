//! Writes for `--import-synapse`, which carries over the unread counts, read
//! stamps and notification list the live append and receipt paths would have
//! built up. Each overwrites, so a resumed import can repeat it.

use ruma::{EventId, RoomId, UserId};
use tuwunel_core::implement;
use tuwunel_database::Json;

use super::Notified;

/// Sets one unread counter pair: the main timeline's, or a thread's.
#[implement(super::Service)]
pub fn import_unread_counts(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	thread_root: Option<&EventId>,
	(notifications, highlights): (u64, u64),
) {
	let db = &self.db;
	match thread_root {
		| None => {
			db.userroomid_notificationcount
				.put((user_id, room_id), notifications);
			db.userroomid_highlightcount
				.put((user_id, room_id), highlights);
		},
		| Some(root) => {
			db.userroomid_notificationcount
				.put((user_id, room_id, root), notifications);
			db.userroomid_highlightcount
				.put((user_id, room_id, root), highlights);
		},
	}
}

/// Stamps the room, or one thread, read up to the PDU at `count`.
#[implement(super::Service)]
pub fn import_last_read(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	thread_root: Option<&EventId>,
	count: u64,
) {
	let db = &self.db.roomuserid_lastnotificationread;
	match thread_root {
		| None => db.put((room_id, user_id), count),
		| Some(root) => db.put((room_id, user_id, root), count),
	}
}

/// Adds the user's notification for the PDU at `count`.
#[implement(super::Service)]
pub fn import_notification(&self, user_id: &UserId, count: u64, notified: &Notified) {
	self.db
		.useridcount_notification
		.put((user_id, count), Json(notified));
}
