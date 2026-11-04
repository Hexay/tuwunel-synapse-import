//! Rooms a user left and then forgot, which tuwunel then leaves out of their
//! sync as Synapse did. A later rejoin is a new membership, not forgotten.

use ruma::OwnedRoomId;
use tuwunel_core::{Result, err};
use tuwunel_service::Services;

use super::super::{
	Report,
	source::{Source, user_id},
};

pub(super) async fn import(services: &Services, source: &Source, report: &mut Report) -> Result {
	let rows = source
		.rows(
			"SELECT c.user_id, c.room_id FROM local_current_membership c JOIN room_memberships \
			 m USING (event_id) WHERE m.forgotten = 1 AND c.membership IN ('leave', 'ban') \
			 ORDER BY 1, 2",
			&[],
		)
		.await?;

	for row in rows {
		let room_id = OwnedRoomId::try_from(row.get::<_, &str>(1))
			.map_err(|e| err!("room ID {:?}: {e}", row.get::<_, &str>(1)));

		let (Some(user_id), Some(room_id)) = (
			report.keep("forgotten room", user_id(&row, 0)),
			report.keep("forgotten room", room_id),
		) else {
			continue;
		};

		services.state_cache.forget(&room_id, &user_id);
		Report::count(&mut report.forgotten_rooms);
	}

	Ok(())
}
