//! Rooms: their events and state, aliases and the public room directory.

mod events;
mod room;
mod state;

use std::collections::HashSet;

use ruma::{OwnedRoomAliasId, OwnedRoomId, OwnedUserId, RoomVersionId};
use tokio_postgres::Row;
use tuwunel_core::{Result, err};
use tuwunel_service::Services;

use super::{
	Report, marker_present,
	source::{Source, user_id},
	stamp,
};

pub(super) async fn import(services: &Services, source: &Source, report: &mut Report) -> Result {
	let erased: HashSet<OwnedUserId> = source
		.rows("SELECT user_id FROM erased_users", &[])
		.await?
		.iter()
		.filter_map(|row| user_id(row, 0).ok())
		.collect();

	let rows = source
		.rows("SELECT room_id, room_version, is_public FROM rooms ORDER BY room_id", &[])
		.await?;

	for row in rows {
		let room_id: OwnedRoomId = row
			.get::<_, &str>(0)
			.try_into()
			.map_err(|e| err!("Synapse room ID: {e}"))?;

		// Per room as well as per phase, so a rerun doesn't redo finished rooms.
		let marker = format!("synapse_import_room:{room_id}");
		if marker_present(services, &marker) {
			continue;
		}

		let version: RoomVersionId = row
			.get::<_, &str>(1)
			.try_into()
			.map_err(|e| err!("{room_id}: room version: {e}"))?;

		room::import(services, source, &room_id, &version, &erased, report).await?;

		if row.get::<_, bool>(2) {
			services.directory.set_public(&room_id, None);
			Report::count(&mut report.public_rooms);
		}

		stamp(services, &marker);
		Report::count(&mut report.rooms);
	}

	import_aliases(services, source, report).await
}

async fn import_aliases(services: &Services, source: &Source, report: &mut Report) -> Result {
	let rows = source
		.rows("SELECT room_alias, room_id, creator FROM room_aliases ORDER BY room_alias", &[])
		.await?;

	for row in rows {
		match import_alias(services, &row) {
			| Ok(()) => Report::count(&mut report.aliases),
			| Err(e) => report.skip(format!("alias {}: {e}", row.get::<_, &str>(0))),
		}
	}

	Ok(())
}

// A Synapse room holding tuwunel's own admin alias is refused here.
fn import_alias(services: &Services, row: &Row) -> Result {
	let alias = OwnedRoomAliasId::try_from(row.get::<_, &str>(0)).map_err(|e| err!("{e}"))?;
	let room_id = OwnedRoomId::try_from(row.get::<_, &str>(1)).map_err(|e| err!("{e}"))?;

	services
		.alias
		.set_alias_by(&alias, &room_id, &user_id(row, 2)?)
}
