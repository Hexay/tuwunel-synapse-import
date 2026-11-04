//! Device-list changes per room. The crypto phase stores keys before the rooms
//! phase writes memberships, so no room records them; a client whose sync
//! position tuwunel can't read restarts from zero and must still be told to
//! re-query every peer, whose devices may have changed since it last synced.

use futures::StreamExt;
use tuwunel_core::Result;
use tuwunel_service::Services;

pub(super) async fn import(services: &Services) -> Result {
	let rooms: Vec<_> = services
		.metadata
		.iter_ids()
		.map(ToOwned::to_owned)
		.collect()
		.await;

	for room_id in rooms {
		services
			.users
			.mark_room_members_device_lists_changed(&room_id)
			.await;
	}

	Ok(())
}
