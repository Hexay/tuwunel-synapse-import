//! Rooms: each Synapse room's version, directory listing, timeline, current
//! state, memberships and forward extremities, plus a deep check of a sample
//! of its events; then rooms tuwunel has that Synapse doesn't, and aliases.

use std::collections::{BTreeSet, HashSet};

use futures::StreamExt;
use ruma::{
	CanonicalJsonObject, EventId, OwnedEventId, OwnedRoomAliasId, OwnedRoomId, OwnedUserId,
	RoomId, RoomVersionId, UserId, room_version_rules::EventsReferenceFormatVersion,
};
use tuwunel_core::{Result, info};
use tuwunel_matrix::{RoomVersionRules, pdu::from_incoming_federation, room_version};

use super::{
	super::{
		rooms::{convert, room_version as supported_version},
		source::user_id,
	},
	Ctx, events,
	keys::Keys,
	state, timeline,
};

/// One room being verified.
pub(super) struct Room<'a> {
	pub(super) id: &'a RoomId,
	pub(super) version: RoomVersionId,
	pub(super) rules: RoomVersionRules,
	erased: &'a HashSet<OwnedUserId>,
}

impl Room<'_> {
	/// An event of this room tuwunel lacks: a mismatch, unless the import
	/// could never have stored it (Synapse lacks it, or it doesn't convert).
	/// Returns whether it was a mismatch.
	pub(super) async fn lacks(
		&self,
		ctx: &mut Ctx<'_>,
		domain: &'static str,
		event_id: &str,
		detail: &str,
	) -> Result<bool> {
		let rows = ctx
			.source
			.rows("SELECT json FROM event_json WHERE event_id = $1", &[&event_id])
			.await?;

		let subject = format!("{} {event_id}", self.id);
		let Some(row) = rows.first() else {
			ctx.out
				.excuse(domain, subject, "Synapse doesn't have the event either");
			return Ok(false);
		};

		match convert(self.id, event_id, row.get(0), &self.version, &self.rules, self.erased) {
			| Ok(_) => match state::unplaceable(ctx, event_id).await? {
				| None => ctx.out.missing(domain, subject, detail),
				| Some(why) => {
					ctx.out
						.excuse(domain, subject, format!("not importable: {why}"));
					return Ok(false);
				},
			},
			| Err(e) => {
				ctx.out
					.excuse(domain, subject, format!("not importable: {e}"));
				return Ok(false);
			},
		}

		Ok(true)
	}

	/// Synapse's federation form of an event as tuwunel stores it.
	pub(super) fn stored_form(
		&self,
		event_id: &EventId,
		event: CanonicalJsonObject,
	) -> CanonicalJsonObject {
		from_incoming_federation(self.id, event_id, event, &self.rules)
	}

	pub(super) fn is_erased(&self, user_id: &UserId) -> bool { self.erased.contains(user_id) }

	/// Room versions 1 and 2 reference events with their hashes, which
	/// tuwunel's stored form drops.
	pub(super) fn hashless_references(&self) -> bool {
		matches!(self.rules.events_reference_format, EventsReferenceFormatVersion::V1)
	}
}

pub(super) async fn verify(ctx: &mut Ctx<'_>) -> Result {
	let services = ctx.services;
	let erased: HashSet<OwnedUserId> = ctx
		.source
		.rows("SELECT user_id FROM erased_users", &[])
		.await?
		.iter()
		.filter_map(|row| user_id(row, 0).ok())
		.collect();

	let mut keys = Keys::load(ctx.source).await?;
	let rows = ctx
		.source
		.rows(
			"SELECT room_id, room_version, coalesce(is_public, false) FROM rooms ORDER BY \
			 room_id",
			&[],
		)
		.await?;

	let total = rows.len();
	let mut synapse = HashSet::with_capacity(total);
	for (i, row) in rows.iter().enumerate() {
		let Ok(room_id) = OwnedRoomId::try_from(row.get::<_, &str>(0)) else {
			ctx.out
				.excuse("rooms", row.get::<_, &str>(0), "not a valid room ID");
			continue;
		};

		synapse.insert(room_id.clone());
		let exists = services.metadata.exists(&room_id).await;
		let version = match supported_version(row.get(1)) {
			| Ok(version) => version,
			| Err(e) if exists => {
				ctx.out
					.differs("rooms", &room_id, format!("imported, though {e}"));
				continue;
			},
			| Err(e) => {
				ctx.out.excuse("rooms", &room_id, e);
				continue;
			},
		};

		if !exists {
			ctx.out.missing("rooms", &room_id, "no such room");
			continue;
		}

		info!("Verifying room {room_id} ({} of {total})", i.saturating_add(1));
		ctx.out.rooms = ctx.out.rooms.saturating_add(1);
		let room = Room {
			id: &room_id,
			rules: room_version::rules(&version)?,
			version,
			erased: &erased,
		};

		metadata(ctx, &room, row.get(2)).await;
		timeline::walk(ctx, &room).await?;
		state::verify(ctx, &room).await?;
		extremities(ctx, &room).await?;
		events::sample(ctx, &room, &mut keys).await?;
	}

	let admin_room = services.admin.get_admin_room().await.ok();
	let tuwunel: Vec<OwnedRoomId> = services
		.metadata
		.iter_ids()
		.map(ToOwned::to_owned)
		.collect()
		.await;
	for room_id in tuwunel {
		if !synapse.contains(&room_id) && admin_room.as_ref() != Some(&room_id) {
			ctx.out
				.extra("rooms", room_id, "not a Synapse room");
		}
	}

	aliases(ctx).await
}

async fn metadata(ctx: &mut Ctx<'_>, room: &Room<'_>, public: bool) {
	let services = ctx.services;
	let version = services.state.get_room_version(room.id).await;
	let listed = services.directory.is_public_room(room.id).await;

	if version.as_ref().ok() != Some(&room.version) {
		ctx.out.differs(
			"rooms",
			room.id,
			format!("version: Synapse {}, tuwunel {version:?}", room.version),
		);
	} else if listed != public {
		ctx.out.differs(
			"rooms",
			room.id,
			format!("in the public directory: Synapse {public}, tuwunel {listed}"),
		);
	} else {
		ctx.out.matched("rooms");
	}
}

async fn extremities(ctx: &mut Ctx<'_>, room: &Room<'_>) -> Result {
	let want: BTreeSet<String> = ctx
		.source
		.rows("SELECT event_id FROM event_forward_extremities WHERE room_id = $1", &[&room
			.id
			.as_str()])
		.await?
		.iter()
		.map(|row| row.get(0))
		.collect();

	let have: BTreeSet<OwnedEventId> = ctx
		.services
		.state
		.get_forward_extremities(room.id)
		.map(ToOwned::to_owned)
		.collect()
		.await;

	for event_id in &want {
		if have.iter().any(|have| have.as_str() == event_id) {
			ctx.out.matched("extremities");
		} else {
			room.lacks(ctx, "extremities", event_id, "not a forward extremity")
				.await?;
		}
	}

	for event_id in have
		.iter()
		.filter(|have| !want.contains(have.as_str()))
	{
		ctx.out.extra(
			"extremities",
			format!("{} {event_id}", room.id),
			"not a Synapse forward extremity",
		);
	}

	Ok(())
}

async fn aliases(ctx: &mut Ctx<'_>) -> Result {
	let services = ctx.services;
	let rows = ctx
		.source
		.rows("SELECT room_alias, room_id FROM room_aliases", &[])
		.await?;

	for row in rows {
		let (alias, room): (&str, &str) = (row.get(0), row.get(1));
		let Ok(alias_id) = OwnedRoomAliasId::try_from(alias) else {
			ctx.out
				.excuse("aliases", alias, "not a valid alias");
			continue;
		};

		if alias_id == services.admin.admin_alias {
			ctx.out
				.excuse("aliases", alias, "tuwunel's admin room alias");
			continue;
		}

		match services
			.alias
			.resolve_local_alias(&alias_id)
			.await
		{
			| Ok(room_id) if room_id.as_str() == room => ctx.out.matched("aliases"),
			| Ok(room_id) =>
				ctx.out
					.differs("aliases", alias, format!("points at {room_id}, not {room}")),
			| Err(_) => ctx
				.out
				.missing("aliases", alias, "doesn't resolve"),
		}
	}

	Ok(())
}
