//! A room's current state, entry by entry, `(type, state_key) -> event ID`;
//! that each state event is stored; and the membership lists tuwunel derives
//! from it (joined, invited, knocking).

use std::collections::{BTreeMap, BTreeSet};

use futures::StreamExt;
use ruma::EventId;
use tuwunel_core::Result;

use super::{Ctx, rooms::Room};

type State = BTreeMap<(String, String), String>;

pub(super) async fn verify(ctx: &mut Ctx<'_>, room: &Room<'_>) -> Result {
	let rows = ctx
		.source
		.rows(
			"SELECT c.type, c.state_key, c.event_id, m.membership FROM current_state_events c \
			 LEFT JOIN room_memberships m USING (event_id) WHERE c.room_id = $1",
			&[&room.id.as_str()],
		)
		.await?;

	let mut want = State::new();
	let mut members: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
	for row in &rows {
		let (kind, state_key, event_id): (&str, &str, &str) =
			(row.get(0), row.get(1), row.get(2));
		if <&EventId>::try_from(event_id).is_err() {
			ctx.out.excuse(
				"state",
				format!("{} {kind} {state_key:?}", room.id),
				"invalid event ID",
			);
			continue;
		}

		if kind == "m.room.member" {
			let membership: Option<&str> = row.get(3);
			members
				.entry(membership.unwrap_or_default())
				.or_default()
				.insert(state_key.to_owned());
		}

		want.insert((kind.to_owned(), state_key.to_owned()), event_id.to_owned());
	}

	let have = current_state(ctx, room).await;
	compare(ctx, room, &want, &have);

	for event_id in want.values() {
		let Ok(id) = <&EventId>::try_from(event_id.as_str()) else {
			continue;
		};

		if ctx
			.services
			.timeline
			.get_pdu_json(id)
			.await
			.is_ok()
		{
			ctx.out.matched("state_events");
		} else {
			room.lacks(ctx, "state_events", event_id, "the state event isn't stored")
				.await?;
		}
	}

	memberships(ctx, room, &members).await;
	Ok(())
}

/// Entries whose short IDs don't resolve come back under a placeholder type,
/// so they show up as a mismatch rather than vanish.
async fn current_state(ctx: &Ctx<'_>, room: &Room<'_>) -> State {
	let services = ctx.services;
	let Ok(shortstatehash) = services
		.state
		.get_room_shortstatehash(room.id)
		.await
	else {
		return State::new();
	};

	let entries: Vec<_> = services
		.state_accessor
		.state_full_ids(shortstatehash)
		.collect()
		.await;

	let mut state = State::new();
	for (shortstatekey, event_id) in entries {
		let key = match services
			.short
			.get_statekey_from_short(shortstatekey)
			.await
		{
			| Ok((kind, state_key)) => (kind.to_string(), state_key.to_string()),
			| Err(_) =>
				(format!("<unresolvable short state key {shortstatekey}>"), String::new()),
		};

		state.insert(key, event_id.to_string());
	}

	state
}

fn compare(ctx: &mut Ctx<'_>, room: &Room<'_>, want: &State, have: &State) {
	for (key, event_id) in want {
		let subject = || format!("{} {} {:?}", room.id, key.0, key.1);
		match have.get(key) {
			| Some(have) if have == event_id => ctx.out.matched("state"),
			| Some(have) =>
				ctx.out
					.differs("state", subject(), format!("Synapse {event_id}, tuwunel {have}")),
			| None => ctx.out.missing(
				"state",
				subject(),
				format!("not in the current state (Synapse {event_id})"),
			),
		}
	}

	for (key, event_id) in have
		.iter()
		.filter(|(key, _)| !want.contains_key(*key))
	{
		ctx.out.extra(
			"state",
			format!("{} {} {:?}", room.id, key.0, key.1),
			format!("not in Synapse's state ({event_id})"),
		);
	}
}

async fn memberships(
	ctx: &mut Ctx<'_>,
	room: &Room<'_>,
	members: &BTreeMap<&str, BTreeSet<String>>,
) {
	let cache = &ctx.services.state_cache;
	let lists = [
		(
			"join",
			cache
				.room_members(room.id)
				.map(ToString::to_string)
				.collect::<BTreeSet<_>>()
				.await,
		),
		(
			"invite",
			cache
				.room_members_invited(room.id)
				.map(ToString::to_string)
				.collect()
				.await,
		),
		(
			"knock",
			cache
				.room_members_knocked(room.id)
				.map(ToString::to_string)
				.collect()
				.await,
		),
	];

	let empty = BTreeSet::new();
	for (membership, have) in lists {
		let want = members.get(membership).unwrap_or(&empty);
		want.intersection(&have)
			.for_each(|_| ctx.out.matched("memberships"));
		for user in want.difference(&have) {
			ctx.out.missing(
				"memberships",
				format!("{} {user}", room.id),
				format!("not listed as {membership}"),
			);
		}

		for user in have.difference(want) {
			ctx.out.extra(
				"memberships",
				format!("{} {user}", room.id),
				format!("listed as {membership}, not in Synapse"),
			);
		}
	}
}
