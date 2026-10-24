//! Synapse's state groups, turned into the state before each event.
//!
//! A group is its parent group's state plus a delta. Synapse records the state
//! after an event; tuwunel wants the state before it, which for a state event
//! is the parent of the group Synapse made for it (see docs/04-design.md 4.4.2).

use std::collections::{HashMap, hash_map::Entry};

use ruma::{EventId, OwnedEventId, RoomId, events::StateEventType};
use tokio_postgres::types::ToSql;
use tuwunel_core::{Err, Result, err};
use tuwunel_matrix::Event as _;
use tuwunel_service::{
	Services,
	rooms::{
		short::{ShortEventId, ShortStateKey},
		state_compressor::{CompressedState, compress_state_event},
	},
};

use super::{super::source::Source, events::Event};

type State = HashMap<ShortStateKey, ShortEventId>;

/// Fully built states kept for reuse; consecutive events mostly share a group.
const CACHE_LIMIT: usize = 64;

pub(super) struct StateGroups {
	groups: HashMap<i64, Group>,
	cache: HashMap<i64, State>,
}

#[derive(Default)]
struct Group {
	parent: Option<i64>,

	/// The event this group was created for.
	event_id: Option<OwnedEventId>,

	delta: Vec<(ShortStateKey, ShortEventId)>,
}

/// Where the state before an event comes from.
pub(super) enum Before {
	Group(i64),

	/// The create event: nothing came before it.
	Nothing,
}

impl StateGroups {
	// TODO: stream state_groups_state; a large room's deltas don't need to be in memory at once.
	pub(super) async fn load(
		services: &Services,
		source: &Source,
		room_id: &RoomId,
	) -> Result<Self> {
		let room: &[&(dyn ToSql + Sync)] = &[&room_id.as_str()];
		let mut groups: HashMap<i64, Group> = HashMap::new();

		for row in source
			.rows("SELECT id, event_id FROM state_groups WHERE room_id = $1", room)
			.await?
		{
			let event_id = row
				.get::<_, Option<&str>>(1)
				.and_then(|event_id| OwnedEventId::try_from(event_id).ok());

			groups.entry(row.get(0)).or_default().event_id = event_id;
		}

		for row in source
			.rows(
				"SELECT e.state_group, e.prev_state_group FROM state_group_edges e JOIN \
				 state_groups g ON g.id = e.state_group WHERE g.room_id = $1",
				room,
			)
			.await?
		{
			groups.entry(row.get(0)).or_default().parent = Some(row.get(1));
		}

		let mut keys: HashMap<(String, String), ShortStateKey> = HashMap::new();
		for row in source
			.rows(
				"SELECT state_group, type, state_key, event_id FROM state_groups_state WHERE \
				 room_id = $1",
				room,
			)
			.await?
		{
			let key = match keys.entry((row.get(1), row.get(2))) {
				| Entry::Occupied(entry) => *entry.get(),
				| Entry::Vacant(entry) => {
					let (event_type, state_key) = entry.key();
					let short = services
						.short
						.get_or_create_shortstatekey(
							&StateEventType::from(event_type.as_str()),
							state_key,
						)
						.await;

					*entry.insert(short)
				},
			};

			let event_id: &EventId = row
				.get::<_, &str>(3)
				.try_into()
				.map_err(|e| err!("Synapse state group has an invalid event ID: {e}"))?;

			let event = services
				.short
				.get_or_create_shorteventid(event_id)
				.await;

			groups
				.entry(row.get(0))
				.or_default()
				.delta
				.push((key, event));
		}

		Ok(Self { groups, cache: HashMap::new() })
	}

	/// Where the state before `event` comes from, or None when Synapse's groups
	/// don't say.
	pub(super) fn before(
		&self,
		event: &Event,
		after_of: impl Fn(&EventId) -> Option<i64>,
	) -> Option<Before> {
		let after = event.after?;
		let pdu = &event.pdu;
		if pdu.state_key().is_none() {
			return Some(Before::Group(after));
		}

		let group = self.groups.get(&after)?;
		if group.event_id.as_deref() == Some(pdu.event_id()) {
			return match group.parent {
				| Some(parent) => Some(Before::Group(parent)),
				| None if pdu.prev_events().next().is_none() => Some(Before::Nothing),
				| None => None,
			};
		}

		// The group wasn't made for this event: fall back to the state after its
		// only predecessor. Several predecessors would need state resolution.
		let mut prev = pdu.prev_events();
		match (prev.next(), prev.next()) {
			| (Some(prev), None) => after_of(prev).map(Before::Group),
			| _ => None,
		}
	}

	pub(super) fn compressed(&mut self, before: &Before) -> Result<CompressedState> {
		let state = match before {
			| Before::Nothing => return Ok(CompressedState::new()),
			| Before::Group(group) => self.state(*group)?,
		};

		Ok(state
			.iter()
			.map(|(&key, &event)| compress_state_event(key, event))
			.collect())
	}

	fn state(&mut self, group: i64) -> Result<&State> {
		if !self.cache.contains_key(&group) {
			let state = self.build(group)?;
			if self.cache.len() >= CACHE_LIMIT {
				self.cache.clear();
			}

			self.cache.insert(group, state);
		}

		Ok(&self.cache[&group])
	}

	/// Walks up to the nearest cached ancestor or the root, then applies each
	/// delta on the way back down.
	fn build(&self, group: i64) -> Result<State> {
		let mut chain = Vec::new();
		let mut state = State::new();
		let mut next = Some(group);

		while let Some(id) = next {
			if let Some(cached) = self.cache.get(&id) {
				state.clone_from(cached);
				break;
			}

			let Some(found) = self.groups.get(&id) else {
				return Err!("Synapse state group {id} is referenced but missing");
			};

			chain.push(found);
			next = found.parent;
		}

		for found in chain.iter().rev() {
			state.extend(found.delta.iter().copied());
		}

		Ok(state)
	}
}
