//! Synapse's state groups, turned into the state before each event.
//!
//! A group is its parent group's state plus a delta, and Synapse records the
//! state after each event. The state before a state event is its group with the
//! event's own key set back to what its predecessors had (see
//! docs/04-design.md 4.4.2).

use std::collections::{HashMap, hash_map::Entry};

use ruma::{EventId, RoomId, events::StateEventType};
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
	keys: HashMap<(String, String), ShortStateKey>,
	cache: HashMap<i64, State>,
}

#[derive(Default)]
struct Group {
	parent: Option<i64>,
	delta: Vec<(ShortStateKey, ShortEventId)>,
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
			.rows("SELECT id FROM state_groups WHERE room_id = $1", room)
			.await?
		{
			groups.entry(row.get(0)).or_default();
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

		Ok(Self { groups, keys, cache: HashMap::new() })
	}

	/// The state before `event`, or None when Synapse's groups don't determine
	/// it: an unknown predecessor, or predecessors that disagree on the event's
	/// key, which would need state resolution.
	pub(super) fn before(
		&mut self,
		event: &Event,
		after_of: impl Fn(&EventId) -> Option<i64>,
	) -> Result<Option<CompressedState>> {
		let Some(after) = event.after else {
			return Ok(None);
		};

		let pdu = &event.pdu;
		let Some(state_key) = pdu.state_key() else {
			return Ok(Some(compress(self.state(after)?)));
		};

		let Some(&key) = self
			.keys
			.get(&(pdu.kind().to_string(), state_key.to_owned()))
		else {
			return Ok(None);
		};

		// What the predecessors had for this key; nothing, for the create event.
		let mut prior: Option<Option<ShortEventId>> = None;
		for prev in pdu.prev_events() {
			let Some(group) = after_of(prev) else {
				return Ok(None);
			};

			let value = self.state(group)?.get(&key).copied();
			if prior.is_some_and(|prior| prior != value) {
				return Ok(None);
			}

			prior = Some(value);
		}

		let mut state = self.state(after)?.clone();
		match prior.flatten() {
			| Some(value) => state.insert(key, value),
			| None => state.remove(&key),
		};

		Ok(Some(compress(&state)))
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

fn compress(state: &State) -> CompressedState {
	state
		.iter()
		.map(|(&key, &event)| compress_state_event(key, event))
		.collect()
}
