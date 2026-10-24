//! One room: its events, the state at each, its current state and forward
//! extremities. Modelled on how tuwunel accepts a federated event: stored as
//! an outlier first, then given its state and appended.

use std::{
	collections::{HashMap, HashSet},
	iter::once,
	sync::Arc,
};

use ruma::{EventId, OwnedEventId, OwnedUserId, RoomId, RoomVersionId, events::StateEventType};
use tuwunel_core::{Result, err};
use tuwunel_matrix::{Event as _, room_version};
use tuwunel_service::{
	Services,
	rooms::{state::RoomMutexGuard, state_compressor::CompressedState},
};

use super::{
	super::{Report, source::Source},
	events,
	state::StateGroups,
};

pub(super) async fn import(
	services: &Services,
	source: &Source,
	room_id: &RoomId,
	version: &RoomVersionId,
	erased: &HashSet<OwnedUserId>,
	report: &mut Report,
) -> Result {
	let rules = room_version::rules(version)?;
	let events = events::load(source, room_id, version, &rules, erased, report).await?;
	let mut groups = StateGroups::load(services, source, room_id).await?;

	services
		.short
		.get_or_create_shortroomid(room_id)
		.await;

	// Auth chains and rejected events stay outliers; the rest are upgraded below.
	for event in &events {
		services
			.timeline
			.add_pdu_outlier(event.pdu.event_id(), &event.json);
	}

	let after: HashMap<&EventId, i64> = events
		.iter()
		.filter_map(|event| Some((event.pdu.event_id(), event.after?)))
		.collect();

	let state_lock = services.state.mutex.lock(room_id).await;

	for event in events.iter().filter(|event| event.timeline) {
		let event_id = event.pdu.event_id();

		// Appended by an earlier run that stopped partway through this room.
		if services
			.timeline
			.get_pdu_id(event_id)
			.await
			.is_ok()
		{
			continue;
		}

		let Some(before) = groups.before(event, |prev| after.get(prev).copied())? else {
			report.skip(format!(
				"{room_id} event {event_id}: Synapse doesn't record the state before it"
			));
			continue;
		};

		let shortstatehash = services
			.state
			.set_event_state(event_id, room_id, Arc::new(before))
			.await?;

		// The next event's state is stored as a diff against the room's current one.
		services
			.state
			.set_room_state(room_id, shortstatehash, &state_lock);

		if event.soft_failed {
			services
				.pdu_metadata
				.mark_as_referenced(room_id, event.pdu.prev_events());
			services
				.pdu_metadata
				.mark_event_soft_failed(event_id);
			Report::count(&mut report.soft_failed);
			continue;
		}

		services
			.timeline
			.append_imported_pdu(&event.pdu, event.json.clone(), once(event_id), &state_lock)
			.await?;

		Report::count(&mut report.events);
	}

	set_current_state(services, source, room_id, &state_lock).await?;
	set_forward_extremities(services, source, room_id, &state_lock).await?;

	report.outliers = report.outliers.saturating_add(
		events
			.iter()
			.filter(|event| !event.timeline)
			.count(),
	);

	Ok(())
}

/// Synapse's current state, installed as the room's, with membership caches
/// brought in line.
async fn set_current_state(
	services: &Services,
	source: &Source,
	room_id: &RoomId,
	state_lock: &RoomMutexGuard,
) -> Result {
	let rows = source
		.rows(
			"SELECT type, state_key, event_id FROM current_state_events WHERE room_id = $1",
			&[&room_id.as_str()],
		)
		.await?;

	if rows.is_empty() {
		return Ok(());
	}

	let mut state = CompressedState::new();
	for row in rows {
		let event_type = StateEventType::from(row.get::<_, &str>(0));
		let key = services
			.short
			.get_or_create_shortstatekey(&event_type, row.get(1))
			.await;

		let event_id: &EventId = row
			.get::<_, &str>(2)
			.try_into()
			.map_err(|e| err!("{room_id}: invalid event ID in current state: {e}"))?;

		state.insert(
			services
				.state_compressor
				.compress_state_event(key, event_id)
				.await,
		);
	}

	let saved = services
		.state_compressor
		.save_state(room_id, Arc::new(state))
		.await?;

	services
		.state
		.force_state(room_id, saved.shortstatehash, saved.added, saved.removed, state_lock)
		.await
}

async fn set_forward_extremities(
	services: &Services,
	source: &Source,
	room_id: &RoomId,
	state_lock: &RoomMutexGuard,
) -> Result {
	let extremities = source
		.rows("SELECT event_id FROM event_forward_extremities WHERE room_id = $1", &[
			&room_id.as_str()
		])
		.await?
		.iter()
		.map(|row| OwnedEventId::try_from(row.get::<_, &str>(0)))
		.collect::<Result<Vec<_>, _>>()
		.map_err(|e| err!("{room_id}: invalid forward extremity: {e}"))?;

	services
		.state
		.set_forward_extremities(room_id, extremities.iter().map(AsRef::as_ref), state_lock)
		.await;

	Ok(())
}
