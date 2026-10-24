//! A room's events as Synapse stored them, checked and converted to tuwunel's
//! stored form.

use std::collections::HashSet;

use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, OwnedEventId, OwnedUserId, RoomId, RoomVersionId,
	canonical_json::redact_in_place, room_version_rules::RoomVersionRules,
};
use serde_json::Value;
use tuwunel_core::{Result, err};
use tuwunel_matrix::{PduEvent, event::gen_event_id};

use super::super::{Report, source::Source};

pub(super) struct Event {
	pub(super) pdu: PduEvent,
	pub(super) json: CanonicalJsonObject,

	/// In the room's timeline: neither an outlier nor rejected.
	pub(super) timeline: bool,

	/// Accepted but hidden, because it failed auth against the room's state.
	pub(super) soft_failed: bool,

	/// Synapse's state group for the state after this event.
	pub(super) after: Option<i64>,
}

/// Every event of the room in Synapse's stream order. Backfilled events have
/// negative stream orderings, so they come first, oldest first: appending in
/// this order keeps tuwunel's timeline chronological without its backfill path.
pub(super) async fn load(
	source: &Source,
	room_id: &RoomId,
	version: &RoomVersionId,
	rules: &RoomVersionRules,
	erased: &HashSet<OwnedUserId>,
	report: &mut Report,
) -> Result<Vec<Event>> {
	let rows = source
		.rows(
			"SELECT e.event_id, e.outlier, e.rejection_reason IS NOT NULL OR r.event_id IS NOT \
			 NULL, j.internal_metadata, j.json, s.state_group FROM events e JOIN event_json j \
			 USING (event_id) LEFT JOIN rejections r USING (event_id) LEFT JOIN \
			 event_to_state_groups s USING (event_id) WHERE e.room_id = $1 ORDER BY \
			 e.stream_ordering",
			&[&room_id.as_str()],
		)
		.await?;

	let mut events = Vec::with_capacity(rows.len());
	for row in rows {
		let event_id: &str = row.get(0);
		match convert(room_id, event_id, row.get(4), version, rules, erased) {
			| Ok((pdu, json)) => {
				let (outlier, rejected): (bool, bool) = (row.get(1), row.get(2));
				events.push(Event {
					pdu,
					json,
					timeline: !outlier && !rejected,
					soft_failed: soft_failed(row.get(3)),
					after: row.get(5),
				});
			},
			| Err(e) => report.skip(format!("{room_id} event {event_id}: {e}")),
		}
	}

	Ok(events)
}

fn convert(
	room_id: &RoomId,
	event_id: &str,
	json: &str,
	version: &RoomVersionId,
	rules: &RoomVersionRules,
	erased: &HashSet<OwnedUserId>,
) -> Result<(PduEvent, CanonicalJsonObject)> {
	// Old room versions allow floats and integers beyond 2^53, which canonical JSON refuses.
	let mut json: CanonicalJsonObject = serde_json::from_str(json)
		.map_err(|e| err!("not canonical JSON, which tuwunel can't store: {e}"))?;

	// Not covered by hashes or signatures; tuwunel rebuilds it.
	json.remove("unsigned");

	let event_id: OwnedEventId = event_id
		.try_into()
		.map_err(|e| err!("invalid event ID: {e}"))?;

	if gen_event_id(&json, version)? != event_id {
		return Err(err!("its content doesn't hash to its event ID"));
	}

	// Synapse prunes an erased user's events whenever it serves them.
	let sender = json
		.get("sender")
		.and_then(CanonicalJsonValue::as_str)
		.and_then(|sender| OwnedUserId::try_from(sender).ok());

	if sender.is_some_and(|sender| erased.contains(&sender)) {
		redact_in_place(&mut json, &rules.redaction, None)
			.map_err(|e| err!("cannot prune an erased user's event: {e}"))?;
	}

	PduEvent::from_object_federation(room_id, &event_id, json, rules)
}

fn soft_failed(internal_metadata: &str) -> bool {
	serde_json::from_str::<Value>(internal_metadata)
		.ok()
		.and_then(|metadata| metadata.get("soft_failed")?.as_bool())
		.unwrap_or(false)
}
