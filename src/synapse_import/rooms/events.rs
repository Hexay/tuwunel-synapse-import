//! A room's events as Synapse stored them, checked and converted to tuwunel's
//! stored form.
//!
//! Read in two passes, so a room never has to fit in memory: every event is
//! first stored as an outlier, then the timeline is replayed from light rows,
//! taking each event back from tuwunel.

use std::{collections::HashSet, pin::pin};

use futures::{Stream, TryStreamExt};
use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, OwnedEventId, OwnedUserId, RoomId, RoomVersionId,
	canonical_json::redact_in_place, room_version_rules::RoomVersionRules,
};
use serde_json::Value;
use tuwunel_core::{Result, err};
use tuwunel_matrix::{Event as _, PduEvent, event::gen_event_id, room_version};
use tuwunel_service::Services;

use super::super::{Report, source::Source};

/// An event's place in the room's timeline.
pub(super) struct Placement {
	pub(super) event_id: OwnedEventId,

	/// Accepted but hidden, because it failed auth against the room's state.
	pub(super) soft_failed: bool,
}

/// Stores every event of the room as an outlier. Auth chains and rejected
/// events stay that way; the timeline's events are appended from them later.
/// Events tuwunel can't store are reported and left out.
pub(super) async fn store_outliers(
	services: &Services,
	source: &Source,
	room_id: &RoomId,
	version: &RoomVersionId,
	erased: &HashSet<OwnedUserId>,
	report: &mut Report,
) -> Result {
	let rules = room_version::rules(version)?;
	let mut rows = pin!(
		source
			.stream(
				"SELECT e.event_id, j.json, e.outlier OR e.rejection_reason IS NOT NULL OR \
				 r.event_id IS NOT NULL FROM events e JOIN event_json j USING (event_id) LEFT \
				 JOIN rejections r USING (event_id) WHERE e.room_id = $1",
				&[&room_id.as_str()],
			)
			.await?
	);

	while let Some(row) = rows.try_next().await? {
		let event_id: &str = row.get(0);
		match convert(room_id, event_id, row.get(1), version, &rules, erased) {
			| Ok((pdu, json)) => {
				services
					.timeline
					.add_pdu_outlier(pdu.event_id(), &json);

				if row.get::<_, bool>(2) {
					Report::count(&mut report.outliers);
				}
			},
			| Err(e) => report.skip(format!("{room_id} event {event_id}: {e}")),
		}
	}

	Ok(())
}

/// The room's timeline in Synapse's stream order. Backfilled events have
/// negative stream orderings, so they come first, oldest first: appending in
/// this order keeps tuwunel's timeline chronological without its backfill path.
pub(super) async fn timeline(
	source: &Source,
	room_id: &RoomId,
) -> Result<impl Stream<Item = Result<Placement>> + Send + use<>> {
	let rows = source
		.stream(
			"SELECT e.event_id, j.internal_metadata FROM events e JOIN event_json j USING \
			 (event_id) LEFT JOIN rejections r USING (event_id) WHERE e.room_id = $1 AND NOT \
			 e.outlier AND e.rejection_reason IS NULL AND r.event_id IS NULL ORDER BY \
			 e.stream_ordering",
			&[&room_id.as_str()],
		)
		.await?;

	Ok(rows.and_then(async |row| {
		Ok(Placement {
			event_id: row
				.get::<_, &str>(0)
				.try_into()
				.map_err(|e| err!("invalid event ID: {e}"))?,
			soft_failed: soft_failed(row.get(1)),
		})
	}))
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
