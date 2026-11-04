//! To-device messages still waiting for their device, such as room keys sent
//! while it was offline.

use std::{collections::HashSet, pin::pin};

use futures::TryStreamExt;
use ruma::{OwnedDeviceId, OwnedUserId};
use serde::Deserialize;
use serde_json::Value;
use tuwunel_core::{Result, err};
use tuwunel_service::Services;

use super::{Report, crypto::existing_device, source::Source};

#[derive(Deserialize)]
pub(super) struct Message {
	sender: OwnedUserId,
	#[serde(rename = "type")]
	event_type: String,
	content: Value,
}

pub(super) async fn import(services: &Services, source: &Source, report: &mut Report) -> Result {
	advance_counter_past_to_device_stream(services, source).await?;

	let mut rows = pin!(
		source
			.stream(
				"SELECT user_id, device_id, message_json FROM device_inbox ORDER BY stream_id",
				&[],
			)
			.await?
	);

	let mut cleared: HashSet<(OwnedUserId, OwnedDeviceId)> = HashSet::new();
	while let Some(row) = rows.try_next().await? {
		let Some((user_id, device_id)) = existing_device(services, &row, report).await else {
			continue;
		};

		// Only this phase fills these queues; emptied first so a rerun doesn't deliver twice.
		if cleared.insert((user_id.clone(), device_id.clone())) {
			services
				.users
				.remove_to_device_events(&user_id, &device_id, None::<u64>)
				.await;
		}

		let message = serde_json::from_str::<Message>(row.get(2))
			.map_err(|e| err!("invalid to-device message: {e}"));
		let Some(message) = report.keep(format_args!("{user_id} {device_id}"), message) else {
			continue;
		};

		services.users.add_to_device_event(
			&message.sender,
			&user_id,
			&device_id,
			&message.event_type,
			&message.content,
		);

		Report::count(&mut report.to_device);
	}

	let unsent = source.count("device_federation_outbox").await?;
	if unsent > 0 {
		report.skip(format!(
			"{unsent} to-device messages for users on other servers were never sent"
		));
	}

	Ok(())
}

/// A sliding sync client keeps the to-device `since` Synapse gave it, a
/// to-device stream position, across a lost `pos`, and tuwunel reads it as its
/// own count: it deletes the queue up to there and serves only what is newer.
/// Counted past every position Synapse issued, the imported queue is newer.
async fn advance_counter_past_to_device_stream(services: &Services, source: &Source) -> Result {
	let row = source
		.rows(
			"SELECT GREATEST((SELECT last_value FROM pg_sequences WHERE schemaname = current_schema() \
			 AND sequencename = 'device_inbox_sequence'), (SELECT max(stream_id) FROM device_inbox), \
			 (SELECT max(stream_id) FROM device_federation_outbox))",
			&[],
		)
		.await?;

	let position = row
		.first()
		.and_then(|row| row.get::<_, Option<i64>>(0))
		.unwrap_or(0);

	drop(
		services
			.globals
			.next_count_at_least(u64::try_from(position).unwrap_or(0)),
	);

	Ok(())
}
