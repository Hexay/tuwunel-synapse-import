//! Account data (global and per room, including the DM list and ignored
//! users), room tags and sync filters.

use std::{collections::BTreeMap, pin::pin};

use futures::TryStreamExt;
use ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId, api::client::filter::FilterDefinition};
use serde_json::{Map, Value, json};
use tuwunel_core::{Result, err};
use tuwunel_service::Services;

use super::{
	Report,
	source::{Source, json, user_id},
};

pub(super) async fn import(services: &Services, source: &Source, report: &mut Report) -> Result {
	import_account_data(services, source, report).await?;
	import_room_account_data(services, source, report).await?;
	import_tags(services, source, report).await?;
	import_filters(services, source, report).await
}

async fn import_account_data(
	services: &Services,
	source: &Source,
	report: &mut Report,
) -> Result {
	// Synapse builds m.push_rules from its own tables; see push_rules.rs.
	let rows = source
		.rows(
			"SELECT user_id, account_data_type, content FROM account_data WHERE \
			 account_data_type <> 'm.push_rules' ORDER BY user_id, stream_id",
			&[],
		)
		.await?;

	for row in rows {
		let user_id = user_id(&row, 0)?;
		if services.users.is_active(&user_id).await {
			put(services, None, &user_id, row.get(1), json(row.get(2))?).await?;
			Report::count(&mut report.account_data);
		}
	}

	Ok(())
}

async fn import_room_account_data(
	services: &Services,
	source: &Source,
	report: &mut Report,
) -> Result {
	let mut rows = pin!(
		source
			.stream(
				"SELECT user_id, room_id, account_data_type, content FROM room_account_data \
				 ORDER BY user_id, stream_id",
				&[],
			)
			.await?
	);

	while let Some(row) = rows.try_next().await? {
		let user_id = user_id(&row, 0)?;
		if services.users.is_active(&user_id).await {
			let room_id = room_id(row.get(1))?;
			put(services, Some(&room_id), &user_id, row.get(2), json(row.get(3))?).await?;
			Report::count(&mut report.account_data);
		}
	}

	Ok(())
}

/// Synapse keeps tags in their own table; clients see them as each room's
/// `m.tag` account data.
async fn import_tags(services: &Services, source: &Source, report: &mut Report) -> Result {
	let rows = source
		.rows("SELECT user_id, room_id, tag, content FROM room_tags", &[])
		.await?;

	let mut tags: BTreeMap<(OwnedUserId, OwnedRoomId), Map<String, Value>> = BTreeMap::new();
	for row in rows {
		tags.entry((user_id(&row, 0)?, room_id(row.get(1))?))
			.or_default()
			.insert(row.get(2), json(row.get(3))?);
	}

	for ((user_id, room_id), tags) in tags {
		if !services.users.is_active(&user_id).await {
			continue;
		}

		let count = tags.len();
		put(services, Some(&room_id), &user_id, "m.tag", json!({ "tags": tags })).await?;
		report.tags = report.tags.saturating_add(count);
	}

	Ok(())
}

async fn import_filters(services: &Services, source: &Source, report: &mut Report) -> Result {
	let server_name = services.globals.server_name();

	// Older rows name the user by localpart only.
	let rows = source
		.rows(
			"SELECT user_id, full_user_id, filter_id, filter_json FROM user_filters ORDER BY \
			 user_id, filter_id",
			&[],
		)
		.await?;

	for row in rows {
		let user_id = match row.get::<_, Option<&str>>(1) {
			| Some(full) => UserId::parse(full)?,
			| None => UserId::parse_with_server_name(row.get::<_, &str>(0), server_name)?,
		};

		if !services.users.is_active(&user_id).await {
			continue;
		}

		let filter_id = row.get::<_, i64>(2).to_string();
		match serde_json::from_slice::<FilterDefinition>(row.get(3)) {
			| Ok(filter) => {
				services
					.users
					.create_filter_with_id(&user_id, &filter_id, &filter);
				Report::count(&mut report.filters);
			},
			| Err(e) => report.skip(format!("{user_id} filter {filter_id}: {e}")),
		}
	}

	Ok(())
}

async fn put(
	services: &Services,
	room_id: Option<&RoomId>,
	user_id: &UserId,
	event_type: &str,
	content: Value,
) -> Result {
	let event = json!({ "type": event_type, "content": content });
	services
		.account_data
		.update(room_id, user_id, event_type.into(), &event)
		.await
}

fn room_id(room_id: &str) -> Result<OwnedRoomId> {
	room_id
		.try_into()
		.map_err(|e| err!("Synapse room ID {room_id:?}: {e}"))
}
