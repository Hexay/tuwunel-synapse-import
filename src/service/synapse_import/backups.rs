//! Server-side room key backups, under the version numbers clients already
//! hold.

use ruma::{OwnedRoomId, UserId, api::client::backup::KeyBackupData, serde::Raw};
use serde::Serialize;
use serde_json::value::{RawValue, to_raw_value};
use tuwunel_core::{Result, err, utils::IterStream};

use super::{
	Report,
	source::{Source, json_column, user_id},
};
use crate::Services;

#[derive(Serialize)]
struct Metadata<'a> {
	algorithm: &'a str,
	auth_data: &'a RawValue,
}

#[derive(Serialize)]
struct SessionKey<'a> {
	first_message_index: i32,
	forwarded_count: i32,
	is_verified: bool,
	session_data: &'a RawValue,
}

pub(super) async fn import(services: &Services, source: &Source, report: &mut Report) -> Result {
	// Ascending, so each version is the latest when its keys are added; tuwunel
	// only writes keys into the latest version.
	let rows = source
		.rows(
			"SELECT user_id, version, algorithm, auth_data FROM e2e_room_keys_versions WHERE \
			 deleted = 0 ORDER BY user_id, version",
			&[],
		)
		.await?;

	for row in rows {
		let user_id = user_id(&row, 0)?;
		if !services.users.is_active(&user_id).await {
			continue;
		}

		let version: i64 = row.get(1);
		let version_string = version.to_string();

		// A rerun after a crash finds the version already there.
		if services
			.key_backups
			.get_backup(&user_id, &version_string)
			.await
			.is_err()
		{
			let auth_data = json_column(row.get(3))?;
			let metadata = to_raw_value(&Metadata {
				algorithm: row.get(2),
				auth_data: &auth_data,
			})?;
			let version = version
				.try_into()
				.map_err(|e| err!("{user_id}: key backup version {version}: {e}"))?;

			services
				.key_backups
				.create_backup_with_version(&user_id, version, &Raw::from_json(metadata))
				.await?;

			Report::count(&mut report.backups);
		}

		import_room_keys(services, source, &user_id, version, report).await?;
	}

	Ok(())
}

async fn import_room_keys(
	services: &Services,
	source: &Source,
	user_id: &UserId,
	version: i64,
	report: &mut Report,
) -> Result {
	let rows = source
		.rows(
			"SELECT room_id, session_id, first_message_index, forwarded_count, is_verified, \
			 session_data FROM e2e_room_keys WHERE user_id = $1 AND version = $2",
			&[&user_id.as_str(), &version],
		)
		.await?;

	let keys = rows
		.iter()
		.map(|row| {
			let room_id: OwnedRoomId = row
				.get::<_, &str>(0)
				.try_into()
				.map_err(|e| err!("{user_id}: backed-up key for an invalid room ID: {e}"))?;

			let session_data = json_column(row.get(5))?;
			let key = to_raw_value(&SessionKey {
				first_message_index: row.get(2),
				forwarded_count: row.get(3),
				is_verified: row.get(4),
				session_data: &session_data,
			})?;

			Ok((room_id, row.get::<_, String>(1), Raw::<KeyBackupData>::from_json(key)))
		})
		.collect::<Result<Vec<_>>>()?;

	let stream = keys
		.iter()
		.map(|(room_id, session_id, key)| (room_id.as_ref(), session_id.as_str(), key))
		.stream();

	services
		.key_backups
		.add_keys(user_id, &version.to_string(), stream)
		.await?;

	report.room_keys = report.room_keys.saturating_add(keys.len());

	Ok(())
}
