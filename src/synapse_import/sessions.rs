//! Devices and their access and refresh tokens, so clients stay logged in.

use std::{collections::HashMap, net::IpAddr, time::Duration};

use ruma::{MilliSecondsSinceUnixEpoch, OwnedDeviceId, OwnedUserId, serde::Raw as RawJson};
use tuwunel_core::{Result, err};
use tuwunel_service::Services;

use super::{
	Report,
	source::{Source, user_id},
};

/// tuwunel refuses shorter access tokens; Synapse's `syt_` tokens are longer.
const MIN_TOKEN_LEN: usize = 32;

struct Token {
	token: String,
	valid_until_ms: Option<i64>,

	/// The refresh token the client holds alongside this access token.
	refresh: Option<String>,
}

pub(super) async fn import(services: &Services, source: &Source, report: &mut Report) -> Result {
	let tokens = latest_tokens(source).await?;

	// Hidden devices are Synapse's records of cross-signing keys, not sessions.
	let rows = source
		.rows(
			"SELECT user_id, device_id, display_name, ip FROM devices WHERE NOT \
			 coalesce(hidden, false) ORDER BY user_id, device_id",
			&[],
		)
		.await?;

	for row in rows {
		let user_id = user_id(&row, 0)?;
		if !services.users.is_active(&user_id).await {
			continue;
		}

		let device_id: OwnedDeviceId = row.get::<_, String>(1).into();
		if services
			.users
			.device_exists(&user_id, &device_id)
			.await
		{
			continue;
		}

		let display_name: Option<String> = row.get(2);
		let ip: Option<IpAddr> = row
			.get::<_, Option<String>>(3)
			.and_then(|ip| ip.parse().ok());

		let token = tokens
			.get(&(user_id.clone(), device_id.clone()))
			.filter(|token| usable(token, report, &user_id, &device_id));

		let access = token.map_or((None, None), |token| {
			(Some(token.token.as_str()), token.valid_until_ms.map(expires_in))
		});

		let refresh = token.and_then(|token| token.refresh.as_deref());
		services
			.users
			.create_device(
				&user_id,
				Some(&device_id),
				access,
				refresh,
				display_name.as_deref(),
				ip,
			)
			.await?;

		Report::count(&mut report.devices);
		if access.0.is_some() {
			Report::count(&mut report.tokens);
		}
	}

	import_dehydrated(services, source, report).await
}

/// Synapse keeps a dehydrated device as a normal device, imported above, plus
/// its encrypted data.
async fn import_dehydrated(services: &Services, source: &Source, report: &mut Report) -> Result {
	let rows = source
		.rows(
			"SELECT user_id, device_id, device_data FROM dehydrated_devices ORDER BY user_id",
			&[],
		)
		.await?;

	for row in rows {
		let user_id = user_id(&row, 0)?;
		if !services.users.is_active(&user_id).await {
			continue;
		}

		let device_id: OwnedDeviceId = row.get::<_, String>(1).into();
		let device_data = RawJson::from_json_string(row.get(2))
			.map_err(|e| err!("{user_id} dehydrated device {device_id}: {e}"))?;

		services
			.users
			.adopt_dehydrated_device(&user_id, &device_id, device_data)
			.await?;

		Report::count(&mut report.dehydrated_devices);
	}

	Ok(())
}

/// The newest access token of each device. Tokens a server admin uses to act as
/// someone else (`puppets_user_id`) are not sessions of that user.
async fn latest_tokens(source: &Source) -> Result<HashMap<(OwnedUserId, OwnedDeviceId), Token>> {
	let rows = source
		.rows(
			"SELECT DISTINCT ON (a.user_id, a.device_id) a.user_id, a.device_id, a.token, \
			 a.valid_until_ms, r.token FROM access_tokens a LEFT JOIN refresh_tokens r ON r.id \
			 = a.refresh_token_id WHERE a.device_id IS NOT NULL AND a.puppets_user_id IS NULL \
			 ORDER BY a.user_id, a.device_id, a.id DESC",
			&[],
		)
		.await?;

	rows.into_iter()
		.map(|row| {
			let user_id = user_id(&row, 0)?;
			let device_id: OwnedDeviceId = row.get::<_, String>(1).into();
			let token = Token {
				token: row.get(2),
				valid_until_ms: row.get(3),
				refresh: row.get(4),
			};

			Ok(((user_id, device_id), token))
		})
		.collect()
}

fn usable(
	token: &Token,
	report: &mut Report,
	user_id: &OwnedUserId,
	device_id: &OwnedDeviceId,
) -> bool {
	if token.token.len() < MIN_TOKEN_LEN {
		report.skip(format!(
			"{user_id} {device_id}: access token too short to keep; sign in again"
		));
		return false;
	}

	true
}

// An expired token still imports; it just fails as it would have on Synapse.
fn expires_in(valid_until_ms: i64) -> Duration {
	let now: i64 = MilliSecondsSinceUnixEpoch::now().get().into();

	Duration::from_millis(
		valid_until_ms
			.saturating_sub(now)
			.max(0)
			.unsigned_abs(),
	)
}
