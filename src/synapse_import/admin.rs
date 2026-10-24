//! Server-wide admin state: rooms an admin blocked, and registration tokens.

use std::time::{Duration, UNIX_EPOCH};

use ruma::{MilliSecondsSinceUnixEpoch, OwnedRoomId};
use tuwunel_core::{Result, err};
use tuwunel_service::{Services, registration_tokens::TokenExpires};

use super::{
	Report,
	config::SynapseConfig,
	source::{Source, user_id},
};

pub(super) async fn import(
	services: &Services,
	config: &SynapseConfig,
	source: &Source,
	report: &mut Report,
) -> Result {
	import_blocked_rooms(services, source, report).await?;
	import_registration_tokens(services, config, source, report).await
}

/// Blocked rooms need not exist locally; neither do tuwunel's bans.
async fn import_blocked_rooms(
	services: &Services,
	source: &Source,
	report: &mut Report,
) -> Result {
	let rows = source
		.rows("SELECT room_id, user_id FROM blocked_rooms ORDER BY room_id", &[])
		.await?;

	for row in rows {
		let room_id: OwnedRoomId = row
			.get::<_, &str>(0)
			.try_into()
			.map_err(|e| err!("invalid blocked room ID: {e}"))?;

		match user_id(&row, 1) {
			| Ok(blocker) => services.metadata.block_room(&room_id, &blocker),
			| Err(_) => services.metadata.ban_room(&room_id),
		}

		Report::count(&mut report.blocked_rooms);
	}

	Ok(())
}

/// Only the tokens still usable. tuwunel demands a token from new users
/// whenever one exists, so they're imported only if Synapse demanded one too.
async fn import_registration_tokens(
	services: &Services,
	config: &SynapseConfig,
	source: &Source,
	report: &mut Report,
) -> Result {
	let now: i64 = MilliSecondsSinceUnixEpoch::now().get().into();
	let rows = source
		.rows(
			"SELECT token, uses_allowed, completed, expiry_time FROM registration_tokens WHERE \
			 (uses_allowed IS NULL OR completed < uses_allowed) AND (expiry_time IS NULL OR \
			 expiry_time > $1) ORDER BY token",
			&[&now],
		)
		.await?;

	if !config.registration_requires_token {
		if !rows.is_empty() {
			report.skip(format!(
				"{} registration tokens: Synapse didn't require one to register, and tuwunel \
				 would start to",
				rows.len()
			));
		}

		return Ok(());
	}

	for row in rows {
		let token: &str = row.get(0);
		if services
			.registration_tokens
			.get_token_info(token)
			.await
			.is_ok()
		{
			continue;
		}

		// Registrations still pending on Synapse never finish here, so only completed uses count.
		let uses = u64::from(row.get::<_, i32>(2).unsigned_abs());
		let expires = TokenExpires {
			max_uses: row
				.get::<_, Option<i32>>(1)
				.map(|allowed| u64::from(allowed.unsigned_abs())),
			max_age: row
				.get::<_, Option<i64>>(3)
				.and_then(|ms| UNIX_EPOCH.checked_add(Duration::from_millis(ms.unsigned_abs()))),
		};

		services
			.registration_tokens
			.create_used_token(token, uses, expires)
			.await?;

		Report::count(&mut report.registration_tokens);
	}

	Ok(())
}
