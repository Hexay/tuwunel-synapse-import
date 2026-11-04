//! Users (with their admin, lock and suspension flags), profiles, devices and
//! access tokens.

use std::collections::{BTreeSet, HashSet};

use futures::StreamExt;
use ruma::{OwnedDeviceId, OwnedUserId, UserId};
use tuwunel_core::Result;
use tuwunel_service::users::device::{MAX_DEVICE_ID_LENGTH, TOKEN_LENGTH};

use super::{
	super::{
		Report,
		accounts::{auth, mas_users, user_rows},
		mas,
		sessions::{Tokens, latest_tokens},
		source::user_id,
	},
	Ctx,
};

pub(super) async fn verify(ctx: &mut Ctx<'_>) -> Result {
	users(ctx).await?;
	profiles(ctx).await?;
	devices(ctx).await?;
	tokens(ctx).await
}

async fn users(ctx: &mut Ctx<'_>) -> Result {
	let services = ctx.services;
	let mut scratch = Report::default();
	let mas_users = mas_users(services, ctx.mas, &mut scratch).await?;

	let mut synapse = HashSet::new();
	for row in user_rows(ctx.source).await? {
		let user_id = match user_id(&row, 0) {
			| Ok(user_id) => user_id,
			| Err(e) => {
				ctx.out.excuse("users", row.get::<_, &str>(0), e);
				continue;
			},
		};

		synapse.insert(user_id.clone());
		let auth = auth(&row, &user_id, mas_users.as_ref());
		let exists = services.users.exists(&user_id).await;
		if auth.guest {
			if exists {
				ctx.out
					.differs("users", &user_id, "a guest account was imported");
			} else {
				ctx.out
					.excuse("users", &user_id, "guest accounts aren't imported");
			}
			continue;
		}

		if !exists {
			ctx.out.missing("users", &user_id, "no such user");
			continue;
		}

		let deactivated = services
			.users
			.is_deactivated(&user_id)
			.await
			.unwrap_or(false);
		if deactivated != auth.deactivated {
			ctx.out.differs(
				"users",
				&user_id,
				format!("deactivated: Synapse {}, tuwunel {deactivated}", auth.deactivated),
			);
			continue;
		}

		if !auth.deactivated {
			ctx.active.insert(user_id.clone());
			let (suspended, ..): (bool, bool, bool) = (row.get(6), row.get(7), row.get(8));
			let flags = [
				("admin", auth.admin, services.admin.user_is_admin(&user_id).await),
				("locked", auth.locked, services.users.is_locked(&user_id).await),
				("suspended", suspended, services.users.is_suspended(&user_id).await),
			];
			if let Some((flag, want, have)) = flags
				.into_iter()
				.find(|(_, want, have)| want != have)
			{
				ctx.out.differs(
					"users",
					&user_id,
					format!("{flag}: Synapse {want}, tuwunel {have}"),
				);
				continue;
			}
		}

		ctx.out.matched("users");
	}

	let tuwunel: Vec<OwnedUserId> = services
		.users
		.stream()
		.map(ToOwned::to_owned)
		.collect()
		.await;

	let senders: HashSet<OwnedUserId> = services
		.appservice
		.read()
		.await
		.values()
		.map(|info| info.sender.clone())
		.collect();

	for user_id in tuwunel {
		if user_id == services.globals.server_user || synapse.contains(&user_id) {
			continue;
		}

		// tuwunel records remote members, and creates appservice senders on loading them.
		if !services.globals.user_is_local(&user_id) || senders.contains(&user_id) {
			continue;
		}

		ctx.out
			.extra("users", user_id, "not a Synapse user");
	}

	Ok(())
}

async fn profiles(ctx: &mut Ctx<'_>) -> Result {
	let services = ctx.services;
	let rows = ctx
		.source
		.rows("SELECT user_id, displayname, avatar_url FROM profiles", &[])
		.await?;

	for row in rows {
		let localpart: &str = row.get(0);
		let Ok(user_id) =
			UserId::parse_with_server_name(localpart, services.globals.server_name())
		else {
			ctx.out
				.excuse("profiles", localpart, "not a valid user ID");
			continue;
		};

		if !ctx.active.contains(&user_id) {
			continue;
		}

		let want: (Option<String>, Option<String>) = (row.get(1), row.get(2));
		let have = (
			services.profile.displayname(&user_id).await.ok(),
			services
				.profile
				.avatar_url(&user_id)
				.await
				.ok()
				.map(|uri| uri.to_string()),
		);

		if want == have {
			ctx.out.matched("profiles");
		} else {
			ctx.out
				.differs("profiles", &user_id, format!("Synapse {want:?}, tuwunel {have:?}"));
		}
	}

	Ok(())
}

async fn devices(ctx: &mut Ctx<'_>) -> Result {
	let services = ctx.services;
	let rows = ctx
		.source
		.rows(
			"SELECT user_id, device_id FROM devices WHERE NOT coalesce(hidden, false) ORDER BY \
			 user_id, device_id",
			&[],
		)
		.await?;

	for row in rows {
		let Ok(user_id) = user_id(&row, 0) else {
			ctx.out
				.excuse("devices", row.get::<_, &str>(0), "not a valid user ID");
			continue;
		};

		let device_id: &str = row.get(1);
		if device_id.len() > MAX_DEVICE_ID_LENGTH {
			ctx.out
				.excuse("devices", format!("{user_id} {device_id}"), "device ID too long");
			continue;
		}

		if ctx.active.contains(&user_id) {
			let device_id: OwnedDeviceId = device_id.into();
			ctx.devices
				.entry(user_id)
				.or_default()
				.insert(device_id);
		}
	}

	let active: Vec<OwnedUserId> = ctx.active.iter().cloned().collect();
	for user_id in active {
		let have: BTreeSet<OwnedDeviceId> = services
			.users
			.all_device_ids(&user_id)
			.map(ToOwned::to_owned)
			.collect()
			.await;

		let want = ctx
			.devices
			.get(&user_id)
			.cloned()
			.unwrap_or_default();
		for device_id in want.difference(&have) {
			ctx.out
				.missing("devices", format!("{user_id} {device_id}"), "no such device");
		}

		for device_id in have.difference(&want) {
			ctx.out
				.extra("devices", format!("{user_id} {device_id}"), "not a Synapse device");
		}

		for _ in want.intersection(&have) {
			ctx.out.matched("devices");
		}
	}

	Ok(())
}

/// Each imported device's access token must still name it. Tokens never
/// appear in the findings.
async fn tokens(ctx: &mut Ctx<'_>) -> Result {
	let services = ctx.services;
	let mut scratch = Report::default();
	let tokens: Tokens = match ctx.mas {
		| Some(mas) => mas::sessions::tokens(mas, services, &mut scratch).await?,
		| None => latest_tokens(ctx.source, &mut scratch).await?,
	};

	let mut tokens: Vec<_> = tokens.into_iter().collect();
	tokens.sort_by(|a, b| a.0.cmp(&b.0));
	for ((user_id, device_id), token) in tokens {
		if !ctx.active.contains(&user_id) {
			continue;
		}

		let subject = format!("{user_id} {device_id}");
		if !ctx.has_device(&user_id, &device_id) {
			ctx.out
				.excuse("tokens", subject, "no such device in Synapse");
			continue;
		}

		if token.token.len() < TOKEN_LENGTH {
			ctx.out
				.excuse("tokens", subject, "too short to keep");
			continue;
		}

		match services.users.find_from_token(&token.token).await {
			| Ok((user, device, _)) if user == user_id && device == device_id =>
				ctx.out.matched("tokens"),
			| Ok((user, device, _)) =>
				ctx.out
					.differs("tokens", subject, format!("names {user} {device}")),
			| Err(_) => ctx
				.out
				.missing("tokens", subject, "the access token isn't known"),
		}
	}

	Ok(())
}
