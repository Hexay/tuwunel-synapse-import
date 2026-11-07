//! Device keys, cross-signing keys, and one-time and fallback keys.

use std::{
	collections::{BTreeMap, BTreeSet},
	pin::pin,
};

use futures::{StreamExt, TryStreamExt};
use ruma::{OwnedDeviceId, OwnedUserId, encryption::CrossSigningKey, serde::Raw};
use serde_json::Value;
use tuwunel_core::Result;

use super::{
	super::source::{json, user_id},
	Ctx,
	compare::same_keys,
};

pub(super) async fn verify(ctx: &mut Ctx<'_>) -> Result {
	device_keys(ctx).await?;
	cross_signing(ctx).await?;
	one_time_keys(ctx).await?;
	fallback_keys(ctx).await
}

/// The active user and Synapse device a row's first two columns name.
fn device(ctx: &Ctx<'_>, row: &tokio_postgres::Row) -> Option<(OwnedUserId, OwnedDeviceId)> {
	let user_id = user_id(row, 0).ok()?;
	let device_id: OwnedDeviceId = row.get::<_, String>(1).into();

	ctx.has_device(&user_id, &device_id)
		.then_some((user_id, device_id))
}

async fn device_keys(ctx: &mut Ctx<'_>) -> Result {
	let services = ctx.services;
	let mut rows = pin!(
		ctx.source
			.stream("SELECT user_id, device_id, key_json FROM e2e_device_keys_json", &[])
			.await?
	);

	while let Some(row) = rows.try_next().await? {
		let Some((user_id, device_id)) = device(ctx, &row) else {
			continue;
		};

		let subject = format!("{user_id} {device_id}");
		let Ok(want) = json(row.get(2)) else {
			ctx.out
				.excuse("device_keys", subject, "Synapse stored invalid JSON");
			continue;
		};

		match services
			.users
			.get_device_keys(&user_id, &device_id)
			.await
		{
			| Err(_) => ctx
				.out
				.missing("device_keys", subject, "no device keys"),
			| Ok(have) => match serde_json::from_str::<Value>(have.json().get()) {
				| Ok(have) if same_keys(&want, &have) => ctx.out.matched("device_keys"),
				| _ =>
					ctx.out
						.differs("device_keys", subject, "keys or Synapse's signatures differ"),
			},
		}
	}

	Ok(())
}

async fn cross_signing(ctx: &mut Ctx<'_>) -> Result {
	let services = ctx.services;
	let rows = ctx
		.source
		.rows(
			"SELECT DISTINCT ON (user_id, keytype) user_id, keytype, keydata FROM \
			 e2e_cross_signing_keys ORDER BY user_id, keytype, stream_id DESC",
			&[],
		)
		.await?;

	let all = |_: &_| true;
	for row in rows {
		let Ok(user_id) = user_id(&row, 0) else {
			continue;
		};

		let kind: &str = row.get(1);
		let local = services.globals.user_is_local(&user_id);
		if (local && !ctx.active.contains(&user_id)) || (!local && kind == "user_signing") {
			continue;
		}

		let subject = format!("{user_id} {kind}");
		let Ok(want) = json(row.get(2)) else {
			ctx.out
				.excuse("cross_signing", subject, "Synapse stored invalid JSON");
			continue;
		};

		let have: Result<Raw<CrossSigningKey>> = match kind {
			| "master" =>
				services
					.users
					.get_master_key(None, &user_id, &all)
					.await,
			| "self_signing" =>
				services
					.users
					.get_self_signing_key(None, &user_id, &all)
					.await,
			| "user_signing" =>
				services
					.users
					.get_user_signing_key(&user_id)
					.await,
			| _ => {
				ctx.out
					.excuse("cross_signing", subject, "unknown key type");
				continue;
			},
		};

		let have = have
			.ok()
			.and_then(|have| serde_json::from_str::<Value>(have.json().get()).ok());
		match have {
			| None => ctx
				.out
				.missing("cross_signing", subject, "no such key"),
			| Some(have) if want.get("keys") == have.get("keys") =>
				ctx.out.matched("cross_signing"),
			| Some(_) => ctx
				.out
				.differs("cross_signing", subject, "a different key"),
		}
	}

	Ok(())
}

/// Counted per device and algorithm; a row whose key doesn't parse isn't
/// imported.
async fn one_time_keys(ctx: &mut Ctx<'_>) -> Result {
	let services = ctx.services;
	let mut rows = pin!(
		ctx.source
			.stream(
				"SELECT user_id, device_id, algorithm, key_json FROM e2e_one_time_keys_json \
				 ORDER BY user_id, device_id",
				&[],
			)
			.await?
	);

	// Ordered by device, so only one device's counts are held at a time.
	let mut want: Option<(Device, BTreeMap<String, u64>)> = None;
	let mut seen = BTreeSet::new();
	while let Some(row) = rows.try_next().await? {
		let Some(device) = device(ctx, &row) else {
			continue;
		};

		if json(row.get(3)).is_err() {
			ctx.out.excuse(
				"one_time_keys",
				format!("{} {}", device.0, device.1),
				"Synapse stored invalid JSON",
			);
			continue;
		}

		if want
			.as_ref()
			.is_some_and(|(held, _)| *held != device)
		{
			compare_one_time_keys(ctx, want.take()).await;
		}

		seen.insert(device.clone());
		let (_, counts) = want.get_or_insert_with(|| (device, BTreeMap::new()));
		let count = counts.entry(row.get(2)).or_default();
		*count = count.saturating_add(1);
	}

	compare_one_time_keys(ctx, want).await;

	// Devices Synapse holds no keys for must hold none in tuwunel either.
	let keyless: Vec<Device> = ctx
		.devices
		.iter()
		.flat_map(|(user_id, devices)| {
			devices
				.iter()
				.map(move |device_id| (user_id.clone(), device_id.clone()))
		})
		.filter(|device| !seen.contains(device))
		.collect();

	for (user_id, device_id) in keyless {
		let have = services
			.users
			.count_one_time_keys(&user_id, &device_id)
			.await;
		let have: u64 = have.values().map(|n| u64::from(*n)).sum();
		if have > 0 {
			ctx.out
				.count("one_time_keys", format!("{user_id} {device_id}"), 0, have);
		}
	}

	Ok(())
}

type Device = (OwnedUserId, OwnedDeviceId);

async fn compare_one_time_keys(ctx: &mut Ctx<'_>, want: Option<(Device, BTreeMap<String, u64>)>) {
	let Some(((user_id, device_id), counts)) = want else {
		return;
	};

	let have = ctx
		.services
		.users
		.count_one_time_keys(&user_id, &device_id)
		.await;
	for (algorithm, want) in counts {
		let have = have
			.iter()
			.find(|(alg, _)| alg.as_str() == algorithm)
			.map_or(0, |(_, n)| u64::from(*n));

		ctx.out
			.count("one_time_keys", format!("{user_id} {device_id} {algorithm}"), want, have);
	}
}

async fn fallback_keys(ctx: &mut Ctx<'_>) -> Result {
	let services = ctx.services;
	let rows = ctx
		.source
		.rows(
			"SELECT user_id, device_id, algorithm, key_json, used FROM e2e_fallback_keys_json",
			&[],
		)
		.await?;

	let mut want: BTreeMap<(OwnedUserId, OwnedDeviceId), BTreeSet<String>> = BTreeMap::new();
	for row in rows {
		let Some(device) = device(ctx, &row) else {
			continue;
		};

		if json(row.get(3)).is_err() {
			ctx.out.excuse(
				"fallback_keys",
				format!("{} {}", device.0, device.1),
				"Synapse stored invalid JSON",
			);
			continue;
		}

		let unused = want.entry(device).or_default();
		if !row.get::<_, bool>(4) {
			unused.insert(row.get(2));
		}
	}

	for ((user_id, device_id), want) in want {
		let have: BTreeSet<String> = services
			.users
			.unused_fallback_key_algorithms(&user_id, &device_id)
			.map(|algorithm| algorithm.to_string())
			.collect()
			.await;

		if want == have {
			ctx.out.matched("fallback_keys");
		} else {
			let subject = format!("{user_id} {device_id}");
			ctx.out.differs(
				"fallback_keys",
				subject,
				format!("unused: Synapse {want:?}, tuwunel {have:?}"),
			);
		}
	}

	Ok(())
}
