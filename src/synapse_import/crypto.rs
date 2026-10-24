//! End-to-end encryption keys: cross-signing keys, device keys, one-time and
//! fallback keys, and the cross-signing signatures between them.

use std::{collections::BTreeMap, iter, pin::pin};

use futures::TryStreamExt;
use ruma::{
	OwnedDeviceId, OwnedOneTimeKeyId, OwnedUserId, encryption::CrossSigningKey, serde::Raw,
};
use tokio_postgres::Row;
use tuwunel_core::{Err, Result, err};
use tuwunel_service::Services;

use super::{
	Report,
	source::{Source, raw_json, user_id},
};

pub(super) async fn import(services: &Services, source: &Source, report: &mut Report) -> Result {
	import_cross_signing_keys(services, source, report).await?;
	import_device_keys(services, source, report).await?;
	import_one_time_keys(services, source, report).await?;
	import_fallback_keys(services, source, report).await?;

	// Last: a signature is only accepted once both keys it links are stored.
	import_signatures(services, source, report).await
}

/// The user and device a row's first two columns name, if that device was
/// imported.
pub(super) async fn existing_device(
	services: &Services,
	row: &Row,
) -> Result<Option<(OwnedUserId, OwnedDeviceId)>> {
	let user_id = user_id(row, 0)?;
	let device_id: OwnedDeviceId = row.get::<_, String>(1).into();

	Ok(services
		.users
		.device_exists(&user_id, &device_id)
		.await
		.then_some((user_id, device_id)))
}

async fn import_cross_signing_keys(
	services: &Services,
	source: &Source,
	report: &mut Report,
) -> Result {
	// Synapse keeps every key a user ever uploaded; only the newest of each kind is current.
	let rows = source
		.rows(
			"SELECT DISTINCT ON (user_id, keytype) user_id, keytype, keydata FROM \
			 e2e_cross_signing_keys ORDER BY user_id, keytype, stream_id DESC",
			&[],
		)
		.await?;

	let mut keys: BTreeMap<OwnedUserId, [Option<Raw<CrossSigningKey>>; 3]> = BTreeMap::new();
	for row in rows {
		let slot = match row.get::<_, &str>(1) {
			| "master" => 0,
			| "self_signing" => 1,
			| "user_signing" => 2,
			| other => return Err!("Synapse cross-signing key of unknown type {other:?}"),
		};

		keys.entry(user_id(&row, 0)?).or_default()[slot] = Some(raw_json(row.get(2))?);
	}

	for (user_id, [master, self_signing, user_signing]) in keys {
		if !services.users.is_active(&user_id).await {
			continue;
		}

		match services
			.users
			.add_cross_signing_keys(&user_id, &master, &self_signing, &user_signing, false)
			.await
		{
			| Ok(()) => Report::count(&mut report.cross_signing),
			| Err(e) => report.skip(format!("{user_id}: cross-signing keys: {e}")),
		}
	}

	Ok(())
}

async fn import_device_keys(services: &Services, source: &Source, report: &mut Report) -> Result {
	let rows = source
		.rows(
			"SELECT user_id, device_id, key_json FROM e2e_device_keys_json ORDER BY user_id, \
			 device_id",
			&[],
		)
		.await?;

	for row in rows {
		let Some((user_id, device_id)) = existing_device(services, &row).await? else {
			continue;
		};

		services
			.users
			.add_device_keys(&user_id, &device_id, &raw_json(row.get(2))?)
			.await;

		Report::count(&mut report.device_keys);
	}

	Ok(())
}

async fn import_one_time_keys(
	services: &Services,
	source: &Source,
	report: &mut Report,
) -> Result {
	// Upload order, so /keys/claim hands out the oldest first as Synapse did (MSC4225).
	let mut rows = pin!(
		source
			.stream(
				"SELECT user_id, device_id, algorithm, key_id, key_json FROM \
				 e2e_one_time_keys_json ORDER BY user_id, device_id, ts_added_ms, key_id",
				&[],
			)
			.await?
	);

	while let Some(row) = rows.try_next().await? {
		let Some((user_id, device_id)) = existing_device(services, &row).await? else {
			continue;
		};

		// One key per call: a batch is stored in key ID order, not upload order.
		let keys = BTreeMap::from([(one_time_key_id(&row)?, raw_json(row.get(4))?)]);
		services
			.users
			.add_one_time_keys(&user_id, &device_id, &keys, 1)
			.await?;

		Report::count(&mut report.one_time_keys);
	}

	Ok(())
}

async fn import_fallback_keys(
	services: &Services,
	source: &Source,
	report: &mut Report,
) -> Result {
	let rows = source
		.rows(
			"SELECT user_id, device_id, algorithm, key_id, key_json, used FROM \
			 e2e_fallback_keys_json ORDER BY user_id, device_id, algorithm",
			&[],
		)
		.await?;

	for row in rows {
		let Some((user_id, device_id)) = existing_device(services, &row).await? else {
			continue;
		};

		let key_id = one_time_key_id(&row)?;
		let key = raw_json(row.get(4))?;
		services
			.users
			.add_fallback_keys(&user_id, &device_id, iter::once((&*key_id, &key)))
			.await?;

		// Claiming is the only way tuwunel marks a fallback key used; the key itself stays.
		if row.get::<_, bool>(5) {
			services
				.users
				.take_fallback_key(&user_id, &device_id, &key_id.algorithm())
				.await?;
		}

		Report::count(&mut report.fallback_keys);
	}

	Ok(())
}

/// Signatures uploaded through /keys/signatures/upload. Synapse keeps them
/// apart from the keys they sign; tuwunel stores them inside the signed key,
/// after checking them.
async fn import_signatures(services: &Services, source: &Source, report: &mut Report) -> Result {
	let rows = source
		.rows(
			"SELECT user_id, key_id, target_user_id, target_device_id, signature FROM \
			 e2e_cross_signing_signatures ORDER BY user_id, target_user_id, target_device_id",
			&[],
		)
		.await?;

	for row in rows {
		let signer = user_id(&row, 0)?;
		if !services.users.is_active(&signer).await {
			continue;
		}

		let target = user_id(&row, 2)?;
		let target_key: String = row.get(3);
		let signature = [(row.get::<_, String>(1), row.get::<_, String>(4))];

		match services
			.users
			.sign_key(&target, &target_key, signature.into(), &signer)
			.await
		{
			| Ok(()) => Report::count(&mut report.signatures),
			| Err(e) =>
				report.skip(format!("{signer}'s signature on {target} {target_key}: {e}")),
		}
	}

	Ok(())
}

fn one_time_key_id(row: &Row) -> Result<OwnedOneTimeKeyId> {
	let id = format!("{}:{}", row.get::<_, &str>(2), row.get::<_, &str>(3));

	id.as_str()
		.try_into()
		.map_err(|e| err!("Synapse one-time key ID {id:?}: {e}"))
}
