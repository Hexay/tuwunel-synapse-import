//! Key backups: each live version, its number of keys, and the latest version.

use std::collections::BTreeMap;

use ruma::OwnedUserId;
use tuwunel_core::Result;

use super::{super::source::user_id, Ctx};

const VERSIONS: &str = "SELECT v.user_id, v.version, (SELECT count(*) FROM e2e_room_keys k WHERE \
                        k.user_id = v.user_id AND k.version = v.version) FROM \
                        e2e_room_keys_versions v WHERE v.deleted = 0 ORDER BY v.user_id, v.version";

pub(super) async fn verify(ctx: &mut Ctx<'_>) -> Result {
	let services = ctx.services;
	let rows = ctx.source.rows(VERSIONS, &[]).await?;

	let mut latest: BTreeMap<OwnedUserId, String> = BTreeMap::new();
	for row in rows {
		let Ok(user_id) = user_id(&row, 0) else {
			continue;
		};

		if !ctx.active.contains(&user_id) {
			continue;
		}

		let version = row.get::<_, i64>(1).to_string();
		let subject = format!("{user_id} version {version}");
		if services
			.key_backups
			.get_backup(&user_id, &version)
			.await
			.is_err()
		{
			ctx.out
				.missing("backups", subject, "no such backup version");
			continue;
		}

		ctx.out.matched("backups");
		let keys = services
			.key_backups
			.count_keys(&user_id, &version)
			.await;
		let want: i64 = row.get(2);
		ctx.out.count(
			"room_keys",
			subject,
			want.try_into().unwrap_or(0),
			keys.try_into().unwrap_or(u64::MAX),
		);
		latest.insert(user_id, version);
	}

	for (user_id, version) in latest {
		match services
			.key_backups
			.get_latest_backup_version(&user_id)
			.await
		{
			| Ok(have) if have == version => {},
			| have => ctx.out.differs(
				"backups",
				&user_id,
				format!("latest version: Synapse {version}, tuwunel {have:?}"),
			),
		}
	}

	Ok(())
}
