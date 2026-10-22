use ruma::{api::client::backup::BackupAlgorithm, serde::Raw, user_id};
use serde_json::json;
use tuwunel_core::{Result, config::Figment};

use crate::test_utils::fixture;

fn algorithm() -> Raw<BackupAlgorithm> {
	let json = json!({
		"algorithm": "m.megolm_backup.v1.curve25519-aes-sha2",
		"auth_data": {
			"public_key": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"signatures": {}
		}
	})
	.to_string();

	Raw::from_json_string(json).expect("valid backup algorithm JSON")
}

#[tokio::test]
async fn imported_version_is_kept_until_a_newer_backup() -> Result {
	let Some(fixture) = fixture(Figment::new()).await? else {
		return Ok(());
	};

	let key_backups = &fixture.services.key_backups;
	let user_id = user_id!("@imported:localhost");

	// Far above a fresh database's global sequence.
	let imported = 1_000_000_000;

	key_backups
		.create_backup_with_version(user_id, imported, &algorithm())
		.await?;

	assert_eq!(
		key_backups
			.get_latest_backup_version(user_id)
			.await?,
		imported.to_string()
	);

	key_backups
		.create_backup_with_version(user_id, imported, &algorithm())
		.await
		.expect_err("an existing version is refused");

	let created = key_backups
		.create_backup(user_id, &algorithm())
		.await?;

	assert!(created.parse::<u64>()? > imported, "{created}");
	assert_eq!(
		key_backups
			.get_latest_backup_version(user_id)
			.await?,
		created
	);

	Ok(())
}
