use ruma::{UserId, user_id};
use tuwunel_core::{Result, config::Figment, utils::hash::is_bcrypt_hash};

use crate::{Services, test_utils::fixture};

// Synapse-written fixtures; see `tuwunel_core::utils::hash::bcrypt` tests.
const TEMP123: &str = "$2b$04$i5HtYTXiQ1Z84wjDcUZ8desol8lW.3WfqH6/m7wM5wkGR1ABftZAy";
const LIGATURE_PEPPERED: &str = "$2b$04$9SMMk5qKoEt.oHskh3mwQuQ0uI/591axMf.QQekk27E9kLsGFfRGG";

async fn imported_user(services: &Services, user_id: &UserId, bcrypt_hash: &str) -> Result {
	services
		.users
		.create(user_id, Some("placeholder"), None)
		.await?;

	services
		.users
		.set_password_hash(user_id, bcrypt_hash)
		.await
}

#[tokio::test]
async fn raw_hash_import_refuses_anything_but_a_hash() -> Result {
	let Some(fixture) = fixture(Figment::new()).await? else {
		return Ok(());
	};

	let users = &fixture.services.users;
	let user_id = user_id!("@raw:localhost");
	imported_user(&fixture.services, user_id, TEMP123).await?;

	for junk in ["temp123", "", "*", &TEMP123[..59], "$argon2id$v=19$m=64,t=1,p=1"] {
		users
			.set_password_hash(user_id, junk)
			.await
			.expect_err(junk);
	}

	assert_eq!(users.password_hash(user_id).await?, TEMP123);
	users
		.verify_and_upgrade_password(user_id, "temp123", TEMP123)
		.await?;

	Ok(())
}

#[tokio::test]
async fn bcrypt_login_upgrades_to_argon2id() -> Result {
	let Some(fixture) = fixture(Figment::new()).await? else {
		return Ok(());
	};

	let users = &fixture.services.users;
	let user_id = user_id!("@imported:localhost");
	imported_user(&fixture.services, user_id, TEMP123).await?;

	users
		.verify_and_upgrade_password(user_id, "temp321", TEMP123)
		.await
		.expect_err("wrong password rejected");
	assert_eq!(users.password_hash(user_id).await?, TEMP123);

	users
		.verify_and_upgrade_password(user_id, "temp123", TEMP123)
		.await?;

	let upgraded = users.password_hash(user_id).await?;
	assert!(upgraded.starts_with("$argon2id$"), "{upgraded}");

	users
		.verify_and_upgrade_password(user_id, "temp123", &upgraded)
		.await?;

	Ok(())
}

#[tokio::test]
async fn bcrypt_login_uses_synapse_pepper() -> Result {
	let config = Figment::new().merge(("synapse_password_pepper", "pepper"));
	let Some(fixture) = fixture(config).await? else {
		return Ok(());
	};

	let users = &fixture.services.users;
	let user_id = user_id!("@peppered:localhost");
	imported_user(&fixture.services, user_id, LIGATURE_PEPPERED).await?;

	users
		.verify_and_upgrade_password(user_id, "\u{fb01}x\u{0301}", LIGATURE_PEPPERED)
		.await?;

	assert!(!is_bcrypt_hash(&users.password_hash(user_id).await?));

	Ok(())
}

#[tokio::test]
async fn upgrade_never_overwrites_a_newer_password() -> Result {
	let Some(fixture) = fixture(Figment::new()).await? else {
		return Ok(());
	};

	let users = &fixture.services.users;
	let user_id = user_id!("@changed:localhost");
	imported_user(&fixture.services, user_id, TEMP123).await?;

	users
		.set_password(user_id, Some("new password"))
		.await?;
	let changed = users.password_hash(user_id).await?;

	users
		.upgrade_bcrypt_hash(user_id, "temp123", TEMP123)
		.await;

	assert_eq!(users.password_hash(user_id).await?, changed);

	Ok(())
}
