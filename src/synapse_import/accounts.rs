//! Users, their password hashes, admin rights and profiles.

use ruma::{OwnedMxcUri, UserId};
use tuwunel_core::Result;
use tuwunel_service::{Services, users::PASSWORD_SENTINEL};

use super::{
	Report,
	source::{Source, user_id},
};

pub(super) async fn import(services: &Services, source: &Source, report: &mut Report) -> Result {
	let rows = source
		.rows(
			"SELECT name, password_hash, admin <> 0, deactivated <> 0, is_guest <> 0 FROM users \
			 ORDER BY name",
			&[],
		)
		.await?;

	for row in rows {
		let user_id = user_id(&row, 0)?;

		// Already imported by a run that stopped partway through this phase.
		if services.users.exists(&user_id).await {
			continue;
		}

		let password_hash: Option<String> = row.get(1);
		let (admin, deactivated, guest): (bool, bool, bool) =
			(row.get(2), row.get(3), row.get(4));

		if guest {
			report.skip(format!("{user_id}: guest accounts aren't imported"));
			continue;
		}

		if deactivated {
			services
				.users
				.create(&user_id, None, None)
				.await?;
			Report::count(&mut report.deactivated);
			continue;
		}

		// A user without a password (SSO, appservice) is still active; an empty
		// value would mark the account deactivated.
		services
			.users
			.create(&user_id, Some(PASSWORD_SENTINEL), None)
			.await?;

		// Synapse stores an empty hash for users an appservice registered.
		if let Some(hash) = password_hash
			.as_deref()
			.filter(|hash| !hash.is_empty())
			&& services
				.users
				.set_password_hash(&user_id, hash)
				.await
				.is_err()
		{
			report.skip(format!("{user_id}: unrecognized password hash; reset this password"));
		}

		if admin {
			services.admin.make_user_admin(&user_id).await?;
			Report::count(&mut report.admins);
		}

		Report::count(&mut report.users);
	}

	import_profiles(services, source, report).await
}

async fn import_profiles(services: &Services, source: &Source, report: &mut Report) -> Result {
	let server_name = services.globals.server_name();

	// `user_id` here is the localpart.
	let rows = source
		.rows("SELECT user_id, displayname, avatar_url FROM profiles", &[])
		.await?;

	for row in rows {
		let localpart: String = row.get(0);
		let user_id = UserId::parse_with_server_name(localpart.as_str(), server_name)?;
		if !services.users.is_active(&user_id).await {
			continue;
		}

		let displayname: Option<String> = row.get(1);
		if displayname.is_some() {
			services
				.profile
				.set_displayname(&user_id, displayname.as_deref(), None)
				.await?;
		}

		let avatar_url: Option<String> = row.get(2);
		if let Some(avatar_url) = avatar_url {
			let avatar_url = OwnedMxcUri::from(avatar_url);
			services
				.profile
				.set_avatar_url(&user_id, Some(&avatar_url), None)
				.await?;
		}

		Report::count(&mut report.profiles);
	}

	Ok(())
}
