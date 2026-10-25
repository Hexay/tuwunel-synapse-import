//! Each user's auth state as MAS holds it, overriding Synapse's stale copy.

use std::collections::HashMap;

use ruma::OwnedUserId;
use tuwunel_core::Result;
use tuwunel_service::Services;

use super::{
	super::{Report, source::user_id},
	Mas,
	config::Scheme,
};

pub(in super::super) struct MasUser {
	/// None when the user has no password, or one tuwunel can't verify.
	pub(in super::super) password_hash: Option<String>,
	pub(in super::super) admin: bool,
	pub(in super::super) locked: bool,
	pub(in super::super) deactivated: bool,
	pub(in super::super) guest: bool,
}

/// Each user with their newest password, which MAS treats as the current one.
const USERS: &str = "SELECT '@' || u.username || ':' || $1, u.can_request_admin, u.locked_at IS \
                     NOT NULL, u.deactivated_at IS NOT NULL, u.is_guest, p.hashed_password, \
                     p.version FROM users u LEFT JOIN LATERAL (SELECT hashed_password, version \
                     FROM user_passwords WHERE user_id = u.user_id ORDER BY created_at DESC \
                     LIMIT 1) p ON true ORDER BY u.username";

pub(in super::super) async fn users(
	mas: &Mas,
	services: &Services,
	report: &mut Report,
) -> Result<HashMap<OwnedUserId, MasUser>> {
	let server_name = services.globals.server_name().as_str();
	let rows = mas.source.rows(USERS, &[&server_name]).await?;

	let mut users = HashMap::with_capacity(rows.len());
	for row in rows {
		let user_id = user_id(&row, 0)?;
		let hash: Option<String> = row.get(5);
		let version: Option<i32> = row.get(6);

		let usable = match version.and_then(|version| mas.config.scheme(version)) {
			| Some(scheme) => verifiable(scheme, services)?,
			| None => version.is_none(),
		};

		if !usable {
			report.skip(format!(
				"{user_id}: tuwunel can't verify their MAS password (scheme version {}); reset \
				 this password",
				version.unwrap_or_default()
			));
		}

		users.insert(user_id, MasUser {
			password_hash: hash.filter(|_| usable),
			admin: row.get(1),
			locked: row.get(2),
			deactivated: row.get(3),
			guest: row.get(4),
		});
	}

	Ok(users)
}

/// Refuses a scheme in use whose secret tuwunel hasn't been given. A secret set
/// to something else isn't refused, since two schemes can't share one setting:
/// that scheme's users are reported instead.
pub(super) async fn check_schemes(
	mas: &Mas,
	services: &Services,
	problem: &mut impl FnMut(String),
) -> Result {
	let rows = mas
		.source
		.rows(
			"SELECT DISTINCT p.version FROM users u JOIN LATERAL (SELECT version FROM \
			 user_passwords WHERE user_id = u.user_id ORDER BY created_at DESC LIMIT 1) p ON \
			 true",
			&[],
		)
		.await?;

	let config = &services.config;
	for version in rows.iter().map(|row| row.get::<_, i32>(0)) {
		let Some(scheme) = mas.config.scheme(version) else {
			problem(format!("MAS's config has no password scheme version {version}"));
			continue;
		};

		let unset = match (scheme.algorithm.as_str(), scheme.secret()?) {
			| ("bcrypt", Some(_)) => config.synapse_password_pepper.is_none(),
			| ("argon2id", Some(_)) => config.imported_argon2_secret.is_none(),
			| _ => false,
		};

		if unset {
			let setting = if scheme.algorithm == "bcrypt" {
				"synapse_password_pepper"
			} else {
				"imported_argon2_secret"
			};

			problem(format!(
				"MAS password scheme {version} ({}) has a secret; set {setting} to it",
				scheme.algorithm
			));
		}
	}

	Ok(())
}

fn verifiable(scheme: &Scheme, services: &Services) -> Result<bool> {
	let config = &services.config;
	let secret = scheme.secret()?;

	Ok(match scheme.algorithm.as_str() {
		| "bcrypt" => config.synapse_password_pepper.as_deref() == secret.as_deref(),
		| "argon2id" => secret.is_none() || config.imported_argon2_secret == secret,
		| _ => false,
	})
}
