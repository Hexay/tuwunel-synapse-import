//! A Synapse that delegated logins to the Matrix Authentication Service (MAS):
//! accounts' auth data, sessions and OAuth clients come from MAS's database
//! instead of Synapse's, and tuwunel's own OAuth server takes over from MAS.
//! See research/mas-full-support.md in the synapse_migrator project.

pub(super) mod clients;
mod config;
pub(super) mod sessions;
pub(super) mod users;

use tokio_postgres::Row;
use tuwunel_core::{Result, err};
use tuwunel_service::Services;

use self::config::MasConfig;
use super::{Import, Report, source::Source};

pub(super) struct Mas {
	pub(super) source: Source,
	config: MasConfig,
}

/// What the import reads, so an unsupported MAS release is refused up front
/// rather than failing partway.
const REQUIRED_COLUMNS: [(&str, &[&str]); 10] = [
	("users", &[
		"username",
		"locked_at",
		"deactivated_at",
		"can_request_admin",
		"is_guest",
	]),
	("user_passwords", &["hashed_password", "version", "created_at"]),
	("user_emails", &["email", "created_at"]),
	("compat_sessions", &["device_id", "finished_at"]),
	("compat_access_tokens", &["access_token", "expires_at"]),
	("compat_refresh_tokens", &["refresh_token", "consumed_at"]),
	("oauth2_sessions", &["user_id", "oauth2_client_id", "scope_list", "finished_at"]),
	("oauth2_refresh_tokens", &["refresh_token", "consumed_at", "revoked_at"]),
	("oauth2_clients", &["redirect_uris", "is_static", "metadata_digest", "contacts"]),
	("upstream_oauth_providers", &["issuer", "claims_imports"]),
];

impl Mas {
	pub(super) async fn connect(import: &Import) -> Result<Option<Self>> {
		let (Some(db), Some(config)) = (&import.mas_db, &import.mas_config) else {
			return Ok(None);
		};

		let config = MasConfig::read(config)?;
		let source = Source::connect(db)
			.await
			.map_err(|e| err!("MAS database: {e}"))?;

		Ok(Some(Self { source, config }))
	}

	pub(super) async fn check(
		&self,
		services: &Services,
		problem: &mut impl FnMut(String),
	) -> Result {
		for (table, columns) in REQUIRED_COLUMNS {
			for column in columns {
				if !self.source.has_column(table, column).await? {
					problem(format!(
						"MAS's database has no {table}.{column}; this MAS release isn't \
						 supported"
					));
				}
			}
		}

		let server_name = services.globals.server_name();
		if self.config.matrix.homeserver != server_name {
			problem(format!(
				"MAS serves {}, but this server is {server_name}",
				self.config.matrix.homeserver
			));
		}

		if services.oauth.server.is_none() && self.source.count("oauth2_sessions").await? > 0 {
			problem(
				"tuwunel's OAuth server is off, so MAS's OAuth sessions would have nowhere to \
				 go; configure well_known.client and an identity_provider or oidc_native_auth"
					.into(),
			);
		}

		users::check_schemes(self, services, problem).await
	}

	/// Email addresses, shaped like the Synapse query `threepids` reads.
	pub(super) async fn emails(&self, services: &Services) -> Result<Vec<Row>> {
		let server_name = services.globals.server_name().as_str();
		let ms = "(extract(epoch FROM e.created_at) * 1000)::bigint";

		self.source
			.rows(
				&format!(
					"SELECT '@' || u.username || ':' || $1, 'email', e.email, {ms}, {ms} FROM \
					 user_emails e JOIN users u USING (user_id) ORDER BY u.username"
				),
				&[&server_name],
			)
			.await
	}

	/// Upstream identity links: issuer, subject, user ID, and whether MAS took
	/// the subject from `sub`, the only claim tuwunel matches on.
	pub(super) async fn links(&self, services: &Services) -> Result<Vec<Row>> {
		let server_name = services.globals.server_name().as_str();

		self.source
			.rows(
				"SELECT coalesce(p.issuer, ''), l.subject, '@' || u.username || ':' || $1, \
				 coalesce(p.claims_imports -> 'subject' ->> 'template', '{{ user.sub }}') IN \
				 ('{{ user.sub }}', '{{user.sub}}') FROM upstream_oauth_links l JOIN \
				 upstream_oauth_providers p USING (upstream_oauth_provider_id) JOIN users u ON \
				 u.user_id = l.user_id ORDER BY u.username",
				&[&server_name],
			)
			.await
	}

	pub(super) async fn leftovers(&self, report: &mut Report) -> Result {
		let leftovers = [
			(
				"SELECT count(*) FROM personal_access_tokens WHERE revoked_at IS NULL AND \
				 coalesce(expires_at > now(), true)",
				"personal access tokens: MAS kept only their hashes, so their owners must \
				 create new ones",
			),
			(
				"SELECT count(*) FROM oauth2_sessions WHERE finished_at IS NULL AND user_id IS \
				 NULL",
				"OAuth sessions of clients acting for themselves (client credentials): tuwunel \
				 has none",
			),
		];

		for (query, what) in leftovers {
			let count: i64 = self.source.rows(query, &[]).await?[0].get(0);
			if count > 0 {
				report.skip(format!("{count} {what}"));
			}
		}

		Ok(())
	}
}

/// MAS shows its UUIDs to clients as ULIDs: the same 128 bits in Crockford
/// base32.
pub(super) fn ulid(uuid: &str) -> Result<String> {
	const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

	let mut bits = u128::from_str_radix(&uuid.replace('-', ""), 16)
		.map_err(|e| err!("MAS stored an invalid UUID {uuid:?}: {e}"))?;

	let mut digits = [0_u8; 26];
	for digit in digits.iter_mut().rev() {
		let index = usize::try_from(bits & 31).expect("five bits fit");
		*digit = ALPHABET
			.get(index)
			.copied()
			.expect("five bits index 32");
		bits = bits.wrapping_shr(5);
	}

	Ok(String::from_utf8(digits.to_vec()).expect("the alphabet is ASCII"))
}

#[cfg(test)]
mod tests {
	use super::ulid;

	#[test]
	fn uuids_become_the_ulids_mas_shows() {
		// The example from the ULID spec.
		assert_eq!(
			ulid("01563e3a-b5d3-d676-4c61-efb99302bd5b").unwrap(),
			"01ARZ3NDEKTSV4RRFFQ69G5FAV"
		);
		assert_eq!(ulid("00000000-0000-0000-0000-000000000000").unwrap(), "0".repeat(26));
		assert_eq!(
			ulid("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap(),
			format!("7{}", "Z".repeat(25))
		);
		assert!(ulid("not a uuid").is_err());
	}
}
