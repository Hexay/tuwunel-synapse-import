//! Refuses an import that cannot be lossless, before anything is written.

use std::ops::RangeInclusive;

use tuwunel_core::{Err, Result, info};

use super::{config::SynapseConfig, source::Source};
use crate::{Services, server_keys::read_synapse_key_file};

/// Synapse schema versions this importer has been tested against (Synapse 1.163).
const SUPPORTED_SCHEMA: RangeInclusive<i32> = 94..=94;

/// Tables that must be empty: work Synapse accepted but hadn't finished.
const MUST_BE_EMPTY: [(&str, &str); 3] = [
	(
		"federation_inbound_events_staging",
		"incoming federation events are still queued",
	),
	("partial_state_rooms", "some rooms were joined with partial state"),
	("background_updates", "Synapse's own database updates haven't finished"),
];

pub(super) async fn check(
	services: &Services,
	config: &SynapseConfig,
	source: &Source,
	resuming: bool,
) -> Result {
	let mut problems = Vec::new();
	let mut problem = |p: String| problems.push(p);

	let server_name = services.globals.server_name();
	if config.server_name != server_name {
		problem(format!(
			"Synapse's server_name is {}, but this server's is {server_name}; they must be the \
			 same",
			config.server_name
		));
	}

	if config.delegates_auth() {
		problem(
			"Synapse delegates logins to the Matrix Authentication Service, which this importer \
			 doesn't support yet"
				.into(),
		);
	}

	check_signing_key(services, config, &mut problem);

	let pepper = config.password_config.pepper.as_deref();
	if services.config.synapse_password_pepper.as_deref() != pepper {
		problem(
			"synapse_password_pepper must be set to Synapse's password_config.pepper (or both \
			 unset)"
				.into(),
		);
	}

	let schema: i32 = source
		.rows("SELECT version FROM schema_version", &[])
		.await?[0]
		.get(0);
	if !SUPPORTED_SCHEMA.contains(&schema) {
		problem(format!(
			"Synapse database schema {schema} is not supported (supported: {}-{})",
			SUPPORTED_SCHEMA.start(),
			SUPPORTED_SCHEMA.end()
		));
	}

	for (table, why) in MUST_BE_EMPTY {
		if source.count(table).await? > 0 {
			problem(format!("{table} isn't empty: {why}. Let Synapse finish, then stop it"));
		}
	}

	// Beyond the server user that a fresh database creates.
	if !resuming && services.users.count().await > 1 {
		problem("this tuwunel database already has users; import into a new one".into());
	}

	if !problems.is_empty() {
		return Err!("Cannot import from Synapse:\n- {}", problems.join("\n- "));
	}

	info!("Preflight passed: Synapse schema {schema}, server {server_name}");
	Ok(())
}

fn check_signing_key(
	services: &Services,
	config: &SynapseConfig,
	problem: &mut impl FnMut(String),
) {
	let synapse = match read_synapse_key_file(&config.signing_key_path) {
		| Ok((keypair, ..)) => keypair,
		| Err(e) => return problem(format!("Synapse's signing key: {e}")),
	};

	let ours = services.server_keys.keypair();
	if ours.version() != synapse.version() || ours.public_key() != synapse.public_key() {
		problem(format!(
			"this server's signing key is ed25519:{}, not Synapse's ed25519:{}; set \
			 signing_key_path = \"{}\" and use a new database",
			ours.version(),
			synapse.version(),
			config.signing_key_path.display()
		));
	}
}
