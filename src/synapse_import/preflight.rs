//! Refuses an import that cannot be lossless, before anything is written.
//!
//! Each check gathers its facts from the services and databases, then hands
//! them to a pure `*_problem` function that words the refusal, so every
//! refusal is unit-tested without a server (see `preflight/tests.rs`).

use std::{ops::RangeInclusive, path::Path};

use ruma::signatures::Ed25519KeyPair;
use tuwunel_core::{Err, Result, info};
use tuwunel_service::{Services, server_keys::read_synapse_key_file};

use super::{config::SynapseConfig, mas::Mas, source::Source};

/// Synapse 1.100 through 1.163, the releases this import is tested against.
/// Columns added in between are checked by name (`Source::column_or`), not by
/// this number.
const SUPPORTED_SCHEMA: RangeInclusive<i32> = 84..=94;

/// Tables that must be empty: work Synapse accepted but hadn't finished.
const MUST_BE_EMPTY: [(&str, &str); 3] = [
	(
		"federation_inbound_events_staging",
		"incoming federation events are still queued",
	),
	("partial_state_rooms", "some rooms were joined with partial state"),
	("background_updates", "Synapse's own database updates haven't finished"),
];

/// Lists every problem at once rather than stopping at the first.
pub(super) async fn check(
	services: &Services,
	config: &SynapseConfig,
	source: &Source,
	mas: Option<&Mas>,
	resuming: bool,
) -> Result {
	let mut problems = Vec::new();
	let mut problem = |p: String| problems.push(p);

	check_identity(services, config, &mut problem);
	check_auth(services, config, mas, &mut problem).await?;
	let schema = check_source(source, &mut problem).await?;
	check_target(services, source, resuming, &mut problem).await?;

	if !problems.is_empty() {
		return Err!("Cannot import from Synapse:\n- {}", problems.join("\n- "));
	}

	let server_name = services.globals.server_name();
	info!("Preflight passed: Synapse schema {schema}, server {server_name}");
	Ok(())
}

/// The same server name and signing key, so other servers see the same server.
fn check_identity(services: &Services, config: &SynapseConfig, problem: &mut impl FnMut(String)) {
	let ours = services.globals.server_name().as_str();
	server_name_problem(config.server_name.as_str(), ours)
		.into_iter()
		.chain(signing_key_problem(&config.signing_key_path, services.server_keys.keypair()))
		.for_each(problem);
}

/// What users log in with: Synapse's password hashes, or MAS's whole database.
async fn check_auth(
	services: &Services,
	config: &SynapseConfig,
	mas: Option<&Mas>,
	problem: &mut impl FnMut(String),
) -> Result {
	let pepper = config.password_config.pepper.as_deref();
	let ours = services.config.imported_bcrypt_pepper.as_deref();
	if let Some(p) = auth_problem(config.delegates_auth(), mas.is_some(), ours, pepper) {
		problem(p);
	}

	if let Some(mas) = mas.filter(|_| config.delegates_auth()) {
		mas.check(services, problem).await?;
		if let Some(clients) = other_clients(&mas.source).await? {
			problem(format!(
				"other clients are connected to MAS's database ({clients}); stop MAS first"
			));
		}
	}

	Ok(())
}

/// A supported, idle Synapse database; returns its schema version.
async fn check_source(source: &Source, problem: &mut impl FnMut(String)) -> Result<i32> {
	if !source.has_table("schema_version").await? {
		return Err!("--synapse-db is not a Synapse database: it has no schema_version table");
	}

	let schema: i32 = source
		.rows("SELECT version FROM schema_version", &[])
		.await?
		.first()
		.map_or(0, |row| row.get(0));

	problem_if(schema_problem(schema), problem);

	if let Some(clients) = other_clients(source).await? {
		problem(format!(
			"other clients are connected to Synapse's database ({clients}); stop Synapse and \
			 its workers first, or both servers will accept messages the other never sees"
		));
	}

	for (table, why) in MUST_BE_EMPTY {
		problem_if(unfinished_problem(table, why, source.count(table).await?), problem);
	}

	Ok(schema)
}

/// A new tuwunel database, whose server user no Synapse user would merge into.
async fn check_target(
	services: &Services,
	source: &Source,
	resuming: bool,
	problem: &mut impl FnMut(String),
) -> Result {
	let server_user = services.globals.server_user.as_str();
	let taken = !source
		.rows("SELECT 1 FROM users WHERE name = $1", &[&server_user])
		.await?
		.is_empty();

	let users = services.users.count().await;
	target_problems(server_user, taken, resuming, users)
		.into_iter()
		.for_each(problem);

	Ok(())
}

fn problem_if(found: Option<String>, problem: &mut impl FnMut(String)) {
	if let Some(p) = found {
		problem(p);
	}
}

/// Who else is connected to the database, as "N from <client>" entries.
async fn other_clients(source: &Source) -> Result<Option<String>> {
	let rows = source
		.rows(
			"SELECT count(*), coalesce(nullif(application_name, ''), host(client_addr), \
			 'local') FROM pg_stat_activity WHERE datname = current_database() AND pid <> \
			 ALL($1) AND backend_type = 'client backend' GROUP BY 2 ORDER BY 2",
			&[source.pids()],
		)
		.await?;

	Ok((!rows.is_empty()).then(|| {
		rows.iter()
			.map(|row| format!("{} from {}", row.get::<_, i64>(0), row.get::<_, String>(1)))
			.collect::<Vec<_>>()
			.join(", ")
	}))
}

fn server_name_problem(synapse: &str, ours: &str) -> Option<String> {
	(synapse != ours).then(|| {
		format!(
			"Synapse's server_name is {synapse}, but this server's is {ours}; they must be the \
			 same"
		)
	})
}

fn signing_key_problem(synapse_key: &Path, ours: &Ed25519KeyPair) -> Option<String> {
	let synapse = match read_synapse_key_file(synapse_key) {
		| Ok((keypair, ..)) => keypair,
		| Err(e) => return Some(format!("Synapse's signing key: {e}")),
	};

	let same = ours.version() == synapse.version() && ours.public_key() == synapse.public_key();
	(!same).then(|| {
		format!(
			"this server's signing key is ed25519:{}, not Synapse's ed25519:{}; set \
			 signing_key_path = \"{}\" and use a new database",
			ours.version(),
			synapse.version(),
			synapse_key.display()
		)
	})
}

/// With MAS, Synapse's own password hashes are stale and its pepper unused, so
/// the pepper is compared only without it.
fn auth_problem(
	delegates: bool,
	mas_given: bool,
	our_pepper: Option<&str>,
	synapse_pepper: Option<&str>,
) -> Option<String> {
	match (delegates, mas_given) {
		| (true, false) => Some(
			"Synapse delegates logins to the Matrix Authentication Service; pass its database \
			 and config with --mas-db and --mas-config"
				.into(),
		),
		| (false, true) => Some(
			"--mas-db was given, but Synapse doesn't delegate logins to the Matrix \
			 Authentication Service"
				.into(),
		),
		| (true, true) => None,
		| (false, false) => (our_pepper != synapse_pepper).then(|| {
			"imported_bcrypt_pepper must be set to Synapse's password_config.pepper (or both \
			 unset)"
				.into()
		}),
	}
}

fn schema_problem(schema: i32) -> Option<String> {
	(!SUPPORTED_SCHEMA.contains(&schema)).then(|| {
		format!(
			"Synapse database schema {schema} is not supported (supported: {}-{})",
			SUPPORTED_SCHEMA.start(),
			SUPPORTED_SCHEMA.end()
		)
	})
}

fn unfinished_problem(table: &str, why: &str, rows: i64) -> Option<String> {
	(rows > 0).then(|| format!("{table} isn't empty: {why}. Let Synapse finish, then stop it"))
}

/// `users` counts the server user that a fresh database creates.
fn target_problems(server_user: &str, taken: bool, resuming: bool, users: usize) -> Vec<String> {
	let mut problems = Vec::new();
	if taken {
		problems.push(format!(
			"Synapse has a user {server_user}, which is this server's own server user; set \
			 server_user_localpart to a name Synapse doesn't use, in a new database"
		));
	}

	if !resuming && users > 1 {
		problems.push("this tuwunel database already has users; import into a new one".into());
	}

	problems
}

#[cfg(test)]
mod tests;
