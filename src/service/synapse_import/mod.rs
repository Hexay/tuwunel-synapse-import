//! One-shot import of a stopped Synapse server into a new tuwunel database.
//!
//! Phases run in dependency order, and each is stamped done in `global` once it
//! completes, so a rerun after a crash resumes at the first unfinished phase.
//! See docs/04-design.md in the synapse_migrator project for the full design.
#![cfg(feature = "synapse_import")]

mod accounts;
mod config;
mod identity;
mod preflight;
mod sessions;
mod source;

use std::path::PathBuf;

use tuwunel_core::{Result, info, warn};

use self::{config::SynapseConfig, source::Source};
use crate::Services;

/// What to import from.
#[derive(Clone, Debug)]
pub struct Import {
	/// Synapse's homeserver.yaml.
	pub synapse_config: PathBuf,

	/// Synapse's PostgreSQL database, as a libpq connection string or URL.
	pub synapse_db: String,
}

/// What an import carried over, and what it had to leave behind.
#[derive(Debug, Default)]
pub struct Report {
	pub old_keys: usize,
	pub users: usize,
	pub deactivated: usize,
	pub admins: usize,
	pub profiles: usize,
	pub devices: usize,
	pub tokens: usize,
	pub skipped: Vec<String>,
}

const STARTED_MARKER: &str = "synapse_import";

/// Imports a stopped Synapse server into this server's new database.
pub async fn import(services: &Services, import: &Import) -> Result<Report> {
	let config = SynapseConfig::read(&import.synapse_config)?;
	let source = Source::connect(&import.synapse_db).await?;
	let resuming = marker_present(services, STARTED_MARKER);

	preflight::check(services, &config, &source, resuming).await?;
	stamp(services, STARTED_MARKER);

	let mut report = Report::default();

	if !marker_present(services, "synapse_import_identity") {
		identity::import(services, &config, &mut report).await?;
		stamp(services, "synapse_import_identity");
	}

	if !marker_present(services, "synapse_import_accounts") {
		accounts::import(services, &source, &mut report).await?;
		stamp(services, "synapse_import_accounts");
	}

	if !marker_present(services, "synapse_import_sessions") {
		sessions::import(services, &source, &mut report).await?;
		stamp(services, "synapse_import_sessions");
	}

	report.log();
	Ok(report)
}

impl Report {
	fn skip(&mut self, why: String) { self.skipped.push(why); }

	fn count(counter: &mut usize) { *counter = counter.saturating_add(1); }

	fn log(&self) {
		info!(
			old_keys = self.old_keys,
			users = self.users,
			deactivated = self.deactivated,
			admins = self.admins,
			profiles = self.profiles,
			devices = self.devices,
			tokens = self.tokens,
			skipped = self.skipped.len(),
			"Synapse import finished"
		);

		for why in &self.skipped {
			warn!("Not imported: {why}");
		}
	}
}

fn marker_present(services: &Services, marker: &str) -> bool {
	services.db["global"]
		.get_blocking(marker.as_bytes())
		.is_ok()
}

fn stamp(services: &Services, marker: &str) {
	services.db["global"].insert(marker.as_bytes(), []);
}
