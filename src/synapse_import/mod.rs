//! One-shot import of a stopped Synapse server into a new tuwunel database.
//!
//! Phases run in dependency order, and each is stamped done in `global` once it
//! completes, so a rerun after a crash resumes at the first unfinished phase.
//! See docs/04-design.md in the synapse_migrator project for the full design.

mod accounts;
mod backups;
mod config;
mod crypto;
mod identity;
mod inbox;
mod preflight;
mod push_rules;
mod pushers;
mod sessions;
mod source;
mod userdata;

use std::path::PathBuf;

use tuwunel_core::{Result, info, warn};
use tuwunel_service::Services;

use self::{config::SynapseConfig, source::Source};

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
	pub cross_signing: usize,
	pub device_keys: usize,
	pub one_time_keys: usize,
	pub fallback_keys: usize,
	pub signatures: usize,
	pub backups: usize,
	pub room_keys: usize,
	pub to_device: usize,
	pub account_data: usize,
	pub tags: usize,
	pub filters: usize,
	pub push_rules: usize,
	pub pushers: usize,
	pub skipped: Vec<String>,
}

const STARTED_MARKER: &str = "synapse_import";

/// Runs a phase unless an earlier run already stamped it done.
macro_rules! phase {
	($services:expr, $marker:literal, $run:expr) => {
		if !marker_present($services, $marker) {
			$run.await?;
			stamp($services, $marker);
		}
	};
}

/// Imports a stopped Synapse server into this server's new database.
pub async fn import(services: &Services, import: &Import) -> Result<Report> {
	let config = SynapseConfig::read(&import.synapse_config)?;
	let source = Source::connect(&import.synapse_db).await?;
	let resuming = marker_present(services, STARTED_MARKER);

	preflight::check(services, &config, &source, resuming).await?;
	stamp(services, STARTED_MARKER);

	let mut report = Report::default();
	let r = &mut report;

	phase!(services, "synapse_import_identity", identity::import(services, &config, r));
	phase!(services, "synapse_import_accounts", accounts::import(services, &source, r));
	phase!(services, "synapse_import_sessions", sessions::import(services, &source, r));
	phase!(services, "synapse_import_crypto", crypto::import(services, &source, r));
	phase!(services, "synapse_import_backups", backups::import(services, &source, r));
	phase!(services, "synapse_import_inbox", inbox::import(services, &source, r));
	phase!(services, "synapse_import_userdata", userdata::import(services, &source, r));
	phase!(services, "synapse_import_push_rules", push_rules::import(services, &source, r));

	// Last, so nothing imported earlier triggers a push notification.
	phase!(services, "synapse_import_pushers", pushers::import(services, &source, r));

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
			cross_signing = self.cross_signing,
			device_keys = self.device_keys,
			one_time_keys = self.one_time_keys,
			fallback_keys = self.fallback_keys,
			signatures = self.signatures,
			backups = self.backups,
			room_keys = self.room_keys,
			to_device = self.to_device,
			account_data = self.account_data,
			tags = self.tags,
			filters = self.filters,
			push_rules = self.push_rules,
			pushers = self.pushers,
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
