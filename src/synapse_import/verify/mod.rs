//! Proves an import against Synapse's database.
//!
//! Compares Synapse's database with what tuwunel holds, domain by domain, and
//! re-checks a sample of events in depth (stored JSON against Synapse's,
//! reference hash, signatures, auth chain). Read-only on
//! both sides: Synapse is read in one read-only snapshot, and tuwunel only
//! through its services' read paths, so it also runs on a database opened
//! read-only (`--import-verify-only`). See docs/deploying/migrating-from-synapse.md.
//!
//! Each Synapse row is expected in tuwunel unless the import reports it as
//! not imported (a guest account, an event tuwunel can't store, ...): such
//! rows are counted as excused, never as a match. Rows tuwunel holds that
//! Synapse doesn't are mismatches too, apart from what tuwunel creates itself
//! (its server user and admin room).

mod accounts;
mod backups;
mod compare;
mod crypto;
mod events;
mod keys;
mod media;
mod receipts;
mod report;
mod rooms;
mod state;
#[cfg(test)]
mod tests;
mod timeline;
mod userdata;

use std::collections::{BTreeMap, BTreeSet, HashSet};

use ruma::{OwnedDeviceId, OwnedUserId};
use tuwunel_core::{Result, info};
use tuwunel_service::Services;

pub use self::report::{Mismatch, Signatures, Tally, Verification};
use super::{Import, config::SynapseConfig, mas::Mas, source::Source};

/// How deep to look.
#[derive(Clone, Debug)]
pub struct Options {
	/// Events per room re-checked in depth; 0 checks every event. Also the
	/// number of media files compared byte for byte.
	pub sample: usize,

	/// Picks the sample, so a rerun checks the same events.
	pub seed: String,
}

impl Default for Options {
	fn default() -> Self { Self { sample: 32, seed: "tuwunel".to_owned() } }
}

/// What every domain's check reads from and writes its findings to.
struct Ctx<'a> {
	services: &'a Services,
	source: &'a Source,
	mas: Option<&'a Mas>,
	config: &'a SynapseConfig,
	options: &'a Options,

	/// Users Synapse has as active, so expected active in tuwunel.
	active: HashSet<OwnedUserId>,

	/// Synapse's (non-hidden) devices of active users.
	devices: BTreeMap<OwnedUserId, BTreeSet<OwnedDeviceId>>,

	out: Verification,
}

impl Ctx<'_> {
	fn has_device(&self, user_id: &OwnedUserId, device_id: &OwnedDeviceId) -> bool {
		self.devices
			.get(user_id)
			.is_some_and(|devices| devices.contains(device_id))
	}
}

/// Compares Synapse's database with this server's. A mismatch is a finding,
/// not an error: only a failing database stops it.
pub async fn verify(
	services: &Services,
	import: &Import,
	options: &Options,
) -> Result<Verification> {
	let config = SynapseConfig::read(&import.synapse_config)?;
	let source = Source::connect(&import.synapse_db).await?;
	let mas = Mas::connect(import).await?;

	let mut ctx = Ctx {
		services,
		source: &source,
		mas: mas.as_ref(),
		config: &config,
		options,
		active: HashSet::new(),
		devices: BTreeMap::new(),
		out: Verification::new(options.sample, &options.seed),
	};

	info!("Verifying accounts");
	accounts::verify(&mut ctx).await?;
	info!("Verifying keys and backups");
	crypto::verify(&mut ctx).await?;
	backups::verify(&mut ctx).await?;
	info!("Verifying rooms");
	rooms::verify(&mut ctx).await?;
	info!("Verifying user data");
	userdata::verify(&mut ctx).await?;
	receipts::verify(&mut ctx).await?;

	// A dry run only checks that media files can be read.
	if !import.dry_run {
		info!("Verifying media");
		media::verify(&mut ctx).await?;
	}

	ctx.out.log();
	Ok(ctx.out)
}
