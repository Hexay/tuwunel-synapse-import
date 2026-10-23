//! `--import-synapse`: start the services without listening, import, then stop.
#![cfg(feature = "synapse_import")]

use std::sync::Arc;

use tuwunel_core::{Err, Result};
use tuwunel_service::synapse_import::{self, Import};

use crate::{Args, Runtime, Server, async_run, async_start, async_stop};

/// Runs the import the arguments ask for, if any. Returns whether one ran.
pub fn exec_if_requested(server: &Arc<Server>, runtime: &Runtime, args: &Args) -> Result<bool> {
	let Some(synapse_config) = &args.import_synapse else {
		return Ok(false);
	};

	let Some(synapse_db) = &args.synapse_db else {
		return Err!("--import-synapse needs --synapse-db");
	};

	let import = Import {
		synapse_config: synapse_config.clone(),
		synapse_db: synapse_db.clone(),
	};

	runtime.block_on(async {
		let services = async_start(server).await?;
		let imported = synapse_import::import(&services, &import).await;

		server.server.shutdown()?;
		drop(services);

		async_run(server).await?;
		async_stop(server).await?;

		imported.map(|_report| ())
	})?;

	Ok(true)
}
