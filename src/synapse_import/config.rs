//! The parts of Synapse's homeserver.yaml the import reads.

use std::{
	collections::BTreeMap,
	path::{Path, PathBuf},
};

use ruma::OwnedServerName;
use serde::Deserialize;
use tuwunel_core::{Result, err};

#[derive(Debug, Deserialize)]
pub(super) struct SynapseConfig {
	pub(super) server_name: OwnedServerName,
	pub(super) signing_key_path: PathBuf,
	#[serde(default)]
	pub(super) old_signing_keys: BTreeMap<String, OldSigningKey>,
	#[serde(default)]
	pub(super) password_config: PasswordConfig,
	#[serde(default)]
	pub(super) experimental_features: ExperimentalFeatures,
	pub(super) matrix_authentication_service: Option<serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
pub(super) struct OldSigningKey {
	pub(super) key: String,
	pub(super) expired_ts: u64,
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct PasswordConfig {
	pub(super) pepper: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct ExperimentalFeatures {
	pub(super) msc3861: Option<serde_yaml::Value>,
}

impl SynapseConfig {
	/// Reads homeserver.yaml, resolving relative paths against its directory
	/// as Synapse does.
	pub(super) fn read(path: &Path) -> Result<Self> {
		let contents = std::fs::read_to_string(path)
			.map_err(|e| err!("Cannot read Synapse config {path:?}: {e}"))?;

		let mut config: Self = serde_yaml::from_str(&contents)
			.map_err(|e| err!("Cannot parse Synapse config {path:?}: {e}"))?;

		if let Some(dir) = path.parent() {
			config.signing_key_path = dir.join(&config.signing_key_path);
		}

		Ok(config)
	}

	/// Whether logins are delegated to the Matrix Authentication Service,
	/// whose tokens tuwunel cannot accept.
	pub(super) fn delegates_auth(&self) -> bool {
		let enabled = |value: &serde_yaml::Value| {
			value
				.get("enabled")
				.and_then(serde_yaml::Value::as_bool)
				.unwrap_or(false)
		};

		self.matrix_authentication_service
			.as_ref()
			.is_some_and(enabled)
			|| self
				.experimental_features
				.msc3861
				.as_ref()
				.is_some_and(enabled)
	}
}
