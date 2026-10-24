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
	#[serde(default = "default_media_store_path")]
	pub(super) media_store_path: PathBuf,
	#[serde(default)]
	pub(super) app_service_config_files: Vec<PathBuf>,
	#[serde(default)]
	pub(super) old_signing_keys: BTreeMap<String, OldSigningKey>,
	#[serde(default)]
	pub(super) password_config: PasswordConfig,
	#[serde(default)]
	pub(super) experimental_features: ExperimentalFeatures,
	pub(super) matrix_authentication_service: Option<serde_yaml::Value>,
	#[serde(default)]
	oidc_providers: Vec<OidcProvider>,
	oidc_config: Option<OidcProvider>,
}

#[derive(Debug, Deserialize)]
pub(super) struct OidcProvider {
	idp_id: Option<String>,
	enabled: Option<bool>,
	pub(super) issuer: String,
	pub(super) user_mapping_provider: Option<serde_yaml::Value>,
}

fn default_media_store_path() -> PathBuf { "media_store".into() }

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
			config.media_store_path = dir.join(&config.media_store_path);
			for file in &mut config.app_service_config_files {
				*file = dir.join(&*file);
			}
		}

		Ok(config)
	}

	/// The OIDC provider Synapse records as `auth_provider` in
	/// user_external_ids: "oidc" for the legacy `oidc_config`, otherwise
	/// "oidc-" and its idp_id.
	pub(super) fn oidc_provider(&self, auth_provider: &str) -> Option<&OidcProvider> {
		// The legacy block is off unless enabled; list entries are on unless disabled.
		let legacy = self
			.oidc_config
			.iter()
			.filter(|provider| provider.enabled == Some(true));

		let listed = self
			.oidc_providers
			.iter()
			.filter(|provider| provider.enabled != Some(false));

		legacy
			.chain(listed)
			.find(|provider| match provider.idp_id.as_deref().unwrap_or("oidc") {
				| "oidc" => auth_provider == "oidc",
				| idp_id => auth_provider.strip_prefix("oidc-") == Some(idp_id),
			})
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
