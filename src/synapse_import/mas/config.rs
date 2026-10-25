//! The parts of MAS's config.yaml the import reads. Everything else it needs
//! (upstream providers, clients) MAS mirrors into its database.

use std::path::{Path, PathBuf};

use ruma::OwnedServerName;
use serde::Deserialize;
use tuwunel_core::{Result, err};

#[derive(Debug, Deserialize)]
pub(in super::super) struct MasConfig {
	pub(super) matrix: Matrix,
	#[serde(default)]
	pub(super) passwords: Passwords,
}

#[derive(Debug, Deserialize)]
pub(super) struct Matrix {
	pub(super) homeserver: OwnedServerName,
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct Passwords {
	#[serde(default)]
	pub(super) schemes: Vec<Scheme>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Scheme {
	pub(super) version: i32,
	pub(super) algorithm: String,
	secret: Option<String>,
	secret_file: Option<PathBuf>,
}

impl MasConfig {
	pub(super) fn read(path: &Path) -> Result<Self> {
		let yaml = std::fs::read_to_string(path)
			.map_err(|e| err!("Cannot read MAS's config {}: {e}", path.display()))?;

		serde_yaml::from_str(&yaml)
			.map_err(|e| err!("Cannot parse MAS's config {}: {e}", path.display()))
	}

	pub(super) fn scheme(&self, version: i32) -> Option<&Scheme> {
		self.passwords
			.schemes
			.iter()
			.find(|scheme| scheme.version == version)
	}
}

impl Scheme {
	pub(super) fn secret(&self) -> Result<Option<String>> {
		match (&self.secret, &self.secret_file) {
			| (Some(secret), _) => Ok(Some(secret.clone())),
			| (None, Some(path)) => std::fs::read_to_string(path)
				.map(Some)
				.map_err(|e| err!("Cannot read MAS password secret {}: {e}", path.display())),
			| (None, None) => Ok(None),
		}
	}
}
