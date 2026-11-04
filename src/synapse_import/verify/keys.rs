//! The public keys sampled events are re-verified against: every key
//! Synapse's cache holds, plus those tuwunel holds (its own and the imported
//! old ones). Nothing is fetched, so a server neither knows stays unverifiable.

use std::collections::BTreeSet;

use ruma::{
	CanonicalJsonObject, OwnedServerName,
	room_version_rules::RoomVersionRules,
	serde::Base64,
	signatures::{
		PublicKeyMap, VerificationError, Verified, required_server_signatures_to_verify_event,
		verify_event,
	},
};
use serde_json::Value;
use tuwunel_core::Result;
use tuwunel_service::Services;

use super::super::source::Source;

#[derive(Default)]
pub(super) struct Keys {
	map: PublicKeyMap,
	asked: BTreeSet<OwnedServerName>,
}

/// How one event's re-verification came out.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Outcome {
	Verified,
	Redacted,
	Unverifiable(String),
	Failed(String),
}

impl Keys {
	pub(super) async fn load(source: &Source) -> Result<Self> {
		let mut keys = Self::default();
		let rows = source
			.rows("SELECT server_name, key_json FROM server_keys_json", &[])
			.await?;

		for row in rows {
			for (key_id, key) in parse_key_json(row.get(1)) {
				keys.insert(row.get(0), key_id, key);
			}
		}

		if source.has_table("server_signature_keys").await? {
			let rows = source
				.rows("SELECT server_name, key_id, verify_key FROM server_signature_keys", &[])
				.await?;

			for row in rows {
				keys.insert(row.get(0), row.get(1), Base64::new(row.get(2)));
			}
		}

		Ok(keys)
	}

	fn insert(&mut self, server: &str, key_id: String, key: Base64) {
		self.map
			.entry(server.to_owned())
			.or_default()
			.insert(key_id, key);
	}

	/// Re-verifies `event`, in its federation form, after adding the keys
	/// tuwunel holds for the servers whose signatures it needs.
	pub(super) async fn verify(
		&mut self,
		services: &Services,
		event: &CanonicalJsonObject,
		rules: &RoomVersionRules,
	) -> Outcome {
		let servers = match required_server_signatures_to_verify_event(event, &rules.signatures) {
			| Ok(servers) => servers,
			| Err(e) => return Outcome::Failed(e.to_string()),
		};

		for server in servers {
			if self.asked.insert(server.clone()) {
				for (key_id, key) in services
					.server_keys
					.verify_keys_for(&server)
					.await
				{
					self.insert(server.as_str(), key_id.to_string(), key.key);
				}
			}
		}

		classify(verify_event(&self.map, event, rules))
	}
}

pub(super) fn classify(result: Result<Verified, VerificationError>) -> Outcome {
	match result {
		| Ok(Verified::All) => Outcome::Verified,
		| Ok(Verified::Signatures) => Outcome::Redacted,
		| Err(
			VerificationError::NoPublicKeysForEntity(server)
			| VerificationError::NoSupportedSignatureForEntity(server),
		) => Outcome::Unverifiable(server),
		| Err(e) => Outcome::Failed(e.to_string()),
	}
}

/// The current and old verify keys of a `/_matrix/key/v2/server` response.
pub(super) fn parse_key_json(json: &[u8]) -> Vec<(String, Base64)> {
	let Ok(json) = serde_json::from_slice::<Value>(json) else {
		return Vec::new();
	};

	["verify_keys", "old_verify_keys"]
		.into_iter()
		.filter_map(|field| json.get(field)?.as_object())
		.flatten()
		.filter_map(|(key_id, key)| {
			let key = Base64::parse(key.get("key")?.as_str()?).ok()?;
			Some((key_id.clone(), key))
		})
		.collect()
}
