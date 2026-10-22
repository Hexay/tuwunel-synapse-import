use std::{env::temp_dir, fs, path::PathBuf};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use ruma::signatures::Ed25519KeyPair;
use tuwunel_core::{Result, config::Figment, utils::random_string};

use super::{init, parse_synapse_key};
use crate::test_utils::fixture;

// RFC 8032 section 7.1, test 1.
const SEED: [u8; 32] = hex("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60");
const PUBLIC: [u8; 32] = hex("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a");

const fn hex(s: &str) -> [u8; 32] {
	const fn nibble(c: u8) -> u8 {
		match c {
			| b'0'..=b'9' => c - b'0',
			| _ => c - b'a' + 10,
		}
	}

	let s = s.as_bytes();
	let mut out = [0; 32];
	let mut i = 0;
	while i < 32 {
		out[i] = (nibble(s[i * 2]) << 4) | nibble(s[i * 2 + 1]);
		i += 1;
	}

	out
}

fn synapse_key_file(seed: &[u8]) -> String {
	format!("ed25519 a_Rfc1 {}\n", STANDARD.encode(seed).trim_end_matches('='))
}

fn write_key_file(seed: &[u8]) -> PathBuf {
	let path = temp_dir().join(format!("tuwunel-signing-{}.key", random_string(16)));
	fs::write(&path, synapse_key_file(seed)).expect("key file written");

	path
}

#[test]
fn parses_a_synapse_key_into_the_same_keypair() {
	let (version, der) = parse_synapse_key(&synapse_key_file(&SEED)).expect("parsed");
	let keypair = Ed25519KeyPair::from_der(&der, version).expect("valid DER");

	assert_eq!(keypair.version(), "a_Rfc1");
	assert_eq!(keypair.public_key(), PUBLIC);
}

#[test]
fn accepts_padded_seeds_and_surrounding_blank_lines() {
	let padded = format!("\n  ed25519 a_Rfc1 {}  \n\n", STANDARD.encode(SEED));
	let (_, der) = parse_synapse_key(&padded).expect("parsed");

	assert_eq!(
		der,
		parse_synapse_key(&synapse_key_file(&SEED))
			.expect("parsed")
			.1
	);
}

#[test]
fn rejects_malformed_key_files() {
	for contents in [
		"",
		"ed25519 a_Rfc1",
		"curve25519 a_Rfc1 nWGxne/9WmC6hEr0kuwsxERJxWl7MmkZcDusAxyuf2A",
		"ed25519 a_Rfc1 not*base64",
		"ed25519 a_Rfc1 nWGxne/9WmC6hEr0kuwsxERJxWl7MmkZ",
	] {
		parse_synapse_key(contents).expect_err(contents);
	}
}

#[tokio::test]
async fn configured_key_becomes_the_server_identity() -> Result {
	let path = write_key_file(&SEED);
	let config = Figment::new().merge(("signing_key_path", &path));
	let Some(fixture) = fixture(config).await? else {
		return Ok(());
	};

	let server_keys = &fixture.services.server_keys;
	assert_eq!(server_keys.active_key_id().as_str(), "ed25519:a_Rfc1");
	assert_eq!(server_keys.keypair().public_key(), PUBLIC);

	init(&fixture.services.db, Some(&path)).expect("the same key is accepted again");

	let mut other_seed = SEED;
	other_seed[0] ^= 1;
	init(&fixture.services.db, Some(&write_key_file(&other_seed)))
		.expect_err("a different key is refused");

	init(&fixture.services.db, Some(&path)).expect("the refused key changed nothing");

	Ok(())
}
