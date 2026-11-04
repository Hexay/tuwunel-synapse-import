use std::path::PathBuf;

use tuwunel_service::server_keys::read_synapse_key_file;

use super::{
	MUST_BE_EMPTY, auth_problem, schema_problem, server_name_problem, signing_key_problem,
	target_problems, unfinished_problem,
};

const SEED_A: &str = "TM0Imyj/ltqdtsNG7BFOD1uKMZ81q6Yk2oz27U+4pvs";
const SEED_B: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

/// A Synapse signing key file, unique per test so they can run in parallel.
fn key_file(test: &str, version: &str, seed: &str) -> PathBuf {
	let path = std::env::temp_dir().join(format!(
		"tuwunel-preflight-{}-{test}.signing.key",
		std::process::id()
	));
	std::fs::write(&path, format!("ed25519 {version} {seed}\n")).expect("temp dir is writable");
	path
}

#[test]
fn schemas_in_the_tested_range_pass() {
	for schema in 84..=94 {
		assert_eq!(schema_problem(schema), None, "schema {schema}");
	}
}

#[test]
fn unsupported_schemas_are_refused() {
	for schema in [0, 83, 95] {
		let problem = schema_problem(schema).expect("refused");
		assert!(problem.contains(&format!("schema {schema} is not supported")), "{problem}");
		assert!(problem.contains("84-94"), "{problem}");
	}
}

#[test]
fn unfinished_work_is_refused_per_table() {
	for (table, why) in MUST_BE_EMPTY {
		assert_eq!(unfinished_problem(table, why, 0), None);
		let problem = unfinished_problem(table, why, 3).expect("refused");
		assert!(problem.starts_with(&format!("{table} isn't empty: {why}")), "{problem}");
	}
}

#[test]
fn background_updates_must_be_finished() {
	assert!(
		MUST_BE_EMPTY
			.iter()
			.any(|(table, _)| *table == "background_updates")
	);
}

#[test]
fn a_fresh_target_passes() {
	assert!(target_problems("@conduit:example.com", false, false, 1).is_empty());
}

#[test]
fn a_target_with_users_is_refused_unless_resuming() {
	let problems = target_problems("@conduit:example.com", false, false, 2);
	assert_eq!(problems, ["this tuwunel database already has users; import into a new one"]);
	assert!(target_problems("@conduit:example.com", false, true, 40).is_empty());
}

#[test]
fn a_synapse_user_named_like_the_server_user_is_refused() {
	for resuming in [false, true] {
		let problems = target_problems("@conduit:example.com", true, resuming, 1);
		assert_eq!(problems.len(), 1);
		assert!(problems[0].contains("Synapse has a user @conduit:example.com"), "{problems:?}");
		assert!(problems[0].contains("server_user_localpart"), "{problems:?}");
	}
}

#[test]
fn server_names_must_match() {
	assert_eq!(server_name_problem("example.com", "example.com"), None);
	let problem = server_name_problem("example.com", "example.org").expect("refused");
	assert!(
		problem.contains("server_name is example.com, but this server's is example.org"),
		"{problem}"
	);
}

#[test]
fn the_same_signing_key_passes() {
	let path = key_file("same", "a_key", SEED_A);
	let (ours, ..) = read_synapse_key_file(&path).unwrap();
	assert_eq!(signing_key_problem(&path, &ours), None);
}

#[test]
fn a_different_signing_key_is_refused() {
	let path = key_file("other-seed", "a_key", SEED_A);
	let (ours, ..) = read_synapse_key_file(&key_file("other-seed-ours", "a_key", SEED_B)).unwrap();
	let problem = signing_key_problem(&path, &ours).expect("refused");
	assert!(problem.contains("is ed25519:a_key, not Synapse's ed25519:a_key"), "{problem}");
	assert!(problem.contains(&format!("signing_key_path = \"{}\"", path.display())), "{problem}");
}

#[test]
fn the_same_key_under_another_id_is_refused() {
	let path = key_file("other-id", "a_key", SEED_A);
	let (ours, ..) = read_synapse_key_file(&key_file("other-id-ours", "b_key", SEED_A)).unwrap();
	let problem = signing_key_problem(&path, &ours).expect("refused");
	assert!(problem.contains("is ed25519:b_key, not Synapse's ed25519:a_key"), "{problem}");
}

#[test]
fn an_unreadable_synapse_key_is_refused() {
	let (ours, ..) = read_synapse_key_file(&key_file("unreadable-ours", "a_key", SEED_A)).unwrap();
	let missing = std::env::temp_dir().join("tuwunel-preflight-no-such.signing.key");
	let problem = signing_key_problem(&missing, &ours).expect("refused");
	assert!(problem.starts_with("Synapse's signing key: "), "{problem}");

	let garbage = key_file("garbage", "a_key", "not base64!");
	let problem = signing_key_problem(&garbage, &ours).expect("refused");
	assert!(problem.starts_with("Synapse's signing key: "), "{problem}");
}

#[test]
fn peppers_must_match_without_mas() {
	assert_eq!(auth_problem(false, false, None, None), None);
	assert_eq!(auth_problem(false, false, Some("p"), Some("p")), None);

	for (ours, synapse) in [(None, Some("p")), (Some("p"), None), (Some("p"), Some("q"))] {
		let problem = auth_problem(false, false, ours, synapse).expect("refused");
		assert!(problem.starts_with("imported_bcrypt_pepper must be set"), "{problem}");
	}
}

#[test]
fn mas_is_required_when_synapse_delegates_to_it() {
	let problem = auth_problem(true, false, None, None).expect("refused");
	assert!(problem.contains("pass its database and config with --mas-db"), "{problem}");
}

#[test]
fn mas_is_refused_when_synapse_does_not_delegate() {
	let problem = auth_problem(false, true, None, None).expect("refused");
	assert!(problem.starts_with("--mas-db was given, but Synapse doesn't delegate"), "{problem}");
}

#[test]
fn the_pepper_is_ignored_with_mas() {
	assert_eq!(auth_problem(true, true, None, Some("stale")), None);
}
