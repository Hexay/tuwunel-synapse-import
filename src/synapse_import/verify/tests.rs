use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, RoomVersionId,
	serde::{Base64, Raw},
	signatures::{Ed25519KeyPair, PublicKeyMap, hash_and_sign_event, verify_event},
};
use serde_json::{Value, json};
use tuwunel_matrix::room_version;

use super::{
	compare::{differing_fields, same_keys},
	events::auth_events,
	keys::{Outcome, classify, parse_key_json},
	receipts::receipts,
	report::Verification,
	userdata::rule_actions,
};

fn object(value: Value) -> CanonicalJsonObject {
	serde_json::from_value(value).expect("a canonical JSON object")
}

#[test]
fn tally_counts_each_outcome() {
	let mut out = Verification::new(8, "seed");
	out.matched("users");
	out.missing("users", "@a:x", "no such user");
	out.extra("users", "@b:x", "not a Synapse user");
	out.differs("users", "@c:x", "admin");
	out.excuse("users", "@d:x", "guest");

	let users = &out.domains["users"];
	assert_eq!((users.synapse, users.tuwunel, users.matched), (4, 3, 1), "items per side");
	assert_eq!((users.excused, users.mismatched), (1, 3), "excused and mismatched");
	assert_eq!(out.mismatches_total, 3, "three mismatches");
	assert!(!out.passed(), "mismatches fail the verification");
}

#[test]
fn count_comparison_matches_items_or_reports_once() {
	let mut out = Verification::new(8, "seed");
	out.count("room_keys", "@a:x 1", 5, 5);
	assert!(out.passed(), "equal counts pass");
	assert_eq!(out.domains["room_keys"].matched, 5, "every item matched");

	out.count("room_keys", "@a:x 2", 5, 4);
	assert_eq!(out.mismatches_total, 1, "one mismatch for the group");
	assert_eq!(out.mismatches[0].detail, "Synapse 5, tuwunel 4", "both counts named");
}

#[test]
fn summary_states_the_verdict() {
	let mut out = Verification::new(0, "seed");
	out.matched("rooms");
	assert!(out.summary().contains("Verification passed"), "a clean run passes");
	assert!(out.summary().contains("every event"), "sample 0 checks every event");

	out.missing("state", "!r:x m.room.topic \"\"", "not in the current state");
	let summary = out.summary();
	assert!(summary.contains("Verification FAILED: 1 mismatches"), "{summary}");
	assert!(summary.contains("MISMATCH state missing: !r:x m.room.topic"), "{summary}");
}

#[test]
fn mismatches_beyond_the_cap_are_counted_not_kept() {
	let mut out = Verification::new(8, "seed");
	for i in 0..1500 {
		out.missing("events", i, "gone");
	}

	assert_eq!(out.mismatches_total, 1500, "all counted");
	assert_eq!(out.mismatches.len(), 1000, "a thousand kept");
	assert!(
		out.summary()
			.contains("... and 1450 more mismatches"),
		"the rest summarized"
	);
}

#[test]
fn device_keys_may_gain_signatures_but_not_lose_them() {
	let want = json!({"keys": {"ed25519:D": "k"}, "signatures": {"@a:x": {"ed25519:D": "s"}}});
	let gained = json!({"keys": {"ed25519:D": "k"}, "signatures": {"@a:x": {"ed25519:D": "s", "ed25519:M": "m"}}});
	let lost = json!({"keys": {"ed25519:D": "k"}, "signatures": {}});
	let rekeyed =
		json!({"keys": {"ed25519:D": "other"}, "signatures": {"@a:x": {"ed25519:D": "s"}}});

	assert!(same_keys(&want, &gained), "a cross-signing signature added");
	assert!(!same_keys(&want, &lost), "the device's own signature dropped");
	assert!(!same_keys(&want, &rekeyed), "a different key");
}

#[test]
fn differing_fields_names_top_level_keys() {
	let a = object(json!({"type": "m.room.message", "content": {"body": "a"}, "depth": 3}));
	let b = object(json!({"type": "m.room.message", "content": {"body": "b"}, "origin": "x"}));

	assert_eq!(differing_fields(&a, &b), "content, depth, origin");
}

#[test]
fn auth_events_reads_both_reference_formats() {
	let modern = object(json!({"auth_events": ["$a", "$b"]}));
	let legacy = object(json!({"auth_events": [["$a:x", {"sha256": "h"}]]}));

	assert_eq!(auth_events(&modern).len(), 2, "plain event IDs");
	assert_eq!(auth_events(&legacy)[0].as_str(), "$a:x", "room v1/v2 pairs");
	assert!(auth_events(&object(json!({}))).is_empty(), "none");
}

#[test]
fn key_json_gives_current_and_old_keys() {
	let key: Base64 = Base64::new(vec![7_u8; 32]);
	let key = key.encode();
	let json = json!({
		"verify_keys": {"ed25519:new": {"key": key}},
		"old_verify_keys": {"ed25519:old": {"key": key, "expired_ts": 1}},
	});

	let keys = parse_key_json(json.to_string().as_bytes());
	let ids: Vec<&str> = keys.iter().map(|(id, _)| id.as_str()).collect();
	assert_eq!(ids, ["ed25519:new", "ed25519:old"]);
	assert!(parse_key_json(b"not json").is_empty(), "unreadable cache rows are ignored");
}

#[test]
fn signature_outcomes_are_classified() {
	let rules = room_version::rules(&RoomVersionId::V11).expect("v11 rules");
	let pair = Ed25519KeyPair::from_der(&Ed25519KeyPair::generate(), "1".to_owned())
		.expect("a key pair");
	let mut event = object(json!({
		"type": "m.room.message", "room_id": "!r:origin.test", "sender": "@a:origin.test",
		"content": {"body": "hi"}, "depth": 1, "origin_server_ts": 1,
		"auth_events": [], "prev_events": [],
	}));
	hash_and_sign_event("origin.test", &pair, &mut event, &rules.redaction).expect("signed");

	let keys: PublicKeyMap = [(
		"origin.test".to_owned(),
		[("ed25519:1".to_owned(), Base64::new(pair.public_key().to_vec()))].into(),
	)]
	.into();

	assert_eq!(classify(verify_event(&keys, &event, &rules)), Outcome::Verified);
	assert_eq!(
		classify(verify_event(&PublicKeyMap::new(), &event, &rules)),
		Outcome::Unverifiable("origin.test".to_owned())
	);

	let mut edited = event.clone();
	edited
		.insert("content".into(), CanonicalJsonValue::Object(object(json!({"body": "edited"}))));
	assert_eq!(
		classify(verify_event(&keys, &edited, &rules)),
		Outcome::Redacted,
		"hash fails, signature holds"
	);

	let mut forged = event;
	forged.insert("origin_server_ts".into(), CanonicalJsonValue::Integer(2_u32.into()));
	assert!(
		matches!(classify(verify_event(&keys, &forged, &rules)), Outcome::Failed(_)),
		"signature fails"
	);
}

#[test]
fn receipts_are_read_from_a_receipt_event() {
	let event = json!({"type": "m.receipt", "content": {
		"$e": {"m.read": {"@a:x": {"ts": 1}, "@b:x": {"ts": 2, "thread_id": "main"}}},
	}});
	let raw = Raw::from_json(serde_json::value::to_raw_value(&event).expect("raw"));

	let mut found = receipts(&raw);
	found.sort();
	assert_eq!(found, [
		("m.read".to_owned(), "@a:x".to_owned(), "$e".to_owned(), None),
		("m.read".to_owned(), "@b:x".to_owned(), "$e".to_owned(), Some("main".to_owned())),
	]);
}

#[test]
fn push_rule_actions_normalize_or_explain() {
	let legacy =
		rule_actions(Some(r#"["notify", {"set_tweak": "highlight"}]"#), None).expect("readable");
	let modern =
		rule_actions(Some(r#"["notify", {"set_tweak": "highlight", "value": true}]"#), None)
			.expect("readable");
	assert_eq!(legacy, modern, "the same actions, as tuwunel writes them");

	assert_eq!(rule_actions(Some("nope"), None), Err("Synapse stored invalid JSON"));
	assert_eq!(rule_actions(Some("[]"), Some("{bad")), Err("Synapse stored invalid JSON"));
}
