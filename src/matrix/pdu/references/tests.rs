use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, RoomVersionId, event_id, owned_event_id, room_id,
	serde::Base64,
	signatures::{Ed25519KeyPair, PublicKeyMap, Verified, hash_and_sign_event, verify_event},
};
use serde_json::{from_value, json};

use super::{super::Pdu, legacy_reference, reference_ids};
use crate::pdu::{from_incoming_federation, into_outgoing_federation};

fn object(value: serde_json::Value) -> CanonicalJsonObject {
	from_value(value).expect("canonical JSON object")
}

/// A room version 1 or 2 event as its origin sent it, signed by origin.test.
fn signed_legacy_event(version: &RoomVersionId) -> (CanonicalJsonObject, PublicKeyMap) {
	let rules = version.rules().expect("rules");
	let pair =
		Ed25519KeyPair::from_der(&Ed25519KeyPair::generate(), "1".to_owned()).expect("pair");
	let mut event = object(json!({
		"event_id": "$e:origin.test", "type": "m.room.message", "room_id": "!r:origin.test",
		"sender": "@a:origin.test", "origin": "origin.test", "content": {"body": "hi"},
		"depth": 3, "origin_server_ts": 1,
		"prev_events": [["$p:origin.test", {"sha256": "cHJldmlvdXM"}]],
		"auth_events": [["$c:origin.test", {"sha256": "Y3JlYXRl"}], ["$m:origin.test", {"sha256": "bWVtYmVy"}]],
	}));
	hash_and_sign_event("origin.test", &pair, &mut event, &rules.redaction).expect("signed");

	let key = [("ed25519:1".to_owned(), Base64::new(pair.public_key().to_vec()))];
	(event, [("origin.test".to_owned(), key.into())].into())
}

#[test]
fn legacy_event_is_served_as_signed() {
	for version in [RoomVersionId::V1, RoomVersionId::V2] {
		let rules = version.rules().expect("rules");
		let (sent, keys) = signed_legacy_event(&version);
		let stored = from_incoming_federation(
			room_id!("!r:origin.test"),
			event_id!("$e:origin.test"),
			sent.clone(),
			&rules,
		);

		let served = into_outgoing_federation(stored, &version);
		assert_eq!(served, sent, "{version}: served as received");
		assert_eq!(verify_event(&keys, &served, &rules).ok(), Some(Verified::All), "{version}");
	}
}

#[test]
fn pdu_reads_both_reference_forms() {
	let (legacy, _) = signed_legacy_event(&RoomVersionId::V1);
	let mut modern = legacy.clone();
	modern.insert("prev_events".into(), CanonicalJsonValue::Array(vec!["$p:origin.test".into()]));

	for event in [legacy, modern] {
		let pdu = Pdu::from_object(event).expect("valid PDU");
		assert_eq!(pdu.prev_events.as_slice(), [owned_event_id!("$p:origin.test")]);
		assert_eq!(pdu.auth_events.len(), 2);
	}

	let mut broken = signed_legacy_event(&RoomVersionId::V1).0;
	broken.insert(
		"prev_events".into(),
		CanonicalJsonValue::Array(vec![CanonicalJsonValue::Array(vec![])]),
	);
	assert!(Pdu::from_object(broken).is_err(), "a pair without an event ID");
}

#[test]
fn reference_ids_reads_both_forms() {
	let (event, _) = signed_legacy_event(&RoomVersionId::V2);
	let ids: Vec<_> = reference_ids(event.get("auth_events")).collect();
	assert_eq!(ids, ["$c:origin.test", "$m:origin.test"]);

	let modern = CanonicalJsonValue::Array(vec!["$x".into(), CanonicalJsonValue::Null]);
	assert_eq!(reference_ids(Some(&modern)).collect::<Vec<_>>(), ["$x"]);
	assert_eq!(reference_ids(None).count(), 0);
}

#[test]
fn legacy_reference_hash_is_synapses() {
	let (event, hash) = synapse_vector();
	let rules = RoomVersionId::V1.rules().expect("rules");
	let id = event["event_id"].as_str().expect("event ID");
	let reference =
		legacy_reference(id.try_into().expect("event ID"), &event, &rules).expect("hash");

	let hashes = CanonicalJsonValue::Object(object(json!({"sha256": hash})));
	assert_eq!(reference, CanonicalJsonValue::Array(vec![id.into(), hashes]));
}

/// A v1 room's create event as Synapse 1.163 stored it, and the hash its next
/// event references it by.
fn synapse_vector() -> (CanonicalJsonObject, &'static str) {
	let create = object(json!({
		"signatures": {"fixture.test": {"ed25519:a_fix": "dm0KZu4apTC0Po5PR6/llvdputWRVLCOCN/X4ku2js3SuclLeChZFu8FN7GAI3gEWuvOFqN5TAGiF+8SoUJ+CA"}},
		"unsigned": {"age_ts": 1_794_066_408_806_u64},
		"auth_events": [], "prev_events": [], "room_id": "!oJQVyTSGLiQglUOMyi:fixture.test",
		"event_id": "$179406640849BLfzI:fixture.test",
		"content": {"creator": "@fern:fixture.test", "room_version": "1"}, "depth": 1,
		"hashes": {"sha256": "xI+1f5EJHZFcLShu342wKFIVejNAkPo9DDkUqhpgi5E"},
		"origin_server_ts": 1_794_066_408_806_u64, "sender": "@fern:fixture.test", "state_key": "",
		"type": "m.room.create",
	}));

	(create, "WnbLRryuupGqDiqSV+D1BAkMm+5AzgowhsUThV3cWuQ")
}
