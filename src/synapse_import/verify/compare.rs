//! Comparisons of JSON values that tuwunel may legitimately extend.

use std::collections::BTreeSet;

use ruma::CanonicalJsonObject;
use serde_json::Value;

/// Device keys: the same apart from signatures, and every signature Synapse
/// holds kept (tuwunel adds the cross-signing ones into the key).
pub(super) fn same_keys(want: &Value, have: &Value) -> bool {
	let strip = |value: &Value| {
		let mut value = value.clone();
		if let Some(object) = value.as_object_mut() {
			object.remove("signatures");
			object.remove("unsigned");
		}
		value
	};

	strip(want) == strip(have) && signatures_kept(want, have)
}

fn signatures_kept(want: &Value, have: &Value) -> bool {
	let Some(want) = want.get("signatures").and_then(Value::as_object) else {
		return true;
	};

	want.iter().all(|(signer, keys)| {
		keys.as_object()
			.into_iter()
			.flatten()
			.all(|(key_id, signature)| {
				have.get("signatures")
					.and_then(|signers| signers.get(signer)?.get(key_id))
					== Some(signature)
			})
	})
}

/// The top-level fields in which two events differ, for a mismatch's detail.
pub(super) fn differing_fields(a: &CanonicalJsonObject, b: &CanonicalJsonObject) -> String {
	let fields: BTreeSet<&str> = a
		.keys()
		.chain(b.keys())
		.filter(|field| a.get(*field) != b.get(*field))
		.map(|field| &**field)
		.collect();

	fields.into_iter().collect::<Vec<_>>().join(", ")
}
