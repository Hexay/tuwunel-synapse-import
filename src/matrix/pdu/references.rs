//! Event references in `prev_events` and `auth_events`.
//!
//! Room versions 1 and 2 reference an event as `[event_id, {"sha256": hash}]`,
//! the referenced event's reference hash; later versions by its ID alone. A
//! stored PDU keeps its references in the form it was signed in, since its
//! content hash and signatures cover them; the typed [`super::Pdu`] holds only
//! the IDs.

#[cfg(test)]
mod tests;

use std::{fmt, marker::PhantomData};

use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, EventId, OwnedEventId,
	room_version_rules::RoomVersionRules, signatures::reference_hash,
};
use serde::{
	Deserialize, Deserializer,
	de::{self, IgnoredAny, SeqAccess, Visitor},
};
use smallvec::{Array, SmallVec};
use tuwunel_core::Result;

/// The event IDs of a `prev_events` or `auth_events` value, in either form.
pub fn reference_ids(value: Option<&CanonicalJsonValue>) -> impl Iterator<Item = &str> + Send {
	value
		.and_then(CanonicalJsonValue::as_array)
		.into_iter()
		.flatten()
		.filter_map(|reference| match reference {
			| CanonicalJsonValue::String(event_id) => Some(event_id.as_str()),
			| CanonicalJsonValue::Array(pair) => pair.first()?.as_str(),
			| _ => None,
		})
}

/// The room version 1 and 2 reference to `event` (in its federation form).
pub fn legacy_reference(
	event_id: &EventId,
	event: &CanonicalJsonObject,
	rules: &RoomVersionRules,
) -> Result<CanonicalJsonValue> {
	let hashes = [("sha256".into(), CanonicalJsonValue::String(reference_hash(event, rules)?))];

	Ok(CanonicalJsonValue::Array(vec![
		CanonicalJsonValue::String(event_id.as_str().to_owned()),
		CanonicalJsonValue::Object(hashes.into()),
	]))
}

/// Deserializes `prev_events` or `auth_events` in either form into event IDs;
/// for any struct reading those fields of a stored PDU.
pub fn deserialize<'de, D, A>(deserializer: D) -> Result<SmallVec<A>, D::Error>
where
	D: Deserializer<'de>,
	A: Array<Item = OwnedEventId>,
{
	deserializer.deserialize_seq(References(PhantomData))
}

struct References<A>(PhantomData<A>);

impl<'de, A: Array<Item = OwnedEventId>> Visitor<'de> for References<A> {
	type Value = SmallVec<A>;

	fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("an array of event references")
	}

	fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Self::Value, S::Error> {
		let mut event_ids = SmallVec::new();
		while let Some(Reference(event_id)) = seq.next_element()? {
			event_ids.push(event_id);
		}

		Ok(event_ids)
	}
}

struct Reference(OwnedEventId);

impl<'de> Deserialize<'de> for Reference {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		deserializer.deserialize_any(ReferenceVisitor)
	}
}

struct ReferenceVisitor;

impl<'de> Visitor<'de> for ReferenceVisitor {
	type Value = Reference;

	fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("an event ID or an [event ID, hashes] pair")
	}

	fn visit_str<E: de::Error>(self, event_id: &str) -> Result<Reference, E> {
		OwnedEventId::try_from(event_id)
			.map(Reference)
			.map_err(E::custom)
	}

	fn visit_seq<S: SeqAccess<'de>>(self, mut pair: S) -> Result<Reference, S::Error> {
		let event_id = pair
			.next_element()?
			.ok_or_else(|| de::Error::invalid_length(0, &self))?;

		while pair.next_element::<IgnoredAny>()?.is_some() {}

		Ok(Reference(event_id))
	}
}
