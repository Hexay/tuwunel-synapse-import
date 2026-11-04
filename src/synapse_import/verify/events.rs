//! Deep checks on a sample of a room's accepted events: the event tuwunel
//! serves over federation is Synapse's (or its redaction, where Synapse has a
//! redaction or the sender was erased), its reference hash still gives its
//! event ID, its hashes and signatures re-verify, and its auth chain is
//! complete in tuwunel.

use std::{collections::HashSet, pin::pin};

use futures::TryStreamExt;
use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, EventId, OwnedEventId, OwnedUserId,
	canonical_json::redact,
};
use tuwunel_core::Result;
use tuwunel_matrix::{event::gen_event_id, pdu::into_outgoing_federation};

use super::{
	Ctx,
	compare::differing_fields,
	keys::{Keys, Outcome},
	rooms::Room,
};

/// Events one auth chain walk visits before giving up on it.
const CHAIN_LIMIT: usize = 20_000;

/// Auth events per room remembered as complete, so later samples stop early.
const MEMO_LIMIT: usize = 100_000;

const SAMPLE: &str = "SELECT e.event_id, j.json FROM events e JOIN event_json j USING \
                      (event_id) LEFT JOIN rejections r USING (event_id) WHERE e.room_id = $1 \
                      AND e.rejection_reason IS NULL AND r.event_id IS NULL ORDER BY \
                      md5(e.event_id || $2) LIMIT $3";

const EVERY: &str = "SELECT e.event_id, j.json FROM events e JOIN event_json j USING (event_id) \
                     LEFT JOIN rejections r USING (event_id) WHERE e.room_id = $1 AND \
                     e.rejection_reason IS NULL AND r.event_id IS NULL";

pub(super) async fn sample(ctx: &mut Ctx<'_>, room: &Room<'_>, keys: &mut Keys) -> Result {
	let room_id = room.id.as_str();
	let limit = i64::try_from(ctx.options.sample).unwrap_or(i64::MAX);
	let mut rows = pin!(match ctx.options.sample {
		| 0 => ctx.source.stream(EVERY, &[&room_id]).await?,
		| _ =>
			ctx.source
				.stream(SAMPLE, &[&room_id, &ctx.options.seed, &limit])
				.await?,
	});

	let mut complete = HashSet::new();
	while let Some(row) = rows.try_next().await? {
		ctx.out.events_sampled = ctx.out.events_sampled.saturating_add(1);
		check(ctx, room, keys, &mut complete, row.get(0), row.get(1)).await?;
	}

	Ok(())
}

async fn check(
	ctx: &mut Ctx<'_>,
	room: &Room<'_>,
	keys: &mut Keys,
	complete: &mut HashSet<OwnedEventId>,
	event_id: &str,
	json: &str,
) -> Result {
	let subject = format!("{} {event_id}", room.id);
	let (Ok(id), Ok(mut synapse)) = (
		<&EventId>::try_from(event_id),
		serde_json::from_str::<CanonicalJsonObject>(json),
	) else {
		ctx.out
			.excuse("event_json", subject, "not importable: invalid event ID or JSON");
		return Ok(());
	};

	let Ok(mut stored) = ctx.services.timeline.get_pdu_json(id).await else {
		room.lacks(ctx, "event_json", event_id, "not stored")
			.await?;
		return Ok(());
	};

	synapse.remove("unsigned");
	stored.remove("unsigned");

	let want = room.stored_form(id, synapse.clone());
	if stored == want {
		ctx.out.matched("event_json");
	} else if !redaction_allowed(ctx, room, id, &synapse).await? {
		ctx.out.differs(
			"event_json",
			&subject,
			format!("differs from Synapse's in: {}", differing_fields(&want, &stored)),
		);
	} else {
		match redact(synapse.clone(), &room.rules.redaction, None)
			.map(|pruned| room.stored_form(id, pruned))
		{
			| Ok(pruned) if pruned == stored => ctx.out.matched("event_json"),
			| pruned => {
				let fields = pruned
					.map(|pruned| differing_fields(&pruned, &stored))
					.unwrap_or_default();
				ctx.out.differs(
					"event_json",
					&subject,
					format!("neither Synapse's event nor its redaction, in: {fields}"),
				);
			},
		}
	}

	let served = into_outgoing_federation(stored, &room.version);
	match gen_event_id(&served, &room.version) {
		| Ok(derived) if derived == id => ctx.out.matched("reference_hash"),
		| derived =>
			ctx.out
				.differs("reference_hash", &subject, format!("its JSON gives {derived:?}")),
	}

	signatures(ctx, room, keys, &subject, &synapse, &served).await;
	auth_chain(ctx, room, complete, &subject, &served).await
}

/// Whether tuwunel may hold the event redacted: Synapse has a redaction of
/// it, or its sender was erased.
async fn redaction_allowed(
	ctx: &Ctx<'_>,
	room: &Room<'_>,
	id: &EventId,
	event: &CanonicalJsonObject,
) -> Result<bool> {
	let erased = event
		.get("sender")
		.and_then(CanonicalJsonValue::as_str)
		.and_then(|sender| OwnedUserId::try_from(sender).ok())
		.is_some_and(|sender| room.is_erased(&sender));

	if erased {
		return Ok(true);
	}

	let rows = ctx
		.source
		.rows("SELECT 1 FROM redactions WHERE redacts = $1", &[&id.as_str()])
		.await?;

	Ok(!rows.is_empty())
}

/// A failure counts only if Synapse's own copy verifies: tuwunel must not
/// break what Synapse held, but can't repair it either.
async fn signatures(
	ctx: &mut Ctx<'_>,
	room: &Room<'_>,
	keys: &mut Keys,
	subject: &str,
	synapse: &CanonicalJsonObject,
	served: &CanonicalJsonObject,
) {
	let signatures = &mut ctx.out.signatures;
	match keys
		.verify(ctx.services, served, &room.rules)
		.await
	{
		| Outcome::Verified => signatures.verified = signatures.verified.saturating_add(1),
		| Outcome::Redacted => signatures.redacted = signatures.redacted.saturating_add(1),
		| Outcome::Unverifiable(server) => {
			signatures.unverifiable = signatures.unverifiable.saturating_add(1);
			ctx.out.unverifiable_servers.insert(server);
		},
		| Outcome::Failed(_) if room.hashless_references() => ctx.out.excuse(
			"signatures",
			subject,
			"room v1/v2: tuwunel stores event references without their hashes, so its copy \
			 can't be re-verified",
		),
		| Outcome::Failed(e) => match keys
			.verify(ctx.services, synapse, &room.rules)
			.await
		{
			| Outcome::Failed(_) =>
				ctx.out
					.excuse("signatures", subject, format!("fails in Synapse too: {e}")),
			| _ => {
				signatures.failed = signatures.failed.saturating_add(1);
				ctx.out.differs("signatures", subject, e);
			},
		},
	}
}

/// Walks the auth events down to the create event; each must be stored.
async fn auth_chain(
	ctx: &mut Ctx<'_>,
	room: &Room<'_>,
	complete: &mut HashSet<OwnedEventId>,
	subject: &str,
	event: &CanonicalJsonObject,
) -> Result {
	let mut queue = auth_events(event);
	let mut visited = HashSet::new();
	let mut whole = true;
	while let Some(id) = queue.pop() {
		if complete.contains(&id) || !visited.insert(id.clone()) {
			continue;
		}

		if visited.len() > CHAIN_LIMIT {
			ctx.out.excuse(
				"auth_chains",
				subject,
				format!("walk stopped after {CHAIN_LIMIT} events"),
			);
			return Ok(());
		}

		match ctx.services.timeline.get_pdu_json(&id).await {
			| Ok(json) => queue.extend(auth_events(&json)),
			| Err(_) =>
				if room
					.lacks(ctx, "auth_events", id.as_str(), "in an auth chain, but not stored")
					.await?
				{
					whole = false;
				},
		}
	}

	if whole {
		ctx.out.matched("auth_chains");
		if complete.len() < MEMO_LIMIT {
			complete.extend(visited);
		}
	} else {
		ctx.out
			.differs("auth_chains", subject, "incomplete");
	}

	Ok(())
}

pub(super) fn auth_events(event: &CanonicalJsonObject) -> Vec<OwnedEventId> {
	let Some(CanonicalJsonValue::Array(ids)) = event.get("auth_events") else {
		return Vec::new();
	};

	ids.iter()
		.filter_map(|id| match id {
			| CanonicalJsonValue::String(id) => OwnedEventId::try_from(id.as_str()).ok(),
			// Room versions 1 and 2 reference events as [id, hashes].
			| CanonicalJsonValue::Array(pair) => pair.first()?.as_str()?.try_into().ok(),
			| _ => None,
		})
		.collect()
}
