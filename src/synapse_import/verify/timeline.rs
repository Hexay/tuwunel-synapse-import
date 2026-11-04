//! A room's timeline, walked on both sides at once in the order clients page
//! it: Synapse's /messages order against tuwunel's. Memory stays constant: an
//! event missing on one side is looked up, not buffered. Once the orders
//! disagree, the rest is checked for presence only.

use std::{future::ready, pin::pin};

use futures::{Stream, TryStreamExt};
use ruma::{EventId, OwnedEventId};
use tuwunel_core::Result;
use tuwunel_matrix::Event as _;

use super::{
	super::rooms::{soft_failed, timeline},
	Ctx,
	rooms::Room,
};

const DOMAIN: &str = "events";

pub(super) async fn walk(ctx: &mut Ctx<'_>, room: &Room<'_>) -> Result {
	let services = ctx.services;
	let synapse = timeline(ctx.source, room.id)
		.await?
		.try_filter(|placement| ready(!placement.soft_failed))
		.map_ok(|placement| placement.event_id);

	let tuwunel = services
		.timeline
		.pdus(None, room.id, None)
		.map_ok(|(_, pdu)| pdu.event_id().to_owned());

	let (mut synapse, mut tuwunel) = (pin!(synapse), pin!(tuwunel));
	let (mut s, mut t) = (synapse.try_next().await?, tuwunel.try_next().await?);
	loop {
		match (s.take(), t.take()) {
			| (None, None) => return Ok(()),
			| (Some(a), Some(b)) if a == b => {
				resolvable(ctx, room, &a).await;
				(s, t) = (synapse.try_next().await?, tuwunel.try_next().await?);
			},
			| (Some(a), b) if !in_tuwunel(ctx, &a).await => {
				room.lacks(ctx, DOMAIN, a.as_str(), "not in the timeline")
					.await?;
				(s, t) = (synapse.try_next().await?, b);
			},
			| (a, Some(b)) if !in_synapse(ctx, room, &b).await? => {
				ctx.out
					.extra(DOMAIN, format!("{} {b}", room.id), "not in Synapse's timeline");
				(s, t) = (a, tuwunel.try_next().await?);
			},
			| (a, b) => {
				let at = a
					.as_ref()
					.or(b.as_ref())
					.map(ToString::to_string)
					.unwrap_or_default();
				ctx.out.differs(
					DOMAIN,
					room.id,
					format!("timeline order differs from Synapse's at {at}"),
				);
				return unordered(ctx, room, a, b, synapse, tuwunel).await;
			},
		}
	}
}

/// After the orders disagree: each remaining event, on each side, must be on
/// the other.
async fn unordered<S, T>(
	ctx: &mut Ctx<'_>,
	room: &Room<'_>,
	s: Option<OwnedEventId>,
	t: Option<OwnedEventId>,
	mut synapse: S,
	mut tuwunel: T,
) -> Result
where
	S: Stream<Item = Result<OwnedEventId>> + Unpin,
	T: Stream<Item = Result<OwnedEventId>> + Unpin,
{
	let mut next = s;
	while let Some(a) = next {
		if in_tuwunel(ctx, &a).await {
			ctx.out.matched(DOMAIN);
		} else {
			room.lacks(ctx, DOMAIN, a.as_str(), "not in the timeline")
				.await?;
		}
		next = synapse.try_next().await?;
	}

	let mut next = t;
	while let Some(b) = next {
		if !in_synapse(ctx, room, &b).await? {
			ctx.out
				.extra(DOMAIN, format!("{} {b}", room.id), "not in Synapse's timeline");
		}
		next = tuwunel.try_next().await?;
	}

	Ok(())
}

/// In the timeline, and found by its ID, as clients and servers ask for it.
async fn resolvable(ctx: &mut Ctx<'_>, room: &Room<'_>, event_id: &EventId) {
	if in_tuwunel(ctx, event_id).await {
		ctx.out.matched(DOMAIN);
	} else {
		ctx.out.differs(
			DOMAIN,
			format!("{} {event_id}", room.id),
			"in the timeline, but its ID doesn't resolve",
		);
	}
}

async fn in_tuwunel(ctx: &Ctx<'_>, event_id: &EventId) -> bool {
	ctx.services
		.timeline
		.get_pdu_id(event_id)
		.await
		.is_ok()
		&& ctx
			.services
			.timeline
			.get_pdu_json(event_id)
			.await
			.is_ok()
}

/// In Synapse's timeline of the room: accepted, and not soft-failed.
async fn in_synapse(ctx: &Ctx<'_>, room: &Room<'_>, event_id: &EventId) -> Result<bool> {
	let rows = ctx
		.source
		.rows(
			"SELECT j.internal_metadata FROM events e JOIN event_json j USING (event_id) LEFT \
			 JOIN rejections r USING (event_id) WHERE e.event_id = $1 AND e.room_id = $2 AND \
			 NOT e.outlier AND e.rejection_reason IS NULL AND r.event_id IS NULL",
			&[&event_id.as_str(), &room.id.as_str()],
		)
		.await?;

	Ok(rows
		.first()
		.is_some_and(|row| !soft_failed(row.get(0))))
}
