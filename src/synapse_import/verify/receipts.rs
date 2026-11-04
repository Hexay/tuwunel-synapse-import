//! Read receipts, public and private: each Synapse receipt of an active user
//! must point at the same event, in the same thread, in tuwunel.

use std::{collections::BTreeSet, pin::pin};

use futures::{StreamExt, TryStreamExt};
use ruma::{OwnedRoomId, RoomId, UserId, events::AnySyncEphemeralRoomEvent, serde::Raw};
use serde_json::Value;
use tuwunel_core::Result;

use super::{super::source::user_id, Ctx};

const DOMAIN: &str = "receipts";

/// (receipt type, user, event, thread) as clients see them.
type Receipt = (String, String, String, Option<String>);

pub(super) async fn verify(ctx: &mut Ctx<'_>) -> Result {
	let mut rows = pin!(
		ctx.source
			.stream(
				"SELECT room_id, receipt_type, user_id, event_id, thread_id FROM \
				 receipts_linearized ORDER BY room_id",
				&[],
			)
			.await?
	);

	// Ordered by room, so only one room's receipts are held at a time.
	let mut held: Option<(OwnedRoomId, BTreeSet<Receipt>)> = None;
	while let Some(row) = rows.try_next().await? {
		let (room, kind, event_id, thread): (&str, &str, &str, Option<&str>) =
			(row.get(0), row.get(1), row.get(3), row.get(4));

		let subject = format!("{room} {kind} {}", row.get::<_, &str>(2));
		let (Ok(room_id), Ok(user_id)) = (<&RoomId>::try_from(room), user_id(&row, 2)) else {
			ctx.out
				.excuse(DOMAIN, subject, "invalid room or user ID");
			continue;
		};

		if !ctx.active.contains(&user_id) {
			continue;
		}

		if kind != "m.read" && kind != "m.read.private" {
			ctx.out
				.excuse(DOMAIN, subject, "a receipt type tuwunel doesn't support");
			continue;
		}

		if held
			.as_ref()
			.is_none_or(|(held, _)| held != room_id)
		{
			let public = public_receipts(ctx, room_id).await;
			held = Some((room_id.to_owned(), public));
		}

		let thread = thread.map(ToOwned::to_owned);
		let receipt = (kind.to_owned(), user_id.to_string(), event_id.to_owned(), thread);
		let found = match kind {
			| "m.read" => held
				.as_ref()
				.is_some_and(|(_, public)| public.contains(&receipt)),
			| _ => private_receipts(ctx, room_id, &user_id)
				.await
				.contains(&receipt),
		};

		if found {
			ctx.out.matched(DOMAIN);
		} else {
			ctx.out
				.missing(DOMAIN, subject, format!("no receipt at {event_id}"));
		}
	}

	Ok(())
}

async fn public_receipts(ctx: &Ctx<'_>, room_id: &RoomId) -> BTreeSet<Receipt> {
	ctx.services
		.read_receipt
		.readreceipts_since(room_id, 0, None)
		.map(|(_, _, event)| receipts(&event))
		.collect::<Vec<_>>()
		.await
		.into_iter()
		.flatten()
		.collect()
}

async fn private_receipts(
	ctx: &Ctx<'_>,
	room_id: &RoomId,
	user_id: &UserId,
) -> BTreeSet<Receipt> {
	ctx.services
		.read_receipt
		.private_read_get(room_id, user_id)
		.await
		.map(|events| events.iter().flat_map(receipts).collect())
		.unwrap_or_default()
}

/// The receipts of an `m.receipt` event.
pub(super) fn receipts(event: &Raw<AnySyncEphemeralRoomEvent>) -> Vec<Receipt> {
	let Ok(event) = serde_json::from_str::<Value>(event.json().get()) else {
		return Vec::new();
	};

	let Some(content) = event.get("content").and_then(Value::as_object) else {
		return Vec::new();
	};

	let mut out = Vec::new();
	for (event_id, kinds) in content {
		for (kind, users) in kinds.as_object().into_iter().flatten() {
			for (user, receipt) in users.as_object().into_iter().flatten() {
				let thread = receipt
					.get("thread_id")
					.and_then(Value::as_str)
					.map(ToOwned::to_owned);

				out.push((kind.clone(), user.clone(), event_id.clone(), thread));
			}
		}
	}

	out
}
