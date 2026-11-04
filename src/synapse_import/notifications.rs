//! Unread counts and the `/notifications` list. After receipts, which the
//! counts were taken against, and before anything live can append.
//!
//! Synapse keeps a user's unread notifications as rolled-up summaries plus the
//! push actions since the last roll-up, and recomputes the counts from both on
//! every sync; [`counts`] repeats that computation once and stores the result
//! in tuwunel's counters. [`stamps`] records each receipt as the point the room
//! or thread was read to, which decides `/notifications`' `read` flag. The
//! list itself is Synapse's push actions that notified, which it keeps for a
//! day (highlights for a month), so tuwunel lists what Synapse would have.

mod counts;
mod stamps;
#[cfg(test)]
mod tests;

use std::pin::pin;

use futures::TryStreamExt;
use ruma::{OwnedEventId, api::client::push::ProfileTag, push::Actions};
use tokio_postgres::Row;
use tuwunel_core::{Err, Result, err};
use tuwunel_matrix::{PduCount, PduId};
use tuwunel_service::{Services, pusher::Notified};

use super::{
	Report,
	source::{Source, user_id},
};

/// What Synapse stores as the empty string: its usual actions, by whether
/// they highlight.
const DEFAULT_NOTIFY: &str = r#"["notify",{"set_tweak":"highlight","value":false}]"#;
const DEFAULT_HIGHLIGHT: &str =
	r#"["notify",{"set_tweak":"sound","value":"default"},{"set_tweak":"highlight"}]"#;

pub(super) async fn import(services: &Services, source: &Source, report: &mut Report) -> Result {
	counts::import(services, source, report).await?;
	stamps::import(services, source).await?;
	list(services, source, report).await
}

async fn list(services: &Services, source: &Source, report: &mut Report) -> Result {
	let mut rows = pin!(
		source
			.stream(
				"SELECT a.user_id, a.event_id, a.actions, a.highlight, a.profile_tag, \
				 e.received_ts FROM event_push_actions a JOIN events e USING (event_id) WHERE \
				 a.notif = 1 AND a.user_id IN (SELECT name FROM users) ORDER BY \
				 a.stream_ordering",
				&[],
			)
			.await?
	);

	while let Some(row) = rows.try_next().await? {
		match import_notification(services, &row).await {
			| Ok(()) => Report::count(&mut report.notifications),
			| Err(e) => report.skip(format!(
				"{}'s notification for {}: {e}",
				row.get::<_, &str>(0),
				row.get::<_, &str>(1)
			)),
		}
	}

	Ok(())
}

async fn import_notification(services: &Services, row: &Row) -> Result {
	let user_id = user_id(row, 0)?;
	let event_id = OwnedEventId::try_from(row.get::<_, &str>(1)).map_err(|e| err!("{e}"))?;
	let actions = actions(row.get(2), row.get::<_, i16>(3) == 1)?;
	let ts = row
		.get::<_, Option<i64>>(5)
		.and_then(|ts| u64::try_from(ts).ok())
		.ok_or_else(|| err!("Synapse has no time it received the event"))?;

	let pdu_id: PduId = services
		.timeline
		.get_pdu_id(&event_id)
		.await
		.map_err(|_| err!("the event wasn't imported"))?
		.into();

	let PduCount::Normal(count) = pdu_id.count else {
		return Err!("the event is backfilled");
	};

	let notified = Notified {
		ts,
		sroomid: pdu_id.shortroomid,
		tag: row
			.get::<_, Option<&str>>(4)
			.map(ProfileTag::from),
		actions,
	};

	services
		.pusher
		.import_notification(&user_id, count, &notified);

	Ok(())
}

/// A push action's actions as Synapse stored them, compressed or not.
fn actions(stored: &str, highlight: bool) -> Result<Actions> {
	let json = match stored {
		| "" if highlight => DEFAULT_HIGHLIGHT,
		| "" => DEFAULT_NOTIFY,
		| json => json,
	};

	serde_json::from_str(json).map_err(|e| err!("Synapse stored invalid actions: {e}"))
}
