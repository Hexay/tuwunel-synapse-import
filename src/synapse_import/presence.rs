//! Local users' last presence and status message. tuwunel resets online users
//! to offline when it starts, keeping their status message, until their
//! clients sync again.

use ruma::{MilliSecondsSinceUnixEpoch, UInt, presence::PresenceState};
use tuwunel_core::Result;
use tuwunel_service::Services;

use super::{
	Report,
	source::{Source, user_id},
};

pub(super) async fn import(services: &Services, source: &Source, report: &mut Report) -> Result {
	// Synapse appends a row per change; the newest is the user's presence.
	let rows = source
		.rows(
			"SELECT DISTINCT ON (user_id) user_id, state, last_active_ts, status_msg FROM \
			 presence_stream ORDER BY user_id, stream_id DESC",
			&[],
		)
		.await?;

	let now = MilliSecondsSinceUnixEpoch::now().get();
	for row in rows {
		let Some(user_id) = report.keep("presence", user_id(&row, 0)) else {
			continue;
		};

		let (state, status_msg): (&str, Option<String>) = (row.get(1), row.get(3));
		let status_msg = status_msg.filter(|status| !status.is_empty());
		if !services.globals.user_is_local(&user_id)
			|| !services.users.is_active(&user_id).await
			|| (state == "offline" && status_msg.is_none())
		{
			continue;
		}

		let last_active_ago = row
			.get::<_, Option<i64>>(2)
			.filter(|&ts| ts > 0)
			.and_then(|ts| UInt::try_from(ts).ok())
			.map(|ts| now.saturating_sub(ts));

		services
			.presence
			.set_presence(&user_id, &PresenceState::from(state), None, last_active_ago, status_msg)
			.await?;

		Report::count(&mut report.presence);
	}

	Ok(())
}
