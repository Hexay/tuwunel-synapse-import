//! Synapse data tuwunel has no place for, counted in the report so nothing is
//! dropped silently.

use tuwunel_core::Result;

use super::{Report, mas::Mas, source::Source};

/// Table, what follows it in `SELECT count(*) FROM <table>`, and what its rows
/// are. Tables older Synapse releases don't have yet are skipped.
const LEFTOVERS: &[(&str, &str, &str)] = &[
	("event_reports", "", "event reports: tuwunel keeps none"),
	("room_reports", "", "room reports: tuwunel keeps none"),
	("user_reports", "", "user reports: tuwunel keeps none"),
	(
		"delayed_events",
		"WHERE NOT is_processed",
		"delayed events: tuwunel has none, so they won't be sent",
	),
	(
		"device_federation_outbox",
		"",
		"to-device messages Synapse hadn't delivered to other servers yet",
	),
	("ratelimit_override", "", "per-user rate limit overrides"),
	(
		"users_pending_deactivation",
		"",
		"deactivations Synapse hadn't finished: those users may still be in rooms",
	),
	(
		"thread_subscriptions",
		"WHERE subscribed",
		"thread subscriptions (MSC4306): tuwunel has none",
	),
	(
		"sticky_events",
		"",
		"sticky events (MSC4354): imported as ordinary events, no longer sticky",
	),
	(
		"user_threepid_id_server",
		"",
		"identity server bindings: tuwunel won't unbind them when the address is removed or the \
		 account deactivated",
	),
	(
		"account_validity",
		"WHERE expiration_ts_ms IS NOT NULL",
		"account expiry dates (account_validity): tuwunel doesn't expire accounts",
	),
	(
		"appservice_room_list",
		"",
		"rooms in appservice network directories: tuwunel has none, so they aren't listed",
	),
	(
		"application_services_txns",
		"",
		"appservice transactions Synapse hadn't delivered: those bridges won't get these events",
	),
	(
		"device_lists_outbound_pokes",
		"WHERE NOT sent",
		"device list updates Synapse hadn't sent to other servers",
	),
	(
		"destinations",
		"d WHERE EXISTS (SELECT 1 FROM destination_rooms r WHERE r.destination = d.destination \
		 AND r.stream_ordering > coalesce(d.last_successful_stream_ordering, 0))",
		"servers Synapse hadn't sent the latest events to: they fetch them with the next one",
	),
	("event_expiry", "", "events due to expire (MSC2228): tuwunel won't delete them"),
	(
		"room_retention",
		"r JOIN current_state_events c USING (event_id) WHERE r.max_lifetime IS NOT NULL",
		"rooms with a message retention policy: tuwunel doesn't purge old messages",
	),
	(
		"scheduled_tasks",
		"WHERE status IN ('scheduled', 'active')",
		"admin tasks Synapse hadn't finished, such as room deletions or purges: run them again",
	),
	(
		"per_user_experimental_features",
		"WHERE enabled",
		"per-user experimental features",
	),
];

pub(super) async fn report(source: &Source, mas: Option<&Mas>, report: &mut Report) -> Result {
	if let Some(mas) = mas {
		mas.leftovers(report).await?;
	}

	for &(table, filter, what) in LEFTOVERS {
		if !source.has_table(table).await? {
			continue;
		}

		let count = source.count_where(table, filter).await?;
		if count > 0 {
			report.skip(format!("{count} {what}"));
		}
	}

	Ok(())
}
