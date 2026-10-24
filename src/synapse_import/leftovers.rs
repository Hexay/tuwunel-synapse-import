//! Synapse data tuwunel has no place for, counted in the report so nothing is
//! dropped silently.

use tuwunel_core::Result;

use super::{Report, source::Source};

const LEFTOVERS: [(&str, &str); 5] = [
	("SELECT count(*) FROM event_reports", "event reports: tuwunel keeps none"),
	(
		"SELECT count(*) FROM delayed_events WHERE NOT is_processed",
		"delayed events: tuwunel has none, so they won't be sent",
	),
	(
		"SELECT count(*) FROM device_federation_outbox",
		"to-device messages Synapse hadn't delivered to other servers yet",
	),
	("SELECT count(*) FROM ratelimit_override", "per-user rate limit overrides"),
	(
		"SELECT count(*) FROM users_pending_deactivation",
		"deactivations Synapse hadn't finished: those users may still be in rooms",
	),
];

pub(super) async fn report(source: &Source, report: &mut Report) -> Result {
	for (query, what) in LEFTOVERS {
		let count: i64 = source.rows(query, &[]).await?[0].get(0);
		if count > 0 {
			report.skip(format!("{count} {what}"));
		}
	}

	Ok(())
}
