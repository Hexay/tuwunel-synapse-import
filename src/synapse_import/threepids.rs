//! Email addresses bound to accounts. tuwunel binds email only, so phone
//! numbers are reported.

use ruma::{MilliSecondsSinceUnixEpoch, UInt, thirdparty::Medium};
use tuwunel_core::Result;
use tuwunel_service::{Services, threepid::canonicalize_email};

use super::{
	Report,
	source::{Source, user_id},
};

pub(super) async fn import(services: &Services, source: &Source, report: &mut Report) -> Result {
	let rows = source
		.rows(
			"SELECT user_id, medium, address, validated_at, added_at FROM user_threepids ORDER \
			 BY user_id",
			&[],
		)
		.await?;

	for row in rows {
		let user_id = user_id(&row, 0)?;
		let (medium, address): (&str, &str) = (row.get(1), row.get(2));
		if !services.users.is_active(&user_id).await {
			continue;
		}

		if medium != "email" {
			report.skip(format!("{user_id}: tuwunel binds email only, not {medium} {address}"));
			continue;
		}

		let Ok(email) = canonicalize_email(address) else {
			report
				.skip(format!("{user_id}: tuwunel doesn't accept the email address {address:?}"));
			continue;
		};

		if let Ok(Some(owner)) = services.threepid.user_id_for_email(&email).await
			&& owner != user_id
		{
			report.skip(format!("{user_id}: {address} is already bound to {owner}"));
			continue;
		}

		services
			.threepid
			.put_binding(
				&user_id,
				&email,
				Medium::Email,
				timestamp(row.get(3)),
				timestamp(row.get(4)),
			)
			.await;

		Report::count(&mut report.threepids);
	}

	Ok(())
}

fn timestamp(millis: i64) -> MilliSecondsSinceUnixEpoch {
	MilliSecondsSinceUnixEpoch(UInt::try_from(millis).unwrap_or_default())
}
