//! Account data (global and per room), room tags, push rules and filters of
//! active users.

use std::{collections::BTreeMap, pin::pin};

use futures::TryStreamExt;
use ruma::{
	OwnedRoomId, RoomId, UserId,
	events::{GlobalAccountDataEventType, push_rules::PushRulesEvent},
	push::{Action, Ruleset},
};
use serde_json::{Map, Value};
use tuwunel_core::Result;

use super::{
	super::{
		push_rules::split_rule_id,
		source::{json, user_id},
	},
	Ctx,
};

pub(super) async fn verify(ctx: &mut Ctx<'_>) -> Result {
	account_data(ctx).await?;
	tags(ctx).await?;
	push_rules(ctx).await?;
	filters(ctx).await
}

async fn account_data(ctx: &mut Ctx<'_>) -> Result {
	let mut rows = pin!(
		ctx.source
			.stream(
				"SELECT user_id, NULL, account_data_type, content FROM account_data WHERE \
				 account_data_type <> 'm.push_rules' AND content <> '{}' UNION ALL SELECT \
				 user_id, room_id, account_data_type, content FROM room_account_data WHERE \
				 content <> '{}'",
				&[],
			)
			.await?
	);

	while let Some(row) = rows.try_next().await? {
		let Ok(user_id) = user_id(&row, 0) else {
			continue;
		};

		if !ctx.active.contains(&user_id) {
			continue;
		}

		let (room, kind): (Option<&str>, &str) = (row.get(1), row.get(2));
		let subject = format!("{user_id} {} {kind}", room.unwrap_or("global"));
		let room_id = room.map(OwnedRoomId::try_from).transpose();
		let (Ok(want), Ok(room_id)) = (json(row.get(3)), room_id) else {
			ctx.out
				.excuse("account_data", subject, "Synapse stored invalid JSON or room ID");
			continue;
		};

		match content(ctx, room_id.as_deref(), &user_id, kind).await {
			| Some(have) if have == want => ctx.out.matched("account_data"),
			| Some(_) => ctx
				.out
				.differs("account_data", subject, "different content"),
			| None => ctx
				.out
				.missing("account_data", subject, "not set"),
		}
	}

	Ok(())
}

/// The content of an account data event tuwunel holds.
async fn content(
	ctx: &Ctx<'_>,
	room_id: Option<&RoomId>,
	user_id: &UserId,
	kind: &str,
) -> Option<Value> {
	let raw = ctx
		.services
		.account_data
		.get_raw(room_id, user_id, kind)
		.await
		.ok()?;
	let mut event: Value = serde_json::from_slice(&raw).ok()?;

	event.get_mut("content").map(Value::take)
}

/// Synapse keeps tags in their own table; tuwunel as `m.tag` room account data.
async fn tags(ctx: &mut Ctx<'_>) -> Result {
	let rows = ctx
		.source
		.rows("SELECT user_id, room_id, tag, content FROM room_tags", &[])
		.await?;

	let mut want: BTreeMap<(String, String), Map<String, Value>> = BTreeMap::new();
	for row in rows {
		let (user, room, tag): (String, String, String) = (row.get(0), row.get(1), row.get(2));
		match json(row.get(3)) {
			| Ok(content) =>
				_ = want
					.entry((user, room))
					.or_default()
					.insert(tag, content),
			| Err(_) => ctx.out.excuse(
				"tags",
				format!("{user} {room} {tag}"),
				"Synapse stored invalid JSON",
			),
		}
	}

	for ((user, room), want) in want {
		let (Ok(user_id), Ok(room_id)) =
			(UserId::parse(&user), OwnedRoomId::try_from(room.as_str()))
		else {
			continue;
		};

		if !ctx.active.contains(&user_id) {
			continue;
		}

		let have = content(ctx, Some(&room_id), &user_id, "m.tag").await;
		let have = have
			.as_ref()
			.and_then(|have| have.get("tags")?.as_object());
		for (tag, content) in &want {
			match have.and_then(|have| have.get(tag)) {
				| Some(have) if have == content => ctx.out.matched("tags"),
				| Some(_) =>
					ctx.out
						.differs("tags", format!("{user} {room} {tag}"), "different content"),
				| None => ctx
					.out
					.missing("tags", format!("{user} {room} {tag}"), "no such tag"),
			}
		}
	}

	Ok(())
}

/// Synapse's own rules (their actions) and its flags on any rule, against
/// the ruleset tuwunel holds. A row Synapse can't parse isn't imported.
async fn push_rules(ctx: &mut Ctx<'_>) -> Result {
	let rows = ctx
		.source
		.rows(
			"SELECT user_name, rule_id, actions, conditions, NULL::boolean FROM push_rules \
			 UNION ALL SELECT user_name, rule_id, NULL, NULL, coalesce(enabled, 0) <> 0 FROM \
			 push_rules_enable",
			&[],
		)
		.await?;

	let mut rulesets: BTreeMap<String, Option<Ruleset>> = BTreeMap::new();
	for row in rows {
		let Ok(user_id) = user_id(&row, 0) else {
			continue;
		};

		if !ctx.active.contains(&user_id) {
			continue;
		}

		let namespaced: &str = row.get(1);
		let subject = format!("{user_id} {namespaced}");
		let Ok((kind, rule_id)) = split_rule_id(namespaced) else {
			ctx.out
				.excuse("push_rules", subject, "not a global rule ID");
			continue;
		};

		if !rulesets.contains_key(user_id.as_str()) {
			let ruleset = ctx
				.services
				.account_data
				.get_global::<PushRulesEvent>(&user_id, GlobalAccountDataEventType::PushRules)
				.await
				.ok()
				.map(|event| event.content.global);
			rulesets.insert(user_id.to_string(), ruleset);
		}

		let enabled: Option<bool> = row.get(4);
		let actions = match enabled {
			| Some(_) => None,
			| None => match rule_actions(row.get(2), row.get(3)) {
				| Ok(actions) => Some(actions),
				| Err(why) => {
					ctx.out.excuse("push_rules", subject, why);
					continue;
				},
			},
		};

		let ruleset = rulesets[user_id.as_str()].as_ref();
		let Some(rule) = ruleset.and_then(|ruleset| ruleset.get(kind.clone(), rule_id)) else {
			match (rule_id.starts_with('.'), enabled) {
				| (true, _) =>
					ctx.out
						.excuse("push_rules", subject, "a default rule tuwunel doesn't have"),
				| (false, Some(_)) =>
					ctx.out
						.excuse("push_rules", subject, "the flag of a deleted rule"),
				| (false, None) => ctx
					.out
					.missing("push_rules", subject, "no such rule"),
			}
			continue;
		};

		let have = serde_json::to_value(rule.actions()).ok();
		match (enabled, actions) {
			| (Some(enabled), _) if enabled != rule.enabled() => ctx.out.differs(
				"push_rules",
				subject,
				format!("enabled: Synapse {enabled}, tuwunel {}", rule.enabled()),
			),
			| (_, Some(actions)) if have.as_ref() != Some(&actions) =>
				ctx.out
					.differs("push_rules", subject, "different actions"),
			| _ => ctx.out.matched("push_rules"),
		}
	}

	Ok(())
}

/// A rule row's actions, as tuwunel serializes them, or why the import
/// couldn't take it.
pub(super) fn rule_actions(
	actions: Option<&str>,
	conditions: Option<&str>,
) -> Result<Value, &'static str> {
	let invalid = "Synapse stored invalid JSON";
	if conditions.is_some_and(|conditions| json(conditions).is_err()) {
		return Err(invalid);
	}

	let actions = json(actions.unwrap_or("[]")).map_err(|_| invalid)?;
	serde_json::from_value::<Vec<Action>>(actions)
		.and_then(serde_json::to_value)
		.map_err(|_| "actions tuwunel can't read")
}

async fn filters(ctx: &mut Ctx<'_>) -> Result {
	let server_name = ctx.services.globals.server_name();
	let mut rows = pin!(
		ctx.source
			.stream("SELECT user_id, full_user_id, filter_id, filter_json FROM user_filters", &[])
			.await?
	);

	while let Some(row) = rows.try_next().await? {
		let user_id = match row.get::<_, Option<&str>>(1) {
			| Some(full) => UserId::parse(full),
			| None => UserId::parse_with_server_name(row.get::<_, &str>(0), server_name),
		};

		let Ok(user_id) = user_id else {
			continue;
		};

		if !ctx.active.contains(&user_id) {
			continue;
		}

		let filter_id = row.get::<_, i64>(2).to_string();
		let subject = format!("{user_id} {filter_id}");
		if serde_json::from_slice::<ruma::api::client::filter::FilterDefinition>(row.get(3))
			.is_err()
		{
			ctx.out
				.excuse("filters", subject, "a filter tuwunel can't read");
		} else if ctx
			.services
			.users
			.get_filter(&user_id, &filter_id)
			.await
			.is_ok()
		{
			ctx.out.matched("filters");
		} else {
			ctx.out
				.missing("filters", subject, "no such filter");
		}
	}

	Ok(())
}
