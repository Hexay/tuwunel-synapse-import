//! Push rules. Synapse stores only each user's changes to the server-default
//! rules: their own rules, and default rules they disabled or gave new
//! actions. These are replayed onto tuwunel's defaults.

use std::collections::BTreeMap;

use ruma::{
	OwnedUserId, UserId,
	events::{
		GlobalAccountDataEventType,
		push_rules::{PushRulesEvent, PushRulesEventContent},
	},
	push::{NewPushRule, RuleKind, Ruleset},
};
use serde_json::{Value, from_value, json};
use tuwunel_core::{Err, Result, err};

use super::{
	Report,
	source::{Source, json, user_id},
};
use crate::Services;

pub(super) async fn import(services: &Services, source: &Source, report: &mut Report) -> Result {
	let mut rulesets: BTreeMap<OwnedUserId, Ruleset> = BTreeMap::new();

	// Lowest priority first: each insert lands above the user's earlier rules.
	let rows = source
		.rows(
			"SELECT user_name, rule_id, conditions, actions FROM push_rules ORDER BY user_name, \
			 priority_class, priority",
			&[],
		)
		.await?;

	for row in rows {
		let user_id = user_id(&row, 0)?;
		let namespaced: &str = row.get(1);
		let ruleset = ruleset(&mut rulesets, &user_id);

		match apply_rule(ruleset, namespaced, &json(row.get(2))?, json(row.get(3))?) {
			| Ok(()) => Report::count(&mut report.push_rules),
			| Err(e) => report.skip(format!("{user_id} push rule {namespaced}: {e}")),
		}
	}

	let rows = source
		.rows(
			"SELECT user_name, rule_id, enabled <> 0 FROM push_rules_enable ORDER BY user_name, \
			 rule_id",
			&[],
		)
		.await?;

	for row in rows {
		let user_id = user_id(&row, 0)?;
		let namespaced: &str = row.get(1);
		let (kind, rule_id) = split_rule_id(namespaced)?;

		// A user rule that's gone left its flag behind; a missing default rule is
		// one tuwunel doesn't have.
		if ruleset(&mut rulesets, &user_id)
			.set_enabled(kind, rule_id, row.get(2))
			.is_err()
			&& rule_id.starts_with('.')
		{
			report.skip(format!("{user_id} push rule {namespaced}: not a rule tuwunel has"));
		}
	}

	for (user_id, ruleset) in rulesets {
		if !services.users.is_active(&user_id).await {
			continue;
		}

		let event = PushRulesEvent {
			content: PushRulesEventContent { global: ruleset },
		};

		services
			.account_data
			.update(
				None,
				&user_id,
				GlobalAccountDataEventType::PushRules
					.to_string()
					.into(),
				&serde_json::to_value(event)?,
			)
			.await?;
	}

	Ok(())
}

fn ruleset<'a>(
	rulesets: &'a mut BTreeMap<OwnedUserId, Ruleset>,
	user_id: &UserId,
) -> &'a mut Ruleset {
	rulesets
		.entry(user_id.to_owned())
		.or_insert_with(|| Ruleset::server_default(user_id))
}

fn apply_rule(
	ruleset: &mut Ruleset,
	namespaced: &str,
	conditions: &Value,
	actions: Value,
) -> Result {
	let (kind, rule_id) = split_rule_id(namespaced)?;

	// Synapse records new actions for a default rule as a rule of its own.
	if rule_id.starts_with('.') {
		return ruleset
			.set_actions(kind, rule_id, from_value(actions)?)
			.map_err(|e| err!("{e}"));
	}

	let rule = match kind {
		| RuleKind::Override => NewPushRule::Override(from_value(json!({
			"rule_id": rule_id, "conditions": conditions, "actions": actions,
		}))?),
		| RuleKind::Underride => NewPushRule::Underride(from_value(json!({
			"rule_id": rule_id, "conditions": conditions, "actions": actions,
		}))?),
		| RuleKind::Content => {
			// Synapse stores a content rule's pattern as its one condition.
			let pattern = conditions
				.pointer("/0/pattern")
				.ok_or_else(|| err!("content rule without a pattern"))?;

			NewPushRule::Content(from_value(json!({
				"rule_id": rule_id, "pattern": pattern, "actions": actions,
			}))?)
		},
		| RuleKind::Room => NewPushRule::Room(from_value(json!({
			"rule_id": rule_id, "actions": actions,
		}))?),
		| RuleKind::Sender => NewPushRule::Sender(from_value(json!({
			"rule_id": rule_id, "actions": actions,
		}))?),
		| kind => return Err!("rules of kind {kind} aren't supported"),
	};

	ruleset
		.insert(rule, None, None)
		.map_err(|e| err!("{e}"))
}

/// `global/content/my_rule` into its kind and the rule ID clients see.
fn split_rule_id(namespaced: &str) -> Result<(RuleKind, &str)> {
	match namespaced.splitn(3, '/').collect::<Vec<_>>()[..] {
		| ["global", kind, rule_id] => Ok((kind.into(), rule_id)),
		| _ => Err!("Synapse push rule ID {namespaced:?} isn't global/<kind>/<id>"),
	}
}
