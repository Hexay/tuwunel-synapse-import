use ruma::push::{Action, SoundTweakValue};
use serde_json::{json, to_value};

use super::{actions, counts::thread_root};

#[test]
fn actions_expand_synapse_compressed_defaults() {
	let notify = actions("", false).expect("the default notify actions");
	let highlight = actions("", true).expect("the default highlight actions");

	assert_eq!(
		to_value(&notify).expect("serializes"),
		json!(["notify", {"set_tweak": "highlight", "value": false}])
	);
	assert_eq!(highlight.len(), 3);
	assert!(matches!(highlight[1].sound(), Some(SoundTweakValue::Default)));
	assert!(!notify.iter().any(Action::is_highlight));
	assert!(highlight.iter().any(Action::is_highlight));
}

#[test]
fn actions_keep_stored_json_and_refuse_garbage() {
	let stored = actions(r#"["notify",{"set_tweak":"sound","value":"ping"}]"#, false)
		.expect("stored actions");

	assert_eq!(stored.len(), 2);
	assert!(actions("nope", false).is_err());
}

#[test]
fn thread_root_names_main_as_none() {
	assert_eq!(thread_root("main").expect("main"), None);

	let root = thread_root("$root:example.org").expect("a root");
	assert_eq!(root.as_ref().map(ToString::to_string).as_deref(), Some("$root:example.org"));
	assert!(thread_root("not an event").is_err());
}
