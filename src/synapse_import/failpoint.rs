//! Debug builds only: lets fixtures/synapse/import-resume.sh stop an import at
//! an exact row, to kill it there. `TUWUNEL_SYNAPSE_IMPORT_FAILPOINT=<name>:<n>`
//! parks the import forever once the point `name` reports its `n`th row;
//! release builds compile every point to nothing.

/// Never returns when `name:count` is the configured failpoint.
#[cfg(debug_assertions)]
pub(super) async fn reached(name: &str, count: usize) {
	use std::sync::OnceLock;

	static TARGET: OnceLock<Option<String>> = OnceLock::new();

	let target = TARGET.get_or_init(|| std::env::var("TUWUNEL_SYNAPSE_IMPORT_FAILPOINT").ok());
	if let Some(target) = target
		&& *target == format!("{name}:{count}")
	{
		tuwunel_core::warn!("Synapse import failpoint {name}:{count} reached, waiting to be killed");
		std::future::pending::<()>().await;
	}
}

#[cfg(not(debug_assertions))]
#[inline]
pub(super) async fn reached(_name: &str, _count: usize) {}
