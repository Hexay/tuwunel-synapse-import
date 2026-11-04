//! Synapse's cache of other servers' media, so files whose server is gone
//! still load. Missing cache files are counted, not named: tuwunel fetches
//! those again on demand.

use std::{path::Path, pin::pin};

use futures::TryStreamExt;
use ruma::{Mxc, ServerName};
use tokio_postgres::Row;
use tuwunel_core::{Err, Result, err};
use tuwunel_service::Services;

use super::{
	Report,
	media::{File, content_path, store},
	source::Source,
};

pub(super) async fn import(
	services: &Services,
	source: &Source,
	media_store: &Path,
	dry_run: bool,
	report: &mut Report,
) -> Result {
	let mut rows = pin!(
		source
			.stream(
				"SELECT media_origin, media_id, media_type, upload_name, filesystem_id, \
				 quarantined_by IS NOT NULL FROM remote_media_cache ORDER BY created_ts",
				&[],
			)
			.await?
	);

	let mut missing: usize = 0;
	while let Some(row) = rows.try_next().await? {
		let (origin, media_id): (&str, &str) = (row.get(0), row.get(1));
		match import_media(services, media_store, dry_run, &row).await {
			| Ok(true) => Report::count(&mut report.remote_media),
			| Ok(false) => Report::count(&mut missing),
			| Err(e) => report.skip(format!("remote media mxc://{origin}/{media_id}: {e}")),
		}
	}

	if missing > 0 {
		report.skip(format!(
			"{missing} cached remote media files were missing from the media store: tuwunel \
			 fetches them again from their servers"
		));
	}

	Ok(())
}

/// False when Synapse's cache no longer holds the file.
async fn import_media(
	services: &Services,
	media_store: &Path,
	dry_run: bool,
	row: &Row,
) -> Result<bool> {
	if row.get::<_, bool>(5) {
		return Err!("quarantined in Synapse, which tuwunel has no equivalent for");
	}

	let (origin, media_id, filesystem_id): (&str, &str, &str) =
		(row.get(0), row.get(1), row.get(4));

	let server_name =
		origin_name(origin).filter(|&name| name != services.globals.server_name());

	let Some(server_name) = server_name else {
		return Err!("{origin:?} isn't another server's name");
	};

	let path = content_path(media_store.join("remote_content").join(origin), filesystem_id)?;
	let exists = tokio::fs::try_exists(&path)
		.await
		.map_err(|e| err!("cannot read its file: {e}"))?;

	if !exists {
		return Ok(false);
	}

	let mxc = Mxc { server_name, media_id };
	let file = File {
		path,
		content_type: row.get(2),
		filename: row.get(3),
	};

	store(services, &mxc, None, &file, dry_run)
		.await
		.map(|()| true)
}

/// The origin as a server name that is also a safe directory name: a leading
/// dot would let `..` pass and leave the media store.
fn origin_name(origin: &str) -> Option<&ServerName> {
	<&ServerName>::try_from(origin)
		.ok()
		.filter(|_| !origin.starts_with('.'))
}

#[cfg(test)]
mod tests {
	use super::origin_name;

	#[test]
	fn origin_name_accepts_server_names() {
		for origin in ["remote.test", "matrix.org:8448", "[::1]:8448", "127.0.0.1"] {
			assert!(origin_name(origin).is_some(), "{origin}");
		}
	}

	#[test]
	fn origin_name_refuses_what_could_leave_the_store() {
		for origin in ["..", ".", "../etc", "a/b", "a\\b", ""] {
			assert!(origin_name(origin).is_none(), "{origin}");
		}
	}
}
