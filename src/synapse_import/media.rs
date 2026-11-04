//! Media uploaded to this server, under the MXC URIs rooms and profiles already
//! point at. Thumbnails are left behind: tuwunel generates them again on
//! demand. Remote media is in `remote_media`.

use std::{
	path::{Path, PathBuf},
	pin::pin,
};

use futures::TryStreamExt;
use ruma::{Mxc, UserId};
use tokio_postgres::Row;
use tuwunel_core::{Err, Result, err, utils::content_disposition::make_content_disposition};
use tuwunel_service::Services;

use super::{
	Report,
	source::{Source, user_id},
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
				"SELECT media_id, media_type, upload_name, user_id, quarantined_by IS NOT NULL \
				 FROM local_media_repository WHERE url_cache IS NULL ORDER BY created_ts",
				&[],
			)
			.await?
	);

	while let Some(row) = rows.try_next().await? {
		let Some(media_id) = row.get::<_, Option<&str>>(0) else {
			report.skip("media without a media ID".to_owned());
			continue;
		};

		match import_media(services, media_store, dry_run, media_id, &row).await {
			| Ok(()) => Report::count(&mut report.media),
			| Err(e) => report.skip(format!("media {media_id}: {e}")),
		}
	}

	Ok(())
}

async fn import_media(
	services: &Services,
	media_store: &Path,
	dry_run: bool,
	media_id: &str,
	row: &Row,
) -> Result {
	if row.get::<_, bool>(4) {
		return Err!("quarantined in Synapse, which tuwunel has no equivalent for");
	}

	let user_id = row
		.get::<_, Option<&str>>(3)
		.map(|_| user_id(row, 3))
		.transpose()?;

	let mxc = Mxc {
		server_name: services.globals.server_name(),
		media_id,
	};

	let file = File {
		path: local_content_path(media_store, media_id)?,
		content_type: row.get(1),
		filename: row.get(2),
	};

	store(services, &mxc, user_id.as_deref(), &file, dry_run).await
}

/// A file in Synapse's media store and what it was served as.
pub(super) struct File<'a> {
	pub(super) path: PathBuf,
	pub(super) content_type: Option<&'a str>,
	pub(super) filename: Option<&'a str>,
}

/// Stores `file` as `mxc`. In a dry run, only checks the file can be opened:
/// the media store may be remote, and a rehearsal mustn't fill it.
pub(super) async fn store(
	services: &Services,
	mxc: &Mxc<'_>,
	user_id: Option<&UserId>,
	file: &File<'_>,
	dry_run: bool,
) -> Result {
	let cannot_read = |e: std::io::Error| err!("cannot read its file: {e}");

	if dry_run {
		return tokio::fs::File::open(&file.path)
			.await
			.map(drop)
			.map_err(cannot_read);
	}

	// Whole, as media::create takes the file as one slice.
	let bytes = tokio::fs::read(&file.path)
		.await
		.map_err(cannot_read)?;

	let content_disposition = make_content_disposition(None, file.content_type, file.filename);
	services
		.media
		.create(mxc, user_id, Some(&content_disposition), file.content_type, &bytes)
		.await?;

	Ok(())
}

fn local_content_path(media_store: &Path, media_id: &str) -> Result<PathBuf> {
	content_path(media_store.join("local_content"), media_id)
}

/// Synapse's layout: `<dir>/ab/cd/efgh…` for file ID `abcdefgh…`. Only the
/// characters Synapse generates are accepted, so the ID can't leave `dir`.
pub(super) fn content_path(dir: PathBuf, media_id: &str) -> Result<PathBuf> {
	if !media_id
		.chars()
		.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
	{
		return Err!("media ID isn't a plain name");
	}

	let rest = media_id.get(4..).filter(|rest| !rest.is_empty());
	let (Some(first), Some(second), Some(rest)) = (media_id.get(..2), media_id.get(2..4), rest)
	else {
		return Err!("media ID is too short for Synapse's media store layout");
	};

	Ok(dir.join(first).join(second).join(rest))
}

#[cfg(test)]
mod tests {
	use std::path::Path;

	use super::local_content_path;

	#[test]
	fn local_content_path_splits_the_id() {
		let path = local_content_path(Path::new("/media"), "abcdefgh").expect("a plain ID");

		assert_eq!(path, Path::new("/media/local_content/ab/cd/efgh"));
	}

	#[test]
	fn local_content_path_refuses_short_ids() {
		local_content_path(Path::new("/media"), "abcd").unwrap_err();
		local_content_path(Path::new("/media"), "").unwrap_err();
	}

	#[test]
	fn local_content_path_refuses_traversal() {
		for media_id in ["../../etc/passwd", "ab/cdefgh", "ab\\cdefgh", "abcd..", "ab:cdefgh"] {
			assert!(local_content_path(Path::new("/media"), media_id).is_err(), "{media_id}");
		}
	}

	#[test]
	fn local_content_path_refuses_non_ascii_ids() {
		local_content_path(Path::new("/media"), "abcdéfgh").unwrap_err();
		local_content_path(Path::new("/media"), "ééééé").unwrap_err();
	}
}
