//! Media uploaded to this server, under the MXC URIs rooms and profiles already
//! point at, with their upload times. Each file is checked against the size
//! and hash Synapse recorded for it. Thumbnails are left behind: tuwunel
//! generates them again on demand. Remote media is in `remote_media`.

use std::{
	collections::HashSet,
	fmt::Write,
	path::{Path, PathBuf},
	pin::pin,
	time::{Duration, SystemTime},
};

use futures::TryStreamExt;
use ruma::{Mxc, UserId};
use sha2::{Digest, Sha256};
use crate::source::Row;
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
	// Synapse records the hash from schema 91 (Synapse 1.120).
	let sha256 = source
		.column_or("local_media_repository", "sha256", "NULL")
		.await?;

	let mut rows = pin!(
		source
			.stream(
				&format!(
					"SELECT media_id, media_type, upload_name, user_id, quarantined_by IS NOT \
					 NULL, created_ts, CAST(media_length AS BIGINT), {sha256} FROM local_media_repository WHERE \
					 url_cache IS NULL ORDER BY created_ts"
				),
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

	report_unknown_files(source, media_store, report).await
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
		created_ts: row.get(5),
		length: row.get(6),
		sha256: row.get(7),
	};

	store(services, &mxc, user_id.as_deref(), &file, dry_run).await
}

/// A file in Synapse's media store, what it was served as, and what Synapse
/// recorded about it.
pub(super) struct File<'a> {
	pub(super) path: PathBuf,
	pub(super) content_type: Option<&'a str>,
	pub(super) filename: Option<&'a str>,
	pub(super) created_ts: Option<i64>,
	pub(super) length: Option<i64>,
	/// Hex, as Synapse stores it.
	pub(super) sha256: Option<&'a str>,
}

/// Stores `file` as `mxc`, with Synapse's upload time. A file that differs
/// from what Synapse recorded isn't stored. In a dry run, the file is only
/// read and checked: a rehearsal mustn't fill the media store.
pub(super) async fn store(
	services: &Services,
	mxc: &Mxc<'_>,
	user_id: Option<&UserId>,
	file: &File<'_>,
	dry_run: bool,
) -> Result {
	// Whole, as media::create takes the file as one slice.
	let bytes = tokio::fs::read(&file.path)
		.await
		.map_err(|e| err!("cannot read its file: {e}"))?;

	if let Some(problem) = file_problem(&bytes, file.length, file.sha256) {
		return Err!("its file {}: {problem}", file.path.display());
	}

	if dry_run {
		return Ok(());
	}

	let content_disposition = make_content_disposition(None, file.content_type, file.filename);
	services
		.media
		.create(mxc, user_id, Some(&content_disposition), file.content_type, &bytes)
		.await?;

	if let Some(created) = file.created_ts.and_then(upload_time) {
		services
			.media
			.set_file_mtime(mxc, created)
			.await?;
	}

	Ok(())
}

/// How the file differs from the size and SHA-256 Synapse recorded, if it
/// does.
pub(super) fn file_problem(bytes: &[u8], length: Option<i64>, sha256: Option<&str>) -> Option<String> {
	let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
	if let Some(length) = length.filter(|&length| u64::try_from(length).ok() != Some(size)) {
		return Some(format!("has {size} bytes, but Synapse recorded {length}"));
	}

	let sha256 = sha256.filter(|sha256| !sha256.is_empty())?;
	let actual = Sha256::digest(bytes)
		.iter()
		.fold(String::with_capacity(64), |mut hex, byte| {
			write!(hex, "{byte:02x}").expect("writing to a String");
			hex
		});

	(!actual.eq_ignore_ascii_case(sha256))
		.then(|| format!("its SHA-256 is {actual}, but Synapse recorded {sha256}"))
}

fn upload_time(created_ts: i64) -> Option<SystemTime> {
	let millis = u64::try_from(created_ts).ok()?;

	SystemTime::UNIX_EPOCH.checked_add(Duration::from_millis(millis))
}

/// Files in `local_content` that Synapse's database doesn't list, usually
/// uploads made after the database was copied. With no uploader, type or name
/// they can't be imported, so they are counted with a few named.
async fn report_unknown_files(
	source: &Source,
	media_store: &Path,
	report: &mut Report,
) -> Result {
	const NAMED: usize = 5;

	let known: HashSet<String> = source
		.rows("SELECT media_id FROM local_media_repository", &[])
		.await?
		.iter()
		.filter_map(|row| row.get::<_, Option<String>>(0))
		.collect();

	let base = media_store.join("local_content");
	let files = tokio::task::spawn_blocking(move || files_under(&base))
		.await
		.map_err(|e| err!("cannot list the media store: {e}"))??;

	let unknown: Vec<_> = files
		.into_iter()
		.filter(|relative| !known.contains(&relative.concat()))
		.map(|relative| relative.join("/"))
		.collect();

	if !unknown.is_empty() {
		let named = unknown
			.iter()
			.take(NAMED)
			.cloned()
			.collect::<Vec<_>>();
		let more = if unknown.len() > NAMED { ", ..." } else { "" };
		report.skip(format!(
			"{} files in the media store's local_content aren't in Synapse's database, so \
			 they weren't imported (uploads after the database was copied?): {}{more}",
			unknown.len(),
			named.join(", "),
		));
	}

	Ok(())
}

/// Every file under `dir`, as the path components below it, sorted. A missing
/// `dir` has none.
fn files_under(dir: &Path) -> Result<Vec<Vec<String>>> {
	let mut files = Vec::new();
	let mut pending = vec![(dir.to_path_buf(), Vec::new())];
	while let Some((path, relative)) = pending.pop() {
		let entries = match std::fs::read_dir(&path) {
			| Ok(entries) => entries,
			| Err(e) if e.kind() == std::io::ErrorKind::NotFound && relative.is_empty() => {
				return Ok(files);
			},
			| Err(e) => return Err!("cannot list {}: {e}", path.display()),
		};

		for entry in entries {
			let entry = entry.map_err(|e| err!("cannot list {}: {e}", path.display()))?;
			let mut relative = relative.clone();
			relative.push(entry.file_name().to_string_lossy().into_owned());
			if entry.file_type()?.is_dir() {
				pending.push((entry.path(), relative));
			} else {
				files.push(relative);
			}
		}
	}

	files.sort();
	Ok(files)
}

pub(super) fn local_content_path(media_store: &Path, media_id: &str) -> Result<PathBuf> {
	content_path(&media_store.join("local_content"), media_id)
}

/// Synapse's layout: `<dir>/ab/cd/efgh…` for file ID `abcdefgh…`. Only the
/// characters Synapse generates are accepted, so the ID can't leave `dir`.
pub(super) fn content_path(dir: &Path, media_id: &str) -> Result<PathBuf> {
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

	use super::{file_problem, local_content_path};

	// SHA-256 of "abc".
	const ABC: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

	#[test]
	fn file_problem_accepts_what_synapse_recorded() {
		assert_eq!(file_problem(b"abc", Some(3), Some(ABC)), None);
		assert_eq!(file_problem(b"abc", Some(3), Some(&ABC.to_uppercase())), None);
		assert_eq!(file_problem(b"abc", None, None), None);
		assert_eq!(file_problem(b"abc", Some(3), Some("")), None);
	}

	#[test]
	fn file_problem_refuses_another_size() {
		assert!(file_problem(b"abcd", Some(3), None).is_some());
		assert!(file_problem(b"", Some(-1), None).is_some());
	}

	#[test]
	fn file_problem_refuses_another_hash() {
		assert!(file_problem(b"abd", Some(3), Some(ABC)).is_some());
	}

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
