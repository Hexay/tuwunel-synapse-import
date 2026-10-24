//! Media uploaded to this server, under the MXC URIs rooms and profiles already
//! point at. Remote media and thumbnails are left behind: tuwunel fetches and
//! generates them again on demand.

use std::{
	path::{Path, PathBuf},
	pin::pin,
};

use futures::TryStreamExt;
use ruma::Mxc;
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
		let media_id: &str = row.get(0);
		match import_media(services, media_store, &row).await {
			| Ok(()) => Report::count(&mut report.media),
			| Err(e) => report.skip(format!("media {media_id}: {e}")),
		}
	}

	Ok(())
}

async fn import_media(services: &Services, media_store: &Path, row: &Row) -> Result {
	let media_id: &str = row.get(0);
	if row.get::<_, bool>(4) {
		return Err!("quarantined in Synapse, which tuwunel has no equivalent for");
	}

	let content_type: Option<&str> = row.get(1);
	let filename: Option<&str> = row.get(2);
	let user_id = user_id(row, 3)?;

	let file = tokio::fs::read(local_content_path(media_store, media_id)?)
		.await
		.map_err(|e| err!("cannot read its file: {e}"))?;

	let mxc = Mxc {
		server_name: services.globals.server_name(),
		media_id,
	};

	let content_disposition = make_content_disposition(None, content_type, filename);
	services
		.media
		.create(&mxc, Some(&user_id), Some(&content_disposition), content_type, &file)
		.await?;

	Ok(())
}

/// Synapse's layout: `local_content/ab/cd/efgh…` for media ID `abcdefgh…`.
fn local_content_path(media_store: &Path, media_id: &str) -> Result<PathBuf> {
	let (Some(first), Some(second), Some(rest)) =
		(media_id.get(..2), media_id.get(2..4), media_id.get(4..))
	else {
		return Err!("media ID is too short for Synapse's media store layout");
	};

	if media_id.contains(['/', '\\', '.']) {
		return Err!("media ID isn't a plain name");
	}

	Ok(media_store
		.join("local_content")
		.join(first)
		.join(second)
		.join(rest))
}
