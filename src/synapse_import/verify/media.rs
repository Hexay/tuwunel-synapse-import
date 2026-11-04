//! Local media: each upload's metadata, and for a sample, the bytes.

use std::pin::pin;

use futures::TryStreamExt;
use ruma::Mxc;
use tuwunel_core::Result;

use super::{super::media::local_content_path, Ctx};

const DOMAIN: &str = "media";

pub(super) async fn verify(ctx: &mut Ctx<'_>) -> Result {
	let services = ctx.services;
	let mut rows = pin!(
		ctx.source
			.stream(
				"SELECT media_id, media_type, quarantined_by IS NOT NULL FROM \
				 local_media_repository WHERE url_cache IS NULL ORDER BY md5(media_id || $1)",
				&[&ctx.options.seed],
			)
			.await?
	);

	let mut compared: usize = 0;
	while let Some(row) = rows.try_next().await? {
		let Some(media_id) = row.get::<_, Option<&str>>(0) else {
			ctx.out.excuse(DOMAIN, "<none>", "no media ID");
			continue;
		};

		if row.get::<_, bool>(2) {
			ctx.out.excuse(DOMAIN, media_id, "quarantined");
			continue;
		}

		let path = match local_content_path(&ctx.config.media_store_path, media_id) {
			| Ok(path) => path,
			| Err(e) => {
				ctx.out.excuse(DOMAIN, media_id, e);
				continue;
			},
		};

		let mxc = Mxc {
			server_name: services.globals.server_name(),
			media_id,
		};
		let Some(metadata) = services.media.get_metadata(&mxc).await else {
			if tokio::fs::metadata(&path).await.is_ok() {
				ctx.out.missing(DOMAIN, media_id, "no such media");
			} else {
				ctx.out
					.excuse(DOMAIN, media_id, "Synapse's file is gone");
			}
			continue;
		};

		let content_type: Option<&str> = row.get(1);
		if metadata.content_type.as_deref() != content_type {
			ctx.out.differs(
				DOMAIN,
				media_id,
				format!(
					"content type: Synapse {content_type:?}, tuwunel {:?}",
					metadata.content_type
				),
			);
			continue;
		}

		let sample = ctx.options.sample;
		if sample == 0 || compared < sample {
			compared = compared.saturating_add(1);
			let synapse = tokio::fs::read(&path).await;
			let tuwunel = services.media.get_stored(&mxc).await;
			match (synapse, tuwunel) {
				| (Ok(synapse), Ok(tuwunel)) if synapse == tuwunel.content => {},
				| (Err(e), _) => {
					ctx.out
						.excuse(DOMAIN, media_id, format!("can't read Synapse's file: {e}"));
					continue;
				},
				| _ => {
					ctx.out
						.differs(DOMAIN, media_id, "different bytes");
					continue;
				},
			}
		}

		ctx.out.matched(DOMAIN);
	}

	Ok(())
}
