//! The tokens of MAS's live sessions, keyed like Synapse's so `sessions`
//! imports them onto Synapse's devices the same way.

use std::collections::HashMap;

use ruma::{OwnedDeviceId, OwnedUserId};
use tuwunel_core::{Result, utils::random_string};
use tuwunel_service::Services;

use super::{
	super::{
		sessions::{MIN_TOKEN_LEN, Token},
		source::user_id,
	},
	Mas,
};

const MS: &str = "(extract(epoch FROM a.expires_at) * 1000)::bigint";

/// Legacy `/login` sessions (`mct_`/`mcr_`, or `syt_`/`syr_` kept by syn2mas).
const COMPAT: &str = "FROM compat_sessions s JOIN users u USING (user_id) LEFT JOIN LATERAL \
                      (SELECT access_token, expires_at FROM compat_access_tokens WHERE \
                      compat_session_id = s.compat_session_id ORDER BY created_at DESC LIMIT 1) \
                      a ON true LEFT JOIN LATERAL (SELECT refresh_token FROM \
                      compat_refresh_tokens WHERE compat_session_id = s.compat_session_id AND \
                      consumed_at IS NULL ORDER BY created_at DESC LIMIT 1) r ON true WHERE \
                      s.finished_at IS NULL AND s.device_id IS NOT NULL";

/// OAuth 2.0 sessions (`mat_`/`mar_`); the device is a scope.
const OAUTH: &str =
	"FROM oauth2_sessions s JOIN users u ON u.user_id = s.user_id JOIN LATERAL (SELECT \
	 substring(scope FROM '^urn:matrix:(?:org\\.matrix\\.msc2967\\.)?client:device:(.+)$') AS \
	 device_id FROM unnest(s.scope_list) scope WHERE scope ~ \
	 '^urn:matrix:(?:org\\.matrix\\.msc2967\\.)?client:device:' LIMIT 1) d ON true LEFT JOIN \
	 LATERAL (SELECT access_token, expires_at FROM oauth2_access_tokens WHERE oauth2_session_id \
	 = s.oauth2_session_id AND revoked_at IS NULL ORDER BY created_at DESC LIMIT 1) a ON true \
	 LEFT JOIN LATERAL (SELECT refresh_token FROM oauth2_refresh_tokens WHERE oauth2_session_id \
	 = s.oauth2_session_id AND consumed_at IS NULL AND revoked_at IS NULL ORDER BY created_at \
	 DESC LIMIT 1) r ON true WHERE s.finished_at IS NULL";

pub(in super::super) async fn tokens(
	mas: &Mas,
	services: &Services,
) -> Result<HashMap<(OwnedUserId, OwnedDeviceId), Token>> {
	let server_name = services.globals.server_name().as_str();
	let mut tokens = HashMap::new();

	for (from, oidc, device) in [(COMPAT, false, "s.device_id"), (OAUTH, true, "d.device_id")] {
		let query = format!(
			"SELECT '@' || u.username || ':' || $1, {device}, a.access_token, {MS}, \
			 r.refresh_token {from}"
		);

		for row in mas.source.rows(&query, &[&server_name]).await? {
			let user_id = user_id(&row, 0)?;
			let device_id: OwnedDeviceId = row.get::<_, String>(1).into();
			let refresh: Option<String> = row.get(4);

			// MAS deletes spent access tokens; tuwunel keeps a refresh token only
			// beside one, so an already expired stand-in carries it.
			let (token, valid_until_ms) = match row.get::<_, Option<String>>(2) {
				| Some(token) => (token, row.get(3)),
				| None if refresh.is_some() => (random_string(MIN_TOKEN_LEN), Some(0)),
				| None => continue,
			};

			tokens.insert((user_id, device_id), Token { token, valid_until_ms, refresh, oidc });
		}
	}

	Ok(tokens)
}
