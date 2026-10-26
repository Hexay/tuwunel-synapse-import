# Migrating from Synapse

`tuwunel --import-synapse` moves a stopped Synapse server into a new tuwunel
database, keeping its server name and signing key. Users keep their accounts,
passwords, rooms and history. Logged-in clients stay logged in and keep their
encryption keys and backups. Other servers see the same server as before.

It reads Synapse's PostgreSQL database and `homeserver.yaml`, and, for a
Synapse that delegated logins to the Matrix Authentication Service (MAS),
MAS's database and config too. Synapse's databases are only read.

## Requirements

- Synapse 1.100 or newer (database schema 84 to 94). Upgrade an older Synapse
  first, and let it finish its background updates.
- PostgreSQL. A SQLite Synapse can be ported first with Synapse's
  `synapse_port_db`.
- A tuwunel built with the `synapse_import` feature:
  `cargo build --release --features synapse_import`.
- A new, empty tuwunel database.

## 1. Configure tuwunel

Configure tuwunel as you would for a new server, plus these settings:

| Setting | Value |
|---|---|
| `server_name` | Synapse's `server_name`, unchanged |
| `signing_key_path` | Synapse's `signing_key_path`, so other servers keep trusting this one |
| `synapse_password_pepper` | Synapse's `password_config.pepper`, if it had one. With MAS: the secret of MAS's `bcrypt` password scheme, if any |
| `imported_argon2_secret` | With MAS only: the secret of MAS's `argon2id` password scheme, if it had one |
| `[[global.identity_provider]]` | One per single sign-on provider users logged in with (Synapse's `oidc_providers`, or MAS's `upstream_oauth2.providers`), with the same `issuer_url` |
| `well_known.client` and `oidc_native_auth = true` | With MAS: tuwunel's own OAuth server takes over MAS's sessions and clients. See [OIDC Authorization](../authentication/oidc-server.md) |

Imported password hashes are re-hashed with tuwunel's own scheme at each
user's next login, after which the pepper and secret are no longer used.

Copy or mount Synapse's media store and any appservice registration files
where `homeserver.yaml` says they are, or edit a copy of `homeserver.yaml` to
point at them.

## 2. Rehearse

With Synapse still running, take a copy of its database (`pg_dump`, or a
snapshot) and rehearse against the copy:

```text
tuwunel -c tuwunel.toml --import-synapse homeserver.yaml \
  --synapse-db postgresql://synapse@localhost/synapse_copy \
  --dry-run --report report.json
```

`--dry-run` imports into a throwaway database beside the real one and
deletes it afterwards; media files are checked for, not copied. Read the report: its `skipped` list
names everything that won't come across (see [What stays behind](#what-stays-behind)),
and the counts show what will. The run's length is a guide to the downtime
the real import needs.

The import refuses to start, listing every problem at once, if something is
wrong: a different server name or signing key, an unsupported schema, a
missing pepper, Synapse still connected to its database, unfinished Synapse
work, or a tuwunel database that already has users.

## 3. Cut over

1. Stop Synapse and all its workers (and MAS). The import refuses while
   anything else is connected to their databases.
2. Run the import against the real databases:

   ```text
   tuwunel -c tuwunel.toml --import-synapse homeserver.yaml \
     --synapse-db postgresql://synapse@localhost/synapse --report report.json
   ```

   With MAS, add `--mas-db postgresql://mas@localhost/mas --mas-config mas-config.yaml`.
3. Start tuwunel normally, and point the reverse proxy for the homeserver
   (and federation) at it in place of Synapse.

If the import stops partway (a crash, a full disk), fix the cause and run it
again with `--resume`: it continues at the first unfinished phase.

Keep Synapse's database until you are satisfied. Going back means starting
Synapse on it again; anything sent to tuwunel in between is lost to it.

## With MAS: keep its hostname

Clients that logged in through MAS learned MAS's address. Element X and
Element Web 1.12.24 or newer find tuwunel through the homeserver and need
nothing more. Older Element Web sessions, and any tab left open across the
cutover, keep using MAS's hostname, so keep it served by tuwunel, for
example with nginx:

```nginx
server {
    server_name auth.example.com;   # MAS's public hostname
    location = /.well-known/openid-configuration {
        proxy_pass http://tuwunel/.well-known/openid-configuration;
    }
    location = /oauth2/token  { proxy_pass http://tuwunel/_tuwunel/oidc/token; }
    location = /oauth2/revoke { proxy_pass http://tuwunel/_tuwunel/oidc/revoke; }
    location / { return 404; }
}
```

Older Element Web never forgets the address it logged in with, so keep this
until those clients have upgraded or logged in again.

## What stays behind

The report lists each of these it finds:

- Events tuwunel can't store: old-room events with floats in their content,
  and rooms on a version tuwunel doesn't support.
- Phone numbers (tuwunel binds email only), shadow bans, per-user rate limit
  overrides, event, room and user reports, and unsent delayed events.
- Remote media and thumbnails: tuwunel fetches and generates them again on
  demand.
- With MAS: personal access tokens (MAS kept only their hashes), sessions
  from the client-credentials grant, and OAuth clients with a client secret
  (tuwunel serves public clients only; they register again).
- Single sign-on links whose provider has no matching `identity_provider`,
  or whose external ID wasn't the provider's `sub` claim.
- Rows Synapse stored malformed, each named in the report.

An erased user's (GDPR-erased) messages are hidden from everyone, where
Synapse still showed them to users who were in the room when they were sent.
