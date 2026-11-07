# Migrating from Synapse

`tuwunel --import-synapse` moves a stopped Synapse server into a new Tuwunel
database, keeping its server name and signing key. Users keep their accounts,
passwords, rooms and history, with their read receipts, unread and highlight
counts and notification lists. Logged-in clients stay logged in and keep their
encryption keys and backups. Other servers see the same server as before.

It reads Synapse's PostgreSQL database and `homeserver.yaml`, and, for a
Synapse that delegated logins to the Matrix Authentication Service (MAS),
MAS's database and config too. Synapse's databases are only read.

## Requirements

- Synapse 1.100 to 1.163 (database schema 84 to 94). Upgrade an older Synapse
  first, and let it finish its background updates.
- PostgreSQL. A SQLite Synapse can be ported first with Synapse's
  `synapse_port_db`. The connection to it is not encrypted, so run the import
  on the database host or over a trusted network.
- A Tuwunel built with the `synapse_import` feature:
  `cargo build --release --features synapse_import`.
- A new, empty Tuwunel database.

## 1. Configure Tuwunel

Configure Tuwunel as you would for a new server, plus these settings:

| Setting | Value |
|---|---|
| `server_name` | Synapse's `server_name`, unchanged |
| `signing_key_path` | Synapse's `signing_key_path`, so other servers keep trusting this one |
| `imported_bcrypt_pepper` | Synapse's `password_config.pepper`, if it had one. With MAS: the secret of MAS's `bcrypt` password scheme, if any |
| `imported_argon2_secret` | With MAS only: the secret of MAS's `argon2id` password scheme, if it had one |
| `[[global.identity_provider]]` | One per single sign-on provider users logged in with (Synapse's `oidc_providers`, or MAS's `upstream_oauth2.providers`), with the same `issuer_url` |
| `well_known.client` and `oidc_native_auth = true` | With MAS: Tuwunel's own OAuth server takes over MAS's sessions and clients. See [OIDC Authorization](../authentication/oidc-server.md) |

Imported password hashes are re-hashed with Tuwunel's own scheme at each
user's next login, after which the pepper and secret are no longer used.

Copy or mount Synapse's media store and any appservice registration files
where `homeserver.yaml` says they are, or edit a copy of `homeserver.yaml` to
point at them. Relative paths in it are read from the directory the import
runs in, as Synapse reads them from its own working directory.

## 2. Rehearse

With Synapse still running, take a copy of its database (`pg_dump`, or a
snapshot) and rehearse against the copy:

```text
tuwunel -c tuwunel.toml --import-synapse homeserver.yaml \
  --synapse-db postgresql://synapse@localhost/synapse_copy \
  --import-dry-run --import-report report.json --import-verify
```

The database URLs can instead be given in `TUWUNEL_SYNAPSE_DB` and
`TUWUNEL_MAS_DB`, keeping passwords off the command line.

`--import-dry-run` imports into a throwaway database beside the real one and
deletes it afterwards; media files are checked for, not copied. Read the
report: its `skipped` list names everything that won't come across (see
[What stays behind](#what-stays-behind)), and the counts show what will.
`--import-verify` checks the throwaway database against Synapse's before it
is deleted (media aside). The run's length is a guide to the downtime the
real import needs.

The import refuses to start, listing every problem at once, if something is
wrong: a different server name or signing key, an unsupported schema, a
missing pepper, Synapse still connected to its database, unfinished Synapse
work, or a Tuwunel database that already has users.

## 3. Cut over

1. Stop Synapse and all its workers (and MAS). The import refuses while
   anything else is connected to their databases.
2. Run the import against the real databases:

   ```text
   tuwunel -c tuwunel.toml --import-synapse homeserver.yaml \
     --synapse-db postgresql://synapse@localhost/synapse --import-report report.json \
     --import-verify --import-verify-report verify.json
   ```

   With MAS, add `--mas-db postgresql://mas@localhost/mas --mas-config mas-config.yaml`.
3. Check the verification passed (see [Verifying the import](#verifying-the-import)).
4. Start Tuwunel normally, and point the reverse proxy for the homeserver
   (and federation) at it in place of Synapse.

If the import stops partway (a crash, a full disk), fix the cause and run it
again with `--import-resume`: it reruns the first unfinished phase and
continues from there.

Other servers carry on as before: they retry what they couldn't deliver
during the cutover, and Tuwunel keeps the signing keys Synapse had fetched
from them, so their past events still verify while one of them is offline.

Keep Synapse's database until you are satisfied. Going back means starting
Synapse on it again; anything sent to Tuwunel in between is lost to it.

Clients open across the cutover carry on. Their next `/sync` sends Synapse's
`since` token, which Tuwunel can't read, so it answers with a full sync from
the start: every room restarts its timeline, and every peer's device list is
marked changed so encryption picks up devices added meanwhile. Sliding sync
connections start over the same way. Messages and room keys queued for a
device while it was away are delivered, including to sliding-sync clients that
kept Synapse's to-device position. History pagination tokens from Synapse get
`M_INVALID_PARAM`; the restarted timelines hand out new ones.

## Verifying the import

`--import-verify` compares Synapse's database with what Tuwunel now holds,
after the import and before Tuwunel starts. To verify an import later
(before Tuwunel has run, since anything it does afterwards shows up as a
difference), use `--import-verify-only` in place of it: that opens Tuwunel's
database read-only and imports nothing. Both read Synapse's database in a
read-only transaction, so neither database is written.

It checks, Synapse against Tuwunel:

- users (and their deactivated, admin, locked and suspended flags), profiles,
  devices, and that each device's access token still signs in as it;
- device keys, cross-signing keys, one-time key counts, unused fallback
  keys, key backup versions and their key counts;
- each room's version and directory listing; its whole timeline, in the
  order clients page it; its current state, entry by entry; its joined,
  invited and knocking members; and its forward extremities;
- in depth, for a sample of each room's events: the stored event against
  Synapse's (or its redaction, where Synapse has a redaction for it or the
  sender was erased), that its reference hash still gives its event ID, that
  its hashes and signatures re-verify against the keys either server holds
  (nothing is fetched), and that its whole auth chain is stored;
- aliases, account data, room tags, push rules, filters, read receipts, and
  local media (for a sample, byte for byte).

`--import-verify-sample N` sets the events per room checked in depth, and the
media files compared byte for byte (default 32; 0 checks every one), and
`--import-verify-seed` picks the sample, so a rerun checks the same events.
Everything else is checked in full, streaming, so memory doesn't grow with
history.

The summary goes to the log; `--import-verify-report verify.json` also
writes it as JSON, with a `verify.txt` summary beside it. Each domain shows
its counts on both sides, what matched, and what was excused: rows the
import reports as not imported (a guest account, an event Tuwunel can't
store, a quarantined file) are excused rather than counted missing. Every
mismatch is listed with what it concerns, and any mismatch makes the run
exit with an error.

Events in rooms of version 1 and 2 reference earlier events with hashes that
Tuwunel doesn't store, so their signatures can't be re-checked from
Tuwunel's copy; those are listed as excused. Events signed by a server whose
key neither database holds are counted as unverifiable.

## With MAS: keep its hostname

Clients that logged in through MAS learned MAS's address. Element X and
Element Web 1.12.24 or newer find Tuwunel through the homeserver and need
nothing more. Older Element Web sessions, and any tab left open across the
cutover, keep using MAS's hostname, so keep it served by Tuwunel, for
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

Rooms users forgot stay forgotten, status messages are kept (everyone shows
offline until their client syncs again), and Synapse's cache of other servers'
media is carried over, so media from servers that are gone still loads.

The report lists each of these it finds:

- Events Tuwunel can't store: old-room events with floats in their content,
  and rooms on a version Tuwunel doesn't support.
- Phone numbers (Tuwunel binds email only), shadow bans, per-user rate limit
  overrides, event, room and user reports, and unsent delayed events.
- To-device messages Synapse hadn't yet delivered to other servers, and
  deactivations it hadn't finished, whose users may still be in rooms.
- Synapse's cache of remote users' devices (`device_lists_remote_cache`),
  counted. Tuwunel keeps no copy of other servers' device keys and asks
  their server on every key query, so while a server is unreachable its
  users' devices and cross-signing keys can't be found: sessions that already
  know them keep encrypting to them, but a device signed in after the move
  sees none, until that server is back. Synapse's copy of remote users'
  cross-signing keys is imported, so your users' verification of remote
  users carries over.
- Work Synapse hadn't finished sending: appservice transactions (those bridges
  miss the events in them), device list updates and the latest events for
  other servers (they catch up from the next event they receive).
- Admin tasks Synapse hadn't finished, such as room deletions and history
  purges: run them again in Tuwunel's admin room.
- Thread subscriptions (MSC4306), stickiness of sticky events (MSC4354; the
  events themselves are imported), per-user experimental features, and
  rooms published in appservice network directories (bridges publish them
  again, or publish them in the public directory).
- Identity server bindings: Tuwunel doesn't talk to identity servers, so
  removing an address or deactivating an account leaves the binding there;
  remove it at the identity server if needed.
- Account expiry dates (`account_validity`) and room retention policies:
  Tuwunel neither expires accounts nor purges old messages.
- Cached remote media whose file was already gone from Synapse's media store,
  counted: Tuwunel fetches it again on demand. Remote thumbnails are
  generated again.
- With MAS: personal access tokens (MAS kept only their hashes), sessions
  from the client-credentials grant, and OAuth clients with a client secret
  (Tuwunel serves public clients only; they register again).
- Single sign-on links whose provider has no matching `identity_provider`,
  or whose external ID wasn't the provider's `sub` claim.
- Rows Synapse stored malformed, each named in the report.

Unread counts come across as Synapse last computed them, but not the unread
message count of its experimental MSC2654, which Tuwunel doesn't keep.
`/notifications` marks an entry read by the room's or its thread's latest
receipt, where Synapse took the user's latest receipt in the room, in any
thread, and lists imported entries room by room in import order rather than
interleaved by time.

An erased user's (GDPR-erased) messages are hidden from everyone, where
Synapse still showed them to users who were in the room when they were sent.
