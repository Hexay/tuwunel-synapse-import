//! SQLite, for a Synapse that ran on it: the file opened read-only and
//! immutable, as nothing may change it while Synapse is stopped.
//!
//! rusqlite is blocking, so lookups run on the blocking pool, one at a time on
//! one connection, and each stream reads on its own thread and connection,
//! handing rows over a channel. Every connection sees the same file, so all
//! of them read the same snapshot.

use std::{
	fmt::Write,
	path::{Path, PathBuf},
	sync::{Arc, Mutex},
};

use futures::Stream;
use md5::{Digest, Md5};
use rusqlite::{
	Connection, OpenFlags,
	functions::FunctionFlags,
	types::{ToSqlOutput, Value as SqlValue, ValueRef},
};
use tokio::sync::mpsc;
use tuwunel_core::{Err, Result, err};

use super::{Param, Row, Value};

/// Rows a stream reads ahead of the import.
const STREAM_BUFFER: usize = 1024;

pub(super) struct Sqlite {
	path: PathBuf,
	connection: Arc<Mutex<Connection>>,
}

impl Sqlite {
	pub(super) async fn connect(path: &Path) -> Result<Self> {
		let path = path.to_path_buf();
		let opened = path.clone();
		let connection = tokio::task::spawn_blocking(move || open(&opened))
			.await
			.map_err(|e| err!("Cannot open the SQLite database: {e}"))??;

		Ok(Self {
			path,
			connection: Arc::new(Mutex::new(connection)),
		})
	}

	pub(super) async fn rows(&self, sql: &str, params: &[&dyn Param]) -> Result<Vec<Row>> {
		let params = owned_params(params, sql)?;
		let sql = placeholders(sql);
		let connection = self.connection.clone();

		tokio::task::spawn_blocking(move || {
			let connection = connection
				.lock()
				.map_err(|_| err!("the SQLite connection was poisoned"))?;

			query(&connection, &sql, params, |row| {
				Ok::<_, std::convert::Infallible>(Some(row))
			})
			.map(|(rows, _)| rows)
		})
		.await
		.map_err(|e| err!("SQLite query failed: {e}"))?
	}

	pub(super) fn stream(
		&self,
		sql: &str,
		params: &[&dyn Param],
	) -> Result<impl Stream<Item = Result<Row>> + Send + use<>> {
		let params = owned_params(params, sql)?;
		let sql = placeholders(sql);
		let path = self.path.clone();
		let (sender, receiver) = mpsc::channel(STREAM_BUFFER);

		tokio::task::spawn_blocking(move || {
			let sent = open(&path).and_then(|connection| {
				query(&connection, &sql, params, |row| {
					// Stops reading once the import drops the stream.
					sender
						.blocking_send(Ok(row))
						.map(|()| None)
						.map_err(drop)
				})
			});

			if let Err(e) = sent {
				sender.blocking_send(Err(e)).ok();
			}
		});

		Ok(futures::stream::unfold(receiver, async |mut receiver| {
			receiver
				.recv()
				.await
				.map(|row| (row, receiver))
		}))
	}

	/// A journal beside the database holds changes the file doesn't have yet:
	/// Synapse is still running, or didn't shut down cleanly.
	pub(super) fn other_clients(&self) -> Result<Option<String>> {
		let mut journals = Vec::new();
		for suffix in ["-wal", "-journal"] {
			let mut journal = self.path.clone().into_os_string();
			journal.push(suffix);
			let journal = PathBuf::from(journal);
			match std::fs::metadata(&journal) {
				| Ok(metadata) if metadata.len() > 0 => journals.push(journal.display().to_string()),
				| Ok(_) => {},
				| Err(e) if e.kind() == std::io::ErrorKind::NotFound => {},
				| Err(e) => return Err!("Cannot check for {}: {e}", journal.display()),
			}
		}

		Ok((!journals.is_empty()).then(|| format!("unwritten changes in {}", journals.join(", "))))
	}
}

/// Read-only and immutable: SQLite takes no locks and writes nothing, not even
/// a `-shm` file. `md5` is registered for the verifier's seeded sampling, which
/// PostgreSQL has built in.
fn open(path: &Path) -> Result<Connection> {
	let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
		| OpenFlags::SQLITE_OPEN_URI
		| OpenFlags::SQLITE_OPEN_NO_MUTEX;

	let connection = Connection::open_with_flags(uri(path), flags)
		.map_err(|e| err!("Cannot open the SQLite database {}: {e}", path.display()))?;

	connection
		.create_scalar_function(
			"md5",
			1,
			FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
			|context| {
				let text: Option<String> = context.get(0)?;
				Ok(text.map(|text| hex(&Md5::digest(text.as_bytes()))))
			},
		)
		.map_err(|e| err!("Cannot set up the SQLite database: {e}"))?;

	// Fails here, not at the first query, on a file that isn't a database.
	connection
		.query_row("SELECT count(*) FROM sqlite_master", [], |_| Ok(()))
		.map_err(|e| err!("{} is not a SQLite database: {e}", path.display()))?;

	Ok(connection)
}

/// Runs `sql`, handing each row to `each` until it returns an error, which
/// ends the query early; returns the rows `each` kept.
fn query<E>(
	connection: &Connection,
	sql: &str,
	params: Vec<SqlValue>,
	mut each: impl FnMut(Row) -> Result<Option<Row>, E>,
) -> Result<(Vec<Row>, bool)> {
	let failed = |e: rusqlite::Error| err!("SQLite query failed: {e}: {sql}");
	let mut statement = connection.prepare_cached(sql).map_err(failed)?;
	let mut rows = statement
		.query(rusqlite::params_from_iter(params))
		.map_err(failed)?;

	let mut kept = Vec::new();
	while let Some(row) = rows.next().map_err(failed)? {
		let columns = row.as_ref().column_count();
		let mut values = Vec::with_capacity(columns);
		for idx in 0..columns {
			values.push(value(row.get_ref(idx).map_err(failed)?));
		}

		match each(Row::new(values)) {
			| Ok(Some(row)) => kept.push(row),
			| Ok(None) => {},
			| Err(_) => return Ok((kept, false)),
		}
	}

	Ok((kept, true))
}

fn value(value: ValueRef<'_>) -> Value {
	match value {
		| ValueRef::Null => Value::Null,
		| ValueRef::Integer(value) => Value::Int(value),
		| ValueRef::Real(value) => Value::Real(value),
		| ValueRef::Text(text) => match std::str::from_utf8(text) {
			| Ok(text) => Value::Text(text.to_owned()),
			| Err(_) => Value::Bytes(text.to_vec()),
		},
		| ValueRef::Blob(bytes) => Value::Bytes(bytes.to_vec()),
	}
}

/// The parameters as values a blocking thread can own.
fn owned_params(params: &[&dyn Param], sql: &str) -> Result<Vec<SqlValue>> {
	params
		.iter()
		.map(|param| match param.sqlite().to_sql() {
			| Ok(ToSqlOutput::Borrowed(value)) => Ok(value.into()),
			| Ok(ToSqlOutput::Owned(value)) => Ok(value),
			| Ok(_) => Err!("unsupported SQLite parameter: {sql}"),
			| Err(e) => Err!("SQLite parameter: {e}: {sql}"),
		})
		.collect()
}

/// PostgreSQL's `$1` as SQLite's `?1`, which binds by number as `$1` does.
/// SQLite would read `$1` as a name, numbered by first appearance instead.
fn placeholders(sql: &str) -> String {
	let mut out = String::with_capacity(sql.len());
	let mut chars = sql.chars().peekable();
	while let Some(c) = chars.next() {
		if c == '$' && chars.peek().is_some_and(char::is_ascii_digit) {
			out.push('?');
		} else {
			out.push(c);
		}
	}

	out
}

/// A `file:` URI for `path`, escaping what a URI would read otherwise, and
/// non-ASCII bytes, which SQLite decodes back.
fn uri(path: &Path) -> String {
	let path = path.to_string_lossy();
	let mut uri = String::from("file:");
	for byte in path.bytes() {
		match byte {
			| b'%' | b'?' | b'#' | 0x80.. =>
				write!(uri, "%{byte:02X}").expect("writing to a String"),
			| _ => uri.push(char::from(byte)),
		}
	}

	uri.push_str("?mode=ro&immutable=1");
	uri
}

fn hex(bytes: &[u8]) -> String {
	bytes
		.iter()
		.fold(String::with_capacity(bytes.len().saturating_mul(2)), |mut hex, byte| {
			write!(hex, "{byte:02x}").expect("writing to a String");
			hex
		})
}

#[cfg(test)]
mod tests {
	use std::path::Path;

	use super::{hex, placeholders, uri};

	#[test]
	fn placeholders_number_like_postgres() {
		assert_eq!(
			placeholders("SELECT 1 FROM t WHERE a = $2 AND b = $1 AND c = $10"),
			"SELECT 1 FROM t WHERE a = ?2 AND b = ?1 AND c = ?10"
		);
		assert_eq!(placeholders("SELECT '$' || x"), "SELECT '$' || x");
	}

	#[test]
	fn uri_escapes_what_a_uri_reads() {
		assert_eq!(
			uri(Path::new("/data/home?server#1%.db")),
			"file:/data/home%3Fserver%231%25.db?mode=ro&immutable=1"
		);
		assert_eq!(uri(Path::new("/é.db")), "file:/%C3%A9.db?mode=ro&immutable=1");
	}

	#[test]
	fn hex_is_lowercase() {
		assert_eq!(hex(&[0x00, 0xab, 0xff]), "00abff");
	}
}
