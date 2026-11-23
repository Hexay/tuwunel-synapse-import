//! Read-only access to Synapse's database, PostgreSQL or SQLite, and to
//! MAS's, which is always PostgreSQL.
//!
//! Queries are written once, in SQL both understand, with PostgreSQL's `$1`
//! placeholders; where the two differ, [`Source::dialect`] picks the variant.
//! Rows come back as [`Row`], whichever database they came from.

mod postgres;
mod row;
mod sqlite;

use std::path::Path;

use futures::{StreamExt, stream::BoxStream};
use ruma::{OwnedUserId, UserId, serde::Raw};
use serde_json::{Value as JsonValue, value::RawValue};
use tuwunel_core::{Err, Result, err};

pub(crate) use self::row::{Row, Value};
use self::{postgres::Postgres, sqlite::Sqlite};

/// One read-only snapshot of a database, shared by every phase.
///
/// Lookups may run while a stream is open, but streams can't nest.
pub(super) struct Source {
	backend: Backend,
}

enum Backend {
	Postgres(Box<Postgres>),
	Sqlite(Sqlite),
}

/// Which SQL a query must be written in, where the two differ.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Dialect {
	Postgres,
	Sqlite,
}

/// A query parameter either database can bind.
pub(super) trait Param: Sync {
	fn postgres(&self) -> &postgres::PgParam<'_>;
	fn sqlite(&self) -> &dyn rusqlite::ToSql;
}

impl<T> Param for T
where
	T: tokio_postgres::types::ToSql + rusqlite::ToSql + Sync,
{
	fn postgres(&self) -> &postgres::PgParam<'_> { self }

	fn sqlite(&self) -> &dyn rusqlite::ToSql { self }
}

impl Source {
	/// Synapse's database: a file is SQLite, anything else a PostgreSQL
	/// connection string or URL. The import can never write to either.
	pub(super) async fn connect(url: &str) -> Result<Self> {
		let path = Path::new(url);
		let backend = if path.is_file() {
			Backend::Sqlite(Sqlite::connect(path).await?)
		} else {
			Backend::Postgres(Box::new(Postgres::connect(url).await?))
		};

		Ok(Self { backend })
	}

	/// MAS's database, which is PostgreSQL only.
	pub(super) async fn connect_postgres(url: &str) -> Result<Self> {
		if Path::new(url).is_file() {
			return Err!("{url} is a file, but MAS keeps its data in PostgreSQL only");
		}

		Ok(Self {
			backend: Backend::Postgres(Box::new(Postgres::connect(url).await?)),
		})
	}

	pub(super) fn dialect(&self) -> Dialect {
		match self.backend {
			| Backend::Postgres(_) => Dialect::Postgres,
			| Backend::Sqlite(_) => Dialect::Sqlite,
		}
	}

	pub(super) async fn rows(&self, sql: &str, params: &[&dyn Param]) -> Result<Vec<Row>> {
		match &self.backend {
			| Backend::Postgres(db) => db.rows(sql, params).await,
			| Backend::Sqlite(db) => db.rows(sql, params).await,
		}
		.map_err(|e| err!("Synapse {e}"))
	}

	/// Rows as the database hands them over, for the tables that grow with
	/// history.
	pub(super) async fn stream(
		&self,
		sql: &str,
		params: &[&dyn Param],
	) -> Result<BoxStream<'static, Result<Row>>> {
		Ok(match &self.backend {
			| Backend::Postgres(db) => db.stream(sql, params).await?.boxed(),
			| Backend::Sqlite(db) => db.stream(sql, params)?.boxed(),
		}
		.map(|row| row.map_err(|e| err!("Synapse {e}")))
		.boxed())
	}

	/// Whatever else uses the database: other connections to PostgreSQL, or
	/// unwritten changes beside a SQLite file, either meaning Synapse (or MAS)
	/// may still be running.
	pub(super) async fn other_clients(&self) -> Result<Option<String>> {
		match &self.backend {
			| Backend::Postgres(db) => db.other_clients().await,
			| Backend::Sqlite(db) => db.other_clients(),
		}
	}

	/// For what older Synapse releases lack. Synapse adds columns within one
	/// schema version across releases, so the schema number can't tell.
	pub(super) async fn has_column(&self, table: &str, column: &str) -> Result<bool> {
		let sql = match self.dialect() {
			| Dialect::Postgres =>
				"SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() \
				 AND table_name = $1 AND column_name = $2",
			| Dialect::Sqlite => "SELECT 1 FROM pragma_table_info($1) WHERE name = $2",
		};

		Ok(!self.rows(sql, &[&table, &column]).await?.is_empty())
	}

	/// `column` if `table` has it, otherwise `fallback`, for a SELECT list.
	pub(super) async fn column_or(
		&self,
		table: &str,
		column: &'static str,
		fallback: &'static str,
	) -> Result<&'static str> {
		Ok(if self.has_column(table, column).await? {
			column
		} else {
			fallback
		})
	}

	pub(super) async fn has_table(&self, table: &str) -> Result<bool> {
		let sql = match self.dialect() {
			| Dialect::Postgres =>
				"SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() \
				 AND table_name = $1",
			| Dialect::Sqlite => "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = $1",
		};

		Ok(!self.rows(sql, &[&table]).await?.is_empty())
	}

	pub(super) async fn count(&self, table: &'static str) -> Result<i64> {
		self.count_where(table, "").await
	}

	/// Rows of `table` that `filter`, a WHERE clause or nothing, keeps.
	pub(super) async fn count_where(
		&self,
		table: &'static str,
		filter: &'static str,
	) -> Result<i64> {
		let sql = format!("SELECT count(*) FROM {table} {filter}");

		Ok(self
			.rows(&sql, &[])
			.await?
			.first()
			.map_or(0, |row| row.get(0)))
	}
}

/// `$1, $2, …` for `count` parameters starting after `after`, for an `IN`
/// list: neither array parameters nor `ANY` work in SQLite.
pub(super) fn placeholders(after: usize, count: usize) -> String {
	(1..=count)
		.map(|n| format!("${}", n.saturating_add(after)))
		.collect::<Vec<_>>()
		.join(", ")
}

pub(super) fn user_id(row: &Row, idx: usize) -> Result<OwnedUserId> {
	let name: &str = row.get(idx);

	UserId::parse(name).map_err(|e| err!("Synapse user {name:?} is not a valid user ID: {e}"))
}

pub(super) fn json(content: &str) -> Result<JsonValue> {
	serde_json::from_str(content).map_err(|e| err!("Synapse stored invalid JSON: {e}"))
}

/// Keeps a JSON column byte for byte, as Synapse stored it.
pub(super) fn json_column(json: String) -> Result<Box<RawValue>> {
	RawValue::from_string(json).map_err(|e| err!("Synapse stored invalid JSON: {e}"))
}

pub(super) fn raw_json<T>(json: String) -> Result<Raw<T>> {
	json_column(json).map(Raw::from_json)
}

#[cfg(test)]
mod tests;
