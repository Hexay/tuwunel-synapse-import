//! Read-only access to Synapse's PostgreSQL database.

use futures::{Stream, TryStreamExt};
use ruma::{OwnedUserId, UserId, serde::Raw};
use serde_json::{Value, value::RawValue};
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls, Row, types::ToSql};
use tuwunel_core::{Err, Result, err, error};

/// One read-only snapshot of Synapse's database, shared by every phase.
pub(super) struct Source {
	client: Client,
	connection: JoinHandle<()>,
}

impl Source {
	/// Connects and opens the snapshot. The transaction is read-only, so the
	/// import can never write to Synapse.
	pub(super) async fn connect(url: &str) -> Result<Self> {
		if std::path::Path::new(url).is_file() {
			return Err!(
				"{url} is a file, so probably Synapse's SQLite database. Copy it into \
				 PostgreSQL with Synapse's own synapse_port_db first, then pass that database \
				 as --synapse-db"
			);
		}

		let (client, connection) = tokio_postgres::connect(url, NoTls)
			.await
			.map_err(|e| err!("Cannot connect to the Synapse database: {e}"))?;

		let connection = tokio::spawn(async move {
			if let Err(e) = connection.await {
				error!("Synapse database connection failed: {e}");
			}
		});

		client
			.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
			.await
			.map_err(|e| err!("Cannot open a Synapse database snapshot: {e}"))?;

		Ok(Self { client, connection })
	}

	pub(super) async fn rows(
		&self,
		sql: &str,
		params: &[&(dyn ToSql + Sync)],
	) -> Result<Vec<Row>> {
		self.client
			.query(sql, params)
			.await
			.map_err(|e| err!("Synapse query failed: {e}: {sql}"))
	}

	/// Rows as Postgres sends them, for the tables that grow with history.
	pub(super) async fn stream(
		&self,
		sql: &str,
		params: &[&(dyn ToSql + Sync)],
	) -> Result<impl Stream<Item = Result<Row>> + Send + use<>> {
		let rows = self
			.client
			.query_raw(sql, params.iter().copied())
			.await
			.map_err(|e| err!("Synapse query failed: {e}: {sql}"))?;

		let sql = sql.to_owned();
		Ok(rows.map_err(move |e| err!("Synapse query failed: {e}: {sql}")))
	}

	/// For what older Synapse releases lack. Synapse adds columns within one
	/// schema version across releases, so the schema number can't tell.
	pub(super) async fn has_column(&self, table: &str, column: &str) -> Result<bool> {
		let rows = self
			.rows(
				"SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() \
				 AND table_name = $1 AND column_name = $2",
				&[&table, &column],
			)
			.await?;

		Ok(!rows.is_empty())
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
		let rows = self
			.rows(
				"SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() \
				 AND table_name = $1",
				&[&table],
			)
			.await?;

		Ok(!rows.is_empty())
	}

	pub(super) async fn count(&self, table: &str) -> Result<i64> {
		let sql = format!("SELECT count(*) FROM {table}");

		self.rows(&sql, &[])
			.await
			.map(|rows| rows[0].get(0))
	}
}

impl Drop for Source {
	fn drop(&mut self) { self.connection.abort(); }
}

pub(super) fn user_id(row: &Row, idx: usize) -> Result<OwnedUserId> {
	let name: String = row.get(idx);

	UserId::parse(name.as_str())
		.map_err(|e| err!("Synapse user {name:?} is not a valid user ID: {e}"))
}

pub(super) fn json(content: &str) -> Result<Value> {
	serde_json::from_str(content).map_err(|e| err!("Synapse stored invalid JSON: {e}"))
}

/// Keeps a JSON column byte for byte, as Synapse stored it.
pub(super) fn json_column(json: String) -> Result<Box<RawValue>> {
	RawValue::from_string(json).map_err(|e| err!("Synapse stored invalid JSON: {e}"))
}

pub(super) fn raw_json<T>(json: String) -> Result<Raw<T>> {
	json_column(json).map(Raw::from_json)
}
