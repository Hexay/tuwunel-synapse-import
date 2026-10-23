//! Read-only access to Synapse's PostgreSQL database.

use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls, Row, types::ToSql};
use tuwunel_core::{Result, err, error};

/// One read-only snapshot of Synapse's database, shared by every phase.
pub(super) struct Source {
	client: Client,
	connection: JoinHandle<()>,
}

impl Source {
	/// Connects and opens the snapshot. The transaction is read-only, so the
	/// import can never write to Synapse.
	pub(super) async fn connect(url: &str) -> Result<Self> {
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

	// TODO: stream with query_raw for the tables that grow with history (events, state).
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
