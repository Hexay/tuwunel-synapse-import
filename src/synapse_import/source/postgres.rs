//! PostgreSQL, for Synapse and MAS: one exported snapshot shared by two
//! read-only connections.

use std::error::Error as StdError;

use futures::{Stream, TryStreamExt};
use tokio::task::JoinHandle;
use tokio_postgres::{
	Client, NoTls,
	types::{FromSql, ToSql, Type},
};
use tuwunel_core::{Err, Result, err, error};

use super::{Param, Row, Value};

/// Streams run on their own connection: on a shared one, a query issued while
/// a stream's rows are still unread would wait behind them forever. So lookups
/// may run while a stream is open, but streams can't nest.
pub(super) struct Postgres {
	client: Client,
	streams: Client,
	pids: Vec<i32>,
	connections: [JoinHandle<()>; 2],
}

impl Postgres {
	/// Connects and opens the snapshot. The transactions are read-only, so the
	/// import can never write to the database.
	pub(super) async fn connect(url: &str) -> Result<Self> {
		let (client, first) = open(url).await?;
		let (streams, second) = open(url).await?;

		let snapshot: String = client
			.query_one("SELECT pg_export_snapshot()", &[])
			.await
			.map_err(|e| err!("Cannot share the database snapshot: {e}"))?
			.get(0);

		streams
			.batch_execute(&format!("SET TRANSACTION SNAPSHOT '{snapshot}'"))
			.await
			.map_err(|e| err!("Cannot share the database snapshot: {e}"))?;

		let mut pids = Vec::with_capacity(2);
		for connection in [&client, &streams] {
			pids.push(backend_pid(connection).await?);
		}

		Ok(Self {
			client,
			streams,
			pids,
			connections: [first, second],
		})
	}

	pub(super) async fn rows(&self, sql: &str, params: &[&dyn Param]) -> Result<Vec<Row>> {
		let params: Vec<_> = params.iter().map(|param| param.postgres()).collect();

		self.client
			.query(sql, &params)
			.await
			.map_err(|e| err!("query failed: {e}: {sql}"))?
			.iter()
			.map(convert)
			.collect::<Result<_>>()
			.map_err(|e| err!("{e}: {sql}"))
	}

	pub(super) async fn stream(
		&self,
		sql: &str,
		params: &[&dyn Param],
	) -> Result<impl Stream<Item = Result<Row>> + Send + use<>> {
		let params = params.iter().map(|param| param.postgres());
		let rows = self
			.streams
			.query_raw(sql, params)
			.await
			.map_err(|e| err!("query failed: {e}: {sql}"))?;

		let sql = sql.to_owned();
		Ok(rows
			.map_err(|e| err!("query failed: {e}"))
			.and_then(|row| futures::future::ready(convert(&row)))
			.map_err(move |e| err!("{e}: {sql}")))
	}

	/// Who else is connected to the database, as "N from <client>" entries.
	pub(super) async fn other_clients(&self) -> Result<Option<String>> {
		let rows = self
			.client
			.query(
				"SELECT count(*), coalesce(nullif(application_name, ''), host(client_addr), \
				 'local') FROM pg_stat_activity WHERE datname = current_database() AND pid <> \
				 ALL($1) AND backend_type = 'client backend' GROUP BY 2 ORDER BY 2",
				&[&self.pids],
			)
			.await
			.map_err(|e| err!("query failed: {e}"))?;

		Ok((!rows.is_empty()).then(|| {
			rows.iter()
				.map(|row| format!("{} from {}", row.get::<_, i64>(0), row.get::<_, String>(1)))
				.collect::<Vec<_>>()
				.join(", ")
		}))
	}
}

impl Drop for Postgres {
	fn drop(&mut self) {
		self.connections
			.iter()
			.for_each(JoinHandle::abort);
	}
}

/// A connection inside a read-only repeatable-read transaction.
async fn open(url: &str) -> Result<(Client, JoinHandle<()>)> {
	let (client, connection) = tokio_postgres::connect(url, NoTls)
		.await
		.map_err(|e| err!("Cannot connect to the database: {e}"))?;

	let connection = tokio::spawn(async move {
		if let Err(e) = connection.await {
			error!("Database connection failed: {e}");
		}
	});

	client
		.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
		.await
		.map_err(|e| err!("Cannot open a database snapshot: {e}"))?;

	Ok((client, connection))
}

async fn backend_pid(client: &Client) -> Result<i32> {
	client
		.query_one("SELECT pg_backend_pid()", &[])
		.await
		.map(|row| row.get(0))
		.map_err(|e| err!("query failed: {e}"))
}

/// The row's values, for the types Synapse and MAS columns have; a query
/// selecting anything else must cast it.
fn convert(row: &tokio_postgres::Row) -> Result<Row> {
	let mut values = Vec::with_capacity(row.len());
	for (idx, column) in row.columns().iter().enumerate() {
		let ty = column.type_();
		let value = match *ty {
			| Type::BOOL => get(row, idx, Value::Bool),
			| Type::INT2 => get(row, idx, |value: i16| Value::Int(value.into())),
			| Type::INT4 => get(row, idx, |value: i32| Value::Int(value.into())),
			| Type::INT8 => get(row, idx, Value::Int),
			| Type::OID => get(row, idx, |value: u32| Value::Int(value.into())),
			| Type::FLOAT4 => get(row, idx, |value: f32| Value::Real(value.into())),
			| Type::FLOAT8 => get(row, idx, Value::Real),
			| Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN =>
				get(row, idx, Value::Text),
			| Type::BYTEA => get(row, idx, Value::Bytes),
			| Type::JSON | Type::JSONB => get(row, idx, |Json(text)| Value::Text(text)),
			| Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => get(row, idx, Value::TextArray),
			| _ => {
				return Err!(
					"column {} has type {ty}, which the import doesn't read; cast it",
					column.name()
				);
			},
		};

		values.push(value?);
	}

	Ok(Row::new(values))
}

fn get<'a, T: FromSql<'a>>(
	row: &'a tokio_postgres::Row,
	idx: usize,
	value: impl FnOnce(T) -> Value,
) -> Result<Value> {
	row.try_get::<_, Option<T>>(idx)
		.map(|found| found.map_or(Value::Null, value))
		.map_err(|e| err!("column {idx}: {e}"))
}

/// JSON or JSONB as its text.
struct Json(String);

impl<'a> FromSql<'a> for Json {
	fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn StdError + Sync + Send>> {
		// JSONB's binary form is a version byte, then the text.
		let text = match *ty {
			| Type::JSONB => match raw.split_first() {
				| Some((1, text)) => text,
				| _ => return Err("unknown JSONB version".into()),
			},
			| _ => raw,
		};

		Ok(Self(std::str::from_utf8(text)?.to_owned()))
	}

	fn accepts(ty: &Type) -> bool { matches!(*ty, Type::JSON | Type::JSONB) }
}

/// Keeps `ToSql` reachable for `Param`.
pub(super) type PgParam<'a> = dyn ToSql + Sync + 'a;
