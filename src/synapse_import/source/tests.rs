//! The SQLite backend against a small database, as the PostgreSQL one needs
//! a server.

use std::path::PathBuf;

use futures::TryStreamExt;

use super::{Dialect, Source, placeholders};

/// A database file that is removed again when dropped.
struct TempDb(PathBuf);

impl TempDb {
	fn new(name: &str, sql: &str) -> Self {
		let path = std::env::temp_dir().join(format!(
			"tuwunel-synapse-import-{name}-{}.db",
			std::process::id()
		));
		std::fs::remove_file(&path).ok();

		let connection = rusqlite::Connection::open(&path).expect("a new database");
		connection.execute_batch(sql).expect("the test schema");
		drop(connection);

		Self(path)
	}

	fn url(&self) -> &str { self.0.to_str().expect("a UTF-8 path") }
}

impl Drop for TempDb {
	fn drop(&mut self) { std::fs::remove_file(&self.0).ok(); }
}

const SCHEMA: &str = "
CREATE TABLE users (name TEXT, admin SMALLINT, locked BOOLEAN, key_json BYTEA);
INSERT INTO users VALUES ('@a:x', 1, 0, X'7b7d'), ('@b:x', 0, 1, NULL), ('@c:x', 0, 0, NULL);
";

#[tokio::test]
async fn reads_a_file_as_sqlite() {
	let db = TempDb::new("rows", SCHEMA);
	let source = Source::connect(db.url()).await.expect("opens");
	assert_eq!(source.dialect(), Dialect::Sqlite);

	let rows = source
		.rows(
			"SELECT name, admin <> 0, locked, key_json FROM users WHERE name <> $1 ORDER BY name",
			&[&"@c:x"],
		)
		.await
		.expect("the query runs");

	assert_eq!(rows.len(), 2);
	assert_eq!(rows[0].get::<_, &str>(0), "@a:x");
	assert!(rows[0].get::<_, bool>(1));
	assert!(!rows[0].get::<_, bool>(2));
	assert_eq!(rows[0].get::<_, &[u8]>(3), b"{}");
	assert!(rows[1].get::<_, bool>(2));
	assert_eq!(rows[1].get::<_, Option<&[u8]>>(3), None);
}

#[tokio::test]
async fn binds_placeholders_by_number() {
	let db = TempDb::new("numbers", SCHEMA);
	let source = Source::connect(db.url()).await.expect("opens");

	let rows = source
		.rows("SELECT $2 || $1, $1", &[&"a", &"b"])
		.await
		.expect("the query runs");

	assert_eq!(rows[0].get::<_, &str>(0), "ba");
	assert_eq!(rows[0].get::<_, &str>(1), "a");

	let names = ["@a:x", "@c:x"];
	let sql = format!("SELECT count(*) FROM users WHERE name IN ({})", placeholders(0, 2));
	let rows = source
		.rows(&sql, &[&names[0], &names[1]])
		.await
		.expect("the query runs");

	assert_eq!(rows[0].get::<_, i64>(0), 2);
}

#[tokio::test]
async fn streams_while_looking_up() {
	let db = TempDb::new("stream", SCHEMA);
	let source = Source::connect(db.url()).await.expect("opens");

	let mut stream = source
		.stream("SELECT name FROM users ORDER BY name", &[])
		.await
		.expect("the stream opens");

	let mut names = Vec::new();
	while let Some(row) = stream.try_next().await.expect("a row") {
		assert_eq!(source.count("users").await.expect("a lookup meanwhile"), 3);
		names.push(row.get::<_, String>(0));
	}

	assert_eq!(names, ["@a:x", "@b:x", "@c:x"]);
}

#[tokio::test]
async fn stream_reports_a_failing_query() {
	let db = TempDb::new("fails", SCHEMA);
	let source = Source::connect(db.url()).await.expect("opens");

	let mut stream = source
		.stream("SELECT nothing FROM nowhere", &[])
		.await
		.expect("the stream opens");

	let error = stream.try_next().await.expect_err("the query fails");
	assert!(error.to_string().contains("nowhere"), "{error}");
}

#[tokio::test]
async fn finds_tables_and_columns() {
	let db = TempDb::new("schema", SCHEMA);
	let source = Source::connect(db.url()).await.expect("opens");

	assert!(source.has_table("users").await.expect("checks"));
	assert!(!source.has_table("rooms").await.expect("checks"));
	assert!(source.has_column("users", "locked").await.expect("checks"));
	assert!(!source.has_column("users", "suspended").await.expect("checks"));
	assert_eq!(
		source
			.column_or("users", "suspended", "FALSE")
			.await
			.expect("checks"),
		"FALSE"
	);
}

#[tokio::test]
async fn md5_matches_postgres() {
	let db = TempDb::new("md5", SCHEMA);
	let source = Source::connect(db.url()).await.expect("opens");

	let rows = source
		.rows("SELECT md5('abc'), md5(NULL)", &[])
		.await
		.expect("the query runs");

	// PostgreSQL's md5('abc').
	assert_eq!(rows[0].get::<_, &str>(0), "900150983cd24fb0d6963f7d28e17f72");
	assert_eq!(rows[0].get::<_, Option<&str>>(1), None);
}

#[tokio::test]
async fn never_writes() {
	let db = TempDb::new("readonly", SCHEMA);
	let source = Source::connect(db.url()).await.expect("opens");

	source
		.rows("DELETE FROM users", &[])
		.await
		.expect_err("the database is read-only");

	assert_eq!(source.count("users").await.expect("counts"), 3);
	assert_eq!(source.other_clients().await.expect("checks"), None);
}

#[tokio::test]
async fn refuses_a_database_with_unwritten_changes() {
	let db = TempDb::new("journal", SCHEMA);
	let journal = PathBuf::from(format!("{}-wal", db.url()));
	std::fs::write(&journal, b"frames").expect("a journal");

	let source = Source::connect(db.url()).await.expect("opens");
	let found = source.other_clients().await.expect("checks");
	std::fs::remove_file(&journal).ok();

	assert!(found.is_some_and(|found| found.contains("-wal")));
}

#[tokio::test]
async fn refuses_a_file_that_isnt_sqlite() {
	let path = std::env::temp_dir().join(format!(
		"tuwunel-synapse-import-notsqlite-{}.db",
		std::process::id()
	));
	std::fs::write(&path, b"homeserver.yaml, not a database, but long enough to be a header")
		.expect("a file");

	let result = Source::connect(path.to_str().expect("a UTF-8 path")).await;
	std::fs::remove_file(&path).ok();

	assert!(result.is_err());
}

#[tokio::test]
async fn mas_needs_postgres() {
	let db = TempDb::new("mas", SCHEMA);

	assert!(Source::connect_postgres(db.url()).await.is_err());
}
