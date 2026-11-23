//! A result row both backends produce, so the phases read PostgreSQL and
//! SQLite rows alike.
//!
//! SQLite has no boolean type and stores Synapse's booleans as integers, and
//! PostgreSQL's integers come in three widths, so reads convert between them;
//! anything else that doesn't fit is a bug in the query and panics, as
//! `tokio_postgres::Row::get` does.

use std::fmt;

/// One column's value.
#[derive(Clone, PartialEq)]
pub(crate) enum Value {
	Null,
	Bool(bool),
	Int(i64),
	Real(f64),
	Text(String),
	Bytes(Vec<u8>),
	TextArray(Vec<String>),
}

/// Leaves out text and bytes, which may be secrets (tokens, password hashes).
impl fmt::Debug for Value {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			| Self::Null => f.write_str("NULL"),
			| Self::Bool(value) => write!(f, "boolean {value}"),
			| Self::Int(value) => write!(f, "integer {value}"),
			| Self::Real(value) => write!(f, "real {value}"),
			| Self::Text(text) => write!(f, "text of {} bytes", text.len()),
			| Self::Bytes(bytes) => write!(f, "{} bytes", bytes.len()),
			| Self::TextArray(texts) => write!(f, "array of {} texts", texts.len()),
		}
	}
}

#[derive(Debug, Default)]
pub(crate) struct Row {
	values: Vec<Value>,
}

/// A column position. Only `usize`, so an integer literal infers to it.
pub(crate) trait RowIndex {
	fn index(&self) -> usize;
}

impl RowIndex for usize {
	fn index(&self) -> usize { *self }
}

impl Row {
	pub(crate) fn new(values: Vec<Value>) -> Self { Self { values } }

	/// The value at `idx` as a `T`, like `tokio_postgres::Row::get`.
	///
	/// # Panics
	/// When there is no such column or its value isn't a `T`.
	#[must_use]
	pub(crate) fn get<'a, I: RowIndex + Copy, T: FromValue<'a>>(&'a self, idx: I) -> T {
		let idx = idx.index();
		let value = self
			.values
			.get(idx)
			.unwrap_or_else(|| panic!("no column {idx} in a row of {}", self.values.len()));

		T::from_value(value).unwrap_or_else(|| {
			panic!("column {idx}: {value:?} can't be read as {}", std::any::type_name::<T>())
		})
	}
}

/// What a column's value can be read as.
pub(crate) trait FromValue<'a>: Sized {
	fn from_value(value: &'a Value) -> Option<Self>;
}

impl<'a, T: FromValue<'a>> FromValue<'a> for Option<T> {
	fn from_value(value: &'a Value) -> Option<Self> {
		match value {
			| Value::Null => Some(None),
			| value => T::from_value(value).map(Some),
		}
	}
}

impl<'a> FromValue<'a> for &'a str {
	fn from_value(value: &'a Value) -> Option<Self> {
		match value {
			| Value::Text(text) => Some(text),
			| Value::Bytes(bytes) => std::str::from_utf8(bytes).ok(),
			| _ => None,
		}
	}
}

impl FromValue<'_> for String {
	fn from_value(value: &Value) -> Option<Self> { <&str>::from_value(value).map(Into::into) }
}

/// Text too: PostgreSQL's JSON as text, SQLite's JSON as whatever it was
/// given.
impl<'a> FromValue<'a> for &'a [u8] {
	fn from_value(value: &'a Value) -> Option<Self> {
		match value {
			| Value::Bytes(bytes) => Some(bytes),
			| Value::Text(text) => Some(text.as_bytes()),
			| _ => None,
		}
	}
}

impl FromValue<'_> for Vec<u8> {
	fn from_value(value: &Value) -> Option<Self> { <&[u8]>::from_value(value).map(Into::into) }
}

impl FromValue<'_> for Vec<String> {
	fn from_value(value: &Value) -> Option<Self> {
		match value {
			| Value::TextArray(texts) => Some(texts.clone()),
			| _ => None,
		}
	}
}

impl FromValue<'_> for bool {
	fn from_value(value: &Value) -> Option<Self> {
		match *value {
			| Value::Bool(value) => Some(value),
			| Value::Int(value) => Some(value != 0),
			| _ => None,
		}
	}
}

impl FromValue<'_> for i64 {
	fn from_value(value: &Value) -> Option<Self> {
		match *value {
			| Value::Int(value) => Some(value),
			| Value::Bool(value) => Some(value.into()),
			| _ => None,
		}
	}
}

impl FromValue<'_> for i32 {
	fn from_value(value: &Value) -> Option<Self> {
		i64::from_value(value).and_then(|value| value.try_into().ok())
	}
}

impl FromValue<'_> for i16 {
	fn from_value(value: &Value) -> Option<Self> {
		i64::from_value(value).and_then(|value| value.try_into().ok())
	}
}

#[cfg(test)]
mod tests {
	use super::{Row, Value};

	#[test]
	fn reads_sqlite_booleans_and_integer_widths() {
		let row = Row::new(vec![Value::Int(1), Value::Int(0), Value::Int(-3), Value::Bool(true)]);

		assert!(row.get::<_, bool>(0));
		assert!(!row.get::<_, bool>(1));
		assert_eq!(row.get::<_, i16>(2), -3);
		assert_eq!(row.get::<_, i64>(3), 1);
	}

	#[test]
	fn reads_null_as_none() {
		let row = Row::new(vec![Value::Null, Value::Text("x".into())]);

		assert_eq!(row.get::<_, Option<&str>>(0), None);
		assert_eq!(row.get::<_, Option<i64>>(0), None);
		assert_eq!(row.get::<_, Option<&str>>(1), Some("x"));
	}

	#[test]
	fn reads_text_as_bytes_and_utf8_bytes_as_text() {
		let row = Row::new(vec![Value::Text("{}".into()), Value::Bytes(b"{}".to_vec())]);

		assert_eq!(row.get::<_, &[u8]>(0), b"{}");
		assert_eq!(row.get::<_, &str>(1), "{}");
	}

	#[test]
	#[should_panic(expected = "column 0")]
	fn refuses_null_where_a_value_is_needed() {
		let row = Row::new(vec![Value::Null]);
		let _value: &str = row.get(0);
	}

	#[test]
	#[should_panic(expected = "can't be read")]
	fn refuses_an_integer_too_wide() {
		let row = Row::new(vec![Value::Int(i64::from(i32::MAX) + 1)]);
		let _value: i32 = row.get(0);
	}
}
