use ruma::RoomVersionId;

use super::room_version;

#[test]
fn room_version_known() {
	assert_eq!(room_version(Some("10")).ok(), Some(RoomVersionId::V10));
}

#[test]
fn room_version_missing_is_an_error() {
	assert!(room_version(None).is_err());
}

#[test]
fn room_version_unsupported_is_an_error() {
	assert!(room_version(Some("org.matrix.msc1767.10")).is_err());
	assert!(room_version(Some("")).is_err());
}
