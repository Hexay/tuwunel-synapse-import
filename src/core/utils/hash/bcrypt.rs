use icu_normalizer::ComposingNormalizerBorrowed;

use crate::{Result, err};

/// Whether a stored password hash is bcrypt, as Synapse writes them.
#[must_use]
pub fn is_bcrypt_hash(hash: &str) -> bool {
	hash.len() == 60
		&& ["$2a$", "$2b$", "$2x$", "$2y$"]
			.iter()
			.any(|prefix| hash.starts_with(prefix))
}

/// Verifies a plaintext password against a bcrypt hash written by Synapse.
///
/// Synapse NFKC-normalizes the password and appends its configured pepper
/// before hashing, and bcrypt reads only the first 72 bytes of the result.
/// Malformed hashes and password mismatches return an error.
pub fn verify_bcrypt_password(password: &str, pepper: &str, hash: &str) -> Result {
	let normalized = ComposingNormalizerBorrowed::new_nfkc().normalize(password);
	let peppered = [normalized.as_bytes(), pepper.as_bytes()].concat();

	::bcrypt::verify(peppered, hash)
		.map_err(|e| err!("{e}"))?
		.then_some(())
		.ok_or_else(|| err!("Password does not match the bcrypt hash."))
}

#[cfg(test)]
mod tests {
	use super::{is_bcrypt_hash, verify_bcrypt_password};

	// Fixtures from Python bcrypt 3.2 running Synapse's `_do_hash` at cost 4.
	const TEMP123: &str = "$2b$04$i5HtYTXiQ1Z84wjDcUZ8desol8lW.3WfqH6/m7wM5wkGR1ABftZAy";
	const LIGATURE_PEPPERED: &str =
		"$2b$04$9SMMk5qKoEt.oHskh3mwQuQ0uI/591axMf.QQekk27E9kLsGFfRGG";
	const X80_PEPPERED: &str = "$2b$04$OLl6gVSgULlJ03KbYhRSaeByYYfG0U7sptc9IzSsp5MIJ0SDJrBpO";

	#[test]
	fn recognizes_bcrypt_and_not_argon2() {
		assert!(is_bcrypt_hash(TEMP123));
		assert!(!is_bcrypt_hash("$argon2id$v=19$m=64,t=1,p=1$c2FsdA$aGFzaA"));
		assert!(!is_bcrypt_hash(&TEMP123[..59]));
		assert!(!is_bcrypt_hash(""));
	}

	#[test]
	fn verifies_a_synapse_hash() {
		verify_bcrypt_password("temp123", "", TEMP123).expect("verified");
		verify_bcrypt_password("temp321", "", TEMP123).expect_err("unverified");
	}

	#[test]
	fn applies_nfkc_and_pepper_like_synapse() {
		verify_bcrypt_password("\u{fb01}x\u{0301}", "pepper", LIGATURE_PEPPERED)
			.expect("verified");
		verify_bcrypt_password("fix\u{0301}", "pepper", LIGATURE_PEPPERED)
			.expect("NFKC-equivalent verified");
		verify_bcrypt_password("\u{fb01}x\u{0301}", "", LIGATURE_PEPPERED)
			.expect_err("missing pepper rejected");
	}

	#[test]
	fn ignores_bytes_past_72_like_synapse() {
		let x72 = "x".repeat(72);

		verify_bcrypt_password(&"x".repeat(80), "pepper", X80_PEPPERED).expect("verified");
		verify_bcrypt_password(&x72, "", X80_PEPPERED).expect("truncated input verified");
		verify_bcrypt_password(&"x".repeat(71), "", X80_PEPPERED).expect_err("short rejected");
	}
}
