//! What a verification found: per-domain counts, signature outcomes and the
//! mismatches, as JSON and as a human-readable summary.

use std::{
	collections::{BTreeMap, BTreeSet},
	fmt::{Display, Write as _},
	path::Path,
};

use serde::Serialize;
use tuwunel_core::{Result, err, info, warn};

/// Mismatches and excuses kept in full; the rest are only counted.
const KEEP: usize = 1000;

/// Mismatches the summary lists.
const SHOWN: usize = 50;

/// One domain's counts.
///
/// `synapse` and `tuwunel` count items seen on each side; `mismatched` counts
/// mismatches, which for a count comparison covers a group of items.
/// `excused` items are ones the import reports as not imported.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct Tally {
	pub synapse: u64,
	pub tuwunel: u64,
	pub matched: u64,
	pub excused: u64,
	pub mismatched: u64,
}

#[derive(Debug, Serialize)]
pub struct Mismatch {
	pub domain: &'static str,
	pub kind: &'static str,
	pub subject: String,
	pub detail: String,
}

/// Re-verification of sampled events' hashes and signatures.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct Signatures {
	pub verified: u64,
	/// Signatures valid, content hash not: a redacted event.
	pub redacted: u64,
	/// Signed by a server whose key neither database holds.
	pub unverifiable: u64,
	pub failed: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct Verification {
	pub sample: usize,
	pub seed: String,
	pub rooms: u64,
	pub events_sampled: u64,
	pub domains: BTreeMap<&'static str, Tally>,
	pub signatures: Signatures,
	pub unverifiable_servers: BTreeSet<String>,
	pub mismatches_total: u64,
	pub mismatches: Vec<Mismatch>,
	pub excused_total: u64,
	pub excused: Vec<String>,
}

fn inc(counter: &mut u64) { *counter = counter.saturating_add(1); }

impl Verification {
	pub(super) fn new(sample: usize, seed: &str) -> Self {
		Self {
			sample,
			seed: seed.to_owned(),
			..Self::default()
		}
	}

	#[must_use]
	pub fn passed(&self) -> bool { self.mismatches_total == 0 }

	pub(super) fn tally(&mut self, domain: &'static str) -> &mut Tally {
		self.domains.entry(domain).or_default()
	}

	pub(super) fn matched(&mut self, domain: &'static str) {
		let tally = self.tally(domain);
		inc(&mut tally.synapse);
		inc(&mut tally.tuwunel);
		inc(&mut tally.matched);
	}

	/// On both sides, but not the same.
	pub(super) fn differs<S: Display, D: Display>(
		&mut self,
		domain: &'static str,
		subject: S,
		detail: D,
	) {
		let tally = self.tally(domain);
		inc(&mut tally.synapse);
		inc(&mut tally.tuwunel);
		self.push(domain, "differs", subject, detail);
	}

	pub(super) fn missing<S: Display, D: Display>(
		&mut self,
		domain: &'static str,
		subject: S,
		detail: D,
	) {
		inc(&mut self.tally(domain).synapse);
		self.push(domain, "missing", subject, detail);
	}

	pub(super) fn extra<S: Display, D: Display>(
		&mut self,
		domain: &'static str,
		subject: S,
		detail: D,
	) {
		inc(&mut self.tally(domain).tuwunel);
		self.push(domain, "extra", subject, detail);
	}

	/// In Synapse, but reported by the import as not imported.
	pub(super) fn excuse<S: Display, W: Display>(
		&mut self,
		domain: &'static str,
		subject: S,
		why: W,
	) {
		let tally = self.tally(domain);
		inc(&mut tally.synapse);
		inc(&mut tally.excused);
		inc(&mut self.excused_total);
		if self.excused.len() < KEEP {
			self.excused
				.push(format!("{domain}: {subject}: {why}"));
		}
	}

	/// One group's items, compared by count.
	pub(super) fn count<S: Display>(
		&mut self,
		domain: &'static str,
		subject: S,
		synapse: u64,
		tuwunel: u64,
	) {
		let tally = self.tally(domain);
		tally.synapse = tally.synapse.saturating_add(synapse);
		tally.tuwunel = tally.tuwunel.saturating_add(tuwunel);
		if synapse == tuwunel {
			tally.matched = tally.matched.saturating_add(synapse);
		} else {
			self.push(domain, "count", subject, format!("Synapse {synapse}, tuwunel {tuwunel}"));
		}
	}

	fn push<S: Display, D: Display>(
		&mut self,
		domain: &'static str,
		kind: &'static str,
		subject: S,
		detail: D,
	) {
		inc(&mut self.tally(domain).mismatched);
		inc(&mut self.mismatches_total);
		if self.mismatches.len() < KEEP {
			self.mismatches.push(Mismatch {
				domain,
				kind,
				subject: subject.to_string(),
				detail: detail.to_string(),
			});
		}
	}

	/// Writes the JSON to `path` and the summary beside it, as `.txt`.
	pub fn write(&self, path: &Path) -> Result {
		let json = serde_json::to_vec_pretty(self).expect("the verification serializes");
		let cannot =
			|e: std::io::Error| err!("Cannot write the verification to {}: {e}", path.display());

		std::fs::write(path, json).map_err(cannot)?;
		std::fs::write(path.with_extension("txt"), self.summary()).map_err(cannot)
	}

	pub fn log(&self) {
		for line in self.summary().lines() {
			if self.passed() {
				info!("{line}");
			} else {
				warn!("{line}");
			}
		}
	}

	#[must_use]
	pub fn summary(&self) -> String {
		let mut out = String::new();
		let sample = match self.sample {
			| 0 => "every event".to_owned(),
			| n => format!("{n} events per room (seed {:?})", self.seed),
		};

		_ = writeln!(
			out,
			"Synapse import verification: {} rooms, deep checks on {sample}",
			self.rooms
		);
		_ = writeln!(
			out,
			"{:<16}{:>10}{:>10}{:>10}{:>10}{:>11}",
			"domain", "synapse", "tuwunel", "matched", "excused", "mismatched"
		);
		for (domain, t) in &self.domains {
			_ = writeln!(
				out,
				"{domain:<16}{:>10}{:>10}{:>10}{:>10}{:>11}",
				t.synapse, t.tuwunel, t.matched, t.excused, t.mismatched
			);
		}

		let s = &self.signatures;
		_ = writeln!(
			out,
			"signatures: {} sampled events, {} verified, {} redacted, {} unverifiable, {} failed",
			self.events_sampled, s.verified, s.redacted, s.unverifiable, s.failed
		);
		if !self.unverifiable_servers.is_empty() {
			let servers: Vec<_> = self
				.unverifiable_servers
				.iter()
				.map(String::as_str)
				.collect();
			_ = writeln!(out, "no signing keys held for: {}", servers.join(", "));
		}

		if self.excused_total > 0 {
			_ = writeln!(
				out,
				"{} items excused: the import reports them as not imported",
				self.excused_total
			);
		}

		for m in self.mismatches.iter().take(SHOWN) {
			_ = writeln!(out, "MISMATCH {} {}: {}: {}", m.domain, m.kind, m.subject, m.detail);
		}

		let hidden = self
			.mismatches_total
			.saturating_sub(SHOWN.try_into().unwrap_or(u64::MAX));
		if hidden > 0 {
			_ = writeln!(out, "... and {hidden} more mismatches");
		}

		_ = match self.mismatches_total {
			| 0 => writeln!(out, "Verification passed: no mismatches"),
			| n => writeln!(out, "Verification FAILED: {n} mismatches"),
		};

		out
	}
}
