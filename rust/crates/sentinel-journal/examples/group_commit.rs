//! Measure what a record costs at each batch size.
//!
//! The performance claim of this rewrite is one sentence: framing a record is
//! cheap and making it durable is not, so the only question is how many records
//! share one `fsync` — and the answer improves as load rises. This prints the
//! evidence rather than leaving it as an argument.
//!
//! ```text
//! cargo run --release -p sentinel-journal --example group_commit
//! ```
//!
//! Run it on the deploy target before quoting the numbers anywhere. macOS APFS
//! issues a full drive barrier on `sync_data`; Linux `fdatasync` on NVMe is one
//! to two orders of magnitude cheaper, so a laptop figure is a floor and not a
//! prediction.

// The workspace forbids floating point because a float in a money type is the
// bug it exists to prevent. This file reports a duration to a human and touches
// no money, which is the one place the rule does not apply.
#![expect(
    clippy::float_arithmetic,
    clippy::cast_precision_loss,
    reason = "a measurement printed for a person to read, never an input to a decision"
)]

use std::time::Instant;

use sentinel_journal::{Journal, RecordKind};
use sentinel_types::Nanos;

/// A payload the size of a real order event.
const PAYLOAD: &[u8; 96] = &[0x5A; 96];
/// Records per measurement, so every row does the same total work.
const RECORDS: u64 = 2_048;

fn main() {
    let root = std::env::temp_dir().join(format!("sentinel-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);

    println!("{RECORDS} records of {} bytes each\n", PAYLOAD.len());
    println!(
        "{:>18} │ {:>12} │ {:>12} │ {:>10}",
        "records per fsync", "µs / record", "records / s", "fsyncs"
    );
    println!("{:─>19}┼{:─>14}┼{:─>14}┼{:─>11}", "", "", "", "");

    for batch in [1u64, 2, 4, 8, 16, 32, 64, 128] {
        let dir = root.join(format!("batch-{batch}"));
        let mut journal = Journal::claim(&dir, Nanos::ZERO).expect("claim");

        let start = Instant::now();
        for i in 0..RECORDS {
            journal
                .append(RecordKind::OrderEvent, Nanos::from_u64(i), PAYLOAD)
                .expect("append");
            if (i + 1) % batch == 0 {
                journal.flush().expect("flush");
            }
        }
        journal.flush().expect("final flush");
        let elapsed = start.elapsed();

        let stats = journal.stats();
        let per_record_us = elapsed.as_secs_f64() * 1e6 / RECORDS as f64;
        let per_second = RECORDS as f64 / elapsed.as_secs_f64();
        println!(
            "{batch:>18} │ {per_record_us:>12.1} │ {per_second:>12.0} │ {:>10}",
            stats.flushes
        );

        drop(journal);
        let _ = std::fs::remove_dir_all(&dir);
    }

    let _ = std::fs::remove_dir_all(&root);
    println!(
        "\nFor comparison, the Python system it replaces measured p50 place_ms\n\
         of 10,272 — three Postgres transactions per placement, over a proxy."
    );
}
