//! The on-disk format, pinned — framing and record layout together.
//!
//! `testdata/journal-golden/` at the repository root is a journal written by
//! this code and read by both this crate and `go/internal/record`. It is the
//! system of record's format, so a change to it reinterprets every log already
//! written: the test fails, and the only way past is `SENTINEL_REGOLD=1`, which
//! puts the byte diff in the review where it belongs.
//!
//! It lives here rather than in `sentinel-journal` because a fixture of opaque
//! bytes would pin the frame header and nothing else — and the layouts most
//! likely to drift are the record payloads.
//!
//! ```text
//! SENTINEL_REGOLD=1 cargo test -p sentinel-record --test golden
//! ```

use std::path::PathBuf;

use sentinel_domain::{EconomicOrderIntent, OrderEvent, OrderState, ReconcileCause, RejectReason};
use sentinel_journal::{Journal, Seq, Tailer};
use sentinel_record::{Actor, Asset, DecisionKind, Note, Record};
use sentinel_types::{
    Authority, ClientOrderId, ExecId, Instrument, Money, Nanos, OrderKind, Price, Qty, Side,
    TraceId, VenueOrderId,
};

/// One of everything, with every field set to something a venue would send.
///
/// Deliberately not generated: a fixture built from a loop over the variants
/// would change shape whenever the variants did, which is the opposite of what
/// pinning means.
fn fixture_records() -> Vec<Record> {
    let coid = ClientOrderId::new("UI-4bb5ba826e2e1").unwrap();
    let btc = Instrument::new("BTCUSD").unwrap();
    let trace = TraceId::from_u128(0x6440_ba96);

    let event = |from: OrderState, to: OrderState, event: OrderEvent| Record::OrderEvent {
        client_order_id: coid,
        trace_id: trace,
        from_state: from,
        to_state: to,
        applied: true,
        event,
    };

    // The same shape with the flag cleared, so the fixture pins both readings.
    // Without this the contract would only ever be exercised one way round,
    // and the case that was wrong is the one where the state does not move.
    let unapplied = |state: OrderState, event: OrderEvent| Record::OrderEvent {
        client_order_id: coid,
        trace_id: trace,
        from_state: state,
        to_state: state,
        applied: false,
        event,
    };

    vec![
        Record::Ticked,
        Record::IntentPersisted(
            EconomicOrderIntent::new(
                coid,
                btc,
                Side::Buy,
                Qty::parse("0.003").unwrap(),
                OrderKind::Limit,
                Some(Price::parse("109432.5").unwrap()),
                None,
                Authority::Entry,
                trace,
                Some(Price::parse("109430").unwrap()),
            )
            .unwrap(),
        ),
        event(
            OrderState::Created,
            OrderState::Submitting,
            OrderEvent::SubmissionStarted,
        ),
        event(
            OrderState::Submitting,
            OrderState::Unknown,
            OrderEvent::SubmissionTimedOut,
        ),
        event(
            OrderState::Unknown,
            OrderState::Reconciling,
            OrderEvent::ReconcileStarted {
                cause: ReconcileCause::SubmissionTimeout,
            },
        ),
        event(
            OrderState::Reconciling,
            OrderState::Working,
            OrderEvent::ReconcileResolved {
                state: OrderState::Working,
                venue_order_id: Some(VenueOrderId::new("V-77").unwrap()),
                filled_qty: Qty::ZERO,
            },
        ),
        event(
            OrderState::Working,
            OrderState::Partial,
            OrderEvent::FillReceived {
                exec_id: ExecId::new("E-9911").unwrap(),
                qty: Qty::parse("0.002").unwrap(),
                price: Price::parse("109432.5").unwrap(),
            },
        ),
        event(
            OrderState::Partial,
            OrderState::CancelPending,
            OrderEvent::CancelRequested,
        ),
        event(
            OrderState::CancelPending,
            OrderState::Canceled,
            OrderEvent::CancelConfirmed,
        ),
        // Evidence that could not be applied: from and to are the same.
        // A fill for an order believed canceled: recorded, applied to nothing.
        unapplied(
            OrderState::Canceled,
            OrderEvent::FillReceived {
                exec_id: ExecId::new("E-9912").unwrap(),
                qty: Qty::parse("0.001").unwrap(),
                price: Price::parse("109440").unwrap(),
            },
        ),
        event(
            OrderState::Submitting,
            OrderState::Rejected,
            OrderEvent::VenueRejected {
                reason: RejectReason::new(-2019, "insufficient_margin"),
            },
        ),
        event(
            OrderState::Submitting,
            OrderState::Working,
            OrderEvent::VenueAcked {
                venue_order_id: VenueOrderId::new("V-553664").unwrap(),
            },
        ),
        Record::Decision {
            instrument: btc,
            actor: Actor::Guards,
            kind: DecisionKind::EntryBlocked,
            trace_id: trace,
            value: Money::parse("-118.81").unwrap(),
            note: Note::new("instrument locked by UI-4bb5ba826e2e1").unwrap(),
        },
        // A fill booked while reconciling: applied, and the state does not
        // move. This is the record whose meaning a consumer cannot infer.
        event(
            OrderState::Reconciling,
            OrderState::Reconciling,
            OrderEvent::FillReceived {
                exec_id: ExecId::new("RCN-UI-4bb5ba826e2e1:3").unwrap(),
                qty: Qty::parse("3").unwrap(),
                price: Price::parse("109432.5").unwrap(),
            },
        ),
        // And one recorded as evidence only, identical but for the flag.
        unapplied(
            OrderState::Filled,
            OrderEvent::FillReceived {
                exec_id: ExecId::new("E-late-9931").unwrap(),
                qty: Qty::parse("1").unwrap(),
                price: Price::parse("109432.5").unwrap(),
            },
        ),
        Record::Mark {
            instrument: btc,
            price: Price::parse("109432.5").unwrap(),
        },
        Record::Balance {
            asset: Asset::new("USD").unwrap(),
            amount: Money::parse("5942.65").unwrap(),
        },
    ]
}

/// Every record is stamped a tenth of a second after the last, so the times in
/// the fixture are as pinned as the payloads.
fn write_fixture(dir: &std::path::Path) {
    let mut journal = Journal::claim(dir, Nanos::from_u64(1_787_761_830_000_000_000)).unwrap();
    let mut clock = 1_787_761_830_000_000_000u64;
    for record in fixture_records() {
        clock += 100_000_000;
        journal
            .append(record.kind(), Nanos::from_u64(clock), &record.to_bytes())
            .unwrap();
    }
    journal.flush().unwrap();
}

fn golden_dir() -> PathBuf {
    let relative =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../testdata/journal-golden");
    relative.canonicalize().unwrap_or(relative)
}

/// Regenerate at most once per process: tests in a file run in parallel, and
/// two of them racing to rewrite one directory produce a mixture of two runs.
fn ensure_golden() -> PathBuf {
    static ONCE: std::sync::Once = std::sync::Once::new();
    let golden = golden_dir();
    ONCE.call_once(|| {
        if std::env::var_os("SENTINEL_REGOLD").is_some() {
            let _ = std::fs::remove_dir_all(&golden);
            write_fixture(&golden);
            // Runtime artefacts, not part of the format.
            let _ = std::fs::remove_file(golden.join("LOCK"));
            let _ = std::fs::remove_file(golden.join("control.sock"));
            eprintln!("regenerated {}", golden.display());
        }
    });
    golden
}

#[test]
fn the_on_disk_format_is_pinned() {
    let golden = ensure_golden();
    assert!(
        golden.join("0000000000000001.jrnl").exists(),
        "golden journal missing at {}; run with SENTINEL_REGOLD=1",
        golden.display()
    );

    let scratch = std::env::temp_dir().join(format!("sentinel-golden-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);
    write_fixture(&scratch);

    let fresh = std::fs::read(scratch.join("0000000000000001.jrnl")).unwrap();
    let pinned = std::fs::read(golden.join("0000000000000001.jrnl")).unwrap();
    let _ = std::fs::remove_dir_all(&scratch);

    assert_eq!(
        fresh.len(),
        pinned.len(),
        "the segment changed size — the format moved"
    );
    if fresh != pinned {
        let at = fresh
            .iter()
            .zip(&pinned)
            .position(|(a, b)| a != b)
            .unwrap_or(0);
        panic!(
            "byte {at} differs: wrote {:#04x}, pinned {:#04x}. \
             If this is intended, SENTINEL_REGOLD=1 and review the diff.",
            fresh[at], pinned[at]
        );
    }
}

#[test]
fn every_record_reads_back_as_what_wrote_it() {
    let golden = ensure_golden();
    let mut tailer = Tailer::open(&golden, Seq::ZERO).unwrap();
    let mut read = Vec::new();
    tailer.drain_into(&mut read).unwrap();

    let expected = fixture_records();
    assert_eq!(read.len(), expected.len() + 1, "the claim plus the fixture");
    assert_eq!(
        read[0].header.kind,
        sentinel_journal::RecordKind::EpochClaimed
    );

    for (i, (want, got)) in expected.iter().zip(&read[1..]).enumerate() {
        let decoded = Record::decode(got.header.kind, &got.payload)
            .unwrap_or_else(|e| panic!("record {i} failed to decode: {e}"));
        assert_eq!(decoded, *want, "record {i}");
        assert_eq!(got.header.seq, Seq::from_u64(i as u64 + 2));
        assert_eq!(got.header.epoch.as_u64(), 1);
    }
}

#[test]
fn the_fixture_covers_every_record_kind() {
    // A fixture that quietly stopped exercising a kind would pin nothing about
    // it, and that is exactly the kind that would drift.
    let kinds: std::collections::BTreeSet<_> =
        fixture_records().iter().map(|r| r.kind().as_u8()).collect();
    for kind in sentinel_journal::RecordKind::ALL {
        if *kind == sentinel_journal::RecordKind::EpochClaimed {
            continue; // written by the claim itself, not by the fixture
        }
        assert!(
            kinds.contains(&kind.as_u8()),
            "{kind} is not in the fixture"
        );
    }
}

#[test]
fn the_fixture_covers_every_event_kind() {
    let kinds: std::collections::BTreeSet<_> = fixture_records()
        .iter()
        .filter_map(|r| match r {
            Record::OrderEvent { event, .. } => Some(event.kind().as_u8()),
            _ => None,
        })
        .collect();
    for kind in sentinel_domain::EventKind::ALL {
        assert!(
            kinds.contains(&kind.as_u8()),
            "{kind} is not in the fixture"
        );
    }
}
