//! A single writer owns the database. Readers use bounded, independent snapshots.
use super::{
    Error, RATING_RETENTION_NS, RETENTION_NS, Rating, Reason, Tier,
    codex::{Kind, Pairing},
    config,
    normalize::{Batch, Sample},
};
use rusqlite::{Connection, OpenFlags, params};
use std::{
    path::Path,
    time::{Duration, Instant},
};

/// Verify the data directory and initialize the raw sample schema.
pub(super) fn open(path: &Path) -> Result<Connection, Error> {
    let mut conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .map_err(|_| Error("database_open_failed"))?;
    conn.busy_timeout(Duration::from_millis(100))?;
    conn.execute_batch(
        "PRAGMA journal_mode=DELETE;
         PRAGMA secure_delete=ON;
         CREATE TABLE IF NOT EXISTS samples (
             harness TEXT NOT NULL,
             request_digest BLOB NOT NULL,
             time_ns INTEGER NOT NULL,
             provider TEXT NOT NULL,
             model TEXT NOT NULL,
             effort TEXT,
             input_tokens INTEGER,
             output_tokens INTEGER,
             cache_read_tokens INTEGER,
             cache_creation_tokens INTEGER,
             cache_creation_5m_tokens INTEGER,
             cache_creation_1h_tokens INTEGER,
             duration_ms REAL,
             ttft_ms REAL,
             UNIQUE(harness, request_digest)
         ) STRICT;
         CREATE INDEX IF NOT EXISTS samples_time ON samples(time_ns);
         CREATE TABLE IF NOT EXISTS ratings (
             agent_id TEXT PRIMARY KEY,
             model TEXT NOT NULL,
             provider TEXT,
             effort TEXT,
             tier TEXT CHECK (tier IN ('S', 'A', 'B', 'C', 'D', 'E', 'F')),
             verdict TEXT NOT NULL
                 CHECK (verdict IN ('accepted', 'accepted_after_fixes', 'rejected')),
             rounds INTEGER NOT NULL CHECK (rounds >= 0),
             reason TEXT,
             checks_passed INTEGER,
             checks_failed INTEGER,
             checks_not_run INTEGER,
             note TEXT,
             created_ns INTEGER NOT NULL,
             updated_ns INTEGER NOT NULL
         ) STRICT;
         CREATE INDEX IF NOT EXISTS ratings_updated ON ratings(updated_ns);",
    )?;
    migrate(&mut conn)?;
    // Bound retained data volume even when all samples are inside the retention window.
    let page_size: u32 = conn.pragma_query_value(None, "page_size", |row| row.get(0))?;
    conn.pragma_update(None, "max_page_count", (256 * 1024 * 1024) / page_size)?;
    Ok(conn)
}

/// Upgrade the original schema atomically; legacy rows keep exact duration provenance.
fn migrate(conn: &mut Connection) -> Result<(), Error> {
    let tx = conn.transaction()?;
    let has_provenance: bool = tx.query_row(
        "SELECT EXISTS (SELECT 1 FROM pragma_table_info('samples') WHERE name = 'duration_source')",
        [],
        |row| row.get(0),
    )?;
    if !has_provenance {
        tx.execute_batch(
            "ALTER TABLE samples ADD COLUMN duration_source TEXT DEFAULT 'exact'
                CHECK (duration_source IN ('exact', 'estimated'));
             ALTER TABLE samples ADD COLUMN reasoning_tokens INTEGER;",
        )?;
    }
    tx.commit()?;
    Ok(())
}

/// Hold the service lock before opening its private database.
pub(super) fn prepare(directory: &Path) -> Result<(std::fs::File, Connection), Error> {
    config::private_dir(directory)?;
    let lock = config::lock(&directory.join("writer.lock"))?;
    let conn = open(&directory.join("stats.sqlite3"))?;
    Ok((lock, conn))
}

/// Commit the whole batch before publishing pairing state or returning row counts.
pub(super) fn insert(
    conn: &mut Connection,
    pairing: &mut Pairing,
    batch: &mut Batch,
) -> Result<(usize, usize), Error> {
    let mut staged = (!batch.codex.is_empty()).then(|| pairing.clone());
    let tx = conn.transaction()?;
    let mut inserted = 0;
    let total = batch.sample_count();
    {
        let mut statement = tx.prepare_cached(
            "INSERT INTO samples (
                harness, request_digest, time_ns, provider, model, effort, input_tokens,
                output_tokens, cache_read_tokens, cache_creation_tokens, cache_creation_5m_tokens,
                cache_creation_1h_tokens, duration_ms, ttft_ms, duration_source, reasoning_tokens
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(harness, request_digest) DO NOTHING",
        )?;
        let mut insert_sample = |sample: &Sample, duration_ms, source| {
            statement.execute(params![
                sample.harness,
                sample.digest.as_slice(),
                sample.time_ns,
                sample.provider,
                sample.model,
                sample.effort,
                sample.tokens[0],
                sample.tokens[1],
                sample.tokens[2],
                sample.tokens[3],
                sample.tokens[4],
                sample.tokens[5],
                duration_ms,
                sample.ttft_ms,
                source,
                sample.reasoning_tokens,
            ])
        };
        for sample in &batch.samples {
            inserted += insert_sample(sample, sample.duration_ms, Some("exact"))?;
        }
        if let Some(state) = &mut staged {
            let now = Instant::now();
            state.expire(now);
            batch.codex.sort_by_key(|event| event.time_ns);
            for event in &batch.codex {
                match &event.kind {
                    Kind::Request(model) => state.request(event, model, now),
                    Kind::Completed(sample) => {
                        let duration = state.duration(event, &sample.model, now);
                        let added = insert_sample(sample, duration, duration.map(|_| "estimated"))?;
                        state.complete(event, now);
                        inserted += added;
                    }
                }
            }
        }
    }
    tx.commit()?;
    if let Some(staged) = staged {
        *pairing = staged;
    }
    Ok((inserted, total - inserted))
}

/// Record a rating, replacing any earlier one for the same Run id and keeping its first-rated time.
/// Returns whether an earlier rating existed.
pub(super) fn rate(conn: &mut Connection, rating: &Rating, now: i64) -> Result<bool, Error> {
    rating.validate()?;
    let tx = conn.transaction()?;
    let replaced: bool = tx.query_row(
        "SELECT EXISTS (SELECT 1 FROM ratings WHERE agent_id = ?)",
        [&rating.agent_id],
        |row| row.get(0),
    )?;
    tx.execute(
        "INSERT INTO ratings (agent_id, model, provider, effort, tier, verdict, rounds, reason,
             checks_passed, checks_failed, checks_not_run, note, created_ns, updated_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?13)
         ON CONFLICT(agent_id) DO UPDATE SET
             model = excluded.model, provider = excluded.provider, effort = excluded.effort,
             tier = excluded.tier, verdict = excluded.verdict, rounds = excluded.rounds,
             reason = excluded.reason, checks_passed = excluded.checks_passed,
             checks_failed = excluded.checks_failed, checks_not_run = excluded.checks_not_run,
             note = excluded.note, updated_ns = max(excluded.updated_ns, ratings.updated_ns)",
        params![
            rating.agent_id,
            rating.model,
            rating.provider,
            rating.effort,
            rating.tier.map(Tier::as_str),
            rating.verdict.as_str(),
            rating.effective_rounds(),
            rating.reason.map(Reason::as_str),
            rating.checks.map(|checks| checks.passed),
            rating.checks.map(|checks| checks.failed),
            rating.checks.map(|checks| checks.not_run),
            rating.note,
            now,
        ],
    )?;
    tx.commit()?;
    Ok(replaced)
}

/// Expire at most 512 samples and 512 ratings, preserving each exact inclusive retention boundary.
pub(super) fn expire(conn: &Connection, now: i64) -> Result<usize, Error> {
    let samples = conn.execute(
        "DELETE FROM samples WHERE rowid IN (
             SELECT rowid FROM samples WHERE time_ns < ? ORDER BY time_ns LIMIT 512
         )",
        [now - RETENTION_NS],
    )?;
    let ratings = conn.execute(
        "DELETE FROM ratings WHERE rowid IN (
             SELECT rowid FROM ratings WHERE updated_ns < ? ORDER BY updated_ns LIMIT 512
         )",
        [now - RATING_RETENTION_NS],
    )?;
    Ok(samples + ratings)
}

/// Open an isolated read-only connection with a short bound on lock contention.
pub(super) fn reader(path: &Path) -> Result<Connection, Error> {
    let mut conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    conn.busy_timeout(Duration::from_millis(100))?;
    let deadline = Instant::now() + Duration::from_secs(1);
    conn.progress_handler(1000, Some(move || Instant::now() >= deadline))?;
    Ok(conn)
}

impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Self {
        match error.sqlite_error_code() {
            Some(rusqlite::ErrorCode::DiskFull) => Self("disk_full"),
            Some(rusqlite::ErrorCode::OperationInterrupted) => Self("query_timeout"),
            Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked) => {
                Self("database_busy")
            }
            _ => Self("database_failure"),
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    reason = "Test setup failures should include their cause"
)]
mod tests {
    use super::*;
    use crate::stats::{HOUR_NS, Verdict};

    /// Upgrade a populated database from the original schema and reopen it idempotently.
    #[test]
    fn migrate_legacy_samples() {
        // SQLite NOFOLLOW rejects symlinked path components, as in some CI temp roots.
        let dir = tempfile::tempdir_in(std::env::temp_dir().canonicalize().expect("temp root"))
            .expect("temp directory");
        let path = dir.path().join("db");
        let legacy = Connection::open(&path).expect("legacy database");
        legacy
            .execute_batch(
                "CREATE TABLE samples (
                 harness TEXT NOT NULL, request_digest BLOB NOT NULL, time_ns INTEGER NOT NULL,
                 provider TEXT NOT NULL, model TEXT NOT NULL, effort TEXT, input_tokens INTEGER,
                 output_tokens INTEGER, cache_read_tokens INTEGER, cache_creation_tokens INTEGER,
                 cache_creation_5m_tokens INTEGER, cache_creation_1h_tokens INTEGER,
                 duration_ms REAL, ttft_ms REAL, UNIQUE(harness, request_digest)
             ) STRICT;
             INSERT INTO samples VALUES (
                 'claude-code', x'0102', 42, 'anthropic', 'legacy', NULL, 12, 20,
                 NULL, NULL, NULL, NULL, 100.0, 50.0
             );",
            )
            .expect("legacy sample");
        drop(legacy);
        for _ in 0..2 {
            let conn = open(&path).expect("upgrade");
            let row: (i64, f64, String, Option<i64>) = conn
                .query_row(
                    "SELECT output_tokens, duration_ms, duration_source, reasoning_tokens
                 FROM samples WHERE request_digest = x'0102' AND time_ns = 42",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .expect("legacy row");
            assert_eq!(row, (20, 100.0, "exact".into(), None));
            // The ratings table is added alongside the legacy samples, once.
            let tables: i64 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_schema WHERE name = 'ratings'",
                    [],
                    |row| row.get(0),
                )
                .expect("ratings table");
            assert_eq!(tables, 1);
        }
    }

    /// Build a rating that varies only in the verdict and the supplied reason.
    fn rating(verdict: Verdict, reason: Option<Reason>) -> Rating {
        Rating {
            agent_id: "run-1".into(),
            model: "fixture".into(),
            provider: Some("anthropic".into()),
            effort: Some("high".into()),
            tier: Some(Tier::B),
            verdict,
            rounds: None,
            reason,
            checks: None,
            note: Some("first".into()),
        }
    }

    /// A later rating replaces the earlier one in place, keeping the first-rated time.
    #[test]
    fn rating_replaces_per_agent() {
        // SQLite NOFOLLOW rejects symlinked path components, as in some CI temp roots.
        let dir = tempfile::tempdir_in(std::env::temp_dir().canonicalize().expect("temp root"))
            .expect("temp directory");
        let mut conn = open(&dir.path().join("db")).expect("database");
        assert!(
            !rate(
                &mut conn,
                &rating(Verdict::Rejected, Some(Reason::Incomplete)),
                100
            )
            .expect("first")
        );
        let mut second = rating(Verdict::AcceptedAfterFixes, None);
        second.rounds = Some(2);
        second.note = None;
        second.checks = Some(crate::stats::Checks {
            passed: 3,
            failed: 1,
            not_run: 0,
        });
        assert!(rate(&mut conn, &second, 200).expect("replace"));
        // A different Run id is a separate rating.
        let mut other = rating(Verdict::Accepted, None);
        other.agent_id = "run-2".into();
        assert!(!rate(&mut conn, &other, 300).expect("other"));
        let rows: i64 = conn
            .query_row("SELECT count(*) FROM ratings", [], |row| row.get(0))
            .expect("count");
        assert_eq!(rows, 2);
        type Stored = (
            String,
            i64,
            Option<String>,
            Option<String>,
            Option<i64>,
            i64,
            i64,
        );
        let row: Stored = conn
            .query_row(
                "SELECT verdict, rounds, reason, note, checks_failed, created_ns, updated_ns
                 FROM ratings WHERE agent_id = 'run-1'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .expect("replaced row");
        assert_eq!(
            row,
            (
                "accepted_after_fixes".into(),
                2,
                None,
                None,
                Some(1),
                100,
                200
            )
        );
        // Invalid ratings never reach the table.
        let mut invalid = rating(Verdict::Accepted, None);
        invalid.rounds = Some(3);
        assert!(matches!(
            rate(&mut conn, &invalid, 400),
            Err(Error("invalid_rounds"))
        ));
    }

    /// Ratings keep a 90-day inclusive boundary on their last change time, independent of samples.
    #[test]
    fn rating_retention_boundary() {
        // SQLite NOFOLLOW rejects symlinked path components, as in some CI temp roots.
        let dir = tempfile::tempdir_in(std::env::temp_dir().canonicalize().expect("temp root"))
            .expect("temp directory");
        let mut conn = open(&dir.path().join("db")).expect("database");
        let now = RATING_RETENTION_NS + HOUR_NS;
        let boundary = now - RATING_RETENTION_NS;
        for (id, time) in [
            ("old", boundary - 1),
            ("edge", boundary),
            ("new", boundary + 1),
        ] {
            let mut rating = rating(Verdict::Accepted, None);
            rating.agent_id = id.into();
            rate(&mut conn, &rating, time).expect("rate");
        }
        // A 31-day-old sample is gone after the sample window but its rating is untouched.
        assert_eq!(expire(&conn, now).expect("expire"), 1);
        let left: Vec<String> = conn
            .prepare("SELECT agent_id FROM ratings ORDER BY updated_ns")
            .expect("query")
            .query_map([], |row| row.get(0))
            .expect("rows")
            .collect::<Result<_, _>>()
            .expect("ids");
        assert_eq!(left, ["edge", "new"]);
    }

    /// Make one row with a controlled identity and timestamp.
    fn sample(id: u8, time_ns: i64) -> Sample {
        Sample {
            harness: "claude-code",
            time_ns,
            digest: [id; 32],
            provider: "anthropic".into(),
            model: "fixture".into(),
            effort: None,
            tokens: [None; 6],
            reasoning_tokens: None,
            duration_ms: None,
            ttft_ms: None,
        }
    }

    /// Expiry and dedup preserve an event at exactly 30 days and delete older events.
    #[test]
    fn retention_boundary() {
        // SQLite NOFOLLOW rejects symlinked path components, as in some CI temp roots.
        let dir = tempfile::tempdir_in(std::env::temp_dir().canonicalize().expect("temp root"))
            .expect("temp directory");
        let mut conn = open(&dir.path().join("db")).expect("database");
        let now = RETENTION_NS + HOUR_NS;
        let boundary = now - RETENTION_NS;
        let mut batch = Batch {
            samples: vec![
                sample(1, boundary - 1),
                sample(2, boundary),
                sample(3, boundary + 1),
            ],
            ..Batch::default()
        };
        assert_eq!(
            insert(&mut conn, &mut Pairing::default(), &mut batch).expect("insert"),
            (3, 0)
        );
        assert_eq!(
            insert(&mut conn, &mut Pairing::default(), &mut batch).expect("replay"),
            (0, 3)
        );
        assert_eq!(expire(&conn, now).expect("expire"), 1);
        let oldest: i64 = conn
            .query_row("SELECT min(time_ns) FROM samples", [], |row| row.get(0))
            .expect("remaining boundary");
        assert_eq!(oldest, boundary);
    }

    /// Force SQLite's FULL path and verify the whole failed transaction rolls back.
    #[test]
    fn disk_full_rolls_back_batch() {
        // SQLite NOFOLLOW rejects symlinked path components, as in some CI temp roots.
        let dir = tempfile::tempdir_in(std::env::temp_dir().canonicalize().expect("temp root"))
            .expect("temp directory");
        let mut conn = open(&dir.path().join("db")).expect("database");
        let mut first = Batch {
            samples: vec![sample(0, HOUR_NS)],
            ..Batch::default()
        };
        assert_eq!(
            insert(&mut conn, &mut Pairing::default(), &mut first).expect("baseline"),
            (1, 0)
        );
        let pages: u32 = conn
            .pragma_query_value(None, "page_count", |row| row.get(0))
            .expect("page count");
        conn.pragma_update(None, "max_page_count", pages)
            .expect("set SQLite cap");
        let mut batch = Batch {
            samples: (1..=255).map(|id| sample(id, HOUR_NS)).collect(),
            ..Batch::default()
        };
        assert!(matches!(
            insert(&mut conn, &mut Pairing::default(), &mut batch),
            Err(Error("disk_full"))
        ));
        let rows: i64 = conn
            .query_row("SELECT count(*) FROM samples", [], |row| row.get(0))
            .expect("row count");
        assert_eq!(rows, 1);
        conn.pragma_update(None, "max_page_count", pages + 64)
            .expect("relax SQLite cap");
        assert_eq!(
            insert(&mut conn, &mut Pairing::default(), &mut batch).expect("retry"),
            (255, 0)
        );
    }
}
