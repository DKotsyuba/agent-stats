//! A single writer owns the database. Readers use bounded, independent snapshots.
use super::{Error, RETENTION_NS, config, normalize::Batch};
use rusqlite::{Connection, OpenFlags, params};
use std::{
    path::Path,
    time::{Duration, Instant},
};

/// Verify the data directory and initialize the raw sample schema.
pub(super) fn open(path: &Path) -> Result<Connection, Error> {
    let conn = Connection::open_with_flags(
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
         CREATE INDEX IF NOT EXISTS samples_time ON samples(time_ns);",
    )?;
    // Bound retained data volume even when all samples are inside the retention window.
    let page_size: u32 = conn.pragma_query_value(None, "page_size", |row| row.get(0))?;
    conn.pragma_update(None, "max_page_count", (256 * 1024 * 1024) / page_size)?;
    Ok(conn)
}

/// Hold the service lock before opening its private database.
pub(super) fn prepare(directory: &Path) -> Result<(std::fs::File, Connection), Error> {
    config::private_dir(directory)?;
    let lock = config::lock(&directory.join("writer.lock"))?;
    let conn = open(&directory.join("stats.sqlite3"))?;
    Ok((lock, conn))
}

/// Commit the whole batch or roll it back. Return counts only after durable commit.
pub(super) fn insert(conn: &mut Connection, batch: &Batch) -> Result<(usize, usize), Error> {
    let tx = conn.transaction()?;
    let mut inserted = 0;
    {
        let mut statement = tx.prepare_cached(
            "INSERT INTO samples VALUES ('claude-code', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(harness, request_digest) DO NOTHING",
        )?;
        for sample in &batch.samples {
            inserted += statement.execute(params![
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
                sample.duration_ms,
                sample.ttft_ms,
            ])?;
        }
    }
    tx.commit()?;
    Ok((inserted, batch.samples.len() - inserted))
}

/// Expire at most 512 rows, preserving the exact inclusive retention boundary.
pub(super) fn expire(conn: &Connection, now: i64) -> Result<usize, Error> {
    Ok(conn.execute(
        "DELETE FROM samples WHERE rowid IN (
             SELECT rowid FROM samples WHERE time_ns < ? ORDER BY time_ns LIMIT 512
         )",
        [now - RETENTION_NS],
    )?)
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
    use crate::stats::{HOUR_NS, normalize::Sample};

    /// Make one row with a controlled identity and timestamp.
    fn sample(id: u8, time_ns: i64) -> Sample {
        Sample {
            time_ns,
            digest: [id; 32],
            provider: "anthropic".into(),
            model: "fixture".into(),
            effort: None,
            tokens: [None; 6],
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
        let batch = Batch {
            samples: vec![
                sample(1, boundary - 1),
                sample(2, boundary),
                sample(3, boundary + 1),
            ],
            ..Batch::default()
        };
        assert_eq!(insert(&mut conn, &batch).expect("insert"), (3, 0));
        assert_eq!(insert(&mut conn, &batch).expect("replay"), (0, 3));
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
        let first = Batch {
            samples: vec![sample(0, HOUR_NS)],
            ..Batch::default()
        };
        assert_eq!(insert(&mut conn, &first).expect("baseline"), (1, 0));
        let pages: u32 = conn
            .pragma_query_value(None, "page_count", |row| row.get(0))
            .expect("page count");
        conn.pragma_update(None, "max_page_count", pages)
            .expect("set SQLite cap");
        let batch = Batch {
            samples: (1..=255).map(|id| sample(id, HOUR_NS)).collect(),
            ..Batch::default()
        };
        assert!(matches!(insert(&mut conn, &batch), Err(Error("disk_full"))));
        let rows: i64 = conn
            .query_row("SELECT count(*) FROM samples", [], |row| row.get(0))
            .expect("row count");
        assert_eq!(rows, 1);
        conn.pragma_update(None, "max_page_count", pages + 64)
            .expect("relax SQLite cap");
        assert_eq!(insert(&mut conn, &batch).expect("retry"), (255, 0));
    }
}
