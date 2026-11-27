//! Agent Stats resident service and read client.
mod api;
mod codex;
mod config;
mod http;
mod normalize;
mod socket;
mod store;

pub(crate) use api::{Bucket, Group, Operation, Window};
use config::Config;
use ring::{digest, hmac};
use serde_json::{Value, json};
pub(crate) use socket::call;
use std::{
    fmt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, mpsc, oneshot},
    task::JoinSet,
};

pub(crate) const SOCKET: &str = config::SOCKET;
const HOUR_NS: i64 = 3_600_000_000_000;
const RETENTION_NS: i64 = 30 * 24 * HOUR_NS;
const MAX_SOCKET_CONNECTIONS: usize = 16;
const HTTP_CONNECTION_HEADROOM: usize = 16;

/// Stable diagnostic code with no request, path or credential data.
#[derive(Debug)]
pub(crate) struct Error(&'static str);

impl Error {
    /// Read the stable code for caller-specific recovery.
    pub(crate) fn code(&self) -> &'static str {
        self.0
    }
}

impl fmt::Display for Error {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(self.0)
    }
}

/// Monotonic, process-local event count.
#[derive(Default)]
struct Count(AtomicU64);

impl Count {
    /// Increment without coordinating unrelated counters.
    fn add(&self, n: usize) {
        self.0.fetch_add(n as u64, Ordering::Relaxed);
    }

    /// Observe the current counter.
    fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Admission, normalization and storage outcomes.
#[derive(Default)]
struct Counters {
    inserted: Count,
    duplicate: Count,
    ignored: Count,
    invalid: Count,
    expired: Count,
    pruned: Count,
    unauthorized: Count,
    socket_denied: Count,
    unsupported: Count,
    malformed: Count,
    oversize: Count,
    overloaded: Count,
    connection_dropped: Count,
    write_failed: Count,
    write_failed_records: Count,
    disk_full: Count,
    retention_failed: Count,
}

impl Counters {
    /// Snapshot independent counters for the versioned status response.
    fn snapshot(&self) -> Value {
        json!({
            "inserted_records": self.inserted.get(), "duplicate_records": self.duplicate.get(),
            "ignored_records": self.ignored.get(), "invalid_records": self.invalid.get(),
            "expired_records": self.expired.get(), "pruned_records": self.pruned.get(),
            "unauthorized_requests": self.unauthorized.get(), "denied_socket_peers": self.socket_denied.get(),
            "unsupported_requests": self.unsupported.get(), "malformed_requests": self.malformed.get(),
            "oversize_requests": self.oversize.get(), "overloaded_requests": self.overloaded.get(),
            "dropped_connections": self.connection_dropped.get(), "failed_write_batches": self.write_failed.get(),
            "failed_write_records": self.write_failed_records.get(), "disk_full_batches": self.disk_full.get(),
            "failed_retention_batches": self.retention_failed.get(),
        })
    }
}

/// Writer messages carry projected rows and a commit receipt, or one bounded expiry pass.
enum Write {
    Batch(normalize::Batch, oneshot::Sender<Result<(), Error>>),
    Expire,
}

/// Service resources shared by admitted connections.
struct State {
    config: Config,
    database: PathBuf,
    token_hash: digest::Digest,
    digest_key: hmac::Key,
    writer: mpsc::Sender<Write>,
    requests: Semaphore,
    reads: Arc<Semaphore>,
    counters: Arc<Counters>,
}

/// Read Unix time in nanoseconds for window and retention decisions.
fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
        .unwrap_or(0)
}

/// Own the SQLite connection until all admitted write messages have completed.
fn writer(mut conn: rusqlite::Connection, mut queue: mpsc::Receiver<Write>, counters: &Counters) {
    let mut pairing = codex::Pairing::default();
    while let Some(message) = queue.blocking_recv() {
        match message {
            Write::Batch(mut batch, reply) => {
                let result = store::insert(&mut conn, &mut pairing, &mut batch).map(
                    |(inserted, duplicate)| {
                        counters.inserted.add(inserted);
                        counters.duplicate.add(duplicate);
                    },
                );
                if let Err(error) = &result {
                    counters.write_failed.add(1);
                    counters.write_failed_records.add(batch.sample_count());
                    if error.0 == "disk_full" {
                        counters.disk_full.add(1);
                    }
                }
                let _ = reply.send(result);
            }
            Write::Expire => {
                pairing.expire(std::time::Instant::now());
                match store::expire(&conn, now_ns()) {
                    Ok(pruned) => counters.pruned.add(pruned),
                    Err(_) => counters.retention_failed.add(1),
                }
            }
        }
    }
}

/// Run the listeners until SIGINT or SIGTERM, then drain admitted writes and release ownership.
pub(crate) async fn serve(path: &Path) -> Result<(), Error> {
    let config = Config::load(path)?;
    let token = config::secret(&config.token_file)?;
    let secret = config::secret(&config.digest_key_file)?;
    if token == secret {
        return Err(Error("credentials_must_differ"));
    }
    let token_hash = digest::digest(&digest::SHA256, &token);
    let digest_key = hmac::Key::new(hmac::HMAC_SHA256, &secret);
    let (_lock, conn) = store::prepare(&config.state_dir)?;
    let listener = TcpListener::bind(config.listen)
        .await
        .map_err(|_| Error("http_bind_failed"))?;
    let (read_listener, _endpoint) = socket::bind(&config.socket).await?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| Error("signal_setup_failed"))?;
    let (send, receive) = mpsc::channel(config.write_backlog);
    let counters = Arc::new(Counters::default());
    let state = Arc::new(State {
        database: config.state_dir.join("stats.sqlite3"),
        requests: Semaphore::new(config.max_requests),
        reads: Arc::new(Semaphore::new(4)),
        config,
        token_hash,
        digest_key,
        writer: send,
        counters: counters.clone(),
    });
    let mut writer_task = tokio::task::spawn_blocking(move || writer(conn, receive, &counters));
    let mut http_tasks = JoinSet::new();
    let mut socket_tasks = JoinSet::new();
    let mut expiry = tokio::time::interval(Duration::from_secs(1));
    expiry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut writer_finished = false;
    let outcome = loop {
        tokio::select! {
            _ = terminate.recv() => break Ok(()),
            signal = tokio::signal::ctrl_c() => break signal.map_err(|_| Error("signal_failed")),
            _ = &mut writer_task => {
                writer_finished = true;
                break Err(Error("writer_stopped"));
            }
            _ = expiry.tick() => { let _ = state.writer.try_send(Write::Expire); }
            _ = http_tasks.join_next(), if !http_tasks.is_empty() => {}
            _ = socket_tasks.join_next(), if !socket_tasks.is_empty() => {}
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { break Err(Error("http_accept_failed")) };
                if http_tasks.len() < state.config.max_requests + HTTP_CONNECTION_HEADROOM {
                    http_tasks.spawn(http::connection(stream, state.clone()));
                } else {
                    state.counters.connection_dropped.add(1);
                }
            }
            accepted = read_listener.accept() => {
                let Ok((stream, _)) = accepted else { break Err(Error("socket_accept_failed")) };
                if !socket::authorized(&stream, &state.config.allowed_uids) {
                    state.counters.socket_denied.add(1);
                } else if socket_tasks.len() >= MAX_SOCKET_CONNECTIONS {
                    state.counters.connection_dropped.add(1);
                } else {
                    socket_tasks.spawn(socket::connection(stream, state.clone()));
                }
            }
        }
    };
    drop(listener);
    drop(read_listener);
    http_tasks.shutdown().await;
    // Reads have bounded frames and work. Join them so none outlive the database lock.
    while socket_tasks.join_next().await.is_some() {}
    drop(state);
    if !writer_finished {
        writer_task.await.map_err(|_| Error("writer_stopped"))?;
    }
    outcome
}
