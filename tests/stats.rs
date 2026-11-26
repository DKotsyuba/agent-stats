//! Black-box v1 service checks using sanitized Claude Code and Codex captures.
#![allow(
    clippy::expect_used,
    reason = "Test setup failures should include their cause"
)]

use serde_json::{Value, json};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    os::unix::{
        fs::{OpenOptionsExt, PermissionsExt},
        net::UnixStream,
    },
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const TOKEN: &str = "fixture-ingest-token-123456789012345";
const KEY: &str = "fixture-digest-key-1234567890123456";

/// Kill and reap the child before the test directory is removed, even on assertion failure.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Service {
    _child: ChildGuard,
    home: tempfile::TempDir,
    port: u16,
    socket: PathBuf,
}

impl Service {
    /// Bootstrap an isolated listener, socket and private SQLite store.
    fn start() -> Self {
        Self::start_with_limits(4096, 2)
    }

    /// Admit a capture with custom body and record limits.
    fn start_with_limits(max_body_bytes: usize, max_records: usize) -> Self {
        let home = tempfile::Builder::new()
            .prefix("st-")
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir()
            .expect("test directory");
        let token = home.path().join("token");
        let key = home.path().join("key");
        secret(&token, TOKEN);
        secret(&key, KEY);
        let port = TcpListener::bind(("127.0.0.1", 0))
            .expect("available port")
            .local_addr()
            .expect("local address")
            .port();
        let socket = home.path().join("read.sock");
        let config = home.path().join("stats.toml");
        fs::write(
            &config,
            format!(
                "listen = '127.0.0.1:{port}'\nstate_dir = {:?}\nsocket = {:?}\ntoken_file = {:?}\ndigest_key_file = {:?}\nmax_body_bytes = {max_body_bytes}\nmax_records = {max_records}\nmax_requests = 1\n",
                home.path().join("db"), socket, token, key,
            ),
        )
        .expect("write config");
        let mut child = ChildGuard(
            Command::new(env!("CARGO_BIN_EXE_agent-stats"))
                .args(["serve", "--config"])
                .arg(&config)
                .stdin(Stdio::null())
                .spawn()
                .expect("spawn service"),
        );
        for _ in 0..200 {
            if UnixStream::connect(&socket).is_ok() {
                return Self {
                    _child: child,
                    home,
                    port,
                    socket,
                };
            }
            assert!(child.0.try_wait().expect("child status").is_none());
            thread::sleep(Duration::from_millis(10));
        }
        panic!("service socket did not become ready");
    }

    /// Issue a one-shot raw HTTP request and return the response status.
    fn post(&self, token: &str, content_type: &str, body: &str) -> u16 {
        let mut client = TcpStream::connect(("127.0.0.1", self.port)).expect("connect HTTP");
        client
            .set_read_timeout(Some(Duration::from_secs(4)))
            .expect("HTTP timeout");
        write!(client, "POST /v1/logs HTTP/1.1\r\nHost: localhost\r\nx-stats-token: {token}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\r\n{body}", body.len()).expect("send HTTP");
        http_status(&mut client)
    }

    /// Exercise the same Unix socket CLI entry point the user runs.
    fn cli(&self, args: &[&str]) -> Value {
        let output = Command::new(env!("CARGO_BIN_EXE_agent-stats"))
            .args(args)
            .arg("--socket")
            .arg(&self.socket)
            .output()
            .expect("read CLI");
        assert!(
            output.status.success(),
            "CLI error: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let value: Value = serde_json::from_slice(&output.stdout).expect("versioned response");
        assert_eq!(value["version"], 1);
        assert_eq!(value["ok"], true);
        value["data"].clone()
    }
}

/// Create a file with the same permissions expected for deployed credentials.
fn secret(path: &Path, value: &str) {
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .expect("create secret")
        .write_all(value.as_bytes())
        .expect("write secret");
}

/// Read just the first HTTP line so fast rejections need not consume the request body.
fn http_status(stream: &mut TcpStream) -> u16 {
    use std::io::BufRead;
    let mut line = String::new();
    std::io::BufReader::new(stream)
        .read_line(&mut line)
        .expect("read HTTP status");
    line.split_whitespace()
        .nth(1)
        .expect("status field")
        .parse()
        .expect("HTTP code")
}

/// Preserve the original fixture shape and replace its timestamp for rolling-window tests.
fn fixture() -> Value {
    let mut batch: Value =
        serde_json::from_str(include_str!("claude_api_request.json")).expect("load fixture");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("wall clock");
    batch["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["timeUnixNano"] =
        json!((now.as_nanos() - 10_000_000_000).to_string());
    batch
}

/// Replay the capture through HTTP and inspect CLI JSON and the SQLite privacy boundary.
#[test]
fn sample_replay_and_exact_aggregates() {
    let service = Service::start();
    let batch = fixture();
    for _ in 0..2 {
        assert_eq!(
            service.post(TOKEN, "application/json", &batch.to_string()),
            200
        );
    }
    let mut other = batch;
    let records = other["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
        .as_array_mut()
        .expect("fixture records");
    records.pop();
    let attrs = records[0]["attributes"]
        .as_array_mut()
        .expect("fixture attributes");
    for attr in attrs.iter_mut() {
        match attr["key"].as_str() {
            Some("request_id") => attr["value"] = json!({"stringValue":"fixture-second"}),
            Some("duration_ms") => attr["value"] = json!({"intValue":"600"}),
            Some("ttft_ms") => attr["value"] = json!({"intValue":594}),
            _ => (),
        }
    }
    assert_eq!(
        service.post(TOKEN, "application/json; charset=utf-8", &other.to_string()),
        200
    );
    let data = service.cli(&["stats", "--group-by", "provider+model"]);
    let group = &data["groups"][0];
    assert_eq!(
        group["key"],
        json!({"model":"claude-haiku-5-5", "provider":"anthropic"})
    );
    let metric = &group["metrics"]["ttft_ms"];
    assert_eq!(
        metric,
        &json!({"count":2,"mean":619.0,"median":619.0,"min":594.0,"max":644.0,"p10":599.0,"p90":639.0})
    );
    assert!(group["metrics"]["speed_tokens_per_second"]["median"].is_null());
    assert_eq!(group["metrics"]["speed_estimated"], false);

    let series = service.cli(&[
        "series",
        "--model",
        "claude-haiku-5-5",
        "--bucket",
        "day",
        "--window",
        "7d",
    ]);
    let buckets = series["buckets"].as_array().expect("series");
    assert_eq!(
        buckets
            .iter()
            .filter(|bucket| bucket["metrics"]["count"] == 2)
            .count(),
        1
    );
    assert!(
        buckets
            .iter()
            .any(|bucket| bucket["metrics"]["ttft_ms"]["median"].is_null())
    );
    let status = service.cli(&["status"]);
    assert_eq!(status["counters"]["inserted_records"], 2);
    assert_eq!(status["counters"]["duplicate_records"], 1);
    assert_eq!(status["counters"]["ignored_records"], 2);
    assert!(status["last_sample_time_unix_ns"].is_string());

    let path = service.home.path().join("db/stats.sqlite3");
    let conn =
        rusqlite::Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("inspect store");
    let retained: (i64, i64, i64) = conn.query_row(
        "SELECT length(request_digest), input_tokens, cache_read_tokens FROM samples ORDER BY rowid LIMIT 1",
        [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).expect("stored allowed fields");
    assert_eq!(retained, (32, 2, 31_762));
    let db = fs::read(path).expect("read database");
    for canary in [
        "fixture-session",
        "fixture-user",
        "fixture-resource",
        "fixture-private-prompt",
        "fixture-ignored-event",
        "fixture-request",
        TOKEN,
        KEY,
    ] {
        assert!(
            !db.windows(canary.len())
                .any(|part| part == canary.as_bytes())
        );
    }
}

/// Rebase the capture into the rolling window without changing event intervals.
fn codex_fixture() -> Value {
    let mut export: Value =
        serde_json::from_str(include_str!("codex_events.json")).expect("Codex capture");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("wall clock");
    let reference = chrono::DateTime::parse_from_rfc3339("2026-11-26T22:51:35.392Z")
        .expect("capture end")
        .timestamp_nanos_opt()
        .expect("capture nanos");
    let offset = i64::try_from(now.as_nanos()).expect("wall nanos") - 10_000_000_000 - reference;
    for record in export["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
        .as_array_mut()
        .expect("capture records")
    {
        for attr in record["attributes"].as_array_mut().expect("attributes") {
            if attr["key"] == "event.timestamp" {
                let time = chrono::DateTime::parse_from_rfc3339(
                    attr["value"]["stringValue"].as_str().expect("time"),
                )
                .expect("RFC3339")
                .timestamp_nanos_opt()
                .expect("capture nanos");
                attr["value"]["stringValue"] =
                    json!(chrono::DateTime::from_timestamp_nanos(time + offset).to_rfc3339());
            }
        }
    }
    export
}

/// Verify exact fields, one-request pairs, ambiguity, replay and CLI provenance.
#[test]
fn codex_capture_ingestion() {
    let service = Service::start_with_limits(65_536, 32);
    let export = codex_fixture();
    for _ in 0..2 {
        assert_eq!(
            service.post(TOKEN, "application/json", &export.to_string()),
            200
        );
    }
    let status = service.cli(&["status"]);
    assert_eq!(status["counters"]["inserted_records"], 3);
    assert_eq!(status["counters"]["duplicate_records"], 3);
    assert_eq!(status["counters"]["ignored_records"], 16);
    let conn =
        rusqlite::Connection::open(service.home.path().join("db/stats.sqlite3")).expect("database");
    let durations: Vec<(Option<f64>, Option<String>)> = conn
        .prepare("SELECT duration_ms, duration_source FROM samples ORDER BY time_ns")
        .expect("durations")
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("rows");
    assert_eq!(
        durations,
        vec![
            (Some(256.0), Some("estimated".into())),
            (Some(173.0), Some("estimated".into())),
            (None, None)
        ]
    );
    let exact: (String, String, String, String, i64, i64, i64, i64, i64, f64, i64) = conn.query_row(
        "SELECT harness, provider, model, effort, input_tokens, output_tokens, cache_read_tokens,
                cache_creation_tokens, reasoning_tokens, ttft_ms, length(request_digest)
         FROM samples ORDER BY time_ns DESC LIMIT 1",
        [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?,
                      row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?, row.get(9)?, row.get(10)?))
    ).expect("completion fields");
    assert_eq!(
        exact,
        (
            "codex".into(),
            "openai".into(),
            "gpt-6-luna".into(),
            "low".into(),
            29290,
            30,
            11648,
            0,
            0,
            451.0,
            32
        )
    );
    let db = fs::read(service.home.path().join("db/stats.sqlite3")).expect("database bytes");
    assert!(
        !db.windows(b"fixture-conversation".len())
            .any(|part| part == b"fixture-conversation")
    );
    let data = service.cli(&["stats", "--group-by", "harness+model"]);
    let group = &data["groups"][1];
    assert_eq!(
        group["key"],
        json!({"harness":"codex","model":"gpt-6-luna"})
    );
    assert_eq!(group["metrics"]["count"], 2);
    assert_eq!(group["metrics"]["ttft_ms"]["mean"], 451.0);
    assert_eq!(group["metrics"]["speed_tokens_per_second"]["count"], 0);
    assert_eq!(group["metrics"]["speed_estimated"], false);
}

/// Pair request-only batches and expose the estimate in stats and its UTC series bucket.
#[test]
fn codex_pairing_across_batches() {
    let service = Service::start();
    let mut export = codex_fixture();
    let path = "/resourceLogs/0/scopeLogs/0/logRecords";
    let records = export
        .pointer(path)
        .and_then(Value::as_array)
        .expect("capture records");
    let request = records[12].clone();
    let completion = records[14].clone();
    for record in [request, completion.clone(), completion] {
        *export.pointer_mut(path).expect("record array") = json!([record]);
        assert_eq!(
            service.post(TOKEN, "application/json", &export.to_string()),
            200
        );
    }
    let stats = service.cli(&["stats"]);
    let group = &stats["groups"][0];
    assert_eq!(group["key"], json!({"model":"gpt-6-luna"}));
    let metrics = &group["metrics"];
    assert_eq!(metrics["count"], 1);
    assert_eq!(metrics["speed_estimated"], true);
    assert_eq!(metrics["speed_tokens_per_second"]["count"], 1);
    assert!(
        (metrics["speed_tokens_per_second"]["mean"]
            .as_f64()
            .expect("speed")
            - 30_000.0 / 524.0)
            .abs()
            < 1e-9
    );
    let series = service.cli(&["series", "--model", "gpt-6-luna", "--bucket", "hour"]);
    let buckets = series["buckets"].as_array().expect("series");
    assert_eq!(
        buckets
            .iter()
            .filter(|bucket| bucket["metrics"] == *metrics)
            .count(),
        1
    );
    assert!(
        buckets
            .iter()
            .filter(|bucket| bucket["metrics"]["count"] == 0)
            .all(|bucket| bucket["metrics"]["speed_estimated"] == false)
    );
}

/// Check that malformed, unauthorized, unsupported and oversized requests fail independently.
#[test]
fn admission_and_metric_validation() {
    let service = Service::start();
    assert_eq!(service.post("wrong", "application/json", "{"), 401);
    assert_eq!(service.post(TOKEN, "application/x-protobuf", "bits"), 415);
    assert_eq!(service.post(TOKEN, "application/json", "{"), 400);
    assert_eq!(
        service.post(TOKEN, "application/json", &"x".repeat(4097)),
        413
    );
    let mut chunked = TcpStream::connect(("127.0.0.1", service.port)).expect("chunked HTTP");
    chunked
        .set_read_timeout(Some(Duration::from_secs(4)))
        .expect("HTTP timeout");
    write!(chunked, "POST /v1/logs HTTP/1.1\r\nHost: localhost\r\nx-stats-token: {TOKEN}\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n1001\r\n{}\r\n0\r\n\r\n", "x".repeat(4097)).expect("write chunk");
    assert_eq!(http_status(&mut chunked), 413);

    let mut batch = fixture();
    let records = batch["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
        .as_array_mut()
        .expect("fixture records");
    let mut invalid = records[0].clone();
    invalid["timeUnixNano"] = json!("bad");
    records.push(invalid);
    assert_eq!(
        service.post(TOKEN, "application/json", &batch.to_string()),
        413
    );
    let records = batch["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
        .as_array_mut()
        .expect("fixture records");
    records.remove(1);
    let attrs = records[0]["attributes"]
        .as_array_mut()
        .expect("fixture attributes");
    attrs.retain(|attr| attr["key"] != "output_tokens" && attr["key"] != "effort");
    assert_eq!(
        service.post(TOKEN, "application/json", &batch.to_string()),
        200
    );
    let data = service.cli(&["stats", "--group-by", "model+effort"]);
    let group = &data["groups"][0];
    assert!(group["key"]["effort"].is_null());
    assert!(group["metrics"]["speed_tokens_per_second"]["median"].is_null());
    assert_eq!(group["metrics"]["ttft_ms"]["median"], 644.0);
    let status = service.cli(&["status"]);
    assert_eq!(status["counters"]["invalid_records"], 1);
    assert_eq!(status["counters"]["unauthorized_requests"], 1);
    assert_eq!(status["counters"]["oversize_requests"], 3);
}

/// Reject unsupported socket protocol versions without emitting arbitrary request content.
#[test]
fn versioned_socket_error() {
    let service = Service::start();
    let mut stream = UnixStream::connect(&service.socket).expect("socket");
    stream
        .set_read_timeout(Some(Duration::from_secs(4)))
        .expect("socket timeout");
    stream
        .write_all(b"{\"version\":2,\"op\":\"status\"}\n")
        .expect("write socket");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read socket");
    assert_eq!(
        serde_json::from_str::<Value>(&response).expect("socket JSON"),
        json!({"version":1,"ok":false,"error":"unsupported_version"})
    );
}

/// Hold one admitted body open and require the next request to reject before reading.
#[test]
fn active_request_limit_returns_429() {
    let service = Service::start();
    let mut first = TcpStream::connect(("127.0.0.1", service.port)).expect("open HTTP");
    first
        .set_read_timeout(Some(Duration::from_secs(4)))
        .expect("HTTP timeout");
    write!(first, "POST /v1/logs HTTP/1.1\r\nHost: localhost\r\nx-stats-token: {TOKEN}\r\nContent-Type: application/json\r\nExpect: 100-continue\r\nContent-Length: 100\r\n\r\n").expect("send headers");
    assert_eq!(http_status(&mut first), 100);
    assert_eq!(service.post(TOKEN, "application/json", "{}"), 429);
    drop(first);
    assert_eq!(
        service.cli(&["status"])["counters"]["overloaded_requests"],
        1
    );
}
