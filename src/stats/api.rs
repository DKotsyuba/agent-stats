//! Versioned read operations and exact, bounded in-memory aggregates.
use super::{Counters, Error, HOUR_NS, RATING_RETENTION_NS, RETENTION_NS};
use clap::ValueEnum;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

const MAX_ROWS: usize = 100_000;
const MAX_GROUPS: usize = 1024;

/// Supported rolling windows use fixed UTC durations.
#[derive(Clone, Copy, Deserialize, Serialize, ValueEnum)]
pub(crate) enum Window {
    #[serde(rename = "24h")]
    #[value(name = "24h")]
    Day,
    #[serde(rename = "7d")]
    #[value(name = "7d")]
    Week,
    #[serde(rename = "30d")]
    #[value(name = "30d")]
    Month,
}

impl Window {
    /// Convert the rolling window to nanoseconds.
    fn nanos(self) -> i64 {
        match self {
            Self::Day => 24 * HOUR_NS,
            Self::Week => 7 * 24 * HOUR_NS,
            Self::Month => RETENTION_NS,
        }
    }
}

/// Dimensions included in an aggregate group.
#[derive(Clone, Copy, Deserialize, Serialize, ValueEnum)]
pub(crate) enum Group {
    #[serde(rename = "model")]
    Model,
    #[serde(rename = "provider+model")]
    #[value(name = "provider+model")]
    ProviderModel,
    #[serde(rename = "harness+model")]
    #[value(name = "harness+model")]
    HarnessModel,
    #[serde(rename = "model+effort")]
    #[value(name = "model+effort")]
    ModelEffort,
}

/// Orchestrator task tier, from hardest (`S`) to easiest (`F`).
#[derive(Clone, Copy, Deserialize, Serialize, ValueEnum)]
#[value(rename_all = "verbatim")]
pub(crate) enum Tier {
    S,
    A,
    B,
    C,
    D,
    E,
    F,
}

impl Tier {
    /// Stored and displayed single-letter label.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::S => "S",
            Self::A => "A",
            Self::B => "B",
            Self::C => "C",
            Self::D => "D",
            Self::E => "E",
            Self::F => "F",
        }
    }
}

/// The orchestrator's acceptance decision for one delegated result.
#[derive(Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub(crate) enum Verdict {
    Accepted,
    AcceptedAfterFixes,
    Rejected,
}

impl Verdict {
    /// Stored and displayed snake_case label.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::AcceptedAfterFixes => "accepted_after_fixes",
            Self::Rejected => "rejected",
        }
    }
}

/// Why a result needed fixes or was rejected.
#[derive(Clone, Copy, Deserialize, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub(crate) enum Reason {
    WrongSolution,
    Incomplete,
    MissingAccess,
    BrokeOther,
    Environment,
    Other,
}

impl Reason {
    /// Stored and displayed snake_case label.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::WrongSolution => "wrong_solution",
            Self::Incomplete => "incomplete",
            Self::MissingAccess => "missing_access",
            Self::BrokeOther => "broke_other",
            Self::Environment => "environment",
            Self::Other => "other",
        }
    }
}

/// Counts of verification checks the orchestrator ran on the result.
#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Checks {
    #[serde(default)]
    pub passed: u32,
    #[serde(default)]
    pub failed: u32,
    #[serde(default)]
    pub not_run: u32,
}

/// One orchestrator rating, keyed by the delegated Run id; a later rating replaces the earlier one.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Rating {
    pub agent_id: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<Tier>,
    pub verdict: Verdict,
    /// Fix rounds before acceptance (or before rejection); defaults by verdict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rounds: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<Reason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checks: Option<Checks>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// Maximum fix rounds a rating may record.
const MAX_ROUNDS: u32 = 1000;
/// Maximum note length in characters.
pub(crate) const MAX_NOTE_CHARS: usize = 200;

impl Rating {
    /// Reject malformed or inconsistent ratings with a field-specific stable code.
    pub(crate) fn validate(&self) -> Result<(), Error> {
        let label = |value: &str| super::normalize::label(value);
        if !label(&self.agent_id) {
            return Err(Error("invalid_agent_id"));
        }
        if !label(&self.model) {
            return Err(Error("invalid_model"));
        }
        if self.provider.as_deref().is_some_and(|value| !label(value)) {
            return Err(Error("invalid_provider"));
        }
        if self.effort.as_deref().is_some_and(|value| !label(value)) {
            return Err(Error("invalid_effort"));
        }
        if self.rounds.is_some_and(|rounds| rounds > MAX_ROUNDS) {
            return Err(Error("invalid_rounds"));
        }
        match self.verdict {
            Verdict::Accepted if self.rounds.unwrap_or(0) != 0 => {
                return Err(Error("invalid_rounds"));
            }
            Verdict::AcceptedAfterFixes if self.rounds == Some(0) => {
                return Err(Error("invalid_rounds"));
            }
            _ => {}
        }
        if self.verdict == Verdict::Accepted && self.reason.is_some() {
            return Err(Error("invalid_reason"));
        }
        if self.note.as_deref().is_some_and(|note| {
            note.chars().count() > MAX_NOTE_CHARS || note.chars().any(char::is_control)
        }) {
            return Err(Error("invalid_note"));
        }
        Ok(())
    }

    /// Fix rounds with the verdict's default: one for accepted-after-fixes, otherwise zero.
    pub(crate) fn effective_rounds(&self) -> u32 {
        self.rounds
            .unwrap_or(u32::from(self.verdict == Verdict::AcceptedAfterFixes))
    }
}

/// UTC-aligned series bucket widths.
#[derive(Clone, Copy, Deserialize, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Bucket {
    Hour,
    Day,
}

/// One request per connection, versioned independently of the binary.
#[derive(Deserialize, Serialize)]
pub(super) struct Request {
    pub version: u32,
    #[serde(flatten)]
    pub operation: Operation,
}

/// Socket methods share their typed argument definitions with the CLI.
#[derive(Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Operation {
    ModelStats {
        window: Window,
        group_by: Group,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
    },
    ModelSeries {
        model: String,
        bucket: Bucket,
        window: Window,
    },
    Status,
    /// Write: record or replace the rating of one delegated result.
    RateResult(Rating),
    /// Read: newest-first page of recorded ratings.
    Ratings {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
    },
}

/// Both stats and series use the same metric accumulator.
#[derive(Default)]
struct Metrics {
    count: usize,
    speed: Vec<f64>,
    speed_estimated: bool,
    ttft: Vec<f64>,
}

/// Minimum output tokens for a speed sample.
const SPEED_MIN_OUTPUT_TOKENS: i64 = 100;
/// Minimum share of the request duration spent after the first token for a speed sample.
const SPEED_MIN_GENERATION_SHARE: f64 = 0.2;

impl Metrics {
    /// Count every row, with independent eligibility for speed and TTFT.
    fn push(
        &mut self,
        output: Option<i64>,
        duration: Option<f64>,
        ttft: Option<f64>,
        estimated: bool,
    ) {
        self.count += 1;
        if let Some(ttft) = ttft {
            self.ttft.push(ttft);
            // Speed needs a real streaming window: at least 100 output tokens (a fixed end-of-stream
            // tail dominates shorter answers) and generation taking at least a fifth of the request
            // (otherwise the body arrived as one burst, e.g. after unstreamed thinking or a buffering gateway).
            if let (Some(output), Some(duration)) = (output, duration)
                && output >= SPEED_MIN_OUTPUT_TOKENS
                && duration - ttft >= 50.0
                && (duration - ttft) >= duration * SPEED_MIN_GENERATION_SHARE
            {
                let speed = output as f64 * 1000.0 / (duration - ttft);
                if speed.is_finite() {
                    self.speed.push(speed);
                    self.speed_estimated |= estimated;
                }
            }
        }
    }

    /// Produce wire metrics with separate eligible counts and explicit empty summaries.
    fn finish(self) -> Value {
        json!({
            "count": self.count,
            "speed_tokens_per_second": summary(self.speed),
            "speed_estimated": self.speed_estimated,
            "ttft_ms": summary(self.ttft),
        })
    }
}

/// Exact rank with linear interpolation at (n-1)p.
fn summary(mut values: Vec<f64>) -> Value {
    if values.is_empty() {
        return json!({"count": 0, "mean": null, "median": null, "min": null,
            "max": null, "p10": null, "p90": null});
    }
    values.sort_unstable_by(f64::total_cmp);
    let last = values.len() - 1;
    let percentile = |p: f64| {
        let index = last as f64 * p;
        let low = index.floor() as usize;
        let high = index.ceil() as usize;
        let fraction = index.fract();
        values[low] + (values[high] - values[low]) * fraction
    };
    let count = values.len();
    let mean = values.iter().enumerate().fold(0.0, |mean, (index, value)| {
        mean + (value - mean) / (index + 1) as f64
    });
    json!({"count": count, "mean": mean, "median": percentile(0.5), "min": values[0],
        "max": values[last], "p10": percentile(0.1), "p90": percentile(0.9)})
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    reason = "Test setup failures should include their cause"
)]
mod tests {
    use super::*;
    use crate::stats::{Checks, Counters, Rating, Tier, store};

    /// Build a rating with only the fields a case varies.
    fn rating(id: &str, model: &str, verdict: Verdict, rounds: Option<u32>) -> Rating {
        Rating {
            agent_id: id.into(),
            model: model.into(),
            provider: Some("anthropic".into()),
            effort: Some("high".into()),
            tier: Some(Tier::C),
            verdict,
            rounds,
            reason: None,
            checks: None,
            note: None,
        }
    }

    /// Exact acceptance numbers per model and per model+effort, with the window and no-sample groups.
    #[test]
    fn rating_aggregation() {
        // SQLite NOFOLLOW rejects symlinked path components, as in some CI temp roots.
        let dir = tempfile::tempdir_in(std::env::temp_dir().canonicalize().expect("temp root"))
            .expect("temp directory");
        let mut conn = store::open(&dir.path().join("db")).expect("database");
        let now = 100 * 24 * HOUR_NS;
        let mut write = |mut rating: Rating, reason: Option<Reason>, age_hours: i64| {
            rating.reason = reason;
            store::rate(&mut conn, &rating, now - age_hours * HOUR_NS).expect("rate");
        };
        let m = "claude-sonnet-5-5";
        write(rating("a1", m, Verdict::Accepted, None), None, 1);
        write(rating("a2", m, Verdict::Accepted, Some(0)), None, 2);
        write(
            rating("a3", m, Verdict::AcceptedAfterFixes, Some(2)),
            Some(Reason::Incomplete),
            3,
        );
        write(
            rating("a4", m, Verdict::Rejected, Some(3)),
            Some(Reason::WrongSolution),
            4,
        );
        write(
            rating("a5", m, Verdict::Rejected, Some(1)),
            Some(Reason::WrongSolution),
            5,
        );
        let mut other_effort = rating("a6", m, Verdict::Rejected, None);
        other_effort.effort = None;
        write(other_effort, Some(Reason::Incomplete), 6);
        // Older than the 7d window but inside 30d and inside rating retention.
        write(rating("old", m, Verdict::Rejected, Some(9)), None, 8 * 24);
        // Rated work with no observed calls still appears, without speed data.
        write(rating("g1", "gpt-6", Verdict::Accepted, None), None, 1);
        conn.execute(
            "INSERT INTO samples (harness, request_digest, time_ns, provider, model, effort,
                 output_tokens, duration_ms, ttft_ms)
             VALUES ('claude-code', x'01', ?, 'anthropic', ?, 'high', 200, 1000.0, 200.0)",
            params![now - HOUR_NS, m],
        )
        .expect("sample");

        let run = |group_by, model: Option<&str>| {
            read(
                &conn,
                Operation::ModelStats {
                    window: Window::Week,
                    group_by,
                    model: model.map(str::to_owned),
                },
                now,
                &Counters::default(),
            )
            .expect("model_stats")
        };
        let data = run(Group::Model, None);
        let groups = data["groups"].as_array().expect("groups");
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0]["key"], json!({"model": m}));
        assert_eq!(groups[0]["metrics"]["count"], 1);
        let ratings = &groups[0]["ratings"];
        assert_eq!(ratings["rated"], 6);
        assert_eq!(ratings["accepted"], 3);
        assert_eq!(ratings["accepted_first_time"], 2);
        assert_eq!(ratings["rejected"], 3);
        assert_eq!(ratings["accepted_rate"], 0.5);
        assert_eq!(ratings["accepted_first_time_rate"], 2.0 / 6.0);
        assert_eq!(ratings["mean_rounds"], 1.0);
        assert_eq!(
            ratings["top_reasons"],
            json!([{"reason": "incomplete", "count": 2}, {"reason": "wrong_solution", "count": 2}])
        );
        assert_eq!(groups[1]["key"], json!({"model": "gpt-6"}));
        assert_eq!(groups[1]["metrics"]["count"], 0);
        assert_eq!(groups[1]["ratings"]["accepted_first_time"], 1);

        let by_effort = run(Group::ModelEffort, Some(m));
        let groups = by_effort["groups"].as_array().expect("groups");
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0]["key"], json!({"model": m, "effort": null}));
        assert_eq!(groups[0]["ratings"]["rated"], 1);
        assert!(groups[0]["metrics"]["count"] == 0);
        assert_eq!(groups[1]["key"], json!({"model": m, "effort": "high"}));
        assert_eq!(groups[1]["ratings"]["rated"], 5);
        assert_eq!(groups[1]["ratings"]["mean_rounds"], 6.0 / 5.0);
        assert_eq!(groups[1]["metrics"]["count"], 1);

        // Ratings carry no harness, so those groups report none.
        let by_harness = run(Group::HarnessModel, Some(m));
        assert!(by_harness["groups"][0].get("ratings").is_none());
        // Groups without ratings omit the object entirely.
        let none = run(Group::Model, Some("claude-haiku-5-5"));
        assert!(none["groups"].as_array().expect("groups").is_empty());
    }

    /// Pages are newest first, bounded, resumable by cursor and carry checks and notes.
    #[test]
    fn ratings_pages() {
        // SQLite NOFOLLOW rejects symlinked path components, as in some CI temp roots.
        let dir = tempfile::tempdir_in(std::env::temp_dir().canonicalize().expect("temp root"))
            .expect("temp directory");
        let mut conn = store::open(&dir.path().join("db")).expect("database");
        let now = 100 * 24 * HOUR_NS;
        for (id, age) in [("b", 3), ("a", 3), ("c", 2), ("d", 1)] {
            let mut rating = rating(id, "m", Verdict::Accepted, None);
            rating.checks = Some(Checks {
                passed: 4,
                failed: 0,
                not_run: 1,
            });
            rating.note = Some("fine".into());
            store::rate(&mut conn, &rating, now - age * HOUR_NS).expect("rate");
        }
        // Beyond rating retention: never listed.
        store::rate(
            &mut conn,
            &rating("gone", "m", Verdict::Accepted, None),
            now - 91 * 24 * HOUR_NS,
        )
        .expect("rate");
        let page = |limit, cursor: Option<&str>| {
            ratings_page(&conn, None, Some(limit), cursor, now).expect("page")
        };
        let first = page(3, None);
        let ids: Vec<_> = first["ratings"]
            .as_array()
            .expect("ratings")
            .iter()
            .map(|row| row["agent_id"].as_str().expect("id").to_owned())
            .collect();
        assert_eq!(ids, ["d", "c", "b"]);
        assert_eq!(first["ratings"][0]["checks"]["not_run"], 1);
        assert_eq!(first["ratings"][0]["note"], "fine");
        let cursor = first["next_cursor"].as_str().expect("cursor").to_owned();
        let second = page(3, Some(&cursor));
        assert_eq!(second["ratings"].as_array().expect("ratings").len(), 1);
        assert_eq!(second["ratings"][0]["agent_id"], "a");
        assert!(second["next_cursor"].is_null());
        assert!(matches!(
            ratings_page(&conn, None, Some(0), None, now),
            Err(Error("invalid_limit"))
        ));
        assert!(matches!(
            ratings_page(&conn, None, Some(101), None, now),
            Err(Error("invalid_limit"))
        ));
        assert!(matches!(
            ratings_page(&conn, None, None, Some("nope"), now),
            Err(Error("invalid_cursor"))
        ));
        let other = ratings_page(&conn, Some("other"), None, None, now).expect("filtered");
        assert!(other["ratings"].as_array().expect("ratings").is_empty());
    }

    /// Verdict, rounds, reason and note combinations are checked before any write.
    #[test]
    fn rating_validation() {
        let base = |verdict, rounds| rating("a", "m", verdict, rounds);
        assert!(base(Verdict::Accepted, None).validate().is_ok());
        assert!(base(Verdict::AcceptedAfterFixes, None).validate().is_ok());
        assert!(base(Verdict::Rejected, Some(1000)).validate().is_ok());
        let code = |rating: Rating| rating.validate().expect_err("invalid").code();
        assert_eq!(code(base(Verdict::Accepted, Some(1))), "invalid_rounds");
        assert_eq!(
            code(base(Verdict::AcceptedAfterFixes, Some(0))),
            "invalid_rounds"
        );
        assert_eq!(code(base(Verdict::Rejected, Some(1001))), "invalid_rounds");
        let mut bad = base(Verdict::Accepted, None);
        bad.reason = Some(Reason::Other);
        assert_eq!(code(bad), "invalid_reason");
        let mut bad = base(Verdict::Rejected, None);
        bad.note = Some("x".repeat(201));
        assert_eq!(code(bad), "invalid_note");
        let mut ok = base(Verdict::Rejected, None);
        ok.note = Some("é".repeat(200));
        assert!(ok.validate().is_ok());
        let mut bad = base(Verdict::Rejected, None);
        bad.note = Some("line\nbreak".into());
        assert_eq!(code(bad), "invalid_note");
        let mut bad = base(Verdict::Rejected, None);
        bad.model = "a b".into();
        assert_eq!(code(bad), "invalid_model");
        let mut bad = base(Verdict::Rejected, None);
        bad.agent_id = String::new();
        assert_eq!(code(bad), "invalid_agent_id");
    }

    /// Output, window and generation-share thresholds are inclusive; only eligible estimates flag a group.
    #[test]
    fn speed_thresholds_and_provenance() {
        let mut metrics = Metrics::default();
        metrics.push(Some(99), Some(1000.0), Some(100.0), true);
        metrics.push(Some(100), Some(149.0), Some(100.0), true);
        metrics.push(Some(100), Some(1000.0), Some(801.0), true);
        metrics.push(Some(100), Some(1000.0), Some(800.0), false);
        let result = metrics.finish();
        assert_eq!(result["count"], 4);
        assert_eq!(result["ttft_ms"]["count"], 4);
        assert_eq!(result["speed_tokens_per_second"]["count"], 1);
        assert_eq!(result["speed_tokens_per_second"]["mean"], 500.0);
        assert_eq!(result["speed_estimated"], false);
    }
}

/// Rating accumulator for one group; verdict counts and fix rounds are exact.
#[derive(Default)]
struct Ratings {
    rated: u64,
    accepted: u64,
    first_time: u64,
    rounds: u64,
    reasons: BTreeMap<&'static str, u64>,
}

/// Number of most frequent reasons reported per group.
const TOP_REASONS: usize = 3;

impl Ratings {
    /// Count one stored rating; reasons are tallied for every result that was not accepted first time.
    fn push(&mut self, verdict: &str, rounds: i64, reason: Option<String>) {
        self.rated += 1;
        self.rounds += u64::try_from(rounds).unwrap_or(0);
        match verdict {
            "accepted" => {
                self.accepted += 1;
                self.first_time += 1;
            }
            "accepted_after_fixes" => self.accepted += 1,
            _ => {}
        }
        if verdict != "accepted"
            && let Some(reason) = reason.as_deref().and_then(reason_label)
        {
            *self.reasons.entry(reason).or_default() += 1;
        }
    }

    /// Produce wire ratings with exact counts, rates over rated results and the most frequent reasons.
    fn finish(self) -> Value {
        let rated = self.rated as f64;
        let mut reasons: Vec<_> = self.reasons.into_iter().collect();
        reasons.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(right.0)));
        let top: Vec<_> = reasons
            .into_iter()
            .take(TOP_REASONS)
            .map(|(reason, count)| json!({"reason": reason, "count": count}))
            .collect();
        json!({
            "rated": self.rated,
            "accepted": self.accepted,
            "accepted_first_time": self.first_time,
            "rejected": self.rated - self.accepted,
            "accepted_rate": self.accepted as f64 / rated,
            "accepted_first_time_rate": self.first_time as f64 / rated,
            "mean_rounds": self.rounds as f64 / rated,
            "top_reasons": top,
        })
    }
}

/// Map a stored reason back to its static label.
fn reason_label(reason: &str) -> Option<&'static str> {
    [
        Reason::WrongSolution,
        Reason::Incomplete,
        Reason::MissingAccess,
        Reason::BrokeOther,
        Reason::Environment,
        Reason::Other,
    ]
    .into_iter()
    .map(Reason::as_str)
    .find(|label| *label == reason)
}

/// A group key owns only dimensions requested by the caller.
#[derive(Ord, PartialOrd, Eq, PartialEq, Serialize)]
struct Key {
    model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    effort: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    harness: Option<String>,
}

/// Evaluate one validated request against a read-only snapshot.
pub(super) fn read(
    conn: &Connection,
    operation: Operation,
    now: i64,
    counters: &Counters,
) -> Result<Value, Error> {
    if matches!(operation, Operation::Status) {
        let last_sample_time_unix_ns: Option<i64> = conn.query_row(
            "SELECT max(time_ns) FROM samples WHERE time_ns BETWEEN ? AND ?",
            params![now - RETENTION_NS, now],
            |row| row.get(0),
        )?;
        return Ok(
            json!({"last_sample_time_unix_ns": last_sample_time_unix_ns.map(|time| time.to_string()),
            "counters": counters.snapshot(), "service_version": env!("CARGO_PKG_VERSION")}),
        );
    }

    if let Operation::Ratings {
        model,
        limit,
        cursor,
    } = &operation
    {
        return ratings_page(conn, model.as_deref(), *limit, cursor.as_deref(), now);
    }

    let (window, model, bucket) = match &operation {
        Operation::ModelStats { window, model, .. } => (*window, model.as_deref(), None),
        Operation::ModelSeries {
            model,
            bucket,
            window,
        } => {
            let width = match bucket {
                Bucket::Hour => HOUR_NS,
                Bucket::Day => 24 * HOUR_NS,
            };
            (*window, Some(model.as_str()), Some(width))
        }
        Operation::Status | Operation::RateResult(_) | Operation::Ratings { .. } => {
            return Err(Error("invalid_request"));
        }
    };

    if model.is_some_and(|model| !super::normalize::label(model)) {
        return Err(Error("invalid_model"));
    }
    let sql = if model.is_some() {
        "SELECT time_ns, model, provider, effort, output_tokens, duration_ms, ttft_ms,
                harness, duration_source IS 'estimated'
         FROM samples WHERE time_ns BETWEEN ?1 AND ?2 AND model = ?4 LIMIT ?3"
    } else {
        "SELECT time_ns, model, provider, effort, output_tokens, duration_ms, ttft_ms,
                harness, duration_source IS 'estimated'
         FROM samples WHERE time_ns BETWEEN ?1 AND ?2 LIMIT ?3"
    };
    let mut statement = conn.prepare(sql)?;
    let start = now - window.nanos();
    let limit = (MAX_ROWS + 1) as i64;
    let mut rows = if let Some(model) = model {
        statement.query(params![start, now, limit, model])?
    } else {
        statement.query(params![start, now, limit])?
    };
    let mut groups: BTreeMap<Key, Metrics> = BTreeMap::new();
    let mut points: BTreeMap<i64, Metrics> = BTreeMap::new();
    let mut count = 0;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > MAX_ROWS {
            return Err(Error("query_too_large"));
        }
        let metrics = match operation {
            Operation::ModelStats { group_by, .. } => {
                let key = Key {
                    model: row.get(1)?,
                    provider: match group_by {
                        Group::ProviderModel => Some(row.get(2)?),
                        _ => None,
                    },
                    effort: match group_by {
                        Group::ModelEffort => Some(row.get(3)?),
                        _ => None,
                    },
                    harness: match group_by {
                        Group::HarnessModel => Some(row.get(7)?),
                        _ => None,
                    },
                };
                groups.entry(key).or_default()
            }
            Operation::ModelSeries { .. } => {
                let time: i64 = row.get(0)?;
                let width = bucket.ok_or(Error("invalid_request"))?;
                points.entry(time.div_euclid(width) * width).or_default()
            }
            Operation::Status | Operation::RateResult(_) | Operation::Ratings { .. } => {
                return Err(Error("invalid_request"));
            }
        };
        metrics.push(row.get(4)?, row.get(5)?, row.get(6)?, row.get(8)?);
        if groups.len() > MAX_GROUPS {
            return Err(Error("query_too_large"));
        }
    }

    Ok(match operation {
        Operation::ModelStats { group_by, .. } => {
            let mut ratings = rate_groups(conn, group_by, model, start, now)?;
            let mut keyed: Vec<_> = groups
                .into_iter()
                .map(|(key, metrics)| {
                    let rated = ratings.remove(&key);
                    (key, metrics, rated)
                })
                .collect();
            // Rated work whose calls were not observed (no telemetry, or outside the window) still counts.
            keyed.extend(
                ratings
                    .into_iter()
                    .map(|(key, rated)| (key, Metrics::default(), Some(rated))),
            );
            if keyed.len() > MAX_GROUPS {
                return Err(Error("query_too_large"));
            }
            keyed.sort_by(|left, right| left.0.cmp(&right.0));
            let groups: Vec<_> = keyed
                .into_iter()
                .map(|(key, metrics, rated)| {
                    let mut group = json!({"key": key, "metrics": metrics.finish()});
                    if let Some(rated) = rated {
                        group["ratings"] = rated.finish();
                    }
                    group
                })
                .collect();
            json!({"window": window, "group_by": group_by, "groups": groups})
        }
        Operation::ModelSeries {
            model,
            bucket: width,
            ..
        } => {
            let width_ns = bucket.ok_or(Error("invalid_request"))?;
            let mut buckets = Vec::new();
            let mut time = start.div_euclid(width_ns) * width_ns;
            while time <= now {
                let metrics = points.remove(&time).unwrap_or_default().finish();
                buckets.push(json!({"start_unix_ns": time.to_string(), "metrics": metrics}));
                time += width_ns;
            }
            json!({"window": window, "model": model, "bucket": width, "buckets": buckets})
        }
        Operation::Status | Operation::RateResult(_) | Operation::Ratings { .. } => {
            return Err(Error("invalid_request"));
        }
    })
}

/// Aggregate ratings changed inside the window into the same group keys as the samples.
/// Ratings carry no harness, so `harness+model` groups report none.
fn rate_groups(
    conn: &Connection,
    group_by: Group,
    model: Option<&str>,
    start: i64,
    now: i64,
) -> Result<BTreeMap<Key, Ratings>, Error> {
    let mut out: BTreeMap<Key, Ratings> = BTreeMap::new();
    if matches!(group_by, Group::HarnessModel) {
        return Ok(out);
    }
    let mut statement = conn.prepare(
        "SELECT model, provider, effort, verdict, rounds, reason FROM ratings
         WHERE updated_ns BETWEEN ?1 AND ?2 AND (?4 IS NULL OR model = ?4) LIMIT ?3",
    )?;
    let mut rows = statement.query(params![start, now, (MAX_ROWS + 1) as i64, model])?;
    let mut count = 0;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > MAX_ROWS {
            return Err(Error("query_too_large"));
        }
        let key = Key {
            model: row.get(0)?,
            provider: match group_by {
                Group::ProviderModel => Some(
                    row.get::<_, Option<String>>(1)?
                        .unwrap_or_else(|| "unknown".into()),
                ),
                _ => None,
            },
            effort: match group_by {
                Group::ModelEffort => Some(row.get(2)?),
                _ => None,
            },
            harness: None,
        };
        out.entry(key)
            .or_default()
            .push(&row.get::<_, String>(3)?, row.get(4)?, row.get(5)?);
        if out.len() > MAX_GROUPS {
            return Err(Error("query_too_large"));
        }
    }
    Ok(out)
}

/// Default and maximum ratings returned by one page.
const DEFAULT_PAGE: u32 = 20;
const MAX_PAGE: u32 = 100;

/// Read one newest-first page of ratings; `cursor` is `<updated_unix_ns>:<agent_id>` from the prior page.
fn ratings_page(
    conn: &Connection,
    model: Option<&str>,
    limit: Option<u32>,
    cursor: Option<&str>,
    now: i64,
) -> Result<Value, Error> {
    let limit = limit.unwrap_or(DEFAULT_PAGE);
    if !(1..=MAX_PAGE).contains(&limit) {
        return Err(Error("invalid_limit"));
    }
    if model.is_some_and(|model| !super::normalize::label(model)) {
        return Err(Error("invalid_model"));
    }
    let after = match cursor {
        None => None,
        Some(cursor) => {
            let (time, id) = cursor.split_once(':').ok_or(Error("invalid_cursor"))?;
            let time: i64 = time.parse().map_err(|_| Error("invalid_cursor"))?;
            if !super::normalize::label(id) {
                return Err(Error("invalid_cursor"));
            }
            Some((time, id.to_owned()))
        }
    };
    let mut statement = conn.prepare(
        "SELECT agent_id, model, provider, effort, tier, verdict, rounds, reason,
                checks_passed, checks_failed, checks_not_run, note, updated_ns
         FROM ratings
         WHERE updated_ns >= ?1 AND (?2 IS NULL OR model = ?2)
           AND (?3 IS NULL OR (updated_ns, agent_id) < (?3, ?4))
         ORDER BY updated_ns DESC, agent_id DESC LIMIT ?5",
    )?;
    let mut rows = statement.query(params![
        now - RATING_RETENTION_NS,
        model,
        after.as_ref().map(|(time, _)| time),
        after.as_ref().map(|(_, id)| id),
        i64::from(limit) + 1,
    ])?;
    let mut ratings = Vec::new();
    let mut next = None;
    let mut more = false;
    while let Some(row) = rows.next()? {
        if ratings.len() == limit as usize {
            more = true;
            break;
        }
        let (id, updated): (String, i64) = (row.get(0)?, row.get(12)?);
        let checks = match (
            row.get::<_, Option<i64>>(8)?,
            row.get::<_, Option<i64>>(9)?,
            row.get::<_, Option<i64>>(10)?,
        ) {
            (Some(passed), Some(failed), Some(not_run)) => {
                json!({"passed": passed, "failed": failed, "not_run": not_run})
            }
            _ => Value::Null,
        };
        next = Some(format!("{updated}:{id}"));
        ratings.push(json!({
            "agent_id": id, "model": row.get::<_, String>(1)?,
            "provider": row.get::<_, Option<String>>(2)?, "effort": row.get::<_, Option<String>>(3)?,
            "tier": row.get::<_, Option<String>>(4)?, "verdict": row.get::<_, String>(5)?,
            "rounds": row.get::<_, i64>(6)?, "reason": row.get::<_, Option<String>>(7)?,
            "checks": checks, "note": row.get::<_, Option<String>>(11)?,
            "updated_unix_ns": updated.to_string(),
        }));
    }
    Ok(json!({"ratings": ratings, "next_cursor": if more { next } else { None }}))
}
