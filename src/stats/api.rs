//! Versioned read operations and exact, bounded in-memory aggregates.
use super::{Counters, Error, HOUR_NS, RETENTION_NS};
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
    #[serde(rename = "model+effort")]
    #[value(name = "model+effort")]
    ModelEffort,
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
    },
    ModelSeries {
        model: String,
        bucket: Bucket,
        window: Window,
    },
    Status,
}

/// Both stats and series use the same metric accumulator.
#[derive(Default)]
struct Metrics {
    count: usize,
    speed: Vec<f64>,
    ttft: Vec<f64>,
}

impl Metrics {
    /// Count every row, with independent eligibility for speed and TTFT.
    fn push(&mut self, output: Option<i64>, duration: Option<f64>, ttft: Option<f64>) {
        self.count += 1;
        if let Some(ttft) = ttft {
            self.ttft.push(ttft);
            if let (Some(output), Some(duration)) = (output, duration)
                && output > 0
                && duration > ttft
            {
                let speed = output as f64 * 1000.0 / (duration - ttft);
                if speed.is_finite() {
                    self.speed.push(speed);
                }
            }
        }
    }

    /// Produce wire metrics with separate eligible counts and explicit empty summaries.
    fn finish(self) -> Value {
        json!({
            "count": self.count,
            "speed_tokens_per_second": summary(self.speed),
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

/// A group key owns only dimensions requested by the caller.
#[derive(Ord, PartialOrd, Eq, PartialEq, Serialize)]
struct Key {
    model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    effort: Option<Option<String>>,
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
            "counters": counters.snapshot()}),
        );
    }

    let (window, model, bucket) = match &operation {
        Operation::ModelStats { window, .. } => (*window, None, None),
        Operation::ModelSeries {
            model,
            bucket,
            window,
        } => {
            if !super::normalize::label(model) {
                return Err(Error("invalid_model"));
            }
            let width = match bucket {
                Bucket::Hour => HOUR_NS,
                Bucket::Day => 24 * HOUR_NS,
            };
            (*window, Some(model.as_str()), Some(width))
        }
        Operation::Status => return Err(Error("invalid_request")),
    };

    let sql = if model.is_some() {
        "SELECT time_ns, model, provider, effort, output_tokens, duration_ms, ttft_ms
         FROM samples WHERE time_ns BETWEEN ?1 AND ?2 AND model = ?4 LIMIT ?3"
    } else {
        "SELECT time_ns, model, provider, effort, output_tokens, duration_ms, ttft_ms
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
                };
                groups.entry(key).or_default()
            }
            Operation::ModelSeries { .. } => {
                let time: i64 = row.get(0)?;
                let width = bucket.ok_or(Error("invalid_request"))?;
                points.entry(time.div_euclid(width) * width).or_default()
            }
            Operation::Status => return Err(Error("invalid_request")),
        };
        metrics.push(row.get(4)?, row.get(5)?, row.get(6)?);
        if groups.len() > MAX_GROUPS {
            return Err(Error("query_too_large"));
        }
    }

    Ok(match operation {
        Operation::ModelStats { group_by, .. } => {
            let groups: Vec<_> = groups
                .into_iter()
                .map(|(key, metrics)| json!({"key": key, "metrics": metrics.finish()}))
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
        Operation::Status => return Err(Error("invalid_request")),
    })
}
