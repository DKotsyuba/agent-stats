//! Read model aggregates through the private Agent Stats socket.
use crate::response::Templates;
use rmcp::model::{CallToolResult, ContentBlock};
#[cfg(unix)]
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::Path;

pub const IMPLEMENTED: bool = true;
pub const TEMPLATE: &str = include_str!("../../assets/mcp/tools/model_stats.txt.j2");

/// Discovery contract for model selection and exact model filtering.
pub fn definition() -> Value {
    json!({"name":"model_stats", "description":"Compare model speed, time to first token, call volume and orchestrator acceptance ratings (see rate_result) over a rolling window.",
        "inputSchema":{"type":"object","properties":{
            "window":{"enum":["24h","7d","30d"],"type":"string","default":"24h"},
            "model":{"type":"string","description":"Exact model identifier"},
            "group_by":{"enum":["model","provider+model","harness+model","model+effort"],"type":"string","default":"model"}
        },"additionalProperties":false},
        "annotations":{"readOnlyHint":true,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}})
}

/// Run the read and preserve the full socket envelope in structured output.
pub async fn call(args: Value, templates: &Templates, socket: &Path) -> CallToolResult {
    #[cfg(unix)]
    {
        unix::call(args, templates, socket).await
    }
    #[cfg(not(unix))]
    {
        let _ = (args, templates, socket);
        CallToolResult::error(vec![ContentBlock::text(
            "ERROR unsupported_platform: model_stats requires a Unix socket.",
        )])
    }
}

#[cfg(unix)]
mod unix {
    use super::*;
    use crate::stats::{self, Group, Operation, Window};

    /// MCP exposes a subset of the groupings supported by the socket API.
    #[derive(Default, Deserialize)]
    enum GroupBy {
        #[default]
        #[serde(rename = "model")]
        Model,
        #[serde(rename = "provider+model")]
        Provider,
        #[serde(rename = "harness+model")]
        Harness,
        #[serde(rename = "model+effort")]
        Effort,
    }

    /// Validated parameters for one aggregate read.
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Arguments {
        #[serde(default = "day")]
        window: Window,
        model: Option<String>,
        #[serde(default)]
        group_by: GroupBy,
    }

    /// Default to the last 24 hours.
    fn day() -> Window {
        Window::Day
    }

    /// Required socket response fields used by the text renderer.
    #[derive(Deserialize)]
    struct Data {
        groups: Vec<Row>,
    }

    /// One socket aggregate group.
    #[derive(Deserialize)]
    struct Row {
        key: Key,
        metrics: Metrics,
        ratings: Option<Ratings>,
    }

    /// Orchestrator acceptance counts; absent for groups with no ratings.
    #[derive(Deserialize)]
    struct Ratings {
        rated: u64,
        accepted: u64,
        accepted_first_time: u64,
    }

    /// Identity and optional grouping dimension.
    #[derive(Deserialize)]
    struct Key {
        model: String,
        provider: Option<String>,
        harness: Option<String>,
        /// Outer `Some` marks a `model+effort` group; the inner value is null without effort.
        #[serde(default, deserialize_with = "present")]
        effort: Option<Option<String>>,
    }

    /// Distinguish an explicit null effort from an absent field.
    fn present<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Option<String>>, D::Error> {
        Option::<String>::deserialize(deserializer).map(Some)
    }

    /// Call count and independent speed and TTFT distributions.
    #[derive(Deserialize)]
    struct Metrics {
        count: u64,
        speed_tokens_per_second: Distribution,
        speed_estimated: bool,
        ttft_ms: Distribution,
    }

    /// Nullable quantiles are absent for distributions with no eligible observations.
    #[derive(Deserialize)]
    struct Distribution {
        median: Option<f64>,
        p10: Option<f64>,
        p90: Option<f64>,
    }

    /// Display fields with seconds and rounded token rates prepared for MiniJinja.
    #[derive(Serialize)]
    struct Line {
        model: String,
        dimension: Option<String>,
        count: u64,
        speed: Option<String>,
        range: Option<String>,
        estimated: bool,
        ttft: Option<String>,
        rating: Option<String>,
    }

    /// Rows supplied to the closed text template.
    #[derive(Serialize)]
    struct View {
        rows: Vec<Line>,
    }

    /// Render rows in descending median speed, with unavailable speeds last.
    fn render(templates: &Templates, mut data: Data) -> Result<String, &'static str> {
        let median = |row: &Row| row.metrics.speed_tokens_per_second.median;
        data.groups
            .sort_by(|left, right| match (median(left), median(right)) {
                (Some(left), Some(right)) => right.total_cmp(&left),
                (left, right) => right.is_some().cmp(&left.is_some()),
            });
        let lines: Vec<_> = data
            .groups
            .into_iter()
            .map(|row| {
                let speed = row.metrics.speed_tokens_per_second;
                Line {
                    model: row.key.model,
                    dimension: row.key.provider.or(row.key.harness).or(row
                        .key
                        .effort
                        .map(|effort| effort.unwrap_or_else(|| "no effort".into()))),
                    rating: row
                        .ratings
                        .filter(|ratings| ratings.rated > 0)
                        .map(|ratings| {
                            format!(
                                "accepted {}/{} (first try {})",
                                ratings.accepted, ratings.rated, ratings.accepted_first_time
                            )
                        }),
                    count: row.metrics.count,
                    speed: speed.median.map(|value| format!("{value:.0}")),
                    range: speed
                        .p10
                        .zip(speed.p90)
                        .map(|(p10, p90)| format!("{p10:.0}–{p90:.0}")),
                    estimated: row.metrics.speed_estimated,
                    ttft: row
                        .metrics
                        .ttft_ms
                        .median
                        .map(|value| format!("{:.1}", value / 1000.0)),
                }
            })
            .collect();
        templates.render("model_stats", &View { rows: lines })
    }

    /// Read a bounded socket reply and select safe recovery text for known errors.
    pub(super) async fn call(args: Value, templates: &Templates, socket: &Path) -> CallToolResult {
        let Ok(args) = serde_json::from_value::<Arguments>(args) else {
            return error(
                "ERROR invalid_arguments: use window 24h, 7d or 30d; group_by model, provider+model, harness+model or model+effort; and an optional exact model.",
            );
        };
        let group_by = match args.group_by {
            GroupBy::Model => Group::Model,
            GroupBy::Provider => Group::ProviderModel,
            GroupBy::Harness => Group::HarnessModel,
            GroupBy::Effort => Group::ModelEffort,
        };
        let reply = match stats::call(
            socket,
            Operation::ModelStats {
                window: args.window,
                group_by,
                model: args.model,
            },
        )
        .await
        {
            Ok(reply) => reply,
            Err(error_code) => {
                return error(match error_code.code() {
                    "socket_unavailable"
                    | "socket_read_failed"
                    | "socket_write_failed"
                    | "invalid_frame" => {
                        "ERROR socket_unavailable: cannot read the Agent Stats socket. Next: ask the operator to add your user to agent-stats-read and verify your UID is authorized and the service is running."
                    }
                    _ => {
                        "ERROR read_failed: could not read model statistics from the Agent Stats service."
                    }
                });
            }
        };
        if reply["ok"] != true {
            return error(match reply["error"].as_str() {
                Some("query_too_large") => {
                    "ERROR query_too_large: narrow the window or filter by model."
                }
                Some("invalid_model") => {
                    "ERROR invalid_model: supply a valid exact model identifier."
                }
                Some("busy") => "ERROR busy: the Agent Stats service is busy. Retry the read.",
                Some("invalid_request") => {
                    "ERROR invalid_request: the Agent Stats service rejected the request. Ask the operator to verify the service version."
                }
                _ => "ERROR read_failed: the Agent Stats service could not read model statistics.",
            });
        }
        let Ok(data) = serde_json::from_value::<Data>(reply["data"].clone()) else {
            return error(
                "ERROR invalid_response: the Agent Stats service returned invalid model statistics.",
            );
        };
        let text = render(templates, data).unwrap_or_else(|_| {
            "OK model_stats. Presentation: degraded. Read structuredContent or narrow the window or model filter.
".to_owned()
        });
        let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
        result.structured_content = Some(reply);
        result
    }

    /// Emit a safe tool execution error as a single text block.
    fn error(message: &'static str) -> CallToolResult {
        CallToolResult::error(vec![ContentBlock::text(message)])
    }

    #[cfg(test)]
    #[allow(clippy::unwrap_used, reason = "Fixture and tool result assertions")]
    mod tests {
        use super::*;

        /// Exercise the sorted text, low sample count and estimate indicators together.
        #[test]
        fn model_stats_rendering() {
            let row = |model, provider, count, median, p10, p90, ttft, estimated| {
                json!({
                    "key": {"model": model, "provider": provider},
                    "metrics": {"count": count, "speed_estimated": estimated,
                        "speed_tokens_per_second": {"median": median, "p10": p10, "p90": p90},
                        "ttft_ms": {"median": ttft}}
                })
            };
            let mut groups = json!([
                row("gpt-6", "openai", 20, 120, 90, 150, 1100, true),
                row("small", "anthropic", 4, 60, 30, 80, 500, false),
                row(
                    "claude-sonnet-5-5",
                    "anthropic",
                    412,
                    170,
                    120,
                    230,
                    900,
                    false
                ),
            ]);
            groups[2]["ratings"] = json!({"rated": 22, "accepted": 18, "accepted_first_time": 14});
            groups[1]["ratings"] = json!({"rated": 1, "accepted": 0, "accepted_first_time": 0});
            let data = serde_json::from_value(json!({ "groups": groups })).unwrap();
            let templates = Templates::new(&[("model_stats", TEMPLATE)]).unwrap();
            assert_eq!(
                render(&templates, data).unwrap(),
                include_str!("../../tests/fixtures/model_stats.txt")
            );
        }

        /// `model+effort` groups label the effort, with a distinct label when none was set.
        #[test]
        fn model_effort_dimension() {
            let group = |effort: Value| {
                json!({"key": {"model": "m", "effort": effort},
                    "metrics": {"count": 7, "speed_estimated": false,
                        "speed_tokens_per_second": {"median": null, "p10": null, "p90": null},
                        "ttft_ms": {"median": null}}})
            };
            let data = serde_json::from_value(
                json!({"groups": [group(json!("high")), group(Value::Null)]}),
            )
            .unwrap();
            let templates = Templates::new(&[("model_stats", TEMPLATE)]).unwrap();
            assert_eq!(
                render(&templates, data).unwrap(),
                "m (high): speed unavailable, TTFT unavailable, 7 calls\nm (no effort): speed unavailable, TTFT unavailable, 7 calls\n"
            );
        }

        /// A missing endpoint returns the operator recovery hint.
        #[tokio::test]
        async fn model_stats_socket_error() {
            let templates = Templates::new(&[("model_stats", TEMPLATE)]).unwrap();
            let root = tempfile::tempdir().unwrap();
            let result = call(json!({}), &templates, &root.path().join("missing.sock")).await;
            assert_eq!(result.is_error, Some(true));
            assert!(
                result.content[0]
                    .as_text()
                    .unwrap()
                    .text
                    .contains("ask the operator to add your user to agent-stats-read")
            );
        }
    }
}
