//! List recent orchestrator result ratings through the private Agent Stats socket.
use crate::response::Templates;
use rmcp::model::{CallToolResult, ContentBlock};
use serde_json::{Value, json};
use std::path::Path;

pub const IMPLEMENTED: bool = true;
pub const TEMPLATE: &str = include_str!("../../assets/mcp/tools/ratings.txt.j2");

/// Discovery contract for the bounded, newest-first rating page.
pub fn definition() -> Value {
    json!({"name":"ratings", "description":"List recent result ratings recorded with rate_result, newest first, up to 90 days.",
        "inputSchema":{"type":"object","properties":{
            "model":{"type":"string","description":"Exact model identifier"},
            "limit":{"type":"integer","minimum":1,"maximum":20,"default":10},
            "cursor":{"type":"string","description":"next_cursor from the previous page"}
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
            "ERROR unsupported_platform: ratings requires a Unix socket.",
        )])
    }
}

#[cfg(unix)]
mod unix {
    use super::*;
    use crate::stats::{self, Operation};
    use serde::{Deserialize, Serialize};

    /// Validated parameters for one page read.
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Arguments {
        model: Option<String>,
        #[serde(default = "page")]
        limit: u32,
        cursor: Option<String>,
    }

    /// Default text page size; longer pages exceed the text budget.
    fn page() -> u32 {
        10
    }

    /// Maximum text page size.
    const MAX_TEXT_PAGE: u32 = 20;
    /// Longest note shown in text; structured output keeps the whole note.
    const NOTE_CHARS: usize = 80;

    /// Required socket response fields used by the text renderer.
    #[derive(Deserialize)]
    struct Data {
        ratings: Vec<Stored>,
        next_cursor: Option<String>,
    }

    /// One stored rating.
    #[derive(Deserialize)]
    struct Stored {
        agent_id: String,
        model: String,
        provider: Option<String>,
        effort: Option<String>,
        tier: Option<String>,
        verdict: String,
        rounds: u64,
        reason: Option<String>,
        checks: Option<Checks>,
        note: Option<String>,
        updated_unix_ns: String,
    }

    /// Verification check counts.
    #[derive(Deserialize)]
    struct Checks {
        passed: u64,
        failed: u64,
        not_run: u64,
    }

    /// Display fields prepared for MiniJinja.
    #[derive(Serialize)]
    struct Line {
        agent_id: String,
        model: String,
        context: Option<String>,
        tier: Option<String>,
        verdict: String,
        rounds: u64,
        reason: Option<String>,
        checks: Option<String>,
        when: String,
        note: Option<String>,
    }

    /// Rows and continuation supplied to the closed text template.
    #[derive(Serialize)]
    struct View {
        rows: Vec<Line>,
        next: Option<String>,
    }

    /// Format Unix nanoseconds as `YYYY-MM-DD HH:MMZ` in UTC.
    fn utc_minute(ns: i64) -> String {
        let minutes = ns.div_euclid(60_000_000_000);
        let (days, rest) = (minutes.div_euclid(1440), minutes.rem_euclid(1440));
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z - era * 146_097;
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = doy - (153 * mp + 2) / 5 + 1;
        let month = if mp < 10 { mp + 3 } else { mp - 9 };
        let year = yoe + era * 400 + i64::from(month <= 2);
        format!(
            "{year:04}-{month:02}-{day:02} {:02}:{:02}Z",
            rest / 60,
            rest % 60
        )
    }

    /// Render the page, newest first as returned by the service.
    fn render(templates: &Templates, data: Data) -> Result<String, &'static str> {
        let rows = data
            .ratings
            .into_iter()
            .map(|row| Line {
                context: match (row.provider, row.effort) {
                    (Some(provider), Some(effort)) => Some(format!("{provider}, {effort}")),
                    (provider, effort) => provider.or(effort),
                },
                checks: row.checks.map(|checks| {
                    format!(
                        "{} passed/{} failed/{} not run",
                        checks.passed, checks.failed, checks.not_run
                    )
                }),
                when: row
                    .updated_unix_ns
                    .parse()
                    .map_or_else(|_| "unknown time".into(), utc_minute),
                note: row.note.map(|note| {
                    if note.chars().count() > NOTE_CHARS {
                        format!("{}…", note.chars().take(NOTE_CHARS).collect::<String>())
                    } else {
                        note
                    }
                }),
                agent_id: row.agent_id,
                model: row.model,
                tier: row.tier,
                verdict: row.verdict,
                rounds: row.rounds,
                reason: row.reason,
            })
            .collect();
        templates.render(
            "ratings",
            &View {
                rows,
                next: data.next_cursor,
            },
        )
    }

    /// Read a bounded socket reply and select safe recovery text for known errors.
    pub(super) async fn call(args: Value, templates: &Templates, socket: &Path) -> CallToolResult {
        let Ok(args) = serde_json::from_value::<Arguments>(args) else {
            return error(
                "ERROR invalid_arguments: use an optional exact model, limit 1 to 20 and the cursor from the previous page.",
            );
        };
        if !(1..=MAX_TEXT_PAGE).contains(&args.limit) {
            return error("ERROR invalid_limit: use limit 1 to 20.");
        }
        let operation = Operation::Ratings {
            model: args.model,
            limit: Some(args.limit),
            cursor: args.cursor,
        };
        let reply = match stats::call(socket, operation).await {
            Ok(reply) => reply,
            Err(code) => {
                return error(match code.code() {
                    "socket_unavailable"
                    | "socket_read_failed"
                    | "socket_write_failed"
                    | "invalid_frame" => {
                        "ERROR socket_unavailable: cannot read the Agent Stats socket. Next: ask the operator to add your user to agent-stats-read and verify your UID is authorized and the service is running."
                    }
                    _ => "ERROR read_failed: could not read ratings from the Agent Stats service.",
                });
            }
        };
        if reply["ok"] != true {
            return error(match reply["error"].as_str() {
                Some("invalid_model") => {
                    "ERROR invalid_model: supply a valid exact model identifier."
                }
                Some("invalid_cursor") => {
                    "ERROR invalid_cursor: pass the next_cursor from the previous page unchanged."
                }
                Some("invalid_limit") => "ERROR invalid_limit: use limit 1 to 20.",
                Some("not_permitted") => {
                    "ERROR not_permitted: your UID may not read. Next: ask the operator to add it to allowed_uids."
                }
                Some("busy") => "ERROR busy: the Agent Stats service is busy. Retry the read.",
                Some("invalid_request") => {
                    "ERROR invalid_request: the Agent Stats service rejected the request. Ask the operator to verify the service version, which must support ratings."
                }
                _ => "ERROR read_failed: the Agent Stats service could not read ratings.",
            });
        }
        let Ok(data) = serde_json::from_value::<Data>(reply["data"].clone()) else {
            return error(
                "ERROR invalid_response: the Agent Stats service returned invalid ratings.",
            );
        };
        let text = render(templates, data).unwrap_or_else(|_| {
            "OK ratings. Presentation: degraded. Read structuredContent or lower the limit.\n"
                .to_owned()
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
    #[allow(clippy::unwrap_used, reason = "Fixture assertions")]
    mod tests {
        use super::*;

        /// Dates convert across leap days and pages show a continuation cursor.
        #[test]
        fn ratings_rendering() {
            assert_eq!(utc_minute(0), "1970-01-01 00:00Z");
            assert_eq!(
                utc_minute(1_709_164_800_000_000_000 + 61 * 60_000_000_000),
                "2024-02-29 01:01Z"
            );
            let data: Data = serde_json::from_value(json!({"ratings": [
                {"agent_id": "run_2", "model": "claude-sonnet-5-5", "provider": "anthropic", "effort": "high",
                 "tier": "C", "verdict": "rejected", "rounds": 2, "reason": "wrong_solution",
                 "checks": {"passed": 5, "failed": 1, "not_run": 0}, "note": "x".repeat(100),
                 "updated_unix_ns": "1795000000000000000"},
                {"agent_id": "run_1", "model": "gpt-6", "provider": null, "effort": null, "tier": null,
                 "verdict": "accepted", "rounds": 0, "reason": null, "checks": null, "note": null,
                 "updated_unix_ns": "1794990000000000000"}
            ], "next_cursor": "1794990000000000000:run_1"})).unwrap();
            let templates = Templates::new(&[("ratings", TEMPLATE)]).unwrap();
            assert_eq!(
                render(&templates, data).unwrap(),
                include_str!("../../tests/fixtures/ratings.txt")
            );
        }
    }
}
