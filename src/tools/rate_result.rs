//! Record the orchestrator's rating of one delegated result through the Agent Stats socket.
use crate::response::Templates;
use rmcp::model::{CallToolResult, ContentBlock};
use serde_json::{Value, json};
use std::path::Path;

pub const IMPLEMENTED: bool = true;
pub const TEMPLATE: &str = include_str!("../../assets/mcp/tools/rate_result.txt.j2");

/// Discovery contract; rating the same `agent_id` again replaces the earlier rating.
pub fn definition() -> Value {
    json!({"name":"rate_result", "description":"Rate a delegated agent's result so model_stats can report acceptance rates. Idempotent per agent_id: a later rating replaces the earlier one. Requires a UID the service allows to rate.",
        "inputSchema":{"type":"object","properties":{
            "agent_id":{"type":"string","description":"Run id of the delegated agent"},
            "model":{"type":"string","description":"Exact model identifier that produced the result"},
            "provider":{"type":"string"},
            "effort":{"type":"string","description":"Reasoning effort the agent ran with"},
            "tier":{"enum":["S","A","B","C","D","E","F"],"type":"string","description":"Task tier"},
            "verdict":{"enum":["accepted","accepted_after_fixes","rejected"],"type":"string"},
            "rounds":{"type":"integer","minimum":0,"maximum":1000,"description":"Fix rounds before acceptance or rejection; 0 for accepted, at least 1 for accepted_after_fixes (default 1)"},
            "reason":{"enum":["wrong_solution","incomplete","missing_access","broke_other","environment","other"],"type":"string","description":"Why fixes were needed or the result was rejected; not allowed for accepted"},
            "checks":{"type":"object","properties":{
                "passed":{"type":"integer","minimum":0},
                "failed":{"type":"integer","minimum":0},
                "not_run":{"type":"integer","minimum":0}},"additionalProperties":false,
                "description":"Verification check counts"},
            "note":{"type":"string","maxLength":200}
        },"required":["agent_id","model","verdict"],"additionalProperties":false},
        "annotations":{"readOnlyHint":false,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}})
}

/// Run the write and render a one-line acknowledgement.
pub async fn call(args: Value, templates: &Templates, socket: &Path) -> CallToolResult {
    #[cfg(unix)]
    {
        unix::call(args, templates, socket).await
    }
    #[cfg(not(unix))]
    {
        let _ = (args, templates, socket);
        CallToolResult::error(vec![ContentBlock::text(
            "ERROR unsupported_platform: rate_result requires a Unix socket. No effect was performed.",
        )])
    }
}

#[cfg(unix)]
mod unix {
    use super::*;
    use crate::stats::{self, Operation, Rating};
    use serde::{Deserialize, Serialize};

    /// Receipt fields returned by the service after the rating committed.
    #[derive(Deserialize)]
    struct Receipt {
        agent_id: String,
        model: String,
        verdict: String,
        rounds: u32,
        replaced: bool,
    }

    /// Acknowledgement values for the closed text template.
    #[derive(Serialize)]
    struct View {
        agent_id: String,
        model: String,
        verdict: String,
        tier: Option<&'static str>,
        rounds: u32,
        reason: Option<&'static str>,
        replaced: bool,
    }

    /// Select recovery text for a local validation code; nothing was sent.
    fn invalid(code: &str) -> Option<&'static str> {
        Some(match code {
            "invalid_agent_id" => {
                "ERROR invalid_agent_id: use the Run id (letters, digits and - _ . : [ ] /, up to 128 characters). No effect was performed."
            }
            "invalid_model" => {
                "ERROR invalid_model: supply the exact model identifier. No effect was performed."
            }
            "invalid_provider" | "invalid_effort" => {
                "ERROR invalid_arguments: provider and effort use letters, digits and - _ . : [ ] /, up to 128 characters. No effect was performed."
            }
            "invalid_rounds" => {
                "ERROR invalid_rounds: accepted takes rounds 0; accepted_after_fixes takes 1 to 1000 (default 1); rejected takes 0 to 1000. No effect was performed."
            }
            "invalid_reason" => {
                "ERROR invalid_reason: reason applies only to accepted_after_fixes or rejected. No effect was performed."
            }
            "invalid_note" => {
                "ERROR invalid_note: use at most 200 characters with no control characters. No effect was performed."
            }
            _ => return None,
        })
    }

    /// Validate, send and acknowledge one rating.
    pub(super) async fn call(args: Value, templates: &Templates, socket: &Path) -> CallToolResult {
        let Ok(rating) = serde_json::from_value::<Rating>(args) else {
            return error(
                "ERROR invalid_arguments: agent_id, model and verdict are required; tier S to F; verdict accepted, accepted_after_fixes or rejected; reason wrong_solution, incomplete, missing_access, broke_other, environment or other; rounds an integer of at least 0; checks {passed, failed, not_run}; note up to 200 characters. No effect was performed.",
            );
        };
        if let Err(code) = rating.validate() {
            return error(invalid(code.code()).unwrap_or(
                "ERROR invalid_arguments: the rating was rejected before sending. No effect was performed.",
            ));
        }
        let (tier, reason) = (
            rating.tier.map(stats::Tier::as_str),
            rating.reason.map(stats::Reason::as_str),
        );
        let reply = match stats::call(socket, Operation::RateResult(rating)).await {
            Ok(reply) => reply,
            Err(code) => {
                return error(match code.code() {
                    "socket_unavailable" => {
                        "ERROR socket_unavailable: cannot reach the Agent Stats socket. No effect was performed. Next: ask the operator to verify the service is running and your UID may use the socket."
                    }
                    _ => {
                        "ERROR outcome_unknown: the Agent Stats service did not confirm the rating. Next: call rate_result again with the same arguments; it is idempotent."
                    }
                });
            }
        };
        if reply["ok"] != true {
            return error(match reply["error"].as_str() {
                Some("not_permitted") => {
                    "ERROR not_permitted: your UID may not write ratings. No effect was performed. Next: ask the operator to add it to rating_uids (or allowed_uids when rating_uids is unset) in the service configuration."
                }
                Some("busy") => {
                    "ERROR busy: the Agent Stats service is busy. No effect was performed. Next: call rate_result again."
                }
                Some("write_timeout" | "write_failed" | "database_busy" | "query_timeout") => {
                    "ERROR outcome_unknown: the Agent Stats service did not confirm the rating. Next: call rate_result again with the same arguments; it is idempotent."
                }
                Some("disk_full") => {
                    "ERROR disk_full: the Agent Stats database is full. No effect was performed. Next: ask the operator to free space."
                }
                Some("invalid_request" | "unsupported_version") => {
                    "ERROR invalid_request: the Agent Stats service rejected the request. Ask the operator to verify the service version, which must support rate_result."
                }
                Some(code) => invalid(code).unwrap_or(
                    "ERROR rating_failed: the Agent Stats service could not record the rating. No effect is confirmed.",
                ),
                None => "ERROR rating_failed: the Agent Stats service could not record the rating. No effect is confirmed.",
            });
        }
        let Ok(receipt) = serde_json::from_value::<Receipt>(reply["data"].clone()) else {
            return error(
                "ERROR invalid_response: the Agent Stats service returned an unreadable receipt; the rating was probably recorded. Next: call ratings to confirm.",
            );
        };
        let view = View {
            agent_id: receipt.agent_id,
            model: receipt.model,
            verdict: receipt.verdict,
            tier,
            rounds: receipt.rounds,
            reason,
            replaced: receipt.replaced,
        };
        let text = templates.render("rate_result", &view).unwrap_or_else(|_| {
            "OK rate_result: rating recorded. Presentation: degraded. Read structuredContent.\n"
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
    #[allow(clippy::unwrap_used, reason = "Fixture and tool result assertions")]
    mod tests {
        use super::*;

        /// The acknowledgement names the verdict, tier, rounds, reason and replacement.
        #[test]
        fn acknowledgement_text() {
            let templates = Templates::new(&[("rate_result", TEMPLATE)]).unwrap();
            let view = |tier, reason, replaced| View {
                agent_id: "agent_7".into(),
                model: "claude-sonnet-5-5".into(),
                verdict: "rejected".into(),
                tier,
                rounds: 2,
                reason,
                replaced,
            };
            assert_eq!(
                templates
                    .render(
                        "rate_result",
                        &view(Some("C"), Some("wrong_solution"), true)
                    )
                    .unwrap(),
                "OK rate_result: agent_7 rejected (claude-sonnet-5-5, tier C, rounds 2, reason wrong_solution). Replaced the earlier rating.\n"
            );
            assert_eq!(
                templates
                    .render("rate_result", &view(None, None, false))
                    .unwrap(),
                "OK rate_result: agent_7 rejected (claude-sonnet-5-5, rounds 2). Recorded.\n"
            );
        }

        /// Invalid input and a missing socket never reach a rating write.
        #[tokio::test]
        async fn rejects_before_sending() {
            let templates = Templates::new(&[("rate_result", TEMPLATE)]).unwrap();
            let root = tempfile::tempdir().unwrap();
            let socket = root.path().join("missing.sock");
            let text = |result: &CallToolResult| result.content[0].as_text().unwrap().text.clone();
            let base = json!({"agent_id": "a1", "model": "m", "verdict": "accepted"});
            let bad = call(json!({"agent_id": "a1"}), &templates, &socket).await;
            assert!(text(&bad).starts_with("ERROR invalid_arguments"));
            let mut rounds = base.clone();
            rounds["rounds"] = json!(2);
            let bad = call(rounds, &templates, &socket).await;
            assert!(text(&bad).starts_with("ERROR invalid_rounds"));
            let down = call(base, &templates, &socket).await;
            assert_eq!(down.is_error, Some(true));
            assert!(text(&down).starts_with("ERROR socket_unavailable"));
        }
    }
}
