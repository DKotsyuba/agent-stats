//! Authoritative Rust tool registry. JSON discovery is an exported snapshot, not a second source.
use crate::response::Templates;
use mcp_presentation::Renderer;
use rmcp::model::{CallToolResult, ContentBlock};
use serde_json::{Value, json};
#[path = "model_stats.rs"]
mod tool_model_stats;
#[path = "rate_result.rs"]
mod tool_rate_result;
#[path = "ratings.rs"]
mod tool_ratings;
// xtask:modules

pub fn definitions() -> Vec<Value> {
    vec![
        json!({"name":"get_status", "description":"Report the product identity and scaffold qualification status. Read-only; does not access files or external services.",
        "inputSchema":{"type":"object","properties":{},"additionalProperties":false},
        "annotations":{"readOnlyHint":true,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}}),
        tool_model_stats::definition(),
        tool_rate_result::definition(),
        tool_ratings::definition(),
        // xtask:definitions
    ]
}
pub fn templates() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "invalid_arguments",
            "ERROR invalid_arguments: get_status accepts an empty argument object.\n",
        ),
        ("model_stats", tool_model_stats::TEMPLATE),
        ("rate_result", tool_rate_result::TEMPLATE),
        ("ratings", tool_ratings::TEMPLATE),
        // xtask:templates
    ]
}
pub fn incomplete() -> Vec<&'static str> {
    let statuses: &[(&str, bool)] = &[
        ("get_status", true),
        ("model_stats", tool_model_stats::IMPLEMENTED),
        ("rate_result", tool_rate_result::IMPLEMENTED),
        ("ratings", tool_ratings::IMPLEMENTED),
        // xtask:readiness
    ];
    statuses
        .iter()
        .filter(|(_, implemented)| !implemented)
        .map(|(name, _)| *name)
        .collect()
}
pub async fn call(
    name: &str,
    args: Value,
    identity: &Renderer,
    _templates: &Templates,
    socket: &std::path::Path,
) -> Option<CallToolResult> {
    match name {
        "get_status" => {
            if !args.as_object().is_some_and(|a| a.is_empty()) {
                let text = _templates
                    .render("invalid_arguments", &())
                    .unwrap_or_else(|_| {
                        "ERROR invalid_arguments: no effect performed. Presentation: degraded.\n"
                            .to_owned()
                    });
                let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
                result.is_error = Some(true);
                return Some(result);
            }
            let reply = identity.identity(
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION"),
                crate::qualification(),
            );
            let mut result =
                CallToolResult::success(vec![ContentBlock::text(reply.text().to_owned())]);
            result.is_error = Some(reply.is_error());
            Some(result)
        }
        "model_stats" => Some(tool_model_stats::call(args, _templates, socket).await),
        "rate_result" => Some(tool_rate_result::call(args, _templates, socket).await),
        "ratings" => Some(tool_ratings::call(args, _templates, socket).await),
        // xtask:routes
        _ => None,
    }
}
