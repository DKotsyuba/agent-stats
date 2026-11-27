# model_stats

Read model performance over the last `24h` (default), `7d` or `30d`.
Supply `model` for an exact model identifier. Set `group_by` to `model`
(default), `provider+model` or `harness+model`.

Text lists groups from highest median output speed to lowest. Speed is in
tokens per second and time to first token (TTFT) is in seconds. Groups
with fewer than five calls show their count and a “too few samples” label.
A tilde and `(est.)` mark speeds that include estimated durations. The
complete versioned socket response in `structuredContent` includes sample
counts and unrounded distributions. When the text exceeds the presentation
budget, the tool still returns the complete structured result.

Run the frontend with `agent-stats mcp`. It uses
`/run/agent-stats/read.sock` by default and accepts `--socket` for a separate
instance. If access fails, ask the operator to add your user to
`agent-stats-read` and verify the service authorizes your UID.

The frontend checks the service version at initialization and before tool
calls, at most once every 30 seconds. On a confirmed mismatch, it sends one
`notifications/tools/list_changed` and tool calls ask you to reconnect. Use
`/mcp → reconnect` in Claude Code or restart the session in Codex.
