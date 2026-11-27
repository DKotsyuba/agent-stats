# model_stats

Read model performance over the last `24h` (default), `7d` or `30d`.
Supply `model` for an exact model identifier. Set `group_by` to `model`
(default), `provider+model`, `harness+model` or `model+effort`.

Text lists groups from highest median output speed to lowest. Speed is in
tokens per second and time to first token (TTFT) is in seconds. Groups
with fewer than five calls show their count and a “too few samples” label.
A tilde and `(est.)` mark speeds that include estimated durations.
Groups with ratings from [`rate_result`](rate_result.md) end with
`· accepted 18/22 (first try 14)`: accepted (including after fixes) over rated
results, and how many were accepted with no fixes. Ratings count when their last
change falls inside the window. `harness+model` groups carry no ratings, and a
rated model with no observed calls shows `0 calls`. Groups without ratings omit
the suffix. The structured `ratings` object adds `rated`, `accepted`,
`accepted_first_time`, `rejected`, `accepted_rate`, `accepted_first_time_rate`,
`mean_rounds` and up to three `top_reasons` over results that were not accepted
first time. The
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
