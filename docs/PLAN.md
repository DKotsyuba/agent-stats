# Agent Stats — technical plan (v2, 2026-11-26)

Merged from the coordinator draft and two independent reviews: Astra (gpt-6.2-astra,
stats-research/AGENT-STATS-PLAN-ASTRA.md) and GLM-5.4 (stats-review-glm54/AGENT-STATS-PLAN-GLM54.md).
Evidence: stats-research/TELEMETRY-RESEARCH.md (docs/source only), stats-research/CLAUDE-OTEL-PROBE.md
(live probe, claude 2.1.326 — wins where they differ).

## Goal and boundaries
- Separate small Rust service from agent-mcp-template, Agent Quota pattern (resident, private SQLite,
  versioned Unix-socket API with peer-UID auth). One job: per-model-call performance.
- Hub shows per model: tokens/s and time to first token — count, mean, median, min, max, p10/p90 — for
  24 h / 7 d / 30 d, plus a trend chart.
- No code changes in Run, Pool, Bridge, Quota. Enabling = environment settings only; rollback = remove them.

## v1 scope: Claude Code harness only
- Covers Anthropic accounts, GLM (Z.ai), OpenCode Go models and every Run/Pool worker on the claude-code
  harness. Source: OTel log event `claude_code.api_request` — model, effort, input/output/cache tokens,
  `duration_ms`, `ttft_ms`, `request_id`, timestamp on ONE event (live probe).
- Codex is v2: `codex.api_request` (duration) and `codex.sse_event` (tokens, ttft) share no id, so pairing
  by process/time is unreliable under retries and concurrency (both reviews); and Run reserves the `[otel]`
  section of Codex worker config (agent-run-config/src/config.rs:853, :900-903 — GLM review), so Run
  workers cannot be reached by config. v2 needs a pinned live capture of Codex 0.167.x first.

## Metric definitions
- speed = output_tokens × 1000 / (duration_ms − ttft_ms); only when output > 0, ttft ≥ 0 and
  duration > ttft. Labelled an estimate (client-observed). TTFT is its own metric.
- Every valid sample counts; invalid/missing values only drop the metric they affect.
- Reasoning tokens are not added to output unless the harness convention is verified.

## Ingest
- OTLP/HTTP logs, `POST /v1/logs` only, on a configurable loopback port (default 4318 if free).
  Accept `http/json` (simplest; protobuf optional later). This is the stack's first inbound HTTP
  listener — owner decision 1.
- Static per-host ingest token in a header (`x-stats-token`), 0600 file on the server, ingest-only;
  never valid for reads. Any local writer holding it can fake samples — accepted (owner decision 2).
- Authenticate before decoding; cap body size, records per batch, concurrency, write backlog; reject
  overload fast; count drops. Bounded CPU/memory/disk independent of Run.
- Privacy: allowlist-only decode. Keep: timestamp, harness, provider label, model, effort, token counts,
  duration, ttft, keyed digest of request_id (dedup). Drop before persistence: prompts/responses,
  email, user/account/org ids, session ids (owner decision 6), paths, URLs, headers, host attributes,
  every non-`api_request` event (emitted by default, dropped unopened).

## Enabling sessions
- Variables: CLAUDE_CODE_ENABLE_TELEMETRY=1, OTEL_LOGS_EXPORTER=otlp, OTEL_METRICS_EXPORTER=none,
  OTEL_EXPORTER_OTLP_PROTOCOL=http/json, OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=http://127.0.0.1:<port>/v1/logs,
  OTEL_EXPORTER_OTLP_HEADERS=x-stats-token=<token>. Prompt/response logging stays off (default).
- Run workers: via the Run service unit environment — the supervisor passes its env to workers and lets
  OTEL_* through (agent-run-core/src/supervisor.rs:1121, agent-run-adapters/src/redact.rs:12-14), and the
  token stays out of provider config readable by tool shells (GLM review). Alternative `[environments]`
  in Run config exists but is undocumented (owner decision 5).
- Owner/agent sessions on all accounts: Crew env for the claude runtime.

## Storage and reads
- One raw table, 30 days (owner decision 4), unique (harness, request digest), indexed by time;
  expiry in small batches. No rollups in v1.
- Exact percentiles from raw samples per window (linear interpolation at (n−1)p); null for empty groups;
  bounded query size.
- Read API on the private socket: `model_stats(window, group_by)`, `model_series(model, bucket, window)`.
  MCP read tools optional, not in v1.

## Hub
- Hub server reads Stats over its socket like Quota (server/src producer pattern), short timeout,
  "unavailable"/"too few samples" states. New page «Скорость моделей»: table per model + trend chart.
  (GLM review claimed the Hub server does not exist — it read a stale checkout; the server is live.)

## Phases
0. **Safety gate (before any shared rollout):** one disposable Claude session with telemetry pointed at
   (a) nothing listening, (b) a listener that hangs, (c) a slow listener — measure first output, turn
   time, exit/flush delay (static evidence: 5 s flush / 2 s shutdown races in claude 2.1.311). Also
   capture real GLM and OpenCode Go events (model labels through custom endpoints). Owner sets the
   acceptable overhead.
1. Service: receiver, normalizer, SQLite, socket API; tests with sanitized fixtures, duplicates, oversize,
   corrupt input, disk full, retention, exact percentiles; template gate. Install as a systemd service.
2. Canary: one opt-in Run worker + one owner session; verify rows arrive and aggregates look right.
3. Hub page.
4. Enable for all claude-code sessions (Run unit env + Crew). v2: Codex after its capture.

## Cut from v1
Account attribution, cost, Run session joins, per-agent speed line, raw-call browsing, MCP tools,
transcript backfill, yearly rollups, traces, Codex.

## Owner decisions
1. Accept one authenticated loopback HTTP listener on dobi.
2. Static ingest token (fakeable by local writers) — accept.
3. Codex deferred to v2.
4. Raw 30 days with exact percentiles (no long-term history in v1).
5. Run workers via the Run unit environment (vs Run config `[environments]`).
6. Do not store session ids.
7. Acceptable latency overhead from phase 0.
