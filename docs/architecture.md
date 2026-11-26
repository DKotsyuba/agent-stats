# Architecture

Agent Stats runs as a resident service with private SQLite state. A single
authenticated loopback HTTP/1 listener accepts OTLP/HTTP JSON at `POST /v1/logs`.
It verifies `x-stats-token` before reading the body. HTTP admission caps headers,
body bytes, records, concurrent requests, and queued write batches. A bounded
connection pool also limits idle or incomplete connections. At its hard
connection limit, excess connections are closed before parsing. Request
overload returns 429, size or record overflow 413, and unsupported content
type or encoding 415. Corrupt JSON returns 400 and bad authentication 401.

The normalizer selects only the `claude_code.api_request` event body name and
projects its allowlisted fields before passing rows to the writer. Model,
request ID and event timestamp are required. Invalid optional fields are
absent, so a call can count even when a metric is unavailable. Provider is
`anthropic` for an explicit standard endpoint, `custom` for a custom endpoint
with no matching configured label, or `unknown` with no valid endpoint flag.
The phase 0 sample includes only the boolean endpoint flag. A configured label
applies when a custom record also supplies `anthropic.base_url`. URLs and raw
request IDs never enter SQLite. Request IDs are HMAC-SHA-256 digests keyed
with a separate service secret. The harness and digest together deduplicate
retries, including across service restarts with the same key. Timestamps are
Unix nanoseconds. Future timestamps are invalid and timestamps older than
30 days are expired.

The writer commits one batch atomically and responds with HTTP 200 only after
successful persistence or when the batch has no eligible records. Database
write errors, including disk full, return 503 and leave the writer available
for retries. A 256 MiB SQLite main-file page limit bounds retained data volume.
Expiry deletes up to 512 rows per pass, once per second when the writer has
capacity, and preserves the inclusive 30-day boundary. Reads enforce the
current rolling window regardless of the expiry pass. SQLite can reuse freed
pages while the file stays allocated.

## Hub socket protocol v1

Connect to `/run/agent-stats/read.sock`. The service authorizes the kernel's peer
UID before reading the request. The service UID and additional configured UIDs
can read. Filesystem permissions also apply. Each connection accepts one
UTF-8 JSON request terminated by a newline (up to 4096 bytes including LF).
It returns one JSON line up to 1 MiB including LF and closes the connection.
Unauthorized peers are closed without a reply.

```json
{"version":1,"op":"model_stats","window":"24h","group_by":"model"}
{"version":1,"op":"model_series","model":"claude-haiku-5-5","bucket":"hour","window":"7d"}
{"version":1,"op":"status"}
```

- Success envelope `{"version":1,"ok":true,"data":{...}}`.
- Error envelope `{"version":1,"ok":false,"error":"query_too_large"}`.
  Bad frames or arguments, unsupported versions, reader saturation, timeout
  and storage failures use the same envelope with their own code.
- `model_stats` returns `window`, `group_by` and `groups`. Each group has a
  `key` with `model` and the requested additional dimension, and a `metrics`
  object. Windows are `24h`, `7d`, `30d`. Groupings are `model`,
  `provider+model` and `model+effort`. Missing effort is JSON null.
- `model_series` returns `window`, `model`, `bucket` and ascending `buckets`,
  each with `start_unix_ns` as a decimal string and `metrics`. Buckets are
  UTC-aligned `hour` or `day`, with empty buckets included. The window may
  cover part of the first or last bucket.
- `status` returns `counters` and `last_sample_time_unix_ns` as a decimal
  string or null. Counters are process-local and distinguish rejection,
  dropped connections, invalid and ignored records, deduplication, commits,
  expiry and write failures.

The shared `metrics` object contains the total call `count` and two
distributions, `speed_tokens_per_second` and `ttft_ms`. Each distribution
has its own eligible `count`, plus `mean`, `median`, `min`, `max`, `p10`
and `p90` (all null when that distribution is empty). Percentiles use exact
raw samples with linear interpolation at rank `(n−1)p`. Speed is a
client-observed estimate of `output_tokens × 1000 / (duration_ms − ttft_ms)`
when output is positive, TTFT is nonnegative, and duration exceeds TTFT.
TTFT is independent of speed. Reads exceeding 100,000 eligible rows for the
request or 1024 groups fail with `query_too_large`, preserving exactness.

## Template delivery

The binary retains stdio MCP identity discovery and the template's immutable
single-binary delivery commands. Service installation is described separately
in [Install Agent Stats](install.md). CLI reads use the same socket protocol
as the Hub. This version adds no MCP tools.
