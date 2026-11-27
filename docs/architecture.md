# Architecture

Agent Stats runs as a resident service with private SQLite state. A single
authenticated loopback HTTP/1 listener accepts OTLP/HTTP JSON at `POST /v1/logs`.
It verifies `x-stats-token` before reading the body. HTTP admission caps headers,
body bytes, records, concurrent requests, and queued write batches. A bounded
connection pool also limits idle or incomplete connections. At its hard
connection limit, excess connections are closed before parsing. Request
overload returns 429, size or record overflow 413, and unsupported content
type or encoding 415. Corrupt JSON returns 400 and bad authentication 401.

For Claude Code, the normalizer selects the `claude_code.api_request` event body name and
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

For Codex, the normalizer selects `codex.sse_event` records with
`event.kind=response.completed` and reads the model, effort, token counts,
TTFT and RFC 3339 event timestamp from allowlisted attributes. It uses
`codex` as the harness and `openai` as the provider. A keyed digest of
conversation ID, event timestamp and model deduplicates completions. The
raw conversation ID is discarded before the writer receives the event.
The writer tracks `codex.websocket_request` events in bounded memory for
up to 15 minutes. A completion has an estimated duration when one strictly
earlier request matches its model and conversation since the previous
completion. Ambiguous or missing pairs yield a null duration. The store
records `estimated` for Codex pairs and `exact` for Claude Code rows,
including rows migrated from the original schema.

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

Result ratings add `rate_result` and `ratings` (see [Result ratings](#result-ratings)).

- Success envelope `{"version":1,"ok":true,"data":{...}}`.
- Error envelope `{"version":1,"ok":false,"error":"query_too_large"}`.
  Bad frames or arguments, unsupported versions, reader saturation, timeout
  and storage failures use the same envelope with their own code.
- `model_stats` returns `window`, `group_by` and `groups`. Each group has a
  `key` with `model` and the requested additional dimension, and a `metrics`
  object. Windows are `24h`, `7d`, `30d`. Groupings are `model`,
  `provider+model`, `model+effort` and `harness+model`. Missing effort is JSON null.
  An optional `model` request field filters by exact identifier before aggregation.
- `model_series` returns `window`, `model`, `bucket` and ascending `buckets`,
  each with `start_unix_ns` as a decimal string and `metrics`. Buckets are
  UTC-aligned `hour` or `day`, with empty buckets included. The window may
  cover part of the first or last bucket.
- `status` returns `service_version`, `counters` and `last_sample_time_unix_ns` as a decimal
  string or null. Counters are process-local and distinguish rejection,
  dropped connections, invalid and ignored records, deduplication, commits,
  expiry and write failures.

The shared `metrics` object contains the total call `count` and two
distributions, `speed_tokens_per_second` and `ttft_ms`. Each distribution
has its own eligible `count`, plus `mean`, `median`, `min`, `max`, `p10`
and `p90` (all null when that distribution is empty). Percentiles use exact
raw samples with linear interpolation at rank `(n−1)p`. Speed is a
client-observed estimate of `output_tokens × 1000 / (duration_ms − ttft_ms)`
when output is at least 100 tokens, duration minus TTFT is at least 50 ms and at least a fifth of the
duration (shorter answers are dominated by the end-of-stream tail; a smaller share means the body arrived
as one burst, e.g. after unstreamed thinking or a buffering gateway).
The metrics object includes `speed_estimated`, true when any contributing
speed sample uses an estimated duration. TTFT is independent of speed.
Reads exceeding 100,000 eligible rows for the
request or 1024 groups fail with `query_too_large`, preserving exactness.

## Result ratings

Orchestrators record how a delegated result turned out. A rating is keyed by
the delegated Run id and holds `model`, optional `provider`, `effort` and
`tier` (`S` to `F`), a `verdict` (`accepted`, `accepted_after_fixes` or
`rejected`), `rounds` (fix rounds), an optional `reason`, optional `checks`
counts and a note of at most 200 characters. The `ratings` table is created
additively next to `samples` and is written only through the single writer.
A later rating of the same Run id replaces the earlier row, keeps its
first-rated time and sets the last change time. Expiry deletes ratings whose
last change is older than 90 days (inclusive boundary, 512 per pass), separately
from the 30-day sample window.

`rate_result` is the only write on the socket. It is refused with `not_permitted`
unless the peer UID may rate: the service user and `allowed_uids`, or exactly the
UIDs in `rating_uids` when that list is configured (the service user is then not
implicit). A UID listed only in `rating_uids` can rate but not read. The ingest
token is not involved. The request is validated before it is queued; the reply
reports `replaced` after the commit. A full writer queue returns `busy` with no
effect.

```json
{"version":1,"op":"rate_result","agent_id":"run-1","model":"claude-sonnet-5-5","verdict":"accepted_after_fixes","rounds":2,"reason":"incomplete","tier":"C"}
{"version":1,"op":"ratings","model":"claude-sonnet-5-5","limit":20,"cursor":"1794990000000000000:run-1"}
```

`ratings` returns up to 100 rows (default 20), newest change first, and a
`next_cursor` of `<updated_unix_ns>:<agent_id>` or null.

`model_stats` adds a `ratings` object to groups that have ratings changed inside
the window, including groups with no observed calls (`count` 0). `model`,
`provider+model` and `model+effort` groupings are supported; ratings have no
harness, so `harness+model` groups report none. A rating without a provider
groups as `unknown` under `provider+model`. The object has `rated`, `accepted`
(including after fixes), `accepted_first_time` (verdict `accepted`), `rejected`,
`accepted_rate`, `accepted_first_time_rate`, `mean_rounds` over all rated results
and `top_reasons`, up to three `{reason, count}` entries over results that were
not accepted first time, ordered by count then name. Groups without ratings omit
the object. Status counters add `ratings_recorded` and `denied_ratings`.

## Template delivery

The binary retains stdio MCP identity discovery and the template's immutable
single-binary delivery commands. Service installation is described separately
in [Install Agent Stats](install.md). CLI reads use the same socket protocol
as the Hub. The MCP frontend uses the same socket to serve
[`model_stats`](tools/model_stats.md), [`rate_result`](tools/rate_result.md) and
[`ratings`](tools/ratings.md) and detect service version mismatches.
