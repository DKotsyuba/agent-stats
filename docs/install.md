# Install Agent Stats on Linux

Use a built binary from the reviewed source and install it at
`/usr/local/bin/agent-stats`. The unit in
[`scripts/systemd/agent-stats.service`](../scripts/systemd/agent-stats.service)
can be copied to `/etc/systemd/system/agent-stats.service`. Provision the
dedicated system user `agent-stats` and group `agent-stats-read` first.
For a Hub running under a different UID, give its process the supplemental
`agent-stats-read` group and add its numeric UID to `allowed_uids` in
[`examples/agent-stats.toml`](../examples/agent-stats.toml). The service UID
can always read. The systemd unit creates the private state and socket directories.

Create `/etc/agent-stats` as a root-owned directory with group
`agent-stats-read` and mode 0750. Install the example there as
`agent-stats.toml` with owner root, group `agent-stats-read` and mode 0640.
If port 4318 is already occupied, select a different loopback port in the
config and in each exporter. Before the first start, run the following in a
root shell to create independent random hex keys. Noclobber prevents replacing
either key on a repeat invocation.

```sh
(
  umask 077
  set -eC
  openssl rand -hex 32 > /etc/agent-stats/ingest.token
  chown agent-stats:agent-stats-read /etc/agent-stats/ingest.token
  openssl rand -hex 32 > /etc/agent-stats/digest.key
  chown agent-stats:agent-stats-read /etc/agent-stats/digest.key
)
```

Both files must be mode 0600 and owned by the service UID. Keep the digest
key stable across restarts to preserve deduplication. Rotating the ingest token
requires restarting Stats and updating exporters. Keep the digest key on Stats
and distribute only the ingest token via the session or Run unit environment.

After reviewing the config, run `systemctl daemon-reload` and
`systemctl enable --now agent-stats.service`. Check the socket with
`agent-stats status` from an authorized account. Runtime errors carry stable
codes and counters. Status counters reset on service restart.

## Enable Claude Code

Use these environment entries with the actual port and ingest token. Keep the
environment file private, mode 0600, and use the Run unit's `EnvironmentFile`
for Run workers. For interactive owner and agent sessions, use the Crew
environment for the Claude runtime. Keep Claude's prompt and response logging
disabled.

```ini
CLAUDE_CODE_ENABLE_TELEMETRY=1
OTEL_LOGS_EXPORTER=otlp
OTEL_METRICS_EXPORTER=none
OTEL_EXPORTER_OTLP_PROTOCOL=http/json
OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=http://127.0.0.1:4318/v1/logs
OTEL_EXPORTER_OTLP_HEADERS=x-stats-token=<ingest-token>
```

Canary one Run worker and one owner session before wider rollout. Verify
`agent-stats status` and `agent-stats stats --window 24h`. Remove these
environment entries from their respective launchers to roll back telemetry.
Existing samples expire on the normal retention schedule.

## Enable Codex

Add this to the Codex configuration used by the session or worker. Use the
actual loopback port and ingest token, keep the configuration private, and
leave user prompt logging disabled.

```toml
[otel]
exporter = { otlp-http = { endpoint = "http://127.0.0.1:4318/v1/logs", protocol = "json", headers = { "x-stats-token" = "<token>" } } }
log_user_prompt = false
```

After a Codex response, verify ingestion with
`agent-stats stats --window 24h --group-by harness+model`. Agent Stats records
reported token counts and time to first token. It estimates duration only
when one observed WebSocket request matches the completion. A group or series
bucket with qualifying speed samples from those pairs has `speed_estimated: true`.
