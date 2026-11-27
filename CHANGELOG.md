# Changelog

## Unreleased

Target version: 0.1.0.

### Changed

- Require at least 20 output tokens and 50 ms after the first token for speed
  aggregates. TTFT eligibility is unchanged.

- `cargo xtask check` follows the Rust gate standard: tests run under cargo-nextest (required; CI installs 0.9.149 from a checksum-verified archive) with every failure reported, doctests via `cargo test --doc`, and test temporaries in a fresh tmpfs directory (`XTASK_TEST_TMPFS=0` opts out). Integration tests build into one `it` binary: every `tests/*.rs` must be a module of `tests/it/main.rs`. Release acceptance runs `--test it <suite>::` filters and refuses a filter that selects no tests. The dev profile keeps line-table debug info only (`--profile debugging` for full).
- Run release checks and payload construction in parallel while keeping publication gated on both.
- Seed separate check/package dependency caches from main; document focused checks for local iteration.
- Reuse Cargo dependency builds and versioned cargo-deny tooling in CI and release jobs.
- Run Clippy/tests once for featureless workspaces; retain both default and all-features configurations when workspace features exist.

### Fixed

- Modern MCP 2026-07-28 tools/list includes a 60000 ms private cache lifetime; legacy catalog responses stay unchanged.
- Raw stdio regression tests cover modern/legacy tools/list and modern server/discover; supported protocol revisions are explicit.

### Added

- Add the read-only MCP `model_stats` tool with rolling model comparisons, exact
  model filtering and full structured aggregates. The MCP frontend checks the
  service version and requests a reconnect on mismatch.

- Ingest Codex OTLP logs with exact reported metrics and estimated durations for
  unambiguous WebSocket pairs. Add `speed_estimated` to stats and series metrics
  and `harness+model` to stats groupings.

- Add a loopback OTLP/HTTP JSON receiver for Claude Code model calls and a private
  30-day SQLite store with keyed request deduplication.
- Expose exact model aggregates, UTC series and service status through a
  peer-UID-authorized Unix socket and CLI.
- Provide a systemd unit, example configuration and Claude Code setup instructions.
- Initial agent-stats working set: the read-only `get_status` identity tool and
  the local development gate.

No published release is implied by the Cargo package version.
