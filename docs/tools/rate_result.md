# rate_result

Record how a delegated agent's result turned out, so [`model_stats`](model_stats.md)
can report acceptance beside speed. This is a write: it stores one rating row
in the Agent Stats database.

| Argument | Meaning |
| --- | --- |
| `agent_id` (required) | Run id of the delegated agent; the rating's identity. |
| `model` (required) | Exact model identifier that produced the result. |
| `provider`, `effort` | Optional labels matching the telemetry groups (`provider+model`, `model+effort`). |
| `tier` | Task tier `S` to `F`. |
| `verdict` (required) | `accepted`, `accepted_after_fixes` or `rejected`. |
| `rounds` | Fix rounds before acceptance or rejection, 0 to 1000. `accepted` takes 0 (default); `accepted_after_fixes` takes at least 1 (default 1). |
| `reason` | `wrong_solution`, `incomplete`, `missing_access`, `broke_other`, `environment` or `other`. Not allowed with `accepted`. |
| `checks` | Optional `{passed, failed, not_run}` verification counts. |
| `note` | Optional text of at most 200 characters, without control characters. |

Rating the same `agent_id` again replaces the earlier rating, so a retry is
safe; the first-rated time is kept and the last change time is updated. Ratings
expire 90 days after their last change.

Success text:

```text
OK rate_result: run-1 accepted_after_fixes (claude-haiku-5-5, tier D, rounds 2, reason incomplete). Replaced the earlier rating.
```

The first rating of an id ends with `Recorded.`. `structuredContent` carries the
socket envelope with `agent_id`, `model`, `verdict`, `rounds`, `replaced` and
`updated_unix_ns`.

Errors are single lines. `invalid_arguments`, `invalid_agent_id`, `invalid_model`,
`invalid_rounds`, `invalid_reason` and `invalid_note` mean nothing was sent.
`not_permitted` means the UID may not write: the service allows the service user
and `allowed_uids`, or exactly `rating_uids` when that list is configured. The
ingest token is not involved. `busy` and `socket_unavailable` changed nothing.
`outcome_unknown` means the write is unconfirmed; call again with the same
arguments.

The CLI mirrors the tool:

```sh
agent-stats rate --agent-id RUN_ID --model claude-sonnet-5-5 --tier C \
  --verdict rejected --rounds 2 --reason wrong_solution \
  --checks-passed 5 --checks-failed 1 --note "tests fail"
```

The service must be 0.3.0 or newer. The frontend reports a version mismatch and
asks you to reconnect.
