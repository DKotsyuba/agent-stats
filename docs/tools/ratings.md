# ratings

List recent ratings recorded with [`rate_result`](rate_result.md), newest change
first, up to 90 days back.

Arguments: `model` filters by exact identifier, `limit` is 1 to 20 (default 10;
the CLI allows up to 100) and `cursor` continues from a previous page. Pages are
ordered by last change time, then Run id, so the cursor never skips or repeats a
row that did not change between calls. A replaced rating moves to the front.

Text has one line per rating and a continuation line when more remain:

```text
run_2: rejected · claude-sonnet-5-5 (anthropic, high) · tier C · rounds 2 · wrong_solution · checks 5 passed/1 failed/0 not run · 2026-11-18 11:06Z · "tests fail"
run_1: accepted · gpt-6 · rounds 0 · 2026-11-18 08:20Z
More: call ratings again with cursor "1794990000000000000:run_1".
```

Text shortens notes to 80 characters. `structuredContent` has the socket envelope:
`ratings` (each with `agent_id`, `model`, `provider`, `effort`, `tier`, `verdict`,
`rounds`, `reason`, `checks`, `note` and `updated_unix_ns` as a decimal string)
and `next_cursor` (null on the last page). An empty result prints
`No ratings recorded.`. `agent-stats ratings` returns the same envelope as JSON.
