# Protocol

The CLI is a thin client. The real public API is a versioned JSON envelope
spoken over a Unix domain socket, one JSON object per line (NDJSON). A web
UI, a CI integration, or a higher-level orchestrator built on Sloop is just
another client of the same socket — it gets exactly what the CLI gets.

The quickest way to see the protocol in action is `--json` on any command:
the CLI prints the daemon's response envelope verbatim.

## Envelope

Request:

```json
{"v": 1, "id": "req-1", "verb": "show", "args": {}, "token": null}
```

- `v` — protocol version; currently `1`. A request with any other version
  is rejected with `unsupported_version` and the versions the daemon
  supports.
- `id` — caller-chosen request ID, echoed back so responses can be matched
  to requests.
- `verb` — the command name, the same set the CLI exposes.
- `args` — verb-specific arguments.
- `token` — the per-run worker token, or null on the operator socket.

Response:

```json
{"id": "req-1", "ok": true, "data": { ... }}
{"id": "req-1", "ok": false, "error": {"code": "not_found", "message": "...", "details": {}}}
```

Exactly one of `data` or `error` is present, matching `ok`. Error codes:
`invalid_arguments`, `invalid_request`, `unsupported_version`,
`unknown_verb`, `daemon_unavailable`, `unauthorized`, `not_found`,
`conflict`, `cooldown_active`, `internal`.

Malformed JSON, an unknown verb, or a wrong version get a structured error
back; none of them take the daemon down.

## Two sockets

The operator/worker split is enforced at the transport layer:

- **Operator socket** — a fixed per-repository path with mode `0600`,
  printed by `sloop daemon`. Connections on it may use every operator
  verb, plus the read-only `show`, which resolves any ticket, run, or
  project; worker tokens are rejected here.
- **Worker socket** — created fresh for each run and torn down with it,
  also mode `0600`. Its path and token reach the agent as the
  `SLOOP_SOCKET` and `SLOOP_TOKEN` environment variables. Only `brief`,
  `show`, `note`, and `verdict` are accepted, the token must match the run,
  and the token stops working when the run settles. `verdict` is accepted
  only for the currently executing stage when its snapshotted result check is
  `reported`; the first report for that stage execution wins.

  `brief` answers *what* the caller was assigned, and is keyed on the stage
  execution rather than the run: alongside the ticket, worktree, and branch it
  carries `stage` — `{"name": …, "attempt": …, "result_check": …}` — and the
  `definition_of_done` that stage's check implies. Both come from the caller's
  own credential: a stage worker's stage is read from the run's live
  stage-process checkpoint, a reviewer's from its seat. Nothing seat-specific
  appears in a brief, so two reviewers on one panel read identical replies.

  A **panel reviewer's** credential is narrower still: it is minted for one
  seat — `(run, stage, attempt, reviewer index)` — and authorises exactly one
  `verdict`. Which report the call lands on is derived from the credential,
  never from its arguments, so no request a reviewer can compose reports for
  another seat, stage, attempt, or run. A run holds one live credential at a
  time, so a seat's token stops validating the moment the next one is issued.

Both rejections use the same `unauthorized` error, so a probing worker
learns nothing about what exists outside its scope.

## Writing an operator client

Connect to the operator socket, write one request envelope per line, read
one response envelope per line. There is no session state; each request
stands alone. A useful client can be a few lines of shell:

```sh
printf '%s\n' '{"v":1,"id":"1","verb":"show","args":{}}' \
  | nc -U "$OPERATOR_SOCKET" | head -1
```

Patterns that fall out of the verbs:

- **Gate CI on a run** — `run` a ticket, then use
  `sloop show <run> --follow --quiet`; it exits `0` only for `merged`. Socket
  clients poll `show` and the scoped `events` feed themselves.
- **Drive a queue from your own tool** — `post` tickets, `hold`/`ready` to
  sequence them, then `show` a ticket pattern to observe why work is not
  running.
- **Register or refresh a ticket** — `post` returns `ticket`, `created`,
  `trigger`, and `trigger_suppressed`. `created` is `false` when the
  file was already registered and the post only refreshed its indexed
  content.

  A trigger is queued only when the post leaves the ticket in `ready`.
  `--manual` and `--hold` request none, so `trigger` is null and
  `trigger_suppressed` is null too. Reposting a ticket that is `merged`,
  `failed`, or `needs_review` still refreshes the index, but a post cannot
  return such a ticket to `ready`, so nothing is queued and nothing is
  rescheduled — including with `--at`. There `trigger` is null and
  `trigger_suppressed` is
  `{"reason": "terminal_ticket", "state": "<ticket state>"}`. Read
  `trigger_suppressed`, not a null `trigger`, to tell "none requested"
  apart from "requested and refused".
- **Build a dashboard or search tickets** — `show` takes optional `ref` and
  `limit` arguments. With neither, it returns the dashboard. The dashboard
  preserves the status fields (`daemon`, `gate`, `runs`,
  `queued_triggers`, `tickets`, and the optional `next_wake` and `dispatch`)
  and adds
  `kind: "dashboard"`, `attention`, `recent`, `recent_total`, and
  `recent_limit`. `recent` is newest first; `recent_total` is the untruncated
  count and `recent_limit` is the requested limit or the default `10`.
  `attention` is every ticket currently in `needs_review` or `failed`, newest
  first, in the same row shape as `recent`. It is never truncated — `limit`
  applies to `recent` alone — and is `[]` when nothing is waiting. `next_wake`
  is an RFC3339 timestamp for the daemon's next internal timer, whatever its
  purpose — including housekeeping such as worktree cleanup. `dispatch` is the
  operator-facing subset: present only when timed *work* is pending, it
  carries `at` (RFC3339), a `cause` of `"cooldown"`, `"running_hours"`, or
  `"scheduled_trigger"`, plus `waiting` (posts held behind closed hours) or
  `trigger` (the ticket or project the scheduled post selects) where the cause
  has one. Absent `dispatch` means the daemon is idle and dispatches new posts
  immediately; only the human rendering shows the instant as a countdown.

  With `ref`, the daemon first resolves exact ticket, run, ticket-name, and
  project reference forms. Exact references retain their existing detail
  response. If none matches, the final rung treats `ref` as a
  case-insensitive ticket-ID/name pattern: plain text is a substring, while
  text containing regex metacharacters is an unanchored regular expression.
  An invalid expression is `invalid_arguments`. Pattern responses retain the
  top-level `tickets` rows and add `kind: "matches"` and the original `ref`.
  Rows are newest first; `limit`, when present, must be at least `1`.
- **Read a ticket's run history** — `show` on a ticket returns `value.runs`,
  every run of the ticket newest attempt first. Each entry carries `id`,
  `alias`, `attempt`, `state`, `terminal`, `started_at_ms`,
  `finished_at_ms` (null while the run is in flight), the derived `reason`,
  `stall`, and `stages`: one compact
  `{stage, state, attempt, advisory, silent_for_ms}` object per stage
  execution, where `state` is `passed`, `failed`, `running`, or `pending`. A
  stage a `return_to` edge re-entered contributes one object per execution, so
  this list is not one-to-one with the flow's stage names.
- **Explain one run** — `show` on a run adds `attempt`, the timeline
  (`claimed_at_ms`, `started_at_ms`, `finished_at_ms`), `agent_exit_code`,
  `halt`, and `stages`. Each stage row carries `stage`, `state`, `attempt`,
  `advisory`, `started_at_ms`, `finished_at_ms`, `duration_ms`, `exit_code`,
  `verdict_source`, `reason`, `confidence`, `silent_for_ms`, and `reviewers`.
  Stage names come from the run's admitted flow snapshot, so a run reports the
  stages it actually had even after the flow file changes.

  `attempt` is which *execution* of the stage the row is: a `return_to` edge
  re-enters a stage and each re-entry is its own row, numbered from `1`. A row
  for a stage the walk has not reached has `attempt: 0`.

  `advisory` is `true` when the stage's `fail_action` is `continue`, so a
  `failed` row with it set was recorded and stepped over rather than ending the
  walk.

  `confidence` is the `--confidence` a `reported` stage's worker sent, or null
  on every other check and on reports made before the field existed. A panel's
  confidences live on its seats instead: `reviewers` is one object per seat, in
  reviewer order, with `reviewer` (the seat index), `target`, `verdict`,
  `confidence`, and `reason`. Seats that never reported are present, as the
  `fail` the quorum counted, with a null `confidence`. It is empty for every
  non-panel check.

  `value.halt` says why the walk stopped short of the end of the flow:
  `fail_action` (the stage's `fail_action` is `fail`), `return_budget_exhausted`
  (it had a `return_to` edge whose attempts were already spent), or
  `corrupt_log` (the stage log does not replay against the run's flow). It is
  null on a run that walked the whole flow and on one still in flight.

  `value.reason` on a run is now populated for every non-merged terminal
  run. It is a classified vendor diagnostic where one exists, and otherwise
  a sentence derived from the stored stage and evidence rows — never from
  anything an agent reported about its own work. It names the last *halting*
  failure; an advisory failure is never named as the cause, because the walk
  provably continued past it. `exit_code` is unchanged and has always been the
  *agent stage's* exit; `agent_exit_code` is the same value under a name that
  cannot be misread as the run's outcome.
- **Report a stage's verdict** — `verdict` takes
  `{"verdict": "pass" | "fail"}` plus optional `reason` (a string) and
  `confidence` (`"low"`, `"medium"`, or `"high"`, defaulting to `"medium"`; a
  float or any other string is `invalid_arguments`). Nothing in the arguments
  says which stage, attempt, or panel seat the report is for — all of that
  comes from the token and the run's own state, so a worker cannot compose a
  request that reports for anything but the execution it is in.

  It returns `{"verdict": {"run", "stage", "attempt", "verdict", "confidence",
  "reason"}}` for a stage worker, and the same object with `reviewer` in place
  of `attempt` for a panel seat. A stage whose result check is not `reported`
  is `unauthorized`; a second report on the same execution or seat is
  `conflict`. `reason` is required from a panel reviewer and optional from a
  stage worker. `confidence` is stored as evidence from either caller and is
  never weighted into a panel's aggregation.
- **Read or stream one run's output** — `logs` takes `{"run": <ref>}` plus
  optional `stage`, `tail` (keep the last N matching entries), and
  `after` (a cursor). It returns the page along with `next_cursor`,
  `complete`, `elided`, and `terminal`. `complete` is false when records
  remain ahead of the cursor; `elided` counts matching records a `tail`
  dropped behind it, which `complete` cannot report because a tail reads to
  the end however much it discards. Poll with `{"after": <cursor>}` to follow
  a live run and stop once a response is both `complete` and `terminal`;
  `sloop logs --follow` is exactly this loop. Filtering is server-side, so a
  dashboard tailing one stage of a large log transfers only that stage. Use
  `show` for the run's derived outcome and stage summary before reading its
  logs.

  `stage` is a selector, not just a name: `"<stage>"` selects every execution,
  and `"<stage>#<attempt>"` selects one — the disambiguation a `return_to` edge
  makes necessary. A stage the run's flow does not define, and a malformed
  attempt, are both `invalid_arguments`; an attempt the stage never reached is
  an empty page. Each returned record carries its own `stage` and `attempt`, so
  a client can group an unfiltered page itself. Both are absent on output
  captured before those fields existed, and such a record is claimed by the
  flow's first agent stage on its only attempt — which is the only execution a
  run that produced one ever had.
- **Stream activity** — `events` returns one page of the append-only
  activity feed (`run_claimed`, `run_started`, `run_finished`,
  `run_aborted`) plus a `next_cursor`. Poll with `{"after": <cursor>}` to
  follow live, or `{"tail": N}` to start near the newest event; the daemon
  keeps no per-client state, so a websocket bridge or UI can fan the same
  feed out however it likes. `sloop show --follow` is this loop.

  Add `{"scope": "<ref-or-pattern>"}` to narrow the page. Resolution uses
  the same ladder as `show`, including exact-reference precedence and the
  final case-insensitive pattern rung. A ticket covers it and every run of it,
  a project or pattern covers its matching tickets and runs, and a run covers
  itself. A valid pattern may match nothing; an invalid regex is
  `invalid_arguments`. Events belonging to no ticket or run are
  repository-wide and appear only in an unscoped request. `next_cursor` still
  advances across rows the scope filtered out, so a scoped poller never
  rescans the feed.

A worker token can read its run-scoped brief and ticket, leave advisory notes,
and report the verdict of a stage explicitly configured to accept one. It
cannot claim, schedule, merge, or otherwise move operator-owned state. If your
integration needs those capabilities, it belongs on the operator socket.

## Stability

The envelope is versioned so clients can fail fast rather than misparse.
Within version `1`, verbs and their response fields may gain data but are
not repurposed. The daemon replies `unsupported_version` (listing what it
supports) rather than guessing at unknown versions.

That rule applies to the optional `show.ref` and `show.limit` arguments and to
the dashboard and match fields above: they are additive protocol-v1 changes.
The existing status fields and top-level ticket rows keep their meanings.
Existing v1 `status`, `list`, and `wait` requests remain accepted over the
socket, although on the CLI `status` and `wait` are hidden aliases for the
`show` surface and the `list` verb has been removed.

`post`'s `created` and `trigger_suppressed` are additive in the same sense.
`trigger` keeps its meaning; it was already null whenever no trigger was
queued, and the new field only explains which null it is.

`brief` is the one exception, in **0.4.0**. Its `stage` block is additive, but
two things changed under clients:

- `ticket.acceptance` is **removed**. It was hardcoded to `[]` on every reply
  and never carried a value, so nothing could have read anything from it.
- `definition_of_done` keeps its type — an array of strings — and changes what
  it says. It used to state the same run-level obligation to every caller; it
  now states what the caller's own stage turns on. Its contents were always
  prose for an agent to read, never a value to branch on.

### The activation → trigger rename

What Sloop used to call an *activation* is now a **trigger**. The concept, the
values, and the scheduling behaviour are unchanged; only the vocabulary moved.
Two things follow for clients.

`queued_activations` on `status` and on the `show` dashboard is now
`queued_triggers`. **Both fields are emitted, carrying the identical value.**
`queued_activations` is deprecated as of **0.4.0** and is **removed in 0.5.0** —
read `queued_triggers` and the transition costs you nothing.

The remaining renames are swaps, not aliases, and they land in 0.4.0 with no
transition period:

| Was | Is |
| --- | --- |
| `post` argument `activation` | `trigger` |
| `run` argument `activation` | `trigger` |
| `post` response `activation` | `trigger` |
| `post` response `activation_suppressed` | `trigger_suppressed` |

Both request arguments are `deny_unknown_fields`, so a client that still sends
`activation` is rejected outright rather than silently defaulted. Trigger ids
also change shape: they were `A<ordinal>` (`A91`) and are now `TR<ordinal>`
(`TR91`). Existing ids are rewritten in place when the daemon upgrades, so a
client holding an old id must re-read it rather than assume it still resolves.

The same rule applies to `show`'s run and stage history: `value.runs`,
`value.stages`, `value.attempt`, `value.agent_exit_code`, and the timeline
fields are additive. `value.reason` on a run is the closest call — it was
previously null unless a vendor error was classified, and is now also
populated by a derived explanation — but it keeps its meaning ("why this run
ended where it did"). A client that tested it for null as a proxy for "no
vendor error" should read `value.classification` instead, which is unchanged.

**The stage model did not bump the envelope.** Everything the `action` /
`result_check` / `fail_action` split, `return_to` loops, advisory stages, the
`sync` and `ff_only` builtins, and reviewer panels added to the wire is
additive within version `1`, one removal aside, and the daemon still accepts and
emits `v: 1` only:

- `verdict.confidence` is a new optional request field. A client that predates
  it simply omits it and is read as `medium`, so every older caller keeps
  working unchanged.
- `value.halt` on a run, `attempt`, `advisory`, `confidence`, and `reviewers`
  on a stage row, and `elided` on a `logs` page are new response fields.
  Nothing existing was repurposed to make room for them.
- `logs.stage` is the one field whose accepted *values* widened: it took a bare
  stage name and now also takes `<stage>#<attempt>`. A bare name still means
  what it always did — every execution of that stage — so no request that was
  valid before means something different now. Only strings containing `#`,
  which were previously always errors, changed from rejected to meaningful.
- The one behavioural narrowing is in `stages`, and it predates this note: a
  run whose walk looped contributes more than one row per stage name. A client
  keying stage rows by name alone must key by `(stage, attempt)` instead.
- The one field **removed** is `attempts` on a stage row. It counted the retries
  an `on_fail` repair block made inside a single execution, and 0.4.0 removes
  those blocks, so the only thing that could ever set it above `1` is gone with
  it. The envelope stays `1` regardless: `v` versions the framing and the
  guarantee that a field is never repurposed, and a field-level removal is
  recorded here in the release that makes it — the same treatment the
  activation → trigger swaps above get. A client that read `attempts` should
  read `attempt` instead and count the rows.
