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
  only for the currently executing stage when its snapshotted policy is
  `reported`; the first report for that stage wins.

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
- **Build a dashboard or search tickets** — `show` takes optional `ref` and
  `limit` arguments. With neither, it returns the dashboard. The dashboard
  preserves the status fields (`daemon`, `gate`, `runs`,
  `queued_activations`, `tickets`, and optional `next_wake`) and adds
  `kind: "dashboard"`, `recent`, `recent_total`, and `recent_limit`.
  `recent` is newest first; `recent_total` is the untruncated count and
  `recent_limit` is the requested limit or the default `10`.

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
  and `stages`: one `{stage, state}` pair per flow stage, where `state` is
  `passed`, `failed`, `running`, or `pending`.
- **Explain one run** — `show` on a run adds `attempt`, the timeline
  (`claimed_at_ms`, `started_at_ms`, `finished_at_ms`), `agent_exit_code`,
  and `stages`: per stage, its `state`, `attempts` (including `on_fail`
  repair retries), `started_at_ms`, `finished_at_ms`, `duration_ms`,
  `exit_code`, `verdict_source`, and `reason`. Stage names come from the
  run's admitted flow snapshot, so a run reports the stages it actually had
  even after the flow file changes.

  `value.reason` on a run is now populated for every non-merged terminal
  run. It is a classified vendor diagnostic where one exists, and otherwise
  a sentence derived from the stored stage and evidence rows — never from
  anything an agent reported about its own work. `exit_code` is unchanged
  and has always been the *agent stage's* exit; `agent_exit_code` is the
  same value under a name that cannot be misread as the run's outcome.
- **Read or stream one run's output** — `logs` takes `{"run": <ref>}` plus
  optional `stage` (one flow stage name; an undefined one is
  `invalid_arguments`), `tail` (keep the last N matching entries), and
  `after` (a cursor). It returns the page along with `next_cursor`,
  `complete`, and `terminal`. Poll with `{"after": <cursor>}` to follow a
  live run and stop once a response is both `complete` and `terminal`;
  `sloop logs --follow` is exactly this loop. Filtering is server-side, so a
  dashboard tailing one stage of a large log transfers only that stage. Use
  `show` for the run's derived outcome and stage summary before reading its
  logs.
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
Existing v1 `status`, `list`, and `wait` requests remain accepted, although
the CLI names are hidden deprecated aliases for the `show` surface.

The same rule applies to `show`'s run and stage history: `value.runs`,
`value.stages`, `value.attempt`, `value.agent_exit_code`, and the timeline
fields are additive. `value.reason` on a run is the closest call — it was
previously null unless a vendor error was classified, and is now also
populated by a derived explanation — but it keeps its meaning ("why this run
ended where it did"). A client that tested it for null as a proxy for "no
vendor error" should read `value.classification` instead, which is unchanged.
