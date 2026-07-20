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
{"v": 1, "id": "req-1", "verb": "status", "args": {}, "token": null}
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
printf '%s\n' '{"v":1,"id":"1","verb":"status","args":{}}' \
  | nc -U "$OPERATOR_SOCKET" | head -1
```

Patterns that fall out of the verbs:

- **Gate CI on a run** — `run` a ticket, then `wait` on the returned run
  ID; the CLI's `wait` exits `0` only for `merged`, or poll `status`
  yourself over the socket.
- **Drive a queue from your own tool** — `post` tickets, `hold`/`ready` to
  sequence them, `list` to observe why something is not running.
- **Build a dashboard** — everything `status`, `list`, `show`, and `logs`
  return is structured JSON; render it however you like.
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
  dashboard tailing one stage of a large log transfers only that stage.
- **Stream activity** — `events` returns one page of the append-only
  activity feed (`run_claimed`, `run_started`, `run_finished`,
  `run_aborted`) plus a `next_cursor`. Poll with `{"after": <cursor>}` to
  follow live, or `{"tail": N}` to start near the newest event; the daemon
  keeps no per-client state, so a websocket bridge or UI can fan the same
  feed out however it likes. `sloop watch` is exactly this loop.

  Add `{"scope": "<ref>"}` to narrow the page to one reference. The daemon
  resolves it exactly as `show` does — a ticket id or name covers the ticket
  and every run of it, a project id covers its tickets and their runs, and a
  run alias, id, or id prefix covers that run alone — so a thin client never
  reimplements that ladder. A reference that resolves to nothing is
  `not_found` on the first request rather than a silently empty stream, and
  events belonging to no ticket or run are repository-wide and so match no
  scope. `next_cursor` still advances across rows the scope filtered out, so
  a scoped poller never rescans the feed. `sloop watch <ref>` is this
  argument.

A worker token can read its run-scoped brief and ticket, leave advisory notes,
and report the verdict of a stage explicitly configured to accept one. It
cannot claim, schedule, merge, or otherwise move operator-owned state. If your
integration needs those capabilities, it belongs on the operator socket.

## Stability

The envelope is versioned so clients can fail fast rather than misparse.
Within version `1`, verbs and their response fields may gain data but are
not repurposed. The daemon replies `unsupported_version` (listing what it
supports) rather than guessing at unknown versions.

That is the rule applied to `show`'s run and stage history: `value.runs`,
`value.stages`, `value.attempt`, `value.agent_exit_code`, and the timeline
fields are all *additive*, and no existing field changed meaning, so they
carry no version break. `value.reason` on a run is the closest call — it was
previously null unless a vendor error was classified, and is now also
populated by a derived explanation — but it keeps its meaning ("why this run
ended where it did"), only widening the set of runs that have one. A client
that reads it as free text is unaffected; one that tested it for null as a
proxy for "no vendor error" should read `value.classification` instead,
which is unchanged.
