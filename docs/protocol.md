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
  verb; worker tokens are rejected here.
- **Worker socket** — created fresh for each run and torn down with it,
  also mode `0600`. Its path and token reach the agent as the
  `SLOOP_SOCKET` and `SLOOP_TOKEN` environment variables. Only `brief`,
  `show`, and `note` are accepted, the token must match the run, `show`
  and `note` are scoped to the run's own ticket, and the token stops
  working when the run settles.

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

The worker's verbs never grow: an agent, or anything holding only a worker
token, can read its brief and leave notes, and nothing else. If your
integration needs to move state, it belongs on the operator socket.

## Stability

The envelope is versioned so clients can fail fast rather than misparse.
Within version `1`, verbs and their response fields may gain data but are
not repurposed. The daemon replies `unsupported_version` (listing what it
supports) rather than guessing at unknown versions.
