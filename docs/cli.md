# CLI Reference

Every command accepts a global `--json` flag. Without it, replies render as
human-readable text; with it, the CLI writes the daemon's JSON response
envelope verbatim. Scripts and agents should always use `--json` — see
[Protocol](protocol.md) for the envelope format.

The commands split into two sets, and the split is enforced by the daemon:

- **Operator commands** decide what happens. They use the operator socket
  and implicitly start the daemon if it is not running. The operator read
  surface is `show` and `logs`; bare `sloop` prints help.
- **Worker commands** (`brief`, `show`, `note`, `verdict`) are for the process
  inside a run. They authenticate with a per-run token. Only `verdict`, when
  the current stage explicitly uses the `reported` policy, affects flow
  evidence.

Two verbs answer without a socket at all: `init` writes files, and `template`
prints compiled-in text. Neither starts a daemon.

`show` is available on both sockets: an operator can inspect any ticket,
run, or project, while a worker's `show` is scoped to its own ticket. The
verb is read-only on either socket.

A running daemon uses the configuration and flow definitions it validated at
startup. Operational commands (`show`, `logs`, `stop`, `pause`, `resume`,
`cancel`, `hold`, and `ready`) and worker commands therefore keep working if a
flow file on disk is later made invalid. Active runs continue from their
admitted flow snapshots. Flow errors surface when `sloop post` reads and
snapshots the current definitions and when daemon startup validates them; fix
the named flow file before posting work or starting a new daemon.

## Operator commands

### sloop init

Scaffold `.agents/sloop/` in the current repository: `config.yaml`, the
default project, the tickets directory, the default flow, and the review
prompt. Never modifies `.gitignore` or other repository policy.

### sloop template <KIND>

Print a fully commented canonical template for a file you author, where
`<KIND>` is `ticket`, `flow`, `project`, or `config`. An unknown kind fails
with the list of valid ones.

The templates are static content compiled into the binary, so this verb
writes nothing, contacts no daemon, and never starts one — it works in a
directory that is not a Sloop repository at all. Each one is a working
example whose comments document every field, and each is parsed by Sloop's
own loaders in the test suite, so a template cannot drift from the grammar
it describes.

Output goes to stdout so it composes; redirect it where you want the file:

```sh
sloop template ticket > .agents/sloop/tickets/add-request-logging.md
sloop template flow    > .agents/sloop/flows/release.yaml
sloop template project > .agents/sloop/projects/web.md
sloop template config  > .agents/sloop/config.yaml
```

Nothing is written into `.agents/sloop/` for you: the ticket directory is a
live queue, and an example file left there is one `sloop post` away from
becoming real work.

With `--json`, the template text is returned as `data.template` alongside
`data.kind`; without it, the raw template is the entire output.

`sloop template flow` is the only complete description of the flow schema
available from an installed binary.

### sloop daemon

Ensure the daemon is running and report `{pid, socket, version, started}`
along with the state directory and log path. Idempotent: connecting to a
live daemon and starting a fresh one look the same. Every other operator
command does this implicitly.

### sloop daemon restart

Ask the daemon to restart when it is safe. The command returns immediately
with the number of active runs still draining. No new runs start during the
drain; active runs continue through every flow and aftercare stage. When the
last run settles, the daemon releases its sockets and lock, then replaces
itself with the binary currently installed at the path from which it started.
Queued activations remain queued and resume automatically in the replacement.

Use `sloop show` to inspect the drain or `sloop show --follow` to stream its
events. `sloop resume` cancels a pending restart and continues dispatching in
the current process. For an immediate teardown, use `sloop stop`; restart has
no force mode.

### sloop post <FILE> [--project P] [--flow F] [--auto | --at TIME | --manual | --hold]

Validate and register a ticket file (which must live below the configured
ticket directory), stamping the allocated ID and worktree branch back into
the file. The activation modes are mutually exclusive:

- `--auto` (default) — queue one run for the next available opportunity.
- `--manual` — register the ticket as ready without queuing a run.
- `--hold` — register the ticket as held; `sloop ready` releases it.
- `--at HH:MM` — queue one run for the next occurrence of that local time;
  it still waits for running hours, capacity, cooldown, and budget gates.

Reposting an edited file updates the ticket in place without queuing a
duplicate run; reposting with a different `--at` time reschedules the
queued run.

### sloop run [TICKET] [--project P] [--only T1,T2] [--at TIME | --every INTERVAL | --overnight]

Enqueue a run. Naming a ticket or a project says *which* work, not
*whether* the gates apply:

- With a ticket — run exactly that ticket. Held or blocked tickets are
  rejected until released.
- With `--project` — select only from that project's ready tickets.
- With neither — select from all ready work.
- `--only T1,T2` — restrict selection to the listed ticket IDs.

Ticket and `--project` are mutually exclusive. Every run, named or not,
passes the same gates: pause, running hours, and capacity.

Time-based activations use the same scheduler gates as an immediate run:

- `--at HH:MM` queues one run for the next occurrence of that local time.
- `--every INTERVAL` queues recurring work, first due after the interval.
  Missed intervals advance on the original cadence rather than causing a
  burst of catch-up runs.
- `--overnight` queues one run for the next open `running_hours` window. If
  no window is configured, it is dispatchable immediately.

### sloop retry <TICKET>

Return a failed ticket to ready and reset its attempt counter.

### sloop hold <TICKET> / sloop ready <TICKET>

Hold a ready ticket so it cannot be dispatched; release it again. Held
tickets are skipped by selection and rejected by named runs.

### sloop show

```text
sloop show [REF_OR_PATTERN] [-N] [--follow] [--quiet]
```

Without an argument, `sloop show` is a dashboard with daemon and gate
state, active runs, queued activations, ticket counts, the next wake time, and
the 10 newest tickets. `sloop show -N` changes the number of recent tickets;
`-n N` and `--limit N` are equivalent. A limit of zero or a non-numeric limit
is a usage error.

With an argument, `show` first tries the exact reference forms: ticket IDs,
run IDs and aliases, unique run-ID prefixes, ticket names, and project IDs. An
exact reference always wins, even when the same text is also a valid pattern.
Exact references render full detail:

- **Ticket** (`TICK-5` or its name) — the frontmatter summary (id, name,
  state, project, worktree, and `blocked_by`/`target`/`model`/`effort` when
  set), then a `runs:` section, then the ticket body read from its committed
  file. A ticket that has never run prints `runs: none`.
- **Run** (`TICK-5-r1`) — the run's ticket, state, branch, worktree, timeline,
  agent exit, derived reason, and per-stage table.
- **Project** — its tickets with each ticket's recent notes (from runtime
  state) and commits (rendered from Git).

If no exact reference matches, the argument becomes a case-insensitive ticket
pattern over IDs and names. Text without regex metacharacters is a substring;
text containing regex metacharacters is an unanchored regular expression, like
`grep`. Quote regular expressions in the shell. Pattern results always use the
ticket-list view, even for one or zero matches, ordered by registration time,
newest first. `-N` limits these results after ordering.

```sh
sloop show verdict
sloop show 'flow|merge' -5
```

Each ticket row includes its state and, when it is not running, the scheduler's
current reason. Failed and cooled-down tickets include a safe vendor diagnostic
when a built-in rule recognized the rejection. With `--json`, the dashboard
adds `kind`, `recent`, `recent_total`, and `recent_limit` to the existing status
fields; a pattern response has top-level `kind`, `ref`, and `tickets` fields.

The `runs:` section lists every run of the ticket, newest attempt first: run
alias, outcome, wall-clock span, and a strip of the run's flow stages marked
`ok`, `FAIL`, `..` (running), or `-` (not reached).

```
runs:
  TICK-5-r2  merged        20:15-20:21  build:ok  test:ok  merge:ok
  TICK-5-r1  needs_review  19:02-19:09  build:ok  test:FAIL  merge:-
```

`sloop show <run>` expands one of those lines:

```
TICK-5-r1  (needs_review)
ticket: TICK-5  Persist cooldowns
branch: sloop/TICK-5-r1
timeline: claimed 19:02  started 19:02  finished 19:09
agent exit: 0
reason: stage `test` failed (exit 1) after agent completed with commits
stages:
  build  passed   19:02-19:05  3m0s   exit 0  verdict from exit_code
  test   failed   19:05-19:09  4m12s  exit 1  verdict from exit_code
  merge  pending  -
```

Two things about that output are deliberate. `agent exit` is labeled rather
than bare, because `exit: 0` on a run whose later stage failed reads as "the
run passed". And `reason` is *derived from the stored stage and evidence
rows* — never from an agent's own claim about its work — so it appears on
every non-merged terminal run, naming the stage that actually failed. A
merged run carries no reason, and a run still in flight shows its current
stage as `..` with an open-ended span rather than a guessed ending.

Stage names come from the run's admitted flow snapshot, so a run still shows
the stages it actually had even if the flow file changed afterwards. A stage
retried by an `on_fail` repair agent reports the total attempts it cost.

Recognized vendor failures include their classification and safe
diagnostic. `show` never writes generated activity into committed files.

`--follow` streams the shown scope through the existing `events` protocol:
a ticket includes all of its runs, a run includes only itself, a project or
pattern includes its matching tickets and runs, and the dashboard includes
repository-wide activity. Ticket and run followers exit when the subject
settles; dashboard, project, and pattern followers continue until interrupted.
`--quiet` requires `--follow`, suppresses the event stream, and returns only
the outcome.

Exit codes are stable for scripting:

- `0`: the read succeeded, or the followed ticket/run merged successfully.
- `1`: another terminal outcome or a daemon error.
- `2`: usage error, invalid regular expression, or deprecated `wait` alias timeout.

### sloop pause / sloop resume

Stop or resume spawning. In-flight agents finish and go through aftercare.
The paused state is persisted and survives daemon restarts. Resume also
cancels a pending `sloop daemon restart` and clears its draining state.

### sloop stop [--force]

Shut the daemon down. Refuses while runs are active and lists them;
`--force` cancels their process groups first. This is the one operator
command that never autostarts a daemon: if the socket is unreachable, the
desired state already holds, so it reports "not running" and exits 0.

### sloop cancel <RUN>

Kill the run's whole process group (including any children the agent
spawned), release its ticket, and preserve the worktree for inspection.

### sloop logs <RUN> [--stage NAME] [--tail N] [--follow]

Show a run's captured output — both stdout and stderr, in order. The
underlying file is `runs/<run-id>/output.ndjson` in the state directory.
Use [`sloop show`](#sloop-show) first for the run's derived outcome, timeline,
and stage summary; use `logs` for the captured output behind that evidence.

`--stage NAME` narrows the output to one flow stage, named exactly as the
flow names it: `--stage build` is the agent's own output, `--stage test` is
that exec stage's. A stage the run's flow does not define is an error listing
the ones it does, not an empty page.

`--tail N` keeps the last N matching entries instead of the first. An entry is
one captured chunk, matching how the NDJSON file is stored, so `--tail 50` is
"the last 50 records" rather than "the last 50 lines".

`--follow` streams entries as they are appended and exits when the run reaches
a terminal state. On a run that has already settled it prints what exists and
exits. All three combine: `sloop logs <RUN> --stage test --tail 50` answers
"why did the test stage fail" in one command, and `--stage test --follow`
streams only that stage.

Filtering happens in the daemon, so any client of the socket gets it: this is
the `logs` verb with `stage`, `tail`, and `after` arguments, returning a page
plus `next_cursor`, `complete`, and `terminal`. `--follow` is a client-side
loop over that cursor, like `show --follow` over `events`.

### Deprecated read aliases

`status`, `list`, `watch`, and `wait` remain accepted as hidden deprecated
aliases. They do not appear in normal help, and each invocation, including
`--json`, writes a note to stderr naming its replacement and warning that the
alias will be removed in a future release:

- `status` and `list` name `sloop show` as the replacement. `list` keeps its
  old all-ticket and limit behavior while the alias remains.
- `watch` names `sloop show --follow`; its optional scope and tail still work.
- `wait` names `sloop show --follow --quiet`; its run and timeout still work.

### sloop reindex

Rebuild the derivable SQLite index from the configured project and ticket
directories, Git branches, and orphaned worktrees. Project and ticket files
remain authoritative for membership, blockers, and worktree branches; Git
restores merged and review-needed states. Runtime history is preserved for
tickets that still exist, while rows belonging to removed tickets are dropped.
If SQLite was deleted, tickets without Git evidence return as ready; holds,
notes, attempts, and other runtime-only history cannot be recovered. The daemon
must be idle before reindexing.

A `needs_review` ticket whose preserved run branch an operator merges into the
default branch by hand no longer needs a reindex: the running daemon settles it
to `merged` on its next reconciliation pass (typically within one interval),
releasing any `blocked_by` dependents. This works only when the branch tip is a
strict ancestor of the default branch tip. Squash- and rebase-merges rewrite the
commits, so ancestry cannot prove them, and those still require `sloop reindex`
with the daemon idle.

## Worker commands

These are a worker process's entire vocabulary. They require the `SLOOP_SOCKET`
and `SLOOP_TOKEN` environment variables that the daemon injects into every
run, are scoped to that run's own ticket, and fail loudly when no daemon is
running rather than starting one. The token stops working when the run
ends.

### sloop brief

Everything needed to work: the ticket body, the selected agent target, the
worktree path, the branch, and the definition of done. Designed to be
re-read at any time — an agent that loses context can recover its
assignment.

### sloop show <REF>

Read-only lookup of the current run's ticket, including its snapshotted
target, model, and effort. References to other tickets are rejected. (The
same verb on the operator socket resolves any ticket, run, or project — see
[Operator commands](#operator-commands).)

### sloop note <TEXT>...

Append an advisory note to the current run. It moves nothing: no note changes
a ticket's state, and claims like "done" carry no weight. Notes appear in
`sloop show <project>` output and may not survive a state rebuild.

### sloop verdict pass|fail [--reason <TEXT>]

Report the current stage's verdict when, and only when, that stage declares
`verdict: reported`. The first report is persisted and final; a second report
is rejected. A reported stage that exits without calling this command fails
with `no verdict reported`.
