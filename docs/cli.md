# CLI Reference

Every command accepts a global `--json` flag. Without it, replies render as
human-readable text; with it, the CLI writes the daemon's JSON response
envelope verbatim. Scripts and agents should always use `--json` — see
[Protocol](protocol.md) for the envelope format.

The commands split into two sets, and the split is enforced by the daemon:

- **Operator commands** decide what happens. They use the operator socket
  and implicitly start the daemon if it is not running. They also include
  read-only views — `list`, `status`, and `show`.
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
startup. Operational commands (`status`, `stop`, `pause`, `resume`, `cancel`,
`logs`, `wait`, `list`, `hold`, and `ready`) and worker commands therefore keep
working if a flow file on disk is later made invalid. Active runs continue from
their admitted flow snapshots. Flow errors surface when `sloop post` reads and
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

Use `sloop status` or `sloop wait` to follow the drain. `sloop resume` cancels
a pending restart and continues dispatching in the current process. For an
immediate teardown, use `sloop stop`; restart has no force mode.

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

### sloop list

Every ticket's name and state, and for each ticket that is not running,
the scheduler's current reason (paused, outside running hours, at capacity,
blocked, held, target cooldown, ...). Failed and cooled-down tickets include
the safe vendor diagnostic when a built-in rule recognized the rejection.

### sloop show <REF>

Read-only lookup that resolves, in order, a ticket id, a run id, a ticket
name, or a project id:

- **Ticket** (`TICK-5` or its name) — the frontmatter summary (id, name,
  state, project, worktree, and `blocked_by`/`target`/`model`/`effort` when
  set) followed by the ticket body read from its committed file.
- **Run** (`R14`) — the run's ticket, state, branch, worktree, and exit
  evidence summary (exit code plus any classified vendor error).
- **Project** — its tickets with each ticket's recent notes (from runtime
  state) and commits (rendered from Git).

Recognized vendor failures include their classification and safe
diagnostic. An unresolvable reference returns `not_found`, naming the
reference kinds `show` accepts and pointing at `sloop list`. Never writes
generated activity into committed files.

### sloop status

Snapshot of daemon state: active runs, ready and queued work, gate state,
active target cooldowns, and the next wake time. The gate state includes
database writability; a full database blocks new dispatch until a write
probe succeeds. A pending restart is shown as `draining for restart` with the
number of active runs remaining.

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

### sloop wait <RUN> [--timeout SECS]

Block until the run reaches a terminal state. The exit code is the
outcome: `0` only for `merged`, `1` for any other terminal state, `2` on
timeout (default 3600 seconds). Lets scripts and CI gate on a run directly.
Recognized vendor failures include their classification and safe diagnostic
in the response.

### sloop logs <RUN> [--stage NAME] [--tail N] [--follow]

Show a run's captured output — both stdout and stderr, in order. The
underlying file is `runs/<run-id>/output.ndjson` in the state directory.

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
loop over that cursor, the same shape as `sloop watch`.

### sloop watch [--tail N]

Follow ticket and run activity as it happens: claims, starts, and settled
outcomes, one line per event. Prints the `--tail` most recent events
(default 20), then streams new ones until interrupted. With `--json`, each
event is written as one NDJSON object, ready to pipe into other tools.

Under the hood this is the `events` verb: a cursor-paginated read of the
daemon's activity feed. Any client — a dashboard, a websocket bridge — can
stream the same feed by polling with the returned cursor.

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
