# CLI Reference

Every command accepts a global `--json` flag. Without it, replies render as
human-readable text; with it, the CLI writes the daemon's JSON response
envelope verbatim. Scripts and agents should always use `--json` — see
[Protocol](protocol.md) for the envelope format.

The commands split into two sets, and the split is enforced by the daemon:

- **Operator commands** decide what happens. They use the operator socket
  and implicitly start the daemon if it is not running.
- **Worker commands** (`brief`, `show`, `note`) are for the agent inside a
  run. They authenticate with a per-run token and cannot change any state.

## Operator commands

### sloop init

Scaffold `.agents/sloop/` in the current repository: `config.yaml`, the
default project, the tickets directory, the default flow, and the review
prompt. Never modifies `.gitignore` or other repository policy.

### sloop daemon

Ensure the daemon is running and report `{pid, socket, version, started}`
along with the state directory and log path. Idempotent: connecting to a
live daemon and starting a fresh one look the same. Every other operator
command does this implicitly.

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
blocked, held, ...).

### sloop show <REF>

Read-only lookup. For a ticket, the structured ticket payload. For a
project, its tickets with each ticket's recent notes (from runtime state)
and commits (rendered from Git). Never writes generated activity into
committed files.

### sloop status

Snapshot of daemon state: active runs, ready and queued work, gate state,
and the next wake time. The gate state includes database writability; a full
database blocks new dispatch until a write probe succeeds.

### sloop pause / sloop resume

Stop or resume spawning. In-flight agents finish and go through aftercare.
The paused state is persisted and survives daemon restarts.

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
timeout (default 3600 seconds). Lets scripts and CI gate on a run
directly.

### sloop logs <RUN>

Show a run's captured output — both stdout and stderr, in order. The
underlying file is `runs/<run-id>/output.ndjson` in the state directory.

### sloop reindex

Rebuild the derivable SQLite index from the configured project and ticket
directories, Git branches, and orphaned worktrees. Project and ticket files
remain authoritative for membership, blockers, and worktree branches; Git
restores merged and review-needed states. Runtime history is preserved for
tickets that still exist, while rows belonging to removed tickets are dropped.
If SQLite was deleted, tickets without Git evidence return as ready; holds,
notes, attempts, and other runtime-only history cannot be recovered. The daemon
must be idle before reindexing.

## Worker commands

These are the agent's entire vocabulary. They require the `SLOOP_SOCKET`
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
target, model, and effort. References to other tickets are rejected.

### sloop note <TEXT>...

Append an advisory note to the current run. This is the worker's only
write, and it moves nothing: no note changes a ticket's state, and claims
like "done" carry no weight. Notes appear in `sloop show <project>` output
and may not survive a state rebuild.
