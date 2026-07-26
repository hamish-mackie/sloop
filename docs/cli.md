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
  the current stage's result check is `reported` or a `panel`, affects flow
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
available from an installed binary. It prints the current grammar — `action`,
`result_check`, and `fail_action` written out on every stage, one `return_to`
edge, one advisory stage, and a commented `panel` block — annotated with the
structural rules the parser enforces, so the printed file is both a working
flow and the schema.

`sloop template ticket`'s `flow:` comment names the two flows that ship inside
the binary and are written out by `sloop init`: `default` (build, review,
merge) and `train` (build, sync, verify, `ff_only` merge).

### sloop daemon

Ensure the daemon is running and report `{pid, socket, version, started}`
along with the state directory and log path. Idempotent: connecting to a
live daemon and starting a fresh one look the same. Every other operator
command does this implicitly.

### sloop daemon restart

Ask the daemon to restart when it is safe. The command returns immediately
with the number of active runs still draining. No new runs start during the
drain; active runs continue through every remaining flow stage. When the
last run settles, the daemon releases its sockets and lock, then replaces
itself with the binary currently installed at the path from which it started.
Queued triggers remain queued and resume automatically in the replacement.

Use `sloop show` to inspect the drain or `sloop show --follow` to stream its
events. `sloop resume` cancels a pending restart and continues dispatching in
the current process. For an immediate teardown, use `sloop stop`; restart has
no force mode.

### sloop post <FILE> [--project P] [--flow F] [--auto | --at TIME | --manual | --hold]

Validate and register a ticket file (which must live below the configured
ticket directory), stamping the allocated ID and worktree branch back into
the file. The trigger modes are mutually exclusive:

- `--auto` (default) — queue one run for the next available opportunity.
- `--manual` — register the ticket as ready without queuing a run.
- `--hold` — register the ticket as held; `sloop ready` releases it.
- `--at HH:MM` — queue one run for the next occurrence of that local time;
  it still waits for running hours, capacity, cooldown, and budget gates.

Reposting an edited file updates the ticket in place without queuing a
duplicate run; reposting with a different `--at` time reschedules the
queued run.

A run is queued only when the post leaves the ticket ready. A post never
moves a ticket out of `merged`, `failed`, or `needs_review`, so reposting a
settled ticket refreshes its indexed content and queues nothing — a run
pinned to it could never be dispatched. Neither `--auto` nor `--at` changes
that, and `--at` does not re-time an existing queued run either. The output
says which state suppressed it:

```
ticket TICK-43 updated from .agents/sloop/tickets/cooldown.md (project default, merged)
no trigger queued: TICK-43 is merged
```

`failed` is the one of the three a single verb undoes, so its line names that
verb:

```
ticket TICK-44 updated from .agents/sloop/tickets/broken.md (project default, failed)
no trigger queued: TICK-44 is failed; `sloop retry TICK-44` returns it to ready
```

With `--json` the same distinction is `trigger_suppressed`:
`{"reason": "terminal_ticket", "state": "<ticket state>"}` when a trigger was
asked for and refused, and `null` when none was asked for at all — which is
what `--manual` and `--hold` produce. Read that field rather than a null
`trigger`, which cannot tell the two apart.

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

Time-based triggers use the same scheduler gates as an immediate run:

- `--at HH:MM` queues one run for the next occurrence of that local time.
- `--every INTERVAL` queues recurring work, first due after the interval.
  Missed intervals advance on the original cadence rather than causing a
  burst of catch-up runs.
- `--overnight` queues one run for the next open `running_hours` window. If
  no window is configured, it is dispatchable immediately.

A queued run pinned to a ticket is retired the moment that ticket merges,
including a merge reconciled from an external branch. Dispatch only ever
selects a `ready` ticket and `merged` is terminal, so such a run could never
start; leaving it queued would overstate pending work in `sloop show`. This
applies to `--every` as well — a recurring run pinned to a merged ticket has
nothing left to recur on. Runs that are not pinned to a ticket are demand for
whatever is ready and survive the merge of any ticket they selected. Other
outcomes leave queued runs alone: `failed`, `held`, and `needs_review` can all
return to `ready`.

A daemon started against state left by an older version completes any queued
run still pinned to an already-merged ticket, logging one
`trigger_completed_on_merged_ticket` record per run it retired.

### sloop retry <TICKET>

Return a failed ticket to ready and reset its attempt counter.

It queues nothing. A failed run consumes the trigger that started it, so a
retried ticket is `ready` with no queued trigger and does not run by itself;
follow it with `sloop run <TICKET>`. `sloop show` says as much on the ticket's
row:

```
TICK-4  ready  Broken ticket  — ready but no queued trigger; enqueue with `sloop run`
```

### sloop hold <TICKET> / sloop ready <TICKET>

Hold a ready ticket so it cannot be dispatched; release it again. Held
tickets are skipped by selection and rejected by named runs.

### sloop show

```text
sloop show [REF_OR_PATTERN] [-N] [--follow] [--quiet]
```

Without an argument, `sloop show` is a dashboard with daemon and gate
state, ticket counts, the next wake time, active runs, everything waiting on a
human, and the 10 newest tickets. Queue depth is not a line of its own: each
recent ticket carries the scheduler's reason instead, so a ticket with no
queued trigger says so where you are already looking. The `--json` payload does
carry the full `queued_triggers` list. `sloop show -N` changes the number of
recent tickets; `-n N` and `--limit N` are equivalent. A limit of zero or a
non-numeric limit is a usage error.

```
daemon: pid 3760 running - 1/2 agents active - next wake in 4m12s
tickets: 0 ready, 1 claimed, 1 needs_review, 77 merged

runs:
  TICK-86-r2  driving  19:51-... (9m0s)  build:ok -> sync:ok -> test:.. -> merge:-  Move db.rs to db/mod.rs

attention:
  TICK-64  needs_review  Split the run store

recent:
  TICK-91  merged  Group the CLI presentation layer into a src/cli directory

77 more - `sloop show -78` for all - `sloop show <ref>` for detail
```

The count line prints a state only when its count is non-zero, except `ready`,
which always appears because an empty queue is itself a signal; `merged` sits
last. `next wake` is a countdown rather than an instant — the `--json` envelope
keeps it as an RFC3339 timestamp. A run or stage that has not finished shows an
open span with the time elapsed so far, `19:51-... (9m0s)`; no end time is
invented for it.

The `attention:` section lists every ticket in `needs_review` or `failed`,
newest first, with its reason when it has one. It is absent when there are
none, and it is not limited by `-N`: a ticket waiting on a person stays visible
however far it has aged past the recent window.

The `(project)` column is printed only when the rows on screen span more than
one project, so a single-project repository never repeats the same name down
every line.

When stdout is a terminal, a restrained palette marks `FAIL` and `failed` in
red, `warn` and `needs_review` in yellow, `ok` in green, and `merged` dim, and
the stage strip joins its markers with `→`. Everything else is unstyled. Set
`NO_COLOR`, redirect to a file, or pipe the output and no escape sequences are
emitted at all, and the strip falls back to the ASCII `->` shown below;
`--json` never carries either.

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
adds `kind`, `attention`, `recent`, `recent_total`, and `recent_limit` to the
existing status fields; a pattern response has top-level `kind`, `ref`, and
`tickets` fields.

The `runs:` section lists every run of the ticket, newest attempt first: run
alias, outcome, wall-clock span, and a strip of the run's flow stages, joined
by an arrow to read as the walk they are, and marked:

| marker | meaning |
| ------ | ------- |
| `ok`   | passed |
| `FAIL` | failed, and the walk stopped there |
| `warn` | failed on a stage whose `fail_action` is `continue` — recorded, stepped over |
| `..`   | running |
| `-`    | not reached |

A stage a `return_to` edge re-entered appears once per execution, and the
re-runs carry a `#<attempt>` suffix. The suffix appears only past the first
attempt, so a flow without loops reads exactly as it always did.

```
runs:
  TICK-5-r3  merged        21:40-21:52  build:ok -> build#2:ok -> test:FAIL -> test#2:ok -> merge:ok
  TICK-5-r2  merged        20:15-20:21  build:ok -> lint:warn -> test:ok -> merge:ok
  TICK-5-r1  needs_review  19:02-19:09  build:ok -> test:FAIL -> merge:-
```

`sloop show <run>` expands one of those lines:

```
TICK-5-r1  (needs_review)
ticket: TICK-5  Persist cooldowns
branch: sloop/TICK-5-a1-9c82d4e1
worktree: <repository>/.worktrees/9c82d4e1
timeline: claimed 19:02  started 19:02  finished 19:09
agent exit: 0
reason: stage `test` failed (exit 1) after agent completed with commits
stages:
  build  passed   19:02-19:05  3m0s  exit 0  verdict from exit_code
  lint   failed   19:05-19:05  4s  exit 2  advisory  verdict from exit_code
  test   failed   19:05-19:09  4m12s  exit 1  verdict from exit_code
  merge  pending  -
```

`branch` and `worktree` are the run's own, not the ticket's: each run gets a
fresh branch named `sloop/<ticket>-a<attempt>-<short run id>` and a worktree
under `worktree_dir`, so two attempts at one ticket never share either.

Two things about that output are deliberate. `agent exit` is labeled rather
than bare, because `exit: 0` on a run whose later stage failed reads as "the
run passed". And `reason` is *derived from the stored stage and evidence
rows* — never from an agent's own claim about its work — so it appears on
every non-merged terminal run, naming the stage that actually failed. A
merged run carries no reason, and a run still in flight shows its current
stage as `..` with an open-ended span rather than a guessed ending.

An advisory failure is never that stage: the walk stepped over it by
construction, so the reason names the last *halting* failure instead. A
terminal run whose only failures were advisory says so explicitly:

```
reason: run ended as needs_review with only advisory failures recorded (stage `lint`)
```

Where the failed stage alone does not explain the ending, the reason gains a
trailing clause:

- a spent backward edge — `…; return_to budget spent`
- a stage log that no longer replays against the run's flow —
  `…; the stage log does not replay against this flow`

A plain `fail_action: fail` halt gets no clause, because the sentence naming
the failed stage already is one. With `--json` the same distinction is a
stable token in `value.halt`: `fail_action`, `return_budget_exhausted`, or
`corrupt_log`.

Stage names come from the run's admitted flow snapshot, so a run still shows
the stages it actually had even if the flow file changed afterwards. A stage a
`return_to` edge re-entered is one row per execution, labelled `#N` past the
first, rather than one row carrying a count.

A stage judged by a `reported` check shows the worker's `--confidence` beside
its verdict source. A stage judged by a `panel` lists every seat under it —
target, verdict, confidence, and reason — including seats that never reported,
because a silent seat is a `Fail` the quorum counted:

```
stages:
  build   passed   19:02-19:05  3m0s  exit 0  verdict from exit_code
  review  failed   19:05-19:11  6m0s  verdict from panel
    claude  pass  confidence high  reads correct
    codex   fail  confidence medium  missing a test
    gemini  fail  no verdict reported
```

A seat that never reported has no confidence to state, so its line carries only
the `fail` the quorum counted and the `no verdict reported` reason.

The dashboard (`sloop show` with no argument) did not widen for any of this. It
is still one line per active run and one per recent ticket; its run lines carry
the same stage strip, so `#2` and `warn` show up there too, but nothing gained a
column and the per-stage detail stays in the ticket and run views.

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

Stop or resume spawning. In-flight runs finish their remaining stages.
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

### sloop logs <RUN> [--stage STAGE[#ATTEMPT]] [--tail N] [--follow]

Show a run's captured output — both stdout and stderr, in order. The
underlying file is `runs/<run-id>/output.ndjson` in the state directory.
Use [`sloop show`](#sloop-show) first for the run's derived outcome, timeline,
and stage summary; use `logs` for the captured output behind that evidence.

A bare read shows the last 64 entries, the way `tail` does, and states what it
left out:

```
showing the last 64 of 312 entries; --tail N or --follow for more
```

A page with no such line showed everything that matched.

`--stage` narrows the output to one flow stage, named exactly as the flow
names it: `--stage build` is the agent's own output, `--stage test` is that
exec stage's. A stage the run's flow does not define is an error listing the
ones it does, not an empty page.

Add `#<attempt>` to narrow further to a single execution. A `return_to` edge
runs one stage more than once, and past that point the name alone selects two
interleaved passes of the same command:

```sh
sloop logs TICK-5-r3 --stage build      # every pass
sloop logs TICK-5-r3 --stage build#2    # only the pass the edge caused
```

The suffix is the same label `sloop show` prints in its stage table and in each
log line's origin (`[stage:build#2]`), so a row read there is a selector that
can be pasted back. An attempt the stage never reached is an empty page, since
the stage exists and how often it ran is exactly what was asked; a malformed
one (`--stage build#two`) is a usage error rather than a silent miss.

`--tail N` widens or narrows that window, up to 1000. An entry is one captured
chunk, matching how the NDJSON file is stored, so `--tail 50` is "the last 50
records" rather than "the last 50 lines".

`--follow` streams the run from its first entry as more are appended, and exits
when the run reaches a terminal state. On a run that has already settled it
prints what exists and exits. All three combine: `sloop logs <RUN> --stage test#2 --tail 50` answers
"why did the second pass of the test stage fail" in one command, and
`--stage test --follow` streams only that stage.

Filtering happens in the daemon, so any client of the socket gets it — the
attempt suffix included: this is the `logs` verb with `stage`, `tail`, and
`after` arguments, returning a page plus `next_cursor`, `complete`, `elided`,
and `terminal`. The tail default is the CLI's own: on the socket, an omitted
`tail` still reads forward from the cursor. `--follow` is a client-side loop over that cursor, like
`show --follow` over `events`.

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
worktree path, the branch, the stage being executed, and the definition of
done. Designed to be re-read at any time — an agent that loses context can
recover its assignment.

The brief is keyed on the **stage execution**, not the run:

```json
"stage": { "name": "review", "attempt": 1, "result_check": "reported" }
```

- `name` is the stage the caller is executing. For a panel reviewer it comes
  from the seat's own credential, so a reviewer reads the stage its token names
  and nothing about the seats beside it.
- `attempt` counts executions of that stage. A `fail_action: { return_to: … }`
  edge re-enters it, and each execution is a separate assignment owed its own
  report — an agent on `attempt: 2` is being run again, not for the first time.
- `result_check` names what the stage turns on: `none`, `reported`, `commits`,
  `exec`, `agent`, or `panel`. It is the *kind* of judge, never its command or
  target: a worker is told about its own stage, not about the flow's shape.

`definition_of_done` follows from that check and from whether the caller is the
stage's worker or a reviewer on its panel, so it says what this stage turns on
and nothing about how to operate the CLI:

| Caller | Definition of done |
| --- | --- |
| a panel reviewer | the stage passes only on its reported verdict; the work under review is not its to change |
| `result_check: reported` | the stage passes only on the worker's reported verdict |
| `result_check: { builtin: commits }` | commit the work to the run branch |
| `result_check: none` | the worker's own exit status is the verdict |
| any other check | an independent check judges the work after the worker exits |

Only a `commits` check asks for a commit. A reviewer's brief never does — its
prompt tells it to change nothing, and the two must agree.

### sloop show <REF>

Read-only lookup of the current run's ticket, including its snapshotted
target, model, and effort. References to other tickets are rejected. (The
same verb on the operator socket resolves any ticket, run, or project — see
[Operator commands](#operator-commands).)

### sloop note <TEXT>...

Append an advisory note to the current run. It moves nothing: no note changes
a ticket's state, and claims like "done" carry no weight. Notes appear in
`sloop show <project>` output and may not survive a state rebuild.

### sloop verdict pass|fail [--reason <TEXT>] [--confidence low|medium|high]

Report the current stage's verdict. Two callers may use it, each exactly once
per stage execution:

- **the stage's own worker**, when that stage declares `result_check: reported`.
  This is the only thing that can pass such a stage; one that exits without
  calling it fails with `no verdict reported`. `--reason` is optional here.
  A worker on any other stage is refused with `stage \`<name>\` does not use
  \`result_check: reported\``.
- **a panel reviewer**, when the stage's check is `result_check: { panel: … }`.
  Its credential names the seat the report lands on, so nothing about the call
  chooses one, and `--reason` is mandatory: a panel's verdict is a tally, and an
  unexplained vote tells the operator reading `sloop show` nothing.

The first report for an execution is persisted and final; a second is rejected
with `conflict`. A `return_to` edge that re-enters the stage begins a fresh
execution, which is owed its own report — the first execution's verdict is
never reused for the second.

`--confidence` defaults to `medium` and takes only those three words — a float
is rejected. It is recorded as evidence and shown by `sloop show`, from either
caller, and never weighted into any decision: a `fail` at low confidence counts
against a panel's quorum exactly as much as one at high.
