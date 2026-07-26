# Concepts

## A scheduler, not an orchestrator

Sloop deliberately does one job: decide *when and where* an agent runs, and
judge *what its run produced*. Everything that decides what happens is
code — deterministic, testable without an LLM. The agent's contract is
narrow: here is a ticket, here is a worktree, make commits, exit. It does
not pick its branch, its merge target, or its next task.

There is no planning, no agent-to-agent coordination, no memory, and no
chat. Higher-level systems that want those things can build them on top of
Sloop's socket API.

## Tickets, runs, and triggers

Three words carry the whole model:

- A **ticket** is *what to do*. You write it as a Markdown file and
  `sloop post` registers it.
- A **run** is *one attempt at it*. Sloop creates it, walks it through a flow,
  and derives its outcome from the evidence.
- A **trigger** is the durable record that *demand exists*. It is the only
  thing that makes the dispatcher pick work up.

That third one is the piece most people are missing, so it is worth stating
flatly:

> **A `ready` ticket does not run until something queues a trigger for it.**
> `ready` means "nothing about this ticket is stopping it", not "this ticket is
> about to run".

Exactly two commands queue a trigger:

- `sloop post <file>` queues one for the ticket it registers — unless you pass
  `--manual` or `--hold`, or the ticket is already in a terminal state.
- `sloop run` queues one at any time: for a named ticket, for a project, or for
  whatever is ready.

Nothing else does. In particular `sloop retry` returns a failed ticket to
`ready` and stops there — the failed run consumed its trigger, so a retried
ticket waits for a `sloop run` like any other. (`sloop hold` and `sloop ready`
only park and release a ticket; a trigger queued before the hold is still
queued after the release, and fires then.)

When a ticket is sitting in `ready` with nothing happening, `sloop show` says so
on the ticket's own row:

```
TICK-1  ready  Add request logging  — ready but no queued trigger; enqueue with `sloop run`
```

### Ticket states

| state | meaning |
| ----- | ------- |
| `ready` | Nothing about the ticket is stopping it. It runs once a trigger is queued for it and the gates are open — and not before. |
| `held` | An operator parked it; `sloop ready` releases it. |
| `blocked` | Some ticket in its `blocked_by` list has not merged yet. Derived, not stored, so it clears the moment the last blocker merges. |
| `claimed` | An active run owns it. |
| `merged` | Terminal: the work was integrated into the default branch. |
| `failed` | Terminal: the run did not succeed; `sloop retry` returns it to `ready`. |
| `needs_review` | Terminal: the run left a branch a human has to judge. |

A post cannot move a ticket out of the three terminal states, so reposting a
settled file refreshes its indexed content and queues nothing.

### When a trigger comes due

A trigger carries a schedule, and its kind is what decides when it fires:

- `sloop post --auto` (the default) and a plain `sloop run` are due on sight —
  the next moment the gates allow.
- `--at HH:MM`, on either command, is due at the next occurrence of that local
  time.
- `sloop run --overnight` is due at the next opening of the configured
  `running_hours`.
- `sloop run --every <interval>` is due repeatedly, rearming each time it fires.
  Missed intervals collapse into one step, so a daemon asleep for an hour on a
  one-minute cadence owes one run, not sixty.

Coming due is not the same as running. A due trigger still passes every gate
below before anything spawns, and a one-shot trigger is retired when it fires
while a recurring one is rearmed. A trigger pinned to a ticket that merges is
completed rather than left as demand that can never be met.

## The life of a run

1. **Select.** The dispatcher pulls ready, unblocked work — a pure function
   of the queue of triggers, optionally scoped to a ticket or project.
2. **Gate.** Every spawn, including explicitly named runs, must pass the
   same checks: not paused, inside running hours, below
   `max_parallel_tasks`.
3. **Claim.** The ticket is claimed with a conditional database update that
   takes a lease. Exactly one claimant can win; a ticket is never
   double-spawned, even when two runs race for it. The lease is the daemon's
   own — it records which daemon process holds the ticket, and the database
   itself permits at most one lease per ticket. Workers never hold or see
   leases; a worker gets a scoped token for its own run instead.
4. **Admission.** Ticket → branch → fresh Git worktree → a driver that owns
   the run from here until it settles.
5. **The walk.** The driver executes the ticket's bound flow one stage at a
   time. An agent stage — in any position, any number of times — is spawned as
   a supervised child process group with its own worker socket and token in the
   environment. Output is captured continuously to the run log; an agent can
   read its assignment with `sloop brief` at any time.
6. **Settlement.** When the walk completes or halts, Sloop derives the run's
   outcome from the evidence every stage left behind.

Merged run worktrees and run branches remain inspectable for the configured
retention period, then periodic reconciliation removes them. Failed and
`needs_review` worktrees remain as evidence until their ticket is resolved;
run output and recorded evidence are retained when cleanup removes Git state.

One async dispatcher task owns every admission decision. Socket handlers and
run drivers send it requests; they never admit anything themselves.
That single ownership — not politeness between callers — is what makes
gate-then-claim atomic.

## Stages and flows

A **flow** is the ordered list of stages a run walks. A **stage** is three
things:

- an **action** — the work: the ticket's agent, an argv in the run worktree, or
  one of Sloop's git builtins;
- a **result check** — what decides the stage's verdict: the action's own exit
  status, a new commit Sloop observed, a separate command, a `sloop verdict`
  report made over credentials scoped to that one stage, or a panel of
  reviewers whose reports a quorum rule counts;
- a **fail action** — what a failing verdict does to the walk: halt it, record
  the failure and continue, or return to an earlier stage and re-run the span.

The split between the first two is the design. An action is an untrusted
process; the check is the part Sloop decides for itself. A stage whose action is
an agent may not be judged by its own exit status, because "the process exited
0" is a claim the agent controls. Everything that decides an outcome is
deterministic code over evidence Sloop observed.

Because a check is a separate step, the vocabulary composes rather than
multiplying: the same `exec` that can be an action can be a check, an agent can
appear in any position any number of times, and a backward `return_to` edge
turns a linear list into a bounded loop without any stage learning about
looping. The [configuration guide](configuration.md#flows) has the grammar.

## Attempts and the evidence log

Every stage execution appends one row to the run's evidence log: which stage,
which attempt, the verdict, where the verdict came from, and why. The log is
append-only and ordered, and the walk's position is *always recomputed by
replaying it* rather than read from a stored cursor. That is what makes a run
resumable — a daemon that died mid-flow replays the same rows, derives the same
next stage, and re-runs the interrupted one idempotently.

`sloop show <run>` is that log, read back:

```
stages:
  build    passed   19:02-19:05  3m0s  exit 0  verdict from exit_code
  build#2  passed   19:08-19:14  6m0s  exit 0  verdict from exit_code
  test     failed   19:05-19:08  3m11s  exit 1  verdict from exit_code
  test#2   passed   19:14-19:17  2m40s  exit 0  verdict from exit_code
  merge    passed   19:17-19:17  0s  exit 0  verdict from exit_code
```

Rows are in flow order, and every execution of one stage sits together under its
name — so the table reads as the flow, not as a clock. The timestamps are what
tell you the walk actually went `build`, `test`, `build#2`, `test#2`.

A stage's **attempt** is which execution of it a row is. A `return_to` edge
re-enters an earlier stage and re-runs the whole span from there, and every
execution gets a row of its own, labelled `#2`, `#3`, and so on past the first —
the rows above show a `test` failure sending the walk back to `build`, and the
second lap passing. Nothing is ever folded into a count on a single row, so a
stage that ran twice is two rows with two verdicts, two spans, and two pages of
output that `sloop logs --stage build#2` can name one of.

A failing stage marked `advisory` was recorded and stepped over rather than
ending the walk; the run's own reason names the stage that actually halted it,
and adds a clause when the ending needs one — `return_to budget spent` when a
backward edge ran out of laps, or `the stage log does not replay against this
flow` when the recorded history no longer describes a walk over the run's flow.

Stage names come from the flow snapshot admitted with the run, so a finished run
still shows the stages it actually had even after the flow file changes.
`--json` returns the same rows structurally; see the
[protocol reference](protocol.md).

## Outcomes come from process and stage evidence

Sloop never trusts what the agent says in free-form output or notes. It derives
the outcome from process, Git, check, merge, and policy-gated report evidence:

- **Exit 0, flow stages pass** → the run branch merges.
- **Exit 0, default agent policy, no commits** → the ticket needs review.
- **Exit 0, a later stage fails with commits** → the branch is kept for human
  review.
- **Exit 0, a later stage fails with known no commits** → the ticket fails.
- **Exit 0, a later stage fails while commit evidence is incomplete** → the
  branch is conservatively kept for human review.
- **Nonzero exit** → the ticket fails, regardless of commit count.
- **The vendor rejects authentication or configuration** → the ticket fails
  with a safe diagnostic.
- **The vendor rate-limits or returns an unknown rejection** → the ticket
  returns to ready and its agent target cools down for five minutes. The
  queued trigger retries after the cooldown, which survives daemon
  restarts. Any commits from a rejected run stay on the preserved run branch.

Sloop recognizes these failures from built-in, versioned vendor rule catalogs.
It matches only captured agent output, never test or merge output, and stores
the matched class, vendor, rule ID, and catalog-authored diagnostic as evidence.
Raw output remains in the run log rather than being copied into diagnostics.

Commit OIDs are recorded using the run branch's creation point as their
baseline. They feed the `{ builtin: commits }` check an agent action gets by
default, and the project activity view. When evidence is incomplete, Sloop does
not infer a pass.

A `needs_review` ticket is resolved by merging its preserved run branch into the
default branch by hand. The running daemon now notices this: on each periodic
reconciliation pass it re-resolves each review branch tip and, when that tip is
a strict ancestor of the default branch tip, settles the ticket to `merged` and
releases its `blocked_by` dependents — typically within one reconciliation
interval, with no reindex. The observation is recorded as evidence, so it is
idempotent across restarts. Squash- and rebase-merges rewrite the commits, so
the branch tip is no longer an ancestor and the integration cannot be proven by
ancestry; those still require `sloop reindex`.

A worker's `sloop note "done, merged, ship it"` stores a note and moves
nothing.

Failed tickets keep an attempt count as evidence; `sloop retry` resets it
and returns the ticket to ready. It queues nothing, so the ticket sits there
until a `sloop run` gives it a trigger.

## The operator/worker split

Two verb sets, enforced by two sockets rather than documentation:

- The **operator socket** has a fixed path and mode `0600`. Whoever holds
  it — you, a script, another program — decides what runs.
- The **worker socket** is created per run and handed to the agent via
  `SLOOP_SOCKET` with a random per-run `SLOOP_TOKEN`.

The daemon rejects operator verbs on worker connections, scopes a worker's
reads and notes to its own run, and invalidates the token when the run ends.
The worker's vocabulary is `brief`, `show`, `note`, and `verdict`. The last is
accepted only while that worker's current stage uses a `reported` or `panel`
result check, and its first persisted report is final. A worker cannot claim work, change
status directly, or merge, even at 3am, even if it tries.

`show` is the one verb both sockets answer. On the worker socket it is
scoped to the run's own ticket; on the operator socket it is an unrestricted
read that resolves any ticket, run, or project. It stays read-only on both:
the split adds an operator read, never a worker capability.

This stops accidents and improvisation, not a determined adversary —
same-uid isolation would need a real sandbox. Accidents are the actual
threat model.

## Crash recovery

The daemon persists enough evidence to recover from its own death. Every
run records its agent's PID *and process start time* — the pair, not the
PID alone, identifies a process, so a recycled PID is never mistaken for a
live agent. At startup, every in-flight run is classified:

- Process still alive (PID and start time match) → re-adopt and keep
  supervising it. Its ticket is not double-spawned.
- Process dead before its exit was checkpointed → release the ticket and keep
  its branch and worktree for autopsy. Commit count does not change this
  classification.
- Daemon died mid-walk → every stage is individually evidenced, so the driver
  replays the log and re-runs the interrupted stage idempotently, wherever in
  the flow it sits.

Recovery is driven entirely by that process identity, never by how old a
lease is. Expiry is a report, not an authority: the daemon renews the lease
of every run it supervises on each reconcile pass, so a live run's lease
stays in the future for as long as the run lasts, and an expired lease
means nobody was there to renew it. Renewal is strict — an expired lease
cannot be renewed — so a daemon returning after longer than the lease TTL
re-arms the lease of each run it readopts, having first confirmed that
run's process is alive. A run whose process identity fails is settled, not
readopted, so its lease is never lifted. The lease row is dropped when the
ticket settles or its claim is rolled back, so a stale row left behind is
evidence of an owner that died mid-work.

A lock file guarantees at most one daemon per repository; a second
`sloop daemon` connects to the first instead of racing it.

If SQLite reports that its storage is full, the daemon keeps active and
finished runs reserved, blocks new dispatch, and reports the storage gate in
`sloop show`. It periodically attempts a small committed
write; after space becomes available, pending outcomes settle and dispatch
resumes automatically. If the agent's exit checkpoint could not be written,
Sloop stops the walk before any side-effecting stage and preserves the run
branch for review.

## Files versus runtime state

Anything a human writes lives in committed files: tickets, projects, flows,
prompts, configuration. They travel with the repository and are reviewable
in a PR.

Everything the daemon learns at runtime — ticket status, runs, leases,
attempts, notes, evidence — lives in a local SQLite database that only the
daemon writes. It is machine-specific and worthless to another clone. The
committed files always win: the daemon reconciles them into its index at
startup, and runtime history (such as notes) is the part that cannot be
reconstructed from files and Git. Queued triggers are in that same
irreplaceable category: nothing in the repository records that you asked for a
run at 02:00, so `sloop reindex` cannot put one back.

Machine-local state never lives in the repository. On Linux it is under
`~/.local/state/sloop/repositories/<repository>/`, with sockets in
`$XDG_RUNTIME_DIR`; on macOS, under `~/Library/Application Support/sloop/`
with logs in `~/Library/Logs/sloop/`. All paths are keyed by the canonical
repository path, and `sloop daemon` prints the exact locations.
