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

## The life of a run

1. **Select.** The dispatcher pulls ready, unblocked work — a pure function
   of the queue, optionally scoped to a ticket or project.
2. **Gate.** Every spawn, including explicitly named runs, must pass the
   same checks: not paused, inside running hours, below
   `max_parallel_tasks`.
3. **Claim.** The ticket is claimed with a conditional database update that
   takes a lease. Exactly one claimant can win; a ticket is never
   double-spawned, even when two runs race for it.
4. **Dispatch.** Ticket → branch → fresh Git worktree → agent, spawned as a
   supervised child process group with its worker socket and token in the
   environment.
5. **Run.** Output is captured continuously to the run log; the agent can
   read its assignment with `sloop brief` at any time.
6. **Aftercare.** After the agent exits, Sloop gathers evidence and decides
   the outcome: run the configured test command, and merge the work if it
   passes.

One async dispatcher task owns every spawn decision. Socket handlers and
run supervisors send it requests; they never spawn anything themselves.
That single ownership — not politeness between callers — is what makes
gate-then-claim atomic.

## Outcomes are derived, never declared

The signature failure of unattended agents is exiting `0` having done
nothing. So Sloop never trusts an exit code, and never trusts what the
agent says. It derives the outcome from evidence:

- **Exit 0, commits exist, tests pass** → the work merges.
- **Exit 0, no commits** → not done. The ticket fails; nothing merges.
- **Commits exist, tests fail** → the work is kept on its branch and the
  ticket is left for human review.
- **The agent crashed but committed first** → the commits are preserved for
  review; a supervisor dying never throws work away.

A worker's `sloop note "done, merged, ship it"` stores a note and moves
nothing.

Failed tickets keep an attempt count as evidence; `sloop retry` resets it
and returns the ticket to ready.

## The operator/worker split

Two verb sets, enforced by two sockets rather than documentation:

- The **operator socket** has a fixed path and mode `0600`. Whoever holds
  it — you, a script, another program — decides what runs.
- The **worker socket** is created per run and handed to the agent via
  `SLOOP_SOCKET` with a random per-run `SLOOP_TOKEN`.

The daemon rejects operator verbs on worker connections, scopes a worker's
`show` and `note` to its own ticket, and invalidates the token when the run
ends. The worker's whole vocabulary is `brief`, `show`, `note` — read,
read, advisory write. An agent cannot claim work, change status, or merge,
even at 3am, even if it tries.

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
- Process dead, commits in the worktree → send the work to aftercare; it is
  not lost because the supervisor died.
- Process dead, worktree bare → release the ticket and keep the worktree
  for autopsy.
- Daemon died mid-aftercare → aftercare stages are individually evidenced,
  so the interrupted stage is re-run idempotently.

A lock file guarantees at most one daemon per repository; a second
`sloop daemon` connects to the first instead of racing it.

If SQLite reports that its storage is full, the daemon keeps active and
finished runs reserved, blocks new dispatch, and reports the storage gate in
`sloop status` and `sloop list`. It periodically attempts a small committed
write; after space becomes available, pending outcomes settle and dispatch
resumes automatically. If the pre-aftercare checkpoint could not be written,
Sloop skips side-effecting aftercare and preserves committed work for review.

## Files versus runtime state

Anything a human writes lives in committed files: tickets, projects, flows,
prompts, configuration. They travel with the repository and are reviewable
in a PR.

Everything the daemon learns at runtime — ticket status, runs, leases,
attempts, notes, evidence — lives in a local SQLite database that only the
daemon writes. It is machine-specific and worthless to another clone. The
committed files always win: the daemon reconciles them into its index at
startup, and runtime history (such as notes) is the part that cannot be
reconstructed from files and Git.

Machine-local state never lives in the repository. On Linux it is under
`~/.local/state/sloop/repositories/<repository>/`, with sockets in
`$XDG_RUNTIME_DIR`; on macOS, under `~/Library/Application Support/sloop/`
with logs in `~/Library/Logs/sloop/`. All paths are keyed by the canonical
repository path, and `sloop daemon` prints the exact locations.
