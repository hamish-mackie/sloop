# Changelog

All notable changes to Sloop will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- A new `{ builtin: sync }` action merges the default branch into the run
  branch, inside the run worktree. It passes when that merge commits cleanly or
  there was nothing to integrate, and fails on a conflict — aborting the merge
  so a `return_to` target starts from a clean tree, with git's conflict output
  captured in the run log and so in the re-entered agent's prompt. Any number
  of sync stages, anywhere before the merge; the shared default-branch checkout
  is only ever read.
- The merge stage takes `{ builtin: merge, ff_only: true }`, which refuses the
  merge commit: the default branch either fast-forwards to the run branch head,
  or the stage fails having touched nothing. Absent, the merge policy is
  unchanged. `ff_only` is meaningful nowhere else and is a parse error on any
  other action.
- `sloop init` writes a `train` flow beside `default.yaml`, opt-in with
  `flow: train`. It is the merge train: `build → sync → verify → ff_only
  merge`, with `fail_action: { return_to: sync }` on the merge, so a default
  branch that moves between the verification and the merge loops the train
  rather than landing a tree no stage tested. Its `verify` stage uses the
  repository's configured `flow.test_cmd` when it has one.
- Flow stages can now bind the two `fail_action` forms that previously parsed
  but were rejected. `fail_action: continue` makes a stage advisory: its
  failure is recorded and visible in `sloop show`, the walk carries on, and the
  run's outcome is unchanged. `fail_action: { return_to: <stage>, attempts: N }`
  is a bounded backward edge: the walk re-enters an earlier stage and re-runs
  the whole span from there, so no verdict earned before the loop can reach the
  merge. Edges must point backwards, `attempts` is capped at 3, and a flow
  whose budgets could execute more than 32 stages is refused at parse time.
- A re-entered `agent` stage is told why it is running again. Sloop appends a
  delimited `previous attempt failed` block to its prompt — after the ticket
  body and the worker instructions — naming the stage that failed, its resolved
  reason, and the last 100 lines of that execution's captured output. The block
  is rebuilt from the run's persisted evidence, so a daemon that restarts
  mid-loop composes the identical prompt. `exec` actions are handed nothing:
  their command line is fixed by the flow.
- `sloop show` renders each stage execution separately, suffixing re-runs with
  their attempt (`build`, `build#2`), and a non-merged run's derived `reason`
  names the failure the walk actually stopped on rather than one a later
  attempt superseded.

### Changed

- Captured output records, stage-process checkpoints, agent-exit checkpoints,
  and reported verdicts are all scoped to `(stage, attempt)` rather than stage
  alone, so a re-entered stage gets its own output, its own worker report, and
  its own exit checkpoint instead of inheriting an earlier attempt's.

### Deprecated

- `on_fail` repair blocks. `fail_action: { return_to: <stage> }` covers the
  same ground with the flow's own stages — a failing check stage returning to
  the stage it guards — and the daemon now logs a `flow_on_fail_deprecated`
  note when it admits a run whose flow uses `on_fail`. Nothing is removed:
  existing flows keep parsing, repairing, and settling exactly as before.

### Fixed

- A Claude session limit now classifies as `rate_limited` and cools the target
  down instead of failing the ticket. The rule required stderr, but the
  `claude` target runs with `--output-format stream-json` and reports vendor
  rejections as synthetic assistant messages on stdout; its signature also only
  covered `You've hit your limit`, not the `You've hit your session limit`
  wording a real run hit. The rule now matches either stream on the wording
  every limit window shares. Without this, one vendor limit could burn several
  tickets' retry attempts and strand them in `failed`.

## [0.3.0] - 2026-07-21

### Added

- The read surface is now one verb. Bare `sloop` or `sloop show` prints a
  dashboard: daemon status, scheduler state, and recent activity.
  `sloop show <REF>` resolves ticket IDs, ticket names, run IDs and aliases,
  unique run-id prefixes, and project IDs; an exact reference always wins.
  Anything else becomes a case-insensitive ticket pattern over IDs and
  names — a substring by default, an unanchored regular expression when the
  text contains regex metacharacters — rendered as a ticket list.
  `-n`/`--limit N` or the shorthand `-N` caps list rows. `--follow` streams
  the shown scope's events; ticket and run followers exit when the subject
  settles, and `--follow --quiet` returns only the outcome for scripting.
  Exit codes are stable: `0` for success or a merged subject, `1` for
  another terminal outcome or a daemon error, `2` for usage errors.
  `sloop show --help` documents the whole resolution ladder. Pattern
  resolution happens in the daemon: the `show` verb accepts patterns, the
  `events` verb gained an optional `scope`, and the `list` verb gained an
  optional `limit`, all additive within protocol version 1.
- `sloop template <kind>` prints fully commented canonical templates for
  the config, flow, project, and ticket files, so a working example is
  always one command away.
- `sloop show <TICKET>` now lists the ticket's runs, newest attempt first:
  alias, outcome, wall-clock span, and a strip of the run's flow stages
  marked `ok`, `FAIL`, `..` (running), or `-` (not reached). A ticket that
  has never run prints `runs: none`.
- `sloop show <RUN>` now shows the run's timeline, a per-stage table (state,
  attempts including `on_fail` retries, duration, exit code, and verdict
  source), and a derived `reason` for any non-merged terminal run — for
  example ``stage `test` failed (exit 1) after agent completed with
  commits``. The reason comes from the stored stage and evidence rows, never
  from an agent's own claim about its work. Stage names come from the run's
  admitted flow snapshot, so a run reports the stages it actually had even
  after the flow file changes.
- The `show` response gained `value.runs` on tickets and `value.stages`,
  `value.attempt`, `value.agent_exit_code`, and the timeline fields on runs,
  all additive within protocol version 1.
- `sloop logs` gained `--stage <NAME>`, `--tail <N>`, and `--follow`.
  `--stage` selects one flow stage by the name the flow gives it, including
  the agent stage, and rejects a name the run's flow does not define instead
  of returning an empty page. `--tail` keeps the last N entries; `--follow`
  streams new entries until the run settles. The three combine, so
  `sloop logs <RUN> --stage test --tail 50` answers "why did the test stage
  fail" in one command. Filtering happens in the daemon: the `logs` verb
  gained `stage`, `tail`, and `after` arguments and a `terminal` response
  field, all additive within protocol version 1.
### Changed

- The default flow's review stage is now a real gate. It previously
  inherited `verdict: exit` and passed whenever the reviewer process exited
  zero, so every fresh `sloop init` deployment silently approved all work.
  The shipped stage now uses `verdict: reported`, and the review prompt
  requires the reviewer to call `sloop verdict pass|fail --reason` exactly
  once; the command, not the prose, decides the stage.
- Compact `sloop --help` now lists only the everyday operator commands
  (`init`, `template`, `daemon`, `post`, `show`, `logs`). Everything else,
  including the worker verb `brief`, remains available and documented under
  `sloop --help --all`.
- `sloop show <RUN>` labels the agent stage's exit as `agent exit:` rather
  than a bare `exit:`, which could read as "the whole run passed" on a run
  whose later stage failed. The JSON `exit_code` field is unchanged.
- Ticket lists — the dashboard, pattern results, and the deprecated `list`
  alias — order tickets by registration time, newest first, instead of
  oldest first, so recently posted and currently running work leads the
  output. State does not affect the order. Tickets registered in the same
  millisecond fall back to their id's numeric ordinal, newest first.
- `sloop post` now reports every problem with a ticket file in a single
  `invalid_arguments` error, one per line under the file path, instead of
  stopping at the first one. A file whose frontmatter cannot be parsed at
  all still fails fast with the parse error, and a file with exactly one
  problem reads as it always has.

### Deprecated

- `status`, `list`, `watch`, and `wait` remain accepted as hidden
  deprecated aliases of `sloop show`. They no longer appear in normal help,
  and each invocation writes a note to stderr naming its replacement:
  `status` and `list` point to `sloop show`, `watch` to `sloop show
  --follow` (its optional scope and tail still work), and `wait` to
  `sloop show --follow --quiet` (its run and timeout still work). The
  aliases will be removed in a future release.

### Fixed

- `sloop reindex` now recognizes patch-equivalent merges. A run branch
  whose commits were squashed or rebased onto the default branch is
  detected with `git cherry` and indexed as merged; previously only true
  ancestor merges counted, and reindex flipped squash-merged tickets to
  `needs_review`.

## [0.2.1] - 2026-07-20

### Fixed

- Keep worker socket paths within the 104-byte macOS socket path limit by
  placing them directly in the runtime directory under the short run id.
  On macOS the previous layout exceeded the limit, every agent spawn failed
  at socket bind, and no run could start.

## [0.2.0] - 2026-07-20

### Fixed

- Draw run-id entropy from `getrandom` on Linux, which musl libc exports,
  instead of `getentropy`, which it does not, so musl builds compile.
- Wait out a stopping predecessor's lock for up to two seconds when the
  daemon starts, so a stop followed by an immediate start cannot lose the
  race against the old process releasing its lock.

### Added

- Initial command-line surface and local daemon implementation.
- Release automation for Homebrew formulae, crates.io publication, and signed
  GitHub artifact provenance.
- Recurring and overnight activations, with `post --at` activations dispatched
  at their scheduled time.
- Blocked ticket dependencies enforced during selection.
- Optional `on_fail` repair agents for exec and merge stages, including merge
  conflict repair before retry.
- Activity events feed and a `sloop watch` command.
- Operator `show` resolves tickets with bodies and runs.
- Draining daemon restart.
- Settled worktree retention and default worktree branches derived from ticket
  file stems.
- Per-target default model and effort, defaulting to `claude`.
- Vendor error classification from agent output.

### Changed

- Store daemon state, logs, sockets, and locks in platform-native per-user
  locations instead of the repository's `.sloop` directory.
- Run start and exit moved behind coordination verbs, with typed run states
  and lease renewal.
- Runs get random internal ids with ticket-derived aliases.
- Commit counts removed from run verdicts; outcomes derive from evidence only.
- Externally merged run branches reconcile out of `needs_review`, and stale
  runs reconcile periodically.
