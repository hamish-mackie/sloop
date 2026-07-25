# Configuration

All committed configuration lives under `.agents/sloop/` in the repository
Sloop schedules. Commands find it by walking up from the current directory
to the nearest `.agents/sloop/config.yaml`; the repository containing that
file is the unit of configuration and scheduling.

Every file described on this page has a commented canonical template built
into the binary, so you do not need this page (or network access) to author
one:

```sh
sloop template config   # the annotated config.yaml below
sloop template ticket   # every frontmatter field
sloop template flow     # the full flow schema
sloop template project  # the project file shape
```

Those templates are round-tripped through Sloop's own parsers in the test
suite, so they always match the grammar the binary you are running enforces.

## config.yaml

`sloop init` generates a working file, and `sloop template config` prints an
annotated one. A fuller example:

```yaml
version: 1

worktree_dir: .worktrees
worktree_retention: 7d

scheduler:
  max_parallel_tasks: 2
  stall_report_after: 10m
  running_hours:
    start: "22:00"
    end: "06:00"

flow:
  test_cmd: ["cargo", "test"]

agent:
  default_target: claude
  targets:
    claude:
      model: opus
      effort: high
      cmd: ["claude", "--print", "--model", "{model}", "--effort", "{effort}", "{prompt}"]
    opencode:
      cmd: ["opencode", "run", "--model", "{model}", "--variant", "{effort}", "{prompt}"]
    codex:
      model: gpt-5.6-sol
      effort: high
      cmd: ["codex", "exec", "--model", "{model}", "--config", 'model_reasoning_effort="{effort}"', "--sandbox", "workspace-write", "--ephemeral", "{prompt}"]

# Optional: replace Markdown ticket pulls with an external source.
sources:
  tickets:
    exec: ["./scripts/ticket-source.sh"]

ids:
  ticket_prefix: TICK
  project_prefix: PROJ
```

### scheduler

- `max_parallel_tasks` — how many agents may be alive at once. This is a
  hard spawn gate; queued work waits for a free slot.
- `stall_report_after` — how long a running agent stage may produce no output
  before Sloop emits a warning and marks it in `show`. The default is `10m`;
  output resuming starts a new silence episode. The duration must be positive.
- `running_hours` — a local-time window in which agents may be spawned. It
  may cross midnight (`22:00`–`06:00` means overnight). Work queued outside
  the window waits for the next opening; agents already running when the
  window closes are allowed to finish. Omit the key to run at any time.

### flow

- `test_cmd` — an argv run inside the worktree after the flow's first stage
  and before its work can merge. A failing command keeps the work out of your
  branch and leaves the ticket for review. Omit it to merge without another
  qualification step.

### agent

Each named target is a command template. `{prompt}` must appear exactly
once and is replaced with the worker instructions at launch; `{model}` and
`{effort}` are filled from the ticket, falling back to the target's own
`model:` and `effort:` when the ticket omits them. A ticket that selects a
target whose template uses `{model}` or `{effort}` must resolve those values
from one of the two places (or the post is rejected — before anything is
registered). Model names are vendor-specific: `claude` accepts aliases like
`opus`, while `opencode` expects provider-qualified names such as
`anthropic/claude-opus-4-8`.

`default_target` is used by tickets that do not name a `target`. Adding an
agent vendor is a config block, not a code change. Keep API keys and other
secrets in environment variables; the agent inherits the daemon's
environment.

Agent targets are repository policy: they are only read from the
repository's config, never from user-level defaults.

### sources

Tickets normally come from Markdown files under `ticket_dir`. Configuring
`sources.tickets.exec` replaces that source for `sloop reindex`; sources are
not merged. The command runs from the repository root and receives one JSON
request on stdin:

```json
{"verb":"pull"}
```

For a pull, stdout must be a JSON array. Each object accepts `id`, `name`,
`project`, `blocked_by`, `target`, `model`, `effort`, `flow`, and `body`;
unknown fields are rejected. `name` and `body` are required, while omitted
`blocked_by` defaults to an empty list and the other optional fields use the
same defaults as Markdown frontmatter.

After a run settles, Sloop invokes the same command with a best-effort
notification:

```json
{"verb":"report","ticket":"TICK-7","outcome":"merged"}
```

A failed pull leaves the current index untouched. A failed report is logged
as a warning and does not change the settled outcome. Source commands are
repository policy and cannot be configured in user-level defaults.

### ids

Prefixes for generated ticket and project IDs (`TICK-7`, `PROJ-2`). New IDs
are allocated as one greater than the largest existing numeric suffix.
Explicit IDs in frontmatter are always preserved.

### Directories

- `worktree_dir` (default `.worktrees`) — where run worktrees are created.
- `worktree_retention` (default `7d`) — how long a settled run's worktree and
  run branch remain available before periodic cleanup. Durations use `s`, `m`,
  `h`, `d`, or `w`; set it to `never` to disable cleanup. Merged runs are
  eligible immediately. Failed and `needs_review` runs are retained as evidence
  until the ticket is resolved by retry, external merge, or reindex; their
  retention period starts at that resolution.
- `project_dir` (default `.agents/sloop/projects`) — project files.
- `ticket_dir` (default `.agents/sloop/tickets`) — ticket files.

All three must stay inside the repository; absolute or escaping paths are
rejected before the daemon starts. They are committed repository policy and
are never inherited from user configuration.

## User defaults

Optional defaults live at `~/.config/sloop/config.yaml`. Repository values
override them. Only scheduler and flow settings may be defaulted this
way; agent targets, ID prefixes, and directory locations are always
repository-scoped.

## Ticket frontmatter

`sloop template ticket` prints this with a comment on every field:

```markdown
---
name: Add request logging      # required, non-empty
blocked_by: []                 # required, a YAML list of ticket IDs
id: TICK-7                     # optional, allocated if omitted
project: default               # optional, defaults to `default`
target: claude                 # optional, defaults to agent.default_target
model: sonnet                  # optional, filled into {model}
effort: medium                 # optional, filled into {effort}
worktree: sloop/add-request-logging  # optional branch, from the file stem if omitted
flow: default                  # optional, defaults to the default flow
---

The body is the assignment the agent receives. It must be non-empty.
```

`name`, `blocked_by`, and the body are deliberate human judgments, so Sloop
refuses to guess them. `blocked_by: []` is the explicit statement that a
ticket has no dependencies. Every listed blocker must already be
registered, and the resulting dependency graph must stay acyclic — a
rejected post registers nothing.

A post that fails validation reports every problem with the file at once,
one per line under the file path, so a partially filled ticket takes one
edit rather than one edit per field. Frontmatter that cannot be parsed at
all is still reported on its own: nothing after it can be read.

`target`, `model`, and `effort` are snapshotted when the ticket is posted:
later config changes do not retroactively change an already-posted ticket.
Reposting an edited file refreshes `name`, `blocked_by`, and `worktree`
without changing the ID or queuing a duplicate run.

## Projects

A project is a group of tickets used for grouping and scheduling scope,
nothing more. Every ticket belongs to exactly one project; `sloop init`
creates `projects/default.md`, and tickets posted without `--project` (or a
`project` frontmatter field) land there.

A project file is Markdown with `id` and `title` frontmatter and a
free-form description; `sloop template project` prints an annotated one.
Project files never list their tickets — membership lives in ticket
frontmatter.

`sloop run --project <id>` restricts selection to that project's ready
tickets. It never bypasses gates or jumps the queue.

## Flows

`sloop template flow` prints a template exercising every action, every result
check, and `on_fail`, with the structural rules spelled out in comments.
`sloop init` scaffolds a smaller working `.agents/sloop/flows/default.yaml`:

```yaml
stages:
  - name: build
    action: agent
    result_check: { builtin: commits }
  - name: review
    result_check: reported
    action:
      exec:
        - claude
        - --print
        - --allowedTools
        - Bash
        - --
        - "Read .agents/sloop/prompts/review.md and follow its instructions."
  - name: merge
    action: { builtin: merge }
    result_check: none
```

The review stage ships as `result_check: reported`: the reviewer must call
`sloop verdict pass|fail --reason <text>` exactly once, and a stage that ends
without one fails with `no verdict reported`. Under the exec default,
`result_check: none`, a reviewer that always exits 0 would approve every run.

The filename is the flow name. Tickets bind to a flow at post time with
`flow: <name>` in frontmatter or `sloop post --flow <name>`; the binding is
validated against the flow files that exist.

Each stage is an `action` — the work, never trusted to grade itself — and a
`result_check` that judges it. The actions are:

- `action: agent` spawns the ticket's agent target in the run worktree,
  prompted by the ticket body. Any position, any number of times; each gets its
  own supervised process and worker credentials.
- `action: { exec: ["argv", "..."] }` runs an argv (no shell) in the run
  worktree.
- `action: { builtin: sync }` merges the default branch into the run branch,
  inside the run worktree. It passes when that merge commits cleanly or there
  was nothing to integrate, and fails on a conflict — leaving no in-progress
  merge behind, so whatever the flow does next starts from a clean tree. Any
  number, anywhere before the merge stage. The shared default-branch checkout
  is only ever read.
- `action: { builtin: merge }` applies the branch using Sloop's merge policy.
  At most one, and it must be last. It takes one option,
  `{ builtin: merge, ff_only: true }`, which refuses the merge commit: the
  default branch either fast-forwards to the run branch head, or the stage
  fails having touched nothing. `ff_only` is meaningful nowhere else, and
  writing it on another action is a parse error rather than an ignored key.

The checks are:

- `result_check: none` passes when the action exits 0.
- `result_check: { builtin: commits }` passes when the action exits 0 and Sloop
  observes at least one new run-branch commit.
- `result_check: { exec: ["argv", "..."] }` requires the action to exit 0, then
  runs the check command in the worktree and uses its exit code.
- `result_check: reported` requires the action to call
  `sloop verdict pass|fail [--reason <text>]`; no report is a failure, and the
  first report is final.
- `result_check: { panel: {...} }` puts several independent reviewers on the
  stage and derives the verdict from a quorum of their reports. See
  [Panels](#panels-several-reviewers-one-deterministic-verdict) below.

The default is the commits builtin for `agent` and `none` for everything else.
An agent action may not use `result_check: none` — grading itself by exiting
cleanly grades nothing. The `merge` and `sync` actions must use
`result_check: none` because what git did *is* their verdict, and neither may
appear as a check: a judge that moves a branch is not judging anything.

### Panels: several reviewers, one deterministic verdict

A single `reported` reviewer is one uncalibrated opinion, and it is the only
thing standing between the agent's work and the merge. A **panel** spends more
tokens to buy independence: `N` reviewers each examine the run alone and
report, and a pure function over their reports decides the stage.

```yaml
- name: review
  action: { exec: ['true'] }
  result_check:
    panel:
      prompt: prompts/review.md
      reviewers:
        - { target: claude }
        - { target: codex }
        - { target: gemini, model: pro }
      require: { quorum: 2 }
```

- `prompt` is a path under `.agents/sloop/`, so `prompts/review.md` is the file
  `sloop init` already scaffolds. Every seat gets the *same* prompt: reviewers
  asked different questions produce answers a quorum cannot meaningfully count.
- `reviewers` is 2 to 5 entries. Each names a `target` from `agent.targets` in
  config.yaml — validated at load, so a typo'd vendor is a startup error rather
  than a reviewer that silently never runs. `model` and `effort` are optional
  per-seat overrides that default to the *target's* own defaults, not the
  ticket's: the ticket says how the work should be done, and a panel is about
  who judges it.
- `require: { quorum: N }` is how many `Pass` reports the stage needs, from 1
  to the number of seats. Omit it and the panel is unanimous — a rule nobody
  wrote down must not silently be the most permissive one it could have been.

**Seat different vendors.** The point of a panel is decorrelated failure modes.
Three seats on one model share its blind spots and mostly buy you the same
opinion three times; three seats across three vendors do not.

**It costs what it looks like it costs.** A three-seat panel spawns three
review agents per execution of that stage, and a `return_to` edge multiplies
that by its attempt budget. Sloop counts panel seats into the worst-case
execution budget and refuses at parse time any flow whose total could exceed
32, so the bill is bounded — but a five-seat panel inside a looping span is
genuinely five times the review tokens, every time round.

Reviewers run **one at a time**, so a panel never occupies more of the daemon
than a single-agent stage does and cannot exceed `max_parallel_tasks`.

Each reviewer gets one-shot credentials bound to its own seat — the run, the
stage, the attempt, and the reviewer index. Which report a `sloop verdict` call
lands on is derived from that credential and never from its arguments, so a
reviewer cannot report for another seat, another stage, another attempt, or
another run, and a second `verdict` call is refused. Reason is mandatory for a
panel reviewer; `--confidence low|medium|high` is optional and defaults to
`medium`.

The aggregation is deliberately dull, and is `Pass` if and only if at least
`quorum` seats reported `Pass`:

- A reviewer that exits without reporting counts as a `Fail` with the reason
  `no verdict reported`. Silence is not an abstention — a panel that could not
  be heard from has approved nothing.
- Confidence is recorded evidence only. It is never weighted, so two
  high-confidence rejections do not outvote three low-confidence approvals.
  There is no veto rule either: quorum only, so a panel's behaviour is
  predictable from the config alone.
- The aggregate is **never stored**. What persists is one append-only evidence
  row per reviewer, and the verdict is recomputed from those rows every time —
  which is what lets a daemon that restarted mid-stage reach the same reading
  as the one that started it.

`sloop show <run>` lists each seat under the panel stage with its verdict,
confidence, and reason, silent seats included:

```
stages:
  build   passed   ...
  review  failed   ...  verdict from panel
    claude  pass  confidence high  reads correct
    codex   fail  confidence medium  missing a test
    gemini  fail  no verdict reported
```

### `fail_action`: what a failure does to the walk

`fail_action` says what the walk does when a stage's result check reads `Fail`.
The default is `fail`, and most stages want it:

- `fail_action: fail` — halt. Stages after the failed one are never requested,
  and the run settles on whatever the walk had already produced. This is the
  default and needs no key.
- `fail_action: continue` — advisory. The failure is recorded and visible in
  `sloop show`, and the walk carries on to the next stage. An advisory failure
  never changes the run's outcome, so a flow can report on something without
  blocking the merge on it.
- `fail_action: { return_to: <stage>, attempts: N }` — loop back. The walk
  re-enters an earlier stage and re-runs the whole span from there through the
  stage that failed, up to `N` times.

A build→test loop, where a failing test sends the agent back to fix it:

```yaml
- name: test
  action: { exec: [cargo, test] }
  result_check: none
  fail_action: { return_to: build, attempts: 2 }
```

`return_to` must name an *earlier* stage: edges only ever point backwards, so a
flow cannot skip forward past work or loop forever. `attempts` defaults to 1
and may not exceed 3, and Sloop refuses at parse time any flow whose budgets
together could execute more than 32 stages in the worst case.

The whole span re-runs, not just the failing stage: a `Pass` recorded inside a
span the walk went back through is superseded by the re-run, so no stale
verdict can reach the merge. When the budget runs out, the run lands exactly
where the same failure would have landed without the edge — a failing `exec` or
`agent` stage ends it `failed`, a conflicted `merge` parks it `needs_review`.

A re-entered `agent` stage is told why. Sloop appends a delimited
`previous attempt failed` block to its prompt — after the ticket body and the
worker instructions — naming the stage that failed, its reason, and the last
100 lines of its captured output. The block is built from the run's persisted
evidence, so a daemon that restarts mid-loop composes the same prompt. `exec`
actions get nothing: their command line is fixed by the flow, so there is
nowhere for context to go.

`sloop show` renders each execution separately, suffixing re-runs with their
attempt (`build`, `build#2`), so a converged loop and a stage that only ever
ran once do not read the same.

### The merge train

The worked example of a backward edge is the `train` flow, which `sloop init`
writes to `.agents/sloop/flows/train.yaml` beside `default.yaml`. Bind a ticket
to it with `flow: train` or `sloop post --flow train`; the `default` flow is
unchanged and is still what a ticket gets when it names none.

```yaml
stages:
  - name: build
    action: agent
    result_check: { builtin: commits }
    fail_action: fail
  - name: sync
    action: { builtin: sync }
    result_check: none
    fail_action: { return_to: build, attempts: 1 }
  - name: verify
    action: { exec: [cargo, test] }
    result_check: none
    fail_action: { return_to: build, attempts: 1 }
  - name: merge
    action: { builtin: merge, ff_only: true }
    result_check: none
    fail_action: { return_to: sync, attempts: 3 }
```

The problem it solves is that what lands on the default branch is not the run
branch — it is the *merge* of the two, a tree no stage ever tested. While a run
is in flight the default branch keeps moving, so an ordinary flow either parks
a conflict in `needs_review` or merges something semantically stale that every
stage nonetheless passed.

The train closes that gap with nothing but ordinary stages. `sync` integrates
the default branch into the run branch, `verify` runs against the tree that
produces, and `ff_only` makes the merge a fast-forward — which can only succeed
while the default branch is still the commit `sync` integrated. If it moved in
between, the fast-forward is impossible, the merge fails without touching
anything, and `return_to: sync` runs the train around again. Each lap is one
more chance to converge, and `attempts: 3` bounds how many the train gets
before the ticket parks for a human. Note where the two edges point: a failing
`sync` or `verify` goes back to `build`, because something about the work needs
changing, while a failed fast-forward goes back to `sync`, because nothing
about the work was wrong — only what it was sitting on.

Two details are load-bearing rather than stylistic. Verification sits **after**
the sync because a merge is the one irreversible act in a flow: once the
default branch has moved, no later stage can un-move it, so everything that
decides whether the work is good has to have run already. And the merge stage
carries `result_check: none` on principle, not for convenience — a check runs
*after* its action, so any verdict it reached would arrive too late to prevent
anything. The merge's own outcome is the only thing it can honestly be judged
by, which is why the grammar refuses every other check there.

A sync that conflicts fails and aborts its own merge, so the stage it returns
to gets a clean worktree rather than one wedged on `MERGE_HEAD`. Git's conflict
output is captured in the run log like any stage's, so a re-entered agent is
handed the conflicting paths in its prompt and can rework its commits to avoid
them. The builtin never resolves a conflict itself and has no rebase mode; if
you want a conflict resolved rather than avoided, route it to an agent stage
with `return_to`.

If your repository sets `flow.test_cmd`, `sloop init` uses that command for the
train's `verify` stage. Prefer naming it there rather than in `flow.test_cmd`
when you use the train: the implicit `test` stage is spliced in immediately
after the first stage, which is *before* the sync, and so tests the tree the
train exists to stop trusting.

The older grammar still parses, as sugar for exactly the same stages:
`kind: agent` (or the deprecated `kind: build`) is `action: agent`,
`kind: exec` with `cmd:` is `action: { exec: [...] }`, `kind: merge` is
`action: { builtin: merge }`, and `verdict: exit | commits | { check: [...] } |
reported` are the four checks above in order. The two spellings may not be
mixed on one stage.

A configured `flow.test_cmd` is inserted as an implicit `result_check:
none` stage named `test` immediately after the agent action, before the flow's
own `exec` stages.

### Repairing a failed stage with `on_fail` (deprecated)

> **Deprecated.** `fail_action: { return_to: <stage> }` does the same job with
> the flow's own stages, and the daemon logs a `flow_on_fail_deprecated` note
> when it admits a run whose flow uses `on_fail`. Nothing is removed yet and
> existing flows keep working, but new flows should prefer a backward edge.
>
> The equivalent of a repair block is a failing check stage returning to the
> stage it guards. Instead of attaching a repair agent to `test`, let `test`
> send the walk back to `build`:
>
> ```yaml
> - name: build
>   action: agent
> - name: test
>   action: { exec: [cargo, test, --all-targets] }
>   fail_action: { return_to: build, attempts: 2 }
> ```
>
> The differences are worth knowing. A `return_to` re-runs the ticket's own
> agent with the failure in its prompt, rather than a separate repair worker
> with a prompt written into the flow file; and it re-runs every stage in the
> span, so nothing between `build` and `test` keeps a verdict earned before the
> fix.

By default a failing `exec` stage ends the run and a conflicted `merge` stage
parks the ticket in `needs_review`. Both often stem from mechanical problems an
agent could fix in place — a test that broke after the build, or a run branch
that conflicts with the default branch because other work merged first. An
optional `on_fail` block on any non-agent stage attaches a repair agent:

```yaml
stages:
  - name: build
    action: agent
  - name: test
    action: { exec: [cargo, test, --all-targets] }
    on_fail:
      agent: "Tests are failing in this worktree. Fix them without weakening assertions, then commit."
      attempts: 2      # optional, default 1, at most 3
      target: claude   # optional, defaults to the ticket's target
      model: haiku     # optional, defaults to the ticket's model
      effort: low      # optional, defaults to the ticket's effort
  - name: merge
    action: { builtin: merge }
```

When the stage fails, Sloop spawns the repair agent in the run worktree with the
configured prompt, and when it exits — however it exits — re-runs the original
stage and re-applies the stage's own result check. The retried run is the only
evidence: the repair agent never reports a verdict, and `on_fail` cannot change a
stage's check, action, or ordering. `attempts` allows up to that many
repair-then-retry cycles (capped at 3); when they run out the outcome is exactly
today's — an exhausted `exec` stage ends the run `failed`, an exhausted `merge`
stage parks `needs_review` with the branch preserved.

For a `merge` stage, the repair agent's job is to integrate the **default branch
into the run branch** (merge or rebase) and resolve conflicts there; the retried
merge then applies Sloop's normal merge policy. Repair agents only ever work in
the run worktree — no agent process touches the default-branch checkout.

`target`, `model`, and `effort` configure the repair worker only; `target` is
validated against your configured agent targets at post time. Each repair spawn
passes the same running-hours, capacity, cooldown, and budget gates as any other
agent spawn and counts against those budgets and rate limits, so a closed gate
simply skips the repair and lets the stage settle as if `on_fail` were absent.

## Worker instructions

Sloop composes the agent's prompt itself: a fixed bootstrap tells the
worker to read `sloop brief`, stay in its worktree, and commit before
finishing. To add repository-specific guidance, create
`.agents/sloop/instructions.md`; its contents are appended after the
built-in bootstrap at every launch. There is no prompt configuration key,
and the bootstrap cannot be replaced.
