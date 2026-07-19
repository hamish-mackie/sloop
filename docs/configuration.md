# Configuration

All committed configuration lives under `.agents/sloop/` in the repository
Sloop schedules. Commands find it by walking up from the current directory
to the nearest `.agents/sloop/config.yaml`; the repository containing that
file is the unit of configuration and scheduling.

## config.yaml

`sloop init` generates a working file. A fuller example:

```yaml
version: 1

worktree_dir: .worktrees

scheduler:
  max_parallel_tasks: 2
  running_hours:
    start: "22:00"
    end: "06:00"

aftercare:
  test_cmd: ["cargo", "test"]

agent:
  default_target: claude
  targets:
    claude:
      cmd: ["claude", "--print", "--model", "{model}", "--effort", "{effort}", "{prompt}"]
    opencode:
      cmd: ["opencode", "run", "--model", "{model}", "--variant", "{effort}", "{prompt}"]
    codex:
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
- `running_hours` — a local-time window in which agents may be spawned. It
  may cross midnight (`22:00`–`06:00` means overnight). Work queued outside
  the window waits for the next opening; agents already running when the
  window closes are allowed to finish. Omit the key to run at any time.

### aftercare

- `test_cmd` — an argv run inside the worktree after the agent exits and
  before its work can merge. A failing command keeps the work out of your
  branch and leaves the ticket for review. Omit it to merge after a successful
  agent exit without another qualification step.

### agent

Each named target is a command template. `{prompt}` must appear exactly
once and is replaced with the worker instructions at launch; `{model}` and
`{effort}` are filled from the ticket. A ticket that selects a target whose
template uses `{model}` or `{effort}` must supply those values (or the post
is rejected — before anything is registered).

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
- `project_dir` (default `.agents/sloop/projects`) — project files.
- `ticket_dir` (default `.agents/sloop/tickets`) — ticket files.

All three must stay inside the repository; absolute or escaping paths are
rejected before the daemon starts. They are committed repository policy and
are never inherited from user configuration.

## User defaults

Optional defaults live at `~/.config/sloop/config.yaml`. Repository values
override them. Only scheduler and aftercare settings may be defaulted this
way; agent targets, ID prefixes, and directory locations are always
repository-scoped.

## Ticket frontmatter

```markdown
---
name: Add request logging      # required, non-empty
blocked_by: []                 # required, a YAML list of ticket IDs
id: TICK-7                     # optional, allocated if omitted
project: default               # optional, defaults to `default`
target: claude                 # optional, defaults to agent.default_target
model: sonnet                  # optional, filled into {model}
effort: medium                 # optional, filled into {effort}
worktree: sloop/TICK-7         # optional branch name, stamped if omitted
flow: default                  # optional, defaults to the default flow
---

The body is the assignment the agent receives. It must be non-empty.
```

`name`, `blocked_by`, and the body are deliberate human judgments, so Sloop
refuses to guess them. `blocked_by: []` is the explicit statement that a
ticket has no dependencies. Every listed blocker must already be
registered, and the resulting dependency graph must stay acyclic — a
rejected post registers nothing.

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
free-form description. Project files never list their tickets — membership
lives in ticket frontmatter.

`sloop run --project <id>` restricts selection to that project's ready
tickets. It never bypasses gates or jumps the queue.

## Flows

`sloop init` scaffolds `.agents/sloop/flows/default.yaml`:

```yaml
stages:
  - name: build
    kind: agent
  - name: review
    kind: exec
    cmd: [opencode, run, "Read .agents/sloop/prompts/review.md and follow its instructions."]
  - name: merge
    kind: merge
```

The filename is the flow name. Tickets bind to a flow at post time with
`flow: <name>` in frontmatter or `sloop post --flow <name>`; the binding is
validated against the flow files that exist.

The first stage must be the flow's only `agent` stage. Sloop then executes
`exec` commands in the run worktree; an optional final `merge` stage applies
the branch using Sloop's merge policy. Every non-merge stage has one verdict
policy:

- `verdict: exit` passes when the stage process exits 0.
- `verdict: commits` passes when the process exits 0 and Sloop observes at
  least one new run-branch commit.
- `verdict: { check: ["argv", "..."] }` requires the stage process to exit 0,
  then runs the check command in the worktree and uses its exit code.
- `verdict: reported` requires the process to call
  `sloop verdict pass|fail [--reason <text>]`; no report is a failure, and the
  first report is final.

The default is `commits` for `agent` and `exit` for `exec`. Merge stages cannot
declare a verdict because the merge result is their verdict. `kind: build`
remains accepted as a deprecated alias for `kind: agent`.

A configured `aftercare.test_cmd` is inserted as an implicit `exit` stage named
`test` immediately after the `agent`, before the flow's own `exec` stages.

## Worker instructions

Sloop composes the agent's prompt itself: a fixed bootstrap tells the
worker to read `sloop brief`, stay in its worktree, and commit before
finishing. To add repository-specific guidance, create
`.agents/sloop/instructions.md`; its contents are appended after the
built-in bootstrap at every launch. There is no prompt configuration key,
and the bootstrap cannot be replaced.
