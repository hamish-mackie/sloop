<p align="center">
  <img src="docs/assets/banner-light.svg#gh-light-mode-only" alt="sloop — a job scheduler for background coding agents" width="100%">
  <img src="docs/assets/banner-dark.svg#gh-dark-mode-only" alt="" width="100%">
</p>

Sloop runs agentic coding work autonomously: each ticket gets its own Git
worktree and agent (Claude Code, Codex, or OpenCode), and Sloop reviews the
result and merges it when it passes.

The model is small: **flows are behaviour, tickets are work, projects are
scope.** A flow says how work proceeds, a ticket says what the work is, and a
project says which work belongs together.

- **Agent orchestration** — each ticket picks its agent, model, and effort:
  design with a powerful model, send implementation and review to specialized
  workers.
- **Agentic loops** — combine predefined flows, parallel multi-agent
  execution, and scheduled runs.
- **Runs while you sleep** — background agents work unattended within your
  running hours; wake up to finished work.
- **Repeatable** — tickets are Markdown files in your repo, easy to share and
  re-run.

Under the hood it's a small Rust daemon — an agent harness that supervises
each run, enforces shared rate limits, and judges outcomes from process exit
and tests rather than trusting the agent's word.

Full documentation lives in [docs/](docs/).

**Install**

```sh
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/hamish-mackie/sloop/releases/latest/download/sloop-installer.sh | sh
```

Prebuilt binaries are also on the [releases page](https://github.com/hamish-mackie/sloop/releases).

**Initialize**

```sh
sloop init    # Configure this repository
sloop daemon  # Start the scheduler
```

**Write a ticket** (`my-ticket.md`)

```markdown
---
name: Add request logging
blocked_by: []
target: claude
model: sonnet
effort: medium
---

Log each HTTP request with its method, path, status, and duration.
```

**Post it**

```sh
sloop post my-ticket.md
sloop show                # Ticket status, and why pending ones aren't running
sloop logs <run-id>       # Follow a run's output
```

---

## Configuration

`sloop init` creates `.agents/sloop/config.yaml` with working targets for
Claude Code, Codex, and OpenCode. To change when work runs or which agent Sloop
uses, edit the generated config:

```yaml
version: 1

scheduler:
  max_parallel_tasks: 2
  running_hours:
    start: "22:00"
    end: "06:00"

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
```

Running hours use local time and may cross midnight; omit them to run at any
time. Custom agent commands must include `{prompt}` exactly once; `{model}`
and `{effort}` come from the ticket, falling back to the target's `model:`
and `effort:` defaults. Keep secrets in environment variables.

## Flows

Flows define the steps a ticket must pass before its work is merged. `sloop init`
creates `.agents/sloop/flows/default.yaml`:

```yaml
stages:
  - name: build
    kind: agent
  - name: review
    kind: exec
    verdict: reported
    cmd:
      - claude
      - --print
      - --allowedTools
      - Bash
      - --
      - "Read .agents/sloop/prompts/review.md and follow its instructions."
  - name: merge
    kind: merge
```

The review stage is `verdict: reported`, so the reviewer decides the stage by
calling `sloop verdict pass|fail --reason <text>` — its exit code is not the
verdict. `--allowedTools Bash` is what lets the reviewer run tests and make
that call at all.

The filename is the flow name. Select one with `flow: <name>` in the ticket or
with `sloop post my-ticket.md --flow <name>`.

The first stage is the supervised coding `agent`; `exec` stages run their argv
in the run worktree, and `merge` applies the branch. Every non-merge stage has
a verdict policy:

- `commits` requires exit 0 and at least one observed commit.
- `exit` requires exit 0.
- `{ check: [cargo, test] }` requires exit 0, then uses the check command's
  exit code.
- `reported` requires the process to call
  `sloop verdict pass|fail [--reason <text>]` exactly once.

```yaml
verdict: { check: [cargo, test] }
```

`agent` defaults to `commits`; `exec` defaults to `exit`. A failed verdict
stops the flow before merge. To add a test gate to every flow, set:

```yaml
aftercare:
  test_cmd: ["cargo", "test"]
```

This compatibility command runs as an implicit `test` stage immediately after
the `agent` stage, before the flow's own `exec` stages.

## Logs

Each run's full output is kept as `runs/<run-id>/output.ndjson` under Sloop's
state directory (on Linux, `~/.local/state/sloop/repositories/<repository>/`).
`sloop daemon` prints the exact log and socket paths on startup, and
`sloop logs <run-id>` is the normal way to read them.

## Tickets and projects

Tickets live under `.agents/sloop/tickets/` and projects under
`.agents/sloop/projects/` (both configurable). Every ticket belongs to one
project; `sloop init` creates a default for unassigned tickets. Projects group
and scope tickets, nothing more.

`blocked_by` lists the ticket IDs that must finish first; `[]` means none.
Posting rejects a missing `name`, an empty body, unknown blockers, and
dependency cycles, and assigns an ID and worktree branch (`sloop/<id>`) unless
you set your own. Reposting an edited file updates the ticket in place.

`sloop show <project>` lists recent notes and commits per ticket; add `--json`
for structured data.

## Design

Human-authored content lives in committed files. Runtime state lives in a
local SQLite database that only the daemon writes (bundled, nothing to
install). `sloop reindex` rebuilds whatever can be derived from committed
files and Git; runtime history such as notes may not survive it.

## License

Copyright 2026 Hamish Mackie. Licensed under the
[Apache License, Version 2.0](LICENSE).
