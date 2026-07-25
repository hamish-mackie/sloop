# Getting Started

This guide takes you from nothing to a merged ticket.

## Install

```sh
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/hamish-mackie/sloop/releases/latest/download/sloop-installer.sh | sh
```

Prebuilt binaries are also on the
[releases page](https://github.com/hamish-mackie/sloop/releases).

You also need at least one coding agent CLI installed and authenticated —
Claude Code, Codex, or OpenCode work out of the box. The scaffolded
defaults dispatch to Claude Code; a one-line config change selects another.

## Initialize a repository

Run inside the Git repository you want agents to work on:

```sh
sloop init
```

This scaffolds committed configuration under `.agents/sloop/`:

- `config.yaml` — scheduler settings and agent commands
- `projects/default.md` — the default project for unassigned tickets
- `tickets/` — where your ticket files live
- `flows/default.yaml` — the default flow (build → review → merge)
- `flows/train.yaml` — the opt-in merge train (build → sync → verify →
  fast-forward merge); bind a ticket to it with `flow: train`. See
  [the merge train](configuration.md#the-merge-train).
- `prompts/review.md` — the prompt used by the default review stage

`init` never edits `.gitignore`; whether worktrees and tickets are committed
is your repository's policy.

`init` gives you a working scaffold, not a reference. When you need to write
one of these files by hand, `sloop template ticket|flow|project|config`
prints a fully commented canonical version to stdout — the authoring
documentation lives in the binary, so it is available offline and always
matches the version you have installed.

Then start the daemon:

```sh
sloop daemon
```

This is idempotent — it connects to a running daemon or starts one, and
prints the daemon's log and socket paths. Every other operator command
ensures the daemon is running, so you rarely need to run it by hand.

After installing a new binary, ask the current daemon to drain and replace
itself with that binary:

```sh
# ...install the new binary over the old one, however you installed it...
sloop daemon restart
```

The command returns immediately. Active runs finish their stages, queued
work waits without changing state, and dispatch resumes under the replacement.

## Write a ticket

A ticket is a Markdown file: YAML frontmatter for the metadata Sloop needs,
then a body that becomes the agent's assignment. To start from a template
that documents every field, redirect one out of the binary:

```sh
sloop template ticket > .agents/sloop/tickets/add-request-logging.md
```

The minimum is short:

```markdown
---
name: Add request logging
blocked_by: []
---

Log each HTTP request with its method, path, status, and duration.
```

Three fields are required, and Sloop rejects a post that omits them rather
than guessing:

- `name` — a non-empty human-readable name.
- `blocked_by` — a YAML list of ticket IDs that must finish first. `[]`
  explicitly means "no dependencies"; omitting the field is an error.
- A non-empty body after the frontmatter.

Everything else is optional. `target`, `model`, and `effort` select the
agent; omitted values fall back to the config's `default_target` and that
target's configured `model:` and `effort:`. The scaffolded config runs
`claude` with `opus` at `high` effort, so the ticket above works as-is;
the `codex` target defaults are filled in too, while `opencode` has
provider-qualified model names you set yourself. Sloop
stamps an `id` and a worktree branch derived from the filename
(`add-request-logging.md` → `sloop/add-request-logging`) for you unless you
set your own. That default requires the file stem to be a lowercase
`abc-def` slug; a ticket named otherwise must set `worktree:` explicitly.

## Post it

```sh
sloop post .agents/sloop/tickets/add-request-logging.md
```

Ticket files must live below the configured ticket directory
(`.agents/sloop/tickets/` by default). Posting validates the ticket, writes
the allocated ID back into the file, and queues one run. The daemon picks it
up at the next opportunity, creates an isolated worktree on the ticket's
branch, and spawns the agent there.

Editing the file and posting it again updates the ticket in place — same ID,
refreshed name, blockers, and worktree — without queuing a duplicate run.

That queued run is a **trigger**, and it is what actually makes the ticket run.
A ticket registered with `--manual` is `ready` but has no trigger, so it waits;
so does a ticket whose run failed and which `sloop retry` returned to `ready`.
`sloop run <ticket>` queues one whenever you want it. See
[Concepts](concepts.md#tickets-runs-and-triggers).

## Watch it run

```sh
sloop show                               # dashboard: running work, ticket counts, next wake
sloop logs <run-id>                      # a run's captured output
sloop show <run-id> --follow --quiet     # block until the run finishes; exit 0 only on merge
```

The ticket's flow is a list of stages, and Sloop walks them in order. Each stage
runs its `action`, judges it with an independent `result_check`, and records the
verdict; a failure halts the walk unless the stage says otherwise. The
scaffolded `default` flow is:

```yaml
stages:
  - name: build
    action: agent
    result_check: { builtin: commits }
  - name: review
    action:
      exec:
        - claude
        - --print
        - --allowedTools
        - Bash
        - --
        - "Read .agents/sloop/prompts/review.md and follow its instructions."
    result_check: reported
  - name: merge
    action: { builtin: merge }
    result_check: none
```

So the agent's own exit code is never the last word. `build` passes only when
Sloop observes a new commit on the run branch, which is why an unchanged branch
is kept for review rather than silently merged. `review` must call
`sloop verdict pass|fail --reason <text>` — a reviewer that merely exits 0 has
approved nothing. Only then does `merge` apply the branch. A configured
`flow.test_cmd` is spliced in as an extra stage immediately after the first one.

## Everyday controls

```sh
sloop run [ticket]      # queue a run: this ticket, or whatever is ready
sloop hold <ticket>     # keep a ready ticket from being dispatched
sloop ready <ticket>    # release it again
sloop retry <ticket>    # return a failed ticket to ready, reset attempts
sloop pause             # stop spawning new agents (in-flight ones finish)
sloop resume            # start spawning again
sloop daemon restart    # drain active runs, then use the installed binary
sloop cancel <run-id>   # kill a run's whole process group, keep its worktree
sloop stop              # shut the daemon down (refuses while runs are active)
```

## Where things live

Human-authored content — tickets, projects, flows, prompts, configuration —
lives in committed files under `.agents/sloop/`, so it travels with the
repository and is reviewable in a PR.

Machine state stays out of your repository. On Linux the state directory is
`~/.local/state/sloop/repositories/<repository>/`; on macOS it is under
`~/Library/Application Support/sloop/`. `sloop daemon` prints the exact
paths. Run worktrees are created under `.worktrees/` in the repository
(configurable).

## Next steps

- [Configuration](configuration.md) — running hours, parallelism, agent
  targets, flows, and projects.
- [CLI reference](cli.md) — the full command surface.
