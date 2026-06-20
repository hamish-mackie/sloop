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
Claude Code, Codex, or OpenCode work out of the box.

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
- `prompts/review.md` — the prompt used by the default review stage

`init` never edits `.gitignore`; whether worktrees and tickets are committed
is your repository's policy.

Then start the daemon:

```sh
sloop daemon
```

This is idempotent — it connects to a running daemon or starts one, and
prints the daemon's log and socket paths. Every other operator command
ensures the daemon is running, so you rarely need to run it by hand.

## Write a ticket

A ticket is a Markdown file: YAML frontmatter for the metadata Sloop needs,
then a body that becomes the agent's assignment.

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

Three fields are required, and Sloop rejects a post that omits them rather
than guessing:

- `name` — a non-empty human-readable name.
- `blocked_by` — a YAML list of ticket IDs that must finish first. `[]`
  explicitly means "no dependencies"; omitting the field is an error.
- A non-empty body after the frontmatter.

Everything else is optional. `target`, `model`, and `effort` select the
agent; omitted values fall back to the repository configuration. Sloop
stamps an `id` and a worktree branch (`sloop/<id>`) for you unless you set
your own.

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

## Watch it run

```sh
sloop list            # every ticket's name, state, and scheduling reason
sloop status          # what is running now, queue depth, next wake
sloop logs <run-id>   # a run's captured output
sloop wait <run-id>   # block until the run finishes; exit 0 only on merge
```

When the agent exits, Sloop does not take its word for it. It checks the
evidence: commits in the worktree, the exit code, and your configured test
command, if any. Work that passes is merged into your branch. Work that
fails or produces no commits is not — a run is never "done" just because
the agent said so.

## Everyday controls

```sh
sloop hold <ticket>     # keep a ready ticket from being dispatched
sloop ready <ticket>    # release it again
sloop retry <ticket>    # return a failed ticket to ready, reset attempts
sloop pause             # stop spawning new agents (in-flight ones finish)
sloop resume            # start spawning again
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
