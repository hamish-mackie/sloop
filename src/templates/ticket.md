---
# A ticket is Markdown: YAML frontmatter Sloop reads, then a body that
# becomes the agent's assignment. Everything Sloop refuses to guess is
# required; everything it can derive is optional.
#
# ---- required ----------------------------------------------------------
# A non-empty human-readable name. `sloop list` and `sloop show` use it,
# and it can be passed anywhere a ticket reference is accepted.
name: Add request logging
# Ticket IDs that must merge before this one may run. A YAML list of
# strings, and the key itself is mandatory: `[]` is the explicit statement
# that this ticket has no dependencies, while omitting the key is an error.
# Every listed blocker must already be posted, and the graph must stay
# acyclic — a rejected post registers nothing.
blocked_by: []
#
# ---- optional ----------------------------------------------------------
# A longer display title. Purely informational; `name` remains the handle.
# title: Add structured request logging
#
# The agent target to dispatch to, naming an entry under `agent.targets`
# in .agents/sloop/config.yaml. Defaults to `agent.default_target`.
# target: claude
#
# The model and reasoning effort substituted into that target's `{model}`
# and `{effort}` placeholders. They default to the target's own `model:`
# and `effort:`; if the target's command uses a placeholder and neither the
# ticket nor the target supplies a value, the post is rejected.
# model: opus
# effort: high
#
# The flow this ticket binds to, naming a file under
# .agents/sloop/flows/ without its extension. Defaults to `default`.
# Run `sloop template flow` for the flow grammar.
# flow: default
#
# ---- stamped by sloop --------------------------------------------------
# `sloop post` writes these back into this file, so leave them out unless
# you deliberately want to pin a value. Sloop never overwrites one that is
# already present, and a wrong hand-authored value is a wrong ticket.
#
# id: allocated as `<ids.ticket_prefix>-<n>`, e.g. TICK-7.
# id: TICK-7
#
# project: the project this ticket belongs to, defaulting to `default`.
# It must name an existing file under .agents/sloop/projects/. A `project`
# here and a conflicting `sloop post --project` is an error, not a merge.
# project: default
#
# worktree: the branch the run works on. Derived from the file stem
# (add-request-logging.md -> sloop/add-request-logging), which requires the
# stem to be a lowercase `abc-def` slug; set it explicitly otherwise.
# worktree: sloop/add-request-logging
---

Everything below the frontmatter is the body, and it must be non-empty: it
is the brief the agent reads with `sloop brief`. Write it the way you would
write a ticket for a colleague who cannot ask you follow-up questions.

## Context

Why this work matters and what the reader needs to know that is not
obvious from the code.

## Required change

What to build, specifically enough that "done" is not a judgment call.

## Acceptance criteria

- The concrete checks the work must satisfy.
- The commands that must pass, for example `cargo test --all-targets`.
