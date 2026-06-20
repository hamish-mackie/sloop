# Sloop Documentation

Sloop is a job scheduler for coding agents. It runs each ticket in its own
Git worktree, judges the result from commits and tests, and merges work
that passes.

Start here:

1. [Getting started](getting-started.md) — install, initialize a repository,
   post your first ticket, and watch it run.
2. [Configuration](configuration.md) — the config file, ticket frontmatter,
   flows, and projects.
3. [CLI reference](cli.md) — every command, its flags, and what it returns.
4. [Concepts](concepts.md) — how Sloop decides what runs and how outcomes are
   determined.
5. [Protocol](protocol.md) — the JSON socket API, for building tools on top
   of Sloop.

If you are new, read the first two pages; they cover everything a day-to-day
user needs. The rest is reference material and internals.
