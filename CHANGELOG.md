# Changelog

All notable changes to Sloop will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-07-20

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
