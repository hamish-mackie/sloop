# Changelog

All notable changes to Sloop will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `sloop watch` now takes an optional ticket, run, or project reference and
  streams only that scope's events, resolved exactly as `sloop show`
  resolves a reference. A reference that resolves to nothing fails with
  `not_found` before anything streams. Bare `sloop watch` is unchanged. The
  `events` verb gained a matching optional `scope` argument, so any protocol
  client scopes the feed the same way.

- `sloop list` accepts a row limit: `--limit N`, `-n N`, or the `head`/`tail`
  shorthand `-N` shows only the N newest tickets. A zero or non-numeric limit
  is a usage error. The `list` verb gained a matching optional `limit`
  argument, so protocol clients page the same way.

### Changed

- `sloop list` now orders tickets by registration time, newest first, instead
  of oldest first, so recently posted and currently running work leads the
  output. State does not affect the order. Tickets registered in the same
  millisecond fall back to their id's numeric ordinal, newest first.
- `sloop post` now reports every problem with a ticket file in a single
  `invalid_arguments` error, one per line under the file path, instead of
  stopping at the first one. A file whose frontmatter cannot be parsed at
  all still fails fast with the parse error, and a file with exactly one
  problem reads as it always has.

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
