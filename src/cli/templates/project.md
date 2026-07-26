---
# A project groups tickets for scheduling scope and nothing more. Save
# this file as .agents/sloop/projects/<id>.md. Every ticket belongs to
# exactly one project; tickets posted without `--project` or a `project`
# frontmatter field land in `default`.
#
# The project's stable identifier, referenced by tickets (`project: web`)
# and by `sloop run --project web` and `sloop show web`. Optional: startup
# stamps one as `<ids.project_prefix>-<n>` (e.g. PROJ-2) if it is absent,
# and never overwrites one that is present. Hand-authoring a short,
# readable id is usually worth it.
id: web
# A human-readable display name shown by `sloop show`. Optional.
title: Web frontend
---

The body is a free-form description: what this project covers and what it
does not, so a ticket lands in the right one.

Project files never list their tickets — membership lives in ticket
frontmatter, so adding a ticket does not mean editing two files.
