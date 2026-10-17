---
name: audit
description: Security review of this repository by four agents (thief, vandal, mayor, chief). Run only when the user invokes /audit.
disable-model-invocation: true
---

# /audit — security review by four agents

You are the dispatcher. Subagents can't start other subagents, so you run the
pipeline. The reports may name weak spots and secrets: they live in `.audit/` only
(gitignored, blocked by a pre-commit hook and a CI check) and are never committed,
pasted into a PR, issue, commit message or any external service.

```
step 1 (parallel)  thief (haiku)   vandal (haiku)   → thief.md, vandal.md
step 2             mayor (opus)                     → mayor.md   (+ one GAP follow-up)
step 3             chief (sonnet)                   → chief.md
```

## Step 0 — guards
1. `uv run python .claude/skills/audit/audit_tools.py check` must print `OK`.
   Otherwise stop and tell the user.
2. Folder: `D=.audit/$(date -u +%F)`; if it exists, add `-2`, `-3`, ...
3. `uv run python .claude/skills/audit/audit_tools.py prepare $D` writes `$D/_context.md`
   (tracked files, local-only files, secret-looking lines in git history; names only).

## Step 1 — scouts (isolated)
Start `thief` and `vandal` **in the same message** (they run in parallel), foreground.
Each prompt contains only: the repo root, the path `$D/_context.md`, and "follow your
instructions; report as your final message". **Never mention the other scout, the
mayor, the chief, or `.audit/` reports.** The scouts have no Write tool, and their
reports don't exist on disk until both have finished.

Write each final message verbatim to `$D/thief.md` / `$D/vandal.md`, then run
`uv run python .claude/skills/audit/audit_tools.py redact $D`.

## Step 2 — mayor
Start `mayor` with the paths of `_context.md`, `thief.md`, `vandal.md`. Write its final
message to `$D/mayor.md`, run `redact`.

If the report has `GAP thief: ...` / `GAP vandal: ...` lines (one round only, at most 3):
- start that scout again with `_context.md` and the gap text as its focus (still no
  mention of the others); append its answer to its file under `## Follow-up: <gap>`;
- start `mayor` again with the follow-up sections only, asking for verdicts on them in
  the same format; append to `mayor.md` under `## Follow-up verdicts`; run `redact`.

## Step 3 — chief
Start `chief` with the paths of all four files. Write its final message to
`$D/chief.md`, run `redact`.

## Step 4 — finish
1. `audit_tools.py check` again; `git status --short` must show nothing under `.audit/`.
2. Tell the user, in chat: the folder `$D`, counts by verdict and severity, the
   "Do now" items, the fix list (titles + branches). No secret values, not even masked
   ones beyond what the reports already show.
3. Do not start fixing. Each fix is its own task on its own branch, after the user
   picks it.
