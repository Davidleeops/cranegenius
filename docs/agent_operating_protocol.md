# Multi-Agent Operating Protocol

## Purpose
Define how controller (Claude) and implementation (Codex) agents coordinate to deliver repository work without stepping on each other or corrupting state.

## Core Model
- Claude owns planning, prioritization, and approval of every task unit.
- Codex executes implementation, writes code, and reports back with verifiable evidence.
- All decisions that affect roadmap, scope, or shared state must originate from Claude; Codex only operates within approved instructions.

## Lane Ownership
- Claude: sequencing, task acceptance/rejection, boundary updates, shared state edits, stakeholder communication.
- Codex: implementation inside the allowed write paths listed in `TASK.md` plus task-specific directories.
- Codex must never edit shared state files (e.g., `TASK.md`, `AGENT_CONTEXT.md`, `CHANGELOG_AGENT.md`, `docs/agent_operating_protocol.md`) unless Claude explicitly grants temporary write access inside the task brief.

## Single Source of Truth
- Repository coordination files (`docs/agent_operating_protocol.md`, `TASK.md`, `AGENT_CONTEXT.md`, `CHANGELOG_AGENT.md`) are the canonical references.
- Claude updates shared state before handoff; Codex treats these files as read-only unless a task explicitly says otherwise.
- Any ambiguity defers to Claude; Codex documents blockers instead of overwriting state.

## Task Unit Rules
1. Claude breaks initiatives into numbered task units listed in `TASK.md`.
2. Each unit states objective, lane owner, and allowed write paths; Codex may not work outside those scopes.
3. Codex documents outcome + validation evidence in `CHANGELOG_AGENT.md` before closing a task.
4. New work cannot start until Claude confirms the previous unit is accepted or explicitly parallelizable.

## Pre-Work Read Protocol
- Claude sends updated `TASK.md` + relevant specs; Codex must reread them before execution and note timestamp.
- Codex pulls latest `main` and runs `git status` before touching files.
- Any referenced design doc or spec must be scanned before coding; uncertainties are logged as clarifying questions to Claude.

## Execution Modes
- **Recon:** gather facts, no writes outside temporary notes.
- **Implementation:** allowed writes limited to directories Claude lists.
- **Dry Run:** (default) produce diffs/tests without pushing until Claude signs off.
- Claude specifies the current mode at handoff; Codex must confirm before coding.

## Branch Discipline
- Codex creates feature branches `codex/<task-slug>` for any change set exceeding a single-file hotfix.
- Rebase or merge only with Claude approval; force pushes prohibited.
- PR descriptions must cite task unit ID, validation steps, and blockers.

## Handoff Template
Claude expects Codex to return:
1. Task unit ID + short outcome statement.
2. Summary of files changed and why.
3. Validation evidence (tests, screenshots, logs).
4. Open issues or follow-ups needed from Claude.
5. Next task recommendation or confirmation awaiting instructions.

## Conflict Prevention Rules
- Claude freezes files before Codex edits them; Codex double-checks `git status` for unexpected changes and halts on conflicts.
- Async updates go through `CHANGELOG_AGENT.md` with timestamps.
- Shared configs are never touched by both agents simultaneously; Claude resolves merges.

## Stop Conditions
- Missing requirements or unclear scope.
- Forbidden path access required.
- Validation fails and fix exceeds current unit scope.
- Detection of unexpected remote changes on branch.
- Claude issues a hold or revokes authorization.

## Quality Gate
- Work must include lint/tests or manual validation relevant to the files touched.
- No TODOs, commented-out hacks, or placeholder data remain in committed code.
- Each task closes with an updated `CHANGELOG_AGENT.md` entry and confirmation that constraints (write paths, safety rules) were honored.
