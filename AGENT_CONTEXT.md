# Agent System Context

## System Purpose
Provide a coordinated autonomy foundation for CraneGenius so controller (Claude) can orchestrate work and Codex can implement safely within constrained lanes.

## Current Phase
Autonomy foundation build-out — establishing specs, scaffolding, and control services before enabling production writes.

## Autonomy Levels
- Level 0 Observe: gather repo intelligence without modifying tracked files.
- Level 1 Suggest: draft plans or diffs for Claude review; no direct writes to shared state.
- Level 2 Controlled Write: implement within approved paths and submit for validation.

## Components
- controller
- event_router
- policy_engine
- codex_runner
- validation_runner
- state_store

## Agents
- repo_monitor
- ci_repair
- scraper_repair
- architecture_sync
- signal_expansion

## Default Mode
Dry run — Codex prepares changes and validation evidence without merging until Claude authorizes.

## Safety Rules
- No writes to main.
- No auto merge.
- No deployment automation.
