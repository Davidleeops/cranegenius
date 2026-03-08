# Operational Memory System — Architecture

This document describes the learning and memory system only.
It does not document src/ pipeline internals.

---

## Purpose

Track what works, what fails, and what we learn across pipeline runs.
Enable any agent or operator to pick up context without re-reading raw data.

---

## Files

| File | What It Tracks |
|---|---|
| docs_learning/lessons_learned.md | Permanent operational rules from real outcomes |
| docs_learning/experiments.md | Hypothesis-driven changes and results |
| docs_learning/signal_intelligence_log.md | Upstream demand signals and their value |
| docs_learning/content_log.md | Content assets and case study material |
| runs/system_metrics_history.csv | Per-run pipeline metrics over time |
| runs/contact_generation_stats.json | Latest run stats written by pipeline |

---

## Scripts

| Script | What It Does |
|---|---|
| scripts/append_lesson.py | Appends a lesson entry to lessons_learned.md |
| scripts/append_experiment.py | Appends an experiment entry to experiments.md |
| scripts/append_signal_log.py | Appends a signal entry to signal_intelligence_log.md |
| scripts/append_metrics_history.py | Appends a metrics row to system_metrics_history.csv |

All scripts are append-safe. They create target files if missing.
None depend on src/.

---

## Update Cadence

- After every pipeline run: append a row to system_metrics_history.csv
- After every meaningful change: append a lesson or experiment entry
- After discovering a new data signal: append to signal_intelligence_log.md

