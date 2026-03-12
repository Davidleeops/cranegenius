# MVP Gap Report

## Current State After This Pass

What is now in place:
- a single orchestrator for the strongest working lane
- explicit stage outputs
- standardized stage interfaces
- operator-ready summary output
- machine-readable run summary output
- stage-level failure visibility
- helper tests for orchestrator output validation

## What Is Still Missing Before MVP-Operational Confidence

### 1. Full integration execution proof
The orchestrator exists, but confidence improves materially once it is exercised repeatedly with live data and reviewed for output quality.

### 2. Deeper validation
Current checks are structural:
- process exit code
- required artifact existence
- simple row-count metrics

Still needed:
- semantic row-quality checks
- threshold gates for suspiciously low output volumes
- DB content validation by stage

### 3. Stronger critical-path tests
Current tests cover helper logic for the new orchestrator, but not a fully mocked end-to-end run.

Still needed:
- subprocess orchestration tests
- failure-path tests
- export contract tests

### 4. Scheduling and alerting
The system can be run repeatedly, but it is not yet self-operating.

Still needed:
- cron or workflow scheduling
- alerting on failed runs
- durable runtime metrics history

### 5. Legacy path consolidation
The repo still contains multiple overlapping systems.

Still needed:
- clear deprecation plan for broken `src/pipeline.py`
- decision on whether Monday pipeline becomes a second orchestrated product lane or stays separate
- cleanup of unused and duplicate files after explicit approval

## MVP-Operational Definition

Call this MVP-operational when all are true:
- one command runs the chosen end-to-end flow
- required outputs are always explicit and reviewable
- failures are visible without reading raw code
- operator outputs are actionable
- machine-readable outputs are stable enough for downstream consumers
- repeated runs show consistent artifact generation

## Next Hardening Steps

1. Add threshold-based validation gates.
2. Add end-to-end mocked orchestrator tests.
3. Add scheduled execution.
4. Add alerts for failed runs.
5. Decide which overlapping legacy paths should be retired.
