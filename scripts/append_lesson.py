#!/usr/bin/env python3
"""Append a lesson entry to docs_learning/lessons_learned.md.
Usage: python3 scripts/append_lesson.py --id L-2026-001 --area Verification --observation "X happened" --experiment "Changed Y" --result "Z improved" --conclusion "Because W" --rule "Always do V"
"""
import argparse, os
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TARGET = os.path.join(ROOT, 'docs_learning', 'lessons_learned.md')

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--id', required=True)
    p.add_argument('--date', default=str(date.today()))
    p.add_argument('--area', required=True)
    p.add_argument('--observation', required=True)
    p.add_argument('--experiment', required=True)
    p.add_argument('--result', required=True)
    p.add_argument('--conclusion', required=True)
    p.add_argument('--rule', required=True)
    a = p.parse_args()
    os.makedirs(os.path.dirname(TARGET), exist_ok=True)
    entry = f"""
---

### {a.id}

- **Date:** {a.date}
- **Area:** {a.area}
- **Observation:** {a.observation}
- **Experiment / Change:** {a.experiment}
- **Result:** {a.result}
- **Conclusion:** {a.conclusion}
- **Reusable Rule:** {a.rule}
"""
    with open(TARGET, 'a') as f:
        f.write(entry)
    print(f'Appended {a.id} to {TARGET}')

if __name__ == '__main__':
    main()
