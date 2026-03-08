#!/usr/bin/env python3
"""Append an experiment entry to docs_learning/experiments.md.
Usage: python3 scripts/append_experiment.py --id E-2026-001 --hypothesis "If X then Y" --change "Did Z" --result "Saw W" --decision "Keep" --lesson "L-2026-001"
"""
import argparse, os
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TARGET = os.path.join(ROOT, 'docs_learning', 'experiments.md')

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--id', required=True)
    p.add_argument('--date', default=str(date.today()))
    p.add_argument('--hypothesis', required=True)
    p.add_argument('--change', required=True)
    p.add_argument('--result', required=True)
    p.add_argument('--decision', required=True)
    p.add_argument('--lesson', default='')
    a = p.parse_args()
    os.makedirs(os.path.dirname(TARGET), exist_ok=True)
    entry = f"""
---

### {a.id}

- **Date:** {a.date}
- **Hypothesis:** {a.hypothesis}
- **Change Made:** {a.change}
- **Result:** {a.result}
- **Decision:** {a.decision}
- **Lesson:** {a.lesson}
"""
    with open(TARGET, 'a') as f:
        f.write(entry)
    print(f'Appended {a.id} to {TARGET}')

if __name__ == '__main__':
    main()
