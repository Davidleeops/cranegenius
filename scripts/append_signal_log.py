#!/usr/bin/env python3
"""Append a signal entry to docs_learning/signal_intelligence_log.md.
Usage: python3 scripts/append_signal_log.py --id SIG-2026-001 --type Permit --industry Construction --source "Chicago Socrata" --observation "X" --why "Y" --followup "Z"
"""
import argparse, os
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TARGET = os.path.join(ROOT, 'docs_learning', 'signal_intelligence_log.md')

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--id', required=True)
    p.add_argument('--date', default=str(date.today()))
    p.add_argument('--type', required=True)
    p.add_argument('--industry', required=True)
    p.add_argument('--source', required=True)
    p.add_argument('--observation', required=True)
    p.add_argument('--why', required=True)
    p.add_argument('--followup', default='')
    a = p.parse_args()
    os.makedirs(os.path.dirname(TARGET), exist_ok=True)
    entry = f"""
---

### {a.id}

- **Date:** {a.date}
- **Signal Type:** {a.type}
- **Industry:** {a.industry}
- **Source:** {a.source}
- **Observation:** {a.observation}
- **Why It Matters:** {a.why}
- **Follow-Up:** {a.followup}
"""
    with open(TARGET, 'a') as f:
        f.write(entry)
    print(f'Appended {a.id} to {TARGET}')

if __name__ == '__main__':
    main()
