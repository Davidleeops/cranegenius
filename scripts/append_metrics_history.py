#!/usr/bin/env python3
"""Append a row to runs/system_metrics_history.csv.
Usage: python3 scripts/append_metrics_history.py --dataset chicago --companies 400 --valid_domains 300 --unresolved_domains 100 --emails_generated 900 --emails_verified_valid 700 --emails_verified_catchall 80 --emails_verified_invalid 120 --notes "weekly run"
"""
import argparse, csv, os
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TARGET = os.path.join(ROOT, 'runs', 'system_metrics_history.csv')
HEADERS = ['timestamp','dataset','companies','valid_domains','unresolved_domains','emails_generated','emails_verified_valid','emails_verified_catchall','emails_verified_invalid','notes']

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--dataset', required=True)
    p.add_argument('--companies', type=int, required=True)
    p.add_argument('--valid_domains', type=int, required=True)
    p.add_argument('--unresolved_domains', type=int, required=True)
    p.add_argument('--emails_generated', type=int, required=True)
    p.add_argument('--emails_verified_valid', type=int, required=True)
    p.add_argument('--emails_verified_catchall', type=int, required=True)
    p.add_argument('--emails_verified_invalid', type=int, required=True)
    p.add_argument('--notes', default='')
    a = p.parse_args()
    os.makedirs(os.path.dirname(TARGET), exist_ok=True)
    write_header = not os.path.exists(TARGET)
    with open(TARGET, 'a', newline='') as f:
        w = csv.DictWriter(f, fieldnames=HEADERS)
        if write_header:
            w.writeheader()
        w.writerow({'timestamp': datetime.utcnow().isoformat()+'Z', 'dataset': a.dataset, 'companies': a.companies, 'valid_domains': a.valid_domains, 'unresolved_domains': a.unresolved_domains, 'emails_generated': a.emails_generated, 'emails_verified_valid': a.emails_verified_valid, 'emails_verified_catchall': a.emails_verified_catchall, 'emails_verified_invalid': a.emails_verified_invalid, 'notes': a.notes})
    print(f'Appended row to {TARGET}')

if __name__ == '__main__':
    main()
