# CraneGenius Intent Pipeline
### Dark 30 Ventures

Automated permit signal engine. Monitors public permit portals, scores lift intent, enriches contractor domains, mines contact pages, verifies emails, and exports sender-ready CSVs every Monday.

## Quick Start

1. Fork or create this repo on GitHub
2. Add your secret: `Settings → Secrets → MILLIONVERIFIER_API_KEY`
3. Create `data/company_domain_seed.csv` (see below)
4. Go to `Actions → Run Intent Pipeline → Run workflow`

## Outputs (in GitHub Artifacts after each run)

| File | What it is |
|------|-----------|
| `data/sender_ready_hot.csv` | Score 7+, verified email, ready to send |
| `data/sender_ready_warm.csv` | Score 5+, verified email |
| `data/qa_report.json` | Run health summary — check this first |

## Seed CSV (required before first real run)

Create `data/company_domain_seed.csv` with two columns:

```
contractor_name_normalized,contractor_domain
abc electrical llc,abcelectrical.com
southwest mechanical inc,swmechanical.com
```

Pull names from Arizona ROC: https://roc.az.gov/verify-a-license  
Pull names from NECA Phoenix chapter directory.  
Goal: 100 rows before first run.

## Adding More Metro Scrapers

Each metro is a module in `src/scrapers/`. Phoenix is built. To add Dallas:
1. Find Dallas Open Data permit CSV URL
2. Add a `DallasScraper` class following the Phoenix pattern in `src/scrapers/phoenix.py`
3. Register it in `src/scrapers/__init__.py`
4. Enable it in `config/sources.yaml`

## Weekly Rhythm

| Day | Action |
|-----|--------|
| Monday | Pipeline runs automatically at 10am UTC |
| Tuesday | Review qa_report.json, download CSVs |
| Wednesday | Load hot list into PlusVibes |
| Friday | Log replies, update scoring if needed |

## Notes

- Does NOT scrape LinkedIn
- Domain resolution is seed-first, then AZ ROC registry fallback
- Canada excluded (CASL compliance)
- All source URLs and timestamps stored for audit
