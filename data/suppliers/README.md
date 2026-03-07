# Supplier Data Foundation

This folder stores supplier records for routing recommendations.

## Datasets
- `suppliers_seed.json`: baseline supplier network for routing demo.
- `suppliers_imported.json` (future): scraper/enrichment output.
- `suppliers_batch_1.json` (future): optional staged import.

## Loader
- `supplier_loader.js` merges available datasets and normalizes required fields.
- `routing_helper.js` matches opportunities to likely suppliers and builds routing-ready lead objects.

## Notes
- Keep shape aligned with `/config/supplier_schema.json`.
- Keep `equipment_categories` aligned with `config/lift_ecosystem_categories.json`.
