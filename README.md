# Datalithics — Taco Bueno Pricing Analytics

Analysis of promotions and discounts across ~65M Taco Bueno POS transactions (Jan 2025 – Sep 2026), built on a Medallion architecture in Databricks with data quality (DQ) validation at every layer and orchestration via Jobs.

## Architecture

```
 597 daily POS CSVs (TB_Daily_TLD_*.csv)
 UC Volume: /Volumes/workspace/default/taco_bueno_files
        │
        ▼
┌──────────────── BRONZE ────────────────────────────────────────────┐
│ taco_bueno_raw_data (85,273,331 rows · 22 STRING cols)              │
│ taco_bueno_clean_data (Phase 1 · notebook 01 · 65,082,100 rows)     │
└─────────────────────────────────────────────────────────────────────┘
        │  03_Pipeline_Silver (exact dedup, type casting, quantity/price
        │  filters, outliers to quarantine · 12 DQ checks)
        ▼
┌──────────────── SILVER ─────────────────────────────────────────────┐
│ taco_bueno_silver (65,082,100 rows · CLUSTER BY date)                │
│ taco_bueno_silver_quarantine (1,586 unique outliers)               │
└─────────────────────────────────────────────────────────────────────┘
        │  04_Pipeline_Gold (analytical aggregates · 15 cross-layer
        │  reconciliation DQ checks Silver ↔ Gold)
        ▼
┌──────────────── GOLD ───────────────────────────────────────────────┐
│ taco_bueno_gold_monthly       · by month                            │
│ taco_bueno_gold_product       · by product                          │
│ taco_bueno_gold_channel       · by channel                          │
│ taco_bueno_gold_hourly        · by hour of day                      │
│ taco_bueno_gold_dayofweek     · by day of week                       │
│ taco_bueno_gold_promo_effectiveness · by product×channel combination │
└─────────────────────────────────────────────────────────────────────┘
        │                              │
        ▼                              ▼
 02_EDA (section 8)            06_Analisis_Hipotesis (H1–H8)

 Governance and observability:
 • taco_bueno_dq_results   — central DQ log (12 silver + 15 gold, all PASS)
 • taco_bueno_kpi_snapshot — append-only run history (KPIs + cadence)
 • 05_Pipeline_Job         — orchestrator; run by 3 scheduled Jobs
```

> Notebook filenames are in Spanish; they are the actual workspace file names.

## Notebooks (`/Notebooks`)

| Notebook | Role |
| --- | --- |
| `01_Limpieza_Datos` | Phase 1: cleaning rules and type definitions (source of `taco_bueno_clean_data`) |
| `02_EDA` | Business EDA. Sections 1–7 on the cleaned table; **section 8** on the Gold tables |
| `03_Pipeline_Silver` | Production pipeline Bronze → Silver (dedup, casts, quarantine, 12 DQ checks) |
| `04_Pipeline_Gold` | Production pipeline Silver → Gold (6 tables + 15 reconciliation DQ checks) |
| `05_Pipeline_Job` | Orchestrator: `%run` 03 → 04, final validation (asserts) and KPI snapshot |
| `06_Analisis_Hipotesis` | Hypothesis exploration H1–H8 on Gold (read-only; not part of the pipeline) |

## Orchestration (Lakeflow Jobs)

Three Jobs run the same `05_Pipeline_Job` orchestrator with the `pipeline_cadence` parameter to tag each run:

| Job | Trigger | `pipeline_cadence` |
| --- | --- | --- |
| Taco Bueno Pricing Pipeline - Semanal (Weekly) | every 1 week | `semanal` |
| Taco Bueno Pricing Pipeline - Quincenal (Biweekly) | every 2 weeks | `quincenal` |
| Taco Bueno Pricing Pipeline - Mensual (Monthly) | cron: 1st of month, 09:00 (Mexico City) | `mensual` |

Each run appends one row to `taco_bueno_kpi_snapshot` with its `trigger_cadence` (or `manual` when run interactively). The pipeline is **idempotent**: a rerun never duplicates rows or double-counts revenue.

## Key dataset figures (non-Void base)

* Period: 2025-01-22 to 2026-09-27 (21 full months)
* 64,862,789 transactions · 13,775,387 orders · 119 stores (115 concurrent max per month)
* Revenue $1,150.14M · average ticket $17.73
* Total discounts $24.14M (2.10% of revenue); discount rate fell from 22.27% (Jan 2025) to ~6-7% (2026)
* 643 products · 12 channels (To Go and Drive Thru account for ~63% of transactions)

## Data quality (DQ)

* **27 automated checks** per run, all logged in `taco_bueno_dq_results` (layer, rule, observed, expected, status).
* Silver (12): correct types, 0 null dates, 0 duplicates, exact reconciliation raw = unique + dups + outliers.
* Gold (15): cross reconciliation of every Gold table against Silver totals (transactions, revenue, orders).
* If a check fails, the orchestrator raises an `assert` and the Job ends in ERROR, visible in the run history.

## How to run

1. Run the `05_Pipeline_Job` notebook top to bottom (or trigger any of the Jobs).
2. Verify DQ: `SELECT * FROM taco_bueno_dq_results WHERE status <> 'PASS'` (must return 0 rows).
3. Compare runs: `SELECT run_ts, trigger_cadence, * FROM taco_bueno_kpi_snapshot ORDER BY run_ts DESC`.

## Permissions (workspace `workspace.default`)

* `taco_bueno_raw_data` and volume `taco_bueno_files`: owned by the team (SELECT / read OK; volume write restricted).
* Silver, Gold, DQ, and snapshot tables: created in Phase 2 by this pipeline.
* `02_EDA` is tolerant to a missing `WRITE VOLUME` grant: charts display inline even when they cannot be saved as SVG.

## Roadmap

* [ ] Email/webhook notifications when a Job fails
* [ ] Incremental processing in the Silver layer (vs. full recompute)
* [ ] Lakeview dashboard on top of the Gold tables
* [ ] A/B experiments to validate causal hypotheses (H1–H8)

---

*Phase 2 — Datalithics (Tec de Monterrey). Hypothesis details and findings live in `06_Analisis_Hipotesis`; the full exploratory analysis in `02_EDA`.*