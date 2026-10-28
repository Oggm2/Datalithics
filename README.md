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
* 64,862,789 transaction lines · 13,780,225 real orders (store+date+check; store+check keys undercount by ~52K because `check_number` is reused across dates) · 119 stores (115 concurrent max per month)
* Gross revenue at check level **$191.66M** · real average ticket **$13.91**. The Gold tables use the per-line convention — `total` is the check amount repeated on every line — which inflates revenue ×6.0 ($1,150.14M) and discounts ×5.2; shares and rates remain valid
* Check-level discounts **$4.61M** (2.41% of gross); item-level discounts (`SUM(item_discount)`) $2.88M; discount rate fell from 22.27% (Jan 2025) to ~6-7% (2026)
* 643 products · 12 channels (To Go and Drive Thru account for ~63% of transactions)

## Key findings and business value

The Gold tables enable insights that were not visible in the raw 85M-row CSVs. Below is a summary of the main findings — full analysis and charts are in `02_EDA` (16 visualizations) and `06_Analisis_Hipotesis` (8 hypotheses with evidence and verdicts).

### Discount strategy effectiveness

* **Discount rate collapsed** from 22.27% (Jan 2025) to ~6–7% (2026), yet **traffic held steady** at 154–223 orders per store per day (4,788–6,679 per store per month; monthly correlation +0.11) (H3). Cutting discounts did NOT cause a traffic drop — the rec was unnecessary (temporal correlation, not causation; A/B test recommended).
* **Steak Soft Taco** — 45.83% of its lines carry an item-level discount (the highest of any high-volume product; #2 reaches 27.2%; 73.29% of lines sit in discounted checks) — with $233,143 in item-level discounts against $562,749 gross item sales, up to $429,622 if full check discounts were attributed to it (H2). This product was effectively canibalizing its own price.
* **Small orders (<$10)** account for 39.8% of all orders but absorb 40.3% of total discount dollars ($1.86M), with an average ticket of just $6.45 (H7). Discounts on tiny orders don't generate incremental basket value.
* **Sides and condiments** received only ≈**$80K** in real item-level discounts (Condiments $38K, Sides $30K, other ~$11K) (H8) — the previously reported $5.26M was check-level discounts wrongly attributed to every line of these groups; the right question is whether check-level promos (combos) depend on them, requiring incremental basket analysis.

### Channel insights

* **Drive Thru** (17.99M lines, 4.18M orders, $40.3M gross item sales) operates at just 4.23% of lines in discounted checks with **$0 item-level discounts** — the lowest among promoted channels (DoorDash has 0% but is a third-party channel without pricing control) — with a **higher** non-discounted check ($17.11) than discounted ($12.60; descriptive comparison). It doesn't need promotions (H1).
* **Mobile app** has a 50.91% discount rate (check-level promos; $0 item-level discounts) on only 7,486 lines / 1,197 orders and ≈$17.6K gross item sales — the digital strategy needs rethinking (H5).
* **DoorDash** ($55.4M per-line revenue, 310,917 orders) and **UberEats** ($18.3M per-line) operate at 0% and 0.08% discount respectively — third-party delivery isn't a discount channel, it's a reach channel.
* To Go and Drive Thru together account for ~63% of all transactions.

### Temporal and operational patterns

* **Happy hour gap (1–3 PM):** revenue dips to 5.73% of daily revenue at 14:00 (the daily minimum) vs 12.1% at 17:00 (dinner peak) — absolute Gold amounts are per-line (inflated ×6), shares are valid — a targeted promo in the 1–3 PM post-lunch window could flatten the curve (H4).
* **Weekend traffic** is lower than weekdays (Saturday is the lowest at 174.5 orders/store/day; Friday 185.9; Sunday 216.3; Thursday 237.8 and Wednesday 237.5 are the highest — weekend −12.7%). Family bundles or weekend combos could even this out (H6).
* **Monthly trend:** revenue is stable at ~$55–73M/month across the 21-month period (per-line convention; check-level is ~÷6), with December 2025 dip likely seasonal.

### Pipeline observability

* `taco_bueno_kpi_snapshot` accumulates one row per run with global KPIs — comparing runs with `LAG() OVER (ORDER BY run_ts)` reveals drift in source data (if a KPI jumps, something changed upstream).
* `taco_bueno_dq_results` provides an auditable trail of all 27 DQ checks per run.

### Hypothesis verdicts (H1–H8)

| # | Hypothesis | Verdict |
| --- | --- | --- |
| H1 | Drive Thru doesn't need discounts | ✅ Confirmed — $0 item-level discounts, lowest discount rate among own channels |
| H2 | Steak Soft Taco canibalized its price | ✅ Confirmed — 45.8% item-discount lines (highest), $233K lost at item level |
| H3 | Cutting discounts didn't cause traffic drop | ✅ Confirmed — traffic held steady (154–223 orders/store/day) as discounts fell 22% → 6% |
| H4 | Happy hour opportunity 1–3 PM (post-lunch valley) | ✅ Confirmed — revenue dips at 14:00 (5.73% of daily) vs peak at 17:00 (12.1%) |
| H5 | App discounts disproportionate to volume | ✅ Confirmed — 51% discount rate on 1,197 orders |
| H6 | Weekend promos to even out traffic | ⚠️ Evaluate — weekend traffic is lower but requires A/B test |
| H7 | Small-order discounts don't pay off | ⚠️ Evaluate — 40% of discounts on <$10 orders, needs A/B |
| H8 | Sides/condiments are giveaways | ⚠️ Revisit — only ~$80K in item-level discounts; needs incremental basket analysis |

## Data quality (DQ)

* **27 automated checks** per run, all logged in `taco_bueno_dq_results` (layer, rule, observed, expected, status).
* Silver (12): correct types, 0 null dates, 0 duplicates, exact reconciliation raw = unique + dups + outliers.
* Gold (15): cross reconciliation of every Gold table against Silver totals (transactions, revenue, orders).
* If a check fails, the orchestrator raises an `assert` and the Job ends in ERROR, visible in the run history.

## Technical best practices

### Liquid clustering

The Silver table uses `CLUSTER BY (date)` to physically co-locate rows by transaction date. This accelerates the most common access pattern — temporal range scans (monthly, weekly, daily aggregates in the Gold layer). Unlike static partitioning, liquid clustering is adaptive: Databricks automatically maintains the layout as data grows, with no need to manage partition keys or run `OPTIMIZE` manually.

### Idempotency (safe reruns)

Every transformation is designed for idempotent re-execution:
* Silver and Gold tables use `CREATE OR REPLACE TABLE AS SELECT` (CTAS) — a full refresh that atomically replaces the table. No duplicates, no partial state.
* `taco_bueno_dq_results` uses `DELETE` + `INSERT` scoped by layer — reruns replace the DQ rows for that layer without accumulating stale rows.
* `taco_bueno_kpi_snapshot` is append-only by design (one `INSERT` per run). A failed run never writes a partial row because the insert happens last, after all asserts pass.

### Data quality expectations and asserts

The pipeline implements a custom expectations framework inspired by Databricks SDP expectations, adapted for the CTAS batch approach:
* **27 automated checks** (12 Silver + 15 Gold) with explicit `PASS` / `WARN` / `FAIL` semantics.
* Each check records: layer, rule name, observed value, expected value, and status — persisted in `taco_bueno_dq_results` for auditability.
* The orchestrator (`05_Pipeline_Job`) raises Python `assert` statements after both notebooks complete. Any `FAIL` ends the Job in ERROR before the KPI snapshot is written.
* This pattern gives us DQ enforcement (like SDP `expect` / `expect_or_drop`) while keeping the flexibility of raw SQL CTAS for the transformations.

### Streaming considerations

The pipeline currently runs in **batch mode** (full refresh via CTAS). This is the right choice today because:
* The source is a static CSV volume (597 files) that is not continuously ingested — there is no real-time stream to subscribe to.
* The 85M-row dataset recomputes in minutes on Serverless, making full refresh more reliable than maintaining incremental state.
* The Gold tables are aggregates that are simpler to recompute from scratch than to incrementally maintain (no merge/upsert complexity).

**Future evolution to streaming:** When the source becomes a real-time stream (e.g. POS events via Kafka or Delta Change Data Feed), the Silver layer can migrate to a streaming table with `CREATE STREAMING TABLE` and Auto Loader. The Gold layer would use `APPLY CHANGES INTO` or incremental MERGE. The DQ expectations would map directly to SDP `expect` / `expect_or_drop` / `expect_or_fail` clauses. The architecture is designed to accommodate this transition without changing the Gold table schemas or downstream consumers.

### Materialized views — why CTAS instead

We considered materialized views (MVs) for the Gold layer but chose `CREATE OR REPLACE TABLE` for these reasons:
* **Scheduled refresh control:** CTAS runs only when the Job triggers it (weekly / biweekly / monthly). MVs auto-refresh on a schedule managed by the SQL warehouse, which is less predictable and can conflict with our DQ enforcement flow (DQ checks must run after the refresh, not before).
* **DQ gating:** Our pattern requires DQ checks to run after the transformation and before the KPI snapshot. With CTAS, the orchestrator controls the exact sequence: transform → check DQ → assert → snapshot. An MV refresh happens asynchronously, breaking this ordering.
* **Cost predictability:** Full-refresh CTAS on Serverless has a fixed cost per run. MVs refresh based on a separate SQL warehouse, with less transparent cost attribution.
* **Simplicity:** CTAS is a single SQL statement with no dependency graph to manage. For 6 aggregate tables with no complex joins between them, the MV dependency tracking adds no value.

MVs would be reconsidered if the Gold layer grew to include complex multi-table joins that benefit from incremental refresh, or if downstream consumers needed always-fresh data between scheduled runs.

### Quarantine table (no silent drops)

Records that fail outlier filters (quantity > 50, menu_price > $50) are written to `taco_bueno_silver_quarantine` with the same schema and transformations applied. This ensures:
* No data is silently lost — every raw row is accounted for in either Silver or quarantine.
* Auditors can inspect excluded records (donations, gift cards, POS test transactions).
* The reconciliation check (`raw_total = silver_unique + duplicates + quarantine`) guarantees arithmetic completeness.

### Other engineering practices

* **ANSI mode disabled** (`spark.sql.ansi.enabled = false`): CAST returns NULL on invalid values instead of failing the job — bad data is caught by DQ checks rather than crashing the pipeline.
* **Column dropping for 100% null columns** (`time_card`, `menu_item_ext_ref`, `total_rounding`): reduces schema noise without losing information.
* **`DISTINCT` in CTAS**: handles exact duplicate rows from overlapping CSVs without requiring a separate dedup step.
* **Concatenated order key** (`store_id || '-' || check_number`): prevents order-count collisions between stores that share `check_number` values.

## How to run

### Prerequisites

* Databricks workspace with Unity Catalog enabled.
* Bronze table `workspace.default.taco_bueno_raw_data` (85.3M rows, loaded from 597 daily POS CSVs in UC Volume `/Volumes/workspace/default/taco_bueno_files`).
* `SELECT` on `taco_bueno_raw_data` and `READ VOLUME` on `taco_bueno_files`.
* `CREATE TABLE` / `USE SCHEMA` on `workspace.default`.
* Compute: Serverless or a Databricks cluster with Spark. The EDA notebook also needs `WRITE VOLUME` on `taco_bueno_files` to save SVG charts (optional — charts display inline regardless).

### Running the pipeline

1. **Interactive:** Run the `05_Pipeline_Job` notebook top to bottom. It `%run`s `03_Pipeline_Silver` then `04_Pipeline_Gold`, asserts all Gold tables exist, verifies DQ has no FAILs, and appends a KPI snapshot row with `trigger_cadence = 'manual'`.
2. **Scheduled:** Trigger any of the three Jobs (Semanal / Quincenal / Mensual). Each Job passes `pipeline_cadence` as a widget parameter to tag the snapshot row.
3. **Verify DQ:** `SELECT * FROM taco_bueno_dq_results WHERE status <> 'PASS'` (must return 0 rows).
4. **Compare runs:** `SELECT run_ts, trigger_cadence, * FROM taco_bueno_kpi_snapshot ORDER BY run_ts DESC`.

### Failure handling and reruns

* **Task dependencies:** `05_Pipeline_Job` runs `03_Pipeline_Silver` → `04_Pipeline_Gold` sequentially via `%run`. If Silver fails, the `%run` raises an exception and Gold never executes — the Job ends in ERROR.
* **DQ asserts:** After both notebooks complete, the orchestrator asserts that all 27 DQ checks pass and 6 Gold tables exist. Any `assert` failure ends the Job in ERROR, visible in the run history with the traceback.
* **Idempotency (safe reruns):** Silver and Gold tables use `CREATE OR REPLACE TABLE` (full refresh), and `taco_bueno_dq_results` uses `DELETE` + `INSERT` scoped by layer — a rerun never duplicates rows, double-counts revenue, or accumulates stale DQ rows.
* **KPI snapshot is append-only:** Each run inserts exactly one row (never deletes history). If a run fails before the snapshot step, no partial row is written.
* **Quarantine table:** Records that fail outlier filters (e.g. qty > 50, price > $50) are not silently dropped — they land in `taco_bueno_silver_quarantine` with the same schema for traceability and audit.
* **Job retry policy:** Jobs do not currently have a retry policy configured. Transient failures require manual rerun. Persistent failures (e.g. bad source data, DQ violation) require investigation before rerunning — check `taco_bueno_dq_results` for the specific failing check.

## Permissions (workspace `workspace.default`)

* `taco_bueno_raw_data` and volume `taco_bueno_files`: owned by the team (SELECT / read OK; volume write restricted).
* Silver, Gold, DQ, and snapshot tables: created in Phase 2 by this pipeline.
* `02_EDA` is tolerant to a missing `WRITE VOLUME` grant: charts display inline even when they cannot be saved as SVG.

## Data dictionary (Gold tables)

All tables are in `workspace.default`. `transactions` = non-Void Silver rows; `orders` = `COUNT(DISTINCT store_id || '-' || check_number)` (undercounts ~52K orders — the correct key is store+date+check, since `check_number` is reused across dates). **Metric semantics (audit 2026-10-28):** `total`, `total_discount`, `tax` and `order_voided` are check-level amounts repeated on every line of a check, so all `SUM(total)` / `SUM(total_discount)` figures (revenue, discount_amount) are inflated ~×6.0 / ×5.2; `menu_price`, `quantity` and `item_discount` are line-level. Shares, rates and counts remain valid.

### `taco_bueno_gold_monthly`

| Column | Type | Description |
| --- | --- | --- |
| `month` | DATE | First day of the calendar month (DATE_TRUNC('month', date)) |
| `transactions` | BIGINT | Non-Void transaction lines in the month |
| `orders` | BIGINT | Distinct orders (store_id + check_number) |
| `revenue` | DOUBLE | SUM(total) |
| `avg_order_value` | DOUBLE | revenue / orders |
| `discount_amount` | DOUBLE | SUM(total_discount) |
| `discount_rate_pct` | DOUBLE | % of transactions with total_discount > 0 |
| `active_stores` | INT | Distinct store_id in the month |

### `taco_bueno_gold_product`

| Column | Type | Description |
| --- | --- | --- |
| `menu_item` | STRING | Product name (excludes NULL) |
| `report_group` | STRING | Category group (e.g. '(M) Tacos') |
| `transactions` | BIGINT | Non-Void transaction lines |
| `units_sold` | BIGINT | SUM(quantity) |
| `revenue` | DOUBLE | SUM(total) |
| `discounted_transactions` | BIGINT | Count of rows with total_discount > 0 |
| `discount_rate_pct` | DOUBLE | discounted_transactions / transactions * 100 |
| `discount_amount` | DOUBLE | SUM(total_discount) |
| `discount_per_unit` | DOUBLE | discount_amount / units_sold |
| `avg_ticket_discounted` | DOUBLE | AVG(total) when total_discount > 0 |
| `avg_ticket_non_discounted` | DOUBLE | AVG(total) when total_discount = 0 |

### `taco_bueno_gold_channel`

| Column | Type | Description |
| --- | --- | --- |
| `channel` | STRING | Sales channel from `pop` (Drive Thru, To Go, Online: DoorDash, Mobile, etc.) |
| `transactions` | BIGINT | Non-Void transaction lines |
| `orders` | BIGINT | Distinct orders |
| `revenue` | DOUBLE | SUM(total) |
| `avg_order_value` | DOUBLE | revenue / orders |
| `discount_amount` | DOUBLE | SUM(total_discount) |
| `discount_rate_pct` | DOUBLE | % of transactions with discount |
| `avg_ticket_discounted` | DOUBLE | AVG(total) when discounted |
| `avg_ticket_non_discounted` | DOUBLE | AVG(total) when not discounted |
| `active_stores` | INT | Distinct store_id in channel |

### `taco_bueno_gold_hourly`

| Column | Type | Description |
| --- | --- | --- |
| `hour_of_day` | INT | Hour extracted from begin_time (0–23) |
| `transactions` | BIGINT | Non-Void transaction lines |
| `orders` | BIGINT | Distinct orders |
| `revenue` | DOUBLE | SUM(total) |
| `avg_ticket` | DOUBLE | AVG(total) |
| `discount_amount` | DOUBLE | SUM(total_discount) |
| `discount_rate_pct` | DOUBLE | % of transactions with discount |

### `taco_bueno_gold_dayofweek`

| Column | Type | Description |
| --- | --- | --- |
| `day_of_week_num` | INT | Day of week (1=Sunday … 7=Saturday, Spark convention) |
| `day_name` | STRING | Day name (Sunday, Monday, …) |
| `is_weekend` | BOOLEAN | True for Saturday and Sunday |
| `store_days` | BIGINT | Distinct store_id × date combinations |
| `transactions` | BIGINT | Non-Void transaction lines |
| `orders` | BIGINT | Distinct orders |
| `avg_orders_per_store_day` | DOUBLE | orders / store_days |
| `revenue` | DOUBLE | SUM(total) |
| `discount_rate_pct` | DOUBLE | % of transactions with discount |

### `taco_bueno_gold_promo_effectiveness`

Product × channel combination table for discount effectiveness analysis.

| Column | Type | Description |
| --- | --- | --- |
| `menu_item` | STRING | Product name |
| `channel` | STRING | Sales channel |
| `transactions` | BIGINT | Non-Void transaction lines |
| `revenue` | DOUBLE | SUM(total) |
| `discount_amount` | DOUBLE | SUM(total_discount) |
| `discount_rate_pct` | DOUBLE | % of transactions with discount |
| `avg_ticket_discounted` | DOUBLE | AVG(total) when discounted |
| `avg_ticket_non_discounted` | DOUBLE | AVG(total) when not discounted |

### Silver table: `taco_bueno_silver`

Grain: one row = one unique POS transaction line (store × date × check × item × hour).

| Column | Type | Description |
| --- | --- | --- |
| `store_id` | INT | Store identifier |
| `date` | DATE | Transaction date (cluster key) |
| `total` | DOUBLE | Check-level total, repeated on every line of the check |
| `check_number` | STRING | Check/order identifier (not unique across stores) |
| `begin_time` | TIMESTAMP | Transaction start time |
| `end_time` | TIMESTAMP | Transaction end time |
| `total_discount` | DOUBLE | Check-level discount, repeated on every line |
| `order_voided` | DOUBLE | Check-level voided amount, repeated on every line |
| `tax` | DOUBLE | Check-level tax, repeated on every line |
| `quantity` | INT | Units sold |
| `transaction_type` | STRING | 'Regular Sale', 'Discount', or 'Void' |
| `menu_item` | STRING | Product name |
| `pop` | STRING | Sales channel (Drive Thru, To Go, etc.) |
| `pos_code` | STRING | POS terminal identifier |
| `menu_price` | DOUBLE | Menu price |
| `item_discount` | DOUBLE | Per-item discount |
| `item_voided` | DOUBLE | Per-item voided flag |
| `report_group` | STRING | Product category group |
| `tender_type` | STRING | Payment method |

## Roadmap

* [ ] **Fix the Gold tables to check/item-level amounts** (revenue per check, discounts via `SUM(item_discount)` or per-check totals, orders keyed by store+date+check) — current absolute dollars are inflated ~×6; see PROJECT_SUMMARY §8
* [ ] Email/webhook notifications when a Job fails (the 2026-10-28 weekly run 443911298462218 failed silently)
* [ ] Consolidate orchestration into a single Job (pause 2 of the 3 redundant jobs) and investigate the failed weekly run
* [ ] Incremental processing in the Silver layer (vs. full recompute)
* [ ] Lakeview dashboard on top of the Gold tables
* [ ] A/B experiments to validate causal hypotheses (H1–H8)

---

*Phase 2 — Datalithics (Tec de Monterrey). Hypothesis details and findings live in `06_Analisis_Hipotesis`; the full exploratory analysis in `02_EDA`.*