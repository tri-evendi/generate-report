# DAP Bronze Initial Load — Benchmark Report (2026-06-04/05)

**Run:** 2026-06-04 15:45 WIB → 2026-06-05 07:51 WIB · **Project:** DAP-Bronze-Oneflux-Initial (BLIV project 26)
**Prepared:** 2026-06-05 · benchmark run id `run_20260604T084411Z`

---

## 1. Executive summary

| Metric | This run | May 2026 run | Change |
|---|---|---|---|
| **Wall-clock (all domains)** | **16.1 h** | 55h 53m actual / 33-35h clean estimate | **2.1× faster than May's best case** |
| Rows loaded | 336,537,005 | 394.7M (incl. ~43M duplicate rerun writes) | clean single pass |
| Data transferred | 255.3 GiB | 351 GiB (incl. reruns) | — |
| field_operations (the bottleneck domain) | **16.1 h** | 32h 28m | **halved** |
| Domains needing reruns / pipeline rebuilds | **0** | 7 reruns, 5 rebuilds | eliminated |
| Post-load row validation | 856/856 tables within tolerance; 10/10 exact spot checks | partial, manual | systematic |

Every number in this report is reconstructed from server-side ground truth (ClickHouse `system.query_log` + per-row ingest metadata), not from the monitoring dashboard. The method, configuration, and rationale are documented so the run is **repeatable and comparable** — re-running the same protocol after any infrastructure change yields directly comparable numbers.

---

## 2. How we measured (benchmark method)

Per DMBOK2 Ch.14 (assess before integrating) and the project's measure-first standard:

- **Pre-flight snapshot** (`bench_preflight.py`): wall-clock T0, three-way clock-skew calibration (laptop / ClickHouse / MariaDB), source row estimates, CDC binlog GTID anchor, and the full load configuration recorded in a manifest. *A benchmark result without its configuration is not comparable to anything.*
- **Unattended run**: no live observation required — recorders are server-side (`system.query_log` has unbounded retention on this instance; every bronze row carries `_ingested_at` / `_batch_id`).
- **Post-run harvest** (`bench_harvest.py`): per-table → per-domain → run-total rollups of wall-clock, rows/s and **bytes/s** (rows/s alone hides 10× variance between narrow and wide tables), plus stall-gap detection.
- **Validation**: estimate sweep across all 1,354 tables + exact `COUNT(*)` spot checks against source.

## 3. Scope

- **1,354 source tables** (locked policy scope, unchanged since the May run) across **13 domains** → 13 per-domain ClickHouse databases (`bronze_dap_oneflux_<domain>`), payroll excluded (empty at source).
- **3,774 extract jobs** ("chunks") in the load queue — sized per table by the byte-budget rule (§5).
- 856 tables carried data; 497 are empty at source (loaded as empty, correctly); 1 excluded (payroll).

## 4. Results

### 4.1 Per-domain

| Domain | Tables | Wall-clock (h) | Rows | GiB | avg MB/s |
|---|---:|---:|---:|---:|---:|
| field_operations | 118 | **16.1** | 252,790,930 | 161.2 | 3.0 |
| partner_data_exchange | 14 | 3.1 | 30,476,211 | 42.8 | 4.1 |
| site_estate | 123 | 2.3 | 18,384,966 | 27.2 | 3.5 |
| customer_management | 202 | 1.1 | 14,821,880 | 10.1 | 2.7 |
| procurement | 109 | 0.7 | 7,469,568 | 6.8 | 2.8 |
| site_build | 127 | 0.5 | 6,738,085 | 3.9 | 2.3 |
| finance_gl | 128 | 0.3 | 3,309,740 | 1.8 | 2.0 |
| reference_data | 8 | 0.3 | 1,428,784 | 0.4 | 0.4 |
| platform_audit_workflow | 12 | 0.2 | 1,100,275 | 1.2 | 1.7 |
| customer_billing | 2 | <0.1 | 12,612 | <0.1 | 2.9 |
| human_capital | 12 | <0.1 | 3,932 | <0.1 | 0.9 |
| finance_ap | 1 | <0.1 | 22 | <0.1 | — |
| **Total** | **856 populated** | **16.1 (parallel)** | **336,537,005** | **255.3** | |

All 13 domains started simultaneously at T0; total wall-clock = the slowest domain (field_operations). The other 12 finished within 3.1 h.

### 4.2 Largest tables (top 15 by data volume)

| Table | Rows | GiB | Notes |
|---|---:|---:|---|
| maintenance_report_child | 230,717,776 | 138.9 | the fleet whale; 1,692 chunks × 125K rows |
| ant_integration_log | 940,385 | 22.4 | blob-class (API payload logs, ~25KB/row) |
| vendor_kpi_input_child | 16,470,569 | 17.1 | 305 chunks × 50K |
| sap_integration_log | 621,618 | 9.6 | blob-class; grew 340× since April (active SAP QA testing) |
| shortener | 1,349,720 | 9.4 | blob-class (URL payloads) |
| ant_integration_child | 27,388,798 | 9.0 | 126 chunks × 200K |
| site_visit_request | 438,168 | 7.9 | blob-class (signature/image metadata) |
| site_visit_visitor_detail | 12,653,722 | 5.1 | |
| purchase_order_item | 2,869,828 | 3.2 | |
| trouble_ticket | 1,000,814 | 2.5 | 177-column wide table |
| location | 92,375 | 1.9 | blob-class |
| material_request_item | 1,876,360 | 1.7 | |
| maintenance_order | 838,654 | 1.7 | 161 columns |
| item_price_correctiv_child | 6,753,674 | 1.6 | |
| material_request | 1,872,242 | 1.4 | |

Full per-table detail (856 rows): `artifacts/benchmarks/run_20260604T084411Z/query_log_per_table.json`.

## 5. Loading strategy and safeguards — what, and why

### 5.1 The platform constraint that drives the design

Two measured facts about this environment (2026-06-04 calibration):
1. A **~26–43s silent-connection kill** sits in front of source MariaDB and affects **every** client path (reproduced from outside BLIV — it is a proxy/LB policy, not the BLIV firewall).
2. Source **cold-read throughput is ~2.5 MB/s** (~5K rows/s on narrow tables) even with the server idle — warm reads are 30–300× faster, which made several earlier investigations chase phantom config fixes (every "fix" tested on a previously-scanned range appeared to work).

Under the cursor-fetch JDBC mode (adopted per day-33 R&D to eliminate May's TCP idle-drop failures), each extract query materializes its result silently before the first byte ships. Any chunk whose **cold materialization exceeds the kill window dies** — so chunk sizing is a per-byte calculation, not per-row.

### 5.2 The chunking rule (ADR-044)

> **≤ 50 MiB of cold reading per chunk** (~20s at 2.5 MB/s, with margin): rows-per-chunk = 50 MiB ÷ measured row width (`data_length / row_estimate` from a same-day inventory refresh), clamped to [25K, 250K] rows. Eligibility threshold is byte-aware (75K rows) so wide mid-size tables aren't missed. **Blob-class tables (≥2 KiB/row) are exempt** — measured behavior shows blob reads stream continuously (no silent phase) and tolerate full-size extracts.

Grounding: **DMBOK2 Ch.6 (Data Storage & Operations)** — capacity/performance decisions from *measured* platform characteristics; **DMBOK2 Ch.14** — profile before integrating. Validated by same-day A/B with virgin-range controls: 1M-row chunks 0/1, 250K retry-dependent, 100K clean 10/10. All sizing decisions are recorded per table with reasons in the chunking artifact (silent transformations are unaccountable — DMBOK2 Ch.12 / project ADR standard).

### 5.3 Safeguard layers (defense in depth — each handles a distinct failure mode)

| # | Safeguard | Failure mode it covers | Provenance |
|---|---|---|---|
| 1 | JDBC URL params (`zeroDateTimeBehavior=convertToNull`, `connectTimeout`, `socketTimeout`, `tcpKeepAlive`, `useCursorFetch`, `defaultFetchSize`) | zero-dates; connect hangs; idle drops | ADR-028 (now fully compliant — May ran with 2 gaps) |
| 2 | Inline SQL date guards on every DATE/DATETIME column | out-of-range dates vs ClickHouse Date32 | ADR-024 |
| 3 | Sentinel companion `*_is_open` flags (5 tables) | business "open-ended" dates (9999/2900/…) preserved as flags, not corrupted | ADR-024 amendment v2 |
| 4 | Catch-all final chunk per chunked table (open upper bound) | rows inserted between manifest generation and extraction | ADR-025 |
| 5 | Byte-budget chunk sizing + blob exemption | the silent-kill window (§5.1) | ADR-044 |
| 6 | NiFi retry ×8 on Extract failure (penalize-flowfile) | residual cold-scan misses; ~10 un-chunkable tables (each failed attempt warms the next) | ADR-044 |
| 7 | `normalize_table_or_column_names=true` | cursor-mode metadata exposes spaced Frappe table names → Avro rejects | day-26 finding, re-confirmed |
| 8 | Blob-class entries ordered last in each domain queue | head-of-line blocking of small tables behind multi-GB blob crawls (single extract thread per domain) | day-34 measurement |
| 9 | GFF trigger processors on a once-a-year cron | accidental "start" would re-emit the whole queue every minute; now inert — Run-Once is the only trigger path | operational failsafe |
| 10 | CDC GTID anchor recorded before load + ReplacingMergeTree | zero-gap CDC resume: binlog replay from the anchor overlaps the load harmlessly (newest version wins) | ADR-005 semantics |
| 11 | Pre-truncate row snapshot + post-load count validation | silent data gaps | this protocol |

### 5.4 Operational protocol

Sequence executed: stop CDC → record GTID anchor (`1-1-3809296`) → pause silver/gold schedules → truncate bronze (snapshot taken) → pre-flight manifest → start all 13 domains in parallel → unattended run → harvest → validate → resume CDC from the anchor.

## 6. Incidents and findings during the run

- **Launch stampede (first ~3 min):** 13 simultaneous extract threads vs the source connection pool's 8 slots produced brief pool-timeout errors; absorbed by the retry layer; self-resolved when the 6 small domains finished. *Future runs: stagger small domains or raise pool max-wait.*
- **Genuine stalls were where predicted:** partner_data_exchange (23–44 min insert gaps = the 8.6 GB SAP-log + 22 GB ANT-log blob crawls) and site_estate (34–42 min = Site/Site-Visit blobs). All completed without intervention.
- **Source data grows fast:** `tabSAP Integration Log` 1.8K → 621K rows (+8.2 GiB) since the April inventory — QA's SAP integration testing writes heavily. Inventory must be refreshed immediately before queue generation (now part of the protocol).
- **Two ops escalations (the real fixes; current design is the workaround):**
  1. The ~26–43s silent-connection kill in front of source MariaDB (all client paths). Raising it to ≥120s would re-enable 1M-row chunks ≈ several hours faster on the whale domain.
  2. Source cold-read throughput ~2.5 MB/s on an idle server — storage-level; this is the fundamental speed limit of any extraction strategy against this QA instance.

## 7. Validation

- **Sweep:** all 1,354 in-scope tables checked; 856 populated tables all within tolerance of same-day source estimates; 497 empty-at-source = empty-in-bronze; 1 expected exclusion (payroll).
- **Exact spot checks (10/10 match):** `tabAccount` 4,747 · `tabHoliday` 365 · `tabService Level Agreement` 20 · `tabPermit Integration Log` 179,190 · `tabRequest Corr MO` 131,593 · `tabAcquisition Order` 3,214 · `tabTower Acquisition Result` 47 · `tabGR Maintenance` 1,034,725 · `tabInsurance Report` 264,122 · `tabClass of Service` 9.
- Anything modified at source after its table's extract is delivered by CDC replay from the GTID anchor (§5.3 #10) — no gap by construction.

## 8. CDC catch-up benchmark (appended 2026-06-05)

**Scenario measured:** CDC stopped 2026-06-04 15:38 WIB (GTID anchor `1-1-3809296` recorded), bronze truncated and re-loaded, CDC resumed 2026-06-05 09:25 WIB → **~17.8 hours of binlog backlog (~4,900+ source transactions)** replayed from the anchor.

| Measure | Result |
|---|---|
| Time-to-current, fastest domains (platform_audit, site_estate, customer_management) | **< 10 min** after restart |
| Time-to-current, all 13 domains | **≤ ~50 min** after restart |
| Replay ordering | strictly chronological per domain (verified via `_source_timestamp_ms` progression) |
| DLQ failures during replay | **0** |
| Replay overlap with the initial load | harmless by design — events the load already captured land as newer row versions; ReplacingMergeTree collapses them (ADR-005) |
| Steady-state event latency after catch-up | 1–2 min pulses (capture processors run on a 1-minute schedule) |
| "Caught-up vs stalled" verification | source-side probe: domains whose freshest replayed event stopped advancing had **zero newer source modifications** — quiet, not stuck |

**Operational notes from the resume (now runbook items):**
1. After any multi-hour CDC stop, the sink connection pool must be verified **before** starting the readers (toggle the controller, then canary one PG and confirm rows land). Readers run ahead of a dead sink and their state advances past dropped events; only the GTID anchor makes that recoverable.
2. The CDC ClickHouse controller's *default database* must always point at an existing database — a dropped test DB referenced there caused all fresh pool connections to fail validation, eight days after the drop. Before dropping any ClickHouse database: grep every DBCP controller's `database_name`.
3. Old binlog reader registrations can linger server-side after a stop ("slave with same server_id already connected") — resuming readers should use fresh `server_id` values.
4. The GTID anchor + ReplacingMergeTree pair is the load-bearing safety mechanism: every recovery in this exercise (three distinct failures) was a clean replay from the anchor with zero data loss and zero manual reconciliation.

---

## Artifacts

| Artifact | Location |
|---|---|
| Run manifest + per-table data + report | `artifacts/benchmarks/run_20260604T084411Z/` |
| Load queue (3,774 jobs, per-chunk SQL) | `artifacts/pipeline_manifests/load_queue_final_20260604.jsonl` |
| Per-table chunk sizing + reasons | `artifacts/inventory/qa/whale_chunking_viability_final_20260604.jsonl` |
| Chunking decision record | `docs/adr_drafts/adr_044_chunk_byte_budget.md` |
| Benchmark tooling | `scripts/benchmarks/` (`bench_preflight.py`, `bench_harvest.py`, `liveness.py`, `truncate_bronze_for_reload.py`) |
| May 2026 baseline report | `docs/initial_load_performance_report.md` |
