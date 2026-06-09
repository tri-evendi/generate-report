# Analisis Infrastruktur — Data Analytics Platform (DAP) Mitratel

**Disusun:** 2026-06-08 (update 2026-06-09) · **Penyusun:** Tim Analisis (TAN Digital)
**Klien:** PT Dayamitra Telekomunikasi Tbk. (Mitratel / MTEL)
**Vendor platform:** PT Bangunindo Teknusa Jaya (BTJ) — produk **Bliv** (di-branding **DAP**)

> **🎯 Tujuan analisis:** menilai & mengukur apakah **kapasitas infrastruktur saat ini cukup & siap** menampung **data production Oneflux + seluruh data Mitratel** (TOBA, NMS, Datamart, Airflow), dan **menentukan apakah kickoff/UAT dijalankan dengan data QA saja atau langsung ke data production**. Lihat **§12** untuk verdict & rekomendasi.
>
> **⚠️ Disclaimer arsitektur:** saat ini tim **baru memakai datalakehouse** (instance ClickHouse **Data Lake**) untuk menyimpan data. Instance **data warehouse (Warehouse-Mart) belum sempat dipakai dan baru akan dipakai** — inilah sebabnya warehouse-mart terlihat idle (§6.1). Maka diperlukan **strategi penempatan tier bronze/silver/gold**: tetap semua di datalakehouse, atau mulai memanfaatkan data warehouse untuk silver/gold. Lihat **§12.3**.

> Analisis ini direkonstruksi dari empat dokumen di `references/`:
> 1. **Laporan Implementasi DAP Mitratel** (v1.2, 28 Jan 2026) — kondisi *as-built* Phase 1.
> 2. **Proposal Teknis DAP Enhancement Phase 2** (rev 28 Apr 2026) — arsitektur & sizing target.
> 3. **DAP Bronze Initial Load — Benchmark Report** (2026-06-04/05) — beban produksi nyata & batasan platform.
> 4. **WhatsApp Image 2026-05-21** — spesifikasi server fisik (bare-metal).
> 5. **Screenshot Grafana node_exporter (2026-06-08)** — utilisasi & alokasi ClickHouse aktual untuk Data Lake & Warehouse-Mart, kondisi steady-state CDC (lihat §6.1).
>
> Catatan: setiap angka di bawah disertai sumbernya. Di mana dokumen saling bertentangan, perbedaannya **diberi tanda eksplisit** sebagai temuan, bukan diratakan.

---

## 1. Ringkasan Eksekutif

DAP Mitratel adalah platform analitik on-premise berbasis stack **Bliv** (Pipeline, Data House, Dashboard, Monitoring, Admin Panel, dan — direncanakan — Explore/AI Studio/MLOps). Kondisinya saat ini berada di **persimpangan tiga keadaan yang tidak sepenuhnya konsisten satu sama lain**:

| Dimensi | Phase 1 (as-built, Jan 2026) | Realita beban (benchmark, Jun 2026) | Phase 2 (proposal target) |
|---|---|---|---|
| Skala data | 2 DB, 2 tabel, **308K row, 16.5 MB** (sampel) | **1.354 tabel, 13 domain, 336.5 juta row, 255 GiB** (Oneflux bronze QA; est. **production ~486 GB CH** §6.4) | ≥20 domain onboard, company-wide |
| Topologi | Container Docker, node tunggal terlihat (4 vCPU/8 GB di node monitoring) | ClickHouse produksi 13 database `bronze_dap_oneflux_*` | **19 VM** di 8 service, klaster 2–3 node |
| HA | "prinsip" HA + HAProxy (belum ada angka) | Recovery teruji lewat GTID anchor + ReplacingMergeTree | HA + Zookeeper 3-node + HAProxy 2-node |

**Empat temuan utama:**

0. **CPU Data Lake jenuh saat window load/CDC-catchup (97% / 16 core), bukan saat steady-state.** Saat pipeline **initial load + CDC catch-up** berjalan, instance data lake (bronze, 16 core) terukur ~**97% CPU** — CPU-bound pada layer ingest/merge. Setelah CDC masuk steady-state (snapshot Grafana 2026-06-08, §6.1), CPU turun ke **~0.7%** (nyaris idle). Jadi ini **bukan bottleneck 24/7, melainkan batas kapasitas pada window onboarding/load.** Relevansinya untuk Phase 2: tiap onboarding sumber baru (SAP/Exware/NMS) dan ≥20 domain akan memicu ulang window load berat ini — throughput onboarding akan dibatasi oleh CPU 16-core lake, bukan oleh storage. Lihat §6.1 & risiko R0.

1. **Beban produksi nyata sudah jauh melampaui apa yang didokumentasikan sebagai "terpasang" di Phase 1.** Laporan Implementasi menggambarkan sampel 308K row, tetapi benchmark Juni menunjukkan beban Oneflux sesungguhnya **336 juta row / 255 GiB** dengan tabel tunggal terbesar (`maintenance_report_child`) **230 juta row / 138.9 GiB**. Sizing Phase 2 harus diuji terhadap angka benchmark, bukan angka sampel Phase 1.

2. **"High Availability" berisiko semu jika 19 VM berjalan di atas satu server fisik.** Spesifikasi server fisik (§4) adalah satu mesin 2U bertenaga sangat besar (384 thread / 1 TB RAM / 25.6 TB NVMe). Itu lebih dari cukup secara kapasitas untuk menampung seluruh 19 VM Phase 2 (total ±110 vCPU / 412 GB / 4.2 TB) — **tetapi klaster multi-node yang seluruhnya menjadi VM pada satu host fisik tidak memberikan ketahanan terhadap kegagalan perangkat keras.** Ini adalah risiko arsitektural paling penting yang perlu diklarifikasi dengan Mitratel.

3. **Batasan terberat bukan di DAP, melainkan di sisi sumber data.** Benchmark mengukur dua fakta keras pada sumber Oneflux: (a) *silent-connection kill* ~26–43 detik di depan MariaDB sumber (kebijakan proxy/LB, bukan firewall), dan (b) throughput baca dingin ~2.5 MB/s pada server idle. Keduanya membatasi strategi ingestion/CDC apa pun dan **tidak bisa diselesaikan dengan menambah kapasitas DAP** — perlu eskalasi ke pemilik infrastruktur sumber.

---

## 2. Inventaris Komponen & Tumpukan Teknologi

Seluruh service di-deploy sebagai **container Docker**, diorkestrasi via **Ansible playbook** (bukan Kubernetes). Image disimpan di registry internal `repo.mitratel.co.id/dap/dap_core/`. Seluruh endpoint pada domain internal `*.mitratel.co.id`.

| Modul DAP | Basis teknologi (open-source) | Versi image (Phase 1) | Status Phase 1 |
|---|---|---|---|
| **DAP Pipeline** | Apache NiFi (ETL/ELT, FlowFile, Processor, Provenance) | `pipeline 2.2.1` | Aktif |
| **DAP Data House** | ClickHouse + Vue.js/FastAPI/PostgreSQL/Redis (versioning, validasi, audit skema) | `datahouse 1.1.0` | Aktif |
| **DAP Dashboard** | Apache Superset (Charts/Datasets/SQL, RLS) | `dashboard 1.2.1` | Aktif |
| **DAP Monitoring** | Grafana + Prometheus + node_exporter | `monitoring 1.0.1` | Aktif |
| **Admin Panel / IAM** | Keycloak (Quarkus 3.x, Java 17, PostgreSQL 17, React 18) | `admin 1.1.0` | Aktif |
| **DAP Explore** | (katalog/lineage/metadata, native MCP, Elasticsearch) | — | **Belum aktif** (scope Phase 2) |
| **DAP AI Studio / MLOps** | — | — | Belum aktif |

**Stack pendukung (backing stores)** — dari diagram arsitektur Phase 1 (Laporan hal. 38):
- **PostgreSQL** — DB primer (metadata operasional + audit trail)
- **Redis** — cache & state management
- **S3 (object storage)** — penyimpanan objek
- **HAProxy** — load balancer / failover (GPLv2)

Engine backend menggunakan Python 3.11 (image `slim-bookworm`) dengan **ODBC driver `msodbcsql18`** terpasang — jalur koneksi ke **SQL Server**, yakni untuk **sumber lain (mis. SAP)**; **sumber Oneflux sendiri = MariaDB** (lihat §5). Frontend Node 20-alpine, error tracking **Sentry**, API ter-dokumentasi via Swagger/OpenAPI.

> **Catatan rilis terbaru (Mitratel.pdf / Bliv stable release):** Data House kini mendukung instance **Oracle & MariaDB**, **Semantic Layer**, **RLS/CLS**, dan Data Lakehouse Connector (InfluxDB, MinIO+Iceberg, Hadoop). Admin Panel menambah IdP **GitHub/LDAP/OIDC/Microsoft**. Explore 1.1.0 menambah **Airflow-less ingestion**, distributed indexing, dan **wajib upgrade Elasticsearch**. MLOps 1.2.0 menambah **HA multi-inference-server via HAProxy**. Menu **Virtualization akan di-deprecate** → pindah ke Data House. Hal ini relevan untuk perencanaan upgrade Phase 2.

---

## 3. Arsitektur & Alur Data

### 3.1 Alur data (medallion: raw → curated → mart)

```
Sumber (Oneflux/SAP/Exware/NMS)
        │  JDBC cursor-fetch / CDC (GTID)
        ▼
   DAP Pipeline (NiFi)  ──► DAP Data House (ClickHouse)
                                ├── Lake / Bronze  (clickhouse-dap:8181)
                                ├── Silver (curated)
                                └── Mart / Gold    (clickhouse-dap:8080)
                                        │  Analytics
                                        ▼
                                DAP Dashboard (Superset)
   Keycloak (OIDC/JWT) menggerbang seluruh service · DAP Monitoring mengamati semua
```

- **Bronze layer** dipartisi per domain: 13 database `bronze_dap_oneflux_<domain>` (field_operations, customer_management, procurement, site_estate, finance_gl, dst.).
- Engine tabel **ReplacingMergeTree** — mendukung CDC idempotent (versi terbaru menang), inti dari mekanisme recovery.
- Phase 2 secara eksplisit menargetkan jalur **raw → curated → mart** dengan Schema Change & Transformation Control di Pipeline, dan **Semantic Layer + Data Mart** di Data House.

### 3.2 Sizing target Phase 2 (Proposal hal. 6)

| Service | CPU | RAM (GB) | SSD (GB) | Node | Subtotal vCPU | Subtotal RAM | Subtotal SSD |
|---|---:|---:|---:|---:|---:|---:|---:|
| Admin Panel + Monitoring | 4 | 16 | 100 | 2 | 8 | 32 | 200 |
| Dashboard | 8 | 32 | 100 | 2 | 16 | 64 | 200 |
| Pipeline | 8 | 32 | 200 | 3 | 24 | 96 | 600 |
| Data House | 4 | 16 | 100 | 2 | 8 | 32 | 200 |
| **ClickHouse Warehouse** | 8 | 32 | **750** | 3 | 24 | 96 | 2.250 |
| Explore | 8 | 32 | 250 | 2 | 16 | 64 | 500 |
| Zookeeper | 2 | 4 | 50 | 3 | 6 | 12 | 150 |
| HAProxy + CH Proxy | 4 | 8 | 50 | 2 | 8 | 16 | 100 |
| **TOTAL** | | | | **19** | **110** | **412** | **4.200** |

Total agregat target: **±19 VM, 110 vCPU, 412 GB RAM, ±4.2 TB SSD.**

> **⚠️ Sizing proposal vs realita produksi tidak cocok.** Proposal mematok ClickHouse Warehouse **8 vCPU / 32 GB / 750 GB × 3 node**. Pengecekan produksi (§6.1) menunjukkan deployment aktual jauh lebih besar dan **sudah jenuh CPU**: data lake **16 core (97% terpakai) / 3 TB**, warehouse **6 TB**. Angka 750 GB di proposal tampak menggambarkan node *placeholder*, bukan instance yang sekarang menanggung beben Oneflux. Sizing CPU pada proposal (8 core/node) **terlalu kecil** dibanding kenyataan 16 core yang sudah mentok.

---

## 4. Infrastruktur Fisik (server bare-metal)

Dari WhatsApp Image 2026-05-21 — **1× DAP Server** (peruntukan: *Server Data Analytic Platform* + *Server Database Oneflux*):

| Komponen | Spesifikasi |
|---|---|
| **CPU** | 2× **AMD EPYC 9655** @ 2.60 GHz (96C/192T per socket) — **192 core / 384 thread** total, 384 MB cache (400W), DDR5-6400 |
| **RAM** | 16× 64 GB RDIMM 6400 MT/s Dual Rank = **±1.024 GB (1 TB)** |
| **Storage data** | 8× **3.2 TB NVMe** Datacenter Mixed-Use (AG Drive E3.S Gen5) = **25.6 TB raw** |
| **Storage boot** | 2× M.2 960 GB (**RAID 1**) + 960 GB SATA |
| **Jaringan** | 2× Broadcom 57414 25GbE SFP28 dual-port (OCP 3.0 NIC + PCIE); 4× Dell transceiver 25GbE SFP28 SR MMF |
| **Daya** | Dual redundant power supply |
| **Form factor** | Rack-mounted **2U**, Pro Support + rail |

**Analisis kapasitas:** kebutuhan VM Phase 2 (110 vCPU / 412 GB / 4.2 TB) **muat dengan sangat longgar** dalam satu server ini (384 thread / 1 TB / 25.6 TB). Bahkan dengan replikasi ClickHouse dan pertumbuhan data, headroom-nya besar (255 GiB bronze hari ini ≪ 25.6 TB).

**Namun ini menimbulkan tiga catatan kritis (lihat §8):**
- Redundansi pada level **VM/cluster** tidak setara dengan redundansi perangkat keras bila semua berada di **satu chassis 2U**.
- Co-location *DAP* dan *Database Oneflux (sumber)* pada server yang sama mencampur beban OLTP sumber dengan OLAP analitik — berpotensi saling memengaruhi (lihat batasan baca-dingin di §6).
- Jumlah unit server tidak tertera jelas (tabel menyebut "1 DAP Server"). Perlu konfirmasi apakah ada node kedua untuk HA fisik.

---

## 5. Sumber Data & Integrasi

| Sumber | Status | Metode | Catatan |
|---|---|---|---|
| **Oneflux** (Frappe/ERPNext **MariaDB**, tabel `tab*`) | **Terintegrasi penuh** (bronze) | JDBC cursor-fetch + CDC binlog GTID | 1.354 tabel, 13 domain |
| **TOBA** (TimescaleDB/PostgreSQL) | Direncanakan ingest | JDBC (driver PG) | DB target: `toba_production` (~19 GB) |
| **NMS** (NMSProd / Zabbix, PostgreSQL) | Direncanakan ingest | JDBC (driver PG) | full DB, dominan `zabbix` ~953 GB |
| **Datamart OF** (PostgreSQL) | Direncanakan ingest | JDBC (driver PG) | DB target: `dm_of_dev` + `dm_of_prod` (~14.2 GB) |
| **Airflow prod** (PostgreSQL) | Direncanakan ingest | JDBC (driver PG) | full DB `airflow` (~8.6 GB) |
| **SAP** | Direncanakan ingest | connector TBD (SAP/HANA) | ⏳ **volume belum diestimasi (TBD)** — belum masuk proyeksi |
| Exware | Disebut di proposal (belum dikonfirmasi) | — | Batasan Phase 2 |

> **Catatan connector:** sumber baru (TOBA/NMS/Datamart/Airflow) semuanya **PostgreSQL/TimescaleDB** — berbeda dari Oneflux (**MariaDB**). Perlu DBCPConnectionPool driver PostgreSQL di NiFi; CDC PostgreSQL memakai logical replication (bukan binlog GTID seperti MariaDB), jadi mekanisme capture-nya berbeda dan perlu divalidasi.

> **✓ Sumber Oneflux = MariaDB (dikonfirmasi).** Konsisten dengan benchmark (skema Frappe `tabXXX`) dan CDC binlog GTID (mekanisme MySQL/MariaDB). Driver ODBC SQL Server (`msodbcsql18`) yang terpasang di image backend Phase 1 berarti untuk **sumber lain** (mis. SAP/MSSQL), **bukan** Oneflux.

Metode integrasi yang didukung platform: **JDBC connection pool, REST API, SFTP, HTTP/S, dan CDC (binlog GTID)**. Pemisahan environment (dev/staging/prod) via NiFi **Parameter Context** (tanpa hardcode kredensial).

---

## 6. Volume Data, Skala & Performa

### 6.1 Kondisi produksi ClickHouse terkini (Grafana node_exporter, 2026-06-08 14:2x, steady-state CDC)

Snapshot diambil saat **CDC sudah steady-state** (initial load selesai). Angka stat-panel (tidak ambigu):

| Instance (host) | Core | RAM | Disk (RootFS) | CPU now | RAM used | SWAP used | Disk used (≈) | Uptime |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **Data Lake** — `bliv-data-lake:9100` (bronze) | **16** | **63 GiB** | 3 TiB | 0.7% | 10.8% (~6.8 GiB) | **26.4% (~2.1 GiB)** | 33.2% (**~1.0 TB**) | 25.4 mgg |
| **Warehouse-Mart** — `clickhouse-warehouse-mart:9100` (disiapkan utk gold/mart — **belum dipakai**) | **32** | **126 GiB** | 6 TiB | 0.5% | 3.3% (~4 GiB) | 0.0% | 5.6% (**~0.34 TB**) | 25.4 mgg |

**Pembacaan analitis:**

- **CPU bukan bottleneck steady-state — tapi bottleneck saat load/onboarding.** Saat snapshot, kedua instance nyaris idle (0.7% / 0.5%). Lonjakan **97% pada 16 core** terukur saat **initial load + CDC catch-up** berjalan (workload merge ReplacingMergeTree + write CDC masif). Ini cocok dengan benchmark: initial load = 16.1 jam berat, steady-state CDC = "pulsa 1–2 menit". → **Sizing CPU lake harus ditentukan oleh window onboarding, bukan steady-state.** Tiap sumber/domain baru di Phase 2 memicu ulang window berat ini.
- **🔴 SWAP 26.4% (~2.1 GiB) di data lake meski RAM 89% bebas = sinyal memory pressure historis.** Halaman ter-swap saat merge/load berat (RAM 63 GiB sempat ketat) dan tidak ditarik kembali. **Untuk host database, swap pada ClickHouse berbahaya bagi performa** — rekomendasi: turunkan `vm.swappiness` (≈1) atau nonaktifkan swap, dan pertimbangkan tambah RAM lake.
- **🔴 Asimetri sumber daya yang janggal:** layer **bronze yang berat = 16 core / 63 GiB RAM**, sedangkan **warehouse/mart yang nyaris idle justru 32 core / 126 GiB RAM (2× di kedua dimensi)**. Alokasi tampak **terbalik dari profil beban** — instance ingest/merge yang sibuk malah paling kecil. Tinjau ulang: bronze lebih butuh CPU+RAM daripada mart yang santai. (Strategi tier Opsi B di §12.3 justru memanfaatkan mart yang besar+idle ini untuk serving silver+gold.)
- **Storage cocok dengan laporan & lega:** lake ~1.0 TB terpakai / 2 TB sisa (vs benchmark 255 GiB — selisih = parts belum ter-merge / beberapa siklus load); mart ~0.34 TB / 6 TB. Tapi **headroom lake tinggal 2 TB** menjelang onboarding SAP/Exware/NMS + ≥20 domain — pantau ketat.
- **Uptime 25.4 minggu** (~6 bulan, sejak ~Des 2025) konsisten dengan go-live Phase 1.

### 6.2 Beban initial load (dari Benchmark 2026-06-04)

> **⚠️ Konteks penting:** angka di bawah berasal dari **environment QA, dengan sebagian tabel di-exclude** — **bukan** volume produksi. Volume **production + archive akan lebih besar** (estimasi menyusul dari Mitratel). Jadi 255 GiB / 336 juta row adalah **batas bawah**, bukan target sizing. Lihat §6.3 untuk gap yang menunggu data.

Ini adalah angka **ground-truth** (dari ClickHouse `system.query_log`) pada QA, bukan sampel:

| Metrik | Nilai |
|---|---|
| Wall-clock initial load (paralel, 13 domain) | **16.1 jam** (2.1× lebih cepat dari best-case Mei) |
| Total row dimuat | **336.537.005** |
| Total data | **255.3 GiB** |
| Tabel terpopulasi / total in-scope | 856 / 1.354 |
| Domain bottleneck | `field_operations` — 252.8 juta row, 161.2 GiB, 16.1 jam |
| Tabel tunggal terbesar | `maintenance_report_child` — **230.7 juta row, 138.9 GiB** (1.692 chunk × 125K) |
| CDC catch-up (≈17.8 jam backlog) | seluruh 13 domain *current* dalam **≤ ~50 menit**, 0 DLQ failure |

**Batasan platform sumber yang diukur (kritis untuk arsitektur ingestion):**
1. **Silent-connection kill ~26–43 detik** di depan MariaDB sumber (kebijakan proxy/LB di seluruh jalur klien). → memaksa strategi chunking **berbasis byte** (ADR-044: ≤50 MiB cold-read per chunk, klem 25K–250K row), bukan berbasis jumlah row.
2. **Throughput baca dingin ~2.5 MB/s** pada server sumber idle (baca hangat 30–300× lebih cepat). → ini adalah **batas kecepatan fundamental** ekstraksi apa pun terhadap instance QA Oneflux.

**Implikasi infrastruktur:** menaikkan timeout sisi sumber ke ≥120 detik akan mengaktifkan kembali chunk 1M-row dan memangkas waktu domain "whale" beberapa jam. Ini **eskalasi ke tim infrastruktur sumber**, bukan tuning DAP. Pertumbuhan sumber cepat (mis. `tabSAP Integration Log` 1.8K → 621K row / +8.2 GiB dalam 2 bulan) → inventaris harus di-refresh tepat sebelum tiap load.

**Safeguard yang sudah terpasang (defense-in-depth, 11 lapis — benchmark §5.3):** parameter JDBC (cursor-fetch, tcpKeepAlive, socketTimeout), guard tanggal SQL inline (vs ClickHouse Date32), sentinel `*_is_open`, catch-all final chunk, byte-budget chunk sizing + blob exemption, NiFi retry ×8, GTID anchor + ReplacingMergeTree, pre/post-load count validation.

### 6.3 Volume sumber data tambahan untuk diingest (terukur 2026-06-09)

Hasil `pg_database_size` per server sumber (PostgreSQL/Timescale), DB sesuai cakupan yang disepakati (asumsi **tanpa tabel di-exclude**):

| Sumber | DB diambil | Ukuran sumber (PG, incl. index/bloat) | Est. on-disk ClickHouse bronze* |
|---|---|---:|---:|
| TOBA | `toba_production` | 19 GB | ~4–10 GB |
| **NMS** | `zabbix` (+`grafana` 54 MB) | **~953 GB** | **~50–300 GB** (time-series, sangat bervariasi) |
| Datamart OF | `dm_of_dev` (14 GB) + `dm_of_prod` (212 MB) | ~14.2 GB | ~3–8 GB |
| Airflow prod | `airflow` | ~8.6 GB | ~2–4 GB |
| **SAP** | (TBD) | ⏳ **belum diestimasi** | ⏳ **belum diestimasi** |
| **TOTAL TAMBAHAN (tanpa SAP)** | | **~995 GB (~1 TB)** | **~60–320 GB** |

*\*estimasi kasar: ClickHouse columnar+kompresi (LZ4/ZSTD) biasanya 3–10× lebih kecil dari row-store PG (yang juga memuat index & bloat). Zabbix history/trends sangat kompresibel (10×+), tapi volumenya besar → tetap dominan.*

**Pembacaan:**
- **NMS/Zabbix (953 GB) adalah 96% dari volume sumber baru** — hampir sebesar seluruh data terpakai di lake hari ini (~1 TB). Bahkan setelah kompresi, ini item terbesar dan **perlu keputusan retensi**: apakah seluruh history Zabbix dibutuhkan di lake, atau cukup window tertentu + TTL/archive ke storage dingin.
- TOBA/Datamart/Airflow relatif kecil (total <42 GB sumber) — dampak storage minor.
- **Worst-case tanpa kompresi**: +1 TB ke lake yang sisa 2 TB → tinggal ~1 TB. Dengan kompresi wajar: +60–320 GB → masih aman. **Tapi belum termasuk Oneflux production penuh** (masih ⏳, lihat di bawah) yang bisa jauh lebih besar dari QA 255 GiB.

### 6.4 Estimasi Oneflux QA / Archive / Production (dari `DB Size Compare.xlsx`, sheet `bronze_size_comparison`, 2026-06-09)

Estimasi per 1.354 tabel Oneflux (bronze `_raw`), beserta ukuran **actual** yang sudah terukur di ClickHouse untuk QA:

| Kolom | Total | Cakupan |
|---|---:|---|
| `qa_total_mb_estimate` | 177,637 MB = **173.47 GB** | 1.354 tabel |
| `archive_total_mb_estimate` | 102,746 MB = **100.34 GB** | 1.298 (56 tabel tanpa estimasi) |
| `prod_total_mb_estimate` | 338,465 MB = **330.53 GB** | 1.350 (4 tabel tanpa estimasi) |
| `qa_ch_actual_mb` (terukur di CH) | 261,375 MB = **255.25 GB** | 856 tabel terpopulasi |

**🔑 Faktor inflasi ClickHouse = 1.47×.** Untuk 856 tabel dengan estimasi+actual: estimasi 177,626 MB → actual **261,375 MB** = **1.471×**. Artinya bronze `_raw` di ClickHouse **membengkak ~1.47× dari estimasi byte sumber** (kolom metadata `_ingested_at`/`_batch_id`, String verbatim, versi ReplacingMergeTree belum ter-merge — bukan terkompresi). Sanity check: 173.47 GB × 1.47 ≈ 255 GB = benchmark "255 GiB" ✓.

**Proyeksi actual ClickHouse (estimasi × 1.47):**

| Skenario Oneflux | Estimasi sumber | Proyeksi actual CH |
|---|---:|---:|
| QA (terukur) | 173.47 GB | 255 GB ✓ |
| **Production** | 330.53 GB | **~486 GB (~0.47 TB)** |
| Archive (dicatat terpisah, tidak digabung) | 100.34 GB | ~148 GB |

Domain terbesar (prod, estimasi): `field_operations` ~160 GB · `partner_data_exchange` ~70 GB · `site_estate` ~60 GB · `customer_management` ~20 GB.

**🔑 Angka di atas adalah scope Oneflux yang SUDAH DIFILTER (in-scope).** Tabel sistem/temp/log sudah dikeluarkan. Sheet `exclude_bronze_size_comparison` mengukur yang dikecualikan:

| | Tabel | Prod (sumber) | Archive | QA |
|---|---:|---:|---:|---:|
| **In-scope** (`bronze_size_comparison`) | 1.354 | **330.53 GB** | 100.34 GB | 173.47 GB |
| **Excluded** (`exclude_...`) | 1.035 | **1,523.15 GB** | 988.68 GB | 999.63 GB |
| Full Oneflux (tanpa filter) | 2.389 | **~1.85 TB** | ~1.09 TB | ~1.17 TB |

Tabel excluded didominasi sistem/temp Frappe (`__Auth`, `__UserSettings`, `__global_search`, `_temp_*`, `_tmp_*`, snapshot bertanggal). **Filter menghapus ~82% volume produksi.** Tanpa filter, Oneflux prod ~1.85 TB sumber → **~2.7 TB di ClickHouse (×1.47) — tidak akan muat** di lake 3 TB. **Filter inilah yang membuat Oneflux muat** — dan sejalan dengan kriteria kelayakan bronze §11 (sistem/temp dikecualikan).

**Tren pertumbuhan Oneflux (semua tabel / unfiltered, bulanan — `sizing-database-onelfux.png`):**

| Periode | Size | Delta |
|---|---:|---:|
| Agu 2025 | 1.170.697 MB (~1.14 TB) | — |
| Sep–Nov 2025 | ~1.17–1.19 TB | +0.3–0.7%/bln |
| **Des 2025** | 322.997 MB (~315 GB) | **−72.77%** (purge/cleanup) |
| Jan 2026 | 398.861 MB (~390 GB) | +23.49% |
| **Feb 2026** | 1.838.755 MB (~1.80 TB) | **+361%** (backfill/reload) |
| Mar 2026 | ~1.82 TB | +1.19% |
| Apr 2026 | ~1.83 TB | +0.87% |
| **Mei 2026** | 1.895.669 MB (**~1.85 TB**) | +1.01% |

Pembacaan:
- **✓ Cross-validation:** full DB Mei 2026 **~1.85 TB** = total full Excel (in-scope 330 GB + excluded 1.523 GB = ~1.85 TB). Dua sumber independen cocok → memperkuat keandalan angka in-scope.
- **Volatilitas ekstrem ada di tabel EXCLUDED**, bukan di data bisnis: drop −72.77% (Des) dan lonjakan +361% (Feb) terjadi di tabel temp/snapshot/log (yang tidak di-ingest). Data in-scope (~330 GB) adalah inti yang stabil.
- **Pertumbuhan steady-state (Feb–Mei 2026): ~1%/bulan** (~18–19 GB/bln pada basis full). Untuk in-scope yang jauh lebih kecil & stabil, pertumbuhan absolut jauh di bawah ini → **bukan risiko kapasitas jangka pendek** untuk Oneflux.
- ⚠️ **Catatan operasional:** lonjakan tipe +361% (backfill/reload massal) bila terjadi pada tabel in-scope akan **membebani window load CPU (R0)** — perlu antisipasi penjadwalan.

### 6.5 Proyeksi konsolidasi data lake & gap tersisa

| Komponen bronze | Proyeksi actual CH |
|---|---:|
| Oneflux **production** (§6.4) | ~0.47 TB |
| Sumber PG baru: TOBA/NMS/Datamart/Airflow (§6.3) | ~0.5–1.0 TB ⚠️ (lihat catatan) |
| **SAP** | ⏳ **belum diestimasi (TBD)** |
| **Total proyeksi lake (tanpa SAP)** | **~1.0–1.5 TB** |

*(Archive Oneflux ~0.14 TB dihitung & dikelola terpisah, tidak digabung ke total di atas.)*

> ⚠️ **SAP belum masuk proyeksi.** Volume SAP belum diestimasi sama sekali. SAP (ERP/HANA) berpotensi besar — bila signifikan, dapat menggeser total melewati headroom lake. **Verdict kapasitas di bawah berlaku tanpa SAP**; perlu estimasi SAP untuk penilaian final.

> **Catatan penting tentang sumber PG baru:** estimasi awal "~60–320 GB" di §6.3 mengasumsikan kompresi 3–10×. **Bukti empiris Oneflux justru inflasi 1.47×**, bukan kompresi — jadi asumsi kompresi untuk sumber baru kemungkinan terlalu optimistis. **Ukuran actual CH untuk TOBA/NMS/Datamart/Airflow harus diukur langsung** (seperti `qa_ch_actual_mb` Oneflux), bukan diasumsikan. Zabbix (953 GB, time-series) bisa berperilaku beda (lebih kompresibel) — perlu uji.
>
> **Kesimpulan kapasitas:** dengan proyeksi total ~1.0–1.5 TB actual (Oneflux production + sumber PG baru; archive terpisah), data lake (3 TB, sisa ~2 TB) **masih cukup untuk muatan awal** — *asalkan* NMS/Zabbix diberi retensi/TTL dan diukur dulu. **Gap tersisa:** (a) actual CH sumber PG baru, (b) **laju pertumbuhan keempat sumber lain (TOBA/NMS/Datamart/Airflow) belum diketahui** — hanya Oneflux yang terukur (~1%/bln), (c) kebijakan retensi/archive.

---

## 7. Keamanan

| Aspek | Implementasi |
|---|---|
| **IAM** | Keycloak terpusat — OIDC, JWT (identitas + client role per service). Mendukung OAuth2/OIDC, SAML 2.0, LDAP/Kerberos/AD, social login |
| **RBAC platform** | 3 peran: SuperAdmin / Admin / Member; lisensi per-modul per-user (perpetual) |
| **RLS/CLS** | Data House: grant per-database (SELECT/INSERT/UPDATE/DELETE/ALTER/CREATE); Superset RLS; Phase 2 menargetkan **row-level & column-level security** + klasifikasi 4 tingkat (public/internal/confidential/strictly confidential) + masking PII |
| **Enkripsi** | In-transit **TLS 1.2/1.3 (AES-256)**; at-rest **AES-256** untuk DB & file |
| **Audit** | Log Management (audit trail) tersimpan di PostgreSQL; dapat di-forward ke SIEM (Splunk/Elastic/Sumo Logic); Data House punya menu Audit Logs |
| **Segregation of duty** | Akses service (client role Keycloak) dipisah dari hak operasional dalam service (JWT-validated) |

**Kelemahan/roadmap yang diakui dokumen:**
- **Secret management masih via environment variable** — diakui perlu ditingkatkan ke **KMS/Vault/HSM atau BYOK** dengan rotasi otomatis.
- **DevSecOps berbasis platform, bukan source-code** (platform closed-source/perpetual) — implementasi via fitur/governance platform, bukan CI/CD atas kode aplikasi.

---

## 8. Operasional: HA, Backup, Monitoring

| Aspek | Kondisi terdokumentasi |
|---|---|
| **Monitoring** | Prometheus + node_exporter → Grafana (DAP Monitoring); auto-refresh 30s; folder per komponen (CH Lake, CH Mart, Oneflux DWH, System) |
| **Alerting** | Rule-based di Grafana (memory, latency, network, pipeline health, query cost) |
| **HA** | Disebut "prinsip" + HAProxy failover; Phase 2 menambah **Zookeeper 3-node** (koordinasi ClickHouse) + **HAProxy/CH Proxy 2-node**. **Tidak ada replica count, topologi replikasi Postgres, RPO/RTO numerik** |
| **Backup** | Disebut sebagai milestone & kriteria ("backup berjalan & lulus restore test") — **tidak ada jadwal/retensi/RPO spesifik** |
| **Recovery (teruji)** | GTID anchor + ReplacingMergeTree = recovery bersih dari 3 kegagalan berbeda, 0 data loss, 0 rekonsiliasi manual |
| **Scheduling** | Ansible playbook (deploy/config); NiFi cron/Run-Once untuk pipeline |

**Runbook items penting dari benchmark (sudah jadi pelajaran operasional):**
- Setelah CDC stop multi-jam, verifikasi sink connection pool **sebelum** start reader (reader bisa lari di depan sink mati; hanya GTID anchor yang membuat ini recoverable).
- *Default database* controller CDC ClickHouse harus selalu menunjuk DB yang ada (DB test yang di-drop pernah menggagalkan validasi koneksi pool 8 hari kemudian).
- Gunakan `server_id` baru saat resume reader (registrasi binlog lama bisa nyangkut).

---

## 9. Temuan, Risiko & Rekomendasi

### Risiko prioritas tinggi

| # | Risiko | Dampak | Rekomendasi |
|---|---|---|---|
| **R0** | **CPU Data Lake jenuh (97%/16 core) pada window initial-load/CDC-catchup** (idle saat steady-state) | Throughput **onboarding** Phase 2 (SAP/Exware/NMS + ≥20 domain) dibatasi CPU lake; load berat berulang tiap sumber baru, risiko merge backlog & lag CDC selama window load | (a) Rencanakan window onboarding bertahap (hindari load paralel banyak sumber); (b) audit beban merge ReplacingMergeTree (partisi, TTL, `OPTIMIZE`, sorting key); (c) siapkan headroom CPU/scale-out lake untuk window load; (d) pertimbangkan pisah node ingest vs query |
| **R0b** | **Alokasi terbalik: lake (sibuk) 16c/63 GiB + swap 26%, vs mart (idle) 32c/126 GiB** | Memory pressure saat merge → swap → degradasi performa ClickHouse | Set `vm.swappiness≈1`/nonaktifkan swap di host DB; **rebalance CPU+RAM** — tambah ke lake (bronze) yang lebih butuh; mart yang besar+idle dipakai untuk serving silver+gold (§12.3) |
| R1 | **HA semu**: 19 VM klaster berpotensi di atas **1 server fisik 2U** | Kegagalan 1 host = seluruh platform down, walau "klaster 3-node" | Konfirmasi jumlah node fisik. Untuk HA sejati minimal **2 server fisik**; distribusikan replica ClickHouse & Zookeeper quorum lintas host |
| R2 | **Co-location DAP (OLAP) + DB Oneflux (OLTP sumber)** pada satu mesin | Beban analitik & batas baca-dingin sumber saling mengganggu | Pisahkan host sumber dari host analitik; isolasi resource (cgroups/VM pinning) bila tetap satu mesin |
| R3 | **Sizing proposal tidak sinkron dengan realita** | Proposal mematok CH 8 core / 750 GB×3, padahal aktual **lake 16c/3 TB + mart 32c/6 TB**. Estimasi Oneflux production ~486 GB CH (§6.4); sumber PG baru belum diukur. Headroom lake **2 TB** | Pakai proyeksi §6.5 (~1.0–1.5 TB) untuk sizing; ukur actual CH sumber PG baru; revisi CPU lake ke atas untuk window load; tetapkan retensi/TTL |
| R4 | **Batasan sumber (silent-kill 26–43s, baca-dingin 2.5 MB/s)** | Membatasi kecepatan ingestion/CDC; bukan masalah DAP | Eskalasi formal ke pemilik infra Oneflux: naikkan timeout proxy/LB ≥120s; pertimbangkan replica baca khusus |
| R5 | **Secret di environment variable** | Kepatuhan & risiko kebocoran kredensial | Implementasi Vault/KMS + rotasi sebelum produksi company-wide (sudah di roadmap) |

### Risiko menengah
- **R6 — ✓ Resolved:** sumber Oneflux dikonfirmasi **MariaDB** (driver `msodbcsql18` di image Phase 1 untuk sumber lain spt SAP, bukan Oneflux). Tidak ada lagi diskrepansi.
- **R7 — RPO/RTO & jadwal backup belum terdefinisi numerik:** definisikan SLA backup/restore (jadwal, retensi, target restore) sebelum go-live; restore test sudah jadi kriteria penyelesaian Phase 2.
- **R8 — Deprecation Virtualization → Data House** & wajib upgrade Elasticsearch (untuk Explore): masukkan ke rencana upgrade Phase 2.
- **R9 — Launch stampede** (13 thread vs pool 8 slot di sumber): stagger domain kecil atau naikkan pool max-wait.
- **R10 — PII / UU PDP pada data produksi:** data produksi (HR, customer) memuat PII yang tidak ada di QA. Wajib klasifikasi data (4 tingkat) + masking/RLS/CLS **sebelum** production load — ini argumen kuat untuk UAT fungsional pakai QA dulu (§12.2). Bukan sekadar teknis, tapi kepatuhan hukum.

### Rekomendasi cepat (quick wins)
0. **Tuning host data lake & rencanakan window onboarding (R0/R0b)** — set `vm.swappiness≈1`, rebalance RAM (lake 63 GiB ↔ mart 126 GiB), audit beban merge, dan jadwalkan onboarding sumber/domain Phase 2 bertahap agar window load berat tidak menumpuk di 16-core lake.
1. **Validasi sizing terhadap angka benchmark + realita produksi**, bukan sampel Phase 1 — gunakan `run_20260604T084411Z` dan utilisasi CPU/storage aktual (§6.1) sebagai baseline.
2. **Klarifikasi jumlah server fisik & topologi HA** — ini menentukan apakah desain HA Phase 2 nyata atau di atas kertas.
3. **Eskalasikan dua batasan sisi-sumber** ke tim infra Mitratel; keduanya adalah blocker performa yang tidak bisa diatasi DAP.
4. **Definisikan RPO/RTO + jadwal backup numerik** dan jalankan restore test sebagai bagian kriteria penyelesaian.
5. **Pindahkan secret ke Vault/KMS** sebelum rollout company-wide.

---

## 10. Konteks Komersial & Timeline (ringkas)

- **Lisensi platform Phase 2:** Bliv Explore, Perpetual, Tier 4, user/connectivity unlimited — **Rp 3.650.400.000** (Year 1).
- **Jasa profesional:** DG Framework (DMBOK) Rp 1.012.000.000 + Implementasi kebijakan ke platform Rp 1.650.000.000 = **Rp 2.662.000.000**.
- **Durasi:** ±6 bulan (24 minggu), 4 tahap (Fondasi & Baseline → Discovery & Implementasi Kebijakan → Pelaksanaan & Rollout → Support/Go-Live).
- **Scope teknis Phase 2:** ≥20 domain onboard, 4 use case prioritas end-to-end, 11 domain DMBOK, 8 dokumen standar tata kelola.
- **Tim vendor:** 26 orang (Project Director, PM, DG Lead/CDMP, Data Architecture Expert/CDMP, DQ/Catalog Specialist, ~16 Data Engineer/Analyst).

---

## 11. Kriteria Kelayakan Data Lake (Bronze) — 🟡 BAHAN DISKUSI

> **STATUS: belum keputusan final.** Section ini adalah *mark untuk dibahas lebih lanjut* (DG Council / Data Owner / tim teknis). Tujuannya: menetapkan **kriteria objektif** sebelum sebuah sumber/DB di-load ke tier bronze, supaya keputusan "ingest atau tidak" akuntabel dan dapat diaudit (selaras SOW Phase 2 tugas 3 "Registrasi Sumber Data & Load Awal Bronze" dan 4.2 "Registrasi Tabel Bronze").

### 11.1 Prinsip dasar

Bronze = **salinan setia (raw) dari sumber otoritatif yang menopang use case analitik tergovernance**. Konsekuensinya: **tidak semua data yang "ada" otomatis layak masuk bronze.** Ingest yang tidak selektif membebani storage (sisa lake 2 TB), CPU window-load (R0), dan biaya tata kelola (tiap tabel butuh owner, klasifikasi, DQ).

### 11.2 Kriteria kelayakan (checklist usulan)

| # | Kriteria | Pertanyaan kunci |
|---|---|---|
| K1 | **Nilai analitik & keterkaitan use case** | Apakah data menopang salah satu use case prioritas / domain yang disepakati? Atau ingest "karena ada"? |
| K2 | **Otoritas sumber (system of record)** | Apakah ini sumber otoritatif, atau **turunan** dari sumber lain (mis. datamart hasil olahan Oneflux)? Bronze utamanya untuk *raw source*, bukan mart pihak lain. |
| K3 | **Sifat data** | Transaksional **bisnis** vs **telemetri/infra** (Zabbix) vs **metadata orkestrasi** (Airflow) vs **log aplikasi**. Apakah data infra/operasional layak di data lake *bisnis*? |
| K4 | **Rasio nilai vs biaya** | Volume × frekuensi merge × storage × window-load CPU. Mis. Zabbix 953 GB — sebanding nilainya? |
| K5 | **Klasifikasi & sensitivitas** | Ada PII / rahasia? Perlu masking/RLS/CLS? (4 tingkat klasifikasi Phase 2) |
| K6 | **Lingkungan & otoritas data** | **prod vs dev/qa/staging.** Data DEV (mis. `dm_of_dev`) **bukan** otoritatif — kenapa masuk lake produksi? |
| K7 | **Duplikasi / overlap** | Apakah data sudah tercakup sumber lain (mis. datamart turunan Oneflux → duplikasi bronze)? |
| K8 | **Kelayakan CDC & dampak ke sumber** | Mendukung CDC? (PostgreSQL = logical replication, beda dari binlog Oneflux). Beban ke sistem sumber? |
| K9 | **Kebutuhan retensi & arsip** | Berapa lama history diperlukan? Bisa downsample/TTL/tiering (penting untuk Zabbix)? |
| K10 | **Kepemilikan & domain** | Ada Data Owner/Steward + domain terdefinisi? (prasyarat governance Phase 2) |
| K11 | **Stabilitas skema** | Skema sering berubah (lingkungan dev) → churn metadata & schema-drift. |

### 11.3 Penerapan ke sumber saat ini (penilaian awal — UNTUK DIBAHAS)

| Sumber / DB | Catatan & pertanyaan terbuka | Rekomendasi awal (tentatif) |
|---|---|---|
| **Oneflux** (`tab*`) | Sumber bisnis otoritatif (ERP). Jelas layak. | ✅ Ingest (sudah berjalan) |
| **NMS / `zabbix` (953 GB)** | **Telemetri monitoring infra**, bukan data bisnis. Volume dominan. Apakah seluruh history dibutuhkan? Zabbix punya downsampling (trends) sendiri. | ⚠️ **Diskusikan**: ingest **selektif** (metrik/host relevan) + **retensi/TTL ketat**, bukan full 953 GB. Pertimbangkan tiering. |
| **Datamart `dm_of_dev` (14 GB)** | **Lingkungan DEV** + kemungkinan **turunan Oneflux**. Bukan otoritatif (K2, K6). | ❓ **Diskusikan**: kenapa DEV, bukan prod? Risiko duplikasi dengan Oneflux. |
| **Datamart `dm_of_prod` (212 MB)** | Mart produksi — mungkin layak sebagai *reference/curated*, bukan bronze raw. | ❓ Tier mana? (silver/gold reference vs bronze) |
| **Airflow `airflow` (8.6 GB)** | **Metadata orkestrasi** (DAG/task runs). Nilai = observability pipeline/SLA, bukan bisnis. | ❓ **Diskusikan**: untuk monitoring DAP sendiri? domain & owner-nya siapa? |
| **TOBA `toba_production` (19 GB)** | Perlu konfirmasi isi/domain bisnisnya & use case. | ❓ Petakan ke use case sebelum ingest. |
| `log_oneflux` (13 MB, di datamart) | Log aplikasi. | ❓ Nilai analitik rendah — kemungkinan skip. |

### 11.4 Pertanyaan untuk dibahas lebih lanjut (parking lot)

1. **Definisi batas:** apakah DAP data lake hanya untuk **data bisnis**, atau juga **data operasional/infra** (Zabbix, Airflow)? Ini menentukan separuh keputusan di atas.
2. **Zabbix 953 GB:** full-history vs selektif + TTL? Siapa konsumen analitiknya?
3. **`dm_of_dev` (DEV):** disengaja? Kalau bukan, ganti ke prod.
4. **Datamart sebagai sumber bronze:** apakah ini bukan duplikasi dari Oneflux? Mart turunan idealnya bukan bronze.
5. **Tier placement:** apakah mart/reference data (dm_of_prod) lebih tepat di silver/gold sebagai reference, bukan bronze raw?
6. **Owner & domain** tiap sumber baru — prasyarat sebelum registrasi bronze (governance Phase 2).
7. **Kebijakan retensi/TTL & tiering** per sumber — wajib ditetapkan sebelum load (terkait headroom lake 2 TB, §6).

> Output yang diharapkan dari sesi diskusi: **matriks keputusan ingest** (per sumber/DB/tabel) dengan kolom: layak? tier tujuan? owner? klasifikasi? retensi/TTL? — menjadi dasar formal "Registrasi Sumber Data Bronze".

---

## 12. Kesiapan Kapasitas, Keputusan Kickoff/UAT & Strategi Tier

### 12.1 Penilaian kesiapan kapasitas (required vs available)

Proyeksi kebutuhan **bronze** (Oneflux production §6.4 + sumber PG baru §6.3; archive terpisah) vs kapasitas terpasang:

| Resource | Dibutuhkan (proyeksi) | Tersedia | Verdict |
|---|---|---|---|
| **Storage bronze (lake)** | ~1.0–1.5 TB (**tanpa SAP**) | lake 3 TB (sisa ~2 TB) | ✅ **Cukup** untuk muatan awal (asal Zabbix di-TTL) |
| **Volume SAP** | ⏳ belum diestimasi | — | ❓ **Gap** — bisa menggeser verdict bila besar |
| **Storage silver+gold (mart)** | tumbuh per use case (kecil di awal) | mart 6 TB (sisa ~5.5 TB) | ✅ **Sangat cukup** |
| **CPU lake (steady-state)** | <1% | 16 core | ✅ Cukup |
| **CPU lake (window load/onboarding)** | jenuh 97% saat load | 16 core | ⚠️ **Cukup tapi lambat** — onboarding harus bertahap (R0) |
| **RAM lake** | butuh lebih (swap 26%) | 63 GB | ⚠️ **Kurang** — naikkan ke ≥128–256 GB (R0b) |
| **HA / redundansi** | server fisik ke-2 | 1 server (asumsi) | ❌ **Belum** (R1) |
| **Actual CH sumber PG baru** | belum diukur | — | ❓ **Perlu diukur** (faktor inflasi 1.47× Oneflux ≠ tentu sama) |

**Kesimpulan kesiapan:** secara **storage, infrastruktur SIAP** menampung seluruh data production Oneflux + data Mitratel lain. Yang **belum siap** bersifat *operasional & tuning*, bukan kapasitas: RAM lake kurang, window-load CPU lambat, HA belum ada, retensi Zabbix & actual-size sumber baru belum final.

### 12.2 Keputusan: Kickoff/UAT dengan QA atau langsung Production?

> **Rekomendasi: pendekatan HIBRID — bukan salah satu.**

| Aspek UAT | Pakai data | Alasan |
|---|---|---|
| **Fungsional & governance** (RBAC, RLS/CLS, workflow approval, catalog, lineage, DQ rules, dashboard) | **QA** | Cukup & aman; tidak butuh volume besar; ini inti sign-off UAT. **QA bebas PII → tanpa risiko UU PDP (R10) saat uji fitur** |
| **Kapasitas & performa** (initial load, merge, query latency, headroom) | **Production-scale (subset terberat)** | Risiko nyata ada di sini — harus diuji sebelum go-live |

**Saran konkret:**
1. **Jalankan UAT fungsional dengan QA** (sesuai milestone Phase 2) → sign-off governance & fitur.
2. **Paralel, jalankan load-test skala produksi** pada bagian terberat **sebelum** go-live: domain whale `field_operations` (~160 GB / `maintenance_report_child` 230 jt row) + **NMS/Zabbix (953 GB)**. Gunakan tooling benchmark yang sudah ada (`bench_preflight`/`bench_harvest`) untuk dapat angka actual CH + durasi window load, sekaligus **memvalidasi CDC PostgreSQL (logical replication)** sumber baru.
3. **Jangan** langsung full company-wide production load saat kickoff tanpa: (1) RAM lake dinaikkan, (2) retensi/TTL Zabbix, (3) strategi tier (§12.3) disepakati, (4) **backup + restore-test lulus (RPO/RTO, R7)**, (5) **klasifikasi data + masking PII (R10)**.

→ **Verdict:** kickoff boleh jalan; **UAT fungsional = QA**, tetapi **kesiapan produksi divalidasi via load-test terarah**, bukan dengan langsung memuat seluruh produksi tanpa validasi.

### 12.3 Strategi penempatan tier (Bronze / Silver / Gold) — 🟡 USULAN untuk disepakati

Karena penyimpanan **hanya ClickHouse** dengan **dua instance** (Lake & Warehouse-Mart), pilihannya: taruh semua di Lake, atau bagi ke Mart juga.

| Opsi | Penempatan | Penilaian |
|---|---|---|
| A — semua di Lake | bronze+silver+gold di `bliv-data-lake` | ❌ Menumpuk semua beban di 16 core/63 GB (perparah R0); Mart 126 GB idle terbuang |
| **B — split (rekomendasi)** | **bronze (+staging silver) di Lake; silver+gold di Warehouse-Mart** | ✅ Pisahkan ingest/merge (Lake, CPU-bound saat load) dari query-serving (Mart, RAM besar untuk Superset); manfaatkan Mart 126 GB/6 TB yang idle; langsung memitigasi R0 |
| C — bronze+silver di Lake; gold di Mart | curated tetap di Lake | 🟡 Tengah — boleh bila transform silver berat & ingin dekat bronze |

**Rekomendasi: Opsi B.**
- **Bronze → Data Lake** (`bliv-data-lake`): raw `_raw`, append + ReplacingMergeTree, beban merge berat diisolasi di sini.
- **Silver (curated) + Gold (mart/agregat) → Warehouse-Mart** (`clickhouse-warehouse-mart`): melayani query Dashboard/Superset; RAM 2× lebih besar & idle; disk 6 TB untuk tumbuh per use case.
- **Mekanisme pindah antar-instance** perlu didesain: `INSERT INTO mart SELECT FROM remote(lake,…)` atau job NiFi yang re-materialisasi bronze→silver→gold. (Catatan: keduanya instance ClickHouse terpisah, bukan satu cluster — perlu konfirmasi apakah ada konektivitas/`remote()` antar keduanya.)

**Hal yang perlu disepakati:** (a) silver di Lake atau Mart; (b) mekanisme propagasi antar-instance; (c) apakah perlu instance/cluster ketiga bila beban serving tumbuh; (d) penempatan reference/master data.

---

*Dokumen ini bersifat analisis turunan dari materi `references/`. Angka teknis dikutip apa adanya dari sumber; di mana sumber tidak menyediakan data (mis. RPO/RTO, jumlah node fisik), hal itu ditandai sebagai gap yang perlu dikonfirmasi, bukan diasumsikan. Section 11 & 12.3 berstatus usulan/bahan diskusi, bukan keputusan final.*
