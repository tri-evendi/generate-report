# Analisis Infrastruktur — Data Analytics Platform (DAP) Mitratel

**Disusun:** 2026-06-08 · **Penyusun:** Tim Analisis (TAN Digital)
**Klien:** PT Dayamitra Telekomunikasi Tbk. (Mitratel / MTEL)
**Vendor platform:** PT Bangunindo Teknusa Jaya (BTJ) — produk **Bliv** (di-branding **DAP**)

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
| Skala data | 2 DB, 2 tabel, **308K row, 16.5 MB** (sampel) | **1.354 tabel, 13 domain, 336.5 juta row, 255 GiB** (Oneflux bronze, **QA, sebagian tabel di-exclude** — prod+archive ⏳ menyusul) | ≥20 domain onboard, company-wide |
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

Engine backend menggunakan Python 3.11 (image `slim-bookworm`) dengan **ODBC driver `msodbcsql18`** terpasang — mengindikasikan jalur koneksi ke **SQL Server**. Frontend Node 20-alpine, error tracking **Sentry**, API ter-dokumentasi via Swagger/OpenAPI.

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
| **Oneflux** (Frappe/ERPNext, tabel `tab*`) | **Terintegrasi penuh** (bronze) | JDBC cursor-fetch + CDC GTID | 1.354 tabel, 13 domain |
| SAP | Direncanakan | (mention di latar belakang) | Batasan Phase 2 |
| Exware | Direncanakan | — | Batasan Phase 2 |
| NMS | Direncanakan | — | Batasan Phase 2 |

> **Temuan diskrepansi sumber:** Laporan Phase 1 memasang ODBC **SQL Server** (`msodbcsql18`) untuk "Oneflux Archive", sedangkan benchmark Juni secara konsisten menyebut sumber sebagai **MariaDB** (skema Frappe `tabXXX`). Kemungkinan ada >1 sumber Oneflux (Archive di MSSQL, operasional di MariaDB) atau perubahan sumber. **Perlu diklarifikasi** karena memengaruhi driver, tuning koneksi, dan strategi CDC.

Metode integrasi yang didukung platform: **JDBC connection pool, REST API, SFTP, HTTP/S, dan CDC (binlog GTID)**. Pemisahan environment (dev/staging/prod) via NiFi **Parameter Context** (tanpa hardcode kredensial).

---

## 6. Volume Data, Skala & Performa

### 6.1 Kondisi produksi ClickHouse terkini (Grafana node_exporter, 2026-06-08 14:2x, steady-state CDC)

Snapshot diambil saat **CDC sudah steady-state** (initial load selesai). Angka stat-panel (tidak ambigu):

| Instance (host) | Core | RAM | Disk (RootFS) | CPU now | RAM used | SWAP used | Disk used (≈) | Uptime |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **Data Lake** — `bliv-data-lake:9100` (bronze) | **16** | **63 GiB** | 3 TiB | 0.7% | 10.8% (~6.8 GiB) | **26.4% (~2.1 GiB)** | 33.2% (**~1.0 TB**) | 25.4 mgg |
| **Warehouse-Mart** — `clickhouse-warehouse-mart:9100` (gold/mart) | n/a* | **126 GiB** | 6 TiB | 0.5% | 3.3% (~4 GiB) | 0.0% | 5.6% (**~0.34 TB**) | 25.4 mgg |

*\*jumlah core warehouse terpotong di screenshot.*

**Pembacaan analitis:**

- **CPU bukan bottleneck steady-state — tapi bottleneck saat load/onboarding.** Saat snapshot, kedua instance nyaris idle (0.7% / 0.5%). Lonjakan **97% pada 16 core** terukur saat **initial load + CDC catch-up** berjalan (workload merge ReplacingMergeTree + write CDC masif). Ini cocok dengan benchmark: initial load = 16.1 jam berat, steady-state CDC = "pulsa 1–2 menit". → **Sizing CPU lake harus ditentukan oleh window onboarding, bukan steady-state.** Tiap sumber/domain baru di Phase 2 memicu ulang window berat ini.
- **🔴 SWAP 26.4% (~2.1 GiB) di data lake meski RAM 89% bebas = sinyal memory pressure historis.** Halaman ter-swap saat merge/load berat (RAM 63 GiB sempat ketat) dan tidak ditarik kembali. **Untuk host database, swap pada ClickHouse berbahaya bagi performa** — rekomendasi: turunkan `vm.swappiness` (≈1) atau nonaktifkan swap, dan pertimbangkan tambah RAM lake.
- **🔴 Asimetri RAM yang janggal:** layer **bronze yang berat hanya 63 GiB RAM**, sedangkan **warehouse/mart yang nyaris idle justru 126 GiB** (2×). Alokasi RAM tampak terbalik dari profil beban. Tinjau ulang — bronze (merge + ingest) lebih butuh RAM daripada mart yang santai.
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

### 6.3 Gap volume Production & Archive (menunggu data Mitratel)

Benchmark = QA dengan tabel sebagian di-exclude. Untuk sizing yang sah, dibutuhkan estimasi berikut (akan disediakan Mitratel) agar §3.2 (sizing VM) dan §6.1 (headroom storage) bisa divalidasi:

| Input yang dibutuhkan | Kegunaan | Status |
|---|---|---|
| Volume **production** (row & GiB) per domain + tabel di-exclude | Skala bronze sebenarnya vs QA 255 GiB | ⏳ menunggu |
| Volume **data archive** (row & GiB) | Kebutuhan storage jangka panjang + kebijakan retensi/TTL | ⏳ menunggu |
| Faktor pertumbuhan bulanan/tahunan | Proyeksi kapasitas 12–24 bulan | ⏳ menunggu |
| Estimasi volume sumber baru (SAP, Exware, NMS) | Beban onboarding tambahan + window load | ⏳ menunggu |

> Setelah angka tersedia: kalikan terhadap baseline QA untuk dapat faktor skala, lalu re-cek apakah **lake 3 TB (sisa 2 TB)** dan **CPU 16-core** masih cukup. Headroom 2 TB di lake **kemungkinan besar tidak memadai** untuk production + archive penuh tanpa retensi/TTL atau penambahan disk.

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
| **R0b** | **SWAP 26.4% & RAM lake hanya 63 GiB (vs mart 126 GiB)** | Memory pressure saat merge → swap → degradasi performa ClickHouse | Set `vm.swappiness≈1`/nonaktifkan swap di host DB; **rebalance RAM** — tambah RAM ke lake (bronze) yang lebih butuh, kaji ulang 126 GiB di mart yang idle |
| R1 | **HA semu**: 19 VM klaster berpotensi di atas **1 server fisik 2U** | Kegagalan 1 host = seluruh platform down, walau "klaster 3-node" | Konfirmasi jumlah node fisik. Untuk HA sejati minimal **2 server fisik**; distribusikan replica ClickHouse & Zookeeper quorum lintas host |
| R2 | **Co-location DAP (OLAP) + DB Oneflux (OLTP sumber)** pada satu mesin | Beban analitik & batas baca-dingin sumber saling mengganggu | Pisahkan host sumber dari host analitik; isolasi resource (cgroups/VM pinning) bila tetap satu mesin |
| R3 | **Sizing proposal tidak sinkron dengan realita + baseline masih QA** | Proposal mematok CH 8 core / 750 GB×3, padahal aktual **16 core / lake 3 TB / warehouse 6 TB**. Benchmark 255 GiB = **QA dengan tabel di-exclude**; prod+archive akan lebih besar (⏳ §6.3). Headroom lake tinggal **2 TB** | Tunda finalisasi sizing sampai estimasi prod+archive masuk (§6.3); revisi CPU lake ke atas untuk window load; tetapkan retensi/TTL; uji ulang `bench_harvest` tiap perubahan infra |
| R4 | **Batasan sumber (silent-kill 26–43s, baca-dingin 2.5 MB/s)** | Membatasi kecepatan ingestion/CDC; bukan masalah DAP | Eskalasi formal ke pemilik infra Oneflux: naikkan timeout proxy/LB ≥120s; pertimbangkan replica baca khusus |
| R5 | **Secret di environment variable** | Kepatuhan & risiko kebocoran kredensial | Implementasi Vault/KMS + rotasi sebelum produksi company-wide (sudah di roadmap) |

### Risiko menengah
- **R6 — Diskrepansi sumber (MSSQL vs MariaDB):** klarifikasi topologi sumber Oneflux; berdampak ke driver & strategi CDC.
- **R7 — RPO/RTO & jadwal backup belum terdefinisi numerik:** definisikan SLA backup/restore (jadwal, retensi, target restore) sebelum go-live; restore test sudah jadi kriteria penyelesaian Phase 2.
- **R8 — Deprecation Virtualization → Data House** & wajib upgrade Elasticsearch (untuk Explore): masukkan ke rencana upgrade Phase 2.
- **R9 — Launch stampede** (13 thread vs pool 8 slot di sumber): stagger domain kecil atau naikkan pool max-wait.

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

*Dokumen ini bersifat analisis turunan dari materi `references/`. Angka teknis dikutip apa adanya dari sumber; di mana sumber tidak menyediakan data (mis. RPO/RTO, jumlah node fisik), hal itu ditandai sebagai gap yang perlu dikonfirmasi, bukan diasumsikan.*
