# Report Kesiapan Infrastruktur DAP — Penilaian Kapasitas & Keputusan Kickoff/UAT

**Klien:** PT Dayamitra Telekomunikasi Tbk. (Mitratel) · **Platform:** DAP (Bliv) oleh PT Bangunindo Teknusa Jaya
**Disusun:** 2026-06-09 · **Penyusun:** Tim Analisis (TAN Digital)
**Dokumen pendukung:** `analisis-infrastruktur-dap.md` (analisis teknis penuh) · `eskalasi-kapasitas-produksi-dap.md` (ceiling & eskalasi)

---

## 1. Overview

### 1.1 Tujuan
Menilai apakah **kapasitas infrastruktur DAP saat ini cukup dan siap** untuk menampung **data production Oneflux beserta seluruh data Mitratel** (TOBA, NMS, Datamart, Airflow, dan SAP), dan atas dasar itu **menentukan apakah kickoff/UAT dijalankan dengan data QA terlebih dahulu, atau langsung ke data production**.

### 1.2 Disclaimer arsitektur
Saat ini tim **baru memakai datalakehouse** (instance ClickHouse **Data Lake**) untuk menyimpan data. Instance **data warehouse (Warehouse-Mart) belum sempat dipakai dan baru akan dipakai** (karena itu masih idle, §2.2). Maka report ini menetapkan **strategi penempatan tier bronze/silver/gold** — termasuk mulai memanfaatkan data warehouse untuk silver/gold (§4).

> 📋 **Daftar lengkap asumsi & hal yang belum pasti ada di §7.** Verdict dalam report ini berlaku dalam batas asumsi tersebut.

### 1.3 Kesimpulan utama (ringkas)
- **Kapasitas storage: SIAP.** Proyeksi total data bronze (Oneflux **production** + 4 sumber baru; archive dikelola terpisah) ≈ **1.0–1.5 TB**, sedangkan data lake punya **sisa ~2 TB** dan warehouse-mart **~5.5 TB**.
- **Yang belum siap bersifat operasional, bukan kapasitas:** RAM lake kurang (sempat swap), CPU jenuh saat *window load*, HA belum ada, retensi NMS/Zabbix belum ditetapkan.
- **Keputusan kickoff/UAT: pendekatan HIBRID** — UAT **fungsional & governance pakai QA**; **kesiapan produksi divalidasi lewat load-test terarah** pada data terberat, sebelum go-live penuh.

---

## 2. Infrastruktur Saat Ini

### 2.1 Server fisik (bare-metal, 1 unit)
| Resource | Spesifikasi |
|---|---|
| CPU | 2× AMD EPYC 9655 — **192 core / 384 thread** |
| RAM | 16× 64 GB DDR5-6400 = **1 TB** |
| Storage data | 8× 3.2 TB NVMe = **25.6 TB raw** (level RAID belum dikonfirmasi → usable 12.8–22.4 TB) |
| Jaringan / form factor | 25 GbE · Rack 2U · dual PSU |

> Catatan: indikasi **satu server fisik** menampung seluruh stack → "klaster" Phase 2 bersifat logis, **belum HA perangkat keras** (perlu konfirmasi & idealnya server ke-2).

### 2.2 Penyimpanan ClickHouse (2 instance) — kondisi steady-state 2026-06-09
| Instance | Peran | Core | RAM | Disk | Terpakai | CPU now | Swap |
|---|---|---:|---:|---:|---:|---:|---:|
| **Data Lake** (`bliv-data-lake`) | Bronze (raw) | 16 | 63 GiB | 3 TiB | ~1.0 TB | 0.7% | ⚠️ 26.4% |
| **Warehouse-Mart** (`clickhouse-warehouse-mart`) | Gold/Mart (disiapkan, **belum dipakai**) | 32 | 126 GiB | 6 TiB | ~0.34 TB | 0.5% | 0.0% |

**Observasi penting:**
- CPU lake **idle saat steady-state (0.7%)**, tetapi **jenuh ~97% saat initial-load/CDC-catchup** (workload merge berat).
- **Swap 26.4% di lake** meski RAM 89% bebas → tekanan memori historis saat merge; RAM 63 GiB **terlalu kecil**.
- **Asimetri:** lake (sibuk) = 16 core/63 GiB, sedangkan mart (idle) = 32 core/126 GiB — **2× di kedua dimensi**, alokasi terbalik dari profil beban. (Strategi tier §4 memanfaatkan mart besar+idle ini untuk serving.)
- **~750 GB belum terjelaskan di lake:** disk terpakai ~1.0 TB padahal data QA Oneflux hanya 255 GB. Selisih kemungkinan parts belum ter-merge / log / data lain — **perlu diinvestigasi** (sebagian mungkin reklaimabel via `OPTIMIZE`). Saat produksi nanti **menggantikan** QA (load fresh, bukan akumulasi), perhitungkan **end-state**: lake ≈ ~1.0–1.5 TB total, bukan 1.0 TB + 1.5 TB.

### 2.3 Komponen pendukung
Ingest **NiFi (Pipeline)** → ClickHouse; serving via **Superset (Dashboard)**; IAM **Keycloak (OIDC)**; observability **Grafana/Prometheus**; orkestrasi deploy **Ansible**. CDC Oneflux memakai **binlog GTID + ReplacingMergeTree** (recovery teruji, 0 data loss).

---

## 3. Data yang Akan Di-ingest

### 3.1 Oneflux (sumber utama existing) — dari `DB Size Compare.xlsx`
**1.354 tabel — scope yang sudah DIFILTER (in-scope).** Tabel sistem/temp/log Frappe (1.035 tabel, prod ~1.52 TB) sudah **dikecualikan**; tanpa filter Oneflux prod ~1.85 TB sumber (→ ~2.7 TB CH, tidak muat). **Filter inilah yang membuat Oneflux muat.** Estimasi ukuran sumber & **actual** terukur di ClickHouse (QA):

| Skenario | Estimasi sumber | Proyeksi actual ClickHouse (×1.47) |
|---|---:|---:|
| QA (terukur) | 173.47 GB | **255 GB** (terukur ✓) |
| **Production** | 330.53 GB | **~486 GB (~0.47 TB)** |
| Archive (terpisah, tidak digabung) | 100.34 GB | ~148 GB (~0.14 TB) |

> **🔑 Faktor inflasi ClickHouse = 1.47× (khusus tier BRONZE).** Data bronze `_raw` terukur **1.47× lebih besar** dari estimasi byte sumber (kolom metadata `_ingested_at`/`_batch_id`, tipe String verbatim, versi ReplacingMergeTree belum ter-merge) — **bukan terkompresi**.
> - Sebagian inflasi (parts duplikat belum ter-merge) **dapat berkurang setelah `OPTIMIZE FINAL`** → 1.47× adalah angka **konservatif (aman)** untuk planning, actual steady-state bisa lebih kecil.
> - **Tier silver/gold sebaliknya akan TER-KOMPRES** (kolom bertipe + codec ZSTD + `ORDER BY` yang tepat) — **jangan terapkan 1.47× ke silver/gold**; sizing mart dihitung terpisah.

Domain terbesar (prod): `field_operations` ~160 GB · `partner_data_exchange` ~70 GB · `site_estate` ~60 GB.

**Pertumbuhan (full DB, semua tabel):** ~1%/bulan steady-state (Feb–Mei 2026); full DB Mei 2026 ~1.85 TB — **cocok dengan total Excel** (cross-validation ✓). Volatilitas ekstrem (Des −72.77%, Feb +361%) terjadi di tabel temp/excluded, bukan data bisnis in-scope. → Pertumbuhan Oneflux in-scope **bukan risiko kapasitas jangka pendek**.

### 3.2 Sumber data Mitratel lainnya (PostgreSQL/Timescale) — terukur 2026-06-09
| Sumber | DB diambil | Ukuran sumber | Est. actual CH* |
|---|---|---:|---:|
| TOBA (Timescale) | `toba_production` | 19 GB | ~ perlu diukur |
| **NMS** (Zabbix) | `zabbix` (+grafana) | **~953 GB** | ~ perlu diukur (dominan) |
| Datamart OF | `dm_of_dev` + `dm_of_prod` | ~14.2 GB | ~ perlu diukur |
| Airflow prod | `airflow` | ~8.6 GB | ~ perlu diukur |
| **SAP** | (TBD) | ⏳ **belum diestimasi** | ⏳ **belum diestimasi** |
| **Total tambahan (tanpa SAP)** | | **~995 GB (~1 TB)** | **~0.5–1.0 TB** |

*\*Catatan: faktor 1.47× berlaku untuk Oneflux (MariaDB). Sumber PG perlu **diukur langsung**, jangan diasumsikan terkompresi. NMS/Zabbix (953 GB) mendominasi dan menuntut keputusan retensi/TTL. **Laju pertumbuhan keempat sumber ini belum diketahui** (hanya Oneflux yang terukur ~1%/bln) — perlu data dari Mitratel untuk proyeksi jangka panjang.*

> Connector: keempat sumber baru = **PostgreSQL** → CDC pakai **logical replication** (beda dari binlog Oneflux) — perlu validasi di NiFi.
>
> Kelayakan tiap sumber masuk bronze (mis. Zabbix telemetri infra? `dm_of_dev` lingkungan DEV?) dibahas di `analisis-infrastruktur-dap.md` §11 (bahan diskusi).

### 3.3 Proyeksi konsolidasi data lake
| Komponen bronze | Proyeksi actual ClickHouse |
|---|---:|
| Oneflux **production** | ~0.47 TB |
| Sumber PG baru | ~0.5–1.0 TB |
| **SAP** | ⏳ belum diestimasi (TBD) |
| **TOTAL (tanpa SAP)** | **~1.0–1.5 TB** |

*(Archive Oneflux ~0.14 TB dikelola terpisah, tidak digabung. **SAP belum masuk** — volume belum ada; bila besar bisa menggeser verdict.)*

---

## 4. Strategi Penempatan Tier (Bronze / Silver / Gold)

Karena penyimpanan **hanya ClickHouse** dengan **2 instance**, pilihannya: semua di Lake, atau dibagi ke Mart.

| Opsi | Penempatan | Penilaian |
|---|---|---|
| A — semua di Lake | bronze+silver+gold di Data Lake | ❌ Menumpuk beban di 16 core/63 GB; Mart 126 GB idle terbuang |
| **B — split (REKOMENDASI)** | **bronze di Lake; silver+gold di Warehouse-Mart** | ✅ Pisah ingest/merge (Lake) dari query-serving (Mart); manfaatkan RAM & disk Mart yang idle; mitigasi langsung kejenuhan CPU lake |
| C — hybrid | bronze+silver di Lake; gold di Mart | 🟡 Opsi tengah bila transform silver berat |

**Rekomendasi: Opsi B** — sekaligus **mulai mengaktifkan data warehouse** yang selama ini belum dipakai.
- **Bronze → Data Lake** (`bliv-data-lake`): raw `_raw`, append + ReplacingMergeTree; beban merge berat **diisolasi** di sini.
- **Silver (curated) + Gold (mart/agregat) → Warehouse-Mart** (`clickhouse-warehouse-mart`): melayani Dashboard/Superset; RAM 2× lebih besar & belum terpakai; 6 TB untuk tumbuh per use case. Mengaktifkannya = memanfaatkan kapasitas yang kini menganggur.

**Yang perlu didesain/disepakati:**
- Mekanisme propagasi antar-instance (`INSERT … SELECT FROM remote(lake,…)` atau job NiFi) — **konfirmasi konektivitas Lake↔Mart** (dua instance terpisah, bukan satu cluster).
- Silver ditempatkan di Lake atau Mart.
- Penempatan reference/master data.
- Kebijakan retensi/TTL & arsip (kritis untuk NMS/Zabbix).

---

## 5. Keputusan: Data QA Dulu atau Production Langsung?

### 5.1 Penilaian kesiapan
| Aspek | Status | Keterangan |
|---|:---:|---|
| Storage bronze (lake) | ✅ Cukup | ~1.0–1.5 TB (**tanpa SAP**) vs sisa ~2 TB (asal Zabbix di-TTL) |
| Volume SAP | ❓ Gap | Belum diestimasi — bisa menggeser verdict bila besar |
| Storage silver+gold (mart) | ✅ Sangat cukup | sisa ~5.5 TB |
| CPU steady-state | ✅ Cukup | <1% |
| CPU window-load | ⚠️ Lambat | jenuh 97% saat load → onboarding bertahap |
| RAM lake | ⚠️ Kurang | 63 GB + swap → naikkan ≥128–256 GB |
| HA / redundansi | ❌ Belum | 1 server fisik (asumsi) |
| **Backup & DR (RPO/RTO)** | ❓ Belum terdefinisi | Kriteria penyelesaian Phase 2 wajib restore-test; jadwal/RPO/RTO belum ada |
| **Klasifikasi & PII (UU PDP)** | ⚠️ Wajib untuk produksi | Data produksi memuat PII (HR/customer) → perlu klasifikasi + masking/RLS sebelum load; QA jauh lebih aman |
| Actual size sumber PG baru | ❓ Belum diukur | terutama Zabbix |
| Retensi/TTL Zabbix | ❓ Belum ditetapkan | penentu headroom |

**Kapasitas storage = SIAP. Hambatan tersisa bersifat operasional/tuning, bukan kapasitas.**

> ⚠️ **Asumsi kritis:** verdict "cukup" berlaku **selama filter tabel Oneflux tetap diterapkan** (in-scope 1.354 tabel). Bila tabel sistem/temp/log yang dikecualikan (~1.52 TB prod) ikut di-ingest, lake **tidak akan muat**. Filter ini wajib dijaga sebagai bagian dari registrasi bronze.

### 5.2 Rekomendasi — **HIBRID (bukan salah satu)**

| Tujuan UAT | Data dipakai | Alasan |
|---|---|---|
| **Fungsional & governance** (RBAC, RLS/CLS, workflow, catalog, lineage, DQ rules, dashboard) | **QA** | Cukup, aman, cepat; inti sign-off UAT. **QA tidak memuat PII produksi → bebas risiko UU PDP saat uji fitur** |
| **Kapasitas & performa** (initial load, merge, query latency, headroom) | **Production-scale (subset terberat)** | Risiko nyata di sini — wajib diuji sebelum go-live |

**Langkah disarankan:**
1. **Kickoff & UAT fungsional dengan data QA** → sign-off governance & fitur (sesuai milestone Phase 2); aman dari sisi PII.
2. **Paralel: load-test skala produksi** pada bagian terberat — domain `field_operations` (whale, `maintenance_report_child` 230 jt row) + **NMS/Zabbix (953 GB)** — pakai tooling benchmark yang sudah ada (`bench_preflight`/`bench_harvest`) untuk dapat **actual size CH + durasi window load**, serta **memvalidasi CDC PostgreSQL (logical replication)** untuk sumber baru.
3. **Prasyarat sebelum full production load:** (a) RAM lake dinaikkan + tuning swap; (b) retensi/TTL Zabbix ditetapkan; (c) strategi tier (§4) disepakati; (d) keputusan HA/server ke-2; (e) **backup + restore-test lulus (RPO/RTO)**; (f) **klasifikasi data & masking PII** untuk data produksi.

### 5.3 Verdict
> **Kickoff boleh berjalan. UAT fungsional dijalankan dengan data QA; kesiapan produksi divalidasi melalui load-test terarah pada data terberat — bukan dengan langsung memuat seluruh data production tanpa validasi.** Setelah load-test + 6 prasyarat di §5.2 terpenuhi, baru lanjut ke production load penuh.

### 5.4 Transisi QA → Production (setelah UAT)

**Pertanyaan: setelah UAT pakai QA, saat production mau di-ingest — apakah QA harus dihapus dulu?**

**Ya — data QA harus dibersihkan dari tabel bronze sebelum/saat load production**, karena keduanya memakai **tabel yang sama** (skema Oneflux `*_raw`). Mencampur baris QA + production di tabel bronze yang sama = data tidak valid (ReplacingMergeTree **tidak** men-dedupe lintas-environment karena kuncinya beda). Dua pendekatan bersih:

| Opsi | Cara | Trade-off |
|---|---|---|
| **1 — Namespace terpisah (REKOMENDASI)** | Load production ke DB terpisah (mis. `bronze_dap_oneflux_<domain>` utk prod, `..._qa` utk QA). QA tetap utuh selama validasi paralel; **drop QA setelah produksi tervalidasi**. | ✅ Tanpa downtime, mudah rollback. Sementara butuh ruang untuk keduanya (QA ~255 GB + prod ~486 GB ≈ 740 GB < sisa 2 TB → muat). |
| **2 — Truncate & reload (cutover)** | Ikuti protokol terbukti (benchmark): stop CDC → catat GTID anchor → **TRUNCATE bronze** → load production → resume CDC. | ⚠️ Bronze kosong selama reload (~window 16 jam Oneflux) → downtime. Cocok bila QA tak diperlukan lagi. |

**Beralih QA→prod bukan sekadar hapus data — juga:**
- **Repoint connector** NiFi (DBCPConnectionPool) dari sumber QA ke sumber **production** (server/DB berbeda).
- **CDC fresh:** GTID anchor baru + `server_id` reader baru (hindari "slave with same server_id"); verifikasi sink pool **sebelum** start reader (runbook benchmark).
- **Pastikan schema match** (bila skema prod beda dari QA, sesuaikan transformasi) + validasi count pre/post-load.
- **PII:** data production memuat PII → masking/klasifikasi harus aktif **sebelum** load (§6 / P5), tidak berlaku di QA.

**Rekomendasi: Opsi 1** selama window validasi (aman, tanpa downtime), lalu **drop QA** setelah produksi tervalidasi. Pakai Opsi 2 hanya bila ingin cutover sederhana & QA sudah tidak dibutuhkan.

---

## 6. Risiko & Prasyarat (aksi yang harus dilakukan)

*Hal yang harus **dikerjakan tim**. Hal yang harus **dikonfirmasi/diukur** (server count, RAID, volume SAP, growth, dst.) ada di §7.*

| Kode | Hal | Aksi |
|---|---|---|
| **R0** | CPU lake jenuh saat window load | Onboarding bertahap; audit merge; siapkan headroom CPU |
| **R0b** | RAM lake 63 GB + swap 26% | Naikkan RAM ≥128–256 GB; `vm.swappiness≈1` |
| **R1** | HA semu (1 server fisik) | Konfirmasi jumlah server; sediakan server ke-2 |
| **P1** | Kelayakan bronze tiap sumber belum diputuskan (Zabbix telemetri? `dm_of_dev` DEV? Airflow metadata?) | Putuskan via §11 (analisis) sebelum registrasi bronze |
| **P2** | Filter tabel Oneflux wajib dijaga | Enforce di registrasi bronze — tanpa filter (~1.52 TB excluded ikut) lake tidak muat |
| **P3** | Retensi/TTL (terutama Zabbix) belum ada | Tetapkan sebelum load |
| **P4** | Backup & DR (RPO/RTO) belum terdefinisi | Tetapkan jadwal + lulus restore-test (kriteria Phase 2) |
| **P5** | PII / UU PDP pada data produksi | Klasifikasi data + masking/RLS sebelum production load |

---

## 7. Disclaimer, Asumsi & Hal yang Belum Pasti

Verdict di report ini **berlaku dalam batas asumsi berikut**. Item ⏳/❓ harus dikonfirmasi sebelum keputusan final / production load.

### 7.1 Infrastruktur
| Hal | Status / asumsi | Dampak bila berbeda |
|---|---|---|
| **Jumlah server fisik** | Diasumsikan **1 unit** | Bila benar 1 → HA hanya logis (bukan HW); butuh server ke-2 |
| **Level RAID** array NVMe | Belum dikonfirmasi (asumsi usable 12.8–22.4 TB) | RAID10 → headroom fisik jauh lebih kecil |
| **DB sumber Oneflux co-located?** | Belum dikonfirmasi | Bila ya → CPU/RAM/IO bersaing dengan DAP |
| **Konektivitas Lake↔Mart** | Belum dikonfirmasi (asumsi bisa `remote()`/NiFi) | Menentukan kelayakan strategi tier Opsi B (§4) |
| **~750 GB lake belum terjelaskan** | Disk ~1.0 TB terpakai vs data QA 255 GB | Perlu investigasi; sebagian mungkin reklaimabel via `OPTIMIZE` |

### 7.2 Data & volume
| Hal | Status / asumsi | Dampak |
|---|---|---|
| **Faktor inflasi 1.47×** | Diturunkan dari **QA Oneflux (MariaDB)**, diasumsikan berlaku untuk production (profil tabel serupa) — **konservatif** | Bila profil prod beda, actual bisa lebih kecil/besar |
| **Actual CH sumber PG baru** | **Belum diukur** (1.47× belum tentu berlaku) | Zabbix (953 GB) bisa lebih kompresibel; harus diukur via load-test |
| **Laju pertumbuhan TOBA/NMS/Datamart/Airflow** | **Belum diketahui** (hanya Oneflux ~1%/bln) | Proyeksi jangka panjang belum bisa final |
| **Volume SAP** | **Belum diestimasi** — belum masuk total ~1.0–1.5 TB | Bila besar (ERP/HANA) → bisa membuat lake tidak cukup |
| **QA vs prod = instance sumber terpisah?** | Diasumsikan ya (untuk transisi §5.4) | Menentukan cara repoint connector & cutover |
| **Satuan unit** | Campur biner (GiB/TiB) & desimal (GB/MB); pembulatan ±5% | Untuk sizing final pakai angka MB/GB mentah |

*(Hal governance/operasional yang harus ditetapkan — kelayakan bronze, filter, retensi, backup, PII — ada di §6 sebagai prasyarat P1–P5.)*

---

## 8. Langkah Selanjutnya (checklist berurut)

**Fase 0 — Konfirmasi input (dari Mitratel/Infra) [→ §7]**
- ☐ Konfirmasi **jumlah server fisik** & **level RAID** array NVMe
- ☐ **Estimasi volume SAP** + **laju pertumbuhan** TOBA/NMS/Datamart/Airflow
- ☐ Konfirmasi **konektivitas Lake↔Mart** & co-location Oneflux (driver sumber Oneflux = **MariaDB**, sudah dikonfirmasi)

**Fase 1 — Kickoff & UAT fungsional (data QA)**
- ☐ UAT fungsional & governance dengan **data QA** → sign-off (aman dari PII)
- ☐ Putuskan **kelayakan bronze tiap sumber** (P1, §11) + tetapkan **filter** (P2)

**Fase 2 — Load-test produksi (paralel dengan Fase 1)**
- ☐ Load-test **`field_operations` (whale) + NMS/Zabbix** → ukur **actual size CH** + durasi window load
- ☐ **Validasi CDC PostgreSQL** (logical replication) untuk sumber baru

**Fase 3 — Prasyarat sebelum production load [→ §6]**
- ☐ Naikkan **RAM lake** + tuning swap (R0b) · siapkan headroom CPU (R0)
- ☐ Tetapkan **retensi/TTL** Zabbix (P3)
- ☐ Sepakati **strategi tier (Opsi B)** + **aktifkan data warehouse** (§4)
- ☐ **Backup + restore-test** lulus / RPO-RTO (P4)
- ☐ **Klasifikasi data + masking PII** (P5)
- ☐ Keputusan **HA / server ke-2** (R1)

**Fase 4 — Cutover ke production**
- ☐ Transisi **QA→prod** (Opsi 1: namespace terpisah; drop QA setelah tervalidasi) [§5.4]
- ☐ Repoint connector + **CDC fresh** (GTID anchor + `server_id` baru) + validasi count pre/post-load

> Urutan ini menjaga: kickoff bisa jalan (Fase 1) tanpa menunggu semua jawaban, sambil risiko produksi divalidasi paralel (Fase 2), dan production load penuh baru dilakukan setelah Fase 3 tuntas.

---

*Report ini adalah sintesis dari analisis teknis (`analisis-infrastruktur-dap.md`) dan eskalasi kapasitas (`eskalasi-kapasitas-produksi-dap.md`). Angka actual ClickHouse Oneflux & faktor inflasi 1.47× bersumber dari `DB Size Compare.xlsx`. Item bertanda ⏳/❓/🟡 adalah keputusan/asumsi yang masih perlu dikonfirmasi.*

*Catatan unit: angka sumber memakai satuan campur (Grafana = GiB/TiB biner; Excel/`pg_size_pretty` = MB/GB). Pembulatan TB di report ini bersifat indikatif (±5%); untuk sizing final pakai angka MB/GB mentah dari sumber.*
