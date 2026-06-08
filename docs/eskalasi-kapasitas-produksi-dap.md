# ESKALASI — Batas Kapasitas Infrastruktur Produksi DAP

**Tanggal:** 2026-06-08 · **Dari:** Tim Analisis (TAN Digital) · **Kepada:** Tim Infrastruktur / IT Mitratel & PT Bangunindo Teknusa Jaya
**Lampiran:** `docs/analisis-infrastruktur-dap.md` · **Status:** menunggu konfirmasi (lihat §6)

---

## 1. Ringkasan & keputusan yang diminta

Server fisik DAP saat ini **sangat besar secara compute & memory**, tetapi **storage adalah satu-satunya batas keras** untuk volume data produksi + archive. Dengan spesifikasi sekarang:

- **CPU & RAM bukan kendala kapasitas** — masih tersisa sangat banyak (≈80% idle). Data Lake bisa dinaikkan dari 16 → puluhan core dalam box yang sama.
- **Storage adalah ceiling.** Dari 25.6 TB NVMe mentah, kapasitas *usable* bergantung level RAID (12.8–22.4 TB). **9 TiB sudah dialokasikan** (lake 3 + warehouse 6). Sisa untuk pertumbuhan = **2–11 TB**, sangat sensitif terhadap level RAID.
- **Baseline volume yang kita punya baru QA (255 GiB).** Production + archive (⏳ menyusul) akan jauh lebih besar.

**Yang diputuskan/dikonfirmasi (detail §6):** level RAID array NVMe, jumlah server fisik (HA), apakah DB sumber Oneflux co-located, target replication factor ClickHouse, dan estimasi volume production + archive.

---

## 2. Pemicu eskalasi

1. **Headroom storage lake tinggal ~2 TB** (3 TB dialokasikan, ~1 TB terpakai di QA) — sebelum production + archive penuh dimuat.
2. **CPU lake jenuh 97% saat window initial-load/CDC-catchup** (16 core) — tiap onboarding sumber/domain Phase 2 memicu ulang beban ini.
3. **Sizing proposal Phase 2 (CH 8 core / 750 GB×3) tidak cocok** dengan realita terpasang (16 core / 3 TB lake / 6 TB warehouse) maupun dengan beban produksi yang belum dipetakan.
4. **Indikasi single physical server** → klaster multi-node bersifat logis, bukan HA fisik.

---

## 3. Spesifikasi terpasang saat ini

### 3.1 Server fisik (WhatsApp image 2026-05-21, "1 DAP Server")

| Resource | Spesifikasi | Total |
|---|---|---|
| CPU | 2× AMD EPYC 9655 @2.60 GHz (96C/192T per socket) | **192 core / 384 thread** |
| RAM | 16× 64 GB RDIMM DDR5-6400 | **1.024 GB (1 TB)** |
| Storage data | 8× 3.2 TB NVMe Datacenter Mixed-Use (E3.S Gen5) | **25.6 TB raw** |
| Storage boot | 2× M.2 960 GB RAID1 (+960 GB SATA) | terpisah dari data |
| Jaringan | 2× Broadcom 57414 25GbE dual-port | 25 GbE |
| Daya | Dual redundant PSU | — |
| Form factor | Rack 2U | peruntukan: *DAP Platform* + *DB Oneflux* |

### 3.2 Alokasi ClickHouse aktual (Grafana, 2026-06-08, steady-state)

| Instance | Core | RAM | Disk | Terpakai |
|---|---:|---:|---:|---:|
| Data Lake (`bliv-data-lake`) | 16 | 63 GiB | 3 TiB | ~1.0 TB (33%) |
| Warehouse-Mart (`clickhouse-warehouse-mart`) | n/a* | 126 GiB | 6 TiB | ~0.34 TB (5.6%) |
| **Subtotal ClickHouse** | | **189 GiB** | **9 TiB** | **~1.34 TB** |

*\*core warehouse terpotong di screenshot — perlu konfirmasi.*

---

## 4. Batas maksimal yang dapat diakomodir (ceiling per resource)

> Asumsi: **satu server fisik**, menampung seluruh stack DAP (ClickHouse + Pipeline/NiFi + Dashboard + Admin + Monitoring + Explore + ZK + HAProxy) **plus** kemungkinan DB sumber Oneflux. Reservasi OS/hypervisor ~10–15%.

### 4.1 Compute (CPU) — **bukan kendala**

| | Thread |
|---|---:|
| Total fisik | 384 |
| Reservasi OS/hypervisor (~12%) | ~46 |
| VM non-ClickHouse Phase 2 (Pipeline 24 + Dashboard 16 + Admin/Mon 8 + DataHouse 8 + Explore 16 + ZK 6 + HAProxy 8) | ~86 |
| **Sisa dapat dialokasikan ke ClickHouse (lake + warehouse + ingest)** | **≈ 250 thread** |

→ Data Lake bisa dinaikkan **16 → 48–64 core** dengan nyaman untuk memangkas window load, tanpa menyentuh ceiling. **Rekomendasi alokasi maksimum aman:** Lake ≤ 64 vCPU, Warehouse ≤ 48 vCPU.

### 4.2 Memory (RAM) — **bukan kendala**

| | GiB |
|---|---:|
| Total fisik | 1.024 |
| VM non-ClickHouse Phase 2 (~316) + OS (~80) | ~396 |
| **Sisa dapat dialokasikan ke ClickHouse** | **≈ 628 GiB** |

→ Saat ini ClickHouse hanya pakai 189 GiB. **Rekomendasi maksimum aman:** Lake 64→**256 GiB**, Warehouse **128–256 GiB**. (Catatan R0b: RAM lake 63 GiB saat ini terlalu kecil untuk beban merge → sumber SWAP 26.4%.)

### 4.3 Storage — **INI ceiling-nya** (bergantung RAID)

| Level RAID | Usable (dari 25.6 TB raw) | Proteksi | Sisa setelah 9 TiB CH terpakai\* |
|---|---:|---|---:|
| RAID0 / JBOD | ~25.6 TB | ❌ tidak ada | ~14–15 TB |
| RAID5 (7+1) | ~22.4 TB | 1 disk | ~11–12 TB |
| RAID6 (6+2) | ~19.2 TB | 2 disk | ~8–9 TB |
| **RAID10** (4 mirror) | **~12.8 TB** | 1/grup | **~2–3 TB** ⚠️ |

*\*setelah dikurangi disk VM lain (~1.5–2 TB) + boot.*

→ **Jika array RAID10, server praktis sudah mendekati penuh** untuk data (hanya ~2–3 TB tersisa) — tidak akan cukup untuk production + archive penuh. **Jika RAID5/6, tersedia 8–12 TB** untuk pertumbuhan. **Inilah variabel paling menentukan dan harus dikonfirmasi (§6-A).**

---

## 5. Berapa volume data produksi yang muat?

Baseline QA = 255 GiB bronze (sebagian tabel di-exclude) → ~1 TB on-disk (parts + overhead ReplacingMergeTree). Estimasi kasar (faktor on-disk ~3–4× raw logical, sebelum replication):

| Skenario prod+archive (raw logical) | On-disk ClickHouse (≈) | Muat di RAID10 (~12.8 TB)? | Muat di RAID6 (~19.2 TB)? |
|---|---:|:---:|:---:|
| 2× QA (~0.5 TB) | ~2 TB | ✅ | ✅ |
| 5× QA (~1.3 TB) | ~5 TB | ⚠️ ketat (+other VM) | ✅ |
| 10× QA (~2.6 TB) | ~10 TB | ❌ | ✅ |
| 10× + replication ×2 | ~20 TB | ❌ | ❌ (perlu disk tambah) |

> **Kesimpulan:** tanpa replication dan dengan RAID5/6, server ini realistis menampung **hingga ~10–12 TB on-disk data ClickHouse** (≈ 8–12× volume QA). Dengan RAID10 **atau** dengan replication ×2/×3, kapasitas turun drastis dan **kemungkinan tidak cukup**. **Replication untuk HA tidak bermanfaat di satu server fisik** (hanya menggandakan data di disk yang sama, tanpa proteksi kegagalan host).

**Pengungkit (lever) untuk tetap muat:** kebijakan **retensi + TTL** (mis. archive ke object storage/S3 dingin), partisi by-date, dan kompresi ClickHouse (codec) — wajib didefinisikan sebelum production load.

---

## 6. Asumsi yang harus dikonfirmasi (escalation asks)

| # | Pertanyaan | Mengapa penting | PIC |
|---|---|---|---|
| **A** | **Level RAID** array 8× 3.2 TB NVMe? | Menentukan usable storage 12.8–25.6 TB → langsung menentukan §4.3 & §5 | Infra Mitratel |
| **B** | **Jumlah server fisik** (1 atau lebih)? | Menentukan apakah HA klaster nyata atau logis; replication CH baru berarti bila ≥2 host | Infra Mitratel |
| **C** | **DB sumber Oneflux co-located** di server ini? | Konsumsi CPU/RAM/IO bersaing dengan OLAP; pengaruhi ceiling | Infra Mitratel |
| **D** | **Target replication factor** ClickHouse (1/2/3)? | Menggandakan kebutuhan storage; tidak berguna di single host | BTJ / Infra |
| **E** | **Estimasi volume production + archive** (row & GiB per domain) + daftar tabel QA di-exclude | Validasi terhadap ceiling §5 | Mitratel |
| **F** | **Core warehouse-mart** aktual (terpotong di screenshot) | Melengkapi inventaris §3.2 | Infra/BTJ |

---

## 7. Rekomendasi — spesifikasi maksimum aman (single-server, current hardware)

| Komponen | Sekarang | **Maksimum aman dalam box ini** | Catatan |
|---|---|---|---|
| ClickHouse Lake — vCPU | 16 | **≤ 64** | naikkan untuk memangkas window load |
| ClickHouse Lake — RAM | 63 GiB | **≤ 256 GiB** | atasi SWAP/merge pressure (R0b) |
| ClickHouse Warehouse — RAM | 126 GiB | **≤ 256 GiB** | rebalance vs lake |
| ClickHouse total storage (data) | 9 TiB | **≤ usable RAID − ~2 TB VM lain** (≈ **2–11 TB** sisa) | **ceiling bergantung §6-A** |
| Replication factor | ? | **1 (di single host)** | HA fisik perlu server ke-2 (§6-B) |
| Swap (host DB) | 26.4% terpakai | **swappiness≈1 / off** | — |

**Langkah yang disarankan:**
1. **Konfirmasi §6-A s/d §6-F** sebelum finalisasi sizing produksi.
2. **Naikkan CPU+RAM Data Lake** (mis. 32–48 vCPU / 192–256 GiB) — gratis secara hardware, langsung memangkas window onboarding. Tuning swap.
3. **Tetapkan kebijakan retensi/TTL + archive ke storage dingin (S3)** sebelum production load — ini penentu apakah data muat.
4. **Untuk HA produksi nyata: adakan server fisik ke-2.** Tanpa itu, "klaster 3-node" + replication hanya menghabiskan disk tanpa proteksi.
5. **Begitu estimasi production+archive (§6-E) masuk:** hitung terhadap §5 dan putuskan apakah perlu (a) tambah disk/RAID rebuild, (b) tiering ke object storage, atau (c) server tambahan.

---

*Angka compute/RAM/storage diturunkan dari spesifikasi server fisik (WhatsApp image 2026-05-21) dan alokasi ClickHouse aktual (Grafana 2026-06-08). Estimasi on-disk & skenario muat bersifat indikatif sampai level RAID, replication factor, dan volume production+archive dikonfirmasi (§6).*
