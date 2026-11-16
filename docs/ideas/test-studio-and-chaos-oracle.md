# PocketQueue Test Studio & Chaos Oracle

> **Document Status**: Refined Concept & Architecture Specification  
> **Date**: 2026-11-16  
> **Category**: Developer Experience (DX) & CI/CD Testing Automation  

---

## 1. Problem Statement

> *"How might we empower microservices developers and DevOps engineers to catch elusive race conditions, test network failure resilience, and verify event streams in CI/CD without burning gigabytes of RAM or waiting on heavy Docker containers?"*

---

## 2. Recommended Direction

Mengembangkan PocketQueue dari broker pasif menjadi **laboratorium pengujian event-driven dan CI/CD Test Oracle yang serba lengkap**:
1. **Di Laptop Developer (Inner-Loop)**:
   - **Time-Travel Traffic Recording & Step Replay (`.pqrec`)**: Rekam aliran event pengetesan ke file NDJSON yang transparan, lalu putar ulang dengan kecepatan yang dapat diatur (`0.5x`, `1x`, `2x`, `step`) untuk mereproduksi bug atau race condition secara deterministik.
   - **Interactive Chaos & Fault Injection (Tombol `x` di TUI)**: Simulasi latensi buatan, *packet drop*, dan *duplicate delivery* (at-least-once delivery) langsung dari antarmuka terminal untuk menguji logika retry dan idempotensi consumer.
2. **Di Pipeline CI/CD (Outer-Loop)**:
   - **Headless Test Assertion CLI (`pocketqueue test`)**: Perintah sekali jalan berkecepatan milidetik yang dapat memverifikasi kedatangan pesan berdasarkan topik, key, atau substring payload tanpa memerlukan Testcontainers, JVM, atau daemon Docker.

Semua kapabilitas ini dibangun di atas runtime murni Go (`CGO_ENABLED=0`) dengan mempertahankan batasan arsitektural utama: ukuran binary **< 6.0 MB** dan idle RAM **< 10.0 MB**.

---

## 3. Resolusi Keputusan Desain (Key Design Decisions)

### A. Format Rekaman Lalu Lintas: Murni NDJSON (`.pqrec`)
- Setiap baris adalah objek JSON valid yang merekam satu event:
  ```json
  {"v":1,"t":1731763200123,"topic":"orders","partition":0,"offset":101,"key":"ord-101","payload":"{\"status\":\"paid\"}","headers":{"trace-id":"abc-123"}}
  ```
- **Alasan**:
  - Transparan: developer dapat membuka, membaca, dan mengedit payload rekaman menggunakan VS Code atau editor teks apa pun.
  - Kompatibel dengan perkakas Unix seperti `grep`, `jq`, dan `awk`.
  - Dapat di-commit ke Git sebagai *test fixture* tim.

### B. Perilaku Offset Consumer Group pada Mode `--replay`
- **Default (Sangat Direkomendasikan)**: **Reset consumer group offsets ke awal rekaman (`offset 0`)**.
  - *Alasan*: Tujuan utama developer menjalankan replay adalah agar consumer mereka memproses ulang seluruh event yang direkam. Jika offset tidak direset, consumer akan langsung melompat ke offset terakhir dan mengabaikan seluruh pesan replay.
- **Opsi Lanjutan**: Flag `--preserve-offsets` jika developer sengaja ingin melanjutkan offset yang tersimpan tanpa reset.

---

## 4. Asumsi Utama untuk Divalidasi (Key Assumptions to Validate)

- [ ] **Asumsi 1 (Adopsi Replay Fixture)**: Developer merasa format rekaman NDJSON `.pqrec` praktis untuk dibagikan ke rekan setim sebagai bukti reproduksi bug (*bug reproduction fixture*). *(Validasi: uji buat 1 skenario fixture incident e-commerce dan putar ulang di mesin berbeda).*
- [ ] **Asumsi 2 (Zero-Overhead Chaos Logic)**: Middleware chaos injection pada `broker.Service` tidak menimbulkan penalti throughput ketika dinonaktifkan (`0% overhead`). *(Validasi: benchmark `go test -bench` kondisi normal vs chaos nonaktif).*
- [ ] **Asumsi 3 (CI Replacement Viability)**: Subcommand `pocketqueue test --expect-...` cukup ekspresif untuk menggantikan Testcontainers Kafka/Redis pada 80% skenario tes integrasi di GitHub Actions. *(Validasi: buat 1 file demo workflow CI yang memverifikasi flow produce & consume dalam < 3 detik).*

---

## 5. Ruang Lingkup MVP (Scope)

### ✅ In Scope (Yang Masuk ke MVP)

#### 1. Traffic Recorder & Replay Engine
- **CLI Flags**:
  - `--record <file.pqrec>`: Merekam seluruh event yang masuk ke file NDJSON secara real-time.
  - `--replay <file.pqrec>`: Memutar ulang event dari file.
  - `--replay-speed <float>`: Multiplier kecepatan replay (`0.5`, `1.0`, `2.0`, default `1.0`).
  - `--preserve-offsets`: Mempertahankan offset consumer group yang tersimpan (default: reset ke 0).
- **TUI Controls**:
  - Shortcut `Shift+R`: Memulai / menghentikan perekaman sesi langsung dari TUI. Indikator `[REC: orders (42 events)]` menyala di status bar.

#### 2. Chaos & Fault Injection Engine
- **CLI Flags & TUI Modal (`x` key)**:
  - `--chaos-latency <ms>` & `--chaos-latency-rate <0-100>`: Menambahkan latensi buatan pada persentase tertentu dari request produce/fetch.
  - `--chaos-drop-rate <0-100>`: Mensimulasikan kegagalan jaringan / drop koneksi.
  - `--chaos-duplicate-rate <0-100>`: Mensimulasikan pengiriman duplikat untuk menguji *idempotent consumer*.
- **TUI Visual Feedback**:
  - Badge status bar berkedip ketika chaos aktif: `[CHAOS: 20% DUP, 300ms JITTER]`.

#### 3. Headless Test Assertion CLI (`pocketqueue test`)
- Subcommand khusus untuk pengujian otomatis di pipeline CI/CD:
  ```bash
  pocketqueue test \
    --expect-topic orders \
    --expect-key ord-101 \
    --expect-payload "paid" \
    --timeout 5s
  ```
- **Exit Code**:
  - `0`: Pesan yang diharapkan berhasil diterima sebelum batas timeout.
  - `1`: Timeout habis atau pesan tidak sesuai kriteria assertion.
  - Menampilkan ringkasan JSON assertion di stdout untuk kemudahan debugging log CI.

---

### ❌ Not Doing (Yang Tegas Ditolak)

- **Web UI Browser Dashboard (`:8080`)**: Ditunda agar fokus 100% pada keunggulan pengujian developer & CI. Antarmuka TUI dan CLI sudah sangat memadai.
- **Kernel-Level Socket Simulation (seperti Toxiproxy)**: PocketQueue menyimulasikan chaos di application/broker layer, bukan TCP socket drop di level OS. Ini 10x lebih sederhana, aman, dan 100% cross-platform (Windows, macOS, Linux).
- **Protokol AMQP/MQTT**: Tetap mempertahankan fokus murni pada Kafka dan Redis Streams.
- **WASM / Lua In-Flight Filter**: Ditolak demi menjaga batas ketat ukuran binary (< 15 MB) dan RAM (< 25 MB).

---

## 6. Spesifikasi Antarmuka CLI & TUI

### CLI Cheatsheet
```bash
# 1. Menjalankan PocketQueue sambil merekam seluruh traffic
pocketqueue --record incident-101.pqrec

# 2. Memutar ulang sesi rekaman dengan kecepatan 2x (otomatis reset offset)
pocketqueue --replay incident-101.pqrec --replay-speed 2.0

# 3. Menjalankan mode chaos untuk stress-test microservice lokal
pocketqueue --chaos-latency 400 --chaos-latency-rate 25 --chaos-duplicate-rate 10

# 4. CI/CD Assertion sekali jalan
pocketqueue test --expect-topic telemetry --expect-payload "battery_low" --timeout 3s
```

### TUI Keybinding Tambahan
| Key | Aksi Baru |
|---|---|
| `Shift+R` (`R`) | Toggle Start/Stop Traffic Session Recording ke file `.pqrec` |
| `x` | Buka Modal Konfigurasi Chaos & Fault Injection |

---

## 7. Metrik Keberhasilan & Batasan Operasional

1. **Ukuran Binary Statis**: Tetap `< 6.0 MB` (saat ini 5.14 MB).
2. **Idle RAM Footprint**: Tetap `< 10.0 MB` (saat ini 7.93 MB).
3. **Overhead CPU**: `< 1%` CPU tambahan saat chaos engine aktif.
4. **Kecepatan Booting**: Tetap `< 5.0 ms`.
