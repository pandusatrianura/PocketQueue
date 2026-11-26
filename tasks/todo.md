# Todo: Milestone 6.1 — Data Resilience & Container DX

## Phase 1: Consumer Group Offset Persistence (TDD)

- [x] **Task 1.1: Implement OffsetStore and FileOffsetStore**
  - **Description**: Create `internal/features/broker/offsets.go` defining the `OffsetStore` interface and `FileOffsetStore` implementing atomic file serialization (`offsets.json.tmp` -> `offsets.json`) with a 50ms debounced flush and synchronous flush on `Close()`.
  - **Acceptance criteria**:
    - `OffsetStore` defines `Save`, `Load`, and `Close` methods.
    - `FileOffsetStore` writes valid JSON with atomic rename.
    - Multiple rapid `Save` calls within 50ms are coalesced into a single disk write.
  - **Verification**: `go test -v ./internal/features/broker -run TestFileOffsetStore` (PASS)
  - **Files**: `internal/features/broker/offsets.go`, `internal/features/broker/offsets_test.go`
  - **Scope**: Small (2 files)

- [x] **Task 1.2: Wire Offset Persistence into Broker Service**
  - **Description**: Update `internal/features/broker/service.go` to accept an optional `OffsetStore`. In `NewService`, load saved offsets if present; in `CommitOffset`, notify the `OffsetStore`. In `Close()`, close the `OffsetStore`.
  - **Acceptance criteria**:
    - Broker restores committed offsets on startup if present in store.
    - `CommitOffset` updates both memory and triggers debounced persistent save.
    - Passing `nil` or `NoopOffsetStore` preserves pure in-memory behavior without nil-pointer panics.
  - **Verification**: `go test -v ./internal/features/broker` (PASS)
  - **Files**: `internal/features/broker/service.go`, `internal/features/broker/service_test.go`
  - **Scope**: Small (2 files)

- [x] **Task 1.3: Wire Offset Store in CLI Main**
  - **Description**: In `cmd/pocketqueue/main.go`, when `cfg.DataDir` is specified, instantiate `broker.NewFileOffsetStore(cfg.DataDir)` and pass it to `broker.NewService`.
  - **Acceptance criteria**:
    - `--data-dir` directory automatically initializes `offsets.json`.
    - Offsets persist across simulated restart in test.
  - **Verification**: `go test -v ./cmd/pocketqueue` (PASS)
  - **Files**: `cmd/pocketqueue/main.go`, `cmd/pocketqueue/main_test.go`
  - **Scope**: Small (1-2 files)

---

## Checkpoint 1: Offset Persistence
- [x] All broker tests pass (`go test -v ./internal/features/broker/...`)
- [x] Offsets restored accurately upon service restart

---

## Phase 2: WAL Retention & Pruning (TDD)

- [x] **Task 2.1: Implement Retention Pruning in WALEngine**
  - **Description**: Add `PruneOldest` in `internal/features/storage/wal.go`. When an append causes a partition file to exceed `maxFileSize` (or `--retention-bytes`), prune the oldest frames, compact the WAL file, and advance `startOffset` (`Earliest` watermark).
  - **Acceptance criteria**:
    - When file exceeds threshold, oldest messages are cleanly pruned.
    - Partition watermarks reflect the new earliest available offset.
    - Subsequent appends and reads continue without file corruption.
  - **Verification**: `go test -v ./internal/features/storage -run TestWAL_RetentionPruning` (PASS)
  - **Files**: `internal/features/storage/wal.go`, `internal/features/storage/wal_test.go`
  - **Scope**: Medium (2 files)

- [x] **Task 2.2: Add Retention Flags to CLI Config**
  - **Description**: In `cmd/pocketqueue/config.go`, add flags `-retention-bytes` (default 100 MB / `104857600`) and `-retention-age` (default 0 / disabled). Pass `retentionBytes` to `storage.NewWALEngineWithLimit`.
  - **Acceptance criteria**:
    - CLI parses `-retention-bytes` and `-retention-age`.
    - Default values maintain existing safe boundaries.
  - **Verification**: `go test -v ./cmd/pocketqueue` (PASS)
  - **Files**: `cmd/pocketqueue/config.go`, `cmd/pocketqueue/config_test.go`, `cmd/pocketqueue/main.go`
  - **Scope**: Small (2-3 files)

---

## Checkpoint 2: Retention Pruning
- [x] All storage tests pass (`go test -v ./internal/features/storage/...`)
- [x] Appends beyond retention budget trigger safe FIFO pruning rather than rejecting writes

---

## Phase 3: Container DX — Ping Probe & Graceful Drain (TDD)

- [x] **Task 3.1: Implement Healthcheck Probe Command (`pocketqueue ping`)**
  - **Description**: Implement `runPingCommand` in `cmd/pocketqueue/ping_cmd.go` and wire into `main.go`. Connect to Kafka TCP port, send `ApiVersions` request, and exit 0 on success or exit 1 on timeout/failure.
  - **Acceptance criteria**:
    - `pocketqueue ping --broker 127.0.0.1:9092` prints latency and exits code 0 when broker is healthy.
    - Exits code 1 when broker is down or unreachable within `--timeout` (default 2s).
  - **Verification**: `go test -v ./cmd/pocketqueue -run TestPingCommand` (PASS)
  - **Files**: `cmd/pocketqueue/ping_cmd.go`, `cmd/pocketqueue/ping_cmd_test.go`, `cmd/pocketqueue/main.go`
  - **Scope**: Small (3 files)

- [x] **Task 3.2: Implement Graceful Connection Draining on Shutdown**
  - **Description**: Add `-drain-timeout` flag (default 500ms) in `cmd/pocketqueue/config.go`. In `main.go`, on `SIGINT`/`SIGTERM`, stop accepting new connections, wait up to drain timeout for in-flight requests to finish, flush storage and offsets, and exit.
  - **Acceptance criteria**:
    - Active connections complete gracefully with clean EOF.
    - Shutdown never hangs longer than `--drain-timeout`.
    - Storage and offsets are flushed to disk before exit.
  - **Verification**: `go test -v ./cmd/pocketqueue` (PASS)
  - **Files**: `cmd/pocketqueue/config.go`, `cmd/pocketqueue/main.go`, `internal/features/protocol/kafka/server.go`, `internal/features/protocol/redis/server.go`
  - **Scope**: Small (4 files)

---

## Checkpoint 3: Container DX
- [x] `pocketqueue ping` passes against active server
- [x] Graceful shutdown drains cleanly within timeout

---

## Phase 4: Full System Verification & Resource Audit

- [x] **Task 4.1: End-to-End Integration Tests for Resilience**
  - **Description**: Create `test/e2e/resilience_e2e_test.go` exercising WAL persistence across process restarts, retention roll under high-volume produce, and ping probe execution.
  - **Acceptance criteria**:
    - All E2E scenarios pass cleanly without timing flakes.
  - **Verification**: `go test -v ./test/e2e` (PASS)
  - **Files**: `test/e2e/resilience_e2e_test.go`
  - **Scope**: Small (1 file)

- [x] **Task 4.2: Full Zero-Cache Regression & Resource Audit**
  - **Description**: Run full regression test suite `go test -count=1 ./...` and execute `scripts/audit.ps1`.
  - **Acceptance criteria**:
    - 100% test pass across all packages.
    - Stripped binary size < 6.0 MB (measured: 5.30 MB).
    - Idle RAM < 10.0 MB (measured: 7.78 MB).
    - Cold boot latency < 5.0 ms (measured: 2.15 ms).
  - **Verification**: `go test -count=1 ./...; powershell -ExecutionPolicy Bypass -File .\scripts\audit.ps1` (ALL PASS)
  - **Files**: None (verification)
  - **Scope**: Small

- [x] **Task 4.3: Documentation & Roadmap Synchronization**
  - **Description**: Update `README.md` and `docs/ROADMAP.md` documenting `--retention-bytes`, `pocketqueue ping`, `--drain-timeout`, and marking Milestone 6.1 as completed.
  - **Acceptance criteria**:
    - README includes Docker `HEALTHCHECK` example with `pocketqueue ping`.
    - ROADMAP reflects Milestone 6.1 completion and Milestone 7 status.
  - **Verification**: Markdown review and git status
  - **Files**: `README.md`, `docs/ROADMAP.md`
  - **Scope**: Small (2 files)

---

## Final Checkpoint: Milestone 6.1 Complete
- [x] All 10 tasks completed and verified
- [x] Zero compiler warnings, zero lint errors
- [x] All operational invariants strictly satisfied
