# Project Handoff Report: DuckDB-Integrated BadgerDB

**Repository:** `github.com/vaishnaviikv/badger`  
**Branch:** `duckdb-integration`  
**Local path:** `/Users/AshleyLuo1/GolandProjects/darshan-badger`  
**Go module:** `github.com/dgraph-io/badger/v4`  
**Last updated:** June 9, 2026

---

## 1. Project Overview

This project is a research fork of [BadgerDB v4](https://github.com/hypermodeinc/badger) — a production-grade embedded key-value store written in Go. The goal is to replace (or augment) Badger's native LSM-tree storage engine with **DuckDB** as an alternative storage backend, while preserving the full Badger MVCC transaction API (`NewTransactionAt`, `CommitAt`, snapshot reads, etc.).

The motivation is to make Badger interoperate with a distributed ordering service called **Divy** (referred to as "divytime" in code), which issues globally-ordered **3-tuple timestamps** of the form `(EpochID, BrokerID, AssignedTs)` instead of simple uint64 counters. DuckDB's columnar SQL engine is well-suited to this because:

- It can store and query on all three timestamp components natively.
- Its in-process columnar engine can exploit SIMD for fast range scans.
- It allows SQL-level compaction (keeping only the latest version per key) without touching the LSM compaction machinery.

The project is active research/benchmarking work driven by three people: **Darshan** (correctness), **Ashley** (throughput), and **Vaishnavi** (repo owner/integration). This Claude instance is meant to be a hands-on coding partner for all three.

---

## 2. Architecture Deep-Dive

### 2.1 Unchanged Badger Components

The following Badger subsystems are **completely unchanged** and work exactly as upstream:

- **Memtable** (`memtable.go`) — The skip-list-based in-memory write buffer. All writes still land here first.
- **MVCC oracle** (`txn.go`) — Manages read timestamps, commit timestamps, conflict detection, and watermarks. Modified to use `types.CustomTs` instead of `uint64`, but the logic is identical.
- **Transaction API** (`txn.go`) — `NewTransactionAt`, `CommitAt`, `Get`, `Set`, `Delete`, `Discard` all work identically. One new method `PrefetchKeys` was added (see §2.4).
- **Iterator API** (`iterator.go`) — Unchanged.
- **Value log** (`value.go`) — Unchanged.
- **Managed DB** (`managed_db.go`) — `OpenManaged` is the entry point used for all DuckDB tests; it enables externally-managed timestamps (necessary for divytime).

### 2.2 The DuckDB Storage Backend

**Files:**
- `duckdb/storage.go` — The entire DuckDB storage implementation (compiled only with `-tags duckdb`)
- `db_duckdb_impl.go` — Adapter between Badger's internal types and DuckDB types (compiled only with `-tags duckdb`)
- `db_duckdb_stub.go` — No-op stubs so the package compiles without the build tag
- `db.go` — Contains the `duckDBIface` interface and `DB.duckDBStorage` field

#### 2.2.1 Data Model

Each DuckDB-backed Badger database creates N **partition tables** (default N=8, configured by `options.PartitionFanOut`). Every partition is a DuckDB table with this schema:

```sql
CREATE TABLE partition_N (
    key       BLOB NOT NULL,
    epoch_id  BIGINT NOT NULL,
    broker_id BIGINT NOT NULL,
    assigned_ts BIGINT NOT NULL,
    value     BLOB,
    deleted   BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (key, epoch_id, broker_id, assigned_ts)
)
```

Keys are routed to partitions by `FNV-1a(key) % N`. This allows parallel writes across partitions and reduces lock contention.

#### 2.2.2 Write Path

There are two write paths:

**1. Memtable flush path** (`handleMemTableFlushPartitioned` in `db_duckdb_impl.go`):  
When the memtable is full, it is iterated and all entries are written to DuckDB via `FlushDarshanEntries`. Used for large batch writes.

**2. Direct append path** (`DirectAppendEntries` in `duckdb/storage.go`):  
On every `CommitAt`, the transaction's entries are appended directly to the persistent `DuckDB Appender` buffer, bypassing the memtable entirely for the DuckDB side. A `CGo Appender.Flush()` fires automatically once 512 rows have accumulated in a partition (`directFlushBatchSize = 512`), amortizing the fixed CGo boundary cost.

The key optimization is that **each partition owns a persistent `partitionAppender`** — a long-lived DuckDB connection and Appender kept open for the lifetime of the database. This eliminates the per-flush cost of connection acquisition and Appender construction.

#### 2.2.3 Read Path

`Read(key, readTs)` in `duckdb/storage.go`:

1. Check `pendingKeys` map under `RLock` — a lightweight set of keys that have been `AppendRow`'d but not yet `Flush`'d to DuckDB.
2. If the key is in `pendingKeys`, acquire `WLock` and call `Appender.Flush()` before querying.
3. Execute a prepared SQL statement (pre-compiled at open time, one per partition) that finds the latest version of the key with timestamp ≤ readTs:

```sql
SELECT key, epoch_id, broker_id, assigned_ts, value, deleted
FROM partition_N
WHERE key = ?
  AND (epoch_id < ? OR
       (epoch_id = ? AND broker_id < ?) OR
       (epoch_id = ? AND broker_id = ? AND assigned_ts <= ?))
ORDER BY epoch_id DESC, broker_id DESC, assigned_ts DESC
LIMIT 1
```

`ReadBatch(requests)` in `duckdb/storage.go`:  
Groups multiple key lookups by partition. For a partition with a single key, uses the pre-compiled LIMIT 1 statement. For multiple keys in the same partition, issues one `IN`-clause query with `ROW_NUMBER() OVER (PARTITION BY key)` to fetch the latest visible row for every key in a single SQL round-trip — reducing CGo round-trips from N to 1.

#### 2.2.4 Compaction

`CompactPartitions()` removes superseded key versions in each partition with:

```sql
DELETE FROM partition_N
WHERE (key, epoch_id, broker_id, assigned_ts) NOT IN (
    SELECT key, epoch_id, broker_id, assigned_ts
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY epoch_id DESC, broker_id DESC, assigned_ts DESC) AS rn
        FROM partition_N
    ) WHERE rn = 1
)
```

**Important caveat:** After compaction, only the latest version per key survives. Snapshot reads at older timestamps will return "Key not found" for compacted keys. This is the documented trade-off of the current compaction design.

### 2.3 The Timestamp System

**File:** `types/custom_ts.go`

The custom timestamp type replaces Badger's original `uint64` version counter:

```go
type CustomTs struct {
    EpochID    uint32
    BrokerID   uint32
    AssignedTs uint32
}
```

Ordering is lexicographic: `EpochID` is most significant, then `BrokerID`, then `AssignedTs`. This maps directly to a real distributed ordering service where `EpochID` is a logical epoch, `BrokerID` identifies the ordering node, and `AssignedTs` is a sequence number within that broker's epoch.

Timestamps are encoded as 12-byte big-endian with **bitwise inversion** for descending sort order in the LSM tree (so the latest version sorts first, matching Badger's existing internal key layout).

`types.MaxTs` is `{math.MaxUint32, math.MaxUint32, math.MaxUint32}` and is used for "read at the absolute latest version."

**File:** `divytime/divytime.go`

The `divytime.Oracle` is an in-process stub for the real Divy ordering service:

```go
type Oracle struct {
    brokerID       int64
    simulatedDelay time.Duration
    counter        int64 // atomic
}
```

`GetTimestamp(epochID)` returns a `(EpochID, BrokerID, AssignedTs)` tuple with an atomically-incremented `AssignedTs`. A configurable `simulatedDelay` (e.g., 50 µs) models the network round-trip to a real ordering service.

### 2.4 Key API Extensions

**`txn.PrefetchKeys(keys [][]byte) error`** (`txn.go`):  
Fetches all given keys from DuckDB in a single `ReadBatch` call and caches the results in `txn.batchCache`. Subsequent `txn.Get()` calls for those keys are served from the cache without any SQL. All SmallBank transaction functions call this at the start of each transaction to reduce CGo round-trips from N reads to 1.

**`txn.batchCache`** (`txn.go`):  
A `map[string]*duckReadBatchResult` field on `Txn`. Populated by `PrefetchKeys`, consumed by `Get`. Cleared on `Discard`.

**Oracle fast path** (`txn.go` — `newCommitTs`):
```go
if o.isManaged && !o.detectConflicts {
    return txn.commitTs, false
}
```
When running in managed mode with conflict detection disabled (which is how DuckDB tests run), the oracle's mutex is skipped entirely. The DAG executor is assumed to pre-order transactions, so no conflicts can arrive concurrently.

### 2.5 Database Open Sequence (DuckDB Mode)

```
OpenManaged(opts{UseDuckDB: true, PartitionFanOut: 8}) 
  → Open()
    → newDuckDBBackend(path, 8)          // duckdb/storage.go: NewDuckDBStorage
      → initializeTables()               // creates partition_0 ... partition_7
      → initPersistentAppenders()        // one conn + appender + readStmt per partition
    → db.duckDBStorage = wrapper         // db_duckdb_impl.go: duckDBStorageWrapper
```

### 2.6 Build Tags

All DuckDB code is gated behind the `duckdb` build tag:

```bash
# Compile with DuckDB
go build -tags duckdb ./...
go test -tags duckdb ./...

# Compile without DuckDB (standard Badger, all DuckDB files are stubs)
go build ./...
go test ./...
```

The stub file `db_duckdb_stub.go` (with `//go:build !duckdb`) provides empty implementations of `newDuckDBBackend` and `handleMemTableFlushPartitioned` so the package compiles in both modes.

---

## 3. Key Modifications to Upstream Badger

| Component | What Changed |
|---|---|
| `types/custom_ts.go` | **New file.** Defines `CustomTs{EpochID, BrokerID, AssignedTs uint32}`, ordering operators (`Less`, `Greater`, `Equal`, `Decr`, `Incr`), byte encoding with bit inversion. |
| `db.go` | Added `duckDBIface` interface, `duckEntry`, `duckReadBatchReq/Result` types, `DB.duckDBStorage` field. |
| `txn.go` | All `uint64` timestamps replaced with `types.CustomTs`. Added `Txn.batchCache`, `PrefetchKeys()`. Oracle fast path for managed+no-conflict mode. |
| `oracle_fastpath_test.go` | Tests verifying the oracle fast path fires correctly in managed vs non-managed mode. |
| `options.go` | Added `UseDuckDB bool` and `PartitionFanOut int` fields. |
| `watermark.go` (y/) | `WaterMark` updated to track `types.CustomTs` instead of `uint64`. |
| `managed_db.go` | `NewTransactionAt` / `CommitAt` updated for `CustomTs`. |
| `duckdb/storage.go` | **New package.** Full DuckDB storage implementation. |
| `db_duckdb_impl.go` | **New file.** Adapter/wrapper for DuckDB backend. Memtable flush. |
| `db_duckdb_stub.go` | **New file.** No-op stubs for non-duckdb builds. |
| `divytime/divytime.go` | **New package.** In-process timestamp oracle. |

---

## 4. Test Suite

### 4.1 File Map

| Test file | What it tests | Build tag |
|---|---|---|
| `db_lockfree_test.go` | Regular Badger: lock-free ingest, MVCC correctness, `TestTimestampScenarios` | none |
| `oracle_fastpath_test.go` | Oracle fast path: managed vs regular, conflict detection | duckdb |
| `db_duckdb_integration_test.go` | Basic DuckDB open/write/flush/read | duckdb |
| `db_duckdb_helpers_test.go` | `withDuckDB` helper, `BenchmarkLockFreeIngest_DuckDB` | duckdb |
| `db_duckdb_correctness_test.go` | `TestDuckDBTimestampScenarios`: full MVCC scenario suite on DuckDB path | duckdb |
| `db_duckdb_bank_test.go` | Bank workload with divytime timestamps: TPS, latency, balance invariant | duckdb |
| `db_duckdb_smallbank_bench_test.go` | SmallBank 6-transaction-type benchmark, mixed workload, phase breakdown | duckdb |
| `db_duckdb_serial_correctness_test.go` | **Serial correctness** for bank and SmallBank (Darshan's work) | duckdb |
| `db_duckdb_epoch_stress_test.go` | **Epoch batch oracle** stress test (Ashley's work) | duckdb |
| `db_duckdb_comparison_test.go` | **Badger vs DuckDB** side-by-side comparison (everyone's work) | duckdb |

### 4.2 Test Commands

```bash
# --- Serial correctness tests (Darshan) ---
go test -v -tags duckdb -run TestDuckDBBankSerialCorrectness      -timeout 120s
go test -v -tags duckdb -run TestDuckDBSmallBankSerialCorrectness  -timeout 300s

# --- Epoch stress tests (Ashley) ---
go test -v -tags duckdb -run TestDuckDBBankEpochStress             -timeout 180s
go test -v -tags duckdb -run TestDuckDBBankEpochStressNoDelay      -timeout 180s

# --- Badger vs DuckDB comparison tests ---
go test -v -tags duckdb -run TestBankBadgerVsDuckDB$               -timeout 120s
go test -v -tags duckdb -run TestBankBadgerVsDuckDBWithDelay       -timeout 120s
go test -v -tags duckdb -run TestSmallBankBadgerVsDuckDB           -timeout 300s

# --- SmallBank per-type isolation ---
go test -v -tags duckdb -run TestSmallBankDuckDB                   -timeout 300s
go test -v -tags duckdb -run TestSmallBankDuckDBMixed              -timeout 120s
go test -v -tags duckdb -run TestSmallBankDuckDBPhases             -timeout 120s

# --- Bank concurrent correctness ---
go test -v -tags duckdb -run TestDuckDBBankDivytime                -timeout 60s
go test -v -tags duckdb -run TestDuckDBBankDivytimeSimulatedDelay  -timeout 120s

# --- Timestamp scenario correctness ---
go test -v -tags duckdb -run TestDuckDBTimestampScenarios          -timeout 60s

# --- Regular Badger (no build tag needed) ---
go test -v -run 'TestLatestWins|TestWritersAreNonBlocking|TestTimestampScenarios' -timeout 60s

# --- Full DuckDB test suite ---
go test -v -tags duckdb -timeout 600s ./...
```

### 4.3 Most Recent Benchmark Numbers (Apple M3, local)

**Bank workload (16 workers, 10 s, zero oracle delay):**

| Backend | TPS  | Avg latency | p90 latency |
|---------|------|-------------|-------------|
| Badger  | 5131 | 3.116 ms    | 6.634 ms    |
| DuckDB  | 3931 | 4.069 ms    | 7.180 ms    |

Badger is ~1.3× faster at zero oracle delay (expected: no CGo overhead in Badger).

**Epoch batching with 50 µs oracle delay (16 workers, 5 s per batch size):**

| BatchSize | TPS  | p90 Latency |
|-----------|------|-------------|
| 1         | 3663 | 5.295 ms    |
| 4         | 3950 | 6.447 ms    |
| 16        | 5750 | 4.857 ms    |
| 32        | 7249 | 4.060 ms    |

**2× TPS improvement** from epoch batching with realistic oracle latency.

**SmallBank phase breakdown (10,000 serial runs):**

| Transaction     | Read avg | Write avg | Commit avg |
|-----------------|----------|-----------|------------|
| Balance (3R 0W) | 1.35 ms  | 0 µs      | 0 µs       |
| DepositChecking | 1.71 ms  | 1 µs      | 64 µs      |
| SendPayment     | 3.63 ms  | 2 µs      | 77 µs      |

**Reads dominate: 95%+ of latency is SQL queries. Writes and commits are negligible.**

**LockFreeIngest benchmark (go benchstat, Badger vs DuckDB):**

| Benchmark | Badger | DuckDB | Ratio |
|---|---|---|---|
| LockFreeIngest (1 goroutine) | 1.007 µs | 1.876 µs | +86% |
| LockFreeIngest-4             | 384.8 ns | 1038.5 ns | +170% |
| LockFreeIngest-8             | 422.3 ns | 991.6 ns  | +135% |

DuckDB ingest is ~2× slower than in-memory Badger per operation, as expected for CGo-based writes.

---

## 5. Known Issues and Open Problems

### 5.1 Streaming / Protobuf Panic (Critical Blocker)

Running the standard Badger test suite **without** the `duckdb` build tag triggers this panic:

```
panic: invalid Go type types.CustomTs for field badgerpb4.KV.version
```

**Cause:** The `pb/badgerpb4.pb.go` protobuf file defines `KV.version` as a `uint64`. The `stream.go` file's `KVToBuffer` function marshals a `*pb.KV` containing the version field. Since `types.CustomTs` is not a protobuf scalar type, `proto.Size()` panics.

**Impact:** The standard streaming/backup/restore tests fail. The DuckDB tests are unaffected (they are skipped without `-tags duckdb`).

**What needs to be done:** Either:
- Add a `ToUint64()` helper on `CustomTs` and use it in the streaming/marshaling path, or
- Encode `CustomTs` as three separate `uint32` fields in the protobuf, or
- Keep `KV.version` as `uint64` in the proto and only use `CustomTs` internally.

### 5.2 `DbGrowth` Benchmark is Broken

The `BenchmarkDbGrowth` benchmark takes hours or hangs when run with `-tags duckdb`. This is because DuckDB's `PRIMARY KEY` constraint on `(key, epoch_id, broker_id, assigned_ts)` results in a de-duplicate scan across the entire table for every insert, causing quadratic behavior as the table grows. Tracked in `bench_scale.txt` (shows `4100572646 ns/op` vs `0.26 ns/op` for Badger).

**What needs to be done:** Remove the `PRIMARY KEY` constraint and instead handle deduplication at the application/compaction layer. This would allow append-only inserts with amortized cost.

### 5.3 Compaction Loses History

`CompactPartitions()` keeps only the latest version per key. This means snapshot reads at older timestamps return "Key not found" after compaction. This is fine for the production use case (where Divy assigns globally monotone timestamps and old snapshots are not held), but it means the DuckDB backend cannot support Badger's `NumVersionsToKeep > 1` semantics.

**What needs to be done:** Either document this as a deliberate design choice, or implement a configurable retention window (keep all versions with `assigned_ts >= cutoff`).

### 5.4 DuckDB Partition Count is Fixed at Open Time

The number of partitions is set by `options.PartitionFanOut` at database open time and cannot be changed without recreating all tables. This is fine for experiments but would be a production concern.

### 5.5 No Persistence Across Restarts

The current test setup uses `t.TempDir()` which is deleted after each test. Persistence to disk (using a real file path in `NewDuckDBStorage`) has not been tested under crash recovery scenarios.

---

## 6. Commit History Summary

| Commit | What was done |
|---|---|
| `cee301f` | Serial correctness tests + epoch stress + Badger comparison + DUCKDB_TESTING.md |
| `766b8fb` | True `IN`-clause batching in `ReadBatch` + key co-location for SmallBank |
| `b47b164` | Fix type errors, test failures, oracle fastpath tests |
| `7559481` | Replace CAS linked list |
| `a06707c` | Replace single CAS root with 101-bucket FNV-1a lock-free hash structure |
| `95f723d` | Fix DuckDB test type errors and MVCC snapshot correctness |
| `6082516` | Merge `modify_ts` branch: adopt `types.CustomTs` 3-tuple timestamps throughout |
| `d32eda1` | Add divytime oracle + 3-tuple timestamps in DuckDB backend + bank TPS benchmark |
| `21dc389` | Benchmark DbGrowth test comparison with dd_exp |
| `0ec9881` | All benchmarks except DbGrowth now faster than baseline |

---

## 7. Next Steps / Open Tasks

The following tasks are the priority items for the next work session, in rough order of importance:

### Task 1 — Fix the Protobuf Streaming Panic (Priority: High)

**Problem:** `stream.go`'s `KVToBuffer` function marshals `*pb.KV` which has a `version` field typed as `uint64` in the `.proto`. Since `types.CustomTs` is a struct (not a scalar), `proto.Marshal` panics.

**Approach:**
1. Open `pb/badgerpb4.proto` and check what `version` is used for in the streaming path.
2. In `stream.go`, find where `item.Version()` is assigned to `kv.Version` and change it to call a `ToUint64()` method on `CustomTs` (lossy but sufficient for streaming/backup purposes, where only the SST version number matters, not the full 3-tuple).
3. Alternatively, if full 3-tuple fidelity in the stream is needed, add `epoch_id`, `broker_id`, `assigned_ts` as separate `uint32` fields to the proto.
4. Regenerate proto with `cd pb && ./gen.sh`.
5. Run the full test suite without `-tags duckdb` to confirm no panics.

**Files to change:** `pb/badgerpb4.proto`, `pb/badgerpb4.pb.go`, `stream.go`, possibly `types/custom_ts.go`.

---

### Task 2 — Remove PRIMARY KEY from DuckDB Schema (Priority: High)

**Problem:** The `PRIMARY KEY (key, epoch_id, broker_id, assigned_ts)` in each partition table causes DuckDB to do a uniqueness check on every insert — effectively a table scan at large sizes, producing the catastrophic `DbGrowth` regression.

**Approach:**
1. In `duckdb/storage.go`'s `initializeTables()`, remove the `PRIMARY KEY` clause.
2. Create a non-unique index on `(key, epoch_id DESC, broker_id DESC, assigned_ts DESC)` instead, so the LIMIT 1 read query remains fast.
3. Add a uniqueness guarantee at the application layer: in `appendPartitionDirect` and `flushPartitionWithAppender`, check if a row with the same `(key, epoch, broker, ts)` already exists before appending. OR rely on `CompactPartitions` to clean up duplicates periodically.
4. Rerun the `BenchmarkDbGrowth` benchmark and confirm the regression is fixed.

**Files to change:** `duckdb/storage.go` (schema + optional dedup logic).

---

### Task 3 — Improve Read Latency: Prefetch All Keys Automatically (Priority: Medium)

**Problem:** `PrefetchKeys` must be called manually at the start of each transaction. If a caller doesn't call it (e.g., in the bank test's `execTransfer`), every `txn.Get()` is a separate CGo round-trip. The bank test currently has 2 `Get` calls per transfer × ~0.45 ms each = ~0.9 ms of read overhead per transfer.

**Approach:**  
Instead of relying on callers to call `PrefetchKeys`, modify `txn.Get()` to buffer up to K keys and issue a deferred `ReadBatch` on first access. This is a lazy-batching approach:
1. Add a `pendingReads [][]byte` field to `Txn`.
2. When `txn.Get(key)` is called and the DB has DuckDB storage, append `key` to `pendingReads` instead of fetching immediately.
3. On `txn.Get(key)` where the result is actually needed, flush `pendingReads` via `ReadBatch` and populate `batchCache`, then return from cache.

Alternatively, keep the explicit `PrefetchKeys` design but add it to the bank test's `execTransfer` function.

**Files to change:** `txn.go`, optionally `db_duckdb_bank_test.go`.

---

### Task 4 — Multi-Epoch Correctness Under Concurrency (Priority: Medium)

**Problem:** The concurrent bank test notes: *"write-skew expected in snapshot isolation."* The test does not assert a strict balance invariant under concurrency, only logs the drift. For the Divy use case, the ordering service assigns timestamps such that committed transactions have globally consistent ordering — meaning the production system should be **serializable**, not merely SI.

**Approach:**
1. Add a test mode where the epoch oracle enforces a strict serial order: `GetTimestamp` returns a globally unique timestamp and no two concurrent transactions can have the same or conflicting timestamps.
2. Verify that in this mode the balance invariant holds exactly (as it does in the serial test), not approximately.
3. Document the boundary: when does DuckDB-backed Badger give SI (concurrent), and when does it give serializable (epoch-batched with Divy ordering)?

**Files to change:** `divytime/divytime.go` (add serializable mode), new test in `db_duckdb_bank_test.go`.

---

### Task 5 — Persistence and Crash Recovery Testing (Priority: Medium)

**Problem:** All current tests use `t.TempDir()` and close cleanly. No test verifies that:
- Data written to DuckDB survives a process restart.
- An incomplete `Appender.Flush()` at crash time doesn't leave the partition in a corrupt state.
- `CompactPartitions` is idempotent and safe under concurrent reads.

**Approach:**
1. Write a test that opens a DuckDB-backed Badger, writes 100 keys, closes it, reopens it, and reads back the same keys.
2. Write a test that simulates a crash mid-flush by calling `os.Exit(1)` in a subprocess and verifying the database is recoverable.
3. Verify that `FlushAllPending()` is called on `DB.Close()` so no data is lost on graceful shutdown.

**Files to change:** New test file `db_duckdb_persistence_test.go`, possibly `db.go` (ensure `FlushAllPending` is in the close sequence).

---

### Task 6 — Full Standard Test Suite Compatibility (Priority: Low)

**Problem:** Many standard Badger tests (e.g., the bank Jepsen-style tests in `managed_db_test.go`, the level/compaction tests) were written assuming `uint64` versions and have not been run or audited with `types.CustomTs`.

**Approach:**
1. Run the full test suite with `-tags duckdb` and catalog all failures.
2. Run the full test suite **without** `-tags duckdb` and catalog all failures (currently blocked by Task 1).
3. Fix failures one by one, starting with the most impactful.

---

### Task 7 — Production Divy Oracle Integration (Priority: Future)

**Problem:** All tests use the in-process `divytime.Oracle`. The real Divy service issues timestamps over the network (gRPC or custom protocol). The `epochBatchOracle` introduced in `db_duckdb_epoch_stress_test.go` is the right abstraction to bridge these.

**Approach:**
1. Define a `TimestampOracle` interface in `divytime/` with a single method `GetTimestamp(epochID int64) (Timestamp, error)`.
2. Implement `LocalOracle` (current in-process stub) and `RemoteOracle` (gRPC client to Divy).
3. Thread the oracle through the DB open path so `OpenManaged` accepts an optional `TimestampOracle` instead of always using the in-process one.
4. Benchmark the remote oracle to confirm the epoch-batching speedup observed locally also applies to the real service.

---

## 8. File Reference Card

```
darshan-badger/
├── duckdb/
│   └── storage.go          ← DuckDB storage engine (build tag: duckdb)
├── divytime/
│   └── divytime.go         ← In-process timestamp oracle
├── types/
│   └── custom_ts.go        ← 3-tuple timestamp type
├── db.go                   ← DB struct + duckDBIface interface
├── db_duckdb_impl.go       ← Adapter: Badger types → DuckDB types (build tag: duckdb)
├── db_duckdb_stub.go       ← No-op stubs (build tag: !duckdb)
├── txn.go                  ← MVCC oracle + Txn + PrefetchKeys
├── options.go              ← Options.UseDuckDB, Options.PartitionFanOut
├── managed_db.go           ← OpenManaged (entry point for DuckDB tests)
├── db_duckdb_helpers_test.go          ← withDuckDB helper
├── db_duckdb_integration_test.go      ← Basic open/write/read
├── db_duckdb_correctness_test.go      ← Timestamp-scenario suite
├── db_duckdb_bank_test.go             ← Bank concurrent workload
├── db_duckdb_smallbank_bench_test.go  ← SmallBank 6-type benchmark
├── db_duckdb_serial_correctness_test.go  ← Serial correctness (Darshan)
├── db_duckdb_epoch_stress_test.go        ← Epoch stress (Ashley)
├── db_duckdb_comparison_test.go          ← Badger vs DuckDB comparison
├── db_lockfree_test.go                   ← Regular Badger tests
├── oracle_fastpath_test.go               ← Oracle fast path tests
├── DUCKDB_TESTING.md                     ← Test guide
└── COPILOT_BATCH_READ_TASK.md            ← Historical task description for ReadBatch
```

---

## 9. Quick Setup for a New Claude Instance

When you start a new conversation, run these commands to orient yourself:

```bash
# Verify the environment
cd /Users/AshleyLuo1/GolandProjects/darshan-badger
go build -tags duckdb ./...         # should produce no output
go vet -tags duckdb ./... 2>&1 | grep -v "publisher\|trie\|db_test"  # only pre-existing issues

# Run a fast sanity check (< 5 seconds)
go test -v -tags duckdb -run TestDuckDBBankSerialCorrectness -timeout 30s

# Check the git log
git log --oneline -10
```

The repo is on branch `duckdb-integration` at `github.com/vaishnaviikv/badger`. The default branch is `main`. All DuckDB work lives on `duckdb-integration`.

When writing new code for this project:
- Use `//go:build duckdb` at the top of any new file that imports the `duckdb` package or uses DuckDB-specific types.
- Use `types.CustomTs` (not `uint64`) for all version/timestamp fields.
- Use `divyToTs(ts)` to convert from `divytime.Timestamp` to `types.CustomTs`.
- Use `withDuckDB(t, true, func(db *DB) { ... })` for DuckDB tests, `withDB(t, true, ...)` for regular Badger tests.
- `OpenManaged` is the entry point, not `Open`, because managed mode allows externally-set timestamps.
