# DuckDB-Integrated Badger — Testing Guide

**Branch:** `duckdb-integration`  
**Repository:** `github.com/vaishnaviikv/badger`

---

## Overview

This branch adds a DuckDB storage backend to BadgerDB.  Writes flow through
Badger's memtable as usual and are flushed into DuckDB partitions instead of
L-SMT SST files.  Reads check the pending-write buffer first, then fall back
to DuckDB SQL queries — so the full MVCC read path is preserved.

The custom timestamp type is a **3-tuple (EpochID, BrokerID, AssignedTs)**
issued by the `divytime` oracle (in-process for tests, a remote ordering
service in production).

---

## Prerequisites

```bash
# DuckDB CGo driver
go get github.com/marcboeker/go-duckdb

# All DuckDB-tagged code requires the build tag
go test -tags duckdb ./...
```

> All test commands below assume the repository root
> `/Users/AshleyLuo1/GolandProjects/darshan-badger` as the working directory.

---

## Standard Experiment Harness

Use the shared harness to run reproducible experiment tracks and collect
timestamped logs/profiles under `artifacts/duckdb/<run-id>/`:

```bash
# smoke validation (correctness + concurrency)
make duckdb-smoke

# side-by-side Badger vs DuckDB comparisons
make duckdb-compare

# read-heavy crossover sweep (Balance txn across customer cardinalities)
go test -v -tags duckdb -run TestReadHeavyBalanceCardinalitySweepBadgerVsDuckDB -timeout 600s

# epoch batching sweeps
make duckdb-epoch

# CPU profile + TPS benchmark runs
make duckdb-profile

# lock-free ingest baseline comparison
make duckdb-lockfree-compare

# Ashley track only (overhead-focused)
make duckdb-ashley

# Ashley read-pool tuning sweep
make duckdb-ashley-readpool-sweep

# run all tracks in one pass
make duckdb-full
```

Direct script usage is also available:

```bash
bash scripts/duckdb_experiments.sh <smoke|compare|epoch|profile|lockfree-compare|ashley|ashley-readpool-sweep|full>
```

Read-pool tuning controls:

```bash
# Per-process read-pool size per partition (default: 2, clamp: 1..64)
export BADGER_DUCKDB_READ_POOL_SIZE=8

# Sweep sizes used by make duckdb-ashley-readpool-sweep
export READ_POOL_SWEEP_SIZES="1 2 4 8 12"
```

The harness automatically adds classic Darwin linker flags for profile runs to
avoid noisy `LC_DYSYMTAB` warnings on macOS arm64.

---

## Test Inventory

### 1. Correctness Tests (Darshan)

#### Serial execution correctness — bank workload

```bash
go test -v -tags duckdb -run TestDuckDBBankSerialCorrectness -timeout 120s
```

**What it does:**  
Runs 500 bank transfers entirely single-threaded.  After every transfer:

1. The pre-commit reads are compared against an in-memory reference map.
2. The post-commit reads are compared against the reference map.
3. At the end the global balance invariant `sum(all accounts) = N × initial_balance`
   is asserted.

Because there is no concurrency, snapshot isolation is equivalent to
serializable isolation — **the invariant must hold exactly** (no write-skew
is possible).

**Expected output:**

```
PASS: serial bank correctness: 500/500 transfers completed,
      global total=200000 (invariant holds exactly)
```

---

#### Serial execution correctness — SmallBank transaction load

```bash
go test -v -tags duckdb -run TestDuckDBSmallBankSerialCorrectness -timeout 300s
```

**What it does:**  
Runs each of the 6 SmallBank transaction types (Balance, DepositChecking,
TransactSavings, SendPayment, WriteCheck, Amalgamate) serially against 500
seeded accounts.  After each commit, the affected keys are read back and
compared to an in-memory reference state.

| Transaction      | Invariant checked                                                    |
|------------------|----------------------------------------------------------------------|
| Balance          | reads match reference `savings[i] + checking[i]`                    |
| DepositChecking  | `checking[i] = old + sbTxAmount`                                     |
| TransactSavings  | `savings[i] = old − sbTxAmount` (when affordable)                   |
| SendPayment      | total checking balance across `src + dst` is preserved               |
| WriteCheck       | `checking[i]` decreases by `sbTxAmount` (matches implementation)    |
| Amalgamate       | `checking[src]=0`, `savings[dst] = old_savings[src]+old_checking[dst]` |

A final global scan confirms every account exactly matches the reference.

**Expected output:**

```
[smallbank-serial] Balance: PASS
[smallbank-serial] DepositChecking: PASS
[smallbank-serial] TransactSavings: PASS
[smallbank-serial] SendPayment: PASS
[smallbank-serial] WriteCheck: PASS
[smallbank-serial] Amalgamate: PASS
[smallbank-serial] PASS — all accounts match reference state exactly
```

---

#### Existing timestamp-scenario tests (DuckDB path)

```bash
go test -v -tags duckdb -run TestDuckDBTimestampScenarios -timeout 60s
```

Mirrors the regular Badger `TestTimestampScenarios` through the DuckDB read
path (pending-write buffer + SQL query) to verify:

- basic overwrite / ascending timestamps
- snapshot reads at arbitrary points in time
- delete tombstones
- flush + compaction scenarios
- partitioned fan-out

---

#### Bank workload — concurrent correctness

```bash
# Zero oracle delay (best-case throughput)
go test -v -tags duckdb -run TestDuckDBBankDivytime -timeout 60s

# Simulated 50 µs oracle delay (production-like)
go test -v -tags duckdb -run TestDuckDBBankDivytimeSimulatedDelay -timeout 120s
```

Both tests verify the final balance invariant.  Because these tests use
snapshot isolation with concurrent writers, **small write-skew deltas are
expected and documented** (each lost unit equals one transfer amount).

---

### 2. Epoch Stress Test — maximise transactions per epoch (Ashley)

```bash
go test -v -tags duckdb -run TestDuckDBBankEpochStress -timeout 180s
```

**What it does:**  
In production the ordering service adds a fixed network latency per timestamp
request (typically 50–200 µs).  *Epoch batching* amortises this latency across
N transactions: one oracle call reserves a contiguous block of `AssignedTs`
slots; all N transactions in the batch use one slot each without waiting.

The test sweeps batch sizes `[1, 2, 4, 8, 16, 32]` with a 50 µs simulated
oracle delay and 16 concurrent workers, printing TPS and p90 latency for each.

**Sample output (local run):**

```
=== DuckDB Epoch Stress Results ===
  Oracle simulated delay: 50µs
  Workers: 16  |  Run duration per batch size: 5s

  BatchSize     TPS             p90 Latency
  --------------------------------------------
  1             3663            5.295ms
  2             2464            9.331ms
  4             3950            6.447ms
  8             4932            5.322ms
  16            5750            4.857ms
  32            7249            4.06ms

PASS: 2.0x TPS improvement (batchSize=32 vs 1)
```

**Zero-delay control** (shows pure DuckDB ceiling, batch size effect minimal):

```bash
go test -v -tags duckdb -run TestDuckDBBankEpochStressNoDelay -timeout 180s
```

---

### 3. Badger vs DuckDB Comparison (All)

#### Bank transfer workload

```bash
# Zero oracle delay
go test -v -tags duckdb -run TestBankBadgerVsDuckDB$ -timeout 120s

# 50 µs oracle delay (shows DuckDB relative advantage shrinking)
go test -v -tags duckdb -run TestBankBadgerVsDuckDBWithDelay -timeout 120s
```

**Sample output (local run):**

```
=== Backend Comparison: Bank Transfer Workload ===
  Backend                     TPS           Avg Latency   p90 Latency
  -------------------------------------------------------------------------------------
  Badger                      5131          3.116ms       6.634ms
  DuckDB                      3931          4.069ms       7.180ms

  Regular Badger is 1.31x faster than DuckDB (expected for in-memory workload)
```

Regular Badger's pure in-memory LSM gives it a latency advantage at zero
oracle delay.  The gap narrows (and may reverse) when oracle latency
dominates, because DuckDB's epoch batching then matters more.

---

#### SmallBank mixed workload (6 transaction types, BenchBase weights)

```bash
go test -v -tags duckdb -run TestSmallBankBadgerVsDuckDB -timeout 300s
```

Runs the BenchBase-weighted mixed workload (Amalgamate/Balance/DepositChecking/
SendPayment/TransactSavings/WriteCheck at 15/15/15/25/15/15) on both backends
and prints per-type TPS and mean latency side by side.

---

#### Per-type SmallBank isolation benchmark (DuckDB only)

```bash
go test -v -tags duckdb -run TestSmallBankDuckDB -timeout 300s
```

Runs each of the 6 transaction types in isolation (16 workers, 5 s each) and
prints latency percentiles + TPS — directly comparable to Divy's
regular-badger vs dd\_exp table.

---

### 4. Integration & Ingest Tests

```bash
# Basic flush + read-back
go test -v -tags duckdb -run TestDuckDBIntegration -timeout 60s

# Parallel ingest: DuckDB vs regular Badger (benchstat-compatible)
go test -v -tags duckdb -bench BenchmarkLockFreeIngest -benchtime 10s
go test -v -tags duckdb -bench BenchmarkLockFreeIngest_DuckDB -benchtime 10s
```

---

## Running All DuckDB Tests

```bash
go test -v -tags duckdb -timeout 600s ./...
```

> The full suite takes roughly 5–8 minutes locally (dominated by the SmallBank
> seeding phase which writes 100 000 accounts).

---

## Running Without DuckDB (Regular Badger, No Locks)

The files in `db_lockfree_test.go` exercise the regular Badger backend with
the lock-free ingest path:

```bash
# Correctness: latest-timestamp-wins, non-blocking writers, timestamp scenarios
go test -v -run 'TestLatestWins|TestWritersAreNonBlocking|TestTimestampScenarios' -timeout 60s

# Ingest benchmark (compare with BenchmarkLockFreeIngest_DuckDB above)
go test -bench BenchmarkLockFreeIngest -benchtime 10s
```

These tests run **without** the `duckdb` build tag and exercise only the
standard Badger MVCC path.

---

## Test Matrix (with and without locks)

| Test suite | Build tag | Locking model | Purpose |
|---|---|---|---|
| `TestLatestWins`, `TestWritersAreNonBlocking` | *(none)* | Badger OCC (lock-free writes) | Baseline MVCC correctness |
| `TestTimestampScenarios` | *(none)* | Badger OCC | Snapshot-read correctness |
| `TestDuckDBTimestampScenarios` | `duckdb` | DuckDB + pending-write buffer | Same scenarios via DuckDB path |
| `TestDuckDBBankSerialCorrectness` | `duckdb` | Serial (no concurrency) | Reference-state exact match |
| `TestDuckDBSmallBankSerialCorrectness` | `duckdb` | Serial | Per-op invariant verification |
| `TestDuckDBBankDivytime` | `duckdb` | Concurrent SI | Final-balance check |
| `TestDuckDBBankEpochStress` | `duckdb` | Concurrent + epoch batching | Throughput vs batch size |
| `TestBankBadgerVsDuckDB` | `duckdb` | Concurrent SI | Badger vs DuckDB TPS comparison |
| `TestSmallBankBadgerVsDuckDB` | `duckdb` | Concurrent SI | Per-type mixed workload comparison |

---

## Key Findings

1. **Serial correctness is exact.**  
   With no concurrency, every committed transfer matches the reference map and
   the global balance invariant holds exactly on both the bank and SmallBank
   workloads.

2. **Concurrent correctness under snapshot isolation.**  
   The concurrent bank test may observe small write-skew deltas (each lost unit
   = one transfer amount).  This is the expected and documented trade-off of
   snapshot isolation vs. serializable isolation.

3. **Epoch batching improves TPS ~2× at 50 µs oracle delay.**  
   Amortising one 50 µs oracle round-trip across 32 transactions doubles
   throughput from ~3 600 to ~7 200 TPS with 16 workers.  The optimal batch
   size grows with oracle latency.

4. **Badger is ~1.3× faster than DuckDB at zero oracle delay.**  
   This is expected — Badger's in-memory LSM avoids CGo overhead.  The gap
   narrows when oracle latency dominates (production scenario).

---

## File Map

| File | Description |
|------|-------------|
| `db_duckdb_serial_correctness_test.go` | **Serial correctness tests** (Darshan) |
| `db_duckdb_stress_test.go` | **Stress + epoch batching stress tests** (Ashley) |
| `db_duckdb_comparison_test.go` | **Badger vs DuckDB comparison** (All) |
| `db_duckdb_bank_test.go` | Concurrent bank workload + TPS benchmark |
| `db_duckdb_smallbank_bench_test.go` | SmallBank per-type and mixed workloads |
| `db_duckdb_correctness_test.go` | Timestamp-scenario + integration correctness tests |
| `db_lockfree_test.go` | Regular Badger lock-free tests + `BenchmarkLockFreeIngest` |
| `duckdb/storage.go` | DuckDB storage layer implementation |
| `divytime/divytime.go` | In-process timestamp oracle |
