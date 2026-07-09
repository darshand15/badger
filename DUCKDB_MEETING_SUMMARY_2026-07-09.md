# DuckDB Backend Findings (Meeting Summary)

Date: 2026-07-09  
Branch: duckdb-integration

## Executive Summary

- Badger remains faster for point-KV transfer workloads.
- DuckDB becomes strongly faster for read-heavy, high-cardinality workloads.
- The crossover in this run happens at 100000 customers.
- Tuned small flush-batch settings (1 to 4) perform best for the Ashley write-path track.
- Nightly compare automation is now running successfully and produces reproducible artifacts.

## Exact Performance Results

### 1) Point-KV transfer (bank)

- No delay:
  - Badger TPS: 16152
  - DuckDB TPS: 5598
  - DuckDB/Badger ratio: 0.347
  - Interpretation: Badger is about 2.88x faster in this mode.
- 50 us oracle delay:
  - Badger TPS: 13083
  - DuckDB TPS: 5298
  - DuckDB/Badger ratio: 0.405
  - Interpretation: Badger is about 2.47x faster.

Why this happens:
- Point-KV transfers are latency-sensitive, short transactions.
- Badger's direct in-memory KV path has lower per-operation overhead in this regime.

### 2) Read-heavy crossover (Balance)

- Cardinality sweep DuckDB/Badger ratios:
  - 1000 customers: 0.024763
  - 5000 customers: 0.130736
  - 20000 customers: 0.610637
  - 100000 customers: 4.183315
- Crossover point: 100000 customers
- At 100000 customers:
  - Badger ops/s: 744.132
  - DuckDB ops/s: 3112.938
  - Ratio: 4.183315

Why this happens:
- As key-space cardinality increases, lookup locality drops for Badger.
- DuckDB benefits from its execution model on read-heavy analytical patterns.

### 3) Concurrency effect at 100000 customers

- 4 workers: 2.864242
- 8 workers: 4.090557
- 16 workers: 5.421668
- 32 workers: 5.091182

Interpretation:
- DuckDB advantage increases up to 16 workers, then slightly tapers at 32 from added contention/overhead.

### 4) Flush-batch sweep (Ashley write-path tuning)

- DuckDB bank TPS by BADGER_DUCKDB_FLUSH_BATCH_SIZE:
  - 1: avg 5052.3 (min 5015, max 5125)
  - 4: avg 5072.7 (min 5028, max 5118)
  - 16: avg 4853.7 (min 4804, max 4886)
  - 64: avg 4905.3 (min 4895, max 4916)
  - 256: avg 4487.3 (min 4450, max 4508)
- Lockfree ingest ns/op by threshold:
  - 1: 2350
  - 4: 2509
  - 16: 2550
  - 64: 2610
  - 256: 2573

Interpretation:
- Small thresholds (1 to 4) are best for the current workload mix.
- Very large thresholds reduce observed transfer TPS.

## What Was Delivered

- Harness and reporting:
  - scripts/duckdb_experiments.sh
  - scripts/duckdb_compare_report.sh
- Compare outputs:
  - readheavy_crossover.csv
  - readheavy_crossover_concurrency.csv
  - compare_summary.md
- Artifact runs used in this summary:
  - artifacts/duckdb/20260708_211239
  - artifacts/duckdb/20260708_234955

## Next Steps

1. Point-KV optimization (priority 1)
- Goal: improve DuckDB no-delay transfer TPS from 5598 to at least 6200.
- Actions:
  - Keep BADGER_DUCKDB_FLUSH_BATCH_SIZE default at 4 for Ashley profile.
  - Add focused microbenchmark for commit + direct append path.
  - Reduce per-transfer allocations in key/value/timestamp handling.

2. Read-heavy validation (priority 2)
- Goal: keep DuckDB/Badger ratio >= 4.0 at 100000 customers and >= 5.0 near peak worker setting.
- Actions:
  - Add sweeps at 150000 and 200000 customers.
  - Add 64-worker point in concurrency matrix.

3. Nightly health signal (priority 3)
- Goal: detect regressions immediately from nightly artifacts.
- Actions:
  - Add warning threshold in summary generation when 100000-customer ratio < 3.5.
  - Write previous-run ratio metadata for day-over-day comparison.

## Double Verification Log

### Scope Verified

- Correctness: serial bank, serial smallbank, timestamp scenarios, integration read/write paths.
- Stress: concurrent bank stress, epoch stress with delay, epoch stress no-delay.
- Completeness: backend comparison tests (point mode, delay mode, smallbank mixed, read-heavy cardinality, read-heavy cardinality x concurrency).
- Oracle fast-path safety: full `TestOracleFastPath_*` suite including new tracker-registration regression test.
- Performance sanity: DuckDB bank TPS and lockfree ingest benchmarks.

### Commands Run

```bash
go test -tags duckdb -timeout 2400s -run 'TestDuckDBBankSerialCorrectness|TestDuckDBSmallBankSerialCorrectness|TestDuckDBTimestampScenarios|TestDuckDBIntegration|TestDuckDBBankStress|TestDuckDBBankEpochStress|TestDuckDBBankEpochStressNoDelay|TestBankBadgerVsDuckDB$|TestBankBadgerVsDuckDBWithDelay|TestSmallBankBadgerVsDuckDB|TestReadHeavyBalanceCardinalitySweepBadgerVsDuckDB|TestReadHeavyBalanceCardinalityConcurrencySweepBadgerVsDuckDB|TestOracleFastPath_.*' .

go test -tags duckdb -run '^$' -bench '^BenchmarkDuckDBBankTPS$|^BenchmarkLockFreeIngest_DuckDB$' -benchtime 5s -count 1 .
```

### Outcomes

- Full DuckDB test matrix above: PASS in 94.754s.
- Benchmark sanity run: PASS.
- BenchmarkDuckDBBankTPS: 5331 txns/sec.
- BenchmarkLockFreeIngest_DuckDB: 2532 ns/op, 1437 B/op, 25 allocs/op.

## Teammate Response (Copy/Paste)

You are correct to flag this. In `duckdb-integration`, when `newCommitTs()` took the managed fast path (`isManaged && !detectConflicts`), it returned before `duckDBTracker.begin(ts)`, so tracker registration could be skipped unless the caller had pre-registered externally.

I fixed this so the fast path also calls `duckDBTracker.begin(txn.commitTs)` before returning. This is safe because `begin()` is idempotent.

Final contract now is:

1. Keep using `RegisterPendingCommit(ts)` atomically with external timestamp issuance to close the issuance-to-commit window.
2. Also register in fast-path `newCommitTs()` as a safety net for any caller that did not pre-register.
3. `doneCommit(ts)` still removes the entry after DirectFlush, and abort paths still deregister.

I added regression coverage (`TestOracleFastPath_NewCommitTsRegistersTracker`) proving fast-path registration happens and `doneCommit()` deregisters correctly.

## Verbal Scripts

### 15-second version

Badger still wins point-KV transfers, but DuckDB now clearly wins read-heavy high-cardinality workloads. In our run, DuckDB crossed over at 100000 customers and reached 4.18x to 5.42x advantage depending on concurrency. We now have nightly compare automation and clear next optimization targets.

### 30-second version

Today, results show two distinct operating modes. For point-KV bank transfers, Badger remains faster: 16152 vs 5598 TPS with no delay, and 13083 vs 5298 with 50 us delay. For read-heavy Balance workloads, DuckDB crosses over at 100000 customers and reaches 3112.9 vs 744.1 ops per second, a 4.18x gain, with concurrency peaking at 5.42x at 16 workers. Flush-batch tuning also shows small values are best, with batch size 4 at about 5072.7 TPS. Next, we focus on lifting point-KV TPS to 6200+ while preserving the 4x+ read-heavy advantage.

### 60-second version

We completed the DuckDB Ashley track with repeatable benchmarking, crossover analysis, and CI automation. The key outcome is that this is not a single-winner story. In point-KV transfer mode, Badger is still better: no-delay throughput is 16152 TPS for Badger versus 5598 for DuckDB, and with 50 us delay it is 13083 versus 5298. In read-heavy mode, behavior flips at scale: cardinality sweep ratios move from 0.0248 at 1000 customers up to 4.1833 at 100000 customers, where DuckDB delivers 3112.9 ops/s versus Badger's 744.1. Concurrency amplifies that advantage to 5.4217x at 16 workers, then slightly tapers at 32. On write-path tuning, small flush-batch values are best: batch size 4 averages 5072.7 TPS, while 256 drops to 4487.3. We also validated nightly compare automation with a successful run in 2 minutes 25 seconds. Next steps are targeted: raise point-KV DuckDB TPS from 5598 to 6200+, extend read-heavy sweeps to 150k and 200k customers plus 64 workers, and add nightly regression thresholds for automatic warning signals.
