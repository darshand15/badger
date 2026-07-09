# DuckDB Backend Findings (Short Meeting Brief)

Date: 2026-07-09  
Branch: duckdb-integration

## What Was Completed

- Reduced avoidable overhead in DuckDB read and write paths.
- Productized the experiment harness and reporting pipeline.
- Added crossover sweeps (cardinality and concurrency) with CSV outputs.
- Added flush-batch tuning sweep for Ashley track.
- Added nightly/manual compare workflow for continuous tracking.

## Main Results

### A) Point-KV transfer workload (bank)

- No delay run:
  - Badger TPS: 16152
  - DuckDB TPS: 5598
  - DuckDB/Badger: 0.347 (Badger is about 2.88x faster)
- 50 us oracle delay run:
  - Badger TPS: 13083
  - DuckDB TPS: 5298
  - DuckDB/Badger: 0.405 (Badger is about 2.47x faster)
- Explanation:
  - In point-KV mode, operations are short and lookup/update overhead dominates; Badger's in-memory KV path remains lower-latency for this pattern.

### B) Read-heavy crossover (Balance transaction)

- Cardinality sweep (DuckDB/Badger ratio):
  - 1000 customers: 0.024763
  - 5000 customers: 0.130736
  - 20000 customers: 0.610637
  - 100000 customers: 4.183315
- Exact crossover point in this run: 100000 customers.
- At 100000 customers (single sweep):
  - Badger ops/s: 744.132
  - DuckDB ops/s: 3112.938
  - Ratio: 4.183315
- Explanation:
  - As key-space cardinality grows, point-lookup locality drops for Badger while DuckDB's scan/aggregation style execution benefits read-heavy access, causing a clear crossover.

### C) Concurrency effect at high cardinality (100000 customers)

- 4 workers: ratio 2.864242
- 8 workers: ratio 4.090557
- 16 workers: ratio 5.421668
- 32 workers: ratio 5.091182
- Explanation:
  - DuckDB's advantage increases with concurrency up to 16 workers in this run, then slightly tapers at 32 due to contention/overheads.

### D) Flush-batch threshold sweep (Ashley write-path tuning)

- DuckDB bank TPS averages by BADGER_DUCKDB_FLUSH_BATCH_SIZE:
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
- Explanation:
  - Small flush thresholds (1 to 4) are best for this workload mix; very large thresholds delay visibility/flush timing enough to reduce observed TPS.

## Deliverables Produced

- Comparison and sweep outputs:
  - readheavy_crossover.csv
  - readheavy_crossover_concurrency.csv
  - compare_summary.md
- Automation:
  - scripts/duckdb_experiments.sh
  - scripts/duckdb_compare_report.sh
  - .github/workflows/ci-duckdb-compare-nightly.yml
- Latest artifact runs:
  - artifacts/duckdb/20260708_211239
  - artifacts/duckdb/20260708_234955

## Recommendation

- Present this as two operating modes, not one winner:
  - OLTP point-KV mode: Badger-favored.
  - Read-heavy/high-cardinality mode: DuckDB-favored.
- Keep a conservative flush-batch default (1 to 4 range), and tune by workload profile.
