# DuckDB Backend Findings (Meeting Summary)

Date: 2026-07-09
Branch: duckdb-integration

## Scope Completed

- Eliminated major avoidable overheads in the DuckDB backend path.
- Built a repeatable experiment harness for smoke, compare, epoch, profile, and Ashley tracks.
- Added dual-mode reporting for point-KV vs read-heavy analytical behavior.
- Added customer-cardinality and concurrency sweeps with CSV artifact export.
- Added flush-batch threshold sweep for write-path tuning.
- Added nightly/manual CI workflow to run compare and publish artifacts.

## Key Changes Delivered

- Read path tuning:
  - Per-partition read pool with env tuning via BADGER_DUCKDB_READ_POOL_SIZE.
  - Pending-key hash presence checks to avoid unnecessary flush/query overhead.
- Write/commit path improvements:
  - Lower overhead direct DuckDB append path behavior in transaction commit flow.
  - Hot key generation optimization in bank workload path.
- Experiment productization:
  - scripts/duckdb_experiments.sh compare now exports:
    - readheavy_crossover.csv
    - readheavy_crossover_concurrency.csv
    - compare_summary.md
  - New report script: scripts/duckdb_compare_report.sh
- New CI workflow:
  - .github/workflows/ci-duckdb-compare-nightly.yml

## Performance Findings

### 1) Point-KV transfer workloads

- Badger remains faster in in-memory point-transfer workloads.
- Typical observed gap: about 2.5x to 2.9x in favor of Badger.

### 2) Read-heavy high-cardinality workloads

- DuckDB crosses over and outperforms Badger at high customer cardinality.
- Crossover observed around 100000 customers.
- Observed DuckDB/Badger ratio at high cardinality reached about 4.1x to 5.4x, and up to about 6.0x for higher worker settings.

### 3) Flush-batch sweep (Ashley)

- Best results were at smaller flush thresholds (around 1 to 4).
- Large threshold values (for example 256) reduced bank TPS.

## Latest Artifact Runs

- artifacts/duckdb/20260708_211239
- artifacts/duckdb/20260708_234955

## Recommendation for Product Positioning

- Keep clear mode framing:
  - Point-KV OLTP mode: Badger favored.
  - Read-heavy/high-cardinality mode: DuckDB favored.
- Default to conservative flush-batch threshold for now (small value), with targeted tuning by workload.
- Continue collecting nightly compare data to track regressions and crossover stability.

## Risks / Open Items

- CI workflow dispatch requires workflow registration in the target repository default branch.
- For vaishnaviikv/badger, permission is currently denied for direct push from this account.

## Suggested Talking Points (1 minute)

- We reduced backend overheads and made performance evaluation repeatable.
- The data now clearly shows two workload regimes rather than one winner.
- DuckDB has a strong win region in read-heavy high-cardinality scenarios.
- We added automation and artifacts so this can be tracked continuously.
