# Read Pool Sweep Summary (100k customers, 128 workers)

Run directory: artifacts/duckdb/20260722_212116_stress128

## Sweep configuration

- Test: TestReadHeavyBalanceCardinalityConcurrencySweepBadgerVsDuckDB
- Fixed cardinality: 100000
- Fixed workers: 128
- Sweep variable: BADGER_DUCKDB_READ_POOL_SIZE in {2, 4, 8, 12}

## Results

Source CSV: artifacts/duckdb/20260722_212116_stress128/readpool_sweep_100k_128.csv

| Read pool size | Badger ops/s | DuckDB ops/s | DuckDB/Badger |
|---:|---:|---:|---:|
| 2 | 533.5 | 3220.5 | 6.04x |
| 4 | 407.0 | 3872.6 | 9.51x |
| 8 | 539.3 | 3987.0 | 7.39x |
| 12 | 541.2 | 4076.0 | 7.53x |

## Interpretation

- DuckDB throughput increased monotonically across the tested pool sizes (3220.5 -> 4076.0 ops/s from pool=2 -> 12), consistent with read-pool pressure at high worker count.
- The largest gain came moving from pool=2 to pool=4; improvements above pool=8 were smaller.
- This suggests the 128-worker flattening is at least partly read-pool-limited, and pool sizes around 8-12 are a better operating region on this machine.

## Notes

- Badger baseline fluctuates run-to-run in this micro-sweep due to noisy short-duration runs; the primary signal here is DuckDB absolute ops/s versus pool size.
- Per-pool raw logs are stored as:
  - readpool_2.log
  - readpool_4.log
  - readpool_8.log
  - readpool_12.log
