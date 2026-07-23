# DuckDB Stress Summary (128-worker sweep)

Run directory: artifacts/duckdb/20260722_212116_stress128
Date: 2026-07-22

## 1) Bank Stress (TestDuckDBBankStress)

Updated test now sweeps workers: 4, 8, 16, 32, 64, 128.
Invariant held in every subtest: total=1,000,000.

### No delay

| Workers | Total TPS | Transfer TPS | Transfer avg | Transfer p90 | SUM avg |
|---:|---:|---:|---:|---:|---:|
| 4 | 1190 | 805 | 2.539ms | 4.638ms | 29ms |
| 8 | 1586 | 1143 | 4.554ms | 8.237ms | 31ms |
| 16 | 1510 | 1059 | 10.772ms | 18.541ms | 38ms |
| 32 | 1538 | 1061 | 23.266ms | 35.425ms | 42ms |
| 64 | 1456 | 1019 | 50.322ms | 73.19ms | 55ms |
| 128 | 1430 | 1025 | 103.384ms | 152.695ms | 79ms |

### 50us oracle delay

| Workers | Total TPS | Transfer TPS | Transfer avg | Transfer p90 | SUM avg |
|---:|---:|---:|---:|---:|---:|
| 4 | 1402 | 979 | 2.302ms | 3.896ms | 25ms |
| 8 | 1384 | 976 | 5.168ms | 9.232ms | 32ms |
| 16 | 1532 | 1079 | 10.895ms | 17.575ms | 36ms |
| 32 | 1389 | 973 | 25.38ms | 39.559ms | 43ms |
| 64 | 1348 | 941 | 53.904ms | 76.774ms | 58ms |
| 128 | 1360 | 944 | 109.417ms | 154.913ms | 81ms |

Takeaway: throughput peaks around 8-16 workers and then flattens/declines; latency rises sharply beyond 32 workers.

## 2) Read-Heavy Concurrency Matrix (100k/150k/200k, up to 128 workers)

CSV: artifacts/duckdb/20260722_212116_stress128/readheavy_crossover_concurrency_extended.csv

### Peak DuckDB ops/s by cardinality

| Customers | Peak DuckDB ops/s | Worker at peak ops | Peak ratio DuckDB/Badger | Worker at peak ratio |
|---:|---:|---:|---:|---:|
| 100000 | 3272.9 | 64 | 6.33x | 16 |
| 150000 | 2385.8 | 8 | 7.33x | 16 |
| 200000 | 1508.2 | 16 | 9.64x | 128 |

Notable detail at 100000 customers: moving 64 -> 128 reduced DuckDB ops/s from 3272.9 to 2959.2 (about -9.6%), indicating diminishing returns beyond 64 on this run.

## 3) Interpretation for next tuning pass

- Scheduler/contended-region pressure is visible at high worker counts (latency growth and ops plateau).
- For read-heavy tests, practical operating region appears around 16-64 workers depending on customer cardinality.
- 128 workers is valuable as a stress ceiling and ratio datapoint, but not the best absolute throughput tier in this run.
