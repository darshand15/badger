# CloudLab One-Command Runbook

This runbook uses profile-driven scripts so hostnames can change between experiments without code edits.

## 1) Set host mapping once per experiment

```bash
cd /Users/AshleyLuo1/GolandProjects/darshan-badger
cp scripts/cloudlab_profile.env.example scripts/cloudlab_profile.env
```

Edit `scripts/cloudlab_profile.env` with the current CloudLab nodes:

- `PC_DIRECTORY`
- `PC_MONITORING`
- `PC_BROKERS`
- `PC_SERVERS`
- optional `PC_CLIENT`

## 2) One-command cluster + BenchBase run

```bash
cd /Users/AshleyLuo1/GolandProjects/darshan-badger
scripts/cloudlab_orchestrate.sh full
```

What `full` does:

1. Rewrites `lock-free-machine/scripts/env-vars.sh` from profile.
2. Rewrites `benchbase-sqlite/config/lockfreedb/sample_smallbank_config.xml` broker hosts from profile.
3. SSH-probes all nodes.
4. Starts monitoring + directory + brokers + servers.
5. Runs BenchBase SmallBank.

## 3) One-command 10M / 40M / 100M runs

10M full-key mode:

```bash
cd /Users/AshleyLuo1/GolandProjects/darshan-badger
JULY30_DUCKDB_SEED_KEY_MODE=full JULY30_DUCKDB_READ_HEAVY_KEY_MODE=full \
scripts/cloudlab_orchestrate.sh run-10m
```

40M full-key mode:

```bash
cd /Users/AshleyLuo1/GolandProjects/darshan-badger
JULY30_DUCKDB_SEED_KEY_MODE=full JULY30_DUCKDB_READ_HEAVY_KEY_MODE=full \
scripts/cloudlab_orchestrate.sh run-40m
```

10M + 40M back-to-back:

```bash
cd /Users/AshleyLuo1/GolandProjects/darshan-badger
JULY30_DUCKDB_SEED_KEY_MODE=full JULY30_DUCKDB_READ_HEAVY_KEY_MODE=full \
scripts/cloudlab_orchestrate.sh run-10m-40m
```

100M full-key mode:

Full-key mode:

```bash
cd /Users/AshleyLuo1/GolandProjects/darshan-badger
JULY30_DUCKDB_SEED_KEY_MODE=full JULY30_DUCKDB_READ_HEAVY_KEY_MODE=full \
scripts/cloudlab_orchestrate.sh run-100m
```

Reduced-key mode:

```bash
cd /Users/AshleyLuo1/GolandProjects/darshan-badger
JULY30_DUCKDB_SEED_KEY_MODE=checking-only JULY30_DUCKDB_READ_HEAVY_KEY_MODE=checking-only \
scripts/cloudlab_orchestrate.sh run-100m
```

## 4) Numeric monitoring outputs (no dashboards required)

Single snapshot:

```bash
cd /Users/AshleyLuo1/GolandProjects/darshan-badger
scripts/cloudlab_collect_numbers.sh
```

Interval summary (min/avg/max):

```bash
cd /Users/AshleyLuo1/GolandProjects/darshan-badger
INTERVAL_SEC=15 DURATION_SEC=300 scripts/cloudlab_monitor_loop.sh
```

Outputs are under `artifacts/cloudlab/...` as CSV + Markdown.

## 5) Distributed reads-from trace and replay

For a correctness run, enable tracing before starting the cluster:

```bash
cd /Users/AshleyLuo1/GolandProjects/darshan-badger
TRACE_READS_FROM=true TRACE_FINAL_STATE=true \
scripts/cloudlab_orchestrate.sh prepare
```

The server writes transaction traces under `logs/reads_from/` and a final-state
snapshot at shutdown. After collecting the logs into a result directory, run:

```bash
python3 /Users/AshleyLuo1/GolandProjects/lock-free-machine/scripts/check_reads_from.py \
  /Users/AshleyLuo1/GolandProjects/lock-free-machine/scripts/results/<run>
```

The checker replays committed transactions in `CustomTs` order and reports
read/version mismatches, duplicate records, and final-state mismatches.
