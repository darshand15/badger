#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-${ROOT_DIR}/artifacts/duckdb/july30}"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="${ARTIFACT_ROOT}/${RUN_ID}"

mkdir -p "${OUT_DIR}"

log() {
  echo "[$(date +%H:%M:%S)] $*"
}

run_cmd() {
  local name="$1"
  shift
  local logfile="${OUT_DIR}/${name}.log"
  log "Running ${name}"
  (
    cd "${ROOT_DIR}"
    "$@"
  ) 2>&1 | tee "${logfile}"
}

write_env() {
  {
    echo "run_id=${RUN_ID}"
    echo "root_dir=${ROOT_DIR}"
    echo "out_dir=${OUT_DIR}"
    echo "go_version=$(go version)"
    echo "uname=$(uname -a)"
    echo "BADGER_DUCKDB_READ_POOL_SIZE=${BADGER_DUCKDB_READ_POOL_SIZE:-}"
    echo "BADGER_DUCKDB_FLUSH_BATCH_SIZE=${BADGER_DUCKDB_FLUSH_BATCH_SIZE:-}"
    echo "BADGER_DUCKDB_SEED_BATCH_SIZE=${BADGER_DUCKDB_SEED_BATCH_SIZE:-}"
    echo "BADGER_DUCKDB_SEED_KEY_MODE=${BADGER_DUCKDB_SEED_KEY_MODE:-}"
    echo "BADGER_DUCKDB_READ_HEAVY_KEY_MODE=${BADGER_DUCKDB_READ_HEAVY_KEY_MODE:-}"
    echo "BADGER_DUCKDB_PARTITION_FANOUT=${BADGER_DUCKDB_PARTITION_FANOUT:-}"
    echo "BADGER_DUCKDB_READ_POOL_SIZE=${BADGER_DUCKDB_READ_POOL_SIZE:-}"
  } >"${OUT_DIR}/env.txt"
}

gate() {
  run_cmd gate_build go build -tags duckdb ./...
  run_cmd gate_crash_suite go test -v -tags duckdb -run 'TestDuckDBCrash' -timeout 300s .
  run_cmd gate_saturation go test -v -tags duckdb -run '^TestDuckDBSaturationProbe$' -timeout 1200s .
  run_cmd gate_soak_short env BADGER_DUCKDB_SOAK_DURATION=30s BADGER_DUCKDB_SOAK_CHECK_INTERVAL=5s \
    go test -v -tags duckdb -run '^TestDuckDBBankSoak$' -timeout 600s .
}

smallbank() {
  run_cmd smallbank_serial go test -v -tags duckdb -run '^TestDuckDBSmallBankSerialCorrectness$' -timeout 300s .
  run_cmd smallbank_isolation go test -v -tags duckdb -run '^TestSmallBankDuckDB$' -timeout 300s .
  run_cmd smallbank_mixed go test -v -tags duckdb -run '^TestSmallBankDuckDBMixed$' -timeout 300s .
  run_cmd smallbank_phases go test -v -tags duckdb -run '^TestSmallBankDuckDBPhases$' -timeout 300s .
  run_cmd smallbank_badger_vs_duckdb go test -v -tags duckdb -run '^TestSmallBankBadgerVsDuckDB$' -timeout 300s .
}

bank_compare() {
  run_cmd bank_badger_vs_duckdb go test -v -tags duckdb -run '^TestBankBadgerVsDuckDB$' -timeout 180s .
  run_cmd bank_badger_vs_duckdb_delay go test -v -tags duckdb -run '^TestBankBadgerVsDuckDBWithDelay$' -timeout 180s .
}

read_heavy_matrix() {
  run_cmd readheavy_cardinality_50k_200k env BADGER_DUCKDB_SWEEP_CSV="${OUT_DIR}/readheavy_50k_200k.csv" \
    BADGER_DUCKDB_SWEEP_CARDINALITIES="50000,200000" \
    go test -v -tags duckdb -run '^TestReadHeavyBalanceCardinalitySweepBadgerVsDuckDB$' -timeout 900s .

  run_cmd readheavy_concurrency_50k_200k env BADGER_DUCKDB_SWEEP_CONC_CSV="${OUT_DIR}/readheavy_concurrency_50k_200k.csv" \
    BADGER_DUCKDB_SWEEP_CONC_CARDINALITIES="50000,200000" BADGER_DUCKDB_SWEEP_WORKERS="16,32,64,128,256,512" \
    go test -v -tags duckdb -run '^TestReadHeavyBalanceCardinalityConcurrencySweepBadgerVsDuckDB$' -timeout 1500s .
}

large_data_probe() {
  # Optional heavy run for paper-scale cardinalities. This is intentionally
  # gated behind env to avoid accidental multi-hour local runs.
  local card="${JULY30_DUCKDB_LARGE_CARDINALITY:-}"
  local probe_timeout="${JULY30_DUCKDB_LARGE_TIMEOUT:-21600s}"
  local seed_batch="${JULY30_DUCKDB_SEED_BATCH_SIZE:-}"
  local seed_mode="${JULY30_DUCKDB_SEED_KEY_MODE:-}"
  local read_mode="${JULY30_DUCKDB_READ_HEAVY_KEY_MODE:-}"
  local partition_fanout="${JULY30_DUCKDB_PARTITION_FANOUT:-}"
  local read_pool_size="${JULY30_DUCKDB_READ_POOL_SIZE:-}"
  if [[ -z "${card}" ]]; then
    log "Skipping large-data probe (set JULY30_DUCKDB_LARGE_CARDINALITY=10000000 or 100000000 to enable)"
    return 0
  fi

  if [[ -z "${seed_batch}" ]]; then
    if (( card >= 40000000 )); then
      seed_batch=2000
    else
      seed_batch=1000
    fi
  fi

  if [[ -z "${seed_mode}" && ${card} -ge 100000000 ]]; then
    seed_mode="checking-only"
  fi
  if [[ -z "${read_mode}" && ${card} -ge 100000000 ]]; then
    read_mode="checking-only"
  fi

  # Full-key 100M runs can exceed local memory due to many per-partition
  # dedicated read connections. Reduce fan-out and read-pool size for this
  # stress tier unless explicitly overridden by env.
  if [[ ${card} -ge 100000000 ]]; then
    if [[ "${seed_mode}" == "full" || "${read_mode}" == "full" ]]; then
      if [[ -z "${partition_fanout}" ]]; then
        partition_fanout=4
      fi
      if [[ -z "${read_pool_size}" ]]; then
        read_pool_size=1
      fi
    fi
  fi

  run_cmd large_data_saturation env BADGER_DUCKDB_SATURATION_CARDINALITY="${card}" \
    BADGER_DUCKDB_SEED_BATCH_SIZE="${seed_batch}" \
    BADGER_DUCKDB_SEED_KEY_MODE="${seed_mode}" \
    BADGER_DUCKDB_READ_HEAVY_KEY_MODE="${read_mode}" \
    BADGER_DUCKDB_PARTITION_FANOUT="${partition_fanout}" \
    BADGER_DUCKDB_READ_POOL_SIZE="${read_pool_size}" \
    BADGER_DUCKDB_SATURATION_WORKERS="32 64 128 256" \
    BADGER_DUCKDB_SATURATION_DURATION=3s \
    BADGER_DUCKDB_SATURATION_CSV="${OUT_DIR}/saturation_${card}.csv" \
    go test -v -tags duckdb -run '^TestDuckDBSaturationProbe$' -timeout "${probe_timeout}" .
}

write_matrix_stub() {
  cat >"${OUT_DIR}/paper_matrix_status.md" <<'EOF'
# July 30 Paper Matrix Status (DuckDB Track)

This file is generated by scripts/july30_duckdb_campaign.sh.

## Scope coverage in this repository

| Benchmark | DuckDB backend | Badger backend | Status in this repo |
|---|---|---|---|
| SmallBank | Yes | Yes (comparison test) | Runnable now |
| TPC-C | No in-repo harness | No in-repo harness | Missing integration |
| YCSB | No in-repo harness | No in-repo harness | Missing integration |

## Requested experiment axes (paper ask)

| Axis | Requested | In-repo support |
|---|---|---|
| Servers | 1, 2, 5 | 1-process test harness only |
| Transactions | 50,000 and 200,000 | Indirect via timed workload and cardinality sweeps |
| Data size | 10M and 100M | Partial (heavy saturation probe optional via env) |

## External competitor status (manual follow-up)

| Competitor | Expected owner | Status |
|---|---|---|
| PostgreSQL | Vaishnavi | Manual integration needed outside this repo |
| GeoGauss | Vaishnavi | Manual integration needed outside this repo |
| DeTock | Tejas | Manual integration needed outside this repo |
| Caracal | Tejas | Manual integration needed outside this repo |

EOF
}

usage() {
  cat <<'EOF'
Usage: scripts/july30_duckdb_campaign.sh <target>

Targets:
  gate              Build + crash + saturation + short soak gate
  smallbank         Run DuckDB/Badger SmallBank tests available in-repo
  bank-compare      Run bank comparison tests
  readheavy-matrix  Run 50k/200k read-heavy sweeps with concurrency matrix
  large-data-probe  Optional large-cardinality saturation probe
  full              Run all targets above (except large-data-probe unless env set)

Environment:
  ARTIFACT_ROOT                         Output root (default artifacts/duckdb/july30)
  JULY30_DUCKDB_LARGE_CARDINALITY       Optional: 10000000 or 100000000
  JULY30_DUCKDB_LARGE_TIMEOUT           Optional: Go test timeout for large probe (default 21600s)

Example:
  bash scripts/july30_duckdb_campaign.sh full
  JULY30_DUCKDB_LARGE_CARDINALITY=10000000 bash scripts/july30_duckdb_campaign.sh large-data-probe
EOF
}

main() {
  local target="${1:-}"
  if [[ -z "${target}" ]]; then
    usage
    exit 1
  fi

  write_env
  write_matrix_stub

  case "${target}" in
    gate)
      gate
      ;;
    smallbank)
      smallbank
      ;;
    bank-compare)
      bank_compare
      ;;
    readheavy-matrix)
      read_heavy_matrix
      ;;
    large-data-probe)
      large_data_probe
      ;;
    full)
      gate
      smallbank
      bank_compare
      read_heavy_matrix
      large_data_probe
      ;;
    *)
      usage
      exit 1
      ;;
  esac

  log "Done. Artifacts written to ${OUT_DIR}"
}

main "$@"
