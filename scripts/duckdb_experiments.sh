#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-${ROOT_DIR}/artifacts/duckdb}"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="${ARTIFACT_ROOT}/${RUN_ID}"

if [[ "${OSTYPE:-}" == darwin* ]]; then
  LD_FLAGS=(-ldflags=-extldflags=-Wl,-ld_classic)
else
  LD_FLAGS=()
fi

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

ensure_duckdb_defaults() {
  if [[ -z "${BADGER_DUCKDB_FLUSH_BATCH_SIZE:-}" ]]; then
    export BADGER_DUCKDB_FLUSH_BATCH_SIZE=4
    log "BADGER_DUCKDB_FLUSH_BATCH_SIZE not set; defaulting to 4"
  fi
}

print_env() {
  {
    echo "run_id=${RUN_ID}"
    echo "root_dir=${ROOT_DIR}"
    echo "out_dir=${OUT_DIR}"
    echo "go_version=$(go version)"
    echo "uname=$(uname -a)"
    echo "BADGER_DUCKDB_FLUSH_BATCH_SIZE=${BADGER_DUCKDB_FLUSH_BATCH_SIZE:-}"
  } >"${OUT_DIR}/env.txt"
}

smoke() {
  run_cmd smoke_serial_bank go test -v -tags duckdb -run TestDuckDBBankSerialCorrectness -timeout 120s .
  run_cmd smoke_serial_smallbank go test -v -tags duckdb -run TestDuckDBSmallBankSerialCorrectness -timeout 300s .
  run_cmd smoke_concurrency go test -v -tags duckdb -run TestDuckDBBankStress -timeout 300s .
}

compare() {
  run_cmd compare_bank_no_delay go test -v -tags duckdb -run TestBankBadgerVsDuckDB$ -timeout 180s .
  run_cmd compare_bank_with_delay go test -v -tags duckdb -run TestBankBadgerVsDuckDBWithDelay -timeout 180s .
  run_cmd compare_smallbank_mixed go test -v -tags duckdb -run TestSmallBankBadgerVsDuckDB -timeout 300s .
  run_cmd compare_readheavy_cardinality env BADGER_DUCKDB_SWEEP_CSV="${OUT_DIR}/readheavy_crossover.csv" \
    go test -v -tags duckdb -run TestReadHeavyBalanceCardinalitySweepBadgerVsDuckDB -timeout 600s .
  run_cmd compare_readheavy_concurrency env BADGER_DUCKDB_SWEEP_CONC_CSV="${OUT_DIR}/readheavy_crossover_concurrency.csv" \
    go test -v -tags duckdb -run TestReadHeavyBalanceCardinalityConcurrencySweepBadgerVsDuckDB -timeout 1200s .
  run_cmd compare_summary bash "${ROOT_DIR}/scripts/duckdb_compare_report.sh" "${OUT_DIR}"
}

compare_extended() {
  compare
  run_cmd compare_ext_readheavy_cardinality env BADGER_DUCKDB_SWEEP_CSV="${OUT_DIR}/readheavy_crossover_extended.csv" \
    BADGER_DUCKDB_SWEEP_CARDINALITIES="1000,5000,20000,100000,150000,200000" \
    go test -v -tags duckdb -run TestReadHeavyBalanceCardinalitySweepBadgerVsDuckDB -timeout 1800s .
  run_cmd compare_ext_readheavy_concurrency env BADGER_DUCKDB_SWEEP_CONC_CSV="${OUT_DIR}/readheavy_crossover_concurrency_extended.csv" \
    BADGER_DUCKDB_SWEEP_CONC_CARDINALITIES="100000,150000,200000" BADGER_DUCKDB_SWEEP_WORKERS="4,8,16,32,64" \
    go test -v -tags duckdb -run TestReadHeavyBalanceCardinalityConcurrencySweepBadgerVsDuckDB -timeout 1800s .
}

epoch() {
  run_cmd epoch_delay_sweep go test -v -tags duckdb -run TestDuckDBBankEpochStress -timeout 240s .
  run_cmd epoch_no_delay_sweep go test -v -tags duckdb -run TestDuckDBBankEpochStressNoDelay -timeout 240s .
}

profile() {
  run_cmd profile_smallbank_balance go test -tags duckdb -run '^$' -bench '^BenchmarkSmallBankBalance$' -benchtime 30s -cpuprofile "${OUT_DIR}/cpu_duckdb.prof" "${LD_FLAGS[@]}" .
  run_cmd profile_duckdb_bank_tps go test -tags duckdb -run '^$' -bench '^BenchmarkDuckDBBankTPS$' -benchtime 10s -count 5 "${LD_FLAGS[@]}" .
}

microbench() {
  run_cmd microbench_commit_pipeline go test -tags duckdb -run '^$' -bench '^BenchmarkDuckDBManagedCommitPipeline$|^BenchmarkDuckDBDirectAppendPipeline$' -benchtime 10s -count 3 "${LD_FLAGS[@]}" .
}

lockfree_compare() {
  run_cmd lockfree_badger go test -run '^$' -bench '^BenchmarkLockFreeIngest$' -benchtime 10s .
  run_cmd lockfree_duckdb go test -tags duckdb -run '^$' -bench '^BenchmarkLockFreeIngest_DuckDB$' -benchtime 10s .
}

full() {
  smoke
  compare
  epoch
  profile
  lockfree_compare
}

ashley() {
  if [[ -z "${BADGER_DUCKDB_READ_POOL_SIZE:-}" ]]; then
    export BADGER_DUCKDB_READ_POOL_SIZE=2
    log "BADGER_DUCKDB_READ_POOL_SIZE not set; defaulting Ashley run to 2"
  fi
  compare
  epoch
  profile
}

ashley_readpool_sweep() {
  local sizes="${READ_POOL_SWEEP_SIZES:-1 2 4 8}"
  for sz in ${sizes}; do
    run_cmd "ashley_readpool_smallbank_balance_${sz}" env BADGER_DUCKDB_READ_POOL_SIZE="${sz}" \
      go test -tags duckdb -run '^$' -bench '^BenchmarkSmallBankBalance$' -benchtime 10s "${LD_FLAGS[@]}" .
    run_cmd "ashley_readpool_duckdb_bank_tps_${sz}" env BADGER_DUCKDB_READ_POOL_SIZE="${sz}" \
      go test -tags duckdb -run '^$' -bench '^BenchmarkDuckDBBankTPS$' -benchtime 10s -count 3 "${LD_FLAGS[@]}" .
  done
}

ashley_flushbatch_sweep() {
  local sizes="${FLUSH_BATCH_SWEEP_SIZES:-1 4 16 64 256}"
  for sz in ${sizes}; do
    run_cmd "ashley_flushbatch_lockfree_ingest_${sz}" env BADGER_DUCKDB_FLUSH_BATCH_SIZE="${sz}" \
      go test -tags duckdb -run '^$' -bench '^BenchmarkLockFreeIngest_DuckDB$' -benchtime 10s "${LD_FLAGS[@]}" .
    run_cmd "ashley_flushbatch_duckdb_bank_tps_${sz}" env BADGER_DUCKDB_FLUSH_BATCH_SIZE="${sz}" \
      go test -tags duckdb -run '^$' -bench '^BenchmarkDuckDBBankTPS$' -benchtime 10s -count 3 "${LD_FLAGS[@]}" .
  done
}

usage() {
  cat <<'EOF'
Usage: scripts/duckdb_experiments.sh <target>

Targets:
  smoke             Run correctness + concurrency smoke checks
  compare           Run Badger vs DuckDB side-by-side comparisons
  compare-extended  Run compare plus 150k/200k and 64-worker sweeps
  epoch             Run epoch batching sweeps
  profile           Generate CPU profile + TPS benchmark logs
  microbench        Run targeted commit/direct-append microbenchmarks
  lockfree-compare  Run lockfree ingest benchmarks for Badger vs DuckDB
  ashley            Run Ashley overhead track (compare + epoch + profile)
  ashley-readpool-sweep
                   Sweep BADGER_DUCKDB_READ_POOL_SIZE and benchmark
  ashley-flushbatch-sweep
                   Sweep BADGER_DUCKDB_FLUSH_BATCH_SIZE and benchmark
  full              Run all targets above

Environment variables:
  ARTIFACT_ROOT     Override artifacts root directory
  READ_POOL_SWEEP_SIZES
                   Space-separated read pool sizes for sweep target
  FLUSH_BATCH_SWEEP_SIZES
                   Space-separated flush batch sizes for sweep target
EOF
}

main() {
  local target="${1:-}"
  if [[ -z "${target}" ]]; then
    usage
    exit 1
  fi

  ensure_duckdb_defaults
  print_env

  case "${target}" in
    smoke)
      smoke
      ;;
    compare)
      compare
      ;;
    compare-extended)
      compare_extended
      ;;
    epoch)
      epoch
      ;;
    profile)
      profile
      ;;
    microbench)
      microbench
      ;;
    lockfree-compare)
      lockfree_compare
      ;;
    ashley)
      ashley
      ;;
    ashley-readpool-sweep)
      ashley_readpool_sweep
      ;;
    ashley-flushbatch-sweep)
      ashley_flushbatch_sweep
      ;;
    full)
      full
      ;;
    *)
      usage
      exit 1
      ;;
  esac

  log "Done. Artifacts written to ${OUT_DIR}"
}

main "$@"
