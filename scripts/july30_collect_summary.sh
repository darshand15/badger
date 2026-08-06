#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE_DIR="${1:-${ROOT_DIR}/artifacts/duckdb/july30}"
OUT_CSV="${BASE_DIR}/paper_summary.csv"
OUT_MD="${BASE_DIR}/paper_summary.md"
EXT_CSV="${BASE_DIR}/external_competitor_template.csv"

if [[ ! -d "${BASE_DIR}" ]]; then
  echo "Base directory not found: ${BASE_DIR}" >&2
  exit 1
fi

echo "run_id,track,test_name,metric_name,metric_value,unit,notes" > "${OUT_CSV}"

for run_dir in "${BASE_DIR}"/*; do
  [[ -d "${run_dir}" ]] || continue
  run_id="$(basename "${run_dir}")"

  soak_log="${run_dir}/gate_soak_short.log"
  if [[ -f "${soak_log}" ]]; then
    soak_line="$(grep -E 'total transfer ops:' "${soak_log}" | tail -n 1 || true)"
    if [[ -n "${soak_line}" ]]; then
      soak_ops="$(echo "${soak_line}" | sed -E 's/.*total transfer ops:[[:space:]]*([0-9]+).*/\1/')"
      soak_tps="$(echo "${soak_line}" | sed -E 's/.*\(([0-9]+)[[:space:]]+TPS avg\).*/\1/')"
      echo "${run_id},duckdb,TestDuckDBBankSoak,total_transfer_ops,${soak_ops},ops,short_gate" >> "${OUT_CSV}"
      echo "${run_id},duckdb,TestDuckDBBankSoak,avg_tps,${soak_tps},tps,short_gate" >> "${OUT_CSV}"
    fi
  fi

  sat_log="${run_dir}/gate_saturation.log"
  if [[ -f "${sat_log}" ]]; then
    while IFS= read -r line; do
      workers="$(echo "${line}" | sed -E 's/.*:[[:space:]]*([0-9]+)[[:space:]]+([0-9]+\.[0-9]+).*/\1/')"
      ops="$(echo "${line}" | sed -E 's/.*:[[:space:]]*([0-9]+)[[:space:]]+([0-9]+\.[0-9]+).*/\2/')"
      if [[ "${workers}" =~ ^[0-9]+$ && "${ops}" =~ ^[0-9]+\.[0-9]+$ ]]; then
        echo "${run_id},duckdb,TestDuckDBSaturationProbe,ops_per_sec_workers_${workers},${ops},ops_per_sec,gate_saturation" >> "${OUT_CSV}"
      fi
    done < <(grep -E 'db_duckdb_saturation_test.go:150: +[0-9]+ +[0-9]+\.[0-9]+' "${sat_log}" || true)
  fi

  for sat_csv in "${run_dir}"/saturation_*.csv; do
    [[ -f "${sat_csv}" ]] || continue
    card="$(basename "${sat_csv}" | sed -E 's/saturation_([0-9]+)\.csv/\1/')"
    while IFS=, read -r workers duckdb_ops avg_ns p90_ns gor_before gor_after heap_before heap_after open_conns in_use idle wait_count wait_ns; do
      if [[ "${workers}" == "workers" || -z "${workers}" ]]; then
        continue
      fi
      echo "${run_id},duckdb,TestDuckDBSaturationProbe,ops_per_sec_card_${card}_workers_${workers},${duckdb_ops},ops_per_sec,large_data_saturation" >> "${OUT_CSV}"
    done < "${sat_csv}"
  done

  sb_mixed_log="${run_dir}/smallbank_mixed.log"
  if [[ -f "${sb_mixed_log}" ]]; then
    total_line="$(grep -E 'TOTAL TPS:' "${sb_mixed_log}" | tail -n 1 || true)"
    if [[ -n "${total_line}" ]]; then
      sb_tps="$(echo "${total_line}" | sed -E 's/.*TOTAL TPS:[[:space:]]*([0-9]+).*/\1/')"
      echo "${run_id},duckdb,TestSmallBankDuckDBMixed,total_tps,${sb_tps},tps,smallbank_mixed" >> "${OUT_CSV}"
    fi
  fi

  sb_cmp_log="${run_dir}/smallbank_badger_vs_duckdb.log"
  if [[ -f "${sb_cmp_log}" ]]; then
    while IFS= read -r row; do
      txn="$(echo "${row}" | awk '{print $1}')"
      badger_tps="$(echo "${row}" | awk '{print $2}')"
      duckdb_tps="$(echo "${row}" | awk '{print $3}')"
      ratio="$(echo "${row}" | awk '{print $6}' | tr -d 'x')"
      if [[ -n "${txn}" && "${badger_tps}" =~ ^[0-9]+$ && "${duckdb_tps}" =~ ^[0-9]+$ ]]; then
        echo "${run_id},badger,TestSmallBankBadgerVsDuckDB,${txn}_tps,${badger_tps},tps,comparison" >> "${OUT_CSV}"
        echo "${run_id},duckdb,TestSmallBankBadgerVsDuckDB,${txn}_tps,${duckdb_tps},tps,comparison" >> "${OUT_CSV}"
      fi
      if [[ -n "${txn}" && "${ratio}" =~ ^[0-9]+\.[0-9]+$ ]]; then
        echo "${run_id},duckdb_vs_badger,TestSmallBankBadgerVsDuckDB,${txn}_ratio,${ratio},x,comparison" >> "${OUT_CSV}"
      fi
    done < <(grep -E 'db_duckdb_comparison_test.go:387: +[A-Za-z]+' "${sb_cmp_log}" | \
      sed -E 's/.*:387:[[:space:]]*//' | \
      awk '{print $1, $2, $3, $4, $5, $6}')
  fi

  rh_csv="${run_dir}/readheavy_50k_200k.csv"
  if [[ -f "${rh_csv}" ]]; then
    while IFS=, read -r customers badger_ops duckdb_ops ratio badger_avg badger_p90 duckdb_avg duckdb_p90; do
      if [[ "${customers}" == "customers" || -z "${customers}" ]]; then
        continue
      fi
      echo "${run_id},badger,TestReadHeavyBalanceCardinalitySweepBadgerVsDuckDB,ops_customers_${customers},${badger_ops},ops_per_sec,readheavy_cardinality" >> "${OUT_CSV}"
      echo "${run_id},duckdb,TestReadHeavyBalanceCardinalitySweepBadgerVsDuckDB,ops_customers_${customers},${duckdb_ops},ops_per_sec,readheavy_cardinality" >> "${OUT_CSV}"
      echo "${run_id},duckdb_vs_badger,TestReadHeavyBalanceCardinalitySweepBadgerVsDuckDB,ratio_customers_${customers},${ratio},x,readheavy_cardinality" >> "${OUT_CSV}"
    done < "${rh_csv}"
  fi

  rhc_csv="${run_dir}/readheavy_concurrency_50k_200k.csv"
  if [[ -f "${rhc_csv}" ]]; then
    while IFS=, read -r customers workers badger_ops duckdb_ops ratio; do
      if [[ "${customers}" == "customers" || -z "${customers}" ]]; then
        continue
      fi
      echo "${run_id},badger,TestReadHeavyBalanceCardinalityConcurrencySweepBadgerVsDuckDB,ops_customers_${customers}_workers_${workers},${badger_ops},ops_per_sec,readheavy_concurrency" >> "${OUT_CSV}"
      echo "${run_id},duckdb,TestReadHeavyBalanceCardinalityConcurrencySweepBadgerVsDuckDB,ops_customers_${customers}_workers_${workers},${duckdb_ops},ops_per_sec,readheavy_concurrency" >> "${OUT_CSV}"
      echo "${run_id},duckdb_vs_badger,TestReadHeavyBalanceCardinalityConcurrencySweepBadgerVsDuckDB,ratio_customers_${customers}_workers_${workers},${ratio},x,readheavy_concurrency" >> "${OUT_CSV}"
    done < "${rhc_csv}"
  fi
done

{
  echo "# July 30 Paper Summary"
  echo
  echo "Source directory: ${BASE_DIR}"
  echo
  echo "## DuckDB Soak/Saturation Highlights"
  echo
  echo "| Run ID | Test | Metric | Value |"
  echo "|---|---|---|---:|"
  awk -F, '
    NR>1 && $3=="TestDuckDBBankSoak" && $4=="avg_tps" {
      printf "| %s | %s | %s | %s %s |\n", $1, $3, $4, $5, $6
    }
    NR>1 && $3=="TestDuckDBSaturationProbe" && $4 ~ /^ops_per_sec_workers_/ {
      printf "| %s | %s | %s | %s %s |\n", $1, $3, $4, $5, $6
    }
  ' "${OUT_CSV}"

  echo
  echo "## Large-Data Probe Highlights"
  echo
  echo "| Run ID | Test | Metric | Value |"
  echo "|---|---|---|---:|"
  awk -F, '
    NR>1 && $3=="TestDuckDBSaturationProbe" && $4 ~ /^ops_per_sec_card_/ {
      printf "| %s | %s | %s | %s %s |\n", $1, $3, $4, $5, $6
    }
  ' "${OUT_CSV}"

  echo
  echo "## SmallBank Highlights"
  echo
  echo "| Run ID | Test | Metric | Value |"
  echo "|---|---|---|---:|"
  awk -F, '
    NR>1 && ($3=="TestSmallBankDuckDBMixed" && $4=="total_tps") {
      printf "| %s | %s | %s | %s %s |\n", $1, $3, $4, $5, $6
    }
    NR>1 && $3=="TestSmallBankBadgerVsDuckDB" && $4 ~ /_ratio$/ {
      printf "| %s | %s | %s | %s %s |\n", $1, $3, $4, $5, $6
    }
  ' "${OUT_CSV}"

  echo
  echo "## Read-Heavy 50k/200k Highlights"
  echo
  echo "| Run ID | Test | Metric | Value |"
  echo "|---|---|---|---:|"
  awk -F, '
    NR>1 && $3=="TestReadHeavyBalanceCardinalitySweepBadgerVsDuckDB" && $4 ~ /^ratio_customers_/ {
      printf "| %s | %s | %s | %s %s |\n", $1, $3, $4, $5, $6
    }
    NR>1 && $3=="TestReadHeavyBalanceCardinalityConcurrencySweepBadgerVsDuckDB" &&
      ($4=="ratio_customers_50000_workers_128" || $4=="ratio_customers_200000_workers_128") {
      printf "| %s | %s | %s | %s %s |\n", $1, $3, $4, $5, $6
    }
  ' "${OUT_CSV}"

  echo
  echo "See CSV for full metrics: paper_summary.csv"
} > "${OUT_MD}"

cat > "${EXT_CSV}" <<'EOF'
run_id,track,test_name,metric_name,metric_value,unit,notes
2026-07-30_pg_1s_50k,postgres,smallbank,total_tps,,,fill_from_pg_run
2026-07-30_pg_2s_200k,postgres,smallbank,total_tps,,,fill_from_pg_run
2026-07-30_geogauss_1s_50k,geogauss,smallbank,total_tps,,,fill_from_geogauss_run
2026-07-30_detock_1s_50k,detock,smallbank,total_tps,,,fill_from_detock_run
2026-07-30_caracal_1s_50k,caracal,smallbank,total_tps,,,fill_from_caracal_run
EOF

echo "Wrote ${OUT_CSV}"
echo "Wrote ${OUT_MD}"
echo "Wrote ${EXT_CSV}"
