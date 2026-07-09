#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: scripts/duckdb_compare_report.sh <artifact_run_dir>" >&2
  exit 1
fi

RUN_DIR="$1"
OUT_MD="${RUN_DIR}/compare_summary.md"
CSV1="${RUN_DIR}/readheavy_crossover.csv"
CSV2="${RUN_DIR}/readheavy_crossover_concurrency.csv"
HISTORY_FILE="${ARTIFACT_HISTORY_FILE:-$(dirname "${RUN_DIR}")/performance_history.csv}"
RATIO_WARN_THRESHOLD="${DUCKDB_RATIO_WARN_THRESHOLD:-3.5}"

extract_tps_pair() {
  local file="$1"
  awk '
    /Backend[[:space:]]+TPS/ {in_tbl=1; next}
    in_tbl && /Badger/ && badger=="" {
      for (i=1; i<=NF; i++) {
        if ($i ~ /^[0-9]+(\.[0-9]+)?$/) {badger=$i; break}
      }
      next
    }
    in_tbl && /DuckDB/ && duck=="" {
      for (i=1; i<=NF; i++) {
        if ($i ~ /^[0-9]+(\.[0-9]+)?$/) {duck=$i; break}
      }
      next
    }
    END {printf "%s,%s", badger, duck}
  ' "$file"
}

point_no_delay=""
point_delay=""
if [[ -f "${RUN_DIR}/compare_bank_no_delay.log" ]]; then
  point_no_delay="$(extract_tps_pair "${RUN_DIR}/compare_bank_no_delay.log")"
fi
if [[ -f "${RUN_DIR}/compare_bank_with_delay.log" ]]; then
  point_delay="$(extract_tps_pair "${RUN_DIR}/compare_bank_with_delay.log")"
fi

ratio_100k=""
if [[ -f "${CSV1}" ]]; then
  ratio_100k=$(awk -F, 'NR>1 && $1==100000 {print $4; exit}' "${CSV1}")
fi

best_conc_ratio=""
best_conc_workers=""
if [[ -f "${CSV2}" ]]; then
  best_pair=$(awk -F, 'NR>1 {if ($5+0>max) {max=$5; w=$2}} END {if (max>0) printf "%.6f,%s", max, w}' "${CSV2}")
  if [[ -n "${best_pair}" ]]; then
    best_conc_ratio="${best_pair%,*}"
    best_conc_workers="${best_pair#*,}"
  fi
fi

{
  echo "# DuckDB vs Badger Compare Summary"
  echo
  echo "Run directory: ${RUN_DIR}"
  echo
  echo "## Mode Summary"
  echo
  echo "| Mode | Badger TPS/Ops | DuckDB TPS/Ops | DuckDB/Badger |"
  echo "|---|---:|---:|---:|"

  if [[ -n "${point_no_delay}" ]]; then
    b="${point_no_delay%,*}"
    d="${point_no_delay#*,}"
    ratio="NA"
    if [[ -n "${b}" && -n "${d}" && "${b}" != "0" ]]; then
      ratio=$(awk -v a="$d" -v b="$b" 'BEGIN{printf "%.3f", a/b}')
    fi
    echo "| Point (Bank, no delay) | ${b:-NA} | ${d:-NA} | ${ratio} |"
  fi

  if [[ -n "${point_delay}" ]]; then
    b="${point_delay%,*}"
    d="${point_delay#*,}"
    ratio="NA"
    if [[ -n "${b}" && -n "${d}" && "${b}" != "0" ]]; then
      ratio=$(awk -v a="$d" -v b="$b" 'BEGIN{printf "%.3f", a/b}')
    fi
    echo "| Point (Bank, 50us delay) | ${b:-NA} | ${d:-NA} | ${ratio} |"
  fi

  if [[ -f "${CSV1}" ]]; then
    last_row=$(tail -n 1 "${CSV1}")
    IFS=',' read -r customers bops dops ratio _ <<< "${last_row}"
    echo "| Read-heavy (Balance @ ${customers} customers) | ${bops} | ${dops} | ${ratio} |"
  fi

  echo
  if [[ -f "${CSV1}" ]]; then
    echo "## Crossover Curve"
    echo
    echo '```mermaid'
    echo 'xychart-beta'
    echo '  title "DuckDB/Badger Ratio vs Customer Cardinality"'
    echo '  x-axis "Customers" [1000, 5000, 20000, 100000]'
    echo '  y-axis "Ratio" 0 --> 6'
    vals=$(awk -F, 'NR>1 {printf "%s%s", (c?", ":""), $4; c=1} END{print ""}' "${CSV1}")
    echo "  line [${vals}]"
    echo '```'
    echo
  fi

  if [[ -f "${CSV2}" ]]; then
    echo "## Concurrency Matrix (excerpt)"
    echo
    echo "| Customers | Workers | DuckDB/Badger |"
    echo "|---:|---:|---:|"
    awk -F, 'NR>1 {printf "| %s | %s | %.3f |\n", $1, $2, $5}' "${CSV2}"
  fi

  if [[ -n "${ratio_100k}" ]]; then
    echo
    echo "## Guardrail"
    echo
    if awk -v r="${ratio_100k}" -v t="${RATIO_WARN_THRESHOLD}" 'BEGIN{exit !(r+0<t+0)}'; then
      echo "WARNING: DuckDB/Badger ratio at 100000 customers is ${ratio_100k}, below threshold ${RATIO_WARN_THRESHOLD}."
    else
      echo "OK: DuckDB/Badger ratio at 100000 customers is ${ratio_100k}, threshold ${RATIO_WARN_THRESHOLD}."
    fi
  fi
} > "${OUT_MD}"

if [[ -n "${ratio_100k}" ]] && awk -v r="${ratio_100k}" -v t="${RATIO_WARN_THRESHOLD}" 'BEGIN{exit !(r+0<t+0)}'; then
  echo "WARNING: DuckDB/Badger ratio at 100000 customers (${ratio_100k}) is below threshold ${RATIO_WARN_THRESHOLD}" >&2
  if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
    echo "::warning::DuckDB/Badger ratio at 100000 customers (${ratio_100k}) is below threshold ${RATIO_WARN_THRESHOLD}" >&2
  fi
fi

if [[ ! -f "${HISTORY_FILE}" ]]; then
  echo "run_id,point_no_delay_ratio,point_delay_ratio,ratio_100k,best_conc_ratio,best_conc_workers" > "${HISTORY_FILE}"
fi

point_no_delay_ratio=""
if [[ -n "${point_no_delay}" ]]; then
  b="${point_no_delay%,*}"
  d="${point_no_delay#*,}"
  if [[ -n "${b}" && -n "${d}" && "${b}" != "0" ]]; then
    point_no_delay_ratio=$(awk -v a="$d" -v b="$b" 'BEGIN{printf "%.6f", a/b}')
  fi
fi

point_delay_ratio=""
if [[ -n "${point_delay}" ]]; then
  b="${point_delay%,*}"
  d="${point_delay#*,}"
  if [[ -n "${b}" && -n "${d}" && "${b}" != "0" ]]; then
    point_delay_ratio=$(awk -v a="$d" -v b="$b" 'BEGIN{printf "%.6f", a/b}')
  fi
fi

run_id=$(basename "${RUN_DIR}")
echo "${run_id},${point_no_delay_ratio:-},${point_delay_ratio:-},${ratio_100k:-},${best_conc_ratio:-},${best_conc_workers:-}" >> "${HISTORY_FILE}"

echo "Wrote ${OUT_MD}"
echo "Updated ${HISTORY_FILE}"
