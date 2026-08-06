#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-${ROOT_DIR}/artifacts/duckdb/july30}"
RUN_TAG="${RUN_TAG:-$(date +%Y%m%d_%H%M%S)_overnight_100m_retry}"
META_DIR="${ARTIFACT_ROOT}/${RUN_TAG}"
CARDINALITY="${JULY30_DUCKDB_LARGE_CARDINALITY:-100000000}"
PROBE_TIMEOUT="${JULY30_DUCKDB_LARGE_TIMEOUT:-21600s}"
TARGET="${JULY30_DUCKDB_OVERNIGHT_TARGET:-large-data-probe}"

mkdir -p "${META_DIR}"

CAFFEINATE_PID=""
cleanup() {
  if [[ -n "${CAFFEINATE_PID}" ]]; then
    kill "${CAFFEINATE_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

(caffeinate -dimsu & echo $! >"${META_DIR}/caffeinate.pid")
CAFFEINATE_PID="$(cat "${META_DIR}/caffeinate.pid")"

{
  echo "RUN_TAG=${RUN_TAG}"
  echo "CAFFEINATE_PID=${CAFFEINATE_PID}"
  echo "CARDINALITY=${CARDINALITY}"
  echo "PROBE_TIMEOUT=${PROBE_TIMEOUT}"
  echo "TARGET=${TARGET}"
} >"${META_DIR}/overnight_meta.txt"

probe_exit=0
summary_exit=0

set +e
(
  cd "${ROOT_DIR}"
  JULY30_DUCKDB_LARGE_CARDINALITY="${CARDINALITY}" \
    JULY30_DUCKDB_LARGE_TIMEOUT="${PROBE_TIMEOUT}" \
    bash scripts/july30_duckdb_campaign.sh "${TARGET}"
) 2>&1 | tee "${META_DIR}/overnight_100m.log"
probe_exit=${PIPESTATUS[0]}

(
  cd "${ROOT_DIR}"
  bash scripts/july30_collect_summary.sh
) 2>&1 | tee "${META_DIR}/overnight_collect_summary.log"
summary_exit=${PIPESTATUS[0]}
set -e

{
  echo "probe_exit=${probe_exit}"
  echo "summary_exit=${summary_exit}"
} >>"${META_DIR}/overnight_meta.txt"

if [[ ${probe_exit} -ne 0 ]]; then
  exit "${probe_exit}"
fi

if [[ ${summary_exit} -ne 0 ]]; then
  exit "${summary_exit}"
fi

echo "Overnight run complete: ${META_DIR}"
