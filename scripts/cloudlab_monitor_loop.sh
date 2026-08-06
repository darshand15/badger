#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE_FILE="${PROFILE_FILE:-${ROOT_DIR}/scripts/cloudlab_profile.env}"

if [[ ! -f "${PROFILE_FILE}" ]]; then
  echo "Profile file not found: ${PROFILE_FILE}" >&2
  echo "Create it from scripts/cloudlab_profile.env.example" >&2
  exit 1
fi

source "${PROFILE_FILE}"

for v in CLOUDLAB_USER PC_MONITORING; do
  if [[ -z "${!v:-}" ]]; then
    echo "Missing required variable: ${v}" >&2
    exit 1
  fi
done

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required (brew install jq)" >&2
  exit 1
fi

INTERVAL_SEC="${INTERVAL_SEC:-15}"
DURATION_SEC="${DURATION_SEC:-300}"
OUT_DIR="${OUT_DIR:-${ROOT_DIR}/artifacts/cloudlab/monitor_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "${OUT_DIR}"

SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=12)

prom_query_scalar() {
  local q="$1"
  ssh "${SSH_OPTS[@]}" "${CLOUDLAB_USER}@${PC_MONITORING}" \
    "curl -fsS --get --data-urlencode 'query=${q}' http://localhost:9090/api/v1/query" \
    | jq -r '.data.result[0].value[1] // "nan"'
}

sample() {
  local ts="$1"
  local broker_req_rate server_req_rate cpu_util mem_used net_rx net_tx disk_r disk_w

  broker_req_rate="$(prom_query_scalar '(sum(rate(http_server_requests_total{role="broker"}[1m])) or vector(0))')"
  server_req_rate="$(prom_query_scalar '(sum(rate(http_server_requests_total{role="server"}[1m])) or vector(0))')"
  cpu_util="$(prom_query_scalar 'avg(1 - rate(node_cpu_seconds_total{mode="idle"}[1m]))')"
  mem_used="$(prom_query_scalar 'avg(100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)))')"
  net_rx="$(prom_query_scalar 'sum(rate(node_network_receive_bytes_total[1m]))')"
  net_tx="$(prom_query_scalar 'sum(rate(node_network_transmit_bytes_total[1m]))')"
  disk_r="$(prom_query_scalar 'sum(rate(node_disk_read_bytes_total[1m]))')"
  disk_w="$(prom_query_scalar 'sum(rate(node_disk_written_bytes_total[1m]))')"

  echo "${ts},${broker_req_rate},${server_req_rate},${cpu_util},${mem_used},${net_rx},${net_tx},${disk_r},${disk_w}" >>"${OUT_DIR}/timeseries.csv"
}

summarize_csv() {
  awk -F, '
    NR==1 { next }
    {
      for (i=2; i<=9; i++) {
        v=$i+0
        if (!(i in min) || v < min[i]) min[i]=v
        if (!(i in max) || v > max[i]) max[i]=v
        sum[i]+=v
        cnt[i]++
      }
    }
    END {
      names[2]="broker_req_rate"
      names[3]="server_req_rate"
      names[4]="cpu_util"
      names[5]="mem_used_pct"
      names[6]="net_rx_bytes_per_sec"
      names[7]="net_tx_bytes_per_sec"
      names[8]="disk_read_bytes_per_sec"
      names[9]="disk_write_bytes_per_sec"

      print "metric,min,avg,max"
      for (i=2; i<=9; i++) {
        if (cnt[i] > 0) {
          avg=sum[i]/cnt[i]
          printf "%s,%.6f,%.6f,%.6f\n", names[i], min[i], avg, max[i]
        }
      }
    }
  ' "${OUT_DIR}/timeseries.csv" >"${OUT_DIR}/summary.csv"
}

main() {
  local end_ts now
  end_ts=$(( $(date +%s) + DURATION_SEC ))

  echo "timestamp,broker_req_rate,server_req_rate,cpu_util,mem_used_pct,net_rx_bytes_per_sec,net_tx_bytes_per_sec,disk_read_bytes_per_sec,disk_write_bytes_per_sec" >"${OUT_DIR}/timeseries.csv"

  while :; do
    now="$(date +%s)"
    if (( now >= end_ts )); then
      break
    fi
    sample "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    sleep "${INTERVAL_SEC}"
  done

  summarize_csv

  cat >"${OUT_DIR}/report.md" <<EOF
# CloudLab Monitoring Numeric Report

- Monitoring host: ${PC_MONITORING}
- Duration (sec): ${DURATION_SEC}
- Interval (sec): ${INTERVAL_SEC}

## Outputs

- timeseries.csv
- summary.csv

Use summary.csv for paper-ready min/avg/max system and request-rate numbers.
EOF

  echo "Wrote monitoring report to: ${OUT_DIR}"
}

main "$@"
