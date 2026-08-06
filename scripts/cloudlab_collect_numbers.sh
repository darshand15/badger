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

OUT_DIR="${OUT_DIR:-${ROOT_DIR}/artifacts/cloudlab/$(date +%Y%m%d_%H%M%S)}"
mkdir -p "${OUT_DIR}"

SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=12)

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required (brew install jq)" >&2
  exit 1
fi

prom_query() {
  local q="$1"
  ssh "${SSH_OPTS[@]}" "${CLOUDLAB_USER}@${PC_MONITORING}" \
    "curl -fsS --get --data-urlencode 'query=${q}' http://localhost:9090/api/v1/query"
}

write_metric_csv() {
  local name="$1"
  local query="$2"
  local json out
  out="${OUT_DIR}/${name}.csv"
  json="$(prom_query "${query}")"
  echo "metric,labels,value" >"${out}"
  echo "${json}" | jq -r '.data.result[] | ["'"${name}"'", (.metric|tojson), .value[1]] | @csv' >>"${out}"
}

main() {
  local now
  now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  # Throughput proxies and runtime health.
  write_metric_csv broker_http_rate '(sum(rate(http_server_requests_total{role="broker"}[1m])) or vector(0))'
  write_metric_csv server_http_rate '(sum(rate(http_server_requests_total{role="server"}[1m])) or vector(0))'
  write_metric_csv broker_process_cpu 'avg(rate(process_cpu_seconds_total{job="l-free-machine",role="broker"}[1m]))'
  write_metric_csv server_process_cpu 'avg(rate(process_cpu_seconds_total{job="l-free-machine",role="server"}[1m]))'

  # Node-level resources.
  write_metric_csv node_cpu_util 'avg(1 - rate(node_cpu_seconds_total{mode="idle"}[1m])) by (instance)'
  write_metric_csv node_mem_used_pct '100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))'
  write_metric_csv node_net_rx_bytes 'sum(rate(node_network_receive_bytes_total[1m])) by (instance)'
  write_metric_csv node_net_tx_bytes 'sum(rate(node_network_transmit_bytes_total[1m])) by (instance)'
  write_metric_csv node_disk_read_bytes 'sum(rate(node_disk_read_bytes_total[1m])) by (instance)'
  write_metric_csv node_disk_write_bytes 'sum(rate(node_disk_written_bytes_total[1m])) by (instance)'

  cat >"${OUT_DIR}/numbers_report.md" <<EOF
# CloudLab Numeric Snapshot

Collected at: ${now}
Monitoring node: ${PC_MONITORING}

## Files

- broker_http_rate.csv
- server_http_rate.csv
- broker_process_cpu.csv
- server_process_cpu.csv
- node_cpu_util.csv
- node_mem_used_pct.csv
- node_net_rx_bytes.csv
- node_net_tx_bytes.csv
- node_disk_read_bytes.csv
- node_disk_write_bytes.csv

Use these CSV files as paper evidence tables (no dashboards required).
EOF

  echo "Wrote numeric snapshot to: ${OUT_DIR}"
}

main "$@"
