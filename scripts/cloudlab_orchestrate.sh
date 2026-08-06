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

require_var() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "Missing required variable: ${name}" >&2
    exit 1
  fi
}

for v in CLOUDLAB_USER REPO_LOCKFREE REPO_BENCHBASE PC_DIRECTORY PC_MONITORING PC_BROKERS PC_SERVERS; do
  require_var "$v"
done

SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=12)

csv_to_array() {
  local raw="$1"
  local IFS=','
  read -r -a out <<<"$raw"
  printf '%s\n' "${out[@]}"
}

log() {
  echo "[$(date +%H:%M:%S)] $*"
}

ssh_run() {
  local host="$1"
  shift
  ssh "${SSH_OPTS[@]}" "${CLOUDLAB_USER}@${host}" "$@"
}

first_broker() {
  local -a brokers
  while IFS= read -r h; do brokers+=("$h"); done < <(csv_to_array "$PC_BROKERS")
  if [[ "${#brokers[@]}" -eq 0 ]]; then
    echo "No brokers configured" >&2
    exit 1
  fi
  echo "${brokers[0]}"
}

write_lockfree_env() {
  local env_file="${REPO_LOCKFREE}/scripts/env-vars.sh"
  local -a brokers servers

  while IFS= read -r h; do brokers+=("$h"); done < <(csv_to_array "$PC_BROKERS")
  while IFS= read -r h; do servers+=("$h"); done < <(csv_to_array "$PC_SERVERS")

  cat >"${env_file}" <<EOF
#!/bin/bash
cloudLabUserName="${CLOUDLAB_USER}"
numServer=${NUM_SERVER:-${#servers[@]}}
numBroker=${NUM_BROKER:-${#brokers[@]}}
experimentName="${EXPERIMENT_NAME:-cloudlab}"
clusterType="emulab"
projectName="l-free-machine"
dropRate=${DROP_RATE:-0}
isTest=${IS_TEST:-false}
useBenchmark=${USE_BENCHMARK:-true}
executor=${EXECUTOR:-1}
useDuckDB=${USE_DUCKDB:-true}

numTxns=${NUM_TXNS:-200}
numPackages=${NUM_PACKAGES:-20000}
totalPackages=\$((numBroker * numPackages))
maxEpochs=${MAX_EPOCHS:-20000}

if [ "\$clusterType" == "emulab" ]; then
    export suffix="net"
else
    export suffix="cloudlab.us"
fi

EOF

  local i
  for i in "${!servers[@]}"; do
    echo "PC_SERVER$((i+1))=\"${servers[$i]}\"" >>"${env_file}"
  done
  for i in "${!brokers[@]}"; do
    echo "PC_BROKER$((i+1))=\"${brokers[$i]}\"" >>"${env_file}"
  done

  cat >>"${env_file}" <<EOF
PC_DIRECTORY="${PC_DIRECTORY}"
PC_MONITORING="${PC_MONITORING}"
PC_CLIENT="${PC_CLIENT:-}"

PC_SERVERS=(
EOF

  for i in "${!servers[@]}"; do
    echo "  \"\${PC_SERVER$((i+1))}\"" >>"${env_file}"
  done

  cat >>"${env_file}" <<EOF
)
PC_BROKERS=(
EOF

  for i in "${!brokers[@]}"; do
    echo "  \"\${PC_BROKER$((i+1))}\"" >>"${env_file}"
  done

  cat >>"${env_file}" <<EOF
)
EOF

  chmod +x "${env_file}"
  log "Updated ${env_file}"
}

write_benchbase_config() {
  local cfg="${REPO_BENCHBASE}/config/lockfreedb/sample_smallbank_config.xml"
  if [[ ! -f "${cfg}" ]]; then
    echo "BenchBase config not found: ${cfg}" >&2
    exit 1
  fi

  local b1 b2
  b1="$(first_broker)"
  b2="$(csv_to_array "$PC_BROKERS" | sed -n '2p')"
  if [[ -z "${b2}" ]]; then
    b2="${b1}"
  fi

  perl -0777 -i -pe 's#<brokers>.*?</brokers>#<brokers>\n        <broker><host>'"${b1}"'</host><port>8083</port></broker>\n        <broker><host>'"${b2}"'</host><port>8083</port></broker>\n    </brokers>#s' "${cfg}"

  perl -0777 -i -pe 's#<time>\d+</time>#<time>'"${BENCHBASE_DURATION_SEC:-300}"'</time>#g; s#<terminals>\d+</terminals>#<terminals>'"${BENCHBASE_TERMINALS:-16}"'</terminals>#g; s#<rate>\d+</rate>#<rate>'"${BENCHBASE_RATE:-4000}"'</rate>#g' "${cfg}"

  log "Updated ${cfg}"
}

ssh_probe_all() {
  local -a all_hosts
  all_hosts+=("${PC_DIRECTORY}" "${PC_MONITORING}")
  while IFS= read -r h; do all_hosts+=("$h"); done < <(csv_to_array "$PC_BROKERS")
  while IFS= read -r h; do all_hosts+=("$h"); done < <(csv_to_array "$PC_SERVERS")
  if [[ -n "${PC_CLIENT:-}" ]]; then
    all_hosts+=("${PC_CLIENT}")
  fi

  local seen=""
  local h
  for h in "${all_hosts[@]}"; do
    [[ -z "$h" ]] && continue
    case ",$seen," in
      *",$h,"*) continue ;;
      *) seen+="${seen:+,}$h" ;;
    esac
    log "SSH check: $h"
    ssh_run "$h" 'hostname; whoami; nproc'
  done
}

start_cluster() {
  pushd "${REPO_LOCKFREE}/scripts" >/dev/null
  bash setup_monitoring.sh
  printf '0\n' | bash run_experiment.sh
  popd >/dev/null
}

run_benchbase_smallbank() {
  pushd "${REPO_BENCHBASE}" >/dev/null
  java -jar benchbase.jar -b smallbank -c config/lockfreedb/sample_smallbank_config.xml --create=false --load=false --execute=true
  popd >/dev/null
}

run_large_probe() {
  local cardinality="$1"

  pushd "${ROOT_DIR}" >/dev/null
  JULY30_DUCKDB_LARGE_CARDINALITY="${cardinality}" \
  JULY30_DUCKDB_LARGE_TIMEOUT="${JULY30_DUCKDB_LARGE_TIMEOUT:-21600s}" \
  JULY30_DUCKDB_SEED_KEY_MODE="${JULY30_DUCKDB_SEED_KEY_MODE:-full}" \
  JULY30_DUCKDB_READ_HEAVY_KEY_MODE="${JULY30_DUCKDB_READ_HEAVY_KEY_MODE:-full}" \
  bash scripts/july30_duckdb_campaign.sh large-data-probe
  bash scripts/july30_collect_summary.sh
  popd >/dev/null
}

run_10m() {
  run_large_probe 10000000
}

run_40m() {
  run_large_probe 40000000
}

run_100m() {
  run_large_probe 100000000
}

usage() {
  cat <<EOF
Usage: $(basename "$0") <command>

Commands:
  prepare          Update lock-free-machine env-vars and BenchBase broker config from profile
  probe            SSH probe all configured nodes
  start-cluster    Install monitoring and start directory/broker/server processes
  run-benchbase    Run BenchBase SmallBank workload
  run-10m          Run 10M DuckDB large-data probe in darshan-badger
  run-40m          Run 40M DuckDB large-data probe in darshan-badger
  run-10m-40m      Run 10M then 40M probes back-to-back
  run-100m         Run 100M DuckDB large-data probe in darshan-badger
  full             prepare + probe + start-cluster + run-benchbase

Env:
  PROFILE_FILE=<path>   Override profile file path (default scripts/cloudlab_profile.env)
EOF
}

main() {
  local cmd="${1:-}"
  case "$cmd" in
    prepare)
      write_lockfree_env
      write_benchbase_config
      ;;
    probe)
      ssh_probe_all
      ;;
    start-cluster)
      start_cluster
      ;;
    run-benchbase)
      run_benchbase_smallbank
      ;;
    run-10m)
      run_10m
      ;;
    run-40m)
      run_40m
      ;;
    run-10m-40m)
      run_10m
      run_40m
      ;;
    run-100m)
      run_100m
      ;;
    full)
      write_lockfree_env
      write_benchbase_config
      ssh_probe_all
      start_cluster
      run_benchbase_smallbank
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "$@"
