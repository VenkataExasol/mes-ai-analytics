#!/bin/bash

set -euo pipefail

echo "Starting Full End-to-End Pipeline..."

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_PATH="${VENV_PATH:-${PROJECT_ROOT}/hackathon-env/bin/activate}"
EXASOL_NANO_BIN="${EXASOL_NANO_BIN:-exasol-nano}"
EXASOL_START_LOG="${EXASOL_START_LOG:-/tmp/exasol_nano_start.log}"
EXASOL_CONNECT_HOST="${EXASOL_CONNECT_HOST:-127.0.0.1}"
EXASOL_CONNECT_PORT="${EXASOL_CONNECT_PORT:-8563}"
EXASOL_READY_TIMEOUT_SEC="${EXASOL_READY_TIMEOUT_SEC:-120}"
PIPELINE_DATA_MODE="${PIPELINE_DATA_MODE:-files}"
SYNTHETIC_BATCHES="${SYNTHETIC_BATCHES:-4}"
SYNTHETIC_ROWS_PER_FILE="${SYNTHETIC_ROWS_PER_FILE:-8}"
SYNTHETIC_INTERVAL_SEC="${SYNTHETIC_INTERVAL_SEC:-3}"
SYNTHETIC_SEED="${SYNTHETIC_SEED:-}"
STARTED_BY_SCRIPT=0

if ! command -v "${EXASOL_NANO_BIN}" >/dev/null 2>&1; then
    echo "Exasol Nano binary not found: ${EXASOL_NANO_BIN}"
    exit 1
fi

is_port_open() {
    local host="$1"
    local port="$2"
    (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1
}

echo "Checking Exasol availability on ${EXASOL_CONNECT_HOST}:${EXASOL_CONNECT_PORT}..."
if is_port_open "${EXASOL_CONNECT_HOST}" "${EXASOL_CONNECT_PORT}"; then
    echo "Exasol already appears to be running. Skipping start."
else
    echo "Starting Exasol in non-interactive mode..."
    nohup "${EXASOL_NANO_BIN}" start --memory-gb 2 --cpus 2 >"${EXASOL_START_LOG}" 2>&1 &
    STARTED_BY_SCRIPT=1
    echo "Background start launched. Log file: ${EXASOL_START_LOG}"
fi

echo "Waiting for Exasol to be ready..."
elapsed=0
until is_port_open "${EXASOL_CONNECT_HOST}" "${EXASOL_CONNECT_PORT}"; do
    sleep 2
    elapsed=$((elapsed + 2))
    if [ "${elapsed}" -ge "${EXASOL_READY_TIMEOUT_SEC}" ]; then
        echo "Exasol did not become ready within ${EXASOL_READY_TIMEOUT_SEC}s."
        if [ -f "${EXASOL_START_LOG}" ]; then
            echo "Last lines from ${EXASOL_START_LOG}:"
            tail -n 40 "${EXASOL_START_LOG}" || true
        fi
        exit 1
    fi
done
echo "Exasol is ready."

if [ -f "${VENV_PATH}" ]; then
    echo "Activating virtual environment..."
    # shellcheck disable=SC1090
    source "${VENV_PATH}"
fi

echo "Running Schema Detection..."
python3 "${SCRIPT_DIR}/schema_detection_agent.py"

echo "Running Mapping Agent..."
python3 "${SCRIPT_DIR}/mapping_agent.py"

# Conditional: Generate synthetic data if in synthetic mode
if [ "${PIPELINE_DATA_MODE}" = "synthetic" ]; then
    echo "🧪 Generating Synthetic Data..."
    echo "Config: batches=${SYNTHETIC_BATCHES} (0 = continuous), rows_per_file=${SYNTHETIC_ROWS_PER_FILE}, interval_sec=${SYNTHETIC_INTERVAL_SEC}, seed=${SYNTHETIC_SEED:-<none>}"
    
    export SYNTHETIC_BATCHES
    export SYNTHETIC_ROWS_PER_FILE
    export SYNTHETIC_INTERVAL_SEC
    export SYNTHETIC_SEED
    
    python3 "${SCRIPT_DIR}/synthetic_data_generator.py"
    
    if [ $? -ne 0 ]; then
        echo "❌ Synthetic data generation failed"
        exit 1
    fi
else
    echo "📁 File mode: Using data from /data directory"
fi

echo "Running Transform + Load Agent..."
export PIPELINE_DATA_MODE
python3 "${SCRIPT_DIR}/transform_load_agent.py"

echo "Fetching Results using Query Agent..."
python3 "${SCRIPT_DIR}/query_agent.py"

if [ "${STARTED_BY_SCRIPT}" -eq 1 ]; then
    echo "Stopping Exasol started by this pipeline..."
    "${EXASOL_NANO_BIN}" stop
else
    echo "Leaving Exasol running (it was already running before this script)."
fi

echo "Pipeline completed successfully."
