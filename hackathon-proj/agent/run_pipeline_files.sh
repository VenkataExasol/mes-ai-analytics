#!/bin/bash

set -euo pipefail

echo "🏭 OEE Pipeline - FILE MODE"
echo "============================"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_PATH="${VENV_PATH:-${PROJECT_ROOT}/hackathon-env/bin/activate}"
EXASOL_NANO_BIN="${EXASOL_NANO_BIN:-exasol-nano}"
EXASOL_START_LOG="${EXASOL_START_LOG:-/tmp/exasol_nano_start.log}"
EXASOL_CONNECT_HOST="${EXASOL_CONNECT_HOST:-127.0.0.1}"
EXASOL_CONNECT_PORT="${EXASOL_CONNECT_PORT:-8563}"
EXASOL_READY_TIMEOUT_SEC="${EXASOL_READY_TIMEOUT_SEC:-120}"
STARTED_BY_SCRIPT=0

if ! command -v "${EXASOL_NANO_BIN}" >/dev/null 2>&1; then
    echo "❌ Exasol Nano binary not found: ${EXASOL_NANO_BIN}"
    exit 1
fi

is_port_open() {
    local host="$1"
    local port="$2"
    (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1
}

echo "🔌 Checking Exasol availability on ${EXASOL_CONNECT_HOST}:${EXASOL_CONNECT_PORT}..."
if is_port_open "${EXASOL_CONNECT_HOST}" "${EXASOL_CONNECT_PORT}"; then
    echo "✅ Exasol already running"
else
    echo "🚀 Starting Exasol..."
    nohup "${EXASOL_NANO_BIN}" start --memory-gb 2 --cpus 2 >"${EXASOL_START_LOG}" 2>&1 &
    STARTED_BY_SCRIPT=1
    echo "   Background process launched. Log: ${EXASOL_START_LOG}"
fi

echo ""
echo "⏳ Waiting for Exasol to be ready..."
elapsed=0
until is_port_open "${EXASOL_CONNECT_HOST}" "${EXASOL_CONNECT_PORT}"; do
    sleep 2
    elapsed=$((elapsed + 2))
    if [ "${elapsed}" -ge "${EXASOL_READY_TIMEOUT_SEC}" ]; then
        echo "❌ Exasol did not become ready within ${EXASOL_READY_TIMEOUT_SEC}s."
        if [ -f "${EXASOL_START_LOG}" ]; then
            echo "📋 Last lines from ${EXASOL_START_LOG}:"
            tail -n 40 "${EXASOL_START_LOG}" || true
        fi
        exit 1
    fi
done
echo "✅ Exasol is ready!"

if [ -f "${VENV_PATH}" ]; then
    echo ""
    echo "🐍 Activating virtual environment..."
    source "${VENV_PATH}"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 1: Schema Detection"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 "${SCRIPT_DIR}/schema_detection_agent.py"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 2: Mapping Agent"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 "${SCRIPT_DIR}/mapping_agent.py"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 3: Transform & Load (Excel Files)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Reading data from: ${PROJECT_ROOT}/data/"
echo ""

export PIPELINE_DATA_MODE=files
python3 "${SCRIPT_DIR}/transform_load_agent.py"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 4: Query Agent"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 "${SCRIPT_DIR}/query_agent.py"

echo ""
if [ "${STARTED_BY_SCRIPT}" -eq 1 ]; then
    echo "🛑 Stopping Exasol (started by this script)..."
    "${EXASOL_NANO_BIN}" stop
else
    echo "✅ Leaving Exasol running (it was already running before)"
fi

echo ""
echo "✅ FILE MODE PIPELINE COMPLETED SUCCESSFULLY!"
