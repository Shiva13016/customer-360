#!/usr/bin/env bash
# quota_preflight.sh - Check Azure VM SKU quota before ADF IR deployment
# Usage: bash scripts/quota_preflight.sh <subscription_id>
set -euo pipefail

SUBSCRIPTION_ID=${1:-}
LOCATION="canadacentral"
REQUIRED_CORES=4

if [ -z "${SUBSCRIPTION_ID}" ]; then
  echo "ERROR: subscription_id required"
    exit 1
    fi

    echo "Running quota pre-flight check in ${LOCATION}..."

    USED=$(az vm list-usage --location "${LOCATION}" \
      --query "[?name.value=='cores'].currentValue" -o tsv 2>/dev/null || echo "0")
      LIMIT=$(az vm list-usage --location "${LOCATION}" \
        --query "[?name.value=='cores'].limit" -o tsv 2>/dev/null || echo "999")

        AVAILABLE=$((LIMIT - USED))
        echo "  vCPU cores: used=${USED}, limit=${LIMIT}, available=${AVAILABLE}"

        if [ "${AVAILABLE}" -lt "${REQUIRED_CORES}" ]; then
          echo "FAIL: Insufficient quota. Need ${REQUIRED_CORES} cores, only ${AVAILABLE} available."
            exit 1
            fi

            echo "PASS: Sufficient quota available (${AVAILABLE} cores free)."
            exit 0
