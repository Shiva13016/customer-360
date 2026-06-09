#!/usr/bin/env bash
# validate_arm.sh - ARM validation and ADF trigger start/stop utility
# Usage:
#   bash scripts/validate_arm.sh stop <adf_name> <resource_group>
#   bash scripts/validate_arm.sh start <adf_name> <resource_group>

set -euo pipefail

ACTION=${1:-validate}
ADF_NAME=${2:-}
RESOURCE_GROUP=${3:-}

stop_triggers() {
  echo "Stopping all active triggers in ADF: ${ADF_NAME}"
  TRIGGERS=$(az datafactory trigger list \
    --factory-name "${ADF_NAME}" \
    --resource-group "${RESOURCE_GROUP}" \
    --query "[?properties.runtimeState=='Started'].name" \
    -o tsv)
  if [ -z "${TRIGGERS}" ]; then
    echo "  No active triggers found."
  else
    for trigger in ${TRIGGERS}; do
      echo "  Stopping: ${trigger}"
      az datafactory trigger stop \
        --factory-name "${ADF_NAME}" \
        --resource-group "${RESOURCE_GROUP}" \
        --name "${trigger}"
    done
  fi
  echo "Done stopping triggers."
}

start_triggers() {
  echo "Starting all triggers in ADF: ${ADF_NAME}"
  TRIGGERS=$(az datafactory trigger list \
    --factory-name "${ADF_NAME}" \
    --resource-group "${RESOURCE_GROUP}" \
    --query "[].name" \
    -o tsv)
  if [ -z "${TRIGGERS}" ]; then
    echo "  No triggers found."
  else
    for trigger in ${TRIGGERS}; do
      echo "  Starting: ${trigger}"
      az datafactory trigger start \
        --factory-name "${ADF_NAME}" \
        --resource-group "${RESOURCE_GROUP}" \
        --name "${trigger}"
    done
  fi
  echo "Done starting triggers."
}

case "${ACTION}" in
  stop) stop_triggers ;;
  start) start_triggers ;;
  *)
    echo "ERROR: Unknown action: ${ACTION}"
    echo "Usage: $0 [stop|start] <adf_name> <resource_group>"
    exit 1
    ;;
esac
