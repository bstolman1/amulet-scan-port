#!/usr/bin/env bash
set -euo pipefail

# Deploy Canton Ledger BigQuery pipeline
#
# Required environment variables:
#   PROJECT_ID   — GCP project ID (e.g., my-gcp-project-123)
#   BUCKET_NAME  — GCS bucket name without gs:// prefix (e.g., canton-network-data)
#
# Optional:
#   BQ_LOCATION  — BigQuery dataset location (default: US)
#   DRY_RUN      — If set to "1", prints rendered SQL without executing
#
# Usage:
#   PROJECT_ID=my-project BUCKET_NAME=my-bucket ./deploy.sh
#   PROJECT_ID=my-project BUCKET_NAME=my-bucket ./deploy.sh bronze
#   PROJECT_ID=my-project BUCKET_NAME=my-bucket ./deploy.sh silver
#   PROJECT_ID=my-project BUCKET_NAME=my-bucket DRY_RUN=1 ./deploy.sh

: "${PROJECT_ID:?PROJECT_ID is required}"
: "${BUCKET_NAME:?BUCKET_NAME is required}"
BQ_LOCATION="${BQ_LOCATION:-US}"
DRY_RUN="${DRY_RUN:-0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

render_sql() {
  sed -e "s|\${PROJECT_ID}|${PROJECT_ID}|g" \
      -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      "$1"
}

run_sql() {
  local file="$1"
  local rendered
  rendered="$(render_sql "$file")"

  echo "--- $(basename "$file") ---"
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "$rendered"
    echo ""
    return
  fi

  echo "$rendered" | bq query \
    --project_id="${PROJECT_ID}" \
    --use_legacy_sql=false \
    --max_rows=0 \
    --nouse_cache
  echo ""
}

deploy_bronze() {
  echo "=== Deploying Bronze Layer ==="
  for f in "$SCRIPT_DIR"/bronze/[0-9]*.sql; do
    run_sql "$f"
  done
}

deploy_silver() {
  echo "=== Deploying Silver Layer ==="
  for f in "$SCRIPT_DIR"/silver/[0-9]*.sql; do
    run_sql "$f"
  done
}

layer="${1:-all}"
case "$layer" in
  bronze) deploy_bronze ;;
  silver) deploy_silver ;;
  all)    deploy_bronze; deploy_silver ;;
  *)      echo "Usage: $0 [bronze|silver|all]"; exit 1 ;;
esac

echo "=== Done ==="
