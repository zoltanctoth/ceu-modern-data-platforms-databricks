#!/bin/bash
# Bidirectional sync between local and Databricks workspace
# Usage: ./sync.sh

set -e

# Get the repo root (parent of .dev folder)
LOCAL_PATH="$(cd "$(dirname "$0")/.." && pwd)"
PROFILE="ceu-prep-2026"
WORKSPACE_PATH="/Workspace/Users/zoltan+de2prep2026@nordquant.com/ceu-modern-data-platforms-databricks"

echo "Starting bidirectional sync with: $WORKSPACE_PATH"
echo "Press Ctrl+C to stop..."

# Cleanup on exit
trap 'kill $(jobs -p) 2>/dev/null' EXIT

# Watch local → Databricks
/usr/local/bin/databricks sync "$LOCAL_PATH" "$WORKSPACE_PATH" --profile "$PROFILE" --watch &

# Watch Databricks → local
/usr/local/bin/databricks sync "$WORKSPACE_PATH" "$LOCAL_PATH" --profile "$PROFILE" --direction TO_LOCAL --watch &

wait
