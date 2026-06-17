#!/usr/bin/env bash
set -euo pipefail

# PreToolUse hooks must never block Codex when the cloud image cannot install
# graphify. Use the expected binary if setup created it, or any graphify on PATH;
# otherwise skip the optional hook check.
GRAPHIFY_BIN="/root/.local/bin/graphify"

if [ -x "$GRAPHIFY_BIN" ]; then
  exec "$GRAPHIFY_BIN" hook-check
fi

if resolved_bin="$(command -v graphify 2>/dev/null)"; then
  exec "$resolved_bin" hook-check
fi

exit 0
