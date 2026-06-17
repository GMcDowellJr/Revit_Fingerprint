#!/usr/bin/env bash
set -euo pipefail

# Ensure the graphify CLI is present where .codex/hooks.json expects it.
# The PyPI package is named "graphifyy" and installs the "graphify" console script.
GRAPHIFY_BIN="/root/.local/bin/graphify"

if [ ! -x "$GRAPHIFY_BIN" ]; then
  python3 -m pip install graphifyy -q --user --break-system-packages
fi

if [ ! -x "$GRAPHIFY_BIN" ]; then
  resolved_bin="$(command -v graphify || true)"
  if [ -n "$resolved_bin" ]; then
    mkdir -p "$(dirname "$GRAPHIFY_BIN")"
    ln -sf "$resolved_bin" "$GRAPHIFY_BIN"
  fi
fi

if [ ! -x "$GRAPHIFY_BIN" ]; then
  echo "graphify CLI was not installed at $GRAPHIFY_BIN" >&2
  exit 1
fi
