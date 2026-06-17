#!/bin/bash
set -euo pipefail

if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

# pytest is the only declared dev dependency (see CLAUDE.md)
pip install pytest -q --break-system-packages 2>/dev/null || pip install pytest -q

# graphify CLI: graphify-out/graph.json is checked into the repo, but the
# CLI binary itself is not. Without it, `graphify query/explain/path` (used
# by .claude/settings.json hooks and CLAUDE.md guidance) silently fails.
if ! command -v graphify >/dev/null 2>&1; then
  pip install graphifyy -q --break-system-packages 2>/dev/null \
    || pip install graphifyy -q
fi
