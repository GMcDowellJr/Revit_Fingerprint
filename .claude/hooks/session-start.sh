#!/bin/bash
set -euo pipefail

if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

# pytest is the only declared dev dependency (see CLAUDE.md)
pip install pytest -q --break-system-packages 2>/dev/null || pip install pytest -q
