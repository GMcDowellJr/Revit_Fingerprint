# Codex environment setup

This repo keeps `graphify-out/graph.json` checked in and registers a Codex
`PreToolUse` hook for Graphify hook checks.

Cloud Codex environments may not be able to install Python packages at hook
runtime. To keep those sessions usable, `.codex/hook-check.sh` treats Graphify
as optional: it runs `/root/.local/bin/graphify hook-check` when that binary is
available, falls back to a `graphify` executable on `PATH`, and otherwise exits
successfully without running the check.

For local or non-cloud Codex environments with pip access, run:

```bash
.codex/setup.sh
```

The setup script installs the PyPI package `graphifyy` with pip's user scheme so
the `graphify` console script is available at `/root/.local/bin/graphify`, which
is the path expected by the hook.
