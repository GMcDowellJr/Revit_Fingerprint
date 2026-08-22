import subprocess
import sys
from pathlib import Path

import pytest

TOOL_DIR = Path(__file__).resolve().parent.parent
TOOL_SCRIPT = TOOL_DIR / "repo_context.py"

# Lets white-box tests `import rc_scan`, `import rc_common`, etc. directly
# (the CLI-level tests below use subprocess via run_tool() instead).
if str(TOOL_DIR) not in sys.path:
    sys.path.insert(0, str(TOOL_DIR))


def run_tool(args, cwd=None, timeout=60):
    cmd = [sys.executable, str(TOOL_SCRIPT)] + [str(a) for a in args]
    # A bounded timeout so a hang-regression fails the test instead of
    # stalling the whole suite/CI run indefinitely.
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout)


def write_files(root: Path, files: dict) -> Path:
    for rel, content in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            p.write_bytes(content)
        else:
            p.write_text(content, encoding="utf-8")
    return root


@pytest.fixture
def repo(tmp_path):
    return tmp_path / "repo"


@pytest.fixture
def out(tmp_path):
    return tmp_path / "out"
