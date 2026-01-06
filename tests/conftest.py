# tests/conftest.py
import os
import sys


def pytest_configure():
    # Ensure repo root is importable so `import core...` works consistently,
    # regardless of how pytest is invoked.
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
