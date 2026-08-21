# -*- coding: utf-8 -*-
"""Pure-Python tests for mapping/_dynamo_bootstrap.py.

No Revit dependency -- covers repo-root resolution priority (explicit >
env vars > __file__ fallback), the explicit-bad-root fail-loud behavior,
unconditional sys.modules purging, and sys.path promotion (remove-then-
reinsert-at-front). add_revit_api_references()/bootstrap()'s clr-touching
path is exercised only up to the point where `import clr` fails outside a
Revit/Dynamo host (bootstrap(add_revit_references=False) covers the rest).
"""

import os
import sys
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import pytest

from mapping import _dynamo_bootstrap as boot


# ---------------------------------------------------------------------------
# looks_like_repo_root / resolve_repo_root
# ---------------------------------------------------------------------------

def test_looks_like_repo_root_true_for_real_checkout():
    assert boot.looks_like_repo_root(_REPO_ROOT) is True


def test_looks_like_repo_root_false_for_bogus_path(tmp_path):
    assert boot.looks_like_repo_root(str(tmp_path)) is False


def test_resolve_repo_root_explicit_valid_wins_over_env(monkeypatch, tmp_path):
    bogus_env_root = tmp_path / "not_a_repo"
    bogus_env_root.mkdir()
    monkeypatch.setenv("REVIT_FINGERPRINT_REPO_ROOT_SELECTED", str(bogus_env_root))

    resolved = boot.resolve_repo_root(_REPO_ROOT)
    assert resolved == os.path.abspath(_REPO_ROOT)


def test_resolve_repo_root_explicit_bad_raises_not_silently_falls_back(monkeypatch, tmp_path):
    # A valid env var is present, but must NOT be used as a silent fallback
    # once an explicit (bad) repo_root was given -- fail loudly instead.
    monkeypatch.setenv("REVIT_FINGERPRINT_REPO_ROOT_SELECTED", _REPO_ROOT)
    bogus = tmp_path / "typo_checkout"
    bogus.mkdir()

    with pytest.raises(RuntimeError):
        boot.resolve_repo_root(str(bogus))


def test_resolve_repo_root_env_var_priority(monkeypatch, tmp_path):
    bogus = tmp_path / "not_a_repo"
    bogus.mkdir()
    monkeypatch.setenv("REVIT_FINGERPRINT_REPO_ROOT_SELECTED", str(bogus))
    monkeypatch.setenv("REVIT_FINGERPRINT_REPO_DIR", _REPO_ROOT)

    # REVIT_FINGERPRINT_REPO_ROOT_SELECTED is checked first; since it doesn't
    # look like a repo root it's skipped, falling through to REPO_DIR.
    resolved = boot.resolve_repo_root(None)
    assert resolved == os.path.abspath(_REPO_ROOT)


def test_resolve_repo_root_falls_back_to_module_file(monkeypatch):
    monkeypatch.delenv("REVIT_FINGERPRINT_REPO_ROOT_SELECTED", raising=False)
    monkeypatch.delenv("REVIT_FINGERPRINT_REPO_DIR", raising=False)

    resolved = boot.resolve_repo_root(None)
    assert resolved == os.path.abspath(_REPO_ROOT)


def test_resolve_repo_root_raises_when_nothing_resolves(monkeypatch):
    monkeypatch.delenv("REVIT_FINGERPRINT_REPO_ROOT_SELECTED", raising=False)
    monkeypatch.delenv("REVIT_FINGERPRINT_REPO_DIR", raising=False)
    monkeypatch.setattr(boot, "__file__", "/nonexistent/nowhere/mapping/_dynamo_bootstrap.py")

    with pytest.raises(RuntimeError):
        boot.resolve_repo_root(None)


# ---------------------------------------------------------------------------
# purge_repo_modules
# ---------------------------------------------------------------------------

def test_purge_repo_modules_removes_matching_prefixes_only():
    sys.modules["mapping._fake_test_module"] = object()
    sys.modules["core._fake_test_module"] = object()
    sys.modules["domains._fake_test_module"] = object()
    sys.modules["unrelated._fake_test_module"] = object()
    try:
        boot.purge_repo_modules()
        assert "mapping._fake_test_module" not in sys.modules
        assert "core._fake_test_module" not in sys.modules
        assert "domains._fake_test_module" not in sys.modules
        assert "unrelated._fake_test_module" in sys.modules
    finally:
        for name in (
            "mapping._fake_test_module",
            "core._fake_test_module",
            "domains._fake_test_module",
            "unrelated._fake_test_module",
        ):
            sys.modules.pop(name, None)


def test_purge_repo_modules_custom_prefixes():
    sys.modules["customprefix._fake_test_module"] = object()
    sys.modules["mapping._fake_test_module_2"] = object()
    try:
        boot.purge_repo_modules(prefixes=("customprefix",))
        assert "customprefix._fake_test_module" not in sys.modules
        # mapping.* is untouched since it wasn't in the custom prefix set.
        assert "mapping._fake_test_module_2" in sys.modules
    finally:
        sys.modules.pop("customprefix._fake_test_module", None)
        sys.modules.pop("mapping._fake_test_module_2", None)


# ---------------------------------------------------------------------------
# promote_on_sys_path
# ---------------------------------------------------------------------------

def test_promote_on_sys_path_inserts_at_front(tmp_path):
    p = str(tmp_path)
    original = list(sys.path)
    try:
        sys.path.append(p)
        boot.promote_on_sys_path(p)
        assert sys.path[0] == p
        assert sys.path.count(p) == 1
    finally:
        sys.path[:] = [x for x in sys.path if x != p]
        sys.path[:] = original


def test_promote_on_sys_path_moves_existing_entry_to_front(tmp_path):
    p = str(tmp_path)
    original = list(sys.path)
    try:
        sys.path.append("some/other/dir")
        sys.path.append(p)
        boot.promote_on_sys_path(p)
        assert sys.path[0] == p
        assert sys.path.count(p) == 1
    finally:
        sys.path[:] = [x for x in sys.path if x != p and x != "some/other/dir"]
        sys.path[:] = original


# ---------------------------------------------------------------------------
# bootstrap() orchestration (without touching Revit assemblies)
# ---------------------------------------------------------------------------

def test_bootstrap_without_revit_references_returns_repo_root(monkeypatch):
    monkeypatch.delenv("REVIT_FINGERPRINT_REPO_ROOT_SELECTED", raising=False)
    monkeypatch.delenv("REVIT_FINGERPRINT_REPO_DIR", raising=False)

    resolved = boot.bootstrap(_REPO_ROOT, add_revit_references=False)
    assert resolved == os.path.abspath(_REPO_ROOT)
    assert sys.path[0] == os.path.abspath(_REPO_ROOT)


def test_bootstrap_purges_stale_modules_before_promoting(monkeypatch):
    sys.modules["core._fake_bootstrap_probe"] = object()
    try:
        boot.bootstrap(_REPO_ROOT, add_revit_references=False)
        assert "core._fake_bootstrap_probe" not in sys.modules
    finally:
        sys.modules.pop("core._fake_bootstrap_probe", None)
