# -*- coding: utf-8 -*-
"""
Shared Dynamo CPython3 bootstrap logic for mapping/ entry points.

Every mapping/create_*_mappings.py Dynamo entry point needs identical
handling to locate the Revit_Fingerprint checkout, make it importable, and
register the Revit API assemblies pythonnet needs before anything else in
this repo can be imported. This logic went through three rounds of review
(PR #441) to get right; the specific failure modes each piece defends
against:

  - NameError on `__file__` when the entry-point script is pasted directly
    into a Dynamo Python Script node (executed from a string -- `File
    "<string>"` -- not loaded from a file on disk).
  - Stale sys.modules left over from a previous run against a DIFFERENT
    checkout, in the same persistent Dynamo CPython3 interpreter (a bare
    "purge only if stale" check keyed on one representative module can miss
    modules cached by an entirely different earlier script).
  - sys.path ordering: the correct checkout can be present on sys.path but
    not first (e.g. a prior run in this same interpreter selected a
    different checkout that inserted itself ahead), so a plain
    "insert if absent" leaves the wrong checkout resolving first.
  - A bad-but-nonempty explicit repo_root (e.g. a typo'd IN[2]) must fail
    loudly rather than silently falling back to the environment or
    `__file__` and running against an unintended checkout.

Chicken-and-egg note for callers
---------------------------------
This module cannot bootstrap its OWN import for a Dynamo Python Script node
running from pasted text: `mapping` is not on sys.path yet (that is exactly
the problem being solved), so `import mapping._dynamo_bootstrap` cannot
succeed until a repo root is already known by some other means. Every entry
point therefore still needs a small, unavoidable inline loader shim that
locates and loads *this file* directly from disk (via
`importlib.util.spec_from_file_location`, using IN[2]/env vars/its own
`__file__` as repo-root candidates) before it can call into `bootstrap()`
below. See `mapping/create_line_pattern_mappings.py` for the canonical
shim -- copy it verbatim into any new entry point; everything past that
point (validating a candidate repo root, purging stale modules, promoting
sys.path, and registering RevitServices/RevitAPI) is centralized here and
must not be re-derived per domain.
"""

from __future__ import annotations

import os
import sys
from typing import Iterable, Optional, Sequence

DEFAULT_PURGE_PREFIXES: Sequence[str] = ("mapping", "core", "domains")

_REPO_ROOT_ENV_KEYS: Sequence[str] = (
    "REVIT_FINGERPRINT_REPO_ROOT_SELECTED",
    "REVIT_FINGERPRINT_REPO_DIR",
)

_REPO_ROOT_MARKER_SUBDIRS: Sequence[str] = ("mapping", "core", "domains")


def looks_like_repo_root(p: object) -> bool:
    """True if `p` looks like a Revit_Fingerprint checkout root (has mapping/,
    core/, and domains/ subdirectories)."""
    try:
        base = os.path.abspath(str(p))
    except Exception:
        return False
    return all(os.path.exists(os.path.join(base, sub)) for sub in _REPO_ROOT_MARKER_SUBDIRS)


def resolve_repo_root(explicit_repo_root: Optional[str] = None) -> str:
    """Resolve the Revit_Fingerprint repo root, in priority order:

    1. `explicit_repo_root` (e.g. a Dynamo entry point's IN[2]) -- an explicit
       selection, not a hint. If it doesn't look like a repo root, this raises
       immediately rather than silently falling through to the environment or
       `__file__`, which could otherwise resolve a *different* checkout (e.g. a
       stale REVIT_FINGERPRINT_REPO_ROOT_SELECTED left over from a previous run
       in the same persistent Dynamo session) with no error at all.
    2. REVIT_FINGERPRINT_REPO_ROOT_SELECTED / REVIT_FINGERPRINT_REPO_DIR
       environment variables, in that order.
    3. This module's own `__file__` (`mapping/_dynamo_bootstrap.py` always
       lives at `<repo_root>/mapping/_dynamo_bootstrap.py` once successfully
       loaded from disk, so its parent-of-parent directory is the repo root --
       equivalent to an entry point's own `__file__` fallback, since both
       files live in the same `mapping/` directory).

    Raises RuntimeError if none of the above resolves to a valid repo root.
    """
    if explicit_repo_root:
        if looks_like_repo_root(explicit_repo_root):
            return os.path.abspath(explicit_repo_root)
        raise RuntimeError(
            "explicit repo_root ({!r}) does not look like a Revit_Fingerprint checkout "
            "(expected mapping/, core/, and domains/ subdirectories) -- not falling back "
            "to the environment or __file__ since it was explicitly given.".format(
                explicit_repo_root
            )
        )

    for env_key in _REPO_ROOT_ENV_KEYS:
        try:
            env_val = str(os.environ.get(env_key, "")).strip()
        except Exception:
            env_val = ""
        if env_val and looks_like_repo_root(env_val):
            return os.path.abspath(env_val)

    try:
        this_dir = os.path.dirname(os.path.abspath(__file__))
        candidate = os.path.dirname(this_dir)
        if looks_like_repo_root(candidate):
            return candidate
    except Exception:
        pass

    raise RuntimeError(
        "Could not determine the Revit_Fingerprint repo root: no explicit repo_root "
        "was given, neither REVIT_FINGERPRINT_REPO_ROOT_SELECTED nor "
        "REVIT_FINGERPRINT_REPO_DIR is set, and this module's own location did not "
        "resolve to a checkout. Pass the checkout's absolute path explicitly."
    )


def purge_repo_modules(prefixes: Iterable[str] = DEFAULT_PURGE_PREFIXES) -> None:
    """Unconditionally purge cached repo modules from sys.modules.

    A persistent Dynamo CPython session keeps its interpreter (and
    sys.modules) alive across node re-runs. An unconditional purge sidesteps
    needing a staleness heuristic: a "purge only if stale" check keyed on one
    representative module can miss modules cached by an entirely different
    earlier script (e.g. the extraction runner) that never touched this
    module's own namespace at all. Mirrors runner/thin_runner.py's
    `_purge_repo_modules()`, generalized to always run.
    """
    prefixes = tuple(prefixes)
    for name in list(sys.modules.keys()):
        if name in prefixes or any(name.startswith(p + ".") for p in prefixes):
            sys.modules.pop(name, None)


def promote_on_sys_path(repo_root: str) -> None:
    """Ensure `repo_root` is first on sys.path, removing any existing entry
    first. A plain "insert if absent" would leave a previously-selected
    checkout earlier on sys.path if this repo_root is already present but not
    first -- purged modules would simply re-resolve from there instead.
    """
    if repo_root in sys.path:
        sys.path.remove(repo_root)
    sys.path.insert(0, repo_root)


def add_revit_api_references() -> None:
    """Reference RevitServices/RevitAPI so pythonnet exposes them.

    A fresh Dynamo CPython3 Python Script node does not expose
    RevitServices/RevitAPI to pythonnet until these assemblies are explicitly
    referenced -- same convention as tools/probes/*.py and
    runner/run_dynamo.py's own clr.AddReference("RevitServices") before
    importing DocumentManager. Must happen before importing DocumentManager
    or any module that imports Autodesk.Revit.DB symbols at module load time.
    """
    import clr  # noqa: F401 -- only available inside the Dynamo/Revit host

    clr.AddReference("RevitServices")
    clr.AddReference("RevitAPI")


def bootstrap(
    explicit_repo_root: Optional[str] = None,
    *,
    add_revit_references: bool = True,
    purge_prefixes: Iterable[str] = DEFAULT_PURGE_PREFIXES,
) -> str:
    """Full bootstrap sequence for a mapping/ Dynamo entry point: resolve the
    repo root, purge stale cached modules, promote the repo root to the front
    of sys.path, and (by default) register the Revit API assemblies.

    Returns the resolved, absolute repo root.
    """
    repo_root = resolve_repo_root(explicit_repo_root)
    purge_repo_modules(purge_prefixes)
    promote_on_sys_path(repo_root)
    if add_revit_references:
        add_revit_api_references()
    return repo_root
