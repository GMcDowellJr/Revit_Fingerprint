"""--dry-run coverage for tools/corpus_update_runbook.py.

Each invocation must spawn no subprocess, delete nothing, and print one
"DRYRUN: " line per would-be command, in step order.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import tools.corpus_update_runbook as runbook  # noqa: E402


def _no_subprocess(*args, **kwargs):
    raise AssertionError(f"subprocess.run called in dry-run: {args!r}")


@pytest.fixture
def exports_root(tmp_path, monkeypatch):
    monkeypatch.setattr(subprocess, "run", _no_subprocess)
    monkeypatch.setattr(os, "chdir", lambda *_: (_ for _ in ()).throw(AssertionError("chdir in dry-run")))
    root = tmp_path / "Fingerprint_Data"
    (root / "records").mkdir(parents=True)
    (root / "name_key").mkdir()
    (root / "segments" / "seg_a" / "results").mkdir(parents=True)
    records_csv = root / "records" / "records.csv"
    records_csv.write_text("")
    name_key_csv = root / "name_key" / "name_key_results.csv"
    name_key_csv.write_text("")
    # name_key newer than records so the freshness guard passes.
    now = time.time()
    os.utime(records_csv, (now - 60, now - 60))
    os.utime(name_key_csv, (now, now))
    (root / "segments" / "seg_a" / "results" / "latent_purgeable.csv").write_text("")
    (root / "records" / "latent_purgeable.csv").write_text("")
    yield root
    runbook.DRY_RUN = False


# (invocation, ordered substrings expected among DRYRUN lines)
CASES = [
    (["-Run", "A"], ["tools/run_extract_all.py"]),
    (["-Run", "A", "-ForceAll"], ["tools/run_extract_all.py"]),
    (["-Run", "A", "-NameKey"], ["tools/run_extract_all.py", "tools/apply_name_key_policy.py"]),
    (["-Run", "B"], []),
    (["-Run", "B", "-RunAuthorityPatterns"],
     ["tools/run_extract_all.py", "tools/label_synthesis/patch_all_domain_patterns.py"]),
    (["-Run", "B", "-NameKey"], ["tools/generate_name_key_patterns.py"]),
    (["-Run", "C"], ["tools/build_segment_manifest.py", "tools/run_segment_orchestrator.py",
                     "tools/build_results_registry.py", "tools/label_synthesis/patch_all_domain_patterns.py"]),
    (["-Run", "C", "-ForceAll"], ["tools/build_segment_manifest.py", "would remove", "would remove",
                                  "tools/run_segment_orchestrator.py", "tools/build_results_registry.py",
                                  "tools/label_synthesis/patch_all_domain_patterns.py"]),
    (["-Run", "C", "-NameKey"], ["tools/build_segment_manifest.py", "tools/run_segment_orchestrator.py",
                                 "tools/build_results_registry.py", "tools/label_synthesis/patch_all_domain_patterns.py"]),
]

STEP_MARKERS = {
    "A": ["=== RUN A: Flatten / Apply / Placeholders ===", "=== RUN A COMPLETE ==="],
    "B": ["=== RUN B: Authority / Patterns / Patch ===", "=== RUN B COMPLETE - proceed to Run C ==="],
    "C": ["--- C1: segment manifest ---", "--- C1.5:", "--- C2: segment orchestrator",
          "--- C2.5: rebuild BI results registry ---", "--- C3: re-patch all segment domain_patterns ---",
          "=== RUN C COMPLETE ==="],
}


def _assert_in_order(haystack, needles):
    pos = 0
    for needle in needles:
        for idx in range(pos, len(haystack)):
            if needle in haystack[idx]:
                pos = idx + 1
                break
        else:
            raise AssertionError(f"{needle!r} not found in order in {haystack!r}")


@pytest.mark.parametrize("invocation,expected", CASES, ids=[" ".join(c[0]) for c in CASES])
def test_dry_run_emits_plan_without_side_effects(invocation, expected, exports_root, capsys, monkeypatch):
    monkeypatch.setenv("NO_COLOR", "1")
    rc = runbook.main([*invocation, "-DryRun", "-ExportsRoot", str(exports_root)])
    out = capsys.readouterr().out
    assert rc == 0
    lines = out.splitlines()
    dry = [line for line in lines if line.startswith("DRYRUN: ")]
    commands = [line for line in dry if "would chdir" not in line]
    assert dry[0].startswith("DRYRUN: would chdir ")
    assert len(commands) == len(expected)
    _assert_in_order(commands, expected)
    _assert_in_order(lines, STEP_MARKERS[invocation[1]])
    assert "guard would fail" not in out
    # Nothing deleted.
    assert (exports_root / "records" / "latent_purgeable.csv").exists()
    assert (exports_root / "segments" / "seg_a" / "results" / "latent_purgeable.csv").exists()


def test_force_all_flag_threads_through(exports_root, capsys, monkeypatch):
    monkeypatch.setenv("NO_COLOR", "1")
    runbook.main(["-Run", "A", "-ForceAll", "-DryRun", "-ExportsRoot", str(exports_root)])
    assert "--force-full-cache" in capsys.readouterr().out
    runbook.main(["-Run", "C", "-ForceAll", "-DryRun", "-ExportsRoot", str(exports_root)])
    orch = [l for l in capsys.readouterr().out.splitlines() if "run_segment_orchestrator.py" in l]
    assert orch and orch[0].endswith(" --force")


def test_name_key_guards_report_and_continue_in_dry_run(exports_root, capsys, monkeypatch):
    monkeypatch.setenv("NO_COLOR", "1")
    nk = exports_root / "name_key" / "name_key_results.csv"
    # Stale: name_key older than records.
    old = time.time() - 3600
    os.utime(nk, (old, old))
    rc = runbook.main(["-Run", "C", "-NameKey", "-DryRun", "-ExportsRoot", str(exports_root)])
    out = capsys.readouterr().out
    assert rc == 0
    assert "DRYRUN: guard would fail:" in out and "older than" in out
    assert "--comparison-target both" in out
    # Missing.
    nk.unlink()
    rc = runbook.main(["-Run", "C", "-NameKey", "-DryRun", "-ExportsRoot", str(exports_root)])
    out = capsys.readouterr().out
    assert rc == 0
    assert "DRYRUN: guard would fail: -NameKey requires" in out
    assert "tools/label_synthesis/patch_all_domain_patterns.py" in out


def test_dry_run_flag_spellings_normalize():
    for spelling in ("-DryRun", "-dryrun", "--dryrun", "--dry-run"):
        assert runbook.normalize_powershell_style_argv([spelling]) == ["--dry-run"]


def test_render_command_uses_native_quoting(monkeypatch):
    cmd = ["C:\\Python\\python.exe", "tools/x.py", "--out", "C:\\My Data\\out"]
    monkeypatch.setattr(runbook.os, "name", "nt")
    assert runbook.render_command(cmd) == 'C:\\Python\\python.exe tools/x.py --out "C:\\My Data\\out"'
    monkeypatch.setattr(runbook.os, "name", "posix")
    assert runbook.render_command(cmd) == "'C:\\Python\\python.exe' tools/x.py --out 'C:\\My Data\\out'"
