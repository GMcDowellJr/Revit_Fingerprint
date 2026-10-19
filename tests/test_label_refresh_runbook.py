"""--dry-run coverage for tools/label_refresh_runbook.py (corrected L1-L4 port)."""
from __future__ import annotations

import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import tools.corpus_update_runbook as corpus_runbook  # noqa: E402
import tools.label_refresh_runbook as runbook  # noqa: E402


def _no_subprocess(*args, **kwargs):
    raise AssertionError(f"subprocess.run called in dry-run: {args!r}")


@pytest.fixture
def exports_root(tmp_path, monkeypatch):
    monkeypatch.setattr(subprocess, "run", _no_subprocess)
    monkeypatch.setattr(os, "chdir", lambda *_: (_ for _ in ()).throw(AssertionError("chdir in dry-run")))
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    monkeypatch.setattr(runbook.getpass, "getpass",
                        lambda *_: (_ for _ in ()).throw(AssertionError("prompted in dry-run")))
    monkeypatch.setenv("NO_COLOR", "1")
    root = tmp_path / "Fingerprint_Data"
    (root / "records").mkdir(parents=True)
    (root / "records" / "run_registry.csv").write_text(
        "segment_id,run_type,status,output_folder\n"
        "s1,bundle,complete,seg_ok\n"
        "s2,bundle,failed,seg_failed\n",
        encoding="utf-8",
    )
    for folder in ("seg_ok", "seg_failed"):
        for view in ("all", "used"):
            d = root / "segments" / folder / "results" / "bundle_analysis" / view / "arrowheads"
            d.mkdir(parents=True)
            (d / "bundles.csv").write_text("")
    yield root
    corpus_runbook.DRY_RUN = False


def _dry(out):
    return [l for l in out.splitlines() if l.startswith("DRYRUN: ")]


def test_dry_run_plan_l1_to_l4(exports_root, capsys):
    rc = runbook.main(["-DryRun", "-ExportsRoot", str(exports_root)])
    out = capsys.readouterr().out
    assert rc == 0
    dry = _dry(out)
    assert dry[0].startswith("DRYRUN: would chdir ")
    assert "DRYRUN: OPENROUTER_API_KEY unset; would prompt" in dry
    assert "OPENROUTER_API_KEY" not in os.environ

    cmds = [l for l in dry if "would " not in l and "OPENROUTER" not in l]
    # L1 + 6 x L2 + L3 + 2 x L4 (seg_ok all/used).
    assert len(cmds) == 10
    assert "build_identity_items_lookup.py" in cmds[0]
    l2 = cmds[1:7]
    assert [c.split("--domain ")[1].split()[0] for c in l2] == list(runbook.L2_DOMAINS)
    for c in l2:
        assert "-m tools.label_synthesis.synthesize_fragmented_labels" in c
        assert "--filter-mode bundles" in c
        assert f"--segments-root {shlex.quote(str(exports_root / 'segments'))}" in c
        assert f"--registry-file {shlex.quote(str(exports_root / 'records' / 'run_registry.csv'))}" in c
    assert "patch_all_domain_patterns.py" in cmds[7]
    assert f"--results-root {shlex.quote(str(exports_root))} " in cmds[7]
    l4 = cmds[8:]
    assert all("export_bundle_pattern_detail.py" in c and "--output-folder seg_ok" in c for c in l4)
    assert "--purge-view all" in l4[0] and "--purge-view used" in l4[1]
    assert "seg_failed" not in out

    # Step markers in order.
    markers = ["--- L1:", "--- L2:", "--- L3:", "--- L4:", "=== LABEL REFRESH COMPLETE ==="]
    positions = [out.index(m) for m in markers]
    assert positions == sorted(positions)


def test_corrected_layout(exports_root, capsys):
    runbook.main(["--dry-run", "--exports-root", str(exports_root)])
    out = capsys.readouterr().out
    # results == exports_root, not exports_root/exports/results.
    assert str(exports_root / "exports" / "results") not in out
    assert f"--records-dir {shlex.quote(str(exports_root / 'records'))}" in out
    assert f"--exports-dir {shlex.quote(str(exports_root / 'exports'))}" in out


def test_l4_skips_view_without_bundles(exports_root, capsys):
    used = exports_root / "segments" / "seg_ok" / "results" / "bundle_analysis" / "used"
    (used / "arrowheads" / "bundles.csv").unlink()
    runbook.main(["-DryRun", "-ExportsRoot", str(exports_root)])
    out = capsys.readouterr().out
    assert "skipping segment=seg_ok  view=used" in out
    assert "--purge-view used" not in out


def test_missing_registry_reported_in_dry_run(exports_root, capsys):
    (exports_root / "records" / "run_registry.csv").unlink()
    rc = runbook.main(["-DryRun", "-ExportsRoot", str(exports_root)])
    out = capsys.readouterr().out
    assert rc == 0
    assert "DRYRUN: guard would fail:" in out
    assert "export_bundle_pattern_detail.py" not in out
