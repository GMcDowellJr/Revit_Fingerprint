"""Tests for policy externalization (PR3) in
tools/generate_governance_narrative.py: apply_governance_policy(),
--policy-dir wiring in main(), and the shipped policies/governance/*.json
files reproducing this generator's pre-externalization Python defaults
exactly.

apply_governance_policy() reassigns module-level globals (EXCLUDED_FROM_SCORING,
PASSIVE_INHERITANCE_RISK_DOMAINS, DOMAIN_GUIDANCE, STATIC_FINDINGS_GUIDANCE,
and every threshold constant) so every existing function keeps reading them
unchanged -- see docs/governance_evidence_package.md. Because these are
process-global, every test in this file resets policy state to the module's
own built-in defaults in an autouse fixture teardown, so no test here can
leak an overridden threshold into a test in another file that runs later in
the same pytest session.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from compare_cross_segment import SUMMARY_FIELDS, POOLED_FIELDS  # noqa: E402
from governance_policy import DEFAULT_POLICY_DIR, load_governance_policy  # noqa: E402
import generate_governance_narrative as g  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_governance_policy():
    """Undo any apply_governance_policy() call a test makes, so overridden
    thresholds/domain lists never leak into a test in another file that runs
    later in the same pytest session (module globals are process-wide)."""
    yield
    g.apply_governance_policy(load_governance_policy(None, g._POLICY_DEFAULTS))


def _summary_row(**overrides):
    r = {f: "" for f in SUMMARY_FIELDS}
    r.update(overrides)
    return r


def _pooled_row(**overrides):
    r = {f: "" for f in POOLED_FIELDS}
    r.update(overrides)
    return r


def _write_csv(path: Path, fields: list, rows: list) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def _minimal_fixture(tmp_path: Path) -> tuple[Path, Path]:
    summary_rows = [
        _summary_row(
            comparison_run_id="run1", segment_id_a="imperial|Template",
            segment_id_b="imperial|Project|acme", governance_role_a="Template",
            governance_role_b="Project", client_label_b="acme",
            comparison_type="template_to_project", domain="line_styles",
            all_pairwise_containment_a_in_b_mean="0.6", all_pairwise_jaccard_mean="0.5",
            n_files_a="3", n_files_b="10",
            executed_utc="2026-07-16T00:00:00Z", unit_system="imperial",
        ),
    ]
    pooled_rows = [
        _pooled_row(
            comparison_run_id="run1", segment_id="imperial|Project|acme",
            client_label="acme", governance_role="Project", pool_scope="parent_sibling",
            domain="line_styles", n_files_focal="10", n_files_pool="30",
            executed_utc="2026-07-16T00:00:00Z",
        ),
    ]
    summary_path = tmp_path / "cross_segment_summary.csv"
    pooled_path = tmp_path / "cross_segment_pooled.csv"
    _write_csv(summary_path, SUMMARY_FIELDS, summary_rows)
    _write_csv(pooled_path, POOLED_FIELDS, pooled_rows)
    return summary_path, pooled_path


def _run_main(monkeypatch, argv):
    monkeypatch.setattr(sys, "argv", ["generate_governance_narrative.py"] + argv)
    g.main()


# ---------------------------------------------------------------------------
# Shipped policies/governance/*.json reproduce the Python defaults exactly
# ---------------------------------------------------------------------------

def test_default_policy_dir_is_policies_governance_in_repo():
    assert DEFAULT_POLICY_DIR == g._DEFAULT_POLICY_DIR
    assert DEFAULT_POLICY_DIR.exists()


@pytest.mark.parametrize("filename,profile_key,behavioral_keys", [
    ("governance_thresholds.json", "thresholds", ("thresholds",)),
    ("domain_governance_policy.json", "domain_policy",
     ("excluded_from_scoring", "passive_inheritance_risk_domains", "domain_guidance",
      "static_findings_guidance")),
    ("client_onboarding_policy.json", "client_onboarding", ("thresholds",)),
    ("anomaly_thresholds.json", "anomaly_thresholds", ("thresholds",)),
])
def test_shipped_policy_file_matches_python_default_profile(filename, profile_key, behavioral_keys):
    """The shipped JSON and generate_governance_narrative.py's own
    _POLICY_DEFAULTS must agree on every BEHAVIORAL key (the values that
    actually drive classification output) -- "notes"/"profile_id" doc text
    is allowed to differ since it carries no behavior."""
    path = DEFAULT_POLICY_DIR / filename
    assert path.exists()
    on_disk = json.loads(path.read_text(encoding="utf-8"))
    default = g._POLICY_DEFAULTS[profile_key]
    for key in behavioral_keys:
        on_disk_value = on_disk[key]
        default_value = default[key]
        if isinstance(on_disk_value, list):
            on_disk_value, default_value = sorted(on_disk_value), sorted(default_value)
        assert on_disk_value == default_value, (
            f"{filename}[{key!r}] has drifted from generate_governance_narrative.py's "
            f"own _POLICY_DEFAULTS['{profile_key}'][{key!r}] -- these must stay "
            "value-identical so the default CLI invocation reproduces "
            "pre-externalization output exactly."
        )


def test_finding_rules_json_documents_every_rule_id_generator_emits():
    """generate_governance_narrative.py's own default finding_rules profile is
    deliberately empty (documentation-only, never drives logic -- see
    apply_governance_policy()'s docstring); the shipped JSON file is the real
    source of rule descriptions and must cover every _RULE_* constant the
    generator actually emits in governance_findings.json."""
    on_disk = json.loads((DEFAULT_POLICY_DIR / "finding_rules.json").read_text(encoding="utf-8"))
    rule_ids_in_code = {
        v for name, v in vars(g).items()
        if name.startswith("_RULE_") and isinstance(v, str)
    }
    assert rule_ids_in_code
    assert rule_ids_in_code <= set(on_disk["rules"].keys())


def test_loading_default_policy_dir_reproduces_module_level_defaults():
    """apply_governance_policy() with the shipped default policy dir must
    leave every threshold/domain-list constant equal to this module's own
    _DEFAULT_* literals -- i.e. loading the shipped JSON is a no-op on
    classification output."""
    policy = load_governance_policy(DEFAULT_POLICY_DIR, g._POLICY_DEFAULTS)
    g.apply_governance_policy(policy)
    assert g.TIER_STRONG_BASELINE_MIN == g._DEFAULT_TIER_STRONG_BASELINE_MIN
    assert g.XC_STRONG_CONVERGENCE == g._DEFAULT_XC_STRONG_CONVERGENCE
    assert g.CLIENT_COHERENCE_LOW == g._DEFAULT_CLIENT_COHERENCE_LOW
    assert g.ONBOARD_WP_STABLE_MIN == g._DEFAULT_ONBOARD_WP_STABLE_MIN
    assert g.EXCLUDED_FROM_SCORING == g._DEFAULT_EXCLUDED_FROM_SCORING
    assert g.PASSIVE_INHERITANCE_RISK_DOMAINS == g._DEFAULT_PASSIVE_INHERITANCE_RISK_DOMAINS
    assert g.DOMAIN_GUIDANCE == g._DEFAULT_DOMAIN_GUIDANCE
    assert g.STATIC_FINDINGS_GUIDANCE == g._DEFAULT_STATIC_FINDINGS_GUIDANCE
    assert g.WEAK_TC_MAX == g._DEFAULT_WEAK_TC_MAX
    assert g.PHASES_TP_EXTENSION_MAX == g._DEFAULT_PHASES_TP_EXTENSION_MAX
    assert g.PHASES_TW_MIN == g._DEFAULT_PHASES_TW_MIN
    assert g.PASSIVE_INHERITANCE_RISK_BUNDLE_SHARE_MAX == g._DEFAULT_PASSIVE_INHERITANCE_RISK_BUNDLE_SHARE_MAX
    assert g.PORTFOLIO_SHAPE_DENSITY_MIN == g._DEFAULT_PORTFOLIO_SHAPE_DENSITY_MIN
    assert all(s["source"] == "policy_file" for s in policy["load_status"].values())


# ---------------------------------------------------------------------------
# apply_governance_policy() overrides actually change classification output
# ---------------------------------------------------------------------------

def test_overriding_tier_threshold_changes_assign_tier_output(tmp_path):
    custom = json.loads(json.dumps(g._POLICY_DEFAULTS["thresholds"]))
    custom["thresholds"]["tier_strong_baseline_min"] = 0.50
    (tmp_path / "governance_thresholds.json").write_text(json.dumps(custom), encoding="utf-8")

    g.apply_governance_policy(load_governance_policy(tmp_path, g._POLICY_DEFAULTS))

    d = {"tc": 0.60, "cp": 0.60, "tp": 0.60, "wp_p10": 0.9, "wp_p90": 0.95, "wp_all": 0.9}
    assert g.assign_tier(d) == g.TIER_STRONG_BASELINE

    # Sanity: the same dict under the built-in default threshold is NOT strong-baseline.
    g.apply_governance_policy(load_governance_policy(None, g._POLICY_DEFAULTS))
    assert g.assign_tier(d) != g.TIER_STRONG_BASELINE


def test_overriding_excluded_from_scoring_changes_build_cascade(tmp_path):
    custom = json.loads(json.dumps(g._POLICY_DEFAULTS["domain_policy"]))
    custom["excluded_from_scoring"] = ["line_styles"]
    (tmp_path / "domain_governance_policy.json").write_text(json.dumps(custom), encoding="utf-8")

    g.apply_governance_policy(load_governance_policy(tmp_path, g._POLICY_DEFAULTS))
    assert g.EXCLUDED_FROM_SCORING == {"line_styles"}

    # build_cascade() skips rows for domains in EXCLUDED_FROM_SCORING.
    row = {f: "" for f in SUMMARY_FIELDS}
    row.update(
        segment_id_a="imperial|Template", segment_id_b="imperial|Project|acme",
        governance_role_a="Template", governance_role_b="Project",
        comparison_type="template_to_project", domain="line_styles",
        all_pairwise_containment_a_in_b_mean="0.9", n_files_a="3", n_files_b="10",
    )
    g.normalise_summary_schema([row])
    cascade = g.build_cascade([row])
    assert "line_styles" not in cascade


def test_overriding_excluded_from_scoring_changes_render_limitations_note(tmp_path):
    """Regression test for a PR review finding: render_limitations()'s
    'Excluded domain' note used to hardcode the literal
    'view_templates_renderings_drafting' rather than reading the resolved
    EXCLUDED_FROM_SCORING set -- a --policy-dir override that excludes a
    different domain must be reflected in the narrative, not just in the
    CSV/health output it's describing."""
    custom = json.loads(json.dumps(g._POLICY_DEFAULTS["domain_policy"]))
    custom["excluded_from_scoring"] = ["line_styles"]
    (tmp_path / "domain_governance_policy.json").write_text(json.dumps(custom), encoding="utf-8")

    g.apply_governance_policy(load_governance_policy(tmp_path, g._POLICY_DEFAULTS))
    md = g.render_limitations({"Project": 10})
    assert "`line_styles`" in md
    assert "view_templates_renderings_drafting" not in md


def test_render_limitations_excluded_note_pluralizes_for_multiple_domains(tmp_path):
    custom = json.loads(json.dumps(g._POLICY_DEFAULTS["domain_policy"]))
    custom["excluded_from_scoring"] = ["line_styles", "materials"]
    (tmp_path / "domain_governance_policy.json").write_text(json.dumps(custom), encoding="utf-8")

    g.apply_governance_policy(load_governance_policy(tmp_path, g._POLICY_DEFAULTS))
    md = g.render_limitations({"Project": 10})
    assert "**Excluded domains:**" in md
    assert "`line_styles`" in md and "`materials`" in md
    assert "are excluded" in md


def test_render_limitations_handles_empty_excluded_set(tmp_path):
    custom = json.loads(json.dumps(g._POLICY_DEFAULTS["domain_policy"]))
    custom["excluded_from_scoring"] = []
    (tmp_path / "domain_governance_policy.json").write_text(json.dumps(custom), encoding="utf-8")

    g.apply_governance_policy(load_governance_policy(tmp_path, g._POLICY_DEFAULTS))
    md = g.render_limitations({"Project": 10})
    assert "**Excluded domains:** none for this run's policy profile." in md


def test_overriding_domain_guidance_changes_detect_anomalies_text(tmp_path):
    custom = json.loads(json.dumps(g._POLICY_DEFAULTS["domain_policy"]))
    custom["domain_guidance"]["loaded_family_types"] = "CUSTOM GUIDANCE TEXT"
    (tmp_path / "domain_governance_policy.json").write_text(json.dumps(custom), encoding="utf-8")

    g.apply_governance_policy(load_governance_policy(tmp_path, g._POLICY_DEFAULTS))

    d = {
        "tc": None, "cp": None, "tp": None, "xc": None,
        "wp_p10": None, "wp_p90": None, "wp_all": None,
        "tp_by_scope": {}, "cp_by_scope": {},
        "bundle_schema": "none", "passive_indicator": None, "bundle_share_all": None,
        "wp_disc": {}, "tw": None,
    }
    notes = g.detect_anomalies("loaded_family_types", d)
    assert "CUSTOM GUIDANCE TEXT" in notes


def test_overriding_static_findings_guidance_changes_rendered_prose(tmp_path):
    custom = json.loads(json.dumps(g._POLICY_DEFAULTS["domain_policy"]))
    custom["static_findings_guidance"] = ["CUSTOM STATIC GUIDANCE LINE"]
    (tmp_path / "domain_governance_policy.json").write_text(json.dumps(custom), encoding="utf-8")

    g.apply_governance_policy(load_governance_policy(tmp_path, g._POLICY_DEFAULTS))

    md = g.render_findings_and_recommendations({}, [], None, findings=[])
    assert "CUSTOM STATIC GUIDANCE LINE" in md


def test_overriding_client_onboarding_threshold_changes_profile_text(tmp_path):
    custom = json.loads(json.dumps(g._POLICY_DEFAULTS["client_onboarding"]))
    custom["thresholds"]["wp_stable_min"] = 0.10
    (tmp_path / "client_onboarding_policy.json").write_text(json.dumps(custom), encoding="utf-8")

    g.apply_governance_policy(load_governance_policy(tmp_path, g._POLICY_DEFAULTS))

    profile = g._client_onboarding_profile({"n_files": 20, "xc_mean": None, "wp_mean": 0.20})
    assert "Stable internal portfolio" in profile["internal_read"]


def test_overriding_anomaly_threshold_changes_detect_anomalies_text(tmp_path):
    """Mirrors D-021's threshold-override test pattern (D-029): a
    --policy-dir override to anomaly_thresholds.json must be reflected in
    detect_anomalies()'s notable_anomalies text, not just the JSON profile."""
    custom = json.loads(json.dumps(g._POLICY_DEFAULTS["anomaly_thresholds"]))
    custom["thresholds"]["weak_tc_max"] = 0.50
    (tmp_path / "anomaly_thresholds.json").write_text(json.dumps(custom), encoding="utf-8")

    g.apply_governance_policy(load_governance_policy(tmp_path, g._POLICY_DEFAULTS))

    d = {
        "tc": 0.30, "cp": None, "tp": None, "xc": None,
        "wp_p10": None, "wp_p90": None, "wp_all": None,
        "tp_by_scope": {}, "cp_by_scope": {},
        "bundle_schema": "none", "passive_indicator": None, "bundle_share_all": None,
        "wp_disc": {}, "tw": None,
    }
    notes = g.detect_anomalies("some_domain", d)
    assert any("propagate weakly into coordination files" in n for n in notes)

    # Sanity: the same dict under the built-in default threshold (0.20) does
    # NOT flag tc=0.30 as weak.
    g.apply_governance_policy(load_governance_policy(None, g._POLICY_DEFAULTS))
    notes_default = g.detect_anomalies("some_domain", d)
    assert not any("propagate weakly into coordination files" in n for n in notes_default)


# ---------------------------------------------------------------------------
# CLI / main() wiring
# ---------------------------------------------------------------------------

def test_main_default_invocation_uses_shipped_policy_dir_and_stays_complete(tmp_path, monkeypatch):
    """No --policy-dir passed -- must resolve to policies/governance/ and
    report overall_status complete (every profile file found)."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])

    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))

    assert manifest["policy_profiles"]["policy_dir"] == str(DEFAULT_POLICY_DIR)
    assert all(p["source"] == "policy_file" for p in manifest["policy_profiles"]["profiles"].values())
    assert health["overall_status"] == "complete"
    assert "governance_policy_built_in_default" not in health["fallbacks_used"]


def test_main_with_policy_dir_missing_some_files_reports_defaulted_and_degraded(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    policy_dir = tmp_path / "partial_policy"
    policy_dir.mkdir()
    # Only override thresholds; domain_policy/client_onboarding/finding_rules
    # fall back to this generator's built-in defaults.
    (policy_dir / "governance_thresholds.json").write_text(
        json.dumps(g._POLICY_DEFAULTS["thresholds"]), encoding="utf-8"
    )
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--policy-dir", str(policy_dir)])

    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))

    assert manifest["policy_profiles"]["profiles"]["thresholds"]["source"] == "policy_file"
    assert manifest["policy_profiles"]["profiles"]["domain_policy"]["source"] == "built_in_default"
    assert "governance_policy_built_in_default" in health["fallbacks_used"]
    conditions = [w["condition"] for w in health["warnings"]]
    assert "governance_policy_profile_defaulted" in conditions
    assert health["overall_status"] == "degraded"


def test_main_with_nonexistent_policy_dir_does_not_crash_and_uses_all_defaults(tmp_path, monkeypatch):
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    policy_dir = tmp_path / "does_not_exist"
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(tmp_path), "--policy-dir", str(policy_dir)])
    assert (tmp_path / "governance_domain_summary.csv").exists()
    manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
    assert all(p["source"] == "built_in_default" for p in manifest["policy_profiles"]["profiles"].values())


def test_main_output_identical_with_default_and_explicit_shipped_policy_dir(tmp_path, monkeypatch):
    """Passing --policy-dir pointing explicitly at the shipped policies/governance/
    directory must produce byte-identical governance_domain_summary.csv to the
    default (no --policy-dir) invocation -- both resolve to the same profiles."""
    summary_path, pooled_path = _minimal_fixture(tmp_path)
    out_default = tmp_path / "default_run"
    out_explicit = tmp_path / "explicit_run"
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(out_default)])
    _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
                            "--out", str(out_explicit), "--policy-dir", str(DEFAULT_POLICY_DIR)])

    default_csv = (out_default / "governance_domain_summary.csv").read_text(encoding="utf-8")
    explicit_csv = (out_explicit / "governance_domain_summary.csv").read_text(encoding="utf-8")
    assert default_csv == explicit_csv
