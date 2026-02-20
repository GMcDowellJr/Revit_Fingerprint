import csv
import json
import subprocess
import sys
from pathlib import Path

from tools.join_key_discovery.eval import build_candidate_join_key_with_details, normalize_policy_block


def _write_csv(path: Path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def test_flat_required_fields_backward_compatible():
    identity = {
        "r1": {
            "a": ("str", "1"),
            "b": ("str", "2"),
        }
    }
    status, _items, _reason, details = build_candidate_join_key_with_details(
        identity,
        "r1",
        ["a", "b"],
        {"required_fields": ["a", "b"]},
    )
    assert status == "ok"
    assert details["effective_required_fields"] == ["a", "b"]


def test_required_items_alias_and_shape_gating():
    policy = {
        "selected_fields": ["dim_type.shape", "dim_type.type_name"],
        "required_items": ["dim_type.shape", "dim_type.type_name"],
        "shape_gating": {
            "discriminator_key": "dim_type.shape",
            "shape_requirements": {
                "Radial": {"additional_required": ["dim_type.center_mark_size"], "additional_optional": []}
            },
            "default_shape_behavior": "common_only",
        },
    }
    norm = normalize_policy_block(policy)
    assert norm["required_fields"] == ["dim_type.shape", "dim_type.type_name"]
    assert norm["gates"]["discriminator_key"] == "dim_type.shape"

    identity = {
        "linear": {
            "dim_type.shape": ("str", "Linear"),
            "dim_type.type_name": ("str", "A"),
        },
        "radial": {
            "dim_type.shape": ("str", "Radial"),
            "dim_type.type_name": ("str", "B"),
        },
    }

    status_linear, _items, _reason, details_linear = build_candidate_join_key_with_details(identity, "linear", norm["selected_fields"], {"required_fields": norm["required_fields"], **norm["gates"]})
    assert status_linear == "ok"
    assert details_linear["discriminator_value"] == "Linear"

    status_radial, _items, reason_radial, details_radial = build_candidate_join_key_with_details(identity, "radial", norm["selected_fields"], {"required_fields": norm["required_fields"], **norm["gates"]})
    assert status_radial == "missing_required"
    assert reason_radial == "dim_type.center_mark_size"
    assert details_radial["discriminator_value"] == "Radial"


def test_apply_diagnostics_include_discriminator_context(tmp_path: Path):
    phase0_dir = tmp_path / "Results_v21" / "phase0_v21"
    records_path = phase0_dir / "phase0_records.csv"
    items_path = phase0_dir / "phase0_identity_items.csv"
    policy_path = tmp_path / "policy.json"

    _write_csv(
        records_path,
        ["export_run_id", "file_id", "domain", "record_pk", "join_hash", "join_key_schema"],
        [
            {"export_run_id": "run", "file_id": "f", "domain": "dimension_types", "record_pk": "linear", "join_hash": "", "join_key_schema": ""},
            {"export_run_id": "run", "file_id": "f", "domain": "dimension_types", "record_pk": "radial", "join_hash": "", "join_key_schema": ""},
        ],
    )
    _write_csv(
        items_path,
        ["domain", "record_pk", "item_key", "item_value_type", "item_value"],
        [
            {"domain": "dimension_types", "record_pk": "linear", "item_key": "dim_type.shape", "item_value_type": "str", "item_value": "Linear"},
            {"domain": "dimension_types", "record_pk": "linear", "item_key": "dim_type.type_name", "item_value_type": "str", "item_value": "L1"},
            {"domain": "dimension_types", "record_pk": "radial", "item_key": "dim_type.shape", "item_value_type": "str", "item_value": "Radial"},
            {"domain": "dimension_types", "record_pk": "radial", "item_key": "dim_type.type_name", "item_value_type": "str", "item_value": "R1"},
        ],
    )

    policy = {
        "domains": {
            "dimension_types": {
                "policy_id": "dimension_types.join_key.v21",
                "policy_version": "1",
                "selected_fields": ["dim_type.shape", "dim_type.type_name"],
                "required_items": ["dim_type.shape", "dim_type.type_name"],
                "shape_gating": {
                    "discriminator_key": "dim_type.shape",
                    "shape_requirements": {
                        "Radial": {"additional_required": ["dim_type.center_mark_size"], "additional_optional": []}
                    },
                    "default_shape_behavior": "common_only",
                },
            }
        }
    }
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    subprocess.run(
        [sys.executable, "tools/v21_apply_join_policy.py", "--phase0-dir", str(phase0_dir), "--join-policy", str(policy_path)],
        check=True,
        cwd=Path(__file__).resolve().parents[1],
    )

    with records_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    linear = next(r for r in rows if r["record_pk"] == "linear")
    radial = next(r for r in rows if r["record_pk"] == "radial")
    assert linear["join_key_status"] == "ok"
    assert radial["join_key_status"] == "missing_required"

    failures_path = phase0_dir.parent / "diagnostics" / "join_policy_failures.csv"
    with failures_path.open("r", encoding="utf-8", newline="") as f:
        failures = list(csv.DictReader(f))
    assert len(failures) == 1
    assert failures[0]["discriminator_key"] == "dim_type.shape"
    assert failures[0]["discriminator_value"] == "Radial"
    assert failures[0]["missing_keys"] == "dim_type.center_mark_size"


def test_optional_items_not_required_or_selected_by_default():
    policy = {
        "required_items": ["a"],
        "optional_items": ["b"],
    }
    norm = normalize_policy_block(policy)
    assert norm["required_fields"] == ["a"]
    assert norm["selected_fields"] == []
    assert norm["optional_items"] == ["b"]


def test_discover_emits_legacy_compat_shape_and_lists(tmp_path: Path):
    phase0_dir = tmp_path / "Results_v21" / "phase0_v21"
    records_path = phase0_dir / "phase0_records.csv"
    items_path = phase0_dir / "phase0_identity_items.csv"
    base_policy_path = tmp_path / "base_policy.json"
    out_policy_path = tmp_path / "out_policy.json"

    _write_csv(
        records_path,
        ["file_id", "domain", "record_pk", "sig_hash"],
        [{"file_id": "f", "domain": "dimension_types", "record_pk": "1", "sig_hash": "s1"}],
    )
    _write_csv(
        items_path,
        ["domain", "record_pk", "item_key", "item_value_type", "item_value"],
        [
            {"domain": "dimension_types", "record_pk": "1", "item_key": "dim_type.shape", "item_value_type": "str", "item_value": "Linear"},
            {"domain": "dimension_types", "record_pk": "1", "item_key": "dim_type.type_name", "item_value_type": "str", "item_value": "L1"},
        ],
    )

    base = {
        "domains": {
            "dimension_types": {
                "optional_items": ["dim_type.center_mark_size"],
                "explicitly_excluded_items": ["dim_type.name"],
                "shape_gating": {
                    "discriminator_key": "dim_type.shape",
                    "shape_requirements": {"Radial": {"additional_required": ["dim_type.center_mark_size"], "additional_optional": []}},
                    "default_shape_behavior": "common_only",
                },
            }
        }
    }
    base_policy_path.write_text(json.dumps(base), encoding="utf-8")

    subprocess.run(
        [
            sys.executable,
            "tools/v21_discover_join_policy.py",
            "--phase0-dir",
            str(phase0_dir),
            "--out-policy",
            str(out_policy_path),
            "--domains",
            "dimension_types",
            "--base-policy",
            str(base_policy_path),
            "--warn-only",
        ],
        check=True,
        cwd=Path(__file__).resolve().parents[1],
    )

    diagnostics_dir = phase0_dir.parent / "diagnostics"
    assert (diagnostics_dir / "join_key_discovery_exploration.csv").exists()
    assert (diagnostics_dir / "join_key_discover.csv").exists()
    assert (diagnostics_dir / "join_key_validate.csv").exists()
    assert (diagnostics_dir / "join_key_harsh.csv").exists()

    out = json.loads(out_policy_path.read_text(encoding="utf-8"))
    dom = out["domains"]["dimension_types"]
    assert dom["required_items"] == dom["required_fields"]
    assert dom["optional_items"] == ["dim_type.center_mark_size"]
    assert dom["explicitly_excluded_items"] == ["dim_type.name"]
    assert dom["shape_gating"]["discriminator_key"] == "dim_type.shape"
    assert dom["gates"]["discriminator_key"] == "dim_type.shape"
