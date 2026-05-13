from __future__ import annotations
import csv, subprocess, sys
from pathlib import Path


def _write_csv(path: Path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)


def test_discover_hash_policy_join_and_sig(tmp_path: Path):
    phase0 = tmp_path / 'Results_v21' / 'phase0_v21'
    _write_csv(phase0/'records.csv',["file_id","domain","record_pk","sig_hash"],[
        {"file_id":"f1","domain":"loaded_family_types","record_pk":"1","sig_hash":"s1"},
        {"file_id":"f1","domain":"loaded_family_types","record_pk":"2","sig_hash":"s2"},
    ])
    _write_csv(phase0/'identity_items.csv',["domain","record_pk","item_key","item_value_type","item_value"],[
        {"domain":"loaded_family_types","record_pk":"1","item_key":"shape_gate.category","item_value_type":"str","item_value":"Doors"},
        {"domain":"loaded_family_types","record_pk":"1","item_key":"family.name","item_value_type":"str","item_value":"A"},
        {"domain":"loaded_family_types","record_pk":"2","item_key":"shape_gate.category","item_value_type":"str","item_value":"Windows"},
        {"domain":"loaded_family_types","record_pk":"2","item_key":"family.name","item_value_type":"str","item_value":"B"},
    ])
    subprocess.run([sys.executable,'tools/discover_hash_policy.py','--phase0-dir',str(phase0),'--domains','loaded_family_types','--discovery-target','both'],cwd=Path(__file__).resolve().parents[1],check=True)
    diag = phase0.parent/'diagnostics'
    assert (diag/'hash_sig_discovery_exploration.csv').exists()
    assert (diag/'hash_join_discovery_exploration.csv').exists()
    with (diag/'hash_sig_discovery_exploration.csv').open('r', encoding='utf-8', newline='') as f:
        rows=list(csv.DictReader(f))
    assert rows
    assert all(r['discovery_target']=='sig' for r in rows)
    assert all(r['shape_gate'] in ('Doors','Windows') for r in rows)


def test_validate_marks_blocked_when_required_fields_missing_from_selected(tmp_path: Path):
    phase0 = tmp_path / 'Results_v21' / 'phase0_v21'
    _write_csv(phase0/'records.csv',["file_id","domain","record_pk","sig_hash"],[
        {"file_id":"f1","domain":"text_types","record_pk":"1","sig_hash":"s1"},
    ])
    _write_csv(phase0/'identity_items.csv',["domain","record_pk","item_key","item_value_type","item_value"],[
        {"domain":"text_types","record_pk":"1","item_key":"text_type.font","item_value_type":"str","item_value":"Arial"},
    ])
    policy = tmp_path / "policy.json"
    policy.write_text(
        '{"domains":{"text_types":{"required_items":["text_type.font","text_type.size_in"],"optional_items":[],"explicitly_excluded_items":[]}}}',
        encoding="utf-8",
    )
    subprocess.run([
        sys.executable,'tools/discover_hash_policy.py','--phase0-dir',str(phase0),
        '--domains','text_types','--policy-json',str(policy),'--policy-modes','validate','--search-modes','greedy'
    ],cwd=Path(__file__).resolve().parents[1],check=True)
    with (phase0.parent/'diagnostics'/'hash_sig_discovery_exploration.csv').open('r', encoding='utf-8', newline='') as f:
        rows=list(csv.DictReader(f))
    assert rows
    assert rows[0]["status"] == "blocked_missing_required"


def test_validate_pareto_auto_bumps_max_k_to_required_count(tmp_path: Path):
    phase0 = tmp_path / 'Results_v21' / 'phase0_v21'
    _write_csv(phase0/'records.csv',["file_id","domain","record_pk","sig_hash"],[
        {"file_id":"f1","domain":"text_types","record_pk":"1","sig_hash":"s1"},
    ])
    _write_csv(phase0/'identity_items.csv',["domain","record_pk","item_key","item_value_type","item_value"],[
        {"domain":"text_types","record_pk":"1","item_key":"text_type.font","item_value_type":"str","item_value":"Arial"},
        {"domain":"text_types","record_pk":"1","item_key":"text_type.size_in","item_value_type":"num","item_value":"0.1"},
        {"domain":"text_types","record_pk":"1","item_key":"text_type.bold","item_value_type":"bool","item_value":"FALSE"},
        {"domain":"text_types","record_pk":"1","item_key":"text_type.italic","item_value_type":"bool","item_value":"FALSE"},
        {"domain":"text_types","record_pk":"1","item_key":"text_type.underline","item_value_type":"bool","item_value":"FALSE"},
    ])
    policy = tmp_path / "policy.json"
    policy.write_text(
        '{"domains":{"text_types":{"required_items":["text_type.font","text_type.size_in","text_type.bold","text_type.italic","text_type.underline"],"optional_items":[],"explicitly_excluded_items":[]}}}',
        encoding="utf-8",
    )
    subprocess.run([
        sys.executable,'tools/discover_hash_policy.py','--phase0-dir',str(phase0),
        '--domains','text_types','--policy-json',str(policy),'--policy-modes','validate','--search-modes','pareto','--max-k','2'
    ],cwd=Path(__file__).resolve().parents[1],check=True)
    with (phase0.parent/'diagnostics'/'hash_sig_discovery_exploration.csv').open('r', encoding='utf-8', newline='') as f:
        rows=list(csv.DictReader(f))
    assert rows
    assert rows[0]["status"] == "ok"


def test_phase0_dir_can_be_results_root(tmp_path: Path):
    results_root = tmp_path / "Results_v21"
    phase0 = results_root / "phase0_v21"
    _write_csv(phase0/'records.csv',["file_id","domain","record_pk","sig_hash"],[
        {"file_id":"f1","domain":"loaded_family_types","record_pk":"1","sig_hash":"s1"},
    ])
    _write_csv(phase0/'identity_items.csv',["domain","record_pk","item_key","item_value_type","item_value"],[
        {"domain":"loaded_family_types","record_pk":"1","item_key":"shape_gate.category","item_value_type":"str","item_value":"Doors"},
    ])
    subprocess.run([
        sys.executable,'tools/discover_hash_policy.py','--phase0-dir',str(results_root),
        '--domains','loaded_family_types','--discovery-target','sig'
    ],cwd=Path(__file__).resolve().parents[1],check=True)
    assert (results_root/'diagnostics'/'hash_sig_discovery_exploration.csv').exists()


def test_phase0_dir_auto_resolves_results_records(tmp_path: Path):
    repo_like_root = tmp_path / "workspace"
    phase0 = repo_like_root / "results" / "records"
    _write_csv(phase0/'records.csv',["file_id","domain","record_pk","sig_hash"],[
        {"file_id":"f1","domain":"loaded_family_types","record_pk":"1","sig_hash":"s1"},
    ])
    _write_csv(phase0/'identity_items.csv',["domain","record_pk","item_key","item_value_type","item_value"],[
        {"domain":"loaded_family_types","record_pk":"1","item_key":"shape_gate.category","item_value_type":"str","item_value":"Doors"},
    ])
    subprocess.run([
        sys.executable,'tools/discover_hash_policy.py','--phase0-dir',str(repo_like_root),
        '--domains','loaded_family_types','--discovery-target','sig'
    ],cwd=Path(__file__).resolve().parents[1],check=True)
    assert (repo_like_root/'results'/'diagnostics'/'hash_sig_discovery_exploration.csv').exists()
