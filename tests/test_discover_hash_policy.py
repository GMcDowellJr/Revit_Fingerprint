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
