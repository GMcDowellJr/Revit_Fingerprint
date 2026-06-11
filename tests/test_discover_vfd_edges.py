import csv
import json
import subprocess
import sys
from pathlib import Path


SCRIPT = Path("tools/archetype/discover_vfd_edges.py")


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_discover_vfd_edges_resolves_builtin_and_groups_edge(tmp_path):
    items_dir = tmp_path / "items"
    out_dir = tmp_path / "out"
    items_dir.mkdir()
    (items_dir / "view_filter_definitions.csv").write_text(
        "export_run_id,record_pk,item_key,item_value,item_value_type\n"
        "f1,r1,vf.categories,\"-2000011,-2000032\",ok\n"
        "f1,r1,vf.rule[001].param_ref.kind,builtin,ok\n"
        "f1,r1,vf.rule[001].param_ref.id,bip:-1005500,ok\n"
        "f2,r2,vf.categories,[-2000011],ok\n"
        "f2,r2,vf.rule[001].param_ref.kind,builtin,ok\n"
        "f2,r2,vf.rule[001].param_ref.id,bip:-1005500,ok\n",
        encoding="utf-8",
    )
    bip_lookup = tmp_path / "bip_lookup.json"
    bip_lookup.write_text(json.dumps({"bip:-1005500": "STRUCTURAL_MATERIAL_PARAM"}), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--identity-items-dir",
            str(items_dir),
            "--bip-lookup",
            str(bip_lookup),
            "--support-min-files",
            "1",
            "--out-dir",
            str(out_dir),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    assert "VFD Edge Discovery Summary" in result.stdout
    inventory = read_csv(out_dir / "vfd_param_inventory.csv")
    assert any(
        row["param_kind"] == "builtin"
        and row["param_name"] == "STRUCTURAL_MATERIAL_PARAM"
        and row["target_domain"] == "materials"
        for row in inventory
    )

    edges = read_csv(out_dir / "vfd_dynamic_edges.csv")
    assert len(edges) == 2
    assert {edge["category_id"] for edge in edges} == {"-2000032", "-2000011"}
    for edge in edges:
        assert edge["edge_id"] == "vfd.structural_material_param__materials"
        assert edge["name_resolved"] == "true"
        assert int(edge["file_count"]) == 2
        scope = json.loads(edge["scope_conditions"])
        assert scope == {"param_ids": ["bip:-1005500"], "category_ids": [-2000032, -2000011]}


def test_discover_vfd_edges_without_shared_names_keeps_guid_out_of_edges(tmp_path):
    items_dir = tmp_path / "items"
    out_dir = tmp_path / "out"
    items_dir.mkdir()
    (items_dir / "view_filter_definitions_identity_items.csv").write_text(
        "export_run_id,record_pk,item_key,item_value,item_value_type\n"
        "f1,r1,vf.categories,-2000011,ok\n"
        "f1,r1,vf.rule[001].param_ref.kind,shared,ok\n"
        "f1,r1,vf.rule[001].param_ref.id,aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa,ok\n",
        encoding="utf-8",
    )
    bip_lookup = tmp_path / "bip_lookup.json"
    bip_lookup.write_text(json.dumps({"bip:-1005500": "STRUCTURAL_MATERIAL_PARAM"}), encoding="utf-8")

    subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--identity-items-dir",
            str(items_dir),
            "--bip-lookup",
            str(bip_lookup),
            "--support-min-files",
            "1",
            "--out-dir",
            str(out_dir),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    inventory = read_csv(out_dir / "vfd_param_inventory.csv")
    assert inventory[0]["param_kind"] == "shared"
    assert inventory[0]["name_resolved"] == "false"
    assert inventory[0]["param_name"] == ""
    assert read_csv(out_dir / "vfd_dynamic_edges.csv") == []


def test_generated_dynamic_edges_include_category_id_for_reference_graph(tmp_path):
    import importlib.util

    items_dir = tmp_path / "items"
    out_dir = tmp_path / "out"
    items_dir.mkdir()
    (items_dir / "view_filter_definitions.csv").write_text(
        "export_run_id,record_pk,item_key,item_value,item_value_type\n"
        "f1,r1,vf.categories,\"-2000011,-2000032\",ok\n"
        "f1,r1,vf.rule[001].param_ref.kind,builtin,ok\n"
        "f1,r1,vf.rule[001].param_ref.id,bip:-1005500,ok\n",
        encoding="utf-8",
    )
    bip_lookup = tmp_path / "bip_lookup.json"
    bip_lookup.write_text(json.dumps({"bip:-1005500": "STRUCTURAL_MATERIAL_PARAM"}), encoding="utf-8")

    subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--identity-items-dir",
            str(items_dir),
            "--bip-lookup",
            str(bip_lookup),
            "--support-min-files",
            "1",
            "--out-dir",
            str(out_dir),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    spec = importlib.util.spec_from_file_location(
        "generate_reference_graph", Path("tools/archetype/generate_reference_graph.py")
    )
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(Path("tools/archetype").resolve()))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)

    ref_edges = module._build_dynamic_edges(
        out_dir / "vfd_dynamic_edges.csv",
        {"bip:-1005500": "STRUCTURAL_MATERIAL_PARAM"},
        {},
        1,
    )

    assert len(ref_edges) == 1
    assert ref_edges[0]["scope_conditions"]["category_ids"] == ["-2000011", "-2000032"]
