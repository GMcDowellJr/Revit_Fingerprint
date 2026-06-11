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



def test_discover_vfd_edges_filters_hint_comments_and_exact_bip_lookup(tmp_path):
    items_dir = tmp_path / "items"
    out_dir = tmp_path / "out"
    items_dir.mkdir()
    (items_dir / "view_filter_definitions.csv").write_text(
        "export_run_id,record_pk,item_key,item_value,item_value_type\n"
        'f1,r1,vf.categories,"[-2000011, ""-2000032""]",ok\n'
        "f1,r1,vf.rule[001].param_ref.kind,builtin,ok\n"
        "f1,r1,vf.rule[001].param_ref.id,bip:-1002107,ok\n",
        encoding="utf-8",
    )
    bip_lookup = tmp_path / "bip_lookup.json"
    bip_lookup.write_text(json.dumps({"bip:-1002107": "MATERIAL_ID_PARAM"}), encoding="utf-8")
    hints = tmp_path / "hints.json"
    hints.write_text(
        json.dumps({
            "exact_bip_id": {
                "_comment_materials": "documentation only",
                "bip:-1002107": {"target_domain": "materials"},
            },
            "name_contains": [
                {"_comment_classification": "documentation only"},
                {"substring": "MATERIAL", "target_domain": "wrong_domain"},
            ],
        }),
        encoding="utf-8",
    )

    subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--identity-items-dir",
            str(items_dir),
            "--bip-lookup",
            str(bip_lookup),
            "--bip-hints",
            str(hints),
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
    assert len(inventory) == 1
    assert inventory[0]["target_domain"] == "materials"
    assert inventory[0]["target_domain_source"] == "exact_bip_id"
    assert inventory[0]["category_names"] == "Floors|Walls"
    assert inventory[0]["unrecognized_category_ids"] == ""

    edges = read_csv(out_dir / "vfd_dynamic_edges.csv")
    assert edges
    assert all(edge["target_domain"] == "materials" for edge in edges)
    assert all(edge["requires_human_review"] == "false" for edge in edges)
    assert {edge["category_id"] for edge in edges} == {"-2000032", "-2000011"}

def test_generated_dynamic_edges_include_category_id_for_reference_graph(tmp_path):
    import importlib.util

    items_dir = tmp_path / "items"
    out_dir = tmp_path / "out"
    items_dir.mkdir()
    (items_dir / "view_filter_definitions.csv").write_text(
        "export_run_id,record_pk,item_key,item_value,item_value_type\n"
        "f1,r1,vf.categories,\"[-2000011,-2000032]\",ok\n"
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

    build_spec = importlib.util.spec_from_file_location(
        "build_cross_domain_items", Path("tools/archetype/build_cross_domain_items.py")
    )
    build_module = importlib.util.module_from_spec(build_spec)
    sys.path.insert(0, str(Path("tools/archetype").resolve()))
    try:
        build_spec.loader.exec_module(build_module)
    finally:
        sys.path.pop(0)

    assert build_module._parse_vf_categories('["-2000011"]') == {"-2000011"}

    dynamic_rows = build_module._build_dynamic_rows(
        ref_edges[0],
        items_dir,
        {},
        {("f1", "view_filter_definitions", "r1"): "source-join-hash"},
    )

    assert len(dynamic_rows) == 1
    assert dynamic_rows[0]["source_join_hash"] == "source-join-hash"


def test_discover_vfd_edges_keeps_same_name_param_categories_separate(tmp_path):
    items_dir = tmp_path / "items"
    out_dir = tmp_path / "out"
    items_dir.mkdir()
    guid_a = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    guid_b = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    (items_dir / "view_filter_definitions.csv").write_text(
        "export_run_id,record_pk,item_key,item_value,item_value_type\n"
        f"f1,r1,vf.categories,-2000011,ok\n"
        f"f1,r1,vf.rule[001].param_ref.kind,shared,ok\n"
        f"f1,r1,vf.rule[001].param_ref.id,{guid_a},ok\n"
        f"f2,r2,vf.categories,-2000032,ok\n"
        f"f2,r2,vf.rule[001].param_ref.kind,shared,ok\n"
        f"f2,r2,vf.rule[001].param_ref.id,{guid_b},ok\n",
        encoding="utf-8",
    )
    bip_lookup = tmp_path / "bip_lookup.json"
    bip_lookup.write_text("{}", encoding="utf-8")
    shared_names = tmp_path / "shared.json"
    shared_names.write_text(json.dumps({guid_a: "Shared Material", guid_b: "Shared Material"}), encoding="utf-8")

    subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--identity-items-dir",
            str(items_dir),
            "--bip-lookup",
            str(bip_lookup),
            "--shared-param-names",
            str(shared_names),
            "--support-min-files",
            "1",
            "--out-dir",
            str(out_dir),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    edges = read_csv(out_dir / "vfd_dynamic_edges.csv")
    assert {(edge["param_id"], edge["category_id"]) for edge in edges} == {
        (guid_a, "-2000011"),
        (guid_b, "-2000032"),
    }
    for edge in edges:
        scope = json.loads(edge["scope_conditions"])
        assert scope["param_ids"] == [edge["param_id"]]
        assert scope["category_ids"] == [int(edge["category_id"])]


def test_discover_vfd_edges_applies_threshold_after_category_aggregation(tmp_path):
    items_dir = tmp_path / "items"
    out_dir = tmp_path / "out"
    items_dir.mkdir()
    (items_dir / "view_filter_definitions.csv").write_text(
        "export_run_id,record_pk,item_key,item_value,item_value_type\n"
        "f1,r1,vf.categories,-2000011,ok\n"
        "f1,r1,vf.rule[001].param_ref.kind,builtin,ok\n"
        "f1,r1,vf.rule[001].param_ref.id,bip:-1005500,ok\n"
        "f2,r2,vf.categories,\"-2000011,-2000032\",ok\n"
        "f2,r2,vf.rule[001].param_ref.kind,builtin,ok\n"
        "f2,r2,vf.rule[001].param_ref.id,bip:-1005500,ok\n",
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
            "2",
            "--out-dir",
            str(out_dir),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    inventory = read_csv(out_dir / "vfd_param_inventory.csv")
    assert {row["meets_threshold"] for row in inventory} == {"false"}

    edges = read_csv(out_dir / "vfd_dynamic_edges.csv")
    assert len(edges) == 1
    assert edges[0]["category_id"] == "-2000011"
    assert edges[0]["file_count"] == "2"
    assert json.loads(edges[0]["scope_conditions"])["category_ids"] == [-2000011]


def test_discover_vfd_edges_skips_edges_without_category_scope(tmp_path):
    items_dir = tmp_path / "items"
    out_dir = tmp_path / "out"
    items_dir.mkdir()
    (items_dir / "view_filter_definitions.csv").write_text(
        "export_run_id,record_pk,item_key,item_value,item_value_type\n"
        "f1,r1,vf.categories,,ok\n"
        "f1,r1,vf.rule[001].param_ref.kind,builtin,ok\n"
        "f1,r1,vf.rule[001].param_ref.id,bip:-1005500,ok\n"
        "f2,r2,vf.categories,not-a-category-list,ok\n"
        "f2,r2,vf.rule[001].param_ref.kind,builtin,ok\n"
        "f2,r2,vf.rule[001].param_ref.id,bip:-1005500,ok\n",
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
    assert inventory
    assert all(row["category_set"] == "" for row in inventory)
    assert read_csv(out_dir / "vfd_dynamic_edges.csv") == []


def test_discover_vfd_edges_ignores_unusable_category_rows(tmp_path):
    items_dir = tmp_path / "items"
    out_dir = tmp_path / "out"
    items_dir.mkdir()
    (items_dir / "view_filter_definitions.csv").write_text(
        "export_run_id,record_pk,item_key,item_value,item_value_type\n"
        "f1,r1,vf.categories,-2000011,ok\n"
        "f1,r1,vf.categories,-2000032,unreadable\n"
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

    inventory = read_csv(out_dir / "vfd_param_inventory.csv")
    assert len(inventory) == 1
    assert inventory[0]["category_set"] == "-2000011"
    assert inventory[0]["category_names"] == "Walls"

    edges = read_csv(out_dir / "vfd_dynamic_edges.csv")
    assert len(edges) == 1
    assert edges[0]["category_id"] == "-2000011"


def test_discover_vfd_edges_ignores_unusable_param_ref_rows_with_item_quality(tmp_path):
    items_dir = tmp_path / "items"
    out_dir = tmp_path / "out"
    items_dir.mkdir()
    (items_dir / "view_filter_definitions.csv").write_text(
        "export_run_id,record_pk,item_key,item_value,item_quality\n"
        "f1,r1,vf.categories,-2000011,ok\n"
        "f1,r1,vf.rule[001].param_ref.kind,builtin,ok\n"
        "f1,r1,vf.rule[001].param_ref.id,bip:-1005500,unreadable\n"
        "f2,r2,vf.categories,-2000011,ok\n"
        "f2,r2,vf.rule[001].param_ref.kind,builtin,ok\n"
        "f2,r2,vf.rule[001].param_ref.id,bip:-1005500,ok\n",
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
            "2",
            "--out-dir",
            str(out_dir),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    inventory = read_csv(out_dir / "vfd_param_inventory.csv")
    assert len(inventory) == 1
    assert inventory[0]["file_count"] == "1"
    assert inventory[0]["meets_threshold"] == "false"
    assert read_csv(out_dir / "vfd_dynamic_edges.csv") == []
