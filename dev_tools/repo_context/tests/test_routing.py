import json

from conftest import run_tool, write_files


def _scan(repo, out, extra=None):
    result = run_tool(["scan", str(repo), "--output", str(out)] + (extra or []))
    assert result.returncode == 0, result.stderr
    return result


def _manifest(out):
    return json.loads((out / "routing" / "routing_manifest.json").read_text(encoding="utf-8"))


def test_routing_catalogs_are_generated_and_deterministic(repo, out):
    write_files(repo, {
        "core/helper.py": "def add(a, b):\n    return a + b\n",
        "tools/report.py": "from core.helper import add\n\ndef build():\n    return add(1, 2)\n",
        "tests/test_helper.py": "from core.helper import add\n\ndef test_add():\n    assert add(1, 2) == 3\n",
    })
    _scan(repo, out)
    manifest_1 = (out / "routing" / "routing_manifest.json").read_text(encoding="utf-8")
    index_1 = (out / "routing" / "index.md").read_text(encoding="utf-8")

    _scan(repo, out, ["--force"])
    manifest_2 = (out / "routing" / "routing_manifest.json").read_text(encoding="utf-8")
    index_2 = (out / "routing" / "index.md").read_text(encoding="utf-8")

    # Timestamps differ between runs; strip them before comparing.
    def strip_time(text):
        return "\n".join(l for l in text.splitlines() if "generated_at_utc" not in l and "Generated (UTC)" not in l)

    assert strip_time(manifest_1) == strip_time(manifest_2)
    assert strip_time(index_1) == strip_time(index_2)


def test_every_included_file_has_exactly_one_primary_catalog(repo, out):
    write_files(repo, {
        "core/a.py": "def f():\n    return 1\n",
        "tools/sub/b.py": "def g():\n    return 2\n",
        "tests/test_a.py": "def test_f():\n    assert True\n",
        "loose_root_script.py": "print('hi')\n",
    })
    _scan(repo, out)
    manifest = _manifest(out)

    seen_paths = {}
    for cat in manifest["catalogs"]:
        cat_text = (out / cat["path"]).read_text(encoding="utf-8")
        for line in cat_text.splitlines():
            if line.startswith("### `") and line.endswith("`"):
                p = line[len("### `"):-1]
                seen_paths.setdefault(p, []).append(cat["key"])

    for p, keys in seen_paths.items():
        assert len(keys) == 1, f"{p} appeared in more than one catalog: {keys}"

    # Every included file from file_inventory.csv appears in exactly one catalog.
    import csv
    with open(out / "file_inventory.csv", encoding="utf-8", newline="") as fh:
        included = [r["relative_path"] for r in csv.DictReader(fh) if r["included"] == "true"]
    for p in included:
        assert p in seen_paths, f"{p} missing from every routing catalog"


def test_test_and_archived_files_get_dedicated_catalogs(repo, out):
    write_files(repo, {
        "core/a.py": "def f():\n    return 1\n",
        "tests/test_a.py": "def test_f():\n    assert True\n",
        "tools/_archive/old.py": "def legacy():\n    return 0\n",
    })
    _scan(repo, out)
    manifest = _manifest(out)
    keys = {c["key"] for c in manifest["catalogs"]}
    assert "tests" in keys
    assert "archived" in keys
    assert "core" in keys

    tests_cat = next(c for c in manifest["catalogs"] if c["key"] == "tests")
    assert tests_cat["roles"] == {"test_harness": 1}
    archived_cat = next(c for c in manifest["catalogs"] if c["key"] == "archived")
    assert archived_cat["roles"] == {"archived_or_legacy": 1}


def test_catalog_line_anchors_match_python_symbols_csv(repo, out):
    write_files(repo, {
        "core/a.py": "def f():\n    return 1\n\n\ndef g():\n    return 2\n",
    })
    _scan(repo, out)
    import csv
    with open(out / "python_symbols.csv", encoding="utf-8", newline="") as fh:
        rows = {r["qualified_name"]: r for r in csv.DictReader(fh) if r["relative_path"] == "core/a.py"}
    cat_text = (out / "routing" / "core.md").read_text(encoding="utf-8")
    assert f"`f` (function) — line {rows['f']['start_line']}" in cat_text
    assert f"`g` (function) — line {rows['g']['start_line']}" in cat_text


def test_routing_catalog_splits_oversized_directory_by_subdirectory(repo, out):
    # No deeper structure to split on -- the splitter must accept the
    # oversized catalog rather than lose or guess at files.
    flat_files = {f"flatdir/mod_{i}.py": f"def f_{i}():\n    return {i}\n" for i in range(10)}
    # Real subdirectories to split on.
    nested_files = {f"nested/group{i % 4}/mod_{i}.py": f"def f_{i}():\n    return {i}\n" for i in range(12)}
    write_files(repo, {**flat_files, **nested_files})
    _scan(repo, out, ["--routing-max-files-per-catalog", "3"])
    manifest = _manifest(out)

    flat_cat = next(c for c in manifest["catalogs"] if c["key"] == "flatdir")
    assert flat_cat["file_count"] == 10

    nested_keys = [c["key"] for c in manifest["catalogs"] if c["key"].startswith("nested/group")]
    assert len(nested_keys) == 4
    for c in manifest["catalogs"]:
        if c["key"].startswith("nested/group"):
            assert c["file_count"] <= 3


def test_routing_index_respects_max_chars(repo, out):
    # One top-level directory per file forces one catalog per file, so the
    # index's *catalog list* (not any single catalog's contents) is what
    # has to be truncated to fit a small --routing-index-max-chars.
    files = {f"dir_{i:02d}/mod.py": f"def f_{i}():\n    return {i}\n" for i in range(40)}
    write_files(repo, files)

    _scan(repo, out)  # default budget: everything fits
    full_index = (out / "routing" / "index.md").read_text(encoding="utf-8")
    assert "omitted from this index" not in full_index
    assert full_index.count("### `routing/") == 40

    _scan(repo, out, ["--routing-index-max-chars", "2200", "--force"])
    small_index = (out / "routing" / "index.md").read_text(encoding="utf-8")
    assert "omitted from this index" in small_index
    assert small_index.count("### `routing/") < 40
    assert len(small_index) <= 2200 + 400  # small slack for the overflow note itself


def test_routing_catalog_respects_max_catalog_chars(repo, out):
    files = {f"core/mod_{i}.py": f'"""Module {i} docstring."""\n\ndef f_{i}():\n    return {i}\n' for i in range(30)}
    write_files(repo, files)
    _scan(repo, out, ["--routing-max-catalog-chars", "2000", "--routing-max-files-per-catalog", "1000"])
    cat_text = (out / "routing" / "core.md").read_text(encoding="utf-8")
    assert len(cat_text) < 2000 + 2000  # header + at least one entry can slightly exceed; omission note caps the rest
    assert "Omitted from this catalog" in cat_text


def test_changed_source_changes_source_manifest_hash(repo, out):
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    _scan(repo, out)
    manifest_1 = _manifest(out)
    write_files(repo, {"core/a.py": "def f():\n    return 2\n"})
    _scan(repo, out, ["--force"])
    manifest_2 = _manifest(out)
    assert manifest_1["source_manifest_hash"] != manifest_2["source_manifest_hash"]


def test_routing_disabled_with_no_routing_flag(repo, out):
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    _scan(repo, out, ["--no-routing"])
    assert not (out / "routing").exists()


def test_purpose_clues_are_traceable_to_deterministic_evidence(repo, out):
    write_files(repo, {"core/thing.py": '"""Widget factory helpers."""\n\ndef make_widget():\n    return 1\n'})
    _scan(repo, out)
    cat_text = (out / "routing" / "core.md").read_text(encoding="utf-8")
    assert "module docstring: Widget factory helpers." in cat_text
    assert "filename/path terms: thing" in cat_text
