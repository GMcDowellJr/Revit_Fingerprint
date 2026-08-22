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
    # One top-level directory per file forces one catalog per file. A
    # small --routing-index-max-chars can no longer truncate the catalog
    # *list* -- every catalog's bare entry must still appear -- so what
    # gets dropped first is the richer per-catalog summary (roles/sample
    # paths), and if even the bare listing doesn't fit, the index runs
    # over budget with an explicit note rather than omitting any catalog.
    files = {f"dir_{i:02d}/mod.py": f"def f_{i}():\n    return {i}\n" for i in range(40)}
    write_files(repo, files)

    _scan(repo, out)  # default budget: everything fits with full summaries
    full_index = (out / "routing" / "index.md").read_text(encoding="utf-8")
    assert "exceeds --routing-index-max-chars" not in full_index
    assert full_index.count("### `routing/") == 40
    assert "Roles present" in full_index

    _scan(repo, out, ["--routing-index-max-chars", "500", "--force"])
    small_index = (out / "routing" / "index.md").read_text(encoding="utf-8")
    # Regression: every catalog's name must still be discoverable, even
    # though the index itself now runs well over the configured limit.
    assert "omitted from this index" not in small_index
    assert small_index.count("### `routing/") == 40
    assert "Roles present" not in small_index  # richer summary dropped first
    assert "exceeds --routing-index-max-chars" in small_index
    assert len(small_index) > 500  # genuinely over budget, not silently truncated


def test_routing_catalog_respects_max_catalog_chars(repo, out):
    files = {f"core/mod_{i}.py": f'"""Module {i} docstring."""\n\ndef f_{i}():\n    return {i}\n' for i in range(30)}
    write_files(repo, files)
    _scan(repo, out, ["--routing-max-catalog-chars", "2000", "--routing-max-files-per-catalog", "1000"])
    cat_text = (out / "routing" / "core.md").read_text(encoding="utf-8")
    assert len(cat_text) < 2000 + 2000  # header + at least one entry can slightly exceed; omission note caps the rest
    assert "Omitted from this catalog" in cat_text


def test_omitted_path_appendix_is_bounded_regardless_of_omitted_count(repo, out):
    # Regression: the "Omitted from this catalog" appendix listed every
    # single omitted path with no cap of its own -- a partition with
    # hundreds of long filenames could make that appendix alone exceed
    # --routing-max-catalog-chars, defeating the exact limit it exists to
    # respect.
    files = {
        f"core/{'x' * 80}_module_number_{i:04d}.py": f'"""Docstring {i}."""\n\ndef f_{i}():\n    return {i}\n'
        for i in range(300)
    }
    write_files(repo, files)
    _scan(repo, out, ["--routing-max-catalog-chars", "100", "--routing-max-files-per-catalog", "1000"])
    cat_text = (out / "routing" / "core.md").read_text(encoding="utf-8")
    assert "Omitted from this catalog" in cat_text
    assert "more (not listed here" in cat_text
    assert len(cat_text) < 6000


def test_manifest_stores_the_complete_omitted_path_list(repo, out):
    # Regression: once the per-catalog appendix was bounded to a 30-path
    # sample, it started directing readers to routing_manifest.json for
    # "the complete list" -- but the manifest only ever stored
    # omitted_file_count and a 3-path sample, so a machine consumer had no
    # way to actually get the rest.
    files = {
        f"core/{'x' * 80}_module_number_{i:04d}.py": f'"""Docstring {i}."""\n\ndef f_{i}():\n    return {i}\n'
        for i in range(300)
    }
    write_files(repo, files)
    _scan(repo, out, ["--routing-max-catalog-chars", "100", "--routing-max-files-per-catalog", "1000"])
    manifest = _manifest(out)
    core_cat = next(c for c in manifest["catalogs"] if c["key"] == "core")
    assert core_cat["omitted_file_count"] > 30
    assert len(core_cat["omitted_paths"]) == core_cat["omitted_file_count"]
    assert all(p.startswith("core/") for p in core_cat["omitted_paths"])


def test_first_catalog_entry_is_also_capped_by_max_catalog_chars(repo, out):
    # Regression: the size check `body_len + len(block) > max_catalog_chars`
    # was skipped whenever `blocks` was still empty, so the very first file
    # entry in a catalog was always included in full (all its sections --
    # purpose clues, important symbols, dependencies, etc.) regardless of
    # size. Measure the real header + full-first-entry costs from a
    # generous run, then pick a limit that fits the header plus only a
    # minimal path+role stub -- the first entry must fall back to that
    # stub (or be omitted), never render its full detail unconditionally.
    files = {
        f"core/{'x' * 80}_module_number_{i:04d}.py": f'"""Docstring {i}."""\n\ndef f_{i}():\n    return {i}\n'
        for i in range(5)
    }
    write_files(repo, files)
    _scan(repo, out, ["--routing-max-files-per-catalog", "1000"])  # generous default max_catalog_chars
    full_text = (out / "routing" / "core.md").read_text(encoding="utf-8")
    header_text, _, rest = full_text.partition("### `")
    first_block = "### `" + rest.split("### `", 1)[0]
    role_line = next(l for l in first_block.splitlines() if l.startswith("- Role: "))
    first_path = first_block.splitlines()[0][len("### `"):-1]
    minimal_stub = f"### `{first_path}`\n{role_line}\n"
    assert len(first_block) > len(minimal_stub) + 20  # the full entry is genuinely bigger than the stub

    tight_limit = len(header_text) + len(minimal_stub) + 5
    _scan(repo, out, ["--routing-max-catalog-chars", str(tight_limit),
                       "--routing-max-files-per-catalog", "1000", "--force"])
    cat_text = (out / "routing" / "core.md").read_text(encoding="utf-8")
    assert cat_text.count("### `") == 1  # only the (stubbed) first entry survives in detail
    assert "Purpose clues" not in cat_text  # full-detail sections did not sneak in for the first entry
    assert f"### `{first_path}`" in cat_text
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


def test_no_routing_removes_stale_routing_from_earlier_scan(repo, out):
    # Regression: rerunning `scan --no-routing` against an output
    # directory that already has routing/ from an earlier (routing-
    # enabled) run must not leave the stale catalogs in place -- they'd
    # silently describe an out-of-date source tree.
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    _scan(repo, out)
    assert (out / "routing" / "index.md").exists()

    _scan(repo, out, ["--no-routing", "--force"])
    assert not (out / "routing").exists()


def test_catalog_filenames_do_not_collide_across_partition_keys(repo, out):
    # Regression: a naive "/" -> "_" filename substitution can make a
    # nested partition key ("a/b") collide with an unrelated top-level
    # partition ("a_b") -- both naively become "a_b.md", so one
    # catalog's content silently overwrote the other's file on disk even
    # though the routing manifest still listed both.
    write_files(repo, {
        "a/b/x1.py": "def x1():\n    return 1\n",
        "a/b/x2.py": "def x2():\n    return 2\n",
        "a/b/x3.py": "def x3():\n    return 3\n",
        "a_b/y1.py": "def y1():\n    return 4\n",
    })
    _scan(repo, out, ["--routing-max-files-per-catalog", "2"])
    manifest = _manifest(out)

    keys = [c["key"] for c in manifest["catalogs"]]
    assert "a/b" in keys
    assert "a_b" in keys

    paths = [c["path"] for c in manifest["catalogs"]]
    assert len(paths) == len(set(paths)), f"duplicate catalog filenames: {paths}"

    a_b_nested = next(c for c in manifest["catalogs"] if c["key"] == "a/b")
    a_b_top = next(c for c in manifest["catalogs"] if c["key"] == "a_b")
    assert a_b_nested["path"] != a_b_top["path"]

    nested_text = (out / a_b_nested["path"]).read_text(encoding="utf-8")
    top_text = (out / a_b_top["path"]).read_text(encoding="utf-8")
    assert "a/b/x1.py" in nested_text
    assert "a_b/y1.py" in top_text
    assert "a_b/y1.py" not in nested_text
    assert "a/b/x1.py" not in top_text


def test_purpose_clues_are_traceable_to_deterministic_evidence(repo, out):
    write_files(repo, {"core/thing.py": '"""Widget factory helpers."""\n\ndef make_widget():\n    return 1\n'})
    _scan(repo, out)
    cat_text = (out / "routing" / "core.md").read_text(encoding="utf-8")
    assert "module docstring: Widget factory helpers." in cat_text
    assert "filename/path terms: thing" in cat_text
