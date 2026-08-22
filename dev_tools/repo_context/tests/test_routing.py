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


def test_catalog_appendix_respects_the_cap_when_header_alone_is_close_to_it(repo, out):
    # Regression: the header and the "Omitted from this catalog" appendix
    # were both appended unconditionally with no size accounting against
    # --routing-max-catalog-chars at all. Scanning a single file with a
    # tiny cap (100) previously still produced ~700+ characters of output
    # once the fixed-framing header and the appendix's own fixed-framing
    # text were added on top of it. The header is unavoidable fixed
    # framing (revision/hash provenance) that always renders in full, but
    # the appendix is optional annotation and must now respect whatever
    # budget is actually left after the header -- which, for a 100-char
    # cap, is none at all, so no appendix should be appended.
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    _scan(repo, out, ["--routing-max-catalog-chars", "100"])
    cat_text = (out / "routing" / "core.md").read_text(encoding="utf-8")
    assert "Omitted from this catalog" not in cat_text
    assert len(cat_text) < 600  # just the fixed header, not header + an unbounded appendix on top


def test_omitted_path_appendix_is_bounded_regardless_of_omitted_count(repo, out):
    # Regression: the "Omitted from this catalog" appendix listed every
    # single omitted path with no cap of its own -- a partition with
    # hundreds of long filenames could make that appendix alone exceed
    # --routing-max-catalog-chars, defeating the exact limit it exists to
    # respect.
    #
    # 1000 (not an absurdly tiny 100) is deliberately still much smaller
    # than what 300 full detailed entries would need -- everything still
    # gets omitted -- but it's big enough to leave room for the (now
    # budget-respecting) short notice plus a sample, which is the actual
    # behavior this test verifies. A cap smaller than the catalog's own
    # fixed header framing can no longer show any appendix at all -- see
    # test_first_catalog_entry_is_also_capped_by_max_catalog_chars for
    # that boundary case.
    files = {
        f"core/{'x' * 80}_module_number_{i:04d}.py": f'"""Docstring {i}."""\n\ndef f_{i}():\n    return {i}\n'
        for i in range(300)
    }
    write_files(repo, files)
    _scan(repo, out, ["--routing-max-catalog-chars", "1000", "--routing-max-files-per-catalog", "1000"])
    cat_text = (out / "routing" / "core.md").read_text(encoding="utf-8")
    assert "Omitted from this catalog" in cat_text
    assert "more (not listed here" in cat_text
    assert len(cat_text) < 1000 + 500  # small slack for the last sample line/trailer that pushed it over


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
    # By construction there are only 5 spare characters left after the
    # header + stub -- nowhere near enough room for even the appendix's
    # short notice, so none is shown here (rather than being forced in
    # over budget); the 4 omitted files are still fully tracked in the
    # manifest (see test_manifest_stores_the_complete_omitted_path_list).
    assert "Omitted from this catalog" not in cat_text
    manifest = _manifest(out)
    core_cat = next(c for c in manifest["catalogs"] if c["key"] == "core")
    assert core_cat["omitted_file_count"] == 4


def test_non_python_files_get_a_lightweight_table_row_not_a_full_block(repo, out):
    # Cataloging a .md/.json/etc. file used to run it through the full
    # Python-oriented block template (_render_file_entry) anyway -- every
    # section (Role, top-level symbols, internal dependencies, callers,
    # related tests) is a Python-specific fact that simply doesn't exist
    # for a non-Python file, so five or six of the block's ~seven lines
    # were always "(none)"/"unknown" boilerplate, with only the filename-
    # derived "Purpose clues" line carrying any real information. A
    # non-Python file must render as a compact table row instead.
    write_files(repo, {
        "core/a.py": "def f():\n    return 1\n",
        "core/readme.md": "# Widget Factory\n\nBuilds widgets.\n",
        "core/data.json": '{"a": 1}\n',
    })
    _scan(repo, out)
    cat_text = (out / "routing" / "core.md").read_text(encoding="utf-8")
    assert "### `core/a.py`" in cat_text  # Python files still get the full block
    assert "### `core/readme.md`" not in cat_text
    assert "### `core/data.json`" not in cat_text
    assert "## Other files (non-Python)" in cat_text
    assert "| `core/readme.md` | Widget Factory | " in cat_text
    assert "| `core/data.json` |" in cat_text
    assert "Important symbols" not in cat_text.split("## Other files")[1]
    assert "(none resolved" not in cat_text.split("## Other files")[1]


def test_markdown_title_prefers_heading_falls_back_to_first_line_then_filename(repo, out):
    write_files(repo, {
        "docs/with_heading.md": "Some preamble\n\n# The Real Title\n\nBody text.\n",
        "docs/no_heading.md": "Just a plain first line of prose.\n\nMore text.\n",
        "docs/empty.md": "",
    })
    _scan(repo, out)
    cat_text = (out / "routing" / "docs.md").read_text(encoding="utf-8")
    # A heading anywhere in the scanned window wins over an earlier plain line.
    assert "| `docs/with_heading.md` | The Real Title | " in cat_text
    # No heading at all -- falls back to the first non-empty line.
    assert "| `docs/no_heading.md` | Just a plain first line of prose. | " in cat_text
    # Unreadable/empty content -- falls back to filename terms, never blank.
    assert "| `docs/empty.md` | empty | " in cat_text


def test_other_files_table_rows_respect_the_catalog_cap(repo, out):
    # The lightweight table must be bounded by --routing-max-catalog-chars
    # the same way the Python block list is -- omitted rows still land in
    # the manifest's per-catalog omitted list, not silently dropped or
    # left unbounded. (With many small, uniform-size rows like these, the
    # greedy packing loop tends to fill right up to the cap and leave too
    # little slack for the appendix's own short notice to fit too -- that
    # is expected, not a bug; the manifest is the authoritative record
    # regardless of whether the appendix text happens to render.)
    files = {f"docs/note_{i:03d}.md": f"# Note {i}\n\nBody.\n" for i in range(200)}
    write_files(repo, files)
    _scan(repo, out, ["--routing-max-catalog-chars", "2000"])
    cat_text = (out / "routing" / "docs.md").read_text(encoding="utf-8")
    assert len(cat_text) < 2000 + 200
    row_count = cat_text.count("| `docs/note_")
    assert 0 < row_count < 200
    manifest = _manifest(out)
    docs_cat = next(c for c in manifest["catalogs"] if c["key"] == "docs")
    assert docs_cat["omitted_file_count"] == 200 - row_count
    assert len(docs_cat["omitted_paths"]) == docs_cat["omitted_file_count"]


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


def test_scan_survives_a_malformed_graphify_top_level_structure(repo, out):
    # Regression: graphify-out/graph.json containing valid JSON but a
    # non-object top level (e.g. a bare list) crashed `data.get(...)` with
    # AttributeError inside load_graphify_communities(). Since routing
    # loads Graphify during every normal `scan`, that crashed the whole
    # scan (after partially writing its output) over one optional,
    # malformed artifact -- instead of treating it as unavailable
    # evidence, the same as any other unreadable/stale graph.json.
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
    (repo / "graphify-out" / "graph.json").write_text("[]", encoding="utf-8")
    _scan(repo, out)  # must not crash -- _scan() itself asserts returncode == 0
    assert (out / "routing" / "index.md").exists()
