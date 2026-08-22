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
    # A directory with no subdirectories to split by (_partition_files())
    # and more files than fit in one page (max_catalog_chars) spills the
    # overflow into <key>__page2.md, __page3.md, etc. instead of dropping
    # anything -- every file must still be covered by *some* page.
    files = {f"core/mod_{i}.py": f'"""Module {i} docstring."""\n\ndef f_{i}():\n    return {i}\n' for i in range(30)}
    write_files(repo, files)
    _scan(repo, out, ["--routing-max-catalog-chars", "2000", "--routing-max-files-per-catalog", "1000"])
    cat_text = (out / "routing" / "core.md").read_text(encoding="utf-8")
    assert len(cat_text) < 2000 + 200  # page 1 itself still respects the cap
    assert (out / "routing" / "core__page2.md").exists()  # overflow spilled into a further page

    manifest = _manifest(out)
    core_pages = [c for c in manifest["catalogs"] if c["key"] == "core" or c["key"].startswith("core (page")]
    assert len(core_pages) > 1
    assert sum(c["file_count"] for c in core_pages) == 30  # every file lands on some page
    assert sum(c["omitted_file_count"] for c in core_pages) == 0  # nothing was actually dropped


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
    # Pagination (see test_routing_catalog_respects_max_catalog_chars) now
    # handles the ordinary "too many files for one page" case by spilling
    # into further pages -- a *hard* omission (this appendix) only occurs
    # when even a single minimal path+role stub can't fit alongside a
    # page's own header, which no page (however many) could ever fix,
    # since every page has the same budget. Whenever that's true for one
    # file it's true for every structurally-similar file too (a minimal
    # stub's size tracks the path length, same as an appendix sample
    # line's does), so this only ever happens with every file on the page
    # simultaneously -- confirmed by construction below: at a 500-char
    # cap, all 300 of these long-filename files hard-omit on page 1
    # itself, and no further page is even attempted (see
    # _render_catalog_page's docstring for why that's the correct
    # stopping point). The appendix's own 30-path sample cap (still in
    # place for defense-in-depth) isn't exercised by this scenario --
    # there isn't room left for a single sample line either -- but the
    # short notice plus the manifest's full list
    # (test_manifest_stores_the_complete_omitted_path_list) together mean
    # nothing is silently lost even here.
    files = {
        f"core/{'x' * 80}_module_number_{i:04d}.py": f'"""Docstring {i}."""\n\ndef f_{i}():\n    return {i}\n'
        for i in range(300)
    }
    write_files(repo, files)
    _scan(repo, out, ["--routing-max-catalog-chars", "600", "--routing-max-files-per-catalog", "1000"])
    cat_text = (out / "routing" / "core.md").read_text(encoding="utf-8")
    assert "Omitted from this catalog" in cat_text
    assert "300 file(s) omitted" in cat_text
    assert len(cat_text) < 600 + 100  # small slack for the short notice itself

    manifest = _manifest(out)
    core_cat = next(c for c in manifest["catalogs"] if c["key"] == "core")
    assert core_cat["file_count"] == 0  # nothing fit at all -- confirms this is the hard-omission case, not paging
    assert core_cat["omitted_file_count"] == 300
    assert not (out / "routing" / "core__page2.md").exists()  # a further page could never help either


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

    # +50 (not +5) so the margin comfortably covers the few extra
    # characters a later page's own header needs for its "(page N)"
    # label -- this cap is reused unchanged for every page (see
    # generate_routing()), not just the first.
    tight_limit = len(header_text) + len(minimal_stub) + 50
    _scan(repo, out, ["--routing-max-catalog-chars", str(tight_limit),
                       "--routing-max-files-per-catalog", "1000", "--force"])
    cat_text = (out / "routing" / "core.md").read_text(encoding="utf-8")
    assert cat_text.count("### `") == 1  # only the (stubbed) first entry survives in detail
    assert "Purpose clues" not in cat_text  # full-detail sections did not sneak in for the first entry
    assert f"### `{first_path}`" in cat_text
    # By construction there's only a little spare room left after the
    # header + stub -- not enough for the appendix's short notice, so
    # none is shown here (rather than being forced in over budget). But
    # unlike the old single-page/drop-the-rest design, the other 4 files
    # are *not* omitted at all -- each is too big for the remainder of
    # this (now-full) page, so each spills onto its own further page.
    assert "Omitted from this catalog" not in cat_text
    for page_num in range(2, 6):
        assert (out / "routing" / f"core__page{page_num}.md").exists()
    manifest = _manifest(out)
    core_pages = [c for c in manifest["catalogs"] if c["key"] == "core" or c["key"].startswith("core (page")]
    assert len(core_pages) == 5  # one file per page, all 5 files accounted for
    assert sum(c["file_count"] for c in core_pages) == 5
    assert sum(c["omitted_file_count"] for c in core_pages) == 0


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
    assert "## Other files (non-Python / boilerplate)" in cat_text
    assert "| `core/readme.md` | Widget Factory | " in cat_text
    assert "| `core/data.json` |" in cat_text
    assert "Important symbols" not in cat_text.split("## Other files")[1]
    assert "(none resolved" not in cat_text.split("## Other files")[1]


def test_init_py_gets_a_lightweight_table_row_with_docstring_title(repo, out):
    # __init__.py is Python, but almost always a re-export/boilerplate
    # stub -- rendering the full Role/symbols/deps/callers/tests block
    # for it is the same low-information waste of catalog budget as doing
    # so for a non-Python file, so it gets the same compact table-row
    # treatment. Unlike an arbitrary non-Python file, it has no markdown
    # content to draw a title from, but it often has a real module
    # docstring -- use that instead of falling straight to filename terms
    # ("init"), which carries no information.
    write_files(repo, {
        "core/__init__.py": '"""Core domain package."""\n',
        "core/a.py": "def f():\n    return 1\n",
    })
    _scan(repo, out)
    cat_text = (out / "routing" / "core.md").read_text(encoding="utf-8")
    assert "### `core/a.py`" in cat_text  # a regular module still gets the full block
    assert "### `core/__init__.py`" not in cat_text
    assert "## Other files (non-Python / boilerplate)" in cat_text
    assert "| `core/__init__.py` | Core domain package. | " in cat_text


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
    # the same way the Python block list is -- rows that don't fit on one
    # page spill onto further pages (docs__page2.md, ...) rather than
    # being silently dropped or left unbounded.
    files = {f"docs/note_{i:03d}.md": f"# Note {i}\n\nBody.\n" for i in range(200)}
    write_files(repo, files)
    _scan(repo, out, ["--routing-max-catalog-chars", "2000"])
    cat_text = (out / "routing" / "docs.md").read_text(encoding="utf-8")
    assert len(cat_text) < 2000 + 200
    row_count = cat_text.count("| `docs/note_")
    assert 0 < row_count < 200
    assert (out / "routing" / "docs__page2.md").exists()

    manifest = _manifest(out)
    docs_pages = [c for c in manifest["catalogs"] if c["key"] == "docs" or c["key"].startswith("docs (page")]
    assert len(docs_pages) > 1
    assert sum(c["file_count"] for c in docs_pages) == 200  # every note lands on some page
    assert sum(c["omitted_file_count"] for c in docs_pages) == 0


def test_paged_catalog_filenames_stay_within_the_byte_cap(repo, out):
    # A partition key long enough that its own (unpaged) filename is
    # already near _MAX_CATALOG_FILENAME_BYTES must still produce a valid,
    # within-cap filename once a "__pageN" suffix is appended for its
    # overflow pages -- _paged_filename() has to re-truncate the base
    # name, not just tack the suffix on unconditionally.
    segment = "x" * 70
    files = {f"{segment}/mod_{i}.py": f'"""Docstring {i}."""\n\ndef f_{i}():\n    return {i}\n' for i in range(40)}
    write_files(repo, files)
    _scan(repo, out, ["--routing-max-catalog-chars", "1000", "--routing-max-files-per-catalog", "1000"])
    manifest = _manifest(out)
    pages = [c for c in manifest["catalogs"] if c["key"] == segment or c["key"].startswith(f"{segment} (page")]
    assert len(pages) > 1  # confirms this scenario actually exercises pagination
    for c in pages:
        filename = c["path"].rsplit("/", 1)[-1]
        assert len(filename.encode("utf-8")) <= 150, f"paged catalog filename exceeds the byte cap: {filename!r}"
        assert (out / c["path"]).exists()
    assert sum(c["file_count"] for c in pages) == 40


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


def test_scan_survives_a_graphify_node_with_unhashable_community(repo, out):
    # Regression: a node with a non-hashable `community` value (e.g. a
    # list) crashed `by_file.setdefault(...).add((community, name))` with
    # TypeError: unhashable type -- the top-level-object check alone
    # (previous fix) doesn't catch a malformed *node* inside an otherwise
    # well-formed graph.json. This is otherwise valid JSON, just malformed
    # in a way this tool's own reader must tolerate rather than crash on.
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
    graph = {
        "built_at_commit": "",
        "nodes": [{"source_file": "core/a.py", "community": []}],
    }
    (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
    _scan(repo, out)  # must not crash
    assert (out / "routing" / "index.md").exists()


def test_scan_survives_mixed_int_and_str_graphify_community_ids(repo, out):
    # Regression: the malformed-node fix only normalized a `community`
    # value when it *wasn't already* str/int, so a legitimate int (e.g.
    # 1) and a legitimate str (e.g. "2") both passed through unchanged.
    # When the *same* source_file appeared in two nodes with those two
    # differently-typed-but-individually-valid community IDs, sorted()
    # on that file's tuple set still crashed with TypeError: '<' not
    # supported between instances of 'str' and 'int'.
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
    graph = {
        "built_at_commit": "",
        "nodes": [
            {"source_file": "core/a.py", "community": 1},
            {"source_file": "core/a.py", "community": "2"},
        ],
    }
    (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
    _scan(repo, out)  # must not crash
    assert (out / "routing" / "index.md").exists()


def test_scan_survives_a_non_string_graphify_built_at_commit(repo, out):
    # Regression: a truthy but non-string built_at_commit (e.g. an int, or
    # a nonempty object) reached `built_at_commit[:12]` in the revision-
    # mismatch message unchanged, raising TypeError (int/dict aren't
    # sliceable) -- since routing loads Graphify during every normal
    # scan, one malformed field in this optional artifact crashed the
    # whole scan instead of just being treated as unavailable evidence.
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    (repo / "graphify-out").mkdir(parents=True, exist_ok=True)
    graph = {
        "built_at_commit": 123,
        "nodes": [{"source_file": "core/a.py", "community": 1}],
    }
    (repo / "graphify-out" / "graph.json").write_text(json.dumps(graph), encoding="utf-8")
    _scan(repo, out)  # must not crash
    assert (out / "routing" / "index.md").exists()


def test_content_derived_purpose_clues_are_redacted(repo, out):
    # Regression: _markdown_title() and the module-docstring purpose clue
    # both pull real content directly from the scanned file, unlike
    # filename terms -- but neither went through redact_secrets() before
    # being written into a routing catalog meant for attachment to an
    # LLM, unlike every excerpt/chunk elsewhere in this tool.
    write_files(repo, {
        "docs/secret.md": "token = 'abcdefghijklmnopqrstuvwxyz'\n\nMore text.\n",
        "core/secret_module.py": '"""token = \'abcdefghijklmnopqrstuvwxyz\'"""\n\ndef f():\n    return 1\n',
    })
    _scan(repo, out)
    docs_text = (out / "routing" / "docs.md").read_text(encoding="utf-8")
    core_text = (out / "routing" / "core.md").read_text(encoding="utf-8")
    assert "abcdefghijklmnopqrstuvwxyz" not in docs_text
    assert "abcdefghijklmnopqrstuvwxyz" not in core_text


def test_markdown_title_redacts_before_truncating(repo, out):
    # Regression: _markdown_title() truncated the candidate title to
    # _MAX_CONTENT_TITLE_CHARS (120) *before* calling redact_secrets(). A
    # headingless markdown file starting with a secret-shaped quoted value
    # whose closing quote falls beyond character 120 had that quote cut
    # off first, breaking _SECRET_ASSIGNMENT_PATTERN's closing-quote
    # backreference -- redaction never matched, and the (truncated)
    # secret prefix leaked into the routing catalog.
    secret_value = "x" * 200
    write_files(repo, {"docs/secret.md": f'token = "{secret_value}"\n\nMore text.\n'})
    _scan(repo, out)
    docs_text = (out / "routing" / "docs.md").read_text(encoding="utf-8")
    assert "xxxxxxxxxx" not in docs_text
    assert "REDACTED" in docs_text


def test_catalog_filenames_are_capped_by_encoded_byte_length_not_char_count(repo, out):
    # Regression: the filename length cap measured Python string length
    # (Unicode code points), not encoded UTF-8 byte length. A partition
    # key built from multi-byte characters (e.g. CJK, 3 bytes/char in
    # UTF-8) could look comfortably under a 150-*character* cap while its
    # actual UTF-8 encoding was 2-3x that many *bytes* -- the quantity a
    # real filesystem's per-component name limit is actually measured in,
    # so the write could still fail with OSError: File name too long
    # despite passing the (character-counting) cap check.
    segment = "模块" * 28  # 56 CJK characters -> 168 bytes in UTF-8:
    # under a 150-*char* cap, over a 150-*byte* cap.
    files = {f"{segment}/mod_{i}.py": f"def f_{i}():\n    return {i}\n" for i in range(61)}
    write_files(repo, files)
    _scan(repo, out, ["--routing-max-files-per-catalog", "30"])
    manifest = _manifest(out)
    assert manifest["catalogs"]
    for cat in manifest["catalogs"]:
        filename = cat["path"].rsplit("/", 1)[-1]
        assert len(filename.encode("utf-8")) <= 150, f"catalog filename exceeds the byte cap: {filename!r}"
        assert (out / cat["path"]).exists()


def test_catalog_filenames_stay_within_filesystem_limits(repo, out):
    # Regression: an oversized partition split through several long
    # directory segments flattened its entire key ("/" -> "_") into a
    # filename with no length cap, which could exceed the filesystem's
    # per-component name limit (compounded by atomic_write_text's own
    # temp-file suffix) and make the write itself fail with OSError:
    # File name too long.
    segment = "x" * 70
    files = {f"{segment}/{segment}/{segment}/{segment}/mod_{i}.py": f"def f_{i}():\n    return {i}\n"
             for i in range(61)}
    write_files(repo, files)
    _scan(repo, out, ["--routing-max-files-per-catalog", "30"])
    manifest = _manifest(out)
    for cat in manifest["catalogs"]:
        assert len(cat["path"]) < 200
        assert (out / cat["path"]).exists()
