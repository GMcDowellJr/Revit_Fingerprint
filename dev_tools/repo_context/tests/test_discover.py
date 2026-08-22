import json

from conftest import run_tool, write_files


def _scan(repo, out):
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr


def test_discover_groups_matches_by_channel_and_writes_draft_request(repo, out):
    write_files(repo, {
        "core/governance.py": '"""Governance narrative tier assignment helpers."""\n\n'
                              "def assign_tier(x):\n    return x\n",
        "tests/test_governance.py": "from core.governance import assign_tier\n\n\n"
                                     "def test_assign_tier():\n    assert assign_tier(1) == 1\n",
    })
    _scan(repo, out)
    result = run_tool([
        "discover", str(repo), "--output", str(out),
        "--question", "Where is governance narrative tier assignment determined?",
    ])
    assert result.returncode == 0, result.stderr

    packets_dir = out / "packets"
    reports = list(packets_dir.glob("discover_*.md"))
    requests = list(packets_dir.glob("discover_*.packet_request.json"))
    assert reports and requests

    report_text = reports[0].read_text(encoding="utf-8")
    assert "## Exact terminology matches" in report_text
    assert "## Symbol-name matches" in report_text
    assert "## Path matches" in report_text
    assert "## Docstring matches" in report_text
    assert "## Structural neighbors" in report_text
    assert "## Related tests" in report_text
    assert "## Graphify candidates" in report_text
    assert "assign_tier" in report_text
    assert "core/governance.py" in report_text

    draft = json.loads(requests[0].read_text(encoding="utf-8"))
    assert draft["schema_version"] == "1.0"
    assert draft["question"] == "Where is governance narrative tier assignment determined?"
    assert "assign_tier" in [s["name"] for s in draft["selectors"]["symbols"]]


def test_discover_output_feeds_directly_into_packet_request(repo, out):
    write_files(repo, {
        "core/archetype.py": '"""Archetype classification."""\n\n'
                              "def classify_archetype(x):\n    return x\n",
        "tests/test_archetype.py": "from core.archetype import classify_archetype\n\n\n"
                                    "def test_classify():\n    assert classify_archetype(1) == 1\n",
    })
    _scan(repo, out)
    result = run_tool([
        "discover", str(repo), "--output", str(out),
        "--question", "Which code produces archetype classifications?",
    ])
    assert result.returncode == 0, result.stderr

    [request_path] = list((out / "packets").glob("discover_*.packet_request.json"))
    result2 = run_tool(["packet", str(repo), "--output", str(out), "--request", str(request_path)])
    assert result2.returncode == 0, result2.stderr


def test_discover_preserves_case_of_mixed_case_identifiers(repo, out):
    # Regression: _terms() used to lowercase every extracted word before
    # writing it into the draft's `search_terms`, but those are matched
    # case-sensitively downstream (rc_request.resolve_search_terms) -- a
    # mixed-case identifier like `WidgetFactory` became the literal term
    # "widgetfactory", which never matches the real (mixed-case) spelling
    # in source.
    write_files(repo, {
        "core/widgets.py": "class WidgetFactory:\n    pass\n",
    })
    _scan(repo, out)
    result = run_tool([
        "discover", str(repo), "--output", str(out),
        "--question", "How does WidgetFactory work?",
    ])
    assert result.returncode == 0, result.stderr
    [request_path] = list((out / "packets").glob("discover_*.packet_request.json"))
    draft = json.loads(request_path.read_text(encoding="utf-8"))
    assert "WidgetFactory" in draft["selectors"]["search_terms"]
    assert "widgetfactory" not in draft["selectors"]["search_terms"]

    # And the term must actually resolve when run through `packet`, not
    # just survive with the right casing in the draft.
    result2 = run_tool(["packet", str(repo), "--output", str(out), "--request", str(request_path)])
    assert result2.returncode == 0, result2.stderr
    [packet_md] = list((out / "packets").glob("packet_*.md"))
    assert "WidgetFactory" in packet_md.read_text(encoding="utf-8")


def test_discover_stopword_only_question_writes_no_draft(repo, out):
    # Regression: a question made up entirely of stopwords/short tokens
    # (e.g. "How do I do it?") produced empty selectors in every channel,
    # so the draft's `files`/`symbols`/`search_terms`/`lines` were all
    # empty -- a request that `packet --request`'s own validation rejects
    # outright as having no usable selector. Discovery must not write an
    # unusable draft; it must say so in the report instead.
    write_files(repo, {"core/a.py": "def f():\n    return 1\n"})
    _scan(repo, out)
    result = run_tool([
        "discover", str(repo), "--output", str(out), "--question", "How do I do it?",
    ])
    assert result.returncode == 1, result.stdout
    assert not list((out / "packets").glob("discover_*.packet_request.json"))
    [report_path] = list((out / "packets").glob("discover_*.md"))
    report_text = report_path.read_text(encoding="utf-8")
    assert "No draft" in report_text
    assert "packet --request" in report_text


def test_discover_draft_symbol_selectors_include_the_resolved_file(repo, out):
    # Regression: the draft's symbol selectors dropped the resolved
    # `file`, keeping only `{"name": qn}`. A qualified name that appears
    # in more than one file (e.g. two top-level `build()` functions) then
    # produced two *identical* draft selectors, and `packet --request`
    # reported both as ambiguous instead of retrieving either -- even
    # though discovery itself had already resolved which file each match
    # came from.
    write_files(repo, {
        "core/build_a.py": "def build():\n    return 1\n",
        "core/build_b.py": "def build():\n    return 2\n",
    })
    _scan(repo, out)
    result = run_tool([
        "discover", str(repo), "--output", str(out), "--question", "How does build work?",
    ])
    assert result.returncode == 0, result.stderr
    [request_path] = list((out / "packets").glob("discover_*.packet_request.json"))
    draft = json.loads(request_path.read_text(encoding="utf-8"))
    symbols = draft["selectors"]["symbols"]
    assert len(symbols) == 2
    assert all("file" in s for s in symbols)
    assert {s["file"] for s in symbols} == {"core/build_a.py", "core/build_b.py"}

    # And each must actually resolve (not "ambiguous") when run through
    # `packet`, since the `file` qualifier disambiguates it.
    result2 = run_tool(["packet", str(repo), "--output", str(out), "--request", str(request_path)])
    assert result2.returncode == 0, result2.stderr
    [sidecar_path] = list((out / "packets").glob("packet_*.resolution.json"))
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    symbol_resolutions = [r for r in sidecar["resolution_report"] if r["selector_type"] == "symbol"]
    assert len(symbol_resolutions) == 2
    assert all(r["status"] == "resolved" for r in symbol_resolutions)


def test_discover_max_per_channel_truncates_with_count(repo, out):
    files = {f"tools/thing_needle_{i}.py": f"def f_{i}():\n    return {i}\n" for i in range(10)}
    write_files(repo, files)
    _scan(repo, out)
    result = run_tool([
        "discover", str(repo), "--output", str(out), "--question", "thing needle", "--max-per-channel", "3",
    ])
    assert result.returncode == 0, result.stderr
    [report_path] = list((out / "packets").glob("discover_*.md"))
    text = report_path.read_text(encoding="utf-8")
    assert "more (not shown" in text
