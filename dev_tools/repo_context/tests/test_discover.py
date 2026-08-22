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
