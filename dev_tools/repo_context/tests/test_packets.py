from conftest import run_tool, write_files


def _scan(repo, out):
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr


def test_packet_by_file(repo, out):
    write_files(repo, {"pkg/mod.py": "def f():\n    return 1\n"})
    _scan(repo, out)
    result = run_tool(["packet", str(repo), "--output", str(out), "--file", "pkg/mod.py"])
    assert result.returncode == 0, result.stderr
    packet_files = list((out / "packets").glob("*.md"))
    assert packet_files
    text = packet_files[-1].read_text(encoding="utf-8")
    assert "pkg/mod.py" in text
    assert "def f():" in text


def test_search_packet_respects_size_budget_on_repetitive_file(repo, out):
    lines = ["needle here"] * 2000
    write_files(repo, {"repetitive.txt": "\n".join(lines) + "\n"})
    _scan(repo, out)
    result = run_tool([
        "packet", str(repo), "--output", str(out), "--search", "needle",
        "--max-lines", "20", "--max-characters", "2000",
    ])
    assert result.returncode == 0, result.stderr
    packet_files = list((out / "packets").glob("packet_search_*.md"))
    assert packet_files
    text = packet_files[0].read_text(encoding="utf-8")
    assert text.count("needle here") <= 20
    assert "Omitted" in text


def test_packet_withholds_excerpt_when_source_changed_since_scan(repo, out):
    write_files(repo, {"a.py": "def f():\n    return 1\n"})
    _scan(repo, out)
    # Change the file after scanning, without rescanning.
    write_files(repo, {"a.py": "def f():\n    return 'changed'\n"})

    result = run_tool(["packet", str(repo), "--output", str(out), "--symbol", "f"])
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_f.md").read_text(encoding="utf-8")
    assert "withheld" in text
    assert "return 1" not in text
    assert "return 'changed'" not in text


def test_packet_by_symbol(repo, out):
    write_files(repo, {"pkg/mod.py": "def unique_symbol_name():\n    return 1\n"})
    _scan(repo, out)
    result = run_tool(["packet", str(repo), "--output", str(out), "--symbol", "unique_symbol_name"])
    assert result.returncode == 0, result.stderr
    text = (out / "packets" / "packet_unique_symbol_name.md").read_text(encoding="utf-8")
    assert "unique_symbol_name" in text
    assert "def unique_symbol_name():" in text


def test_packet_by_search(repo, out):
    write_files(repo, {"pkg/mod.py": "def f():\n    write_detail_marker()\n    return 1\n"})
    _scan(repo, out)
    result = run_tool(["packet", str(repo), "--output", str(out), "--search", "write_detail_marker"])
    assert result.returncode == 0, result.stderr
    packet_files = list((out / "packets").glob("packet_search_*.md"))
    assert packet_files
    text = packet_files[0].read_text(encoding="utf-8")
    assert "pkg/mod.py:2" in text


def test_packet_by_line(repo, out):
    write_files(repo, {"pkg/mod.py": "def f():\n    x = 1\n    return x\n"})
    _scan(repo, out)
    result = run_tool(["packet", str(repo), "--output", str(out), "--line", "pkg/mod.py:2"])
    assert result.returncode == 0, result.stderr
    packet_files = list((out / "packets").glob("*.md"))
    text = packet_files[-1].read_text(encoding="utf-8")
    assert "def f():" in text


def test_ambiguous_symbol_requires_qualifier_or_all_matches(repo, out):
    write_files(repo, {
        "a.py": "def run():\n    return 1\n",
        "b.py": "def run():\n    return 2\n",
    })
    _scan(repo, out)

    ambiguous = run_tool(["packet", str(repo), "--output", str(out), "--symbol", "run", "--name", "ambig1"])
    assert ambiguous.returncode == 0, ambiguous.stderr
    text = (out / "packets" / "packet_ambig1.md").read_text(encoding="utf-8")
    assert "Ambiguous symbol" in text
    assert "a.py" in text and "b.py" in text
    assert "def run():" not in text  # no bodies shown for an unresolved ambiguity

    resolved_by_file = run_tool([
        "packet", str(repo), "--output", str(out), "--symbol", "run", "--file", "a.py", "--name", "ambig2",
    ])
    assert resolved_by_file.returncode == 0
    text2 = (out / "packets" / "packet_ambig2.md").read_text(encoding="utf-8")
    assert "def run():" in text2

    all_matches = run_tool([
        "packet", str(repo), "--output", str(out), "--symbol", "run", "--all-matches", "--name", "ambig3",
    ])
    assert all_matches.returncode == 0
    text3 = (out / "packets" / "packet_ambig3.md").read_text(encoding="utf-8")
    assert text3.count("def run():") == 2


def test_packet_accepts_windows_style_relative_path(repo, out):
    write_files(repo, {"pkg/mod.py": "def f():\n    return 1\n"})
    _scan(repo, out)
    result = run_tool(["packet", str(repo), "--output", str(out), "--file", r"pkg\mod.py"])
    assert result.returncode == 0, result.stderr
    packet_files = list((out / "packets").glob("*.md"))
    text = packet_files[-1].read_text(encoding="utf-8")
    assert "pkg/mod.py" in text
    assert "def f():" in text


def test_packet_without_prior_scan_fails_cleanly(repo, out):
    write_files(repo, {"a.py": "x = 1\n"})
    out.mkdir(parents=True, exist_ok=True)
    result = run_tool(["packet", str(repo), "--output", str(out), "--file", "a.py"])
    assert result.returncode != 0
