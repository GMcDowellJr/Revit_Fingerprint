import csv

from conftest import run_tool, write_files


def _chunk_rows(out, source):
    with open(out / "chunk_manifest.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    return sorted((r for r in rows if r["source_relative_path"] == source), key=lambda r: int(r["chunk_number"]))


def _assert_full_contiguous_coverage(rows, total_lines):
    assert int(rows[0]["start_line"]) == 1
    assert int(rows[-1]["end_line"]) == total_lines
    for a, b in zip(rows, rows[1:]):
        assert int(b["start_line"]) <= int(a["end_line"]) + 1, "gap between chunks"
        assert int(b["start_line"]) >= int(a["start_line"]), "chunk order violated"


def _make_big_python_file(n_funcs=80) -> str:
    lines = ['"""Big module."""', ""]
    for i in range(n_funcs):
        lines += [f"def func_{i}(x):", f'    """Doc {i}."""', f"    if x > {i}:",
                   f"        return x + {i}", f"    return x - {i}", ""]
    text = "\n".join(lines) + "\n"
    assert text.count("\n") > 400
    return text


def test_large_python_file_gets_chunked(repo, out):
    text = _make_big_python_file(n_funcs=200)  # comfortably over the 1000-line default threshold
    total_lines = text.count("\n")
    write_files(repo, {"big.py": text})
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    rows = _chunk_rows(out, "big.py")
    assert len(rows) > 1
    _assert_full_contiguous_coverage(rows, total_lines)

    # Python chunking should not overlap (symbol boundaries are the logical boundary).
    for r in rows:
        assert int(r["overlap_lines"]) == 0

    # Every chunk file exists and hash matches.
    import hashlib
    for r in rows:
        p = out / r["chunk_relative_path"]
        assert p.exists()
        assert hashlib.sha256(p.read_bytes()).hexdigest() == r["chunk_sha256"]


def test_single_oversized_function_is_split_by_line_range(repo, out):
    lines = ["def big_func(x):"]
    for i in range(1200):
        lines.append(f"    y{i} = x + {i}")
    lines.append("    return x")
    text = "\n".join(lines) + "\n"
    total_lines = text.count("\n")
    write_files(repo, {"oversized.py": text})
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    rows = _chunk_rows(out, "oversized.py")
    assert len(rows) >= 2
    _assert_full_contiguous_coverage(rows, total_lines)
    for r in rows:
        assert "big_func" in r["symbols"]


def test_chunk_target_lines_one_does_not_hang(repo, out):
    # A blank line just before the ideal boundary could previously send
    # _find_logical_boundary below the current chunk's start, producing
    # an inverted range that never advanced (infinite loop).
    lines = []
    for i in range(60):
        lines.append(f"line {i}")
        if i % 5 == 0:
            lines.append("")
    text = "\n".join(lines) + "\n"
    total_lines = text.count("\n")
    write_files(repo, {"notes.txt": text})

    result = run_tool([
        "scan", str(repo), "--output", str(out),
        "--chunk-target-lines", "1", "--chunk-line-threshold", "10", "--chunk-char-threshold", "10000",
    ], timeout=20)
    assert result.returncode == 0, result.stderr

    rows = _chunk_rows(out, "notes.txt")
    assert rows
    _assert_full_contiguous_coverage(rows, total_lines)


def test_generic_text_chunking_has_overlap_and_full_coverage(repo, out):
    paragraphs = []
    for i in range(400):
        paragraphs.append(f"Line {i} of a long changelog entry describing change number {i}.")
    text = "\n".join(paragraphs) + "\n"
    total_lines = text.count("\n")
    write_files(repo, {"CHANGELOG_BIG.txt": text})
    result = run_tool([
        "scan", str(repo), "--output", str(out),
        "--chunk-line-threshold", "100", "--chunk-char-threshold", "100000",
        "--chunk-target-lines", "120", "--chunk-overlap-lines", "10",
    ])
    assert result.returncode == 0, result.stderr

    rows = _chunk_rows(out, "CHANGELOG_BIG.txt")
    assert len(rows) > 1
    _assert_full_contiguous_coverage(rows, total_lines)
    assert any(int(r["overlap_lines"]) > 0 for r in rows[1:])
