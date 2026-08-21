import csv

from conftest import run_tool, write_files


def _big_text(n=1500):
    return "\n".join(f"line {i}" for i in range(n)) + "\n"


def test_validate_passes_on_freshly_generated_output(repo, out):
    write_files(repo, {
        "a.py": "def f():\n    return 1\n",
        "big.txt": _big_text(),
    })
    r = run_tool(["scan", str(repo), "--output", str(out)])
    assert r.returncode == 0, r.stderr
    v = run_tool(["validate", str(out)])
    assert v.returncode == 0, v.stdout + v.stderr
    assert "PASS" in v.stdout


def test_validate_detects_modified_chunk(repo, out):
    write_files(repo, {"big.txt": _big_text()})
    r = run_tool(["scan", str(repo), "--output", str(out)])
    assert r.returncode == 0, r.stderr

    with open(out / "chunk_manifest.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert rows
    chunk_path = out / rows[0]["chunk_relative_path"]
    chunk_path.write_text(chunk_path.read_text(encoding="utf-8") + "TAMPERED", encoding="utf-8")

    v = run_tool(["validate", str(out)])
    assert v.returncode != 0
    assert "hash mismatch" in v.stdout


def test_validate_detects_missing_chunk(repo, out):
    write_files(repo, {"big.txt": _big_text()})
    r = run_tool(["scan", str(repo), "--output", str(out)])
    assert r.returncode == 0, r.stderr

    with open(out / "chunk_manifest.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert rows
    chunk_path = out / rows[0]["chunk_relative_path"]
    chunk_path.unlink()

    v = run_tool(["validate", str(out)])
    assert v.returncode != 0
    assert "missing" in v.stdout.lower()


def test_validate_catches_entirely_deleted_chunk_rows(repo, out):
    write_files(repo, {"big.txt": _big_text()})
    r = run_tool(["scan", str(repo), "--output", str(out)])
    assert r.returncode == 0, r.stderr

    manifest_path = out / "chunk_manifest.csv"
    rows = list(csv.reader(open(manifest_path, newline="", encoding="utf-8")))
    with open(manifest_path, "w", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerow(rows[0])  # header only -- all rows for big.txt gone

    v = run_tool(["validate", str(out)])
    assert v.returncode != 0
    assert "has no rows in chunk_manifest.csv" in v.stdout


def test_scan_does_not_crash_on_malformed_prior_chunk_manifest(repo, out):
    lines = ["def big_func(x):"]
    for i in range(1200):
        lines.append(f"    y{i} = x + {i}")
    lines.append("    return x")
    write_files(repo, {"big.py": "\n".join(lines) + "\n"})
    r1 = run_tool(["scan", str(repo), "--output", str(out)])
    assert r1.returncode == 0, r1.stderr

    manifest_path = out / "chunk_manifest.csv"
    rows = list(csv.reader(open(manifest_path, newline="", encoding="utf-8")))
    rows[1][3] = "CORRUPT"  # start_line column
    with open(manifest_path, "w", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerows(rows)

    r2 = run_tool(["scan", str(repo), "--output", str(out)])
    assert r2.returncode == 0, r2.stderr
    assert "Traceback" not in r2.stdout and "Traceback" not in r2.stderr


def test_validate_reports_malformed_chunk_manifest_instead_of_crashing(repo, out):
    lines = ["def big_func(x):"]
    for i in range(1200):
        lines.append(f"    y{i} = x + {i}")
    lines.append("    return x")
    write_files(repo, {"big.py": "\n".join(lines) + "\n"})
    r = run_tool(["scan", str(repo), "--output", str(out)])
    assert r.returncode == 0, r.stderr

    manifest_path = out / "chunk_manifest.csv"
    rows = list(csv.reader(open(manifest_path, newline="", encoding="utf-8")))
    rows[1][3] = "NOT_A_NUMBER"  # start_line column
    with open(manifest_path, "w", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerows(rows)

    v = run_tool(["validate", str(out)])
    assert v.returncode != 0
    assert "malformed" in v.stdout.lower()
    assert "Traceback" not in v.stdout and "Traceback" not in v.stderr


def test_validate_fails_on_missing_required_file(repo, out):
    write_files(repo, {"a.py": "x = 1\n"})
    r = run_tool(["scan", str(repo), "--output", str(out)])
    assert r.returncode == 0, r.stderr
    (out / "python_imports.csv").unlink()
    v = run_tool(["validate", str(out)])
    assert v.returncode != 0
    assert "python_imports.csv" in v.stdout
