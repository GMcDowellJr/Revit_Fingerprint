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


def test_validate_fails_on_missing_required_file(repo, out):
    write_files(repo, {"a.py": "x = 1\n"})
    r = run_tool(["scan", str(repo), "--output", str(out)])
    assert r.returncode == 0, r.stderr
    (out / "python_imports.csv").unlink()
    v = run_tool(["validate", str(out)])
    assert v.returncode != 0
    assert "python_imports.csv" in v.stdout
