import time

from conftest import run_tool, write_files


def _make_repo(repo):
    write_files(repo, {
        "pkg/__init__.py": "",
        "pkg/a.py": "def a():\n    return 1\n",
        "pkg/b.py": "from pkg.a import a\n\n\ndef b():\n    return a() + 1\n",
        "tests/test_a.py": "from pkg.a import a\n\n\ndef test_a():\n    assert a() == 1\n",
    })


def test_deterministic_output_across_identical_runs(repo, tmp_path):
    _make_repo(repo)
    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"
    r1 = run_tool(["scan", str(repo), "--output", str(out1)])
    r2 = run_tool(["scan", str(repo), "--output", str(out2)])
    assert r1.returncode == 0 and r2.returncode == 0

    for name in ("file_inventory.csv", "python_symbols.csv", "python_imports.csv",
                 "python_calls.csv", "repository_tree.txt", "chunk_manifest.csv"):
        assert (out1 / name).read_text(encoding="utf-8") == (out2 / name).read_text(encoding="utf-8"), name


def test_incremental_regeneration_reuses_unchanged_chunk_output(repo, out):
    lines = ["def big_func(x):"]
    for i in range(1200):
        lines.append(f"    y{i} = x + {i}")
    lines.append("    return x")
    write_files(repo, {"big.py": "\n".join(lines) + "\n", "small.py": "x = 1\n"})

    r1 = run_tool(["scan", str(repo), "--output", str(out)])
    assert r1.returncode == 0, r1.stderr
    chunk_files = sorted((out / "chunks").glob("*big.py*"))
    assert chunk_files
    mtimes_before = {p: p.stat().st_mtime_ns for p in chunk_files}

    time.sleep(0.05)
    r2 = run_tool(["scan", str(repo), "--output", str(out)])
    assert r2.returncode == 0, r2.stderr
    mtimes_after = {p: p.stat().st_mtime_ns for p in chunk_files}
    assert mtimes_before == mtimes_after, "unchanged file's chunks were rewritten"

    import json
    manifest = json.loads((out / "generation_manifest.json").read_text(encoding="utf-8"))
    assert manifest["incremental"]["chunks_regenerated_for_files"] == 0
    assert manifest["incremental"]["chunks_reused_for_files"] >= 1

    # Now modify the big file: its chunks must regenerate.
    (repo / "big.py").write_text("\n".join(lines) + "\n    extra = 1\n", encoding="utf-8")
    time.sleep(0.05)
    r3 = run_tool(["scan", str(repo), "--output", str(out)])
    assert r3.returncode == 0, r3.stderr
    manifest3 = json.loads((out / "generation_manifest.json").read_text(encoding="utf-8"))
    assert manifest3["incremental"]["chunks_regenerated_for_files"] >= 1


def test_force_bypasses_incremental_reuse(repo, out):
    write_files(repo, {"a.py": "x = 1\n"})
    r1 = run_tool(["scan", str(repo), "--output", str(out)])
    assert r1.returncode == 0
    r2 = run_tool(["scan", str(repo), "--output", str(out), "--force"])
    assert r2.returncode == 0
    import json
    manifest = json.loads((out / "generation_manifest.json").read_text(encoding="utf-8"))
    assert manifest["incremental"]["reuse_active"] is False
