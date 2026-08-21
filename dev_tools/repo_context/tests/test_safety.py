import csv

from conftest import run_tool, write_files


def test_refuses_output_dir_equal_to_root(repo):
    write_files(repo, {"a.py": "x = 1\n"})
    result = run_tool(["scan", str(repo), "--output", str(repo)])
    assert result.returncode != 0


def test_output_dir_inside_root_is_never_scanned_into_itself(repo):
    write_files(repo, {"a.py": "x = 1\n"})
    out = repo / "repo_context"  # default-style output name, nested inside root
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    with open(out / "file_inventory.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    paths = {r["relative_path"] for r in rows}
    assert not any(p.startswith("repo_context/") for p in paths)


def test_custom_named_output_dir_inside_root_is_auto_excluded(repo):
    write_files(repo, {"a.py": "x = 1\n"})
    out = repo / "my_custom_context_dir"
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    with open(out / "file_inventory.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    paths = {r["relative_path"] for r in rows}
    assert not any(p.startswith("my_custom_context_dir/") for p in paths)


def test_nested_output_dir_excludes_only_itself_not_its_whole_parent(repo):
    # --output docs/context must exclude only docs/context, not all of docs/.
    write_files(repo, {
        "docs/readme.md": "# real docs\n",
        "docs/other/notes.md": "# more docs\n",
        "a.py": "x = 1\n",
    })
    out = repo / "docs" / "context"
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    with open(out / "file_inventory.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    paths = {r["relative_path"] for r in rows}
    assert "docs/readme.md" in paths
    assert "docs/other/notes.md" in paths
    assert not any(p.startswith("docs/context/") for p in paths)


def test_invalid_root_returns_nonzero(tmp_path):
    missing_root = tmp_path / "does_not_exist"
    out = tmp_path / "out"
    result = run_tool(["scan", str(missing_root), "--output", str(out)])
    assert result.returncode != 0


def test_missing_subcommand_returns_nonzero():
    result = run_tool([])
    assert result.returncode != 0
