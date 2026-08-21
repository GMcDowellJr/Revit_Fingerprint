import csv
import os

import pytest

from conftest import run_tool, write_files

pytestmark = pytest.mark.skipif(
    os.name == "nt", reason="symlink creation typically requires elevated privileges on Windows"
)


def test_symlink_escaping_root_is_excluded(tmp_path, repo, out):
    write_files(repo, {"inside.py": "x = 1\n"})
    outside = tmp_path / "outside"
    write_files(outside, {"secret_outside.py": "SECRET = 'do-not-leak'\n"})

    (repo / "escape_link").symlink_to(outside, target_is_directory=True)

    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    with open(out / "file_inventory.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    paths = {r["relative_path"] for r in rows}
    assert not any("secret_outside" in p for p in paths)
    assert not any("escape_link" in p for p in paths)

    tree_text = (out / "repository_tree.txt").read_text(encoding="utf-8")
    assert "secret_outside" not in tree_text

    for csv_path in (out / "file_inventory.csv", out / "python_symbols.csv"):
        assert "do-not-leak" not in csv_path.read_text(encoding="utf-8")


def test_symlink_cycle_inside_root_does_not_hang(repo, out):
    write_files(repo, {"inside.py": "x = 1\n"})
    (repo / "cycle").mkdir()
    (repo / "cycle" / "loop").symlink_to(repo / "cycle", target_is_directory=True)

    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr
