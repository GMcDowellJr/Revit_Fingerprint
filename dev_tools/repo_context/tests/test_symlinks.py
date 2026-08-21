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


def test_file_symlink_escaping_root_is_excluded_without_being_read(tmp_path, repo, out):
    write_files(repo, {"normal.py": "x = 1\n"})
    outside_file = tmp_path / "outside_secret.py"
    write_files(tmp_path, {"outside_secret.py": "SUPER_SECRET_OUTSIDE_CONTENT = 1\n"})

    (repo / "leaked.py").symlink_to(outside_file)

    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    with open(out / "file_inventory.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert not any("leaked" in r["relative_path"] for r in rows)
    for csv_path in (out / "file_inventory.csv", out / "python_symbols.csv"):
        assert "SUPER_SECRET_OUTSIDE_CONTENT" not in csv_path.read_text(encoding="utf-8")


def test_symlink_cycle_inside_root_does_not_hang(repo, out):
    write_files(repo, {"inside.py": "x = 1\n"})
    (repo / "cycle").mkdir()
    (repo / "cycle" / "loop").symlink_to(repo / "cycle", target_is_directory=True)

    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr


def test_symlink_pointing_at_output_dir_is_excluded(repo):
    # The output directory must be *nested inside* root for this to
    # exercise the real scenario: an internal symlink whose target is the
    # output directory, reached under a different name than its own
    # exact relative path -- the plain lexical-path check alone can't
    # catch that.
    write_files(repo, {"normal.py": "x = 1\n"})
    out = repo / "context_output"
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    (repo / "alias").symlink_to(out, target_is_directory=True)

    result2 = run_tool(["scan", str(repo), "--output", str(out), "--force"])
    assert result2.returncode == 0, result2.stderr

    with open(out / "file_inventory.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert not any("alias" in r["relative_path"] for r in rows)
    assert not any("context_output" in r["relative_path"] for r in rows)


def test_symlink_into_output_subdirectory_is_excluded(repo):
    write_files(repo, {"normal.py": "x = 1\n"})
    out = repo / "context_output"
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    # A symlink whose target is *inside* the output dir (not equal to it),
    # e.g. pointing straight at the generated chunks/ subdirectory.
    (out / "chunks").mkdir(parents=True, exist_ok=True)
    (repo / "alias").symlink_to(out / "chunks", target_is_directory=True)

    result2 = run_tool(["scan", str(repo), "--output", str(out), "--force"])
    assert result2.returncode == 0, result2.stderr

    with open(out / "file_inventory.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert not any("alias" in r["relative_path"] for r in rows)


def test_fifo_directly_under_root_does_not_hang(repo, out):
    # No symlink involved -- a bare FIFO (no writer) would block forever
    # if the walker ever opened it for hashing.
    write_files(repo, {"normal.py": "x = 1\n"})
    os.mkfifo(repo / "pipe.txt")

    result = run_tool(["scan", str(repo), "--output", str(out)], timeout=20)
    assert result.returncode == 0, result.stderr

    with open(out / "file_inventory.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert not any("pipe" in r["relative_path"] for r in rows)
    assert any(r["relative_path"] == "normal.py" for r in rows)
