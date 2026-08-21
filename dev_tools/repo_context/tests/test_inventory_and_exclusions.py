import csv

from conftest import run_tool, write_files


def _read_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_basic_repository_inventory(repo, out):
    write_files(repo, {
        "README.md": "# Hello\n",
        "src/app.py": "def f():\n    return 1\n",
        "data.json": '{"a": 1}\n',
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    rows = {r["relative_path"]: r for r in _read_csv(out / "file_inventory.csv")}
    assert set(rows) == {"README.md", "src/app.py", "data.json"}
    assert rows["README.md"]["category"] == "documentation"
    assert rows["src/app.py"]["category"] == "python_source"
    assert rows["data.json"]["category"] == "data"
    for r in rows.values():
        assert r["included"] == "true"
        assert len(r["sha256"]) == 64

    assert (out / "repository_tree.txt").exists()
    assert (out / "repository_overview.md").exists()
    assert (out / "README.md").exists()
    assert (out / "generation_manifest.json").exists()


def test_default_exclusions(repo, out):
    write_files(repo, {
        ".git/config": "junk",
        "__pycache__/mod.cpython-311.pyc": "junk",
        "node_modules/pkg/index.js": "junk",
        ".venv/lib/site.py": "junk",
        "src/keep.py": "x = 1\n",
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    rows = {r["relative_path"]: r for r in _read_csv(out / "file_inventory.csv")}
    assert "src/keep.py" in rows
    for excluded_dir in (".git", "__pycache__", "node_modules", ".venv"):
        assert not any(p.startswith(excluded_dir + "/") for p in rows), f"{excluded_dir} leaked into inventory"

    tree_text = (out / "repository_tree.txt").read_text(encoding="utf-8")
    assert ".git" not in tree_text
    assert "node_modules" not in tree_text


def test_binary_file_detection(repo, out):
    write_files(repo, {
        "image.bin": b"\x00\x01\x02\xff\xfe\x00binarydata",
        "text.txt": "just some text\n",
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    rows = {r["relative_path"]: r for r in _read_csv(out / "file_inventory.csv")}
    assert rows["image.bin"]["text_or_binary"] == "binary"
    assert rows["image.bin"]["line_count"] == ""
    assert rows["image.bin"]["chunked"] == "false"
    assert rows["text.txt"]["text_or_binary"] == "text"


def test_duplicate_filenames_in_different_directories(repo, out):
    write_files(repo, {
        "a/util.py": "def helper():\n    return 1\n",
        "b/util.py": "def helper():\n    return 2\n",
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    rows = {r["relative_path"]: r for r in _read_csv(out / "file_inventory.csv")}
    assert "a/util.py" in rows and "b/util.py" in rows
    assert rows["a/util.py"]["sha256"] != rows["b/util.py"]["sha256"]

    symbols = _read_csv(out / "python_symbols.csv")
    files_with_helper = {r["relative_path"] for r in symbols if r["qualified_name"] == "helper"}
    assert files_with_helper == {"a/util.py", "b/util.py"}


def test_secret_files_excluded_by_default(repo, out):
    write_files(repo, {
        ".env": "SECRET=1\n",
        "id_rsa": "-----BEGIN PRIVATE KEY-----\n",
        "credentials.json": "{}\n",
        "app.py": "x = 1\n",
    })
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    rows = {r["relative_path"]: r for r in _read_csv(out / "file_inventory.csv")}
    for secret_file in (".env", "id_rsa", "credentials.json"):
        assert rows[secret_file]["included"] == "false"
        assert "secret_pattern" in rows[secret_file]["exclusion_reason"]
    assert rows["app.py"]["included"] == "true"

    # explicit override
    result2 = run_tool(["scan", str(repo), "--output", str(out), "--include-secrets", "--force"])
    assert result2.returncode == 0, result2.stderr
    rows2 = {r["relative_path"]: r for r in _read_csv(out / "file_inventory.csv")}
    assert rows2[".env"]["included"] == "true"
