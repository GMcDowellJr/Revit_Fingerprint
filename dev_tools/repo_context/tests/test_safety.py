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


def test_non_positive_chunk_target_lines_rejected(repo, out):
    write_files(repo, {"a.py": "x = 1\n"})
    result = run_tool(["scan", str(repo), "--output", str(out), "--chunk-target-lines", "0"])
    assert result.returncode != 0
    result_neg = run_tool(["scan", str(repo), "--output", str(out), "--chunk-target-lines", "-5"])
    assert result_neg.returncode != 0


def test_validate_does_not_require_packets_dir(repo, out):
    # Git doesn't track empty directories, so a freshly checked-out
    # repo_context/ output that hasn't had `packet` run yet has no
    # packets/ directory at all -- validate must not treat that as an
    # error (see PR #444 review discussion).
    write_files(repo, {"a.py": "x = 1\n"})
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr
    import shutil
    shutil.rmtree(out / "packets")

    v = run_tool(["validate", str(out)])
    assert v.returncode == 0, v.stdout + v.stderr


def test_redaction_does_not_remove_valid_code_statements_naming_token_or_secret():
    # Regression for a known prior defect: the secret-line redaction regex
    # matched on *label words* like "token"/"secret"/"password" plus any
    # sufficiently-long unquoted value, which swallowed ordinary code
    # statements (variable references, function calls) that merely
    # mention one of those words but carry no actual secret literal.
    from rc_common import redact_secrets

    benign = [
        "token = user_provided_value_from_config",
        "access_token = fetch_access_token_from_provider()",
        "def get_password_hint():\n    return None",
        "session_token_field = SessionTokenField(required=True)",
        "secret_name = os.environ.get('MY_SECRET_NAME')",
    ]
    for line in benign:
        assert redact_secrets(line) == line, f"benign statement was redacted: {line!r}"

    secret_like = [
        'password = "SuperSecretValue123"',
        "AWS_KEY = AKIAABCDEFGHIJKLMNOP",
        "api_key: sk-ABCdef123456ghijklmnop",
        "-----BEGIN RSA PRIVATE KEY-----",
    ]
    for line in secret_like:
        redacted = redact_secrets(line)
        assert "[REDACTED-POSSIBLE-SECRET]" in redacted, f"secret-shaped line was not redacted: {line!r}"


def test_redaction_regression_flows_through_generated_chunks(repo, out):
    body_lines = ["def f():"] + [f"    x{i} = {i}" for i in range(1200)]
    body_lines.insert(1, "    token = user_provided_value_from_config")
    write_files(repo, {"big.py": "\n".join(body_lines) + "\n"})
    result = run_tool(["scan", str(repo), "--output", str(out)])
    assert result.returncode == 0, result.stderr

    chunk_files = list((out / "chunks").glob("*big.py*"))
    assert chunk_files
    combined = "\n".join(p.read_text(encoding="utf-8") for p in chunk_files)
    assert "user_provided_value_from_config" in combined
    assert "[REDACTED-POSSIBLE-SECRET]" not in combined
