# repo_context

A local, read-only repository-context generator. It scans a repository or
plain folder and produces a set of files that prepare that codebase for
analysis by a document-grounded LLM such as Microsoft 365 Copilot —
without uploading, transmitting, executing, importing, or modifying any
of the scanned source code.

This tool is standalone and lives outside `core/`, `domains/`, `runner/`,
and `tools/`: it has nothing to do with the Revit Fingerprint extraction
or analysis pipelines and can be pointed at any Python-containing
repository, including this one.

## Requirements

- Python 3.10+
- Standard library only. No install step, no `requirements.txt`.
- Works on Windows, macOS, and Linux.

## Commands

```bash
python repo_context.py scan ROOT --output OUTPUT_DIR [options]
python repo_context.py packet ROOT --output OUTPUT_DIR [selector options]
python repo_context.py validate OUTPUT_DIR [options]
```

### Examples

Scan this repository:

```bash
python dev_tools/repo_context/repo_context.py scan . --output /tmp/revit_fp_context --verbose
```

Scan a large Python module and see how it was chunked:

```bash
python repo_context.py scan C:\path\to\repo --output C:\path\to\repo_context
# then look up the file in chunk_manifest.csv, e.g.:
#   grep tools/generate_governance_narrative.py C:\path\to\repo_context\chunk_manifest.csv
```

Find a function and its callers:

```bash
python repo_context.py packet C:\path\to\repo --output C:\path\to\repo_context --symbol run_model_spec
# -> packets/packet_run_model_spec.md lists statically-resolved callers/callees
```

Create a packet around a failure line (e.g. a traceback pointing at
`guardian_model_compare_common.py:842`):

```bash
python repo_context.py packet C:\path\to\repo --output C:\path\to\repo_context --line guardian_model_compare_common.py:842
```

Search for a term and see which files/symbols mention it:

```bash
python repo_context.py packet C:\path\to\repo --output C:\path\to\repo_context --search write_detail
```

Re-generate after making changes (only chunk output for unchanged files is
reused; everything else is recomputed fresh):

```bash
python repo_context.py scan C:\path\to\repo --output C:\path\to\repo_context
```

Force a full rebuild:

```bash
python repo_context.py scan C:\path\to\repo --output C:\path\to\repo_context --force
```

Validate a generated output directory:

```bash
python repo_context.py validate C:\path\to\repo_context
```

## What each generated file contains

- `repository_overview.md` — concise statistics and navigation info (file
  counts, largest files, likely entry points, parse failures, directories
  by role, unusually large/complex Python modules). No unsupported
  architectural claims.
- `repository_tree.txt` — a deterministic, size/line-annotated tree of
  included files. Excluded directories can be shown (without their
  contents) with `--show-excluded-dirs`.
- `file_inventory.csv` / `file_inventory.jsonl` — one row per considered
  file: path, extension, category, size, text/binary, line count,
  SHA-256, included/excluded (+ reason), chunked, parse status, and a
  generated/vendor flag.
- `python_symbols.csv` / `python_symbols.jsonl` — one row per Python
  module, class, function, async function, method, or nested function,
  with qualified name, line range, parent, decorators, parameters, base
  classes, return annotation, docstring presence + first line (not the
  full docstring or body), an **approximate** cyclomatic complexity, and
  directly nested symbol names.
- `python_imports.csv` — every `import`/`from ... import ...` statement
  with a conservative best-effort resolution to a repository file.
  `resolution_status` is `resolved` only when exactly one file matches;
  otherwise `ambiguous` or `unresolved_external_or_missing`.
- `python_calls.csv` — syntactic call sites from AST analysis, each with a
  best-effort candidate target and a `confidence` of `high`, `medium`, or
  `unresolved`, plus a plain-language explanation. **These are static
  call-expression candidates, not proof of runtime dispatch.**
- `entrypoint_candidates.csv` — Python files with a
  `if __name__ == "__main__":` guard or a conventional entrypoint
  filename (`main.py`, `cli.py`, `manage.py`, `app.py`, `__main__.py`).
- `parse_warnings.csv` — Python files that failed to parse, with location
  and message. The rest of the scan still completes.
- `chunk_manifest.csv` — one row per chunk: source file, chunk file,
  original line range, overlap, symbols present, source/chunk SHA-256,
  character count, and a rough estimated token count.
- `chunks/` — line-numbered Markdown chunks of files too large to hand to
  an LLM directly. Python files are split on class/function boundaries;
  an oversized single symbol is split by line range and every part is
  labeled. Other text files split by line range with a small overlap,
  nudged toward blank lines/headings when possible. Lines that look like
  secrets are redacted by default (`--no-redact-secrets` to disable).
- `packets/` — targeted context packets from the `packet` command.
- `generation_manifest.json` — tool version, options used, counts, and
  incremental-reuse stats for this run.

## How the data was produced

A single read-only filesystem walk (skipping default-excluded
directories, never following a symlink that points outside the selected
root) plus `ast`-based static analysis of Python files. No scanned file
is ever imported or executed. Hashing is streamed SHA-256. Import and
call resolution is deliberately conservative — a target is only marked
`resolved` when exactly one repository file/symbol matches.

## Important limitations

- Call-graph and import resolution are **best-effort static analysis**.
  Dynamic imports, `getattr`/reflection dispatch, monkey-patching, and
  polymorphic instance-method dispatch (`obj.method()` where `obj`'s type
  isn't statically known) are reported as `unresolved`, never guessed.
- `self.method()` resolution only looks at the enclosing class's own body
  and its statically-known same-file or unambiguously-imported base
  classes — not a full MRO.
- Cyclomatic-complexity numbers are **approximations** (a simple decision-
  point count: `if`/`for`/`while`/`try`/`with`/`assert`/boolean operators/
  comprehension `if`s), not a formally verified metric.
- Only Python gets symbol/import/call analysis. Other text files are
  still inventoried, classified, and chunked.
- Directory-role labels in `repository_overview.md` are a
  by-file-classification-count heuristic only, not an architectural claim.
- Incremental mode reuses only unchanged **chunk output** (a pure function
  of a file's own content plus the chunking options); file inventory and
  Python symbol/import/call analysis are always recomputed fresh on every
  run, and cross-file resolution always runs against the current full
  file set — because a change elsewhere in the repo can change what an
  unchanged file's imports/calls resolve to. `--force` bypasses chunk
  reuse entirely.
- `--include-glob` can rescue a file matched by the default secret
  patterns or `--exclude-glob`, but it cannot pull files out of a
  hard-excluded directory (e.g. `.git/`, `node_modules/`) — those are
  never walked at all.

## Microsoft 365 Copilot workflow

- **Orientation questions** ("what is this repo", "where's the test
  suite") → attach `repository_overview.md` and `repository_tree.txt`.
- **"What does this file do"** → attach the file's chunk(s) from
  `chunks/` (look it up in `chunk_manifest.csv`), or generate a packet:
  `packet ROOT --output OUT --file <path>`.
- **"Where is X used / what calls X"** → `packet ROOT --output OUT
  --symbol X`, then attach the resulting `packets/packet_X.md`.
- **Debugging a specific failure line** → `packet ROOT --output OUT
  --line <path>:<line>`.
- **"Where is <text> handled"** → `packet ROOT --output OUT --search
  <text>`.
- Avoid attaching all of `chunks/` at once — packets are the compact,
  targeted option; chunks are for when you already know exactly which
  large file you need.

**Generated context can contain proprietary source code and must be
handled under the same security rules as the repository it came from.**

## Regenerating and validating

```bash
python repo_context.py scan ROOT --output OUTPUT_DIR      # incremental
python repo_context.py scan ROOT --output OUTPUT_DIR --force   # full rebuild
python repo_context.py validate OUTPUT_DIR                # sanity checks, nonzero exit on failure
```

## Running the tool's own tests

```bash
cd dev_tools/repo_context
python -m pytest tests/ -v
```

These tests build small synthetic repositories under pytest's `tmp_path`
and drive the CLI end-to-end (no network access, nothing outside
`tmp_path` is touched).
