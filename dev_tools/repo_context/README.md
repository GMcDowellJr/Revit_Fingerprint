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
python repo_context.py packet ROOT --output OUTPUT_DIR [selector options | --request packet_request.json]
python repo_context.py discover ROOT --output OUTPUT_DIR --question "..." [options]
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

## Discovery-to-packet workflow (starting from a question, not a symbol)

A user does not need to already know an exact file, symbol, or line to get
a bounded, source-backed answer. The intended flow:

```
Natural-language question
    -> LLM (or you) reads routing/index.md, then a routing/<catalog>.md
    -> LLM (or you) writes packet_request.json (schema/packet_request.schema.json)
    -> repo_context.py packet ROOT --output OUT --request packet_request.json
    -> repo_context.py validates every selector and reports resolution/ambiguity
    -> packets/packet_<name>.md: bounded, exact, current source excerpts
       with an explicit "why included" origin per item, plus
       packets/packet_<name>.resolution.json for machine consumption
```

1. **`scan`** now also generates `routing/` (skip with `--no-routing`):
   - `routing/index.md` — a small, persistent guide: repository revision +
     dirty-worktree state, a source-manifest hash (freshness evidence),
     every catalog's path/coverage/counts, and instructions for writing a
     `packet_request.json`. Kept under `--routing-index-max-chars`
     (default 6000) by first dropping per-catalog summaries, then
     truncating the catalog list itself with an explicit count of what was
     omitted.
   - `routing/<key>.md` — one catalog per partition (derived from actual
     repository paths, not a hardcoded domain list: test-classified files
     always get their own `tests.md`; archived/legacy files get
     `archived.md`; everything else buckets by top-level directory,
     further split by an additional path segment once a directory exceeds
     `--routing-max-files-per-catalog`, default 60). Each file entry lists
     its operational-role classification (with the evidence/rule that
     produced it — see below), purpose clues (module docstring + filename
     terms, never an invented summary), top-level symbols with exact line
     numbers, entrypoint evidence, resolved internal imports, statically
     resolved callers, related tests, and (if `graphify-out/graph.json` is
     present and its `built_at_commit` matches the current HEAD) a
     clearly-labeled Graphify community. Capped at
     `--routing-max-catalog-chars` (default 24000); anything over the cap
     is listed by bare path instead of a full entry, never dropped
     silently.
   - `routing/routing_manifest.json` — the same data in machine-readable
     form (per-catalog file/symbol counts and source hashes, git revision,
     options used), for tooling that wants to consume routing output
     directly instead of parsing Markdown.
   - **Operational-role classification** (evidence-based, conservative —
     an uncertain case is `unknown`, never silently `active`):
     `operator_entrypoint`, `active_pipeline`, `developer_utility`,
     `test_harness`, `migration`, `archived_or_legacy`, `library_module`,
     `unknown`.

2. **`packet --request packet_request.json`** validates the request
   against `schema/packet_request.schema.json` (rejected: unknown schema
   version, unknown fields, paths outside the scanned repository, invalid
   line ranges — see `examples/packet_request.invalid_*.json`), resolves
   every selector deterministically, and reports the outcome:
   - `resolved` — exact file/symbol/line identity, included in the packet.
   - `ambiguous` — an unqualified symbol matched more than one definition;
     **never silently picked** — every qualified alternative is listed so
     the request can be narrowed (add a `"file"` field or a fully
     qualified name).
   - `missing` / `invalid` — reported, and (unless `"strict": true` in the
     request) every other selector is still processed rather than
     aborting the whole packet.
   - A hard `limits.max_estimated_tokens` budget is enforced, but an
     **explicit** selector (a file/symbol/line the request named directly)
     is never silently truncated to make room — if it doesn't fit, packet
     generation stops and reports exactly which selector(s) conflict with
     the budget, asking for the request or the limit to be revised.
     Non-explicit content (caller/callee/import/test expansions, search
     matches) is budgeted normally and reported under "Omitted /
     unresolved" if it doesn't fit.
   - Every included item states its origin(s) — `explicit_file_selector`,
     `explicit_symbol_selector`, `explicit_line_selector`,
     `exact_search_match`, `caller_expansion`, `callee_expansion`,
     `import_expansion`, `related_test_expansion` — never a merged
     relevance score.
   - Direct `--file`/`--symbol`/`--search`/`--line`/`--changed` selectors
     keep working exactly as before; `--request` is a separate, additional
     path through the same `packet` command.

3. **`discover ROOT --output OUT --question "..."`** (optional, fully
   deterministic, no embeddings/LLM call) groups literal matches for the
   question's terms by channel — exact terminology, symbol name, path,
   docstring, structural neighbor (caller/callee of a matched symbol), and
   Graphify candidate (if available) — **without merging them into a
   score**. It never answers the question; it writes
   `packets/discover_<question>.md` (the grouped report) and
   `packets/discover_<question>.packet_request.json` (a draft request to
   review/edit, then run through `packet --request`).

See `schema/packet_request.schema.json` for the full contract and
`examples/` for one valid and several invalid request files.

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
  generated/vendor flag. `parse_status` is `ok`/`failed` for Python files,
  `n/a` for everything else, or `skipped_too_large` for an included text
  file bigger than the 30 MB in-memory read cap — it still gets a
  streamed line count and SHA-256, but no symbol/import/call analysis or
  chunking (nothing is silently dropped without a visible reason).
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
- `routing/` — `index.md`, one catalog per partition, and
  `routing_manifest.json`; see "Discovery-to-packet workflow" above.
  Skippable with `scan --no-routing`.

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
  reuse entirely. Every run also removes any `chunks/` file left over
  from a previous run that the current `chunk_manifest.csv` no longer
  references (source deleted, newly excluded, or no longer large enough
  to need chunking), so stale content never lingers on disk.
- `--include-glob` can rescue a file matched by the default secret
  patterns or `--exclude-glob`, but it cannot pull files out of a
  hard-excluded directory (e.g. `.git/`, `node_modules/`) — those are
  never walked at all.

## Microsoft 365 Copilot workflow

- **Don't already know a file/symbol/line?** → attach `routing/index.md`
  (small enough to keep as persistent context), let Copilot pick a
  `routing/<catalog>.md`, have it write a `packet_request.json`, then run
  `packet ROOT --output OUT --request packet_request.json`. See
  "Discovery-to-packet workflow" above.
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
