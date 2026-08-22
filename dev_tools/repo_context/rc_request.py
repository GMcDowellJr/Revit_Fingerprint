"""packet_request.json: schema validation, deterministic selector
resolution, and request-driven packet generation.

This is the "LLM produces packet_request.json -> repo_context validates
selectors -> repo_context generates a bounded, source-backed evidence
packet" half of the discovery-to-packet workflow described in
schema/packet_request.schema.json. Nothing here executes repository code;
selectors are resolved purely against the CSV indexes a prior `scan`
already produced, plus read-only source excerpts.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import rc_graphify
from rc_common import TOOL_VERSION, atomic_write_text, get_git_info, redact_secrets, sanitize_stem, sha256_text
# Sibling-module reuse: these are the same read-only, budget-aware
# rendering primitives the direct --file/--symbol/--search/--line packet
# path already uses. Reusing them (rather than re-implementing excerpt
# extraction, freshness checks, and BFS caller/callee walks a second time)
# keeps both packet paths consistent.
from rc_packet import (
    Budget, _load_csv, _norm_rel, _file_is_fresh, _safe_excerpt,
    _find_symbol_candidates, _bfs_callers, _bfs_callees, _candidate_tests_for_file,
)

SUPPORTED_SCHEMA_VERSIONS = {"1.0"}

_PATH_SELECTOR_FIELDS = {"selectors.files[]", "selectors.symbols[].file", "selectors.lines[].file"}


class RequestError(Exception):
    """Raised for a structurally invalid request (schema errors). Distinct
    from resolution issues (missing/ambiguous selectors), which are
    reported per-selector instead of raising."""


@dataclass
class ResolvedRequest:
    schema_version: str
    question: str
    strict: bool
    files: list          # list[str] repo-relative paths, as given
    symbols: list         # list[dict] {"name":..., "file": optional}
    search_terms: list
    lines: list           # list[dict] {"file":..., "line":..., "end_line": optional}
    include_callers: bool
    include_callees: bool
    include_imports: bool
    include_related_tests: bool
    include_graphify: bool
    search_as_regex: bool
    max_hops: int
    max_estimated_tokens: int
    max_files: int


def _is_safe_repo_relative_path(p: str) -> bool:
    if not p or not isinstance(p, str):
        return False
    norm = p.replace("\\", "/")
    if norm.startswith("/") or norm.startswith("~"):
        return False
    if re.match(r"^[A-Za-z]:", norm):  # drive letter (Windows absolute path)
        return False
    parts = norm.split("/")
    if any(part == ".." for part in parts):
        return False
    return True


def validate_request_dict(data: dict) -> list:
    """Structural validation mirroring schema/packet_request.schema.json.
    Returns a list of human-readable error strings (empty == valid). Does
    NOT check selectors against the scanned repository -- that happens
    during resolution, where a missing file/symbol is reported per-item
    rather than failing the whole request.
    """
    errors = []
    if not isinstance(data, dict):
        return ["request must be a JSON object"]

    allowed_top = {"schema_version", "question", "strict", "selectors", "expansion", "limits"}
    for key in data:
        if key not in allowed_top:
            errors.append(f"unknown top-level field: '{key}' (schema does not permit extension fields)")

    version = data.get("schema_version")
    if version is None:
        errors.append("missing required field: schema_version")
    elif not isinstance(version, str):
        # `in SUPPORTED_SCHEMA_VERSIONS` (a set) raises TypeError on an
        # unhashable value (a list/dict) -- check the type explicitly
        # first so malformed-but-valid JSON is reported as a normal
        # validation error instead of crashing the CLI.
        errors.append(f"'schema_version' must be a string, got {type(version).__name__}")
    elif version not in SUPPORTED_SCHEMA_VERSIONS:
        errors.append(f"unsupported schema_version: {version!r} (supported: {sorted(SUPPORTED_SCHEMA_VERSIONS)})")

    question = data.get("question")
    if not isinstance(question, str) or not question.strip():
        errors.append("missing or empty required field: question")

    if "strict" in data and not isinstance(data["strict"], bool):
        errors.append("'strict' must be a boolean")

    selectors = data.get("selectors")
    if selectors is None:
        errors.append("missing required field: selectors")
    elif not isinstance(selectors, dict):
        errors.append("'selectors' must be an object")
    else:
        allowed_sel = {"files", "symbols", "search_terms", "lines"}
        for key in selectors:
            if key not in allowed_sel:
                errors.append(f"unknown field under 'selectors': '{key}'")

        files = selectors.get("files", [])
        if not isinstance(files, list) or not all(isinstance(x, str) for x in files):
            errors.append("'selectors.files' must be an array of strings")
        else:
            for p in files:
                if not p:
                    errors.append("'selectors.files' contains an empty path")
                elif not _is_safe_repo_relative_path(p):
                    errors.append(f"'selectors.files' contains a path outside the scanned repository: {p!r}")

        symbols = selectors.get("symbols", [])
        if not isinstance(symbols, list):
            errors.append("'selectors.symbols' must be an array")
        else:
            for i, s in enumerate(symbols):
                if not isinstance(s, dict) or "name" not in s or not isinstance(s.get("name"), str) or not s["name"]:
                    errors.append(f"'selectors.symbols[{i}]' must be an object with a non-empty 'name'")
                    continue
                extra = set(s.keys()) - {"name", "file"}
                if extra:
                    errors.append(f"'selectors.symbols[{i}]' has unknown field(s): {sorted(extra)}")
                if "file" in s and (not isinstance(s["file"], str) or not _is_safe_repo_relative_path(s["file"])):
                    errors.append(f"'selectors.symbols[{i}].file' is not a safe repo-relative path: {s.get('file')!r}")

        terms = selectors.get("search_terms", [])
        if not isinstance(terms, list) or not all(isinstance(x, str) and x for x in terms):
            errors.append("'selectors.search_terms' must be an array of non-empty strings")

        lines = selectors.get("lines", [])
        if not isinstance(lines, list):
            errors.append("'selectors.lines' must be an array")
        else:
            for i, ln in enumerate(lines):
                if not isinstance(ln, dict) or "file" not in ln or "line" not in ln:
                    errors.append(f"'selectors.lines[{i}]' must be an object with 'file' and 'line'")
                    continue
                extra = set(ln.keys()) - {"file", "line", "end_line"}
                if extra:
                    errors.append(f"'selectors.lines[{i}]' has unknown field(s): {sorted(extra)}")
                if not isinstance(ln["file"], str) or not _is_safe_repo_relative_path(ln["file"]):
                    errors.append(f"'selectors.lines[{i}].file' is not a safe repo-relative path: {ln.get('file')!r}")
                # bool is a subclass of int in Python (isinstance(True, int) is
                # True), so `"line": true` would otherwise pass as line 1 --
                # JSON Schema (and this contract) does not treat booleans as
                # integers, so exclude them explicitly.
                line_val = ln.get("line")
                line_ok = isinstance(line_val, int) and not isinstance(line_val, bool)
                if not line_ok or line_val < 1:
                    errors.append(f"'selectors.lines[{i}].line' must be a positive integer")
                if "end_line" in ln:
                    end_val = ln["end_line"]
                    end_ok = isinstance(end_val, int) and not isinstance(end_val, bool)
                    if not end_ok or end_val < 1:
                        errors.append(f"'selectors.lines[{i}].end_line' must be a positive integer")
                    elif line_ok and end_val < line_val:
                        errors.append(f"'selectors.lines[{i}].end_line' ({end_val}) is before 'line' ({line_val})")

        if isinstance(selectors, dict) and not any(selectors.get(k) for k in allowed_sel):
            errors.append("'selectors' must contain at least one non-empty selector list "
                          "(files/symbols/search_terms/lines)")

    expansion = data.get("expansion", {})
    if not isinstance(expansion, dict):
        errors.append("'expansion' must be an object")
    else:
        allowed_exp = {"include_callers", "include_callees", "include_imports", "include_related_tests",
                       "include_graphify", "search_as_regex", "max_hops"}
        for key in expansion:
            if key not in allowed_exp:
                errors.append(f"unknown field under 'expansion': '{key}'")
        if "max_hops" in expansion:
            mh = expansion["max_hops"]
            if not isinstance(mh, int) or isinstance(mh, bool) or not (0 <= mh <= 5):
                errors.append("'expansion.max_hops' must be an integer between 0 and 5 "
                              "(a deep expansion is bounded, not unlimited)")
        for key in allowed_exp - {"max_hops"}:
            if key in expansion and not isinstance(expansion[key], bool):
                errors.append(f"'expansion.{key}' must be a boolean")

    limits = data.get("limits", {})
    if not isinstance(limits, dict):
        errors.append("'limits' must be an object")
    else:
        allowed_lim = {"max_estimated_tokens", "max_files"}
        for key in limits:
            if key not in allowed_lim:
                errors.append(f"unknown field under 'limits': '{key}'")
        for key in allowed_lim:
            if key in limits and (not isinstance(limits[key], int) or isinstance(limits[key], bool) or limits[key] < 1):
                errors.append(f"'limits.{key}' must be a positive integer")

    return errors


def parse_and_validate_request(text: str) -> tuple:
    """Returns (ResolvedRequest, errors). If errors is non-empty, the
    ResolvedRequest half is None."""
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        return None, [f"request is not valid JSON: {exc}"]

    errors = validate_request_dict(data)
    if errors:
        return None, errors

    selectors = data.get("selectors", {})
    expansion = data.get("expansion", {}) or {}
    limits = data.get("limits", {}) or {}

    resolved = ResolvedRequest(
        schema_version=data["schema_version"],
        question=data["question"],
        strict=bool(data.get("strict", False)),
        files=[_norm_rel(p) for p in selectors.get("files", [])],
        symbols=[{"name": s["name"], "file": _norm_rel(s["file"]) if s.get("file") else None}
                 for s in selectors.get("symbols", [])],
        search_terms=list(selectors.get("search_terms", [])),
        lines=[{"file": _norm_rel(ln["file"]), "line": ln["line"], "end_line": ln.get("end_line", ln["line"])}
               for ln in selectors.get("lines", [])],
        include_callers=bool(expansion.get("include_callers", True)),
        include_callees=bool(expansion.get("include_callees", True)),
        include_imports=bool(expansion.get("include_imports", True)),
        include_related_tests=bool(expansion.get("include_related_tests", True)),
        include_graphify=bool(expansion.get("include_graphify", False)),
        search_as_regex=bool(expansion.get("search_as_regex", False)),
        max_hops=int(expansion.get("max_hops", 1)),
        max_estimated_tokens=int(limits.get("max_estimated_tokens", 12000)),
        max_files=int(limits.get("max_files", 12)),
    )
    return resolved, []


# --- Selector resolution -------------------------------------------------

@dataclass
class SelectorResolution:
    selector_type: str   # "file" | "symbol" | "line" | "search_term"
    requested: str
    status: str           # "resolved" | "ambiguous" | "missing" | "invalid"
    detail: str
    candidates: list = field(default_factory=list)
    resolved_rows: list = field(default_factory=list)  # the matched row(s) for "resolved"


def resolve_files(files: list, files_by_path: dict) -> list:
    out = []
    for p in files:
        row = files_by_path.get(p)
        if row is None:
            out.append(SelectorResolution("file", p, "missing", f"'{p}' not found in file_inventory.csv"))
        elif row.get("included") != "true":
            out.append(SelectorResolution("file", p, "missing",
                                           f"'{p}' was excluded from the scan (reason: {row.get('exclusion_reason')})"))
        else:
            out.append(SelectorResolution("file", p, "resolved", f"matched included file '{p}'", resolved_rows=[row]))
    return out


def resolve_symbols(symbols: list, symbols_rows: list) -> list:
    out = []
    for sel in symbols:
        name, file_constraint = sel["name"], sel["file"]
        candidates = _find_symbol_candidates(name, symbols_rows, file_constraint)
        if not candidates:
            out.append(SelectorResolution("symbol", name, "missing",
                                           f"no symbol matching '{name}'"
                                           + (f" in '{file_constraint}'" if file_constraint else "")
                                           + " was found in python_symbols.csv"))
        elif len(candidates) > 1:
            alts = [f"{c['qualified_name']} ({c['relative_path']}:{c['start_line']}-{c['end_line']})" for c in candidates]
            out.append(SelectorResolution(
                "symbol", name, "ambiguous",
                f"'{name}' matches {len(candidates)} symbols; qualify with a fully-qualified name or a 'file' field",
                candidates=alts,
            ))
        else:
            row = candidates[0]
            out.append(SelectorResolution("symbol", name, "resolved",
                                           f"resolved to '{row['qualified_name']}' in '{row['relative_path']}'",
                                           resolved_rows=[row]))
    return out


def resolve_lines(lines: list, files_by_path: dict) -> list:
    out = []
    for ln in lines:
        f, start, end = ln["file"], ln["line"], ln["end_line"]
        label = f"{f}:{start}" + (f"-{end}" if end != start else "")
        row = files_by_path.get(f)
        if row is None:
            out.append(SelectorResolution("line", label, "missing", f"'{f}' not found in file_inventory.csv"))
            continue
        if row.get("included") != "true":
            out.append(SelectorResolution("line", label, "missing",
                                           f"'{f}' was excluded from the scan (reason: {row.get('exclusion_reason')})"))
            continue
        line_count = row.get("line_count")
        if line_count and line_count.isdigit() and end > int(line_count):
            out.append(SelectorResolution("line", label, "invalid",
                                           f"end line {end} exceeds '{f}' line count ({line_count})"))
            continue
        out.append(SelectorResolution("line", label, "resolved", f"resolved to '{f}' lines {start}-{end}",
                                       resolved_rows=[{"file": f, "start": start, "end": end}]))
    return out


def resolve_search_terms(root: Path, terms: list, search_as_regex: bool, files_rows: list) -> tuple:
    """Resolve each search term against the scanned repository's current
    source (same matching rule Tier 2 rendering uses). Returns
    (resolutions: list[SelectorResolution], matches_by_term: dict[str,
    list[(rel_path, line, text)]], stale_files_skipped: int).

    Done up front (before the strict-mode gate) so an invalid regex or a
    zero-match term is visible to strict mode exactly like a missing file
    or ambiguous symbol -- search terms were previously invisible to
    `all_resolutions` entirely, so strict mode could not catch them.
    """
    resolutions = []
    matches_by_term: dict = {}
    stale_files_skipped = 0
    for term in terms:
        try:
            pattern = re.compile(term) if search_as_regex else None
        except re.error as exc:
            resolutions.append(SelectorResolution("search_term", term, "invalid", f"not a valid regex: {exc}"))
            matches_by_term[term] = []
            continue
        term_matches = []
        for frow in files_rows:
            if frow.get("included") != "true" or frow.get("text_or_binary") == "binary":
                continue
            if not _file_is_fresh(root, frow["relative_path"], frow.get("sha256", "")):
                stale_files_skipped += 1
                continue
            excerpt = _safe_excerpt(root, frow["relative_path"], 1, 10_000_000)
            if excerpt is None:
                continue
            for ln, text in excerpt:
                hit = pattern.search(text) if pattern else (term in text)
                if hit:
                    term_matches.append((frow["relative_path"], ln, text))
        matches_by_term[term] = term_matches
        if term_matches:
            resolutions.append(SelectorResolution("search_term", term, "resolved",
                                                    f"{len(term_matches)} match(es) found"))
        else:
            resolutions.append(SelectorResolution("search_term", term, "missing", "no matches found"))
    return resolutions, matches_by_term, stale_files_skipped


# --- Rendering ------------------------------------------------------------

def _render_origin_header(title: str, origins: list) -> str:
    return f"\n### {title}\n_Included because: {', '.join(origins)}._\n"


def _render_excerpt_block(root: Path, rel_path: str, start: int, end: int, budget: Budget, out: list,
                           expected_sha256: str) -> str:
    """Returns "rendered", "stale" (withheld -- source changed since scan),
    "unavailable" (file missing/unreadable), or "too_large" (would not fit
    within the remaining budget). Only "too_large" is a hard, must-not-be-
    silently-dropped conflict for an *explicit* selector -- "stale" and
    "unavailable" are reported as ordinary (non-fatal) omissions, matching
    the direct --file/--symbol/--line packet path's existing behavior."""
    if not _file_is_fresh(root, rel_path, expected_sha256):
        msg = (f"_Source excerpt withheld: `{rel_path}` has changed on disk since the last `scan` "
               f"(SHA-256 mismatch); re-run `scan` for an up-to-date packet._\n")
        if budget.allow(msg, 1):
            out.append(msg)
            budget.spend(msg, 1)
        budget.omissions.append(f"`{rel_path}` changed since the last scan; excerpt withheld. Re-run scan.")
        return "stale"
    excerpt = _safe_excerpt(root, rel_path, start, end)
    if excerpt is None:
        out.append("_Source excerpt unavailable (file missing or unreadable)._\n")
        budget.omissions.append(f"`{rel_path}` excerpt unavailable (file missing or unreadable).")
        return "unavailable"
    body_lines = [f"{ln:>6}| {text}" for ln, text in excerpt]
    body = "\n".join(body_lines)
    if not budget.allow(body, len(body_lines)):
        return "too_large"
    body = redact_secrets(body)
    out.append("```\n" + body + "\n```\n")
    budget.spend(body, len(body_lines))
    return "rendered"


def _symbol_expansion(row: dict, calls_rows: list, imports_rows: list, files_by_path: dict,
                       req: ResolvedRequest, budget: Budget, out: list, note_focus_file,
                       communities_by_file: Optional[dict] = None) -> None:
    rel, qn = row["relative_path"], row["qualified_name"]

    if req.include_callers:
        callers = [c for c in _bfs_callers(rel, qn, calls_rows, req.max_hops) if c["confidence"] != "unresolved"]
        if callers:
            header = f"\nCallers of `{qn}` (statically resolved, max_hops={req.max_hops}):\n"
            if budget.allow(header, 1):
                out.append(header); budget.spend(header, 1)
                for c in callers:
                    line = (f"- `{c['caller_symbol']}` in `{c['caller_file']}`:{c['line']} "
                            f"— `{c['call_expression']}` ({c['confidence']}: {c['explanation']}) "
                            f"[origin: caller_expansion]")
                    if not budget.allow(line, 1):
                        budget.omissions.append(f"More callers of `{qn}` omitted (packet size limit reached); see python_calls.csv.")
                        break
                    out.append(line); budget.spend(line, 1)
            else:
                budget.omissions.append(f"Callers listing for `{qn}` omitted entirely (packet size limit reached).")

    if req.include_callees:
        callees = [c for c in _bfs_callees(rel, qn, calls_rows, req.max_hops) if c["confidence"] != "unresolved"]
        if callees:
            header = f"\nCallees of `{qn}` (statically resolved, max_hops={req.max_hops}):\n"
            if budget.allow(header, 1):
                out.append(header); budget.spend(header, 1)
                for c in callees:
                    line = (f"- `{c['call_expression']}` at line {c['line']} -> `{c['candidate_symbol']}` "
                            f"in `{c['candidate_file']}` ({c['confidence']}: {c['explanation']}) "
                            f"[origin: callee_expansion]")
                    if not budget.allow(line, 1):
                        budget.omissions.append(f"More callees of `{qn}` omitted (packet size limit reached); see python_calls.csv.")
                        break
                    out.append(line); budget.spend(line, 1)
            else:
                budget.omissions.append(f"Callees listing for `{qn}` omitted entirely (packet size limit reached).")

    if req.include_imports:
        file_imports = [i for i in imports_rows if i["source_file"] == rel and i["resolved_file"]]
        if file_imports:
            header = f"\nInternal imports of `{rel}` (import_expansion):\n"
            if budget.allow(header, 1):
                out.append(header); budget.spend(header, 1)
                for i in file_imports[:20]:
                    line = f"- line {i['line']}: `{i['imported_name'] or i['imported_module']}` -> `{i['resolved_file']}`"
                    if not budget.allow(line, 1):
                        break
                    out.append(line); budget.spend(line, 1)

    if req.include_related_tests:
        tests = _candidate_tests_for_file(rel, imports_rows, calls_rows, files_by_path)
        if tests:
            header = f"\nRelated tests for `{rel}` (related_test_expansion):\n"
            if budget.allow(header, 1):
                out.append(header); budget.spend(header, 1)
                for t in tests:
                    if not note_focus_file(t):
                        # Route through the same global-focus-file gate as
                        # every other tier -- a hard-coded high ceiling here
                        # would let related-test expansion silently bypass
                        # limits.max_files.
                        budget.omissions.append(
                            f"Related test `{t}` for `{rel}` omitted: limits.max_files ({req.max_files}) reached."
                        )
                        break
                    line = f"- `{t}`"
                    if not budget.allow(line, 1):
                        break
                    out.append(line); budget.spend(line, 1)

    if req.include_graphify and communities_by_file:
        my_communities = communities_by_file.get(rel, [])
        if my_communities:
            comm_ids = {cid for cid, _ in my_communities}
            peers = sorted(
                f for f, comms in communities_by_file.items()
                if f != rel and any(cid in comm_ids for cid, _ in comms)
            )[:10]
            if peers:
                header = f"\nGraphify community peers of `{rel}` ({rc_graphify.format_communities(my_communities)}):\n"
                if budget.allow(header, 1):
                    out.append(header); budget.spend(header, 1)
                    for p in peers:
                        if not note_focus_file(p):
                            # Route through the same global focus-file gate
                            # as every other expansion tier -- otherwise a
                            # Graphify peer could silently exceed
                            # limits.max_files while the resolution
                            # sidecar's focus_files list stayed under it.
                            budget.omissions.append(
                                f"Graphify community peer `{p}` of `{rel}` omitted: "
                                f"limits.max_files ({req.max_files}) reached."
                            )
                            break
                        line = f"- `{p}` [origin: graphify_expansion]"
                        if not budget.allow(line, 1):
                            budget.omissions.append(
                                f"More Graphify community peers of `{rel}` omitted (packet size limit reached)."
                            )
                            break
                        out.append(line); budget.spend(line, 1)
                else:
                    budget.omissions.append(
                        f"Graphify community peers listing for `{rel}` omitted entirely (packet size limit reached)."
                    )


def generate_packet_from_request(root: Path, output_dir: Path, request_path: Path,
                                  name_override: Optional[str] = None) -> tuple:
    """Returns (packet_path_or_None, resolution_report, error_message_or_None).

    error_message_or_None is set (and packet_path is None) for a
    structurally invalid request, or when strict mode / an unresolvable
    explicit-selector budget conflict blocks generation -- no partial
    packet is written in either case.
    """
    try:
        request_text = request_path.read_text(encoding="utf-8")
    except OSError as exc:
        return None, [], f"could not read request file {request_path}: {exc}"

    request_hash = sha256_text(request_text)
    resolved, errors = parse_and_validate_request(request_text)
    if errors:
        return None, [], "invalid packet_request.json:\n" + "\n".join(f"  - {e}" for e in errors)

    files_rows = _load_csv(output_dir / "file_inventory.csv")
    symbols_rows = _load_csv(output_dir / "python_symbols.csv")
    imports_rows = _load_csv(output_dir / "python_imports.csv")
    calls_rows = _load_csv(output_dir / "python_calls.csv")
    files_by_path = {r["relative_path"]: r for r in files_rows}
    symbols_by_file: dict = {}
    for r in symbols_rows:
        symbols_by_file.setdefault(r["relative_path"], []).append(r)

    file_resolutions = resolve_files(resolved.files, files_by_path)
    symbol_resolutions = resolve_symbols(resolved.symbols, symbols_rows)
    line_resolutions = resolve_lines(resolved.lines, files_by_path)
    search_resolutions, search_matches_by_term, stale_search_files = resolve_search_terms(
        root, resolved.search_terms, resolved.search_as_regex, files_rows,
    )
    all_resolutions = file_resolutions + symbol_resolutions + line_resolutions + search_resolutions

    unresolved_explicit = [r for r in all_resolutions if r.status != "resolved"]
    if resolved.strict and unresolved_explicit:
        lines = [f"  - {r.selector_type} '{r.requested}': {r.status} — {r.detail}" for r in unresolved_explicit]
        return None, [_res_to_dict(r) for r in all_resolutions], (
            "strict mode: aborting because the following selector(s) did not resolve cleanly:\n" + "\n".join(lines)
        )

    budget = Budget(max_lines=1_000_000, max_characters=resolved.max_estimated_tokens * 4)
    out: list = []
    focus_files: list = []

    communities_by_file: dict = {}
    if resolved.include_graphify:
        _git_for_graphify = get_git_info(root)
        communities_by_file, graphify_warnings_for_request = rc_graphify.load_graphify_communities(
            root, _git_for_graphify.get("commit") if _git_for_graphify.get("available") else None,
        )
        for w in graphify_warnings_for_request:
            budget.omissions.append(f"expansion.include_graphify was requested but unavailable: {w}")

    def note_focus_file(rel: str) -> bool:
        if rel in focus_files:
            return True
        if len(focus_files) >= resolved.max_files:
            return False
        focus_files.append(rel)
        return True

    # --- Tier 1: explicit selectors (never silently dropped) ---
    # explicit_conflicts collects only "the explicit excerpt itself doesn't
    # fit the budget" -- a hard, must-be-reported-not-truncated conflict.
    # Freshness withholding and non-mandatory expansions (callers/callees/
    # imports/tests) are still recorded as ordinary, non-fatal omissions on
    # `budget` and must NOT abort generation.
    explicit_conflicts: list = []

    for res in file_resolutions:
        if res.status != "resolved":
            continue
        rel = res.requested
        if not note_focus_file(rel):
            # This is an *explicit* selector -- limits.max_files being too
            # small to fit every distinct file the request named is the
            # same category of hard conflict as a token-budget overflow,
            # not something to quietly omit (which would otherwise leave
            # the resolution report claiming "resolved" for content that
            # was never actually rendered).
            explicit_conflicts.append(
                f"explicit file selector `{rel}` does not fit: limits.max_files ({resolved.max_files}) reached"
            )
            continue
        row = res.resolved_rows[0]
        out.append(_render_origin_header(f"File: `{rel}`", ["explicit_file_selector"]))
        top_level = sorted(
            [r for r in symbols_by_file.get(rel, []) if r["parent_symbol"] == "<module>" and r["symbol_type"] != "module"],
            key=lambda r: int(r["start_line"]),
        )
        if top_level:
            header = "Top-level symbols:"
            if budget.allow(header, 1):
                out.append(header)
                budget.spend(header, 1)
                for idx, r in enumerate(top_level):
                    line = f"- `{r['qualified_name']}` ({r['symbol_type']}, lines {r['start_line']}-{r['end_line']})"
                    if not budget.allow(line, 1):
                        budget.omissions.append(
                            f"{len(top_level) - idx} more top-level symbol(s) in `{rel}` omitted from the "
                            f"listing (packet size limit reached); see python_symbols.csv."
                        )
                        break
                    out.append(line)
                    budget.spend(line, 1)
            else:
                budget.omissions.append(
                    f"Top-level symbol listing for `{rel}` omitted entirely (packet size limit reached); "
                    f"see python_symbols.csv."
                )
        try:
            line_count = int(row.get("line_count") or 0)
        except ValueError:
            line_count = 0
        if line_count:
            status = _render_excerpt_block(root, rel, 1, line_count, budget, out, row.get("sha256", ""))
            if status == "too_large":
                explicit_conflicts.append(f"explicit file selector `{rel}` ({line_count} lines) does not fit")
        for r in top_level:
            _symbol_expansion(r, calls_rows, imports_rows, files_by_path, resolved, budget, out, note_focus_file,
                              communities_by_file)

    for res in symbol_resolutions:
        if res.status != "resolved":
            continue
        row = res.resolved_rows[0]
        rel = row["relative_path"]
        if not note_focus_file(rel):
            explicit_conflicts.append(
                f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit: "
                f"limits.max_files ({resolved.max_files}) reached"
            )
            continue
        out.append(_render_origin_header(f"Symbol: `{row['qualified_name']}` — `{rel}`", ["explicit_symbol_selector"]))
        status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
                                        files_by_path.get(rel, {}).get("sha256", ""))
        if status == "too_large":
            explicit_conflicts.append(f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit")
        _symbol_expansion(row, calls_rows, imports_rows, files_by_path, resolved, budget, out, note_focus_file,
                          communities_by_file)

    for res in line_resolutions:
        if res.status != "resolved":
            continue
        info = res.resolved_rows[0]
        rel = info["file"]
        if not note_focus_file(rel):
            explicit_conflicts.append(
                f"explicit line selector `{res.requested}` does not fit: limits.max_files ({resolved.max_files}) reached"
            )
            continue
        enclosing = [
            r for r in symbols_by_file.get(rel, [])
            if int(r["start_line"]) <= info["start"] <= int(r["end_line"]) and r["symbol_type"] != "module"
        ]
        out.append(_render_origin_header(f"Line selector: `{res.requested}`", ["explicit_line_selector"]))
        if enclosing:
            enclosing.sort(key=lambda r: int(r["end_line"]) - int(r["start_line"]))
            row = enclosing[0]
            out.append(f"Enclosing symbol: `{row['qualified_name']}` ({row['symbol_type']}, "
                       f"lines {row['start_line']}-{row['end_line']})\n")
            status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
                                            files_by_path.get(rel, {}).get("sha256", ""))
        else:
            start, end = max(1, info["start"] - 10), info["end"] + 10
            status = _render_excerpt_block(root, rel, start, end, budget, out, files_by_path.get(rel, {}).get("sha256", ""))
        if status == "too_large":
            explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit")

    if explicit_conflicts:
        # An explicit selection itself didn't fit -- either the token
        # budget or limits.max_files -- per contract this is a hard
        # conflict, not something to truncate silently.
        return None, [_res_to_dict(r) for r in all_resolutions], (
            "the requested explicit selector(s) do not fit within limits.max_estimated_tokens "
            f"({resolved.max_estimated_tokens}) / limits.max_files ({resolved.max_files}); increase the "
            f"relevant limit or narrow the request. Conflicts:\n"
            + "\n".join(f"  - {o}" for o in explicit_conflicts)
        )

    # --- Tier 2: exact search-term matches ---
    # Matches were already computed by resolve_search_terms() above (before
    # the strict-mode gate) -- reused here rather than re-scanning the
    # repository a second time.
    for term in resolved.search_terms:
        matches = search_matches_by_term.get(term, [])
        term_status = next((r.status for r in search_resolutions if r.requested == term), None)
        if term_status == "invalid":
            notice = f"\n_Search term `{term}` is not a valid regex; skipped._\n"
            if not budget.allow(notice, 1):
                # Unbounded per-term notices (e.g. a request with hundreds
                # of invalid regex terms) would otherwise bypass
                # limits.max_estimated_tokens entirely, same failure shape
                # as the earlier unbudgeted resolution-report finding.
                budget.omissions.append(
                    f"Invalid-regex notice for `{term}` omitted (packet size limit reached); "
                    f"see the resolution report."
                )
                continue
            out.append(notice)
            budget.spend(notice, 1)
            continue
        header = f"\n### Search: `{term}` ({len(matches)} match(es))\n_Included because: exact_search_match._\n"
        if not budget.allow(header, 1):
            budget.omissions.append(f"Search results for `{term}` omitted entirely (packet size limit reached).")
            continue
        out.append(header); budget.spend(header, 1)
        for rel, ln, text in matches:
            if not note_focus_file(rel):
                # note_focus_file enforces limits.max_files against the
                # *global* focus-file set shared across every selector/tier
                # in this packet, not just this one search term -- so the
                # cap holds even when different terms match different files.
                budget.omissions.append(f"Additional `{term}` matches omitted beyond limits.max_files ({resolved.max_files}).")
                break
            line_text = redact_secrets(text.strip()[:200])
            line = f"- `{rel}:{ln}` — `{line_text}`"
            if not budget.allow(line, 1):
                budget.omissions.append(f"Additional `{term}` matches omitted (packet size limit reached).")
                break
            out.append(line); budget.spend(line, 1)
    if stale_search_files:
        budget.omissions.append(f"{stale_search_files} file(s) changed on disk since the last scan and were "
                                 f"skipped for search terms; re-run scan.")

    if budget.omissions:
        # The omissions *list* itself is unbounded in memory (a request
        # that triggers hundreds of distinct omission reasons -- e.g. many
        # invalid regex search terms -- can produce hundreds of entries),
        # so rendering it must be budgeted the same way the resolution
        # report is: otherwise this section alone could bypass
        # limits.max_estimated_tokens. The full list is always available,
        # unbudgeted, in the packet_*.resolution.json sidecar's
        # "omissions" field.
        header = "\n## Omitted / unresolved\n"
        if budget.allow(header, 1):
            out.append(header)
            budget.spend(header, 1)
            omitted_omissions = 0
            for idx, o in enumerate(budget.omissions):
                line = f"- {o}"
                if not budget.allow(line, 1):
                    omitted_omissions = len(budget.omissions) - idx
                    break
                out.append(line)
                budget.spend(line, 1)
            if omitted_omissions:
                note = (f"- ... and {omitted_omissions} more omission(s) not listed here (packet size limit "
                        f"reached); see the accompanying packet_*.resolution.json's \"omissions\" field for "
                        f"the complete list.")
                if budget.allow(note, 1):
                    out.append(note)
                    budget.spend(note, 1)

    # (Unresolved/ambiguous selectors are reported once, in the "Selector
    # resolution report" section built into the header below -- not
    # repeated here.)

    git_info = get_git_info(root)
    header_lines = [
        "# Repo Context Packet (from packet_request.json)\n",
        f"- Root: `{root.resolve().name}`",
        f"- Question: {resolved.question}",
        f"- schema_version: {resolved.schema_version}",
        f"- Tool version: {TOOL_VERSION}",
        f"- Request file: `{request_path.name}` (sha256: `{request_hash[:16]}…`)",
    ]
    if git_info.get("available"):
        dirty = "dirty" if git_info.get("dirty") else ("clean" if git_info.get("dirty") is False else "unknown")
        header_lines.append(f"- Repository revision: `{git_info['commit']}` ({dirty} worktree)")
    else:
        header_lines.append("- Repository revision: not available (not a git repository, or git is not installed)")
    header_lines.append(
        f"- Limits: max_estimated_tokens={resolved.max_estimated_tokens}, max_files={resolved.max_files}, "
        f"max_hops={resolved.max_hops}"
    )

    # The resolution report scales with the *request*, not the source
    # repository (a request naming hundreds of missing/ambiguous
    # selectors could otherwise render an unbounded report regardless of
    # limits.max_estimated_tokens) -- charge it against the same budget
    # as everything else, with a count-of-omitted note rather than an
    # unbounded listing. The full, untruncated report is always available
    # in the accompanying packet_<name>.resolution.json sidecar.
    resolution_lines = ["## Selector resolution report\n"]
    omitted_selector_count = 0
    for idx, r in enumerate(all_resolutions):
        entry = [f"- {r.selector_type} `{r.requested}`: **{r.status}** — {r.detail}"]
        entry.extend(f"  - candidate: `{c}`" for c in r.candidates)
        entry_text = "\n".join(entry)
        if not budget.allow(entry_text, len(entry)):
            omitted_selector_count = len(all_resolutions) - idx
            break
        resolution_lines.extend(entry)
        budget.spend(entry_text, len(entry))
    if omitted_selector_count:
        note = (f"- ... and {omitted_selector_count} more selector(s) omitted from this report (packet size "
                f"limit reached); see the accompanying packet_*.resolution.json for the complete report.")
        if budget.allow(note, 1):
            resolution_lines.append(note)
            budget.spend(note, 1)
    resolution_lines.append("")

    # Computed last so it reflects the resolution report's own budget spend too.
    header_lines.append(
        f"- Estimated tokens used: ~{round(budget.chars_used / 4)} "
        f"(chars_used={budget.chars_used}/{budget.max_characters})\n"
    )
    header_lines.extend(resolution_lines)

    out.append("\n_Static analysis only. Call/import relationships above are candidates, not proof of runtime "
                "dispatch. See README.md in this output directory for full limitations._\n")

    text = "\n".join(header_lines) + "\n".join(out) + "\n"

    stem = sanitize_stem(name_override) if name_override else sanitize_stem(request_path.stem)
    packet_path = output_dir / "packets" / f"packet_{stem}.md"
    atomic_write_text(packet_path, text)

    resolution_report = [_res_to_dict(r) for r in all_resolutions]
    sidecar = {
        "tool_version": TOOL_VERSION,
        "schema_version": resolved.schema_version,
        "question": resolved.question,
        "request_file_sha256": request_hash,
        "git": git_info,
        "estimated_tokens_used": round(budget.chars_used / 4),
        "focus_files": focus_files,
        "omissions": budget.omissions,
        "resolution_report": resolution_report,
    }
    atomic_write_text(output_dir / "packets" / f"packet_{stem}.resolution.json",
                       json.dumps(sidecar, indent=2, sort_keys=True) + "\n")

    return packet_path, resolution_report, None


def _res_to_dict(r: SelectorResolution) -> dict:
    return {
        "selector_type": r.selector_type, "requested": r.requested,
        "status": r.status, "detail": r.detail, "candidates": r.candidates,
    }
