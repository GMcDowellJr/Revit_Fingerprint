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
import signal
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import rc_graphify
from rc_common import (
    TOOL_VERSION, atomic_write_text, generated_output_exclude_paths, get_git_info, redact_secrets,
    sanitize_stem, sha256_text,
)
# Sibling-module reuse: these are the same read-only, budget-aware
# rendering primitives the direct --file/--symbol/--search/--line packet
# path already uses. Reusing them (rather than re-implementing excerpt
# extraction, freshness checks, and BFS caller/callee walks a second time)
# keeps both packet paths consistent.
from rc_packet import (
    Budget, _load_csv, _norm_rel, _file_is_fresh, _iter_safe_lines,
    _find_symbol_candidates, _bfs_callers, _bfs_callees, _candidate_tests_for_file,
)

SUPPORTED_SCHEMA_VERSIONS = {"1.0"}
MAX_QUESTION_LENGTH = 4000

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
    elif len(question) > MAX_QUESTION_LENGTH:
        # `question` is copied verbatim into every packet's header,
        # unbudgeted (it's small, fixed provenance, not user-supplied
        # excerpt content) -- without a cap here, an oversized value would
        # let a packet exceed limits.max_estimated_tokens entirely through
        # the header alone, the same failure shape as the earlier
        # unbudgeted-resolution-report/omissions findings.
        errors.append(f"'question' is too long ({len(question)} chars; max {MAX_QUESTION_LENGTH})")

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


_REGEX_SEARCH_TIMEOUT_SECONDS = 5.0


def _scan_term_matches(term: str, pattern: Optional["re.Pattern"], files_rows: list, root: Path,
                        collect_cap: int) -> tuple:
    term_matches = []
    stale = 0
    for frow in files_rows:
        if frow.get("included") != "true" or frow.get("text_or_binary") == "binary":
            continue
        if len(term_matches) >= collect_cap:
            break
        # A single common term (or a repetitive/generated file) can
        # otherwise produce an unbounded number of matches before
        # max_files or the packet budget is ever applied during Tier-2
        # rendering -- cap collection itself, the same bound the direct
        # `--search` path (rc_packet.py) already uses.
        if not _file_is_fresh(root, frow["relative_path"], frow.get("sha256", "")):
            stale += 1
            continue
        # Stream lines in rather than materializing up to 10 million of
        # them via _safe_excerpt() before any matching happens -- the
        # scanner deliberately keeps files over MAX_TEXT_READ_BYTES in the
        # inventory without ever reading them into memory whole (see
        # rc_scan.py), so a large included file could otherwise exhaust
        # memory searching for a term that appears once (or not at all).
        # _iter_safe_lines() evaluates and discards one line at a time,
        # so a hit early in the file (or collect_cap being reached) stops
        # reading the rest of it immediately.
        lines = _iter_safe_lines(root, frow["relative_path"], 1, 10_000_000)
        if lines is None:
            continue
        try:
            for ln, text in lines:
                hit = pattern.search(text) if pattern else (term in text)
                if hit:
                    term_matches.append((frow["relative_path"], ln, text))
                    if len(term_matches) >= collect_cap:
                        break
        finally:
            lines.close()
    return term_matches, stale


class _RegexSearchTimeout(Exception):
    pass


def _raise_regex_timeout(signum, frame) -> None:
    raise _RegexSearchTimeout()


_UNSUPPORTED_PLATFORM = "unsupported_platform"


def _scan_term_matches_bounded(term: str, pattern, files_rows: list, root: Path, collect_cap: int,
                                timeout: Optional[float] = None):
    """Runs _scan_term_matches with a wall-clock ceiling, to bound a
    pathological `search_as_regex` pattern's catastrophic-backtracking
    blowup (e.g. `(a+)+$` against a long, nearly-matching source line)
    instead of hanging the CLI indefinitely. Returns None on timeout, or
    the module-level _UNSUPPORTED_PLATFORM sentinel if this platform has
    no way to bound the match at all.

    Uses SIGALRM (POSIX only) rather than a background thread: CPython's
    regex engine checks for pending signals periodically even mid-match,
    so the alarm reliably interrupts a runaway match in the *same* thread.
    A background-thread timeout was tried first and rejected -- Python's
    `re` can't be safely killed once started, so an abandoned worker kept
    running and, worse, kept contending for the GIL, which starved the
    "recovered" main thread just as badly (confirmed: the whole process
    still hadn't returned after 60s in that version, despite the join()
    itself returning at 5s).

    On platforms without SIGALRM (Windows), there's no safe way to
    interrupt a C-level regex match from Python at all -- silently
    falling back to an unbounded scan there would let the exact same
    pathological pattern hang the CLI indefinitely on that platform, so
    this refuses the term instead (the caller reports it as rejected, not
    resolved) rather than risking the hang this whole mechanism exists to
    prevent."""
    if not hasattr(signal, "SIGALRM"):
        return _UNSUPPORTED_PLATFORM
    if timeout is None:
        timeout = _REGEX_SEARCH_TIMEOUT_SECONDS
    previous_handler = signal.signal(signal.SIGALRM, _raise_regex_timeout)
    signal.setitimer(signal.ITIMER_REAL, timeout)
    try:
        return _scan_term_matches(term, pattern, files_rows, root, collect_cap)
    except _RegexSearchTimeout:
        return None
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


_REGEX_SEARCH_TOTAL_TIMEOUT_SECONDS = 30.0


def resolve_search_terms(root: Path, terms: list, search_as_regex: bool, files_rows: list, max_files: int) -> tuple:
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
    # Same collection cap the direct --search path (rc_packet.py) already
    # uses -- a common term matching most of a large repository would
    # otherwise accumulate an unbounded number of tuples before max_files
    # or the packet budget is ever applied during Tier-2 rendering.
    collect_cap = max(1, max_files) * 5
    # The per-term SIGALRM bound (_scan_term_matches_bounded) stops any one
    # pathological regex pattern from hanging forever, but the schema
    # places no cap on how many search_terms a request can carry either
    # way -- a request with hundreds/thousands of absent *literal* terms
    # re-reads every included text file once per term (collect_cap only
    # bounds how many *matches* pile up, not how many scans happen when a
    # term matches nothing at all). Track one wall-clock deadline across
    # *all* terms in this call, regex or literal, and skip remaining terms
    # outright once it's passed, instead of granting every term its own
    # fresh scan unconditionally.
    search_deadline = time.monotonic() + _REGEX_SEARCH_TOTAL_TIMEOUT_SECONDS
    for term in terms:
        try:
            pattern = re.compile(term) if search_as_regex else None
        except re.error as exc:
            resolutions.append(SelectorResolution("search_term", term, "invalid", f"not a valid regex: {exc}"))
            matches_by_term[term] = []
            continue
        remaining = search_deadline - time.monotonic()
        if remaining <= 0:
            resolutions.append(SelectorResolution(
                "search_term", term, "invalid",
                f"skipped: this request's aggregate search evaluation time exceeded "
                f"{_REGEX_SEARCH_TOTAL_TIMEOUT_SECONDS:.0f}s across all its search_terms; reduce the "
                f"number of search_terms",
            ))
            matches_by_term[term] = []
            continue
        if search_as_regex:
            scanned = _scan_term_matches_bounded(term, pattern, files_rows, root, collect_cap,
                                                  timeout=min(_REGEX_SEARCH_TIMEOUT_SECONDS, remaining))
            if scanned == _UNSUPPORTED_PLATFORM:
                resolutions.append(SelectorResolution(
                    "search_term", term, "invalid",
                    "search_as_regex rejected: this platform has no way (SIGALRM/POSIX only) to safely "
                    "bound a pathological pattern's evaluation time, and running it unbounded risks "
                    "hanging the CLI indefinitely. Disable search_as_regex, or use a literal search_terms "
                    "value instead.",
                ))
                matches_by_term[term] = []
                continue
            if scanned is None:
                resolutions.append(SelectorResolution(
                    "search_term", term, "invalid",
                    f"regex evaluation exceeded {_REGEX_SEARCH_TIMEOUT_SECONDS:.0f}s (likely catastrophic "
                    f"backtracking); term skipped -- simplify the pattern or disable search_as_regex",
                ))
                matches_by_term[term] = []
                continue
            term_matches, stale = scanned
        else:
            term_matches, stale = _scan_term_matches(term, pattern, files_rows, root, collect_cap)
        stale_files_skipped += stale
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
    lines = _iter_safe_lines(root, rel_path, start, end)
    if lines is None:
        out.append("_Source excerpt unavailable (file missing or unreadable)._\n")
        budget.omissions.append(f"`{rel_path}` excerpt unavailable (file missing or unreadable).")
        return "unavailable"
    # Stream the requested range rather than materializing it whole up
    # front -- `end` can be an oversized file's real line_count (the
    # scanner deliberately keeps files over MAX_TEXT_READ_BYTES in the
    # inventory, with a real streamed-counted line_count, without ever
    # reading them into memory whole -- see rc_scan.py), so an explicit
    # file selector naming such a file could otherwise allocate hundreds
    # of megabytes or more just to learn the excerpt is "too_large".
    # Track the raw (pre-redaction) size as lines come in and bail out as
    # soon as it clearly exceeds the remaining budget, without reading
    # the rest of the file. redact_secrets() can grow a line slightly (a
    # short matched secret replaced by the fixed-length placeholder), so
    # this early check is deliberately against the raw size, not a
    # substitute for the exact post-redaction budget.allow() check below
    # -- it only ever *skips* reading further, never changes what content
    # that does get read is judged against.
    body_lines = []
    # Reserve room for the "```\n"/"\n```\n" fence markers up front too --
    # they're charged along with `body` below (see `fragment`).
    remaining_chars = budget.max_characters - budget.chars_used - len("```\n\n```\n")
    raw_chars = 0
    too_large = False
    try:
        for ln, text in lines:
            rendered = f"{ln:>6}| {text}"
            body_lines.append(rendered)
            raw_chars += len(rendered) + 1  # +1 for the joining newline
            if raw_chars > remaining_chars:
                too_large = True
                break
    finally:
        lines.close()
    if too_large:
        return "too_large"
    # Redact *before* the budget check, not after -- redact_secrets()
    # replaces a matched secret with a placeholder that can be longer
    # than the original text, so checking budget.allow() against the raw
    # body and only redacting afterward let the actually-written content
    # end up bigger than what was verified to fit.
    body = redact_secrets("\n".join(body_lines))
    # Charge the *rendered fragment actually appended to `out`* -- the
    # fenced code block, not just its inner `body` -- or the "```\n"/
    # "\n```\n" fence markers (9 chars) ride along uncounted on every
    # excerpt, letting the packet's true size creep past
    # limits.max_estimated_tokens by a few characters per excerpt.
    fragment = "```\n" + body + "\n```\n"
    if not budget.allow(fragment, len(body_lines)):
        return "too_large"
    out.append(fragment)
    budget.spend(fragment, len(body_lines))
    return "rendered"


def _symbol_expansion(row: dict, calls_rows: list, req: ResolvedRequest, budget: Budget, out: list,
                       note_focus_file) -> None:
    """Callers/callees are inherently per-symbol (each symbol has its own
    call graph neighborhood), unlike imports/related-tests/Graphify peers
    below in _file_expansion(), which describe the containing file and
    must not be repeated once per symbol in it."""
    rel, qn = row["relative_path"], row["qualified_name"]

    if req.include_callers:
        callers = [c for c in _bfs_callers(rel, qn, calls_rows, req.max_hops) if c["confidence"] != "unresolved"]
        if callers:
            header = f"\nCallers of `{qn}` (statically resolved, max_hops={req.max_hops}):\n"
            if budget.allow(header, 1):
                out.append(header); budget.spend(header, 1)
                max_files_note_emitted = False
                for c in callers:
                    line = (f"- `{c['caller_symbol']}` in `{c['caller_file']}`:{c['line']} "
                            f"— `{c['call_expression']}` ({c['confidence']}: {c['explanation']}) "
                            f"[origin: caller_expansion]\n")
                    # Budget-check before reserving a focus-file slot -- a
                    # caller entry that ultimately doesn't fit must not
                    # consume the slot on behalf of content that was never
                    # actually rendered.
                    if not budget.allow(line, 1):
                        budget.omissions.append(f"More callers of `{qn}` omitted (packet size limit reached); see python_calls.csv.")
                        break
                    if not note_focus_file(c["caller_file"]):
                        # A file beyond limits.max_files doesn't mean every
                        # *later* caller is unreachable too -- a later one
                        # may be in a file already in focus_files, which
                        # note_focus_file accepts for free. Skip this entry
                        # and keep checking the rest instead of abandoning
                        # the whole listing.
                        if not max_files_note_emitted:
                            budget.omissions.append(
                                f"Caller(s) of `{qn}` beyond limits.max_files ({req.max_files}) omitted."
                            )
                            max_files_note_emitted = True
                        continue
                    out.append(line); budget.spend(line, 1)
            else:
                budget.omissions.append(f"Callers listing for `{qn}` omitted entirely (packet size limit reached).")

    if req.include_callees:
        callees = [c for c in _bfs_callees(rel, qn, calls_rows, req.max_hops) if c["confidence"] != "unresolved"]
        if callees:
            header = f"\nCallees of `{qn}` (statically resolved, max_hops={req.max_hops}):\n"
            if budget.allow(header, 1):
                out.append(header); budget.spend(header, 1)
                max_files_note_emitted = False
                for c in callees:
                    line = (f"- `{c['call_expression']}` at line {c['line']} -> `{c['candidate_symbol']}` "
                            f"in `{c['candidate_file']}` ({c['confidence']}: {c['explanation']}) "
                            f"[origin: callee_expansion]\n")
                    if not budget.allow(line, 1):
                        budget.omissions.append(f"More callees of `{qn}` omitted (packet size limit reached); see python_calls.csv.")
                        break
                    if not note_focus_file(c["candidate_file"]):
                        if not max_files_note_emitted:
                            budget.omissions.append(
                                f"Callee(s) of `{qn}` beyond limits.max_files ({req.max_files}) omitted."
                            )
                            max_files_note_emitted = True
                        continue
                    out.append(line); budget.spend(line, 1)
            else:
                budget.omissions.append(f"Callees listing for `{qn}` omitted entirely (packet size limit reached).")


def _file_expansion(rel: str, imports_rows: list, calls_rows: list, files_by_path: dict,
                     req: ResolvedRequest, budget: Budget, out: list, note_focus_file,
                     communities_by_file: Optional[dict] = None) -> None:
    """Internal imports, related tests, and Graphify community peers all
    describe the *file*, not any one symbol in it -- unlike
    callers/callees above, which are inherently per-symbol. Must be
    called at most once per file regardless of how many of that file's
    symbols are being expanded (see the caller: it used to call this
    content once per top-level symbol, rendering identical "Internal
    imports of X"/"Related tests for X"/"Graphify community peers of X"
    sections over and over for a multi-symbol file)."""
    if req.include_imports:
        file_imports = [i for i in imports_rows if i["source_file"] == rel and i["resolved_file"]]
        if file_imports:
            header = f"\nInternal imports of `{rel}` (import_expansion):\n"
            if budget.allow(header, 1):
                out.append(header); budget.spend(header, 1)
                max_files_note_emitted = False
                for i in file_imports[:20]:
                    line = f"- line {i['line']}: `{i['imported_name'] or i['imported_module']}` -> `{i['resolved_file']}`\n"
                    if not budget.allow(line, 1):
                        break
                    if not note_focus_file(i["resolved_file"]):
                        if not max_files_note_emitted:
                            budget.omissions.append(
                                f"Import(s) of `{rel}` beyond limits.max_files ({req.max_files}) omitted."
                            )
                            max_files_note_emitted = True
                        continue
                    out.append(line); budget.spend(line, 1)

    if req.include_related_tests:
        tests = _candidate_tests_for_file(rel, imports_rows, calls_rows, files_by_path)
        if tests:
            header = f"\nRelated tests for `{rel}` (related_test_expansion):\n"
            if budget.allow(header, 1):
                out.append(header); budget.spend(header, 1)
                max_files_note_emitted = False
                for t in tests:
                    line = f"- `{t}`\n"
                    if not budget.allow(line, 1):
                        break
                    if not note_focus_file(t):
                        # Route through the same global-focus-file gate as
                        # every other tier -- a hard-coded high ceiling here
                        # would let related-test expansion silently bypass
                        # limits.max_files.
                        if not max_files_note_emitted:
                            budget.omissions.append(
                                f"Related test(s) for `{rel}` beyond limits.max_files ({req.max_files}) omitted."
                            )
                            max_files_note_emitted = True
                        continue
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
                    max_files_note_emitted = False
                    for p in peers:
                        line = f"- `{p}` [origin: graphify_expansion]\n"
                        if not budget.allow(line, 1):
                            budget.omissions.append(
                                f"More Graphify community peers of `{rel}` omitted (packet size limit reached)."
                            )
                            break
                        if not note_focus_file(p):
                            # Route through the same global focus-file gate
                            # as every other expansion tier -- otherwise a
                            # Graphify peer could silently exceed
                            # limits.max_files while the resolution
                            # sidecar's focus_files list stayed under it.
                            if not max_files_note_emitted:
                                budget.omissions.append(
                                    f"Graphify community peer(s) of `{rel}` beyond limits.max_files "
                                    f"({req.max_files}) omitted."
                                )
                                max_files_note_emitted = True
                            continue
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
        root, resolved.search_terms, resolved.search_as_regex, files_rows, resolved.max_files,
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
        _git_for_graphify = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
        communities_by_file, graphify_warnings_for_request = rc_graphify.load_graphify_communities(
            root, _git_for_graphify.get("commit") if _git_for_graphify.get("available") else None,
            current_dirty=_git_for_graphify.get("dirty") if _git_for_graphify.get("available") else None,
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

    # --- Reserve the fixed framing's budget cost up front ---
    # The header, the selector-resolution report, and the footer are
    # essential, always-rendered provenance -- not optional content -- so
    # none of them can be dropped to make room. But charging their cost
    # only after Tier-1/Tier-2 content had already spent against the full,
    # unreserved budget (as a prior version did) let Tier-1/Tier-2 content
    # spend as if framing were free: a request could "succeed" with
    # Tier-1 content that, combined with the framing charged on top
    # afterward, made the packet's *actual* rendered size exceed
    # limits.max_estimated_tokens even though generation reported success.
    # Reserving framing's cost first means a too-tight budget now
    # correctly surfaces as an explicit_conflicts abort (an explicit
    # selector's excerpt no longer fits once framing's real cost is
    # subtracted) instead of a "successful" packet whose true size is
    # larger than what was requested.
    git_info = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
    # Every entry below carries its own trailing "\n" -- header_lines is
    # assembled with plain concatenation ("".join), not "\n".join(), so
    # that the *number* of entries (which grows later: the "Estimated
    # tokens used" line, then every resolution-report line) can never
    # introduce an uncharged join-separator character. An earlier version
    # relied on "\n".join(header_lines) for spacing, which silently added
    # one character per entry that was never included in any budget.spend()
    # call -- harmless for the fixed initial entries (accounted for
    # correctly at the time header_text below was computed), but wrong
    # once more entries were extended in afterward.
    header_lines = [
        "# Repo Context Packet (from packet_request.json)\n",
        f"- Root: `{root.resolve().name}`\n",
        f"- Question: {resolved.question}\n",
        f"- schema_version: {resolved.schema_version}\n",
        f"- Tool version: {TOOL_VERSION}\n",
        f"- Request file: `{request_path.name}` (sha256: `{request_hash[:16]}…`)\n",
    ]
    if git_info.get("available"):
        dirty = "dirty" if git_info.get("dirty") else ("clean" if git_info.get("dirty") is False else "unknown")
        header_lines.append(f"- Repository revision: `{git_info['commit']}` ({dirty} worktree)\n")
    else:
        header_lines.append("- Repository revision: not available (not a git repository, or git is not installed)\n")
    header_lines.append(
        f"- Limits: max_estimated_tokens={resolved.max_estimated_tokens}, max_files={resolved.max_files}, "
        f"max_hops={resolved.max_hops}\n"
    )
    header_text = "".join(header_lines)

    # Same fixed-framing reasoning as the header -- always rendered in
    # full, reserved together with it (see below) so nothing else can
    # spend against budget the footer will also need.
    footer = ("\n_Static analysis only. Call/import relationships above are candidates, not proof of runtime "
              "dispatch. See README.md in this output directory for full limitations._\n")

    # The "Estimated tokens used" summary line (appended at the very end,
    # once Tier-1/Tier-2 are done) reports budget.chars_used itself -- its
    # own exact text isn't knowable up front, but its *worst-case width*
    # is: chars_used can never exceed budget.max_characters (every spend
    # of variable content is gated by budget.allow() first), so building
    # the placeholder with max_characters standing in for both the
    # rounded-token and raw-char figures is guaranteed to be at least as
    # wide as the real line will be. Reserving that placeholder now, atomically
    # with the header and footer, means the real line (substituted in
    # unchanged at the end, needing no separate spend) can never be the
    # thing that pushes the packet over budget.
    estimated_line_placeholder = (
        f"- Estimated tokens used: ~{resolved.max_estimated_tokens} "
        f"(chars_used={budget.max_characters}/{budget.max_characters})\n"
    )

    # The "## Selector resolution report" section heading is, like the
    # header/footer/summary line, always rendered in full regardless of
    # request content (every valid request resolves at least one
    # selector) -- reserve it in the same atomic check rather than as a
    # separate, easy-to-forget budget.spend() of its own.
    resolution_report_header = "## Selector resolution report\n"

    # Header, footer, the summary-line placeholder, and the resolution-
    # report heading must be reserved *together*, in one atomic check,
    # before anything else (including individual resolution-report
    # entries below) is allowed to spend -- reserving the header alone
    # first (an earlier version of this fix) still let a resolution-
    # report entry's own budget.allow() check pass against a budget that
    # hadn't yet accounted for the footer, so the footer's later
    # unconditional spend pushed the total over the cap anyway. All four
    # are mandatory and unshrinkable, so if they don't fit *together* in
    # the requested budget, no amount of Tier-1/Tier-2 selector content
    # could ever have fit either -- fail the request outright instead of
    # writing a packet whose true size exceeds what was asked for.
    framing_text = header_text + estimated_line_placeholder + resolution_report_header + footer
    if not budget.allow(framing_text, len(header_lines) + 3):
        return None, [_res_to_dict(r) for r in all_resolutions], (
            f"limits.max_estimated_tokens ({resolved.max_estimated_tokens}) is too small to fit this packet's "
            f"fixed framing (header + footer + packet-size summary line + resolution-report heading, before "
            f"any selector content or individual resolution-report entries) alone; increase "
            f"limits.max_estimated_tokens."
        )
    budget.spend(framing_text, len(header_lines) + 3)

    # The resolution report's *entries* scale with the *request* (a
    # request naming hundreds of missing/ambiguous selectors could
    # otherwise render an unbounded report regardless of
    # limits.max_estimated_tokens) -- charge each against the same budget
    # as everything else, with a count-of-omitted note rather than an
    # unbounded listing. The full, untruncated report is always available
    # in the accompanying packet_<name>.resolution.json sidecar. Computed
    # here (reserved up front, alongside the header/footer) rather than
    # after Tier-1/Tier-2 render, since it depends only on
    # `all_resolutions` (already resolved above), not on anything
    # Tier-1/Tier-2 produce.
    # Same self-terminated-line convention as header_lines above -- every
    # entry ends with its own "\n" so resolution_lines can be concatenated
    # (not "\n".join()'d) into header_lines without an uncharged separator
    # per entry. The section heading itself was already reserved above
    # (as part of framing_text), so it's included here only for
    # rendering, not spent a second time.
    resolution_lines = [resolution_report_header]
    omitted_selector_count = 0
    for idx, r in enumerate(all_resolutions):
        entry = [f"- {r.selector_type} `{r.requested}`: **{r.status}** — {r.detail}\n"]
        entry.extend(f"  - candidate: `{c}`\n" for c in r.candidates)
        entry_text = "".join(entry)
        if not budget.allow(entry_text, len(entry)):
            omitted_selector_count = len(all_resolutions) - idx
            break
        resolution_lines.extend(entry)
        budget.spend(entry_text, len(entry))
    if omitted_selector_count:
        note = (f"- ... and {omitted_selector_count} more selector(s) omitted from this report (packet size "
                f"limit reached); see the accompanying packet_*.resolution.json for the complete report.\n")
        if budget.allow(note, 1):
            resolution_lines.append(note)
            budget.spend(note, 1)

    # --- Tier 1: explicit selectors (never silently dropped) ---
    # explicit_conflicts collects only "the explicit excerpt itself doesn't
    # fit the budget" -- a hard, must-be-reported-not-truncated conflict.
    # Freshness withholding and non-mandatory expansions (callers/callees/
    # imports/tests) are still recorded as ordinary, non-fatal omissions on
    # `budget` and must NOT abort generation.
    #
    # Two passes, deliberately in this order:
    #   1. Render every explicit selector's own header/excerpt first (and
    #      only once per distinct target -- two selectors naming the same
    #      file/symbol/line render it a single time). None of this may be
    #      pre-empted by expansion content.
    #   2. Only once every explicit item has had its guaranteed shot at the
    #      budget do optional expansions (callers/callees/imports/tests/
    #      Graphify) spend whatever budget remains. Interleaving expansion
    #      spend between explicit items (the previous structure) let one
    #      selector's expansions manufacture a budget conflict for a later,
    #      otherwise-fitting explicit selector.
    explicit_conflicts: list = []
    rendered_files: set = set()
    rendered_symbols: set = set()
    rendered_lines: set = set()
    file_expansion_items: list = []    # [(rel, top_level_rows)]
    symbol_expansion_items: list = []  # [row]

    def _spend_header(header: str) -> bool:
        if not budget.allow(header, 2):
            return False
        out.append(header)
        budget.spend(header, 2)
        return True

    for res in file_resolutions:
        if res.status != "resolved":
            continue
        rel = res.requested
        if rel in rendered_files:
            # Duplicate selector for a file already attempted -- its first
            # occurrence's outcome (rendered, or a recorded conflict) is
            # final; re-attempting an identical selector would just repeat
            # the same header/budget work (or the same conflict message)
            # once per repeat, which is itself an unbounded-output shape
            # for a request naming the same selector many times over.
            continue
        rendered_files.add(rel)
        if not note_focus_file(rel):
            explicit_conflicts.append(
                f"explicit file selector `{rel}` does not fit: limits.max_files ({resolved.max_files}) reached"
            )
            continue
        row = res.resolved_rows[0]
        header = _render_origin_header(f"File: `{rel}`", ["explicit_file_selector"])
        if not _spend_header(header):
            explicit_conflicts.append(f"explicit file selector `{rel}` does not fit (header alone exceeds budget)")
            continue
        # Render the mandatory excerpt (the actual content the selector asked
        # for) before the optional top-level-symbols inventory, so the
        # inventory can never spend the shared budget ahead of the excerpt
        # itself and force it into an "explicit_conflicts" abort.
        try:
            line_count = int(row.get("line_count") or 0)
        except ValueError:
            line_count = 0
        if line_count:
            status = _render_excerpt_block(root, rel, 1, line_count, budget, out, row.get("sha256", ""))
            if status == "too_large":
                explicit_conflicts.append(f"explicit file selector `{rel}` ({line_count} lines) does not fit")
                continue
        top_level = sorted(
            [r for r in symbols_by_file.get(rel, []) if r["parent_symbol"] == "<module>" and r["symbol_type"] != "module"],
            key=lambda r: int(r["start_line"]),
        )
        # The "Top-level symbols:" inventory itself is optional metadata,
        # same as this symbol's caller/callee/etc. expansions -- deferred
        # to the pass below (after every explicit file/symbol/line
        # selector has had its guaranteed shot at the budget), so file A's
        # inventory can never spend budget that file B's own explicit
        # excerpt needed.
        file_expansion_items.append((rel, top_level))

    for res in symbol_resolutions:
        if res.status != "resolved":
            continue
        row = res.resolved_rows[0]
        rel = row["relative_path"]
        symbol_key = (rel, row["qualified_name"])
        if symbol_key in rendered_symbols:
            continue  # duplicate selector; first occurrence's outcome is final
        rendered_symbols.add(symbol_key)
        if not note_focus_file(rel):
            explicit_conflicts.append(
                f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit: "
                f"limits.max_files ({resolved.max_files}) reached"
            )
            continue
        header = _render_origin_header(f"Symbol: `{row['qualified_name']}` — `{rel}`", ["explicit_symbol_selector"])
        if not _spend_header(header):
            explicit_conflicts.append(
                f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit (header alone exceeds budget)"
            )
            continue
        status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
                                        files_by_path.get(rel, {}).get("sha256", ""))
        if status == "too_large":
            explicit_conflicts.append(f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit")
        symbol_expansion_items.append(row)

    for res in line_resolutions:
        if res.status != "resolved":
            continue
        info = res.resolved_rows[0]
        rel = info["file"]
        line_key = (rel, info["start"], info["end"])
        if line_key in rendered_lines:
            continue  # duplicate selector; first occurrence's outcome is final
        rendered_lines.add(line_key)
        if not note_focus_file(rel):
            explicit_conflicts.append(
                f"explicit line selector `{res.requested}` does not fit: limits.max_files ({resolved.max_files}) reached"
            )
            continue
        # Only substitute a smaller enclosing symbol when it contains
        # *both* endpoints of the requested range -- a symbol containing
        # just the start line (the old check) could be smaller than the
        # actual request (e.g. lines 2-7 where line 2's enclosing function
        # ends at line 2), silently truncating the rendered excerpt to
        # that symbol's own bounds while the resolution report still
        # claimed the full range was resolved.
        enclosing = [
            r for r in symbols_by_file.get(rel, [])
            if int(r["start_line"]) <= info["start"] and info["end"] <= int(r["end_line"])
            and r["symbol_type"] != "module"
        ]
        header = _render_origin_header(f"Line selector: `{res.requested}`", ["explicit_line_selector"])
        if not _spend_header(header):
            explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit (header alone exceeds budget)")
            continue
        if enclosing:
            enclosing.sort(key=lambda r: int(r["end_line"]) - int(r["start_line"]))
            row = enclosing[0]
            enclosing_line = (f"Enclosing symbol: `{row['qualified_name']}` ({row['symbol_type']}, "
                               f"lines {row['start_line']}-{row['end_line']})\n")
            # Metadata, not the mandatory excerpt itself -- budgeted like
            # every other optional line, with a non-fatal skip if it
            # doesn't fit rather than silently rendering it unbudgeted.
            if budget.allow(enclosing_line, 1):
                out.append(enclosing_line)
                budget.spend(enclosing_line, 1)
            else:
                budget.omissions.append(
                    f"Enclosing-symbol note for `{res.requested}` omitted (packet size limit reached)."
                )
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

    # Every explicit selector fit -- now spend whatever budget remains on
    # optional metadata/expansions (callers/callees/imports/tests/
    # Graphify, plus each explicit file selector's own "Top-level
    # symbols:" inventory). Done only now, not interleaved with tier-1's
    # rendering above, so a symbol's expansions -- or one file's inventory
    # -- can never manufacture a budget conflict for a later explicit
    # selector.
    for rel, top_level in file_expansion_items:
        if top_level:
            header = "Top-level symbols:\n"
            if budget.allow(header, 1):
                out.append(header)
                budget.spend(header, 1)
                for idx, r in enumerate(top_level):
                    line = f"- `{r['qualified_name']}` ({r['symbol_type']}, lines {r['start_line']}-{r['end_line']})\n"
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
    file_level_expansion_done: set = set()

    def _maybe_file_expansion(rel: str) -> None:
        # Once per file regardless of how many symbols in it get
        # expanded, and regardless of whether that file is reached via a
        # file selector's own top-level symbols or a distinct explicit
        # symbol selector in the same file.
        if rel in file_level_expansion_done:
            return
        file_level_expansion_done.add(rel)
        _file_expansion(rel, imports_rows, calls_rows, files_by_path, resolved, budget, out, note_focus_file,
                         communities_by_file)

    for rel, top_level in file_expansion_items:
        for r in top_level:
            _symbol_expansion(r, calls_rows, resolved, budget, out, note_focus_file)
        # File-level expansion (imports/related-tests/Graphify peers) must
        # run for every explicitly selected file, not just ones with
        # top-level symbols -- a symbol-free file (e.g. an __init__.py
        # re-export shim, or a plain config module) previously never got
        # its imports/related-tests/Graphify-peer expansion at all, since
        # this loop used to skip straight past it when `top_level` was
        # empty. Those expansions describe the file, not any symbol in
        # it, so they apply regardless of whether the file happens to
        # define any top-level symbols.
        _maybe_file_expansion(rel)
    for row in symbol_expansion_items:
        _symbol_expansion(row, calls_rows, resolved, budget, out, note_focus_file)
        _maybe_file_expansion(row["relative_path"])

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
        max_files_note_emitted = False
        for rel, ln, text in matches:
            # Redact the full line before truncating it, not after -- a
            # secret-shaped value whose closing quote falls beyond
            # character 200 would otherwise have that quote cut off
            # first, breaking _SECRET_ASSIGNMENT_PATTERN's closing-quote
            # backreference so redact_secrets() never matches and the
            # (truncated) secret prefix leaks into the packet.
            line_text = redact_secrets(text.strip())[:200]
            line = f"- `{rel}:{ln}` — `{line_text}`\n"
            # Check the budget *before* reserving a focus-file slot for
            # this match -- otherwise a match that ultimately doesn't fit
            # (and is never rendered) could still consume the one
            # remaining limits.max_files slot, starving a later, shorter
            # match from a different file that would have fit.
            if not budget.allow(line, 1):
                budget.omissions.append(f"Additional `{term}` matches omitted (packet size limit reached).")
                break
            if not note_focus_file(rel):
                # note_focus_file enforces limits.max_files against the
                # *global* focus-file set shared across every selector/tier
                # in this packet, not just this one search term -- so the
                # cap holds even when different terms match different files.
                # A match in a file beyond the cap doesn't mean every
                # *later* match is unreachable too -- a later match may be
                # in a file already in focus_files (e.g. the selected
                # file), which costs no new slot. Skip this one match and
                # keep checking the rest instead of abandoning the term.
                if not max_files_note_emitted:
                    budget.omissions.append(
                        f"Additional `{term}` match(es) beyond limits.max_files ({resolved.max_files}) omitted."
                    )
                    max_files_note_emitted = True
                continue
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
                line = f"- {o}\n"
                if not budget.allow(line, 1):
                    omitted_omissions = len(budget.omissions) - idx
                    break
                out.append(line)
                budget.spend(line, 1)
            if omitted_omissions:
                note = (f"- ... and {omitted_omissions} more omission(s) not listed here (packet size limit "
                        f"reached); see the accompanying packet_*.resolution.json's \"omissions\" field for "
                        f"the complete list.\n")
                if budget.allow(note, 1):
                    out.append(note)
                    budget.spend(note, 1)

    # (Unresolved/ambiguous selectors are reported once, in the "Selector
    # resolution report" section built into the header below -- not
    # repeated here. header_lines/resolution_lines/footer were built and
    # charged against the budget up front, before Tier-1/Tier-2 rendering
    # -- see the "Reserve the fixed framing's budget cost up front"
    # comment above.)

    # Computed last so it reflects Tier-1/Tier-2's and the resolution
    # report's/footer's own budget spend too. Its width was already
    # reserved (as a worst-case placeholder) atomically with the header/
    # footer above, so this real line -- guaranteed no wider than that
    # placeholder, since chars_used can never exceed max_characters --
    # needs no separate budget.spend() of its own.
    header_lines.append(
        f"- Estimated tokens used: ~{round(budget.chars_used / 4)} "
        f"(chars_used={budget.chars_used}/{budget.max_characters})\n"
    )
    header_lines.extend(resolution_lines)

    out.append(footer)

    # Plain concatenation, not "\n".join() -- every element of both lists
    # is self-terminated with its own "\n" (see the comments where
    # header_lines/resolution_lines/out entries are built), so no
    # separator character needs inserting between them. A join here would
    # silently add one uncharged character per element -- exactly the gap
    # that let a packet's true rendered size exceed limits.max_estimated_tokens
    # while the sidecar reported it fit.
    text = "".join(header_lines) + "".join(out) + "\n"

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
