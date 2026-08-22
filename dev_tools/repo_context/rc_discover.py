"""Lightweight, deterministic discovery: natural-language question ->
grouped candidate selectors, to seed a `packet_request.json`.

This never answers the question and never merges channels into a scored
ranking -- it groups literal, deterministic matches by channel (path,
symbol name, docstring, exact terminology, structural neighbor, optional
Graphify) and leaves interpretation to the reader (human or LLM). Reuses
the same CSV indexes and read helpers the `packet` command already uses;
adds no new scan and no embeddings/LLM dependency.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import rc_graphify
from rc_common import atomic_write_text, get_git_info, sanitize_stem
from rc_packet import _load_csv, _safe_excerpt, _file_is_fresh, _bfs_callers, _bfs_callees

_STOPWORDS = {
    "the", "a", "an", "is", "are", "was", "were", "of", "in", "on", "for", "to", "and", "or",
    "why", "where", "which", "what", "how", "does", "do", "did", "code", "produce", "produces",
    "produced", "which", "that", "this", "with", "from", "one", "likely", "implementation",
}


def _terms(question: str) -> list:
    words = re.findall(r"[A-Za-z_][A-Za-z0-9_]{2,}", question)
    seen = []
    for w in words:
        lw = w.lower()
        if lw in _STOPWORDS or lw in seen:
            continue
        seen.append(lw)
    return seen


def run_discover(root: Path, output_dir: Path, question: str, max_per_channel: int = 15) -> tuple:
    """Writes `packets/discover_<stem>.md` (grouped report) and
    `packets/discover_<stem>.packet_request.json` (a draft request -- meant
    to be reviewed/edited, not run blindly). Returns (report_path, draft_request_path).
    """
    files_rows = _load_csv(output_dir / "file_inventory.csv")
    symbols_rows = _load_csv(output_dir / "python_symbols.csv")
    calls_rows = _load_csv(output_dir / "python_calls.csv")
    terms = _terms(question)

    path_matches = []
    for r in files_rows:
        if r.get("included") != "true":
            continue
        lower = r["relative_path"].lower()
        hit = [t for t in terms if t in lower]
        if hit:
            path_matches.append((r["relative_path"], hit))

    symbol_matches = []
    docstring_matches = []
    for r in symbols_rows:
        qn_lower = r["qualified_name"].lower()
        hit = [t for t in terms if t in qn_lower]
        if hit:
            symbol_matches.append((r["relative_path"], r["qualified_name"], int(r["start_line"]), hit))
        doc = (r.get("docstring_first_line") or "").lower()
        if doc:
            hit_doc = [t for t in terms if t in doc]
            if hit_doc:
                docstring_matches.append((r["relative_path"], r["qualified_name"], int(r["start_line"]), hit_doc))

    literal_phrase = question.strip().strip("?").strip()
    exact_terms = ([literal_phrase] if len(literal_phrase.split()) > 1 else []) + [t for t in terms if len(t) >= 5]
    exact_terms = list(dict.fromkeys(exact_terms))[:8]
    exact_matches = []
    for term in exact_terms:
        count = 0
        for r in files_rows:
            if count >= max_per_channel:
                break
            if r.get("included") != "true" or r.get("text_or_binary") == "binary":
                continue
            if not _file_is_fresh(root, r["relative_path"], r.get("sha256", "")):
                continue
            excerpt = _safe_excerpt(root, r["relative_path"], 1, 10_000_000)
            if excerpt is None:
                continue
            for ln, text in excerpt:
                if term in text:
                    exact_matches.append((term, r["relative_path"], ln))
                    count += 1
                    if count >= max_per_channel:
                        break

    test_matches = [pm for pm in path_matches if pm[0].startswith("tests/") or "/tests/" in f"/{pm[0].lower()}"]

    neighbor_lines = []
    for rel, qn, _ln, _hit in symbol_matches[:5]:
        for c in _bfs_callers(rel, qn, calls_rows, 1):
            if c["confidence"] != "unresolved":
                neighbor_lines.append(f"`{qn}` <- `{c['caller_symbol']}` (`{c['caller_file']}`:{c['line']})")
        for c in _bfs_callees(rel, qn, calls_rows, 1):
            if c["confidence"] != "unresolved":
                neighbor_lines.append(f"`{qn}` -> `{c['candidate_symbol']}` (`{c['candidate_file']}`)")

    git_info = get_git_info(root)
    communities_by_file, graphify_warnings = rc_graphify.load_graphify_communities(
        root, git_info.get("commit") if git_info.get("available") else None,
        current_dirty=git_info.get("dirty") if git_info.get("available") else None,
    )
    matched_files = {pm[0] for pm in path_matches} | {sm[0] for sm in symbol_matches}
    graphify_candidates = [(f, communities_by_file[f]) for f in sorted(matched_files) if f in communities_by_file]

    lines = [
        "# Discovery report\n",
        f"- Question: {question}",
        f"- Extracted terms: {', '.join(terms) if terms else '(none)'}",
        "- Channels below are literal, deterministic matches grouped by kind. They are **not** combined "
        "into a score or ranking; use judgement (or hand this report to an LLM) to pick selectors.\n",
    ]

    def section(title: str, items: list, render) -> None:
        lines.append(f"## {title} ({len(items)})\n")
        for item in items[:max_per_channel]:
            lines.append(render(item))
        if len(items) > max_per_channel:
            lines.append(f"- ... and {len(items) - max_per_channel} more (not shown; narrow the question or "
                          f"raise --max-per-channel)")
        if not items:
            lines.append("- (none)")
        lines.append("")

    section("Exact terminology matches", exact_matches, lambda m: f"- `{m[0]}` in `{m[1]}`:{m[2]}")
    section("Symbol-name matches", symbol_matches, lambda m: f"- `{m[1]}` in `{m[0]}`:{m[2]} (matched: {', '.join(m[3])})")
    section("Path matches", path_matches, lambda m: f"- `{m[0]}` (matched: {', '.join(m[1])})")
    section("Docstring matches", docstring_matches, lambda m: f"- `{m[1]}` in `{m[0]}`:{m[2]} (matched: {', '.join(m[3])})")
    section("Structural neighbors", neighbor_lines, lambda m: f"- {m}")
    section("Related tests", test_matches, lambda m: f"- `{m[0]}`")

    lines.append(f"## Graphify candidates ({len(graphify_candidates)})\n")
    for w in graphify_warnings:
        lines.append(f"- (unavailable) {w}")
    for f, comm in graphify_candidates[:max_per_channel]:
        lines.append(f"- `{f}` — {rc_graphify.format_communities(comm)}")
    if not graphify_candidates and not graphify_warnings:
        lines.append("- (none)")
    lines.append("")

    lines.append(
        "_This report suggests selectors; it does not answer the question. Review/edit the draft "
        "`packet_request.json` below, then run:_\n"
        "`python repo_context.py packet ROOT --output OUT --request <draft_request.json>`\n"
    )

    report_text = "\n".join(lines) + "\n"

    draft = {
        "schema_version": "1.0",
        "question": question,
        "selectors": {
            "files": [p for p, _ in path_matches[:5]],
            "symbols": [{"name": qn} for _, qn, _, _ in symbol_matches[:5]],
            "search_terms": terms[:5],
            "lines": [],
        },
        "expansion": {
            "include_callers": True, "include_callees": True, "include_imports": True,
            "include_related_tests": True, "max_hops": 1,
        },
        "limits": {"max_estimated_tokens": 12000, "max_files": 12},
    }

    stem = sanitize_stem(question[:60])
    report_path = output_dir / "packets" / f"discover_{stem}.md"
    request_path = output_dir / "packets" / f"discover_{stem}.packet_request.json"
    atomic_write_text(report_path, report_text)
    atomic_write_text(request_path, json.dumps(draft, indent=2, sort_keys=True) + "\n")
    return report_path, request_path
