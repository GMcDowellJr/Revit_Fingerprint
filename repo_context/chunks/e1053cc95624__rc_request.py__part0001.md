# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 1 of 3
- Original line range: 1-488
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: RequestError, ResolvedRequest, _is_safe_repo_relative_path, validate_request_dict, parse_and_validate_request, SelectorResolution, resolve_files, resolve_symbols, resolve_lines, _scan_term_matches, _RegexSearchTimeout, _raise_regex_timeout, _scan_term_matches_bounded, resolve_search_terms, _render_origin_header, _render_excerpt_block
- Source SHA-256: f8fc322e94f1d42391838800f006205cb1179854a0162063d062fbcc18f13f91
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| """packet_request.json: schema validation, deterministic selector
     2| resolution, and request-driven packet generation.
     3| 
     4| This is the "LLM produces packet_request.json -> repo_context validates
     5| selectors -> repo_context generates a bounded, source-backed evidence
     6| packet" half of the discovery-to-packet workflow described in
     7| schema/packet_request.schema.json. Nothing here executes repository code;
     8| selectors are resolved purely against the CSV indexes a prior `scan`
     9| already produced, plus read-only source excerpts.
    10| """
    11| from __future__ import annotations
    12| 
    13| import json
    14| import re
    15| import signal
    16| from dataclasses import dataclass, field
    17| from pathlib import Path
    18| from typing import Optional
    19| 
    20| import rc_graphify
    21| from rc_common import (
    22|     TOOL_VERSION, atomic_write_text, generated_output_exclude_paths, get_git_info, redact_secrets,
    23|     sanitize_stem, sha256_text,
    24| )
    25| # Sibling-module reuse: these are the same read-only, budget-aware
    26| # rendering primitives the direct --file/--symbol/--search/--line packet
    27| # path already uses. Reusing them (rather than re-implementing excerpt
    28| # extraction, freshness checks, and BFS caller/callee walks a second time)
    29| # keeps both packet paths consistent.
    30| from rc_packet import (
    31|     Budget, _load_csv, _norm_rel, _file_is_fresh, _safe_excerpt,
    32|     _find_symbol_candidates, _bfs_callers, _bfs_callees, _candidate_tests_for_file,
    33| )
    34| 
    35| SUPPORTED_SCHEMA_VERSIONS = {"1.0"}
    36| MAX_QUESTION_LENGTH = 4000
    37| 
    38| _PATH_SELECTOR_FIELDS = {"selectors.files[]", "selectors.symbols[].file", "selectors.lines[].file"}
    39| 
    40| 
    41| class RequestError(Exception):
    42|     """Raised for a structurally invalid request (schema errors). Distinct
    43|     from resolution issues (missing/ambiguous selectors), which are
    44|     reported per-selector instead of raising."""
    45| 
    46| 
    47| @dataclass
    48| class ResolvedRequest:
    49|     schema_version: str
    50|     question: str
    51|     strict: bool
    52|     files: list          # list[str] repo-relative paths, as given
    53|     symbols: list         # list[dict] {"name":..., "file": optional}
    54|     search_terms: list
    55|     lines: list           # list[dict] {"file":..., "line":..., "end_line": optional}
    56|     include_callers: bool
    57|     include_callees: bool
    58|     include_imports: bool
    59|     include_related_tests: bool
    60|     include_graphify: bool
    61|     search_as_regex: bool
    62|     max_hops: int
    63|     max_estimated_tokens: int
    64|     max_files: int
    65| 
    66| 
    67| def _is_safe_repo_relative_path(p: str) -> bool:
    68|     if not p or not isinstance(p, str):
    69|         return False
    70|     norm = p.replace("\\", "/")
    71|     if norm.startswith("/") or norm.startswith("~"):
    72|         return False
    73|     if re.match(r"^[A-Za-z]:", norm):  # drive letter (Windows absolute path)
    74|         return False
    75|     parts = norm.split("/")
    76|     if any(part == ".." for part in parts):
    77|         return False
    78|     return True
    79| 
    80| 
    81| def validate_request_dict(data: dict) -> list:
    82|     """Structural validation mirroring schema/packet_request.schema.json.
    83|     Returns a list of human-readable error strings (empty == valid). Does
    84|     NOT check selectors against the scanned repository -- that happens
    85|     during resolution, where a missing file/symbol is reported per-item
    86|     rather than failing the whole request.
    87|     """
    88|     errors = []
    89|     if not isinstance(data, dict):
    90|         return ["request must be a JSON object"]
    91| 
    92|     allowed_top = {"schema_version", "question", "strict", "selectors", "expansion", "limits"}
    93|     for key in data:
    94|         if key not in allowed_top:
    95|             errors.append(f"unknown top-level field: '{key}' (schema does not permit extension fields)")
    96| 
    97|     version = data.get("schema_version")
    98|     if version is None:
    99|         errors.append("missing required field: schema_version")
   100|     elif not isinstance(version, str):
   101|         # `in SUPPORTED_SCHEMA_VERSIONS` (a set) raises TypeError on an
   102|         # unhashable value (a list/dict) -- check the type explicitly
   103|         # first so malformed-but-valid JSON is reported as a normal
   104|         # validation error instead of crashing the CLI.
   105|         errors.append(f"'schema_version' must be a string, got {type(version).__name__}")
   106|     elif version not in SUPPORTED_SCHEMA_VERSIONS:
   107|         errors.append(f"unsupported schema_version: {version!r} (supported: {sorted(SUPPORTED_SCHEMA_VERSIONS)})")
   108| 
   109|     question = data.get("question")
   110|     if not isinstance(question, str) or not question.strip():
   111|         errors.append("missing or empty required field: question")
   112|     elif len(question) > MAX_QUESTION_LENGTH:
   113|         # `question` is copied verbatim into every packet's header,
   114|         # unbudgeted (it's small, fixed provenance, not user-supplied
   115|         # excerpt content) -- without a cap here, an oversized value would
   116|         # let a packet exceed limits.max_estimated_tokens entirely through
   117|         # the header alone, the same failure shape as the earlier
   118|         # unbudgeted-resolution-report/omissions findings.
   119|         errors.append(f"'question' is too long ({len(question)} chars; max {MAX_QUESTION_LENGTH})")
   120| 
   121|     if "strict" in data and not isinstance(data["strict"], bool):
   122|         errors.append("'strict' must be a boolean")
   123| 
   124|     selectors = data.get("selectors")
   125|     if selectors is None:
   126|         errors.append("missing required field: selectors")
   127|     elif not isinstance(selectors, dict):
   128|         errors.append("'selectors' must be an object")
   129|     else:
   130|         allowed_sel = {"files", "symbols", "search_terms", "lines"}
   131|         for key in selectors:
   132|             if key not in allowed_sel:
   133|                 errors.append(f"unknown field under 'selectors': '{key}'")
   134| 
   135|         files = selectors.get("files", [])
   136|         if not isinstance(files, list) or not all(isinstance(x, str) for x in files):
   137|             errors.append("'selectors.files' must be an array of strings")
   138|         else:
   139|             for p in files:
   140|                 if not p:
   141|                     errors.append("'selectors.files' contains an empty path")
   142|                 elif not _is_safe_repo_relative_path(p):
   143|                     errors.append(f"'selectors.files' contains a path outside the scanned repository: {p!r}")
   144| 
   145|         symbols = selectors.get("symbols", [])
   146|         if not isinstance(symbols, list):
   147|             errors.append("'selectors.symbols' must be an array")
   148|         else:
   149|             for i, s in enumerate(symbols):
   150|                 if not isinstance(s, dict) or "name" not in s or not isinstance(s.get("name"), str) or not s["name"]:
   151|                     errors.append(f"'selectors.symbols[{i}]' must be an object with a non-empty 'name'")
   152|                     continue
   153|                 extra = set(s.keys()) - {"name", "file"}
   154|                 if extra:
   155|                     errors.append(f"'selectors.symbols[{i}]' has unknown field(s): {sorted(extra)}")
   156|                 if "file" in s and (not isinstance(s["file"], str) or not _is_safe_repo_relative_path(s["file"])):
   157|                     errors.append(f"'selectors.symbols[{i}].file' is not a safe repo-relative path: {s.get('file')!r}")
   158| 
   159|         terms = selectors.get("search_terms", [])
   160|         if not isinstance(terms, list) or not all(isinstance(x, str) and x for x in terms):
   161|             errors.append("'selectors.search_terms' must be an array of non-empty strings")
   162| 
   163|         lines = selectors.get("lines", [])
   164|         if not isinstance(lines, list):
   165|             errors.append("'selectors.lines' must be an array")
   166|         else:
   167|             for i, ln in enumerate(lines):
   168|                 if not isinstance(ln, dict) or "file" not in ln or "line" not in ln:
   169|                     errors.append(f"'selectors.lines[{i}]' must be an object with 'file' and 'line'")
   170|                     continue
   171|                 extra = set(ln.keys()) - {"file", "line", "end_line"}
   172|                 if extra:
   173|                     errors.append(f"'selectors.lines[{i}]' has unknown field(s): {sorted(extra)}")
   174|                 if not isinstance(ln["file"], str) or not _is_safe_repo_relative_path(ln["file"]):
   175|                     errors.append(f"'selectors.lines[{i}].file' is not a safe repo-relative path: {ln.get('file')!r}")
   176|                 # bool is a subclass of int in Python (isinstance(True, int) is
   177|                 # True), so `"line": true` would otherwise pass as line 1 --
   178|                 # JSON Schema (and this contract) does not treat booleans as
   179|                 # integers, so exclude them explicitly.
   180|                 line_val = ln.get("line")
   181|                 line_ok = isinstance(line_val, int) and not isinstance(line_val, bool)
   182|                 if not line_ok or line_val < 1:
   183|                     errors.append(f"'selectors.lines[{i}].line' must be a positive integer")
   184|                 if "end_line" in ln:
   185|                     end_val = ln["end_line"]
   186|                     end_ok = isinstance(end_val, int) and not isinstance(end_val, bool)
   187|                     if not end_ok or end_val < 1:
   188|                         errors.append(f"'selectors.lines[{i}].end_line' must be a positive integer")
   189|                     elif line_ok and end_val < line_val:
   190|                         errors.append(f"'selectors.lines[{i}].end_line' ({end_val}) is before 'line' ({line_val})")
   191| 
   192|         if isinstance(selectors, dict) and not any(selectors.get(k) for k in allowed_sel):
   193|             errors.append("'selectors' must contain at least one non-empty selector list "
   194|                           "(files/symbols/search_terms/lines)")
   195| 
   196|     expansion = data.get("expansion", {})
   197|     if not isinstance(expansion, dict):
   198|         errors.append("'expansion' must be an object")
   199|     else:
   200|         allowed_exp = {"include_callers", "include_callees", "include_imports", "include_related_tests",
   201|                        "include_graphify", "search_as_regex", "max_hops"}
   202|         for key in expansion:
   203|             if key not in allowed_exp:
   204|                 errors.append(f"unknown field under 'expansion': '{key}'")
   205|         if "max_hops" in expansion:
   206|             mh = expansion["max_hops"]
   207|             if not isinstance(mh, int) or isinstance(mh, bool) or not (0 <= mh <= 5):
   208|                 errors.append("'expansion.max_hops' must be an integer between 0 and 5 "
   209|                               "(a deep expansion is bounded, not unlimited)")
   210|         for key in allowed_exp - {"max_hops"}:
   211|             if key in expansion and not isinstance(expansion[key], bool):
   212|                 errors.append(f"'expansion.{key}' must be a boolean")
   213| 
   214|     limits = data.get("limits", {})
   215|     if not isinstance(limits, dict):
   216|         errors.append("'limits' must be an object")
   217|     else:
   218|         allowed_lim = {"max_estimated_tokens", "max_files"}
   219|         for key in limits:
   220|             if key not in allowed_lim:
   221|                 errors.append(f"unknown field under 'limits': '{key}'")
   222|         for key in allowed_lim:
   223|             if key in limits and (not isinstance(limits[key], int) or isinstance(limits[key], bool) or limits[key] < 1):
   224|                 errors.append(f"'limits.{key}' must be a positive integer")
   225| 
   226|     return errors
   227| 
   228| 
   229| def parse_and_validate_request(text: str) -> tuple:
   230|     """Returns (ResolvedRequest, errors). If errors is non-empty, the
   231|     ResolvedRequest half is None."""
   232|     try:
   233|         data = json.loads(text)
   234|     except json.JSONDecodeError as exc:
   235|         return None, [f"request is not valid JSON: {exc}"]
   236| 
   237|     errors = validate_request_dict(data)
   238|     if errors:
   239|         return None, errors
   240| 
   241|     selectors = data.get("selectors", {})
   242|     expansion = data.get("expansion", {}) or {}
   243|     limits = data.get("limits", {}) or {}
   244| 
   245|     resolved = ResolvedRequest(
   246|         schema_version=data["schema_version"],
   247|         question=data["question"],
   248|         strict=bool(data.get("strict", False)),
   249|         files=[_norm_rel(p) for p in selectors.get("files", [])],
   250|         symbols=[{"name": s["name"], "file": _norm_rel(s["file"]) if s.get("file") else None}
   251|                  for s in selectors.get("symbols", [])],
   252|         search_terms=list(selectors.get("search_terms", [])),
   253|         lines=[{"file": _norm_rel(ln["file"]), "line": ln["line"], "end_line": ln.get("end_line", ln["line"])}
   254|                for ln in selectors.get("lines", [])],
   255|         include_callers=bool(expansion.get("include_callers", True)),
   256|         include_callees=bool(expansion.get("include_callees", True)),
   257|         include_imports=bool(expansion.get("include_imports", True)),
   258|         include_related_tests=bool(expansion.get("include_related_tests", True)),
   259|         include_graphify=bool(expansion.get("include_graphify", False)),
   260|         search_as_regex=bool(expansion.get("search_as_regex", False)),
   261|         max_hops=int(expansion.get("max_hops", 1)),
   262|         max_estimated_tokens=int(limits.get("max_estimated_tokens", 12000)),
   263|         max_files=int(limits.get("max_files", 12)),
   264|     )
   265|     return resolved, []
   266| 
   267| 
   268| # --- Selector resolution -------------------------------------------------
   269| 
   270| @dataclass
   271| class SelectorResolution:
   272|     selector_type: str   # "file" | "symbol" | "line" | "search_term"
   273|     requested: str
   274|     status: str           # "resolved" | "ambiguous" | "missing" | "invalid"
   275|     detail: str
   276|     candidates: list = field(default_factory=list)
   277|     resolved_rows: list = field(default_factory=list)  # the matched row(s) for "resolved"
   278| 
   279| 
   280| def resolve_files(files: list, files_by_path: dict) -> list:
   281|     out = []
   282|     for p in files:
   283|         row = files_by_path.get(p)
   284|         if row is None:
   285|             out.append(SelectorResolution("file", p, "missing", f"'{p}' not found in file_inventory.csv"))
   286|         elif row.get("included") != "true":
   287|             out.append(SelectorResolution("file", p, "missing",
   288|                                            f"'{p}' was excluded from the scan (reason: {row.get('exclusion_reason')})"))
   289|         else:
   290|             out.append(SelectorResolution("file", p, "resolved", f"matched included file '{p}'", resolved_rows=[row]))
   291|     return out
   292| 
   293| 
   294| def resolve_symbols(symbols: list, symbols_rows: list) -> list:
   295|     out = []
   296|     for sel in symbols:
   297|         name, file_constraint = sel["name"], sel["file"]
   298|         candidates = _find_symbol_candidates(name, symbols_rows, file_constraint)
   299|         if not candidates:
   300|             out.append(SelectorResolution("symbol", name, "missing",
   301|                                            f"no symbol matching '{name}'"
   302|                                            + (f" in '{file_constraint}'" if file_constraint else "")
   303|                                            + " was found in python_symbols.csv"))
   304|         elif len(candidates) > 1:
   305|             alts = [f"{c['qualified_name']} ({c['relative_path']}:{c['start_line']}-{c['end_line']})" for c in candidates]
   306|             out.append(SelectorResolution(
   307|                 "symbol", name, "ambiguous",
   308|                 f"'{name}' matches {len(candidates)} symbols; qualify with a fully-qualified name or a 'file' field",
   309|                 candidates=alts,
   310|             ))
   311|         else:
   312|             row = candidates[0]
   313|             out.append(SelectorResolution("symbol", name, "resolved",
   314|                                            f"resolved to '{row['qualified_name']}' in '{row['relative_path']}'",
   315|                                            resolved_rows=[row]))
   316|     return out
   317| 
   318| 
   319| def resolve_lines(lines: list, files_by_path: dict) -> list:
   320|     out = []
   321|     for ln in lines:
   322|         f, start, end = ln["file"], ln["line"], ln["end_line"]
   323|         label = f"{f}:{start}" + (f"-{end}" if end != start else "")
   324|         row = files_by_path.get(f)
   325|         if row is None:
   326|             out.append(SelectorResolution("line", label, "missing", f"'{f}' not found in file_inventory.csv"))
   327|             continue
   328|         if row.get("included") != "true":
   329|             out.append(SelectorResolution("line", label, "missing",
   330|                                            f"'{f}' was excluded from the scan (reason: {row.get('exclusion_reason')})"))
   331|             continue
   332|         line_count = row.get("line_count")
   333|         if line_count and line_count.isdigit() and end > int(line_count):
   334|             out.append(SelectorResolution("line", label, "invalid",
   335|                                            f"end line {end} exceeds '{f}' line count ({line_count})"))
   336|             continue
   337|         out.append(SelectorResolution("line", label, "resolved", f"resolved to '{f}' lines {start}-{end}",
   338|                                        resolved_rows=[{"file": f, "start": start, "end": end}]))
   339|     return out
   340| 
   341| 
   342| _REGEX_SEARCH_TIMEOUT_SECONDS = 5.0
   343| 
   344| 
   345| def _scan_term_matches(term: str, pattern: Optional["re.Pattern"], files_rows: list, root: Path) -> tuple:
   346|     term_matches = []
   347|     stale = 0
   348|     for frow in files_rows:
   349|         if frow.get("included") != "true" or frow.get("text_or_binary") == "binary":
   350|             continue
   351|         if not _file_is_fresh(root, frow["relative_path"], frow.get("sha256", "")):
   352|             stale += 1
   353|             continue
   354|         excerpt = _safe_excerpt(root, frow["relative_path"], 1, 10_000_000)
   355|         if excerpt is None:
   356|             continue
   357|         for ln, text in excerpt:
   358|             hit = pattern.search(text) if pattern else (term in text)
   359|             if hit:
   360|                 term_matches.append((frow["relative_path"], ln, text))
   361|     return term_matches, stale
   362| 
   363| 
   364| class _RegexSearchTimeout(Exception):
   365|     pass
   366| 
   367| 
   368| def _raise_regex_timeout(signum, frame) -> None:
   369|     raise _RegexSearchTimeout()
   370| 
   371| 
   372| def _scan_term_matches_bounded(term: str, pattern, files_rows: list, root: Path,
   373|                                 timeout: Optional[float] = None):
   374|     """Runs _scan_term_matches with a wall-clock ceiling, to bound a
   375|     pathological `search_as_regex` pattern's catastrophic-backtracking
   376|     blowup (e.g. `(a+)+$` against a long, nearly-matching source line)
   377|     instead of hanging the CLI indefinitely. Returns None on timeout.
   378| 
   379|     Uses SIGALRM (POSIX only) rather than a background thread: CPython's
   380|     regex engine checks for pending signals periodically even mid-match,
   381|     so the alarm reliably interrupts a runaway match in the *same* thread.
   382|     A background-thread timeout was tried first and rejected -- Python's
   383|     `re` can't be safely killed once started, so an abandoned worker kept
   384|     running and, worse, kept contending for the GIL, which starved the
   385|     "recovered" main thread just as badly (confirmed: the whole process
   386|     still hadn't returned after 60s in that version, despite the join()
   387|     itself returning at 5s).
   388| 
   389|     On platforms without SIGALRM (Windows), there's no safe way to
   390|     interrupt a C-level regex match from Python at all, so this runs
   391|     unbounded there -- the same behavior as before this fix, not a
   392|     regression, just a gap this fix doesn't close on that platform."""
   393|     if timeout is None:
   394|         timeout = _REGEX_SEARCH_TIMEOUT_SECONDS
   395|     if not hasattr(signal, "SIGALRM"):
   396|         return _scan_term_matches(term, pattern, files_rows, root)
   397|     previous_handler = signal.signal(signal.SIGALRM, _raise_regex_timeout)
   398|     signal.setitimer(signal.ITIMER_REAL, timeout)
   399|     try:
   400|         return _scan_term_matches(term, pattern, files_rows, root)
   401|     except _RegexSearchTimeout:
   402|         return None
   403|     finally:
   404|         signal.setitimer(signal.ITIMER_REAL, 0)
   405|         signal.signal(signal.SIGALRM, previous_handler)
   406| 
   407| 
   408| def resolve_search_terms(root: Path, terms: list, search_as_regex: bool, files_rows: list) -> tuple:
   409|     """Resolve each search term against the scanned repository's current
   410|     source (same matching rule Tier 2 rendering uses). Returns
   411|     (resolutions: list[SelectorResolution], matches_by_term: dict[str,
   412|     list[(rel_path, line, text)]], stale_files_skipped: int).
   413| 
   414|     Done up front (before the strict-mode gate) so an invalid regex or a
   415|     zero-match term is visible to strict mode exactly like a missing file
   416|     or ambiguous symbol -- search terms were previously invisible to
   417|     `all_resolutions` entirely, so strict mode could not catch them.
   418|     """
   419|     resolutions = []
   420|     matches_by_term: dict = {}
   421|     stale_files_skipped = 0
   422|     for term in terms:
   423|         try:
   424|             pattern = re.compile(term) if search_as_regex else None
   425|         except re.error as exc:
   426|             resolutions.append(SelectorResolution("search_term", term, "invalid", f"not a valid regex: {exc}"))
   427|             matches_by_term[term] = []
   428|             continue
   429|         if search_as_regex:
   430|             scanned = _scan_term_matches_bounded(term, pattern, files_rows, root)
   431|             if scanned is None:
   432|                 resolutions.append(SelectorResolution(
   433|                     "search_term", term, "invalid",
   434|                     f"regex evaluation exceeded {_REGEX_SEARCH_TIMEOUT_SECONDS:.0f}s (likely catastrophic "
   435|                     f"backtracking); term skipped -- simplify the pattern or disable search_as_regex",
   436|                 ))
   437|                 matches_by_term[term] = []
   438|                 continue
   439|             term_matches, stale = scanned
   440|         else:
   441|             term_matches, stale = _scan_term_matches(term, pattern, files_rows, root)
   442|         stale_files_skipped += stale
   443|         matches_by_term[term] = term_matches
   444|         if term_matches:
   445|             resolutions.append(SelectorResolution("search_term", term, "resolved",
   446|                                                     f"{len(term_matches)} match(es) found"))
   447|         else:
   448|             resolutions.append(SelectorResolution("search_term", term, "missing", "no matches found"))
   449|     return resolutions, matches_by_term, stale_files_skipped
   450| 
   451| 
   452| # --- Rendering ------------------------------------------------------------
   453| 
   454| def _render_origin_header(title: str, origins: list) -> str:
   455|     return f"\n### {title}\n_Included because: {', '.join(origins)}._\n"
   456| 
   457| 
   458| def _render_excerpt_block(root: Path, rel_path: str, start: int, end: int, budget: Budget, out: list,
   459|                            expected_sha256: str) -> str:
   460|     """Returns "rendered", "stale" (withheld -- source changed since scan),
   461|     "unavailable" (file missing/unreadable), or "too_large" (would not fit
   462|     within the remaining budget). Only "too_large" is a hard, must-not-be-
   463|     silently-dropped conflict for an *explicit* selector -- "stale" and
   464|     "unavailable" are reported as ordinary (non-fatal) omissions, matching
   465|     the direct --file/--symbol/--line packet path's existing behavior."""
   466|     if not _file_is_fresh(root, rel_path, expected_sha256):
   467|         msg = (f"_Source excerpt withheld: `{rel_path}` has changed on disk since the last `scan` "
   468|                f"(SHA-256 mismatch); re-run `scan` for an up-to-date packet._\n")
   469|         if budget.allow(msg, 1):
   470|             out.append(msg)
   471|             budget.spend(msg, 1)
   472|         budget.omissions.append(f"`{rel_path}` changed since the last scan; excerpt withheld. Re-run scan.")
   473|         return "stale"
   474|     excerpt = _safe_excerpt(root, rel_path, start, end)
   475|     if excerpt is None:
   476|         out.append("_Source excerpt unavailable (file missing or unreadable)._\n")
   477|         budget.omissions.append(f"`{rel_path}` excerpt unavailable (file missing or unreadable).")
   478|         return "unavailable"
   479|     body_lines = [f"{ln:>6}| {text}" for ln, text in excerpt]
   480|     body = "\n".join(body_lines)
   481|     if not budget.allow(body, len(body_lines)):
   482|         return "too_large"
   483|     body = redact_secrets(body)
   484|     out.append("```\n" + body + "\n```\n")
   485|     budget.spend(body, len(body_lines))
   486|     return "rendered"
   487| 
   488| 
```
