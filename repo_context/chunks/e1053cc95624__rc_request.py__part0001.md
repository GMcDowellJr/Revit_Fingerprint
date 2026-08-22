# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 1 of 3
- Original line range: 1-512
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: RequestError, ResolvedRequest, _is_safe_repo_relative_path, validate_request_dict, parse_and_validate_request, SelectorResolution, resolve_files, resolve_symbols, resolve_lines, _scan_term_matches, _RegexSearchTimeout, _raise_regex_timeout, _scan_term_matches_bounded, resolve_search_terms, _render_origin_header, _render_excerpt_block
- Source SHA-256: 9afae857c0fde058a6c80995f21f1d847647d7b327a1e3e9e5318ec40212b388
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
    16| import time
    17| from dataclasses import dataclass, field
    18| from pathlib import Path
    19| from typing import Optional
    20| 
    21| import rc_graphify
    22| from rc_common import (
    23|     TOOL_VERSION, atomic_write_text, generated_output_exclude_paths, get_git_info, redact_secrets,
    24|     sanitize_stem, sha256_text,
    25| )
    26| # Sibling-module reuse: these are the same read-only, budget-aware
    27| # rendering primitives the direct --file/--symbol/--search/--line packet
    28| # path already uses. Reusing them (rather than re-implementing excerpt
    29| # extraction, freshness checks, and BFS caller/callee walks a second time)
    30| # keeps both packet paths consistent.
    31| from rc_packet import (
    32|     Budget, _load_csv, _norm_rel, _file_is_fresh, _safe_excerpt,
    33|     _find_symbol_candidates, _bfs_callers, _bfs_callees, _candidate_tests_for_file,
    34| )
    35| 
    36| SUPPORTED_SCHEMA_VERSIONS = {"1.0"}
    37| MAX_QUESTION_LENGTH = 4000
    38| 
    39| _PATH_SELECTOR_FIELDS = {"selectors.files[]", "selectors.symbols[].file", "selectors.lines[].file"}
    40| 
    41| 
    42| class RequestError(Exception):
    43|     """Raised for a structurally invalid request (schema errors). Distinct
    44|     from resolution issues (missing/ambiguous selectors), which are
    45|     reported per-selector instead of raising."""
    46| 
    47| 
    48| @dataclass
    49| class ResolvedRequest:
    50|     schema_version: str
    51|     question: str
    52|     strict: bool
    53|     files: list          # list[str] repo-relative paths, as given
    54|     symbols: list         # list[dict] {"name":..., "file": optional}
    55|     search_terms: list
    56|     lines: list           # list[dict] {"file":..., "line":..., "end_line": optional}
    57|     include_callers: bool
    58|     include_callees: bool
    59|     include_imports: bool
    60|     include_related_tests: bool
    61|     include_graphify: bool
    62|     search_as_regex: bool
    63|     max_hops: int
    64|     max_estimated_tokens: int
    65|     max_files: int
    66| 
    67| 
    68| def _is_safe_repo_relative_path(p: str) -> bool:
    69|     if not p or not isinstance(p, str):
    70|         return False
    71|     norm = p.replace("\\", "/")
    72|     if norm.startswith("/") or norm.startswith("~"):
    73|         return False
    74|     if re.match(r"^[A-Za-z]:", norm):  # drive letter (Windows absolute path)
    75|         return False
    76|     parts = norm.split("/")
    77|     if any(part == ".." for part in parts):
    78|         return False
    79|     return True
    80| 
    81| 
    82| def validate_request_dict(data: dict) -> list:
    83|     """Structural validation mirroring schema/packet_request.schema.json.
    84|     Returns a list of human-readable error strings (empty == valid). Does
    85|     NOT check selectors against the scanned repository -- that happens
    86|     during resolution, where a missing file/symbol is reported per-item
    87|     rather than failing the whole request.
    88|     """
    89|     errors = []
    90|     if not isinstance(data, dict):
    91|         return ["request must be a JSON object"]
    92| 
    93|     allowed_top = {"schema_version", "question", "strict", "selectors", "expansion", "limits"}
    94|     for key in data:
    95|         if key not in allowed_top:
    96|             errors.append(f"unknown top-level field: '{key}' (schema does not permit extension fields)")
    97| 
    98|     version = data.get("schema_version")
    99|     if version is None:
   100|         errors.append("missing required field: schema_version")
   101|     elif not isinstance(version, str):
   102|         # `in SUPPORTED_SCHEMA_VERSIONS` (a set) raises TypeError on an
   103|         # unhashable value (a list/dict) -- check the type explicitly
   104|         # first so malformed-but-valid JSON is reported as a normal
   105|         # validation error instead of crashing the CLI.
   106|         errors.append(f"'schema_version' must be a string, got {type(version).__name__}")
   107|     elif version not in SUPPORTED_SCHEMA_VERSIONS:
   108|         errors.append(f"unsupported schema_version: {version!r} (supported: {sorted(SUPPORTED_SCHEMA_VERSIONS)})")
   109| 
   110|     question = data.get("question")
   111|     if not isinstance(question, str) or not question.strip():
   112|         errors.append("missing or empty required field: question")
   113|     elif len(question) > MAX_QUESTION_LENGTH:
   114|         # `question` is copied verbatim into every packet's header,
   115|         # unbudgeted (it's small, fixed provenance, not user-supplied
   116|         # excerpt content) -- without a cap here, an oversized value would
   117|         # let a packet exceed limits.max_estimated_tokens entirely through
   118|         # the header alone, the same failure shape as the earlier
   119|         # unbudgeted-resolution-report/omissions findings.
   120|         errors.append(f"'question' is too long ({len(question)} chars; max {MAX_QUESTION_LENGTH})")
   121| 
   122|     if "strict" in data and not isinstance(data["strict"], bool):
   123|         errors.append("'strict' must be a boolean")
   124| 
   125|     selectors = data.get("selectors")
   126|     if selectors is None:
   127|         errors.append("missing required field: selectors")
   128|     elif not isinstance(selectors, dict):
   129|         errors.append("'selectors' must be an object")
   130|     else:
   131|         allowed_sel = {"files", "symbols", "search_terms", "lines"}
   132|         for key in selectors:
   133|             if key not in allowed_sel:
   134|                 errors.append(f"unknown field under 'selectors': '{key}'")
   135| 
   136|         files = selectors.get("files", [])
   137|         if not isinstance(files, list) or not all(isinstance(x, str) for x in files):
   138|             errors.append("'selectors.files' must be an array of strings")
   139|         else:
   140|             for p in files:
   141|                 if not p:
   142|                     errors.append("'selectors.files' contains an empty path")
   143|                 elif not _is_safe_repo_relative_path(p):
   144|                     errors.append(f"'selectors.files' contains a path outside the scanned repository: {p!r}")
   145| 
   146|         symbols = selectors.get("symbols", [])
   147|         if not isinstance(symbols, list):
   148|             errors.append("'selectors.symbols' must be an array")
   149|         else:
   150|             for i, s in enumerate(symbols):
   151|                 if not isinstance(s, dict) or "name" not in s or not isinstance(s.get("name"), str) or not s["name"]:
   152|                     errors.append(f"'selectors.symbols[{i}]' must be an object with a non-empty 'name'")
   153|                     continue
   154|                 extra = set(s.keys()) - {"name", "file"}
   155|                 if extra:
   156|                     errors.append(f"'selectors.symbols[{i}]' has unknown field(s): {sorted(extra)}")
   157|                 if "file" in s and (not isinstance(s["file"], str) or not _is_safe_repo_relative_path(s["file"])):
   158|                     errors.append(f"'selectors.symbols[{i}].file' is not a safe repo-relative path: {s.get('file')!r}")
   159| 
   160|         terms = selectors.get("search_terms", [])
   161|         if not isinstance(terms, list) or not all(isinstance(x, str) and x for x in terms):
   162|             errors.append("'selectors.search_terms' must be an array of non-empty strings")
   163| 
   164|         lines = selectors.get("lines", [])
   165|         if not isinstance(lines, list):
   166|             errors.append("'selectors.lines' must be an array")
   167|         else:
   168|             for i, ln in enumerate(lines):
   169|                 if not isinstance(ln, dict) or "file" not in ln or "line" not in ln:
   170|                     errors.append(f"'selectors.lines[{i}]' must be an object with 'file' and 'line'")
   171|                     continue
   172|                 extra = set(ln.keys()) - {"file", "line", "end_line"}
   173|                 if extra:
   174|                     errors.append(f"'selectors.lines[{i}]' has unknown field(s): {sorted(extra)}")
   175|                 if not isinstance(ln["file"], str) or not _is_safe_repo_relative_path(ln["file"]):
   176|                     errors.append(f"'selectors.lines[{i}].file' is not a safe repo-relative path: {ln.get('file')!r}")
   177|                 # bool is a subclass of int in Python (isinstance(True, int) is
   178|                 # True), so `"line": true` would otherwise pass as line 1 --
   179|                 # JSON Schema (and this contract) does not treat booleans as
   180|                 # integers, so exclude them explicitly.
   181|                 line_val = ln.get("line")
   182|                 line_ok = isinstance(line_val, int) and not isinstance(line_val, bool)
   183|                 if not line_ok or line_val < 1:
   184|                     errors.append(f"'selectors.lines[{i}].line' must be a positive integer")
   185|                 if "end_line" in ln:
   186|                     end_val = ln["end_line"]
   187|                     end_ok = isinstance(end_val, int) and not isinstance(end_val, bool)
   188|                     if not end_ok or end_val < 1:
   189|                         errors.append(f"'selectors.lines[{i}].end_line' must be a positive integer")
   190|                     elif line_ok and end_val < line_val:
   191|                         errors.append(f"'selectors.lines[{i}].end_line' ({end_val}) is before 'line' ({line_val})")
   192| 
   193|         if isinstance(selectors, dict) and not any(selectors.get(k) for k in allowed_sel):
   194|             errors.append("'selectors' must contain at least one non-empty selector list "
   195|                           "(files/symbols/search_terms/lines)")
   196| 
   197|     expansion = data.get("expansion", {})
   198|     if not isinstance(expansion, dict):
   199|         errors.append("'expansion' must be an object")
   200|     else:
   201|         allowed_exp = {"include_callers", "include_callees", "include_imports", "include_related_tests",
   202|                        "include_graphify", "search_as_regex", "max_hops"}
   203|         for key in expansion:
   204|             if key not in allowed_exp:
   205|                 errors.append(f"unknown field under 'expansion': '{key}'")
   206|         if "max_hops" in expansion:
   207|             mh = expansion["max_hops"]
   208|             if not isinstance(mh, int) or isinstance(mh, bool) or not (0 <= mh <= 5):
   209|                 errors.append("'expansion.max_hops' must be an integer between 0 and 5 "
   210|                               "(a deep expansion is bounded, not unlimited)")
   211|         for key in allowed_exp - {"max_hops"}:
   212|             if key in expansion and not isinstance(expansion[key], bool):
   213|                 errors.append(f"'expansion.{key}' must be a boolean")
   214| 
   215|     limits = data.get("limits", {})
   216|     if not isinstance(limits, dict):
   217|         errors.append("'limits' must be an object")
   218|     else:
   219|         allowed_lim = {"max_estimated_tokens", "max_files"}
   220|         for key in limits:
   221|             if key not in allowed_lim:
   222|                 errors.append(f"unknown field under 'limits': '{key}'")
   223|         for key in allowed_lim:
   224|             if key in limits and (not isinstance(limits[key], int) or isinstance(limits[key], bool) or limits[key] < 1):
   225|                 errors.append(f"'limits.{key}' must be a positive integer")
   226| 
   227|     return errors
   228| 
   229| 
   230| def parse_and_validate_request(text: str) -> tuple:
   231|     """Returns (ResolvedRequest, errors). If errors is non-empty, the
   232|     ResolvedRequest half is None."""
   233|     try:
   234|         data = json.loads(text)
   235|     except json.JSONDecodeError as exc:
   236|         return None, [f"request is not valid JSON: {exc}"]
   237| 
   238|     errors = validate_request_dict(data)
   239|     if errors:
   240|         return None, errors
   241| 
   242|     selectors = data.get("selectors", {})
   243|     expansion = data.get("expansion", {}) or {}
   244|     limits = data.get("limits", {}) or {}
   245| 
   246|     resolved = ResolvedRequest(
   247|         schema_version=data["schema_version"],
   248|         question=data["question"],
   249|         strict=bool(data.get("strict", False)),
   250|         files=[_norm_rel(p) for p in selectors.get("files", [])],
   251|         symbols=[{"name": s["name"], "file": _norm_rel(s["file"]) if s.get("file") else None}
   252|                  for s in selectors.get("symbols", [])],
   253|         search_terms=list(selectors.get("search_terms", [])),
   254|         lines=[{"file": _norm_rel(ln["file"]), "line": ln["line"], "end_line": ln.get("end_line", ln["line"])}
   255|                for ln in selectors.get("lines", [])],
   256|         include_callers=bool(expansion.get("include_callers", True)),
   257|         include_callees=bool(expansion.get("include_callees", True)),
   258|         include_imports=bool(expansion.get("include_imports", True)),
   259|         include_related_tests=bool(expansion.get("include_related_tests", True)),
   260|         include_graphify=bool(expansion.get("include_graphify", False)),
   261|         search_as_regex=bool(expansion.get("search_as_regex", False)),
   262|         max_hops=int(expansion.get("max_hops", 1)),
   263|         max_estimated_tokens=int(limits.get("max_estimated_tokens", 12000)),
   264|         max_files=int(limits.get("max_files", 12)),
   265|     )
   266|     return resolved, []
   267| 
   268| 
   269| # --- Selector resolution -------------------------------------------------
   270| 
   271| @dataclass
   272| class SelectorResolution:
   273|     selector_type: str   # "file" | "symbol" | "line" | "search_term"
   274|     requested: str
   275|     status: str           # "resolved" | "ambiguous" | "missing" | "invalid"
   276|     detail: str
   277|     candidates: list = field(default_factory=list)
   278|     resolved_rows: list = field(default_factory=list)  # the matched row(s) for "resolved"
   279| 
   280| 
   281| def resolve_files(files: list, files_by_path: dict) -> list:
   282|     out = []
   283|     for p in files:
   284|         row = files_by_path.get(p)
   285|         if row is None:
   286|             out.append(SelectorResolution("file", p, "missing", f"'{p}' not found in file_inventory.csv"))
   287|         elif row.get("included") != "true":
   288|             out.append(SelectorResolution("file", p, "missing",
   289|                                            f"'{p}' was excluded from the scan (reason: {row.get('exclusion_reason')})"))
   290|         else:
   291|             out.append(SelectorResolution("file", p, "resolved", f"matched included file '{p}'", resolved_rows=[row]))
   292|     return out
   293| 
   294| 
   295| def resolve_symbols(symbols: list, symbols_rows: list) -> list:
   296|     out = []
   297|     for sel in symbols:
   298|         name, file_constraint = sel["name"], sel["file"]
   299|         candidates = _find_symbol_candidates(name, symbols_rows, file_constraint)
   300|         if not candidates:
   301|             out.append(SelectorResolution("symbol", name, "missing",
   302|                                            f"no symbol matching '{name}'"
   303|                                            + (f" in '{file_constraint}'" if file_constraint else "")
   304|                                            + " was found in python_symbols.csv"))
   305|         elif len(candidates) > 1:
   306|             alts = [f"{c['qualified_name']} ({c['relative_path']}:{c['start_line']}-{c['end_line']})" for c in candidates]
   307|             out.append(SelectorResolution(
   308|                 "symbol", name, "ambiguous",
   309|                 f"'{name}' matches {len(candidates)} symbols; qualify with a fully-qualified name or a 'file' field",
   310|                 candidates=alts,
   311|             ))
   312|         else:
   313|             row = candidates[0]
   314|             out.append(SelectorResolution("symbol", name, "resolved",
   315|                                            f"resolved to '{row['qualified_name']}' in '{row['relative_path']}'",
   316|                                            resolved_rows=[row]))
   317|     return out
   318| 
   319| 
   320| def resolve_lines(lines: list, files_by_path: dict) -> list:
   321|     out = []
   322|     for ln in lines:
   323|         f, start, end = ln["file"], ln["line"], ln["end_line"]
   324|         label = f"{f}:{start}" + (f"-{end}" if end != start else "")
   325|         row = files_by_path.get(f)
   326|         if row is None:
   327|             out.append(SelectorResolution("line", label, "missing", f"'{f}' not found in file_inventory.csv"))
   328|             continue
   329|         if row.get("included") != "true":
   330|             out.append(SelectorResolution("line", label, "missing",
   331|                                            f"'{f}' was excluded from the scan (reason: {row.get('exclusion_reason')})"))
   332|             continue
   333|         line_count = row.get("line_count")
   334|         if line_count and line_count.isdigit() and end > int(line_count):
   335|             out.append(SelectorResolution("line", label, "invalid",
   336|                                            f"end line {end} exceeds '{f}' line count ({line_count})"))
   337|             continue
   338|         out.append(SelectorResolution("line", label, "resolved", f"resolved to '{f}' lines {start}-{end}",
   339|                                        resolved_rows=[{"file": f, "start": start, "end": end}]))
   340|     return out
   341| 
   342| 
   343| _REGEX_SEARCH_TIMEOUT_SECONDS = 5.0
   344| 
   345| 
   346| def _scan_term_matches(term: str, pattern: Optional["re.Pattern"], files_rows: list, root: Path) -> tuple:
   347|     term_matches = []
   348|     stale = 0
   349|     for frow in files_rows:
   350|         if frow.get("included") != "true" or frow.get("text_or_binary") == "binary":
   351|             continue
   352|         if not _file_is_fresh(root, frow["relative_path"], frow.get("sha256", "")):
   353|             stale += 1
   354|             continue
   355|         excerpt = _safe_excerpt(root, frow["relative_path"], 1, 10_000_000)
   356|         if excerpt is None:
   357|             continue
   358|         for ln, text in excerpt:
   359|             hit = pattern.search(text) if pattern else (term in text)
   360|             if hit:
   361|                 term_matches.append((frow["relative_path"], ln, text))
   362|     return term_matches, stale
   363| 
   364| 
   365| class _RegexSearchTimeout(Exception):
   366|     pass
   367| 
   368| 
   369| def _raise_regex_timeout(signum, frame) -> None:
   370|     raise _RegexSearchTimeout()
   371| 
   372| 
   373| def _scan_term_matches_bounded(term: str, pattern, files_rows: list, root: Path,
   374|                                 timeout: Optional[float] = None):
   375|     """Runs _scan_term_matches with a wall-clock ceiling, to bound a
   376|     pathological `search_as_regex` pattern's catastrophic-backtracking
   377|     blowup (e.g. `(a+)+$` against a long, nearly-matching source line)
   378|     instead of hanging the CLI indefinitely. Returns None on timeout.
   379| 
   380|     Uses SIGALRM (POSIX only) rather than a background thread: CPython's
   381|     regex engine checks for pending signals periodically even mid-match,
   382|     so the alarm reliably interrupts a runaway match in the *same* thread.
   383|     A background-thread timeout was tried first and rejected -- Python's
   384|     `re` can't be safely killed once started, so an abandoned worker kept
   385|     running and, worse, kept contending for the GIL, which starved the
   386|     "recovered" main thread just as badly (confirmed: the whole process
   387|     still hadn't returned after 60s in that version, despite the join()
   388|     itself returning at 5s).
   389| 
   390|     On platforms without SIGALRM (Windows), there's no safe way to
   391|     interrupt a C-level regex match from Python at all, so this runs
   392|     unbounded there -- the same behavior as before this fix, not a
   393|     regression, just a gap this fix doesn't close on that platform."""
   394|     if timeout is None:
   395|         timeout = _REGEX_SEARCH_TIMEOUT_SECONDS
   396|     if not hasattr(signal, "SIGALRM"):
   397|         return _scan_term_matches(term, pattern, files_rows, root)
   398|     previous_handler = signal.signal(signal.SIGALRM, _raise_regex_timeout)
   399|     signal.setitimer(signal.ITIMER_REAL, timeout)
   400|     try:
   401|         return _scan_term_matches(term, pattern, files_rows, root)
   402|     except _RegexSearchTimeout:
   403|         return None
   404|     finally:
   405|         signal.setitimer(signal.ITIMER_REAL, 0)
   406|         signal.signal(signal.SIGALRM, previous_handler)
   407| 
   408| 
   409| _REGEX_SEARCH_TOTAL_TIMEOUT_SECONDS = 30.0
   410| 
   411| 
   412| def resolve_search_terms(root: Path, terms: list, search_as_regex: bool, files_rows: list) -> tuple:
   413|     """Resolve each search term against the scanned repository's current
   414|     source (same matching rule Tier 2 rendering uses). Returns
   415|     (resolutions: list[SelectorResolution], matches_by_term: dict[str,
   416|     list[(rel_path, line, text)]], stale_files_skipped: int).
   417| 
   418|     Done up front (before the strict-mode gate) so an invalid regex or a
   419|     zero-match term is visible to strict mode exactly like a missing file
   420|     or ambiguous symbol -- search terms were previously invisible to
   421|     `all_resolutions` entirely, so strict mode could not catch them.
   422|     """
   423|     resolutions = []
   424|     matches_by_term: dict = {}
   425|     stale_files_skipped = 0
   426|     # The per-term SIGALRM bound (_scan_term_matches_bounded) stops any one
   427|     # pathological pattern from hanging forever, but the schema places no
   428|     # cap on how many search_terms a request can carry -- hundreds of
   429|     # distinct catastrophic-backtracking patterns could still each burn
   430|     # their own full per-term allowance before packet budgeting even
   431|     # begins. Track a wall-clock deadline across *all* regex terms in this
   432|     # call and shrink (or zero out) each remaining term's allowance once
   433|     # it's been spent, instead of granting every term a fresh timeout.
   434|     regex_deadline = time.monotonic() + _REGEX_SEARCH_TOTAL_TIMEOUT_SECONDS if search_as_regex else None
   435|     for term in terms:
   436|         try:
   437|             pattern = re.compile(term) if search_as_regex else None
   438|         except re.error as exc:
   439|             resolutions.append(SelectorResolution("search_term", term, "invalid", f"not a valid regex: {exc}"))
   440|             matches_by_term[term] = []
   441|             continue
   442|         if search_as_regex:
   443|             remaining = regex_deadline - time.monotonic()
   444|             if remaining <= 0:
   445|                 resolutions.append(SelectorResolution(
   446|                     "search_term", term, "invalid",
   447|                     f"skipped: this request's aggregate search_as_regex evaluation time exceeded "
   448|                     f"{_REGEX_SEARCH_TOTAL_TIMEOUT_SECONDS:.0f}s across all its terms; reduce the number "
   449|                     f"of regex terms or disable search_as_regex",
   450|                 ))
   451|                 matches_by_term[term] = []
   452|                 continue
   453|             scanned = _scan_term_matches_bounded(term, pattern, files_rows, root,
   454|                                                   timeout=min(_REGEX_SEARCH_TIMEOUT_SECONDS, remaining))
   455|             if scanned is None:
   456|                 resolutions.append(SelectorResolution(
   457|                     "search_term", term, "invalid",
   458|                     f"regex evaluation exceeded {_REGEX_SEARCH_TIMEOUT_SECONDS:.0f}s (likely catastrophic "
   459|                     f"backtracking); term skipped -- simplify the pattern or disable search_as_regex",
   460|                 ))
   461|                 matches_by_term[term] = []
   462|                 continue
   463|             term_matches, stale = scanned
   464|         else:
   465|             term_matches, stale = _scan_term_matches(term, pattern, files_rows, root)
   466|         stale_files_skipped += stale
   467|         matches_by_term[term] = term_matches
   468|         if term_matches:
   469|             resolutions.append(SelectorResolution("search_term", term, "resolved",
   470|                                                     f"{len(term_matches)} match(es) found"))
   471|         else:
   472|             resolutions.append(SelectorResolution("search_term", term, "missing", "no matches found"))
   473|     return resolutions, matches_by_term, stale_files_skipped
   474| 
   475| 
   476| # --- Rendering ------------------------------------------------------------
   477| 
   478| def _render_origin_header(title: str, origins: list) -> str:
   479|     return f"\n### {title}\n_Included because: {', '.join(origins)}._\n"
   480| 
   481| 
   482| def _render_excerpt_block(root: Path, rel_path: str, start: int, end: int, budget: Budget, out: list,
   483|                            expected_sha256: str) -> str:
   484|     """Returns "rendered", "stale" (withheld -- source changed since scan),
   485|     "unavailable" (file missing/unreadable), or "too_large" (would not fit
   486|     within the remaining budget). Only "too_large" is a hard, must-not-be-
   487|     silently-dropped conflict for an *explicit* selector -- "stale" and
   488|     "unavailable" are reported as ordinary (non-fatal) omissions, matching
   489|     the direct --file/--symbol/--line packet path's existing behavior."""
   490|     if not _file_is_fresh(root, rel_path, expected_sha256):
   491|         msg = (f"_Source excerpt withheld: `{rel_path}` has changed on disk since the last `scan` "
   492|                f"(SHA-256 mismatch); re-run `scan` for an up-to-date packet._\n")
   493|         if budget.allow(msg, 1):
   494|             out.append(msg)
   495|             budget.spend(msg, 1)
   496|         budget.omissions.append(f"`{rel_path}` changed since the last scan; excerpt withheld. Re-run scan.")
   497|         return "stale"
   498|     excerpt = _safe_excerpt(root, rel_path, start, end)
   499|     if excerpt is None:
   500|         out.append("_Source excerpt unavailable (file missing or unreadable)._\n")
   501|         budget.omissions.append(f"`{rel_path}` excerpt unavailable (file missing or unreadable).")
   502|         return "unavailable"
   503|     body_lines = [f"{ln:>6}| {text}" for ln, text in excerpt]
   504|     body = "\n".join(body_lines)
   505|     if not budget.allow(body, len(body_lines)):
   506|         return "too_large"
   507|     body = redact_secrets(body)
   508|     out.append("```\n" + body + "\n```\n")
   509|     budget.spend(body, len(body_lines))
   510|     return "rendered"
   511| 
   512| 
```
