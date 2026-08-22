# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 2 of 3
- Original line range: 489-619
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _symbol_expansion
- Source SHA-256: f8fc322e94f1d42391838800f006205cb1179854a0162063d062fbcc18f13f91
- Starts inside symbol: no
- Ends inside symbol: no

```
   489| def _symbol_expansion(row: dict, calls_rows: list, imports_rows: list, files_by_path: dict,
   490|                        req: ResolvedRequest, budget: Budget, out: list, note_focus_file,
   491|                        communities_by_file: Optional[dict] = None) -> None:
   492|     rel, qn = row["relative_path"], row["qualified_name"]
   493| 
   494|     if req.include_callers:
   495|         callers = [c for c in _bfs_callers(rel, qn, calls_rows, req.max_hops) if c["confidence"] != "unresolved"]
   496|         if callers:
   497|             header = f"\nCallers of `{qn}` (statically resolved, max_hops={req.max_hops}):\n"
   498|             if budget.allow(header, 1):
   499|                 out.append(header); budget.spend(header, 1)
   500|                 for c in callers:
   501|                     line = (f"- `{c['caller_symbol']}` in `{c['caller_file']}`:{c['line']} "
   502|                             f"— `{c['call_expression']}` ({c['confidence']}: {c['explanation']}) "
   503|                             f"[origin: caller_expansion]")
   504|                     # Budget-check before reserving a focus-file slot -- a
   505|                     # caller entry that ultimately doesn't fit must not
   506|                     # consume the slot on behalf of content that was never
   507|                     # actually rendered.
   508|                     if not budget.allow(line, 1):
   509|                         budget.omissions.append(f"More callers of `{qn}` omitted (packet size limit reached); see python_calls.csv.")
   510|                         break
   511|                     if not note_focus_file(c["caller_file"]):
   512|                         budget.omissions.append(
   513|                             f"Caller of `{qn}` in `{c['caller_file']}` omitted: "
   514|                             f"limits.max_files ({req.max_files}) reached."
   515|                         )
   516|                         break
   517|                     out.append(line); budget.spend(line, 1)
   518|             else:
   519|                 budget.omissions.append(f"Callers listing for `{qn}` omitted entirely (packet size limit reached).")
   520| 
   521|     if req.include_callees:
   522|         callees = [c for c in _bfs_callees(rel, qn, calls_rows, req.max_hops) if c["confidence"] != "unresolved"]
   523|         if callees:
   524|             header = f"\nCallees of `{qn}` (statically resolved, max_hops={req.max_hops}):\n"
   525|             if budget.allow(header, 1):
   526|                 out.append(header); budget.spend(header, 1)
   527|                 for c in callees:
   528|                     line = (f"- `{c['call_expression']}` at line {c['line']} -> `{c['candidate_symbol']}` "
   529|                             f"in `{c['candidate_file']}` ({c['confidence']}: {c['explanation']}) "
   530|                             f"[origin: callee_expansion]")
   531|                     if not budget.allow(line, 1):
   532|                         budget.omissions.append(f"More callees of `{qn}` omitted (packet size limit reached); see python_calls.csv.")
   533|                         break
   534|                     if not note_focus_file(c["candidate_file"]):
   535|                         budget.omissions.append(
   536|                             f"Callee of `{qn}` in `{c['candidate_file']}` omitted: "
   537|                             f"limits.max_files ({req.max_files}) reached."
   538|                         )
   539|                         break
   540|                     out.append(line); budget.spend(line, 1)
   541|             else:
   542|                 budget.omissions.append(f"Callees listing for `{qn}` omitted entirely (packet size limit reached).")
   543| 
   544|     if req.include_imports:
   545|         file_imports = [i for i in imports_rows if i["source_file"] == rel and i["resolved_file"]]
   546|         if file_imports:
   547|             header = f"\nInternal imports of `{rel}` (import_expansion):\n"
   548|             if budget.allow(header, 1):
   549|                 out.append(header); budget.spend(header, 1)
   550|                 for i in file_imports[:20]:
   551|                     line = f"- line {i['line']}: `{i['imported_name'] or i['imported_module']}` -> `{i['resolved_file']}`"
   552|                     if not budget.allow(line, 1):
   553|                         break
   554|                     if not note_focus_file(i["resolved_file"]):
   555|                         budget.omissions.append(
   556|                             f"Import of `{rel}` resolving to `{i['resolved_file']}` omitted: "
   557|                             f"limits.max_files ({req.max_files}) reached."
   558|                         )
   559|                         break
   560|                     out.append(line); budget.spend(line, 1)
   561| 
   562|     if req.include_related_tests:
   563|         tests = _candidate_tests_for_file(rel, imports_rows, calls_rows, files_by_path)
   564|         if tests:
   565|             header = f"\nRelated tests for `{rel}` (related_test_expansion):\n"
   566|             if budget.allow(header, 1):
   567|                 out.append(header); budget.spend(header, 1)
   568|                 for t in tests:
   569|                     line = f"- `{t}`"
   570|                     if not budget.allow(line, 1):
   571|                         break
   572|                     if not note_focus_file(t):
   573|                         # Route through the same global-focus-file gate as
   574|                         # every other tier -- a hard-coded high ceiling here
   575|                         # would let related-test expansion silently bypass
   576|                         # limits.max_files.
   577|                         budget.omissions.append(
   578|                             f"Related test `{t}` for `{rel}` omitted: limits.max_files ({req.max_files}) reached."
   579|                         )
   580|                         break
   581|                     out.append(line); budget.spend(line, 1)
   582| 
   583|     if req.include_graphify and communities_by_file:
   584|         my_communities = communities_by_file.get(rel, [])
   585|         if my_communities:
   586|             comm_ids = {cid for cid, _ in my_communities}
   587|             peers = sorted(
   588|                 f for f, comms in communities_by_file.items()
   589|                 if f != rel and any(cid in comm_ids for cid, _ in comms)
   590|             )[:10]
   591|             if peers:
   592|                 header = f"\nGraphify community peers of `{rel}` ({rc_graphify.format_communities(my_communities)}):\n"
   593|                 if budget.allow(header, 1):
   594|                     out.append(header); budget.spend(header, 1)
   595|                     for p in peers:
   596|                         line = f"- `{p}` [origin: graphify_expansion]"
   597|                         if not budget.allow(line, 1):
   598|                             budget.omissions.append(
   599|                                 f"More Graphify community peers of `{rel}` omitted (packet size limit reached)."
   600|                             )
   601|                             break
   602|                         if not note_focus_file(p):
   603|                             # Route through the same global focus-file gate
   604|                             # as every other expansion tier -- otherwise a
   605|                             # Graphify peer could silently exceed
   606|                             # limits.max_files while the resolution
   607|                             # sidecar's focus_files list stayed under it.
   608|                             budget.omissions.append(
   609|                                 f"Graphify community peer `{p}` of `{rel}` omitted: "
   610|                                 f"limits.max_files ({req.max_files}) reached."
   611|                             )
   612|                             break
   613|                         out.append(line); budget.spend(line, 1)
   614|                 else:
   615|                     budget.omissions.append(
   616|                         f"Graphify community peers listing for `{rel}` omitted entirely (packet size limit reached)."
   617|                     )
   618| 
   619| 
```
