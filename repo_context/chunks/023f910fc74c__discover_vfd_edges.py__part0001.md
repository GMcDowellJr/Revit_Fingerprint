# Chunk of tools/archetype/discover_vfd_edges.py

- Source relative path: `tools/archetype/discover_vfd_edges.py`
- Chunk: 1 of 3
- Original line range: 1-486
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: RawObservation, ResolvedParam, DomainHint, ParsedCategories, warn, read_json_required, read_json_optional, atomic_write_csv, load_file_metadata, bool_s, find_identity_items_path, is_bad_param_id, row_quality, is_usable_identity_item_value, canonical_param_kind, flush_record, stream_observations, resolve_params, load_bip_hints, hint_target_and_verify, iter_name_contains_rules, infer_domain, parse_category_tokens, sort_category_tokens, parse_category_ints, category_entry_name
- Source SHA-256: 95fe05c8009121c853de6753dc3020bdb0607b4cb260aeb1ac07496433793634
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| #!/usr/bin/env python3
     2| """Discover View Filter Definition dynamic edges from flat identity_items CSV exports.
     3| 
     4| Inputs:
     5|   - identity_items_by_domain/view_filter_definitions.csv (or
     6|     view_filter_definitions_identity_items.csv)
     7|   - bip_lookup.json
     8|   - vfd_category_domain_map.json
     9|   - vfd_bip_target_domain_hints.json
    10|   - shared_param_names.json (optional)
    11|   - file_metadata.csv (optional; required when --dump-unresolved-files is
    12|     set), keyed by export_run_id with client_label, governance_role,
    13|     unit_system columns.
    14| 
    15| Outputs:
    16|   - vfd_param_inventory.csv
    17|   - vfd_dynamic_edges.csv, one row per discovered (edge, param_id) with
    18|     scope_conditions.category_ids listing every supported category and
    19|     category_file_counts giving the per-category file support, so
    20|     generate_reference_graph.py can rebuild dynamic scope_conditions.
    21|   - vfd_unresolved_files.csv (optional; written when --dump-unresolved-files
    22|     is set), one row per (unresolved shared-parameter GUID, export_run_id)
    23|     joined to file_metadata.csv, to help locate source files for resolving
    24|     shared_param_names.json.
    25| 
    26| The output CSVs are intended as offline inputs to generate_reference_graph.py.
    27| All paths are supplied at runtime; the category/domain reference files default
    28| next to this script.
    29| """
    30| from __future__ import annotations
    31| 
    32| import argparse
    33| import csv
    34| import json
    35| import re
    36| import sys
    37| from collections import defaultdict
    38| from dataclasses import dataclass
    39| from pathlib import Path
    40| from tempfile import NamedTemporaryFile
    41| from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple
    42| 
    43| STAGE = "vfd_discover"
    44| RULE_KEY_RE = re.compile(r"^vf\.rule\[(\d+)\]\.param_ref\.(kind|id)$")
    45| BIP_RE = re.compile(r"^bip:-\d+$")
    46| GUID_RE = re.compile(
    47|     r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    48| )
    49| EDGE_ID_RE = re.compile(r"^vfd\.[a-z0-9_]+__[A-Za-z0-9_]+$")
    50| SENTINEL_IDS = {"", "null", "none", "missing", "unreadable", "<none>", "<missing>", "<unreadable>"}
    51| INVALID_ITEM_VALUES = {"", "<NONE>", "<MISSING>", "<UNREADABLE>", "<NOT_APPLICABLE>"}
    52| INVALID_ITEM_QUALITIES = {
    53|     "missing",
    54|     "unreadable",
    55|     "unsupported",
    56|     "unsupported.not_applicable",
    57|     "unsupported.not_implemented",
    58| }
    59| 
    60| INVENTORY_FIELDS = [
    61|     "param_id",
    62|     "param_kind",
    63|     "param_name",
    64|     "name_resolved",
    65|     "target_domain",
    66|     "target_domain_source",
    67|     "target_domain_verified",
    68|     "category_set",
    69|     "category_names",
    70|     "unrecognized_category_ids",
    71|     "has_unextracted_domain",
    72|     "has_unverified_category_mapping",
    73|     "file_count",
    74|     "rule_count",
    75|     "meets_threshold",
    76|     "requires_human_review",
    77|     "candidate_domain",
    78|     "candidate_domain_blocked_reason",
    79| ]
    80| 
    81| DOMAIN_GAP_FIELDS = [
    82|     "candidate_domain",
    83|     "domain_extracted",
    84|     "identity_items_present",
    85|     "blocked_reason",
    86|     "file_count_demand",
    87|     "param_count",
    88|     "category_ids",
    89|     "category_names",
    90| ]
    91| 
    92| EDGE_FIELDS = [
    93|     "edge_id",
    94|     "param_id",
    95|     "param_kind",
    96|     "param_name",
    97|     "param_name_normalized",
    98|     "target_domain",
    99|     "scope_conditions",
   100|     "category_file_counts",
   101|     "file_count",
   102|     "rule_count",
   103|     "name_resolved",
   104|     "target_domain_source",
   105|     "target_domain_verified",
   106|     "requires_human_review",
   107| ]
   108| 
   109| UNRESOLVED_FILE_FIELDS = [
   110|     "param_id",
   111|     "export_run_id",
   112|     "client_label",
   113|     "governance_role",
   114|     "unit_system",
   115|     "rule_count",
   116| ]
   117| 
   118| 
   119| @dataclass(frozen=True)
   120| class RawObservation:
   121|     export_run_id: str
   122|     record_pk: str
   123|     param_id: str
   124|     param_kind: str
   125|     categories_raw: str
   126| 
   127| 
   128| @dataclass(frozen=True)
   129| class ResolvedParam:
   130|     param_id: str
   131|     param_kind: str
   132|     param_name: Optional[str]
   133|     name_resolved: bool
   134| 
   135| 
   136| @dataclass(frozen=True)
   137| class DomainHint:
   138|     target_domain: Optional[str]
   139|     source: str
   140|     verified: bool
   141| 
   142| 
   143| @dataclass(frozen=True)
   144| class ParsedCategories:
   145|     category_set: str
   146|     category_ids: Tuple[str, ...]
   147|     category_names: str
   148|     unrecognized_category_ids: str
   149|     has_unextracted_domain: bool
   150|     has_unverified_category_mapping: bool
   151| 
   152| 
   153| def warn(message: str) -> None:
   154|     sys.stderr.write(f"WARNING [{STAGE}] {message}\n")
   155| 
   156| 
   157| def read_json_required(path: Path, label: str) -> Dict[str, Any]:
   158|     if not path.is_file():
   159|         raise SystemExit(f"ERROR [{STAGE}] required {label} not found: {path}")
   160|     with path.open("r", encoding="utf-8-sig") as f:
   161|         data = json.load(f)
   162|     if not isinstance(data, dict):
   163|         raise SystemExit(f"ERROR [{STAGE}] {label} must be a JSON object: {path}")
   164|     return data
   165| 
   166| 
   167| def read_json_optional(path: Optional[Path], label: str) -> Dict[str, Any]:
   168|     if path is None:
   169|         return {}
   170|     if not path.is_file():
   171|         warn(f"optional {label} not found at {path}; GUIDs will remain unresolved.")
   172|         return {}
   173|     with path.open("r", encoding="utf-8-sig") as f:
   174|         data = json.load(f)
   175|     if not isinstance(data, dict):
   176|         raise SystemExit(f"ERROR [{STAGE}] {label} must be a JSON object: {path}")
   177|     return data
   178| 
   179| 
   180| def atomic_write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, Any]]) -> None:
   181|     path.parent.mkdir(parents=True, exist_ok=True)
   182|     with NamedTemporaryFile("w", encoding="utf-8", newline="", delete=False, dir=str(path.parent), suffix=".tmp") as tmp:
   183|         tmp_path = Path(tmp.name)
   184|         writer = csv.DictWriter(tmp, fieldnames=list(fieldnames))
   185|         writer.writeheader()
   186|         for row in rows:
   187|             writer.writerow({name: row.get(name, "") for name in fieldnames})
   188|     tmp_path.replace(path)
   189| 
   190| 
   191| def load_file_metadata(path: Path) -> Dict[str, Dict[str, str]]:
   192|     if not path.is_file():
   193|         raise SystemExit(f"ERROR [{STAGE}] required file_metadata.csv not found: {path}")
   194|     metadata: Dict[str, Dict[str, str]] = {}
   195|     with path.open("r", encoding="utf-8-sig", newline="") as f:
   196|         reader = csv.DictReader(f)
   197|         fieldnames = set(reader.fieldnames or [])
   198|         required = {"export_run_id", "client_label", "governance_role", "unit_system"}
   199|         missing = required.difference(fieldnames)
   200|         if missing:
   201|             raise SystemExit(f"ERROR [{STAGE}] file_metadata.csv is missing required columns: {sorted(missing)}")
   202|         for row in reader:
   203|             export_run_id = (row.get("export_run_id") or "").strip()
   204|             if not export_run_id:
   205|                 continue
   206|             metadata[export_run_id] = {
   207|                 "client_label": (row.get("client_label") or "").strip(),
   208|                 "governance_role": (row.get("governance_role") or "").strip(),
   209|                 "unit_system": (row.get("unit_system") or "").strip(),
   210|             }
   211|     return metadata
   212| 
   213| 
   214| def bool_s(value: bool) -> str:
   215|     return "true" if value else "false"
   216| 
   217| 
   218| def find_identity_items_path(identity_items_dir: Path) -> Path:
   219|     candidates = [
   220|         identity_items_dir / "view_filter_definitions.csv",
   221|         identity_items_dir / "view_filter_definitions_identity_items.csv",
   222|     ]
   223|     for path in candidates:
   224|         if path.is_file():
   225|             return path
   226|     raise SystemExit(
   227|         "ERROR [vfd_discover] view_filter_definitions identity_items CSV not found. Checked: "
   228|         + ", ".join(str(p) for p in candidates)
   229|     )
   230| 
   231| 
   232| def is_bad_param_id(value: str) -> bool:
   233|     stripped = (value or "").strip()
   234|     return stripped.lower() in SENTINEL_IDS or stripped.startswith("<")
   235| 
   236| 
   237| def row_quality(row: Dict[str, str]) -> str:
   238|     return (row.get("item_value_type") or row.get("item_quality") or "").strip()
   239| 
   240| 
   241| def is_usable_identity_item_value(item_value: str, quality: str) -> bool:
   242|     stripped = (item_value or "").strip()
   243|     if stripped.upper() in INVALID_ITEM_VALUES:
   244|         return False
   245|     if quality.strip().lower() in INVALID_ITEM_QUALITIES:
   246|         return False
   247|     return True
   248| 
   249| 
   250| def canonical_param_kind(param_id: str, raw_kind: str) -> str:
   251|     kind = (raw_kind or "").strip().lower()
   252|     if BIP_RE.match(param_id):
   253|         return "builtin"
   254|     if GUID_RE.match(param_id):
   255|         return "shared"
   256|     if kind in {"builtin", "shared"}:
   257|         return kind
   258|     return "unresolved"
   259| 
   260| 
   261| def flush_record(record_key: Optional[Tuple[str, str]], categories_raw: str, rules: Dict[str, Dict[str, str]]) -> Iterator[RawObservation]:
   262|     if record_key is None:
   263|         return
   264|     export_run_id, record_pk = record_key
   265|     for rule in rules.values():
   266|         param_id = (rule.get("id") or "").strip()
   267|         raw_kind = (rule.get("kind") or "").strip()
   268|         if not param_id or "kind" not in rule or "id" not in rule or is_bad_param_id(param_id):
   269|             continue
   270|         yield RawObservation(
   271|             export_run_id=export_run_id,
   272|             record_pk=record_pk,
   273|             param_id=param_id,
   274|             param_kind=canonical_param_kind(param_id, raw_kind),
   275|             categories_raw=categories_raw,
   276|         )
   277| 
   278| 
   279| def stream_observations(path: Path) -> Tuple[List[RawObservation], int, Set[str]]:
   280|     observations: List[RawObservation] = []
   281|     rows_read = 0
   282|     export_run_ids: Set[str] = set()
   283|     record_order: List[Tuple[str, str]] = []
   284|     record_states: Dict[Tuple[str, str], Dict[str, Any]] = {}
   285| 
   286|     with path.open("r", encoding="utf-8-sig", newline="") as f:
   287|         reader = csv.DictReader(f)
   288|         fieldnames = set(reader.fieldnames or [])
   289|         required = {"export_run_id", "record_pk", "item_key", "item_value"}
   290|         missing = required.difference(fieldnames)
   291|         if missing:
   292|             raise SystemExit(f"ERROR [{STAGE}] identity_items CSV is missing required columns: {sorted(missing)}")
   293|         if "item_value_type" not in fieldnames and "item_quality" not in fieldnames:
   294|             raise SystemExit(
   295|                 f"ERROR [{STAGE}] identity_items CSV is missing required quality column: "
   296|                 "expected item_value_type (preferred) or item_quality"
   297|             )
   298| 
   299|         for row in reader:
   300|             rows_read += 1
   301|             export_run_id = (row.get("export_run_id") or "").strip()
   302|             record_pk = (row.get("record_pk") or "").strip()
   303|             if export_run_id:
   304|                 export_run_ids.add(export_run_id)
   305| 
   306|             item_key = (row.get("item_key") or "").strip()
   307|             match = RULE_KEY_RE.match(item_key)
   308|             if item_key != "vf.categories" and not match:
   309|                 continue
   310| 
   311|             row_key = (export_run_id, record_pk)
   312|             if row_key not in record_states:
   313|                 record_order.append(row_key)
   314|                 record_states[row_key] = {"categories": "", "rules": defaultdict(dict)}
   315|             state = record_states[row_key]
   316|             item_value = (row.get("item_value") or "").strip()
   317|             if not is_usable_identity_item_value(item_value, row_quality(row)):
   318|                 continue
   319| 
   320|             if item_key == "vf.categories":
   321|                 state["categories"] = item_value
   322|             elif match:
   323|                 index, leaf = match.groups()
   324|                 state["rules"][index][leaf] = item_value
   325| 
   326|     for record_key in record_order:
   327|         state = record_states[record_key]
   328|         observations.extend(flush_record(record_key, state["categories"], state["rules"]))
   329|     return observations, rows_read, export_run_ids
   330| 
   331| def resolve_params(
   332|     observations: Sequence[RawObservation],
   333|     bip_lookup: Dict[str, Any],
   334|     shared_param_names: Dict[str, Any],
   335| ) -> Dict[str, ResolvedParam]:
   336|     files_by_param: Dict[str, Set[str]] = defaultdict(set)
   337|     kind_by_param: Dict[str, str] = {}
   338|     for obs in observations:
   339|         files_by_param[obs.param_id].add(obs.export_run_id)
   340|         kind_by_param.setdefault(obs.param_id, obs.param_kind)
   341| 
   342|     resolved: Dict[str, ResolvedParam] = {}
   343|     for param_id in sorted(files_by_param):
   344|         kind = canonical_param_kind(param_id, kind_by_param.get(param_id, ""))
   345|         if BIP_RE.match(param_id):
   346|             name = bip_lookup.get(param_id) or bip_lookup.get(param_id[len("bip:"):])
   347|             if name:
   348|                 resolved[param_id] = ResolvedParam(param_id, "builtin", str(name), True)
   349|             else:
   350|                 warn(f"{param_id} not in bip_lookup.json ({len(files_by_param[param_id])} files). Extend bip_lookup.json to resolve.")
   351|                 resolved[param_id] = ResolvedParam(param_id, "builtin", None, False)
   352|         elif GUID_RE.match(param_id):
   353|             name = shared_param_names.get(param_id) or shared_param_names.get(param_id.lower()) or shared_param_names.get(param_id.upper())
   354|             if name:
   355|                 resolved[param_id] = ResolvedParam(param_id, "shared", str(name), True)
   356|             else:
   357|                 warn(f"GUID {param_id} unresolved ({len(files_by_param[param_id])} files). Provide --shared-param-names to resolve.")
   358|                 resolved[param_id] = ResolvedParam(param_id, "shared", None, False)
   359|         else:
   360|             resolved[param_id] = ResolvedParam(param_id, "unresolved", None, False)
   361|     return resolved
   362| 
   363| 
   364| def load_bip_hints(path: Path) -> Dict[str, Any]:
   365|     hints = read_json_required(path, "vfd_bip_target_domain_hints.json")
   366| 
   367|     exact = hints.get("exact_bip_id", {})
   368|     if isinstance(exact, dict):
   369|         hints["exact_bip_id"] = {
   370|             str(key): value
   371|             for key, value in exact.items()
   372|             if not str(key).startswith("_")
   373|         }
   374| 
   375|     name_rules = hints.get("name_contains", [])
   376|     if isinstance(name_rules, list):
   377|         filtered_rules = []
   378|         for rule in name_rules:
   379|             if isinstance(rule, dict) and any(str(key).startswith("_comment") for key in rule):
   380|                 continue
   381|             filtered_rules.append(rule)
   382|         hints["name_contains"] = filtered_rules
   383|     elif isinstance(name_rules, dict):
   384|         hints["name_contains"] = {
   385|             str(key): value
   386|             for key, value in name_rules.items()
   387|             if not str(key).startswith("_")
   388|         }
   389| 
   390|     return hints
   391| 
   392| 
   393| def hint_target_and_verify(entry: Any) -> Tuple[Optional[str], bool]:
   394|     if isinstance(entry, str):
   395|         return entry, True
   396|     if isinstance(entry, dict):
   397|         target = entry.get("target_domain") or entry.get("domain")
   398|         return (str(target) if target not in (None, "") else None), not bool(entry.get("_verify", False))
   399|     return None, True
   400| 
   401| 
   402| def iter_name_contains_rules(hints: Dict[str, Any]) -> Iterator[Tuple[str, Any]]:
   403|     rules = hints.get("name_contains", [])
   404|     if isinstance(rules, dict):
   405|         for substring, entry in rules.items():
   406|             if not str(substring).startswith("_"):
   407|                 yield str(substring), entry
   408|     elif isinstance(rules, list):
   409|         for entry in rules:
   410|             if not isinstance(entry, dict):
   411|                 continue
   412|             if any(str(key).startswith("_comment") for key in entry):
   413|                 continue
   414|             substring = entry.get("substring") or entry.get("contains") or entry.get("name_contains")
   415|             if substring and not str(substring).startswith("_"):
   416|                 yield str(substring), entry
   417| 
   418| 
   419| def infer_domain(param_id: str, param_name: Optional[str], hints: Dict[str, Any]) -> DomainHint:
   420|     exact = hints.get("exact_bip_id", {})
   421|     if isinstance(exact, dict):
   422|         entry = exact.get(param_id)
   423|         if entry is not None:
   424|             target, verified = hint_target_and_verify(entry)
   425|             return DomainHint(target, "exact_bip_id", verified)
   426| 
   427|     if param_name:
   428|         param_name_lower = param_name.lower()
   429|         for substring, entry in iter_name_contains_rules(hints):
   430|             if substring.lower() in param_name_lower:
   431|                 target, verified = hint_target_and_verify(entry)
   432|                 return DomainHint(target, "name_contains", verified)
   433| 
   434|     return DomainHint(None, "unresolved", True)
   435| 
   436| 
   437| def parse_category_tokens(raw: str) -> Optional[List[str]]:
   438|     value = (raw or "").strip()
   439|     if not value:
   440|         return []
   441| 
   442|     if value.startswith("["):
   443|         try:
   444|             data = json.loads(value)
   445|         except json.JSONDecodeError:
   446|             return None
   447|         if not isinstance(data, list):
   448|             return None
   449|         tokens: List[str] = []
   450|         for item in data:
   451|             if isinstance(item, int):
   452|                 tokens.append(str(item))
   453|             elif isinstance(item, str):
   454|                 token = item.strip()
   455|                 if not re.fullmatch(r"[-+]?\d+", token):
   456|                     return None
   457|                 tokens.append(token)
   458|             else:
   459|                 return None
   460|         return tokens
   461| 
   462|     tokens = [part.strip() for part in value.split(",")]
   463|     if tokens and all(token and re.fullmatch(r"[-+]?\d+", token) for token in tokens):
   464|         return tokens
   465|     return None
   466| 
   467| 
   468| def sort_category_tokens(tokens: Iterable[str]) -> Tuple[str, ...]:
   469|     return tuple(sorted({str(token).strip() for token in tokens}, key=lambda token: int(token)))
   470| 
   471| 
   472| def parse_category_ints(raw: str) -> Optional[List[int]]:
   473|     tokens = parse_category_tokens(raw)
   474|     if tokens is None:
   475|         return None
   476|     return [int(token) for token in tokens]
   477| 
   478| def category_entry_name(entry: Any) -> Optional[str]:
   479|     if isinstance(entry, str):
   480|         return entry
   481|     if isinstance(entry, dict):
   482|         name = entry.get("name") or entry.get("category_name") or entry.get("built_in_category") or entry.get("bic")
   483|         return str(name) if name not in (None, "") else None
   484|     return None
   485| 
   486| 
```
