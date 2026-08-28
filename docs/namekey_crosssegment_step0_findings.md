# Step 0 Findings: Name-Key + Config-Hash Cross-Segment Revision/Rename Detection

Findings-only. No pipeline code was changed to produce this doc. `tools/build_segment_manifest.py`
and `tools/compare_cross_segment.py` were read for orientation only, per the task's protected-files
note; no changes are proposed to either.

## Environment caveat (read this first)

This investigation ran in a remote session with **no access to
`C:\Users\gmcdowell\Documents\Fingerprint_Data`** (a local path on the user's own Windows machine) and
no materialized `Fingerprint_Data` tree anywhere on this machine — confirmed by a full-filesystem
search for `Fingerprint_Data`, `*.details.json`, `records.csv`, and `domain_patterns.csv` (all empty).
Questions that require reading real exported/materialized data (**A.2, C.6, C.7, C.8**) could not be
answered with real corpus numbers here. Each is answered below with the exact command to run and what
to look for; those need to be executed locally (or in a session with `Fingerprint_Data` mounted) before
Step 1 design can rely on them. Everything else — code behavior, file existence, formula properties —
was verified directly against the current repository (branch `claude/name-key-config-hash-detection-qpozbi`,
matching `origin/main` plus no local changes at investigation time).

---

## A. Is `name_key_results.csv` materialized anywhere today?

### A.1 — Runbook wiring

**It is wired in, but as an opt-in switch, not a default step, in both runbooks.**

`tools/corpus_update_runbook.ps1`:
- Declares `[switch]$NameKey` (line 16) and `$NAME_KEY_CSV = "$RESULTS\name_key\name_key_results.csv"` (line 49).
- **Run A** (`-Run A -NameKey`): after the mandatory flatten/apply/placeholders step, lines 157–168 run
  `python tools\apply_name_key_policy.py --export-dir $EXPORTS --name-key-policy $NAME_KEY_POL --out $NAME_KEY_CSV`
  (line 161–164), gated entirely on `if ($NameKey)` (line 157). Without `-NameKey`, this block does not execute.
- **Run B** (`-Run B -NameKey`): lines 215–226 run
  `python tools\generate_name_key_patterns.py --comparison-target name --name-key-csv $NAME_KEY_CSV --out-root "$RESULTS\name_key\patterns"`
  (line 220–223), explicitly commented "OPTIONAL; not required before Run C — Run C re-clusters per segment" (line 216).
- **Run C**: lines 279–304 build `$nameKeyArgs = @("--comparison-target", "both", "--name-key-results-csv", $NAME_KEY_CSV)`
  only `if ($NameKey)` (line 280), passed to `run_segment_orchestrator.py` at line 316. A staleness guard
  (lines 285–301) hard-fails Run C's NameKey path if `$NAME_KEY_CSV`'s mtime is older than `records.csv`'s
  mtime, specifically to prevent silently omitting new/changed exports from the name projection.

Note: the task description's step labels "A/B/C or L1–L4" don't literally match the code — the runbook
uses `-Run A/B/C` (matches), and the per-segment orchestrator uses **Step 2b** / **Step 3b** as its
internal step names (`run_segment_orchestrator.py` lines 1551, 1602, 1605), not "L1–L4". There is no
"L1–L4" naming anywhere in either runbook.

`tools/run_segment_orchestrator.py`:
- Accepts `--comparison-target {config,name,both}` (default `config`) and `--name-key-results-csv`
  (required when target includes `name`, enforced at line 2310).
- `_filter_name_key_csv_to_segment()` (lines 819–902) filters the corpus-wide `name_key_results.csv`
  down to one segment's file population.
- `_run_one_segment()`'s **Step 2b** (lines 1551–1577, gated `comparison_target in ("name", "both")`)
  re-invokes `generate_name_key_patterns.py --comparison-target name` per segment against the filtered CSV.
- **Step 3b** (lines 1602–1689, gated `run_type == "bundle" and comparison_target in ("name", "both")`)
  runs the name-projection bundle stage, writing `results/bundle_analysis/name_all/...` (ALL view only —
  no used-view/compare/share-profile equivalent yet; see DECISIONS.md D-037 and the runbook's own
  line-344 comment).
- This is invoked from `corpus_update_runbook.ps1` line 306–316 only when `-NameKey` is passed to Run C.

**Conclusion**: `apply_name_key_policy.py`/`generate_name_key_patterns.py` ARE wired into both
runbook steps end-to-end (corpus-wide generation → per-segment filter → per-segment re-cluster → bundle
ALL-view), but every stage of that chain is gated behind the `-NameKey` switch. If an operator has never
passed `-NameKey`, no `name_key_results.csv` and no `results/bundle_analysis/name_all/` tree exists for
any segment, regardless of how many times Run A/B/C have otherwise been run.

### A.2 — Does it exist on disk today, for any segment? **BLOCKED — no corpus access.**

Cannot be answered from this session (see Environment caveat). To check locally:

```powershell
Get-ChildItem -Path $ExportsRoot -Recurse -Filter "name_key_results.csv"
Get-ChildItem -Path "$ExportsRoot\segments" -Recurse -Directory -Filter "name_key"
```

If found, compare staleness against the segment's `analysis_run_id` (in that segment's
`results/analysis/*.csv` or `run_registry.csv`) using the same mtime-vs-`records.csv` check the runbook
itself already automates (`corpus_update_runbook.ps1` lines 291–301) — a stale `name_key_results.csv`
(older than the corpus `records.csv`) means recent exports never got processed for name-identity.

### A.3 — Minimal invocation, and does it need raw JSON or can it run off `records.csv`?

**Confirmed (not corrected): it must re-read the original `*.details.json`/`*.index.json` export files. It
cannot run off `records.csv` alone.**

`tools/apply_name_key_policy.py`'s own docstring (lines 1–18) states this, and the code matches:
`_rows_for_export()` (line 110) does `json.load()` directly on each resolved export path (line 127–128)
and calls `build_name_key_for_record(record, domain_name, name_key_policies)` (line 138) against the raw
record dict — it never opens `records.csv`, `identity_items.csv`, or any other flattened CSV.  This is by
design: per the module docstring, `identity_basis.items`, phase2 bucket items, and `label.display` are
all "already present in existing exports," so no live Revit re-extraction (`domains/*.py` +
`runner/run_dynamo.py`) is required — but a fresh parse of the on-disk export JSON is required every time
this tool runs; there's no incremental/cache mode analogous to `run_a_incremental.py`'s.

Exact argparse block (`tools/apply_name_key_policy.py` lines 174–182):

```
--export            Single *.details.json export file.
--export-dir         Directory of export JSON files (non-recursive).
--name-key-policy    Path to domain_name_key_policies.json. (default: policies/domain_name_key_policies.json)
--out                Output CSV path. (default: Results_v21/name_key/name_key_results.csv)
```
One of `--export`/`--export-dir` is required (line 182: `raise SystemExit("Provide --export or --export-dir")`).

Minimal invocation matching the runbook's own usage (corpus-wide, all exports in one directory):

```
python tools/apply_name_key_policy.py --export-dir <exports_dir> \
    --name-key-policy policies/domain_name_key_policies.json \
    --out Results_v21/name_key/name_key_results.csv
```

**There is no native single-segment mode.** `--export-dir` globs one flat directory
(`_iter_export_paths()`, line 52), and the corpus's raw exports all live in one shared directory
(`$EXPORTS = "$ExportsRoot\exports"` in the runbook, line 37) — segments are logical groupings over
`file_metadata.csv`, not separate physical export directories. The only supported way to get a
single-segment `name_key_results.csv` is what `run_segment_orchestrator.py` already does: run
`apply_name_key_policy.py` once, corpus-wide, then filter down with
`_filter_name_key_csv_to_segment()` (lines 819–902) using that segment's `export_run_ids` membership set.

---

## B. Join-key consistency between `records.csv` and `name_key_results.csv`

### B.4 — Is `export_file` the same string as `export_run_id`? **No — not for split-export pairs. A normalization function already exists for this.**

`apply_name_key_policy.py`'s row construction (`_rows_for_export()`, line 143):
```python
rows.append({
    "export_file": export_path.name,   # <-- line 143
    ...
```
`export_path.name` is whatever `_iter_export_paths()` (line 52–87) resolved: **if any `*.details.json`
files exist in `--export-dir`, only the `.details.json` tier is returned** (lines 75–77) — `.index.json`
files are never touched when a `.details.json` sibling tier exists anywhere in the directory. So for a
split-export model, `export_file` = `<base>.details.json`.

`tools/extractor.py`'s `export_run_id` (used for `records.csv`'s `export_run_id` column, `_RECORD_FIELDS`
line 964) is set at line 1017: `export_run_id = _file_id(primary, file_id_mode)`, where `file_id_mode`
is always `"basename"` (default at line 1209, and explicitly passed at `run_extract_all.py` lines 978,
1034, 1373) — so `_file_id()` (line 136–141) returns `primary.name`. Critically, `primary` for a
split-export pair is **not** the details file: `_iter_export_files()` (lines 67–104) builds
`index_by_base`/`details_by_base` maps per model basename, and at lines 90–91:
```python
if idx is not None:
    split_pairs.append((idx.name, idx, det))   # primary = idx, secondary = det
```
i.e. whenever both a `.index.json` and `.details.json` exist for the same base name, **the `.index.json`
file is `primary`**, and `export_run_id` is stamped from the index filename, not the details filename.

**So for any split-export model: `records.csv`'s `export_run_id` = `<base>.index.json`, but
`name_key_results.csv`'s `export_file` = `<base>.details.json`.** A literal string-equality join on
`(export_run_id == export_file)` silently fails to match every split-export model in the corpus.

This is a known, already-diagnosed problem, not a new one: `DECISIONS.md` D-037 states "Split export IDs
normalize from details to index names only when the known metadata IDs support that choice," and the
fix already exists as `normalize_export_run_id()` in `tools/bundle_analysis/name_projection_adapter.py`
(lines 55–90):
```python
if export_file.lower().endswith(".details.json"):
    normalized = export_file[: -len(".details.json")] + ".index.json"
```
with a `known_ids` fallback (tries normalized first, then the raw value, since a *details-only* export —
no sibling `.index.json` at all — legitimately keeps its `.details.json` name as `export_run_id`, and
blindly rewriting that case breaks it per the function's own PR #390-review note, lines 70–79).
`run_segment_orchestrator.py`'s `_filter_name_key_csv_to_segment()` (lines 819–902) already uses exactly
this function to match `name_key_results.csv` rows to a segment's real `export_run_ids` set (lines
874–891): try `normalize_export_run_id(raw) in allowed_ids` first, then the raw value.

**Implication for the proposed join**: joining `records.csv` to `name_key_results.csv` on raw
`(export_run_id, domain, record_id)` string equality is wrong out of the box for any split-export file.
The join must reuse `normalize_export_run_id()` (with `known_ids` = the segment's/corpus's real
`export_run_id` set from `file_metadata.csv`, exactly as `_filter_name_key_csv_to_segment()` does) rather
than inventing new normalization logic. This is a solved problem in the codebase already, not an open
unknown — but it does mean the classifier cannot join naively.

### B.5 — Is `record_id` unique within `(export_run_id, domain)`, and is there a fallback-to-`name` collision risk? **Guaranteed by the record.v2 contract in general; but the two tools derive `record_id` differently, which is a separate, real gap.**

`tools/extractor.py` line 1066:
```python
record_id = _safe_str(rec.get("record_id") or rec.get("id") or rec.get("name"))
```
falls back `record_id → id → name` if `record_id` is falsy. Per `contracts/record_contract_v2.md` line 25,
every record.v2 record MUST carry `record_id` as a "domain-local deterministic key" — so under contract
this fallback should rarely fire against well-formed exports. Per-domain `record_id` construction (spot
checked: `domains/arrowheads.py` line 634, `"arrowhead_type_id:{}".format(type_id_s)`, keyed off a Revit
`ElementId`, which is unique within a document) is designed to be unique per element, so the *contractual*
answer to "is `record_id` unique within `(export_run_id, domain)`" is **yes, by construction**, for a
compliant export. Whether a real corpus ever emits a malformed/degraded record that trips the `id`/`name`
fallback (and whether two such records could then collide on the same `name`) can only be confirmed
against real data — see the C-section blockers below; this session found no counter-example in code
because the fallback path is defensive, not a designed-in domain behavior.

**Separate finding, not asked directly but load-bearing for the join**: `apply_name_key_policy.py` does
**not** apply the same fallback. Line 145: `"record_id": str(record.get("record_id", ""))` — a direct
`.get()` with no `id`/`name` fallback. So for the (contractually-disallowed-but-possible) case of a
record missing `record_id`, `records.csv` would carry a fallback value (`id` or `name`) while
`name_key_results.csv` would carry `""` for the same record — a silent join miss for that one record. This
is a real code-level asymmetry worth fixing or documenting before Step 1, independent of whether it's ever
actually observed in the current corpus.

---

## C. Pattern-level rollup validity

### C.6, C.7, C.8 — **BLOCKED — require real `imperial_project_architectural` segment data not available in this session.**

What the code guarantees, and what still needs empirical confirmation:

**C.6 — `join_hash` uniqueness per `pattern_id` within a domain.** Guaranteed by construction on both
sides, via the identical mechanism:
- Config side: `tools/extractor.py`'s `_process_one_domain()` (line 617) iterates
  `cluster_items` — already grouped by `(domain, schema, join_hash)` — and calls `_stable_pattern_id(dom,
  schema, join_hash, pattern_ids_taken)` once per group (line 648–649).
- Name side: `tools/pattern_id_utils.py`'s `build_clusters()` (lines 92–182) returns a
  `Dict[(domain, schema, join_hash), cluster]`, i.e. a Python dict keyed on that exact tuple — two
  different `join_hash` values cannot share one `pattern_id` and one `pattern_id` cannot be assigned to
  two different `join_hash` values within the same `(domain, schema)`, because each dict key gets exactly
  one `stable_pattern_id()` call (lines 111, matching `tools/extractor.py`'s `_stable_pattern_id`
  byte-for-byte in formula: SHA1 → base32 → collision-extend loop starting at 16 chars).
- This is a structural guarantee (dict keys can't collide with their own values), not a probabilistic one
  — the only way to violate it would be a `_stable_pattern_id`/`stable_pattern_id` bug, not a data
  condition. **Still needs the requested empirical spot-check** because a structural guarantee in the
  clustering code says nothing about whether upstream data (e.g. two different `domain_patterns.csv` rows
  claiming the same `pattern_id` from a stale re-run, or a `patch_all_domain_patterns.py` merge issue) could
  violate it downstream of clustering. Run locally:
  ```bash
  awk -F, 'NR>1{print $<pattern_id_col>","$<join_hash_col>}' \
      segments/imperial_project_architectural/results/analysis_v21/domain_patterns.csv \
      | sort -u | awk -F, '{c[$1]++} END{for (k in c) if (c[k]>1) print k, c[k]}'
  ```
  (substitute real column indices/names from that file's header) — a nonempty result would mean the
  by-construction guarantee is being violated somewhere downstream, which would itself be a real bug worth
  reporting back before Step 1.

**C.7 — arrowheads: one name-hash per config-`pattern_id`, or can a pattern split across multiple
name-hashes?** Cannot be answered without the file. This is the single most important empirical question
in the whole investigation — it's what decides whether the classifier's "one name per pattern" assumption
needs a multiplicity/tie-break rule before any code gets written, exactly as the task's deliverable
instructions anticipate. To answer it locally once `name_key_results.csv` and `records.csv` exist for that
segment:
  1. Join `records.csv` (domain=arrowheads) to that segment's `domain_patterns.csv` on `join_hash` to get
     each record's config `pattern_id`.
  2. Join the result to `name_key_results.csv` on `(normalize_export_run_id(export_run_id), record_id)`
     (per B.4/B.5 above — do not join on raw `export_file` string equality).
  3. Group by `pattern_id`, count distinct name-key `join_hash` values (restricted to `status == "ok"`
     rows, per C.8) per group.
  A ready-made Python snippet (not run here, since there's no data):
  ```python
  import csv, collections
  from tools.bundle_analysis.name_projection_adapter import normalize_export_run_id

  records = [r for r in csv.DictReader(open(".../records.csv")) if r["domain"] == "arrowheads"]
  patterns = {r["join_hash"]: r["pattern_id"] for r in csv.DictReader(open(".../domain_patterns.csv"))}
  known_ids = {r["export_run_id"] for r in records}
  name_rows = {
      (normalize_export_run_id(r["export_file"], known_ids), r["record_id"]): r
      for r in csv.DictReader(open(".../name_key_results.csv"))
      if r["domain"] == "arrowheads" and r["status"] == "ok"
  }
  by_pattern = collections.defaultdict(set)
  for r in records:
      pid = patterns.get(r["join_hash"])
      nk = name_rows.get((r["export_run_id"], r["record_id"]))
      if pid and nk:
          by_pattern[pid].add(nk["join_hash"])
  single = sum(1 for v in by_pattern.values() if len(v) == 1)
  multi = {k: v for k, v in by_pattern.items() if len(v) > 1}
  print(f"single-name-hash patterns: {single}; multi-name-hash patterns: {len(multi)}")
  ```

**C.8 — `status == "ok"` fraction for arrowheads in that segment.** The gate itself is confirmed to exist
exactly as described: `tools/generate_name_key_patterns.py`'s `build_name_patterns()`, line 188:
```python
if r.get("domain") in ELIGIBLE_DOMAINS and r.get("status") == "ok" and r.get("join_hash")
```
(function starts line 166; the `--- trace ---` docstring at line 176–180 explicitly calls this out as an
inline literal, not a named constant). Real fraction is unknown without the CSV; once obtained:
```bash
awk -F, -v OFS=, 'NR==1{for(i=1;i<=NF;i++)h[$i]=i;next} $h["domain"]=="arrowheads"{c[$h["status"]]++} END{for(s in c) print s, c[s]}' \
    name_key_results.csv
```

---

## D. Cross-segment applicability of the name-key hash itself

### D.9 — Policy version field: does `domain_name_key_policies.json` carry one, and is there a cross-segment mismatch gate for it? **No version field exists on either policy; the config-side gate is not (and cannot yet be) mirrored for name-key.**

`policies/domain_name_key_policies.json` has exactly two top-level keys, `_schema_notes` and `domains`
(confirmed: `python3 -c "import json; print(json.load(open('policies/domain_name_key_policies.json')).keys())"`
→ `['_schema_notes', 'domains']`). No per-domain entry (spot-checked `arrowheads`) carries a
`policy_version`/`version` key — full grep for `version` across the file: no matches.

`policies/domain_join_key_policies.json` (the config-side file) is exactly the same shape structurally —
also only `{"domains": {...}}` at the top level, and no per-domain entry carries `policy_version` either
(spot-checked `arrowheads`: keys are `explicitly_excluded_items, hash_alg, join_key_schema, notes,
optional_items, required_items, shape_gating` — no version field).

`core/join_key_policy.py`'s `get_domain_join_key_policy()` (lines 288–296) — the shared loader both
`apply_name_key_policy.py` and the config pipeline use — just returns `policies["domains"][domain_name]`
verbatim; it does not read, inject, or default a version field itself.

The `join_key_policy_version` column that `compare_reference.py` actually gates on
(`REASON_CROSS_SEGMENT_JOIN_POLICY_MISMATCH`, defined line 108, used line 899) is **not** read from the
policy JSON file at compare time — it's a per-record CSV column stamped during the **apply** stage.
`tools/apply_join_policy.py` line 166–170:
```python
policy_id = str(p.get("policy_id") or p.get("join_key_schema") or f"{domain}.join_key.v21")
policy_version = str(p.get("policy_version") or "1")   # defaults to "1" since no domain sets policy_version
r["join_key_policy_id"] = policy_id
r["join_key_policy_version"] = policy_version
```
Since no domain in `domain_join_key_policies.json` sets `policy_version`, every record's
`join_key_policy_version` in `records.csv` is currently `"1"` (the hardcoded fallback) — the gate exists
and is wired, but today it's comparing a constant against itself for every domain.

`apply_name_key_policy.py`'s `_OUTPUT_FIELDS` (lines 40–49) has **no** `join_key_policy_id` /
`join_key_policy_version` column at all — `["export_file", "domain", "record_id", "label_display",
"join_key_schema", "join_hash", "status", "missing_required"]`. So there is currently no mechanism, even
in principle, by which `compare_reference.py`'s existing gate (or any gate) could detect a name-key policy
change between two segments' `name_key_results.csv` files — the column that would carry that information
doesn't exist yet. **A name-key-specific version-mismatch gate would need net-new work**: (a) add a
`policy_version` convention to `domain_name_key_policies.json` entries (or a file-level hash, as
`run_a_cache.py` already does for the config sig_hash/join_hash policy files — see
`CLAUDE.md`'s note that Run A's cache is invalidated "keyed by (cache schema version, sig_hash policy file
hash, join policy file hash)"), (b) emit it as a new column in `apply_name_key_policy.py`'s output, (c)
add the comparison logic. None of this exists today.

### D.10 — Is `canonicalize_str()` a pure function with no segment-specific state? **Confirmed.**

`core/record_v2.py` lines 83–107:
```python
def canonicalize_str(v: Any) -> Tuple[Optional[str], str]:
    if v is None:
        return None, ITEM_Q_MISSING
    try:
        s = str(v)
    except Exception:
        return None, ITEM_Q_UNREADABLE
    s2 = s.strip()
    if not s2:
        return None, ITEM_Q_MISSING
    return s2, ITEM_Q_OK
```
Reads only its single argument `v`; no file I/O, no module-level mutable state, no environment variable
reads, no reference to which extraction run, export, or segment produced `v`. `core/name_key_builder.py`
imports it unmodified (`from core.record_v2 import canonicalize_str`, line 26) and calls it directly (line
127) on whatever raw value it pulled from the record. Two segments' extractions that produce the
post-`.strip()`-identical name string for a given domain's required name item are therefore guaranteed —
not merely likely — to feed `build_join_key_from_policy()`/the shared `phase2_join_hash` mechanism the
identical canonicalized string, and (assuming identical `hash_alg`/`join_key_schema`, which D.9 shows is
unversioned and therefore always identical today) produce identical `join_hash` values. The guarantee is
about the canonicalization step specifically; it says nothing about whether the *raw* pre-canonicalization
name strings genuinely agree semantically across segments (e.g. trailing-whitespace-only differences
collapse correctly, but a human synonym like "Standard" vs. "Default" would not).

---

## E. Domain coverage boundary

### E.11 — `ELIGIBLE_DOMAINS` vs. `domain_name_key_policies.json`'s `"domains"` keys: **Exact match, 25/25, confirmed programmatically.**

```
ELIGIBLE_DOMAINS count: 25
policy domains count:   25
in ELIGIBLE but not in policy: set()
in policy but not in ELIGIBLE:  set()
```
(`core/name_key_coverage.py`'s `ELIGIBLE_DOMAINS = NATIVE_DOMAINS | WIDENED_DOMAINS`, 7 + 18 = 25, asserted
at import time at lines 86–88; `policies/domain_name_key_policies.json["domains"]` has exactly the same 25
keys.) No discrepancy — the registry (`core/name_key_coverage.py`) and the policy file are in lockstep, as
`core/name_key_coverage.py`'s own module docstring (lines 4–8) says they must be.

### E.12 — Mapping-layer domain coverage (`line_patterns`, `fill_patterns`, `arrowheads`, `text_types`, `dimension_types`, `line_styles`)

Two of the six names in the mapping-layer feasibility taxonomy are domain-family names, not the actual
per-partition domain names the name-key coverage registry keys on (D-015: `fill_patterns` and
`dimension_types` are each split into multiple partitions). Checked every partition individually via
`core.name_key_coverage.coverage_class()`:

| Mapping-layer domain | Actual name-key eligibility |
|---|---|
| `line_patterns` | **Widened**, eligible |
| `fill_patterns_drafting` | **Widened**, eligible |
| `fill_patterns_model` | **Widened**, eligible |
| `arrowheads` | **Widened**, eligible |
| `text_types` | **Native**, eligible |
| `dimension_types_linear` | **Widened**, eligible |
| `dimension_types_angular` | **Widened**, eligible |
| `dimension_types_radial` | **Widened**, eligible |
| `dimension_types_diameter` | **Widened**, eligible |
| `dimension_types_spot_slope` | **Widened**, eligible |
| `dimension_types_spot_elevation` | **Excluded** — `referenced_element_name_not_own_label` (`core/name_key_coverage.py` line 78–79: the name-shaped candidate item names a referenced tick-mark/leader symbol, not the dimension type's own label) |
| `dimension_types_spot_coordinate` | **Excluded** — same reason as spot_elevation |
| `line_styles` | **Excluded** — `no_name_like_key` (`core/name_key_coverage.py` line 70) |

**Bounding statement for the whole effort**: of the six mapping-layer domains named in the task, `line_styles`
is entirely out of scope for this whole classifier design (no name-key hash can ever be computed for it), and
`dimension_types` is only partially in scope — 5 of its 7 partitions are eligible
(`linear/angular/radial/diameter/spot_slope`), but `spot_elevation` and `spot_coordinate` are permanently
excluded by the same "referenced element, not own label" reasoning documented in `policies/
domain_name_key_policies.json`'s own `_schema_notes` (lines listing excluded domains and reasons, matching
`core/name_key_coverage.py`'s `EXCLUDED_DOMAINS` dict verbatim). `line_patterns`, `arrowheads`, and
`text_types` are fully eligible. Any classifier built from this design can never report a name/config split
for `line_styles`, `dimension_types_spot_elevation`, or `dimension_types_spot_coordinate` — those three will
always show up as "excluded, no name evidence" rather than either bucket of the proposed 2x2.

---

## Recommendation

**The 2x2 classifier is viable to build, but not on the naive join described in the hypothesis — three
concrete, already-diagnosed corrections are required first, and one empirical question (C.7) is still
completely open and gates the design of the multiplicity rule:**

1. **Join key must be normalized, not literal.** (B.4) Config `export_run_id` and name-key `export_file`
   diverge for every split-export model (index-filename vs. details-filename). The fix already exists —
   `tools/bundle_analysis/name_projection_adapter.py::normalize_export_run_id()` — and is already proven in
   production by `run_segment_orchestrator.py`'s own `_filter_name_key_csv_to_segment()`. Step 1 should reuse
   it, not reinvent it.

2. **`record_id` derivation must be reconciled or the fallback gap accepted explicitly.** (B.5)
   `extractor.py` falls back `record_id → id → name`; `apply_name_key_policy.py` does not. This is a latent
   join-miss source for any record that trips the contractually-disallowed-but-possible missing-`record_id`
   case. Low risk given the record.v2 contract, but worth a one-line fix (mirror the fallback) or an explicit
   documented caveat before Step 1, rather than silent misses.

3. **No cross-segment policy-version safety net exists yet for the name-key hash.** (D.9) Unlike the
   config `join_hash`, which — even though currently trivial (constant `"1"` everywhere) — has the
   *mechanism* (`join_key_policy_version` column + `CROSS_SEGMENT_JOIN_POLICY_MISMATCH` gate) already wired
   for when it stops being trivial, the name-key path has neither the column nor the gate. `canonicalize_str`
   itself is provably pure (D.10), so today's cross-segment name-hash comparisons are sound in practice — but
   there is no structural protection against a future policy edit silently producing incomparable hashes
   across segments extracted before/after the edit, the way there already is on the config side. This should
   be flagged as a design requirement for Step 1, not necessarily built in Step 1 itself.

4. **C.7 is the load-bearing unknown and cannot be resolved without real data.** Whether arrowhead patterns
   in a real segment routinely split across multiple name-hashes (many names sharing one config) — which
   would require a documented multiplicity/tie-break rule (e.g. "report the plurality name-hash and flag the
   pattern as `name_ambiguous`" or "explode one config pattern into N name-facets") — or whether the corpus is
   clean enough that "one name per pattern" holds in practice, changes the shape of the classifier's output
   schema, not just its edge-case handling. This is a hard blocker for finalizing Step 1's design (not
   necessarily for starting it) and needs the query in C.7's snippet run against real `imperial_project_
   architectural` segment data before that design is locked.

5. **Scope boundary is fixed and small enough to state up front.** (E.11, E.12) 25 domains total are ever
   eligible; of the six domains named as this effort's initial target set, `line_styles` and two
   `dimension_types` partitions can never produce a name-hash and should be reported as a fixed "excluded, no
   name evidence" bucket rather than treated as a data gap to chase.

None of C.6's structural guarantee, D.10's purity guarantee, or E.11/E.12's coverage match are in question —
those hold by construction and were verified against current code. What remains open is real-data
verification (C.6's spot-check, C.7, C.8, A.2) and the three code-level gaps above (B.4, B.5, D.9), all of
which are scoped, understood, and — for B.4 specifically — already solved elsewhere in the codebase.
