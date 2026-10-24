# Emergent Standards Analysis — PR 1 and PR 2 Implementation Plan

Status date: 2026-09-18
Companion to: `capability_gap.md` (Stage 0 inventory)
Scope: design only. No implementation committed. Feasibility probes were run on synthetic data at real corpus dimensions; they are throwaway and are not part of either PR.

---

## Summary of what the probes changed

Three numeric findings materially altered this design. All were produced by running stdlib-only simulations at the real shape (184 sources, 41 domains). They are **measured on synthetic data**, not on your corpus.

**1. Raw Cramér's V and NMI are unusable here.** At N=184, two *completely independent* domains score:

| Cardinality | Cramér's V | Bias-corrected V | NMI |
|---|---|---|---|
| 5 × 5 | 0.143 | 0.000 | 0.026 |
| 20 × 20 | 0.328 | 0.065 | 0.334 |
| 50 × 50 | 0.514 | 0.056 | 0.625 |
| 100 × 100 | 0.681 | 0.135 | 0.781 |
| 184 × 184 (near-unique) | **1.000** | n/a | **1.000** |

A domain where every file has its own configuration — `loaded_family_types` is the obvious candidate — would score a **perfect 1.000 association with anything**, purely as an artifact. Shipping raw V or NMI would have manufactured exactly the false archetypes this programme exists to avoid. Bergsma's bias-corrected V holds the line (0.000–0.177 under independence vs 1.000 coupled, 0.667 at 70% coupling).

**2. Jaccard is prevalence-sensitive and is not an association metric.** Two independent binary variables at prevalence 0.5 score **Jaccard 0.313**. Jaccard ignores the shared-absence cell, so it cannot distinguish coupling from base rate. The phi coefficient on the same data scores 0.000. This matters beyond PR 2: `cluster_archetype_signals.py` currently derives its coupling threshold from raw Jaccard, and inherits this sensitivity. That is a pre-existing observation about the authored pipeline, not a defect introduced here, and is **not** in scope for either PR.

**3. Residual bias in corrected V is cardinality-dependent but calibratable.** 300-permutation null, N=184:

| Cardinality | p95 of V_bc under independence | max observed |
|---|---|---|
| 2 | 0.120 | 0.239 |
| 10 | 0.107 | 0.164 |
| 50 | 0.141 | 0.215 |
| 100 | 0.187 | 0.248 |
| 184 | 0.205 | 0.322 |

So a single global threshold would be wrong. A cardinality-aware null is required, and a per-pair permutation test is affordable (see timings below).

**4. The whole engine is cheap.** Measured on synthetic data at 41 domains × 184 sources, stdlib only, single core: all 820 pair point-estimates in **0.30 s**; a 200-permutation null for all 820 pairs in **~60 s**; state matrix ~69 KB resident.

---

## Finding: why `config/archetype/archetype_definitions.json` is absent

Investigated as requested, and reported without repair.

- `git log --all -- config/archetype/archetype_definitions.json` returns **nothing**. The file has never existed in the repository's history on any ref.
- It is **not** gitignored (`git check-ignore` exit 1). `.gitignore` covers only `__pycache__/`, `*.pyc`, `graphify-out/`, and `tools/probes/Exports/*`.
- `config/archetype/` contains exactly one file: `static_edges_seed.json`.
- `tools/archetype/README.md` documents it as "hand-maintained, not generated", produced by curating `generate_archetype_candidates.py`'s output (`archetype_definitions_candidates.json`), copying promising candidates in, setting `governance_question` / `approach_label`, and flipping `promoted` to `true`.

**Classification: an authored durable-config artifact that was never committed.** Not accidentally deleted (no delete event exists), not generated (README is explicit), not a stale documentation reference (four code modules genuinely require it). The most likely explanation is that the DP1 curation pass was performed against a local export root and the resulting file was never promoted back into the repo — or was never performed at all.

Consequence: stages 3–5 of the authored pipeline, and the entire clustering chain, cannot run. **No shared dependency forces repair inside PR 1 or PR 2.** The emergent engine reads neither the definitions file nor the seed edges (it reads the seed edges only in the evaluation report, and treats absence as "no positive controls available"). Recommend a **separate PR 0**, independent of this work: either commit the curated file, or add a `--definitions` preflight that fails with an actionable message instead of a bare `FileNotFoundError`, plus tests for the six uncovered stage modules.

---

# A. PR 1 design — formalize `evidence.v1`

**Goal:** a versioned semantic contract between Fingerprint outputs and any downstream analysis, with zero upstream behavioral change.

## A.1 Current data-flow map

Verified paths and producers. Segment root is a directory produced by `tools/run_segment_orchestrator.py`; the corpus-wide case is the root segment.

| Artifact | Produced by | Path | Grain |
|---|---|---|---|
| `file_metadata.csv` | `tools/extractor.py::emit_records` → `_merge_meta_row` (line ~1315) | `<segment>/results/records/file_metadata.csv` | one row per `export_run_id` |
| `pattern_presence_file.csv` | `tools/extractor.py::emit_analysis` (writer line 1546; rows built 828–853) | `<segment>/results/analysis/pattern_presence_file.csv` | one row per (`export_run_id`, `domain`, `pattern_id`) |
| `domain_patterns.csv` | `tools/extractor.py::emit_analysis` (line 1525) | `<segment>/results/analysis/domain_patterns.csv` | one row per (`domain`, `pattern_id`) |
| `membership_matrix.csv` | `tools/bundle_analysis/step1_membership_matrix.py::build_membership_matrix` | `<segment>/results/bundle_analysis/{all,used}/<domain>/membership_matrix.csv` | one row per (`export_run_id`, `pattern_id`) per view |
| `records.csv` | `tools/extractor.py` | `<segment>/results/records/records.csv` | one row per record — **not read by PR 1** |

Consumers today: `pattern_presence_file.csv` is read by 11 modules, `file_metadata.csv` by 26. There are **169 distinct hardcoded CSV basenames** across `tools/` (a correction to the earlier "~40" estimate, which counted only the frequently-referenced subset).

### Column semantics verified from source

`pattern_presence_file.csv` columns: `schema_version, analysis_run_id, export_run_id, domain, pattern_id, pattern_share_pct, is_dominant_pattern, deviation_score, corpus_classification`.

Four semantics that the contract must not misrepresent, all read from `tools/extractor.py:790-853`:

- **A row exists only where the file has records in that domain with a resolvable `pattern_id`.** Domain absence is therefore encoded as *absence of rows*, not as a zero row. This is what makes domain-level presence derivable at all.
- **`is_dominant_pattern` is file-local, not corpus-conformance.** It marks the most frequent pattern within *this file's* records for *this* domain. On a tie it is `false` for every pattern (the file is counted into `files_with_tied_dominant`), so a file can have **no** dominant pattern. Do not read it as "conforms to the standard".
- **`deviation_score` is also file-local**: `dominant_share - share` within the file.
- **`corpus_classification`** (`CORPUS_STANDARD` / `CORPUS_VARIANT`) is a property of the **pattern**, constant across files, thresholded on `domain_pattern_presence_pct >= STANDARD_PRESENCE_MIN`.
- **Rows with `pattern_id == ""` and `corpus_classification == "UNKNOWN"`** are emitted for records whose `join_hash` did not resolve. These are real signal about extraction quality and **must be carried through the contract as an explicit unknown mass**, not silently dropped.

`file_metadata.csv` columns: `schema_version, export_run_id, file_id, project_id, model_id, project_label, model_label, central_path, central_path_norm, lineage_hash, revit_version_number, revit_version_name, revit_build, is_workshared, tool_version, exported_utc, client_label, governance_role, unit_system, discipline_label, business_center_label, collection_label`.

## A.2 Proposed `evidence.v1` contract

Two required entities, one optional. Deliberately minimal: only what cross-domain analysis needs.

### Entity: `Source` (one row per analysed file)

| Field | Type | Req | Null | Notes |
|---|---|---|---|---|
| `source_id` | str | ✅ | no | PK. From `export_run_id`. |
| `governance_role` | str | ✅ | no | Enum-ish: Template / Container / Project / Generic / `""`. Empty allowed and meaningful (inference found no match). |
| `project_id` | str | ○ | yes | |
| `client_label` | str | ○ | yes | Human-curated upstream; may be blank. |
| `discipline_label` | str | ○ | yes | |
| `business_center_label` | str | ○ | yes | |
| `collection_label` | str | ○ | yes | |
| `unit_system` | str | ○ | yes | |
| `software_version` | str | ○ | yes | From `revit_version_number`. Generic name — a non-Revit producer fills its own. |
| `software_build` | str | ○ | yes | From `revit_build`. |
| `is_workshared` | bool | ○ | yes | |
| `parent_source_id` | str | ○ | yes | **Defined, always empty in the v1 Fingerprint adapter.** See A.7. |
| `context_fingerprint` | str | ✅ | no | Adapter-computed stable hash of the populated context fields; lets a stratifier key on "same context" without enumerating columns. |

Deliberately **excluded**: `central_path`, `central_path_norm`, `lineage_hash`, `model_label`, `project_label`. These are file-local identifiers or heuristic self-identity. Excluding them from the contract is what prevents a future consumer from inventing pseudo-lineage out of path matching. `capability_gap.md` established `lineage_hash` is self-identity, not inheritance; the contract should make that structurally unavailable rather than merely discouraged.

### Entity: `Observation` (one row per source × domain × configuration)

| Field | Type | Req | Null | Notes |
|---|---|---|---|---|
| `source_id` | str | ✅ | no | FK → Source. |
| `domain` | str | ✅ | no | |
| `config_id` | str | ✅ | yes-as-empty | From `pattern_id`. Empty ⇔ unresolved (`is_unknown` true). |
| `is_unknown` | bool | ✅ | no | True for the `pattern_id == ""` / `UNKNOWN` rows. |
| `share` | float | ✅ | no | From `pattern_share_pct`, 0.0–1.0. |
| `is_source_modal` | bool | ✅ | no | From `is_dominant_pattern`. **Named to prevent the file-local/corpus-modal confusion.** |
| `corpus_class` | str | ○ | yes | `CORPUS_STANDARD` / `CORPUS_VARIANT` / `UNKNOWN`. |
| `used_state` | str | ✅ | no | `all` / `used` / `unknown`. `unknown` when bundle_analysis output is absent. |

Uniqueness: (`source_id`, `domain`, `config_id`, `used_state`) unique. Shares within (`source_id`, `domain`, `used_state`) sum to 1.0 ± tolerance.

### Entity: `DomainRegistry` (optional, adapter-populated)

`domain`, `n_sources_observed`, `n_distinct_configs`, `is_single_valued_per_source`. Cheap to compute during adaptation, and PR 2 needs it for gating. Marking `identity`, `units_doc`, `worksets_doc`, `browser_organization` as single-valued is derivable from the data rather than hardcoded.

### Versioning strategy

- `evidence_schema_version = "evidence.v1"`, carried in `manifest.json` and as a column on each table. **Distinct from** the CSV `schema_version = "2.1"`, which is a serialization version.
- Additive optional fields → same version. Any change to a required field's meaning, type, or uniqueness → `evidence.v2`.
- The adapter records `producer` (`fingerprint_v21`), `producer_version` (from `tool_version`), and `source_analysis_run_id` so a consumer can tell which producer and which run generated the evidence.
- Consumers declare the versions they accept; the reader raises on an unaccepted version rather than best-effort parsing.

### Physical format

**Stay on CSV.** Justification: the analysis inputs are aggregates (~10⁵ rows), the repo has a standing stdlib-only rule for `tools/`, and measured memory for the derived state matrix is ~69 KB. No measured bottleneck justifies Parquet or DuckDB. The contract is defined as a *logical model with a serialization-independent reader* (`read_sources()`, `read_observations()`), so a columnar backend can be added later without touching any analysis code.

## A.3 Adapter design

New package `evidence/`, no existing file modified.

    evidence/
      __init__.py
      contract.py          dataclasses Source, Observation, DomainRow; EVIDENCE_SCHEMA_VERSION;
                           accepted-version check; field specs used by both writer and validator
      reader.py            read_sources(), read_observations(), read_manifest()
                           — the only API downstream code may use
      validate.py          validate_bundle() -> list[Finding]; severity ok/warn/blocked
      adapters/
        __init__.py
        fingerprint_v21.py build_evidence(segment_root, out_dir, *, used_view=...) -> Manifest

`fingerprint_v21.build_evidence` is a **projection, not a transformation**:

1. Read `<segment>/results/records/file_metadata.csv` → `Source` rows. Pure column rename + subset. `context_fingerprint` is the one computed field.
2. Read `<segment>/results/analysis/pattern_presence_file.csv`, filter to a single `analysis_run_id` (fail loudly if more than one, matching `compare_reference.py`'s existing precedent at line 567) → `Observation` rows with `used_state="all"`. Renames only; `is_unknown` derived from `pattern_id == ""`.
3. **Optional** `used` enrichment: if `<segment>/results/bundle_analysis/used/<domain>/membership_matrix.csv` exists for a domain, emit that domain's `used_state="used"` observations. Absent → no `used` rows for that domain and a recorded warning. This keeps PR 1 independent of the bundle pipeline, which per `CLAUDE.md` is currently failing at near-100% on `run_type=bundle` runs.
4. Compute `DomainRegistry` in the same pass.
5. Write `sources.csv`, `observations.csv`, `domains.csv`, `manifest.json` atomically (temp + `os.replace`, matching `tools/archetype/_common.py::atomic_write_csv`).

No hash is recomputed. No pattern is re-derived. No record is re-read. The adapter never opens `records.csv`, `phase0_identity_items.csv`, or any export JSON.

## A.4 Validation rules

`validate.py` returns structured findings with severity, never silently repairing.

**Blocking:** missing required column; missing required file; `evidence_schema_version` unrecognized; duplicate on (`source_id`,`domain`,`config_id`,`used_state`); duplicate `source_id` in `sources.csv`; observation referencing an unknown `source_id`; `share` unparseable, negative, or > 1.0; `is_unknown` false with empty `config_id` (or the converse); more than one `analysis_run_id` in the input.

**Warning:** shares within (`source_id`,`domain`,`used_state`) not summing to 1.0 ± 0.01; a domain in `domain_patterns.csv` with zero observations; a source in `sources.csv` with zero observations; optional context column entirely blank corpus-wide (blocks stratification later — worth surfacing early); `governance_role` empty for > 20% of sources; no `used` rows at all.

**Informational:** per-domain `n_sources_observed` and `n_distinct_configs`; count of unknown-mass observations per domain.

Unknown domain identifiers are treated as a **warning, not a blocker** — a domain present in observations but absent from `policies/domain_sig_hash_policies.json` is recorded in the manifest. Blocking would couple `evidence/` to Fingerprint policy files, defeating the purpose.

## A.5 Reconciliation tests

New `tests/test_evidence_contract.py` and `tests/test_evidence_adapter_reconciliation.py`.

1. **Row-count reconciliation.** `len(observations where used_state='all')` equals the row count of `pattern_presence_file.csv` for the selected `analysis_run_id`. `len(sources)` equals `file_metadata.csv` row count. Exact equality.
2. **Per-domain reconciliation.** For every domain: distinct `source_id` count and distinct `config_id` count match the same computed directly from the source CSV. Catches a filter that silently drops the unknown rows.
3. **Value fidelity.** For a sampled 5% of rows, `share` round-trips to `pattern_share_pct` within float tolerance and `is_source_modal` equals `is_dominant_pattern`.
4. **Unknown-mass preservation.** Count of `is_unknown` observations equals the count of `pattern_id == ""` rows. This is the single most likely thing to be accidentally dropped.
5. **No upstream mutation.** Snapshot `sha256` of every file under `<segment>/results/` before and after `build_evidence`; assert unchanged. This is the formal guarantee that PR 1 changes no analytical result.
6. **Order invariance.** Shuffle input CSV row order; assert byte-identical adapter output.
7. **Determinism.** Two runs produce byte-identical output including `manifest.json` modulo its timestamp field.
8. **Second-producer independence.** A fixture generator emits `evidence.v1` directly with no Fingerprint code in the path; `reader.py` + `validate.py` accept it and report clean. This is the test that proves producer-independence rather than asserting it.
9. **Validator negative cases.** One test per blocking rule, each asserting the specific finding code.

## A.6 Filename-decoupling scope

**Change zero existing call sites in PR 1.**

- In scope: the four filenames `fingerprint_v21.py` itself references (`file_metadata.csv`, `pattern_presence_file.csv`, `domain_patterns.csv`, `membership_matrix.csv`), which become internal to one adapter module.
- Out of scope: all 169 basenames across `tools/`, and every one of the 11 + 26 existing consumers of the two key inputs.

Rationale: the contract's value is that *new* code depends on it. Migrating existing consumers is a large, risky, behavior-preserving refactor with no analytical payoff, and would put PR 1's "no upstream change" guarantee at risk for nothing. Revisit only if a second producer actually appears.

## A.7 Backward compatibility and `parent_source_id`

Existing authored-archetype code is **entirely unaffected**: it reads `results/records/` and `results/analysis/` directly, and PR 1 adds files in a separate output directory without touching those. Both pipelines can run concurrently, in either order, indefinitely. `evidence/` must not be imported by anything under `tools/archetype/` in PR 1 or PR 2.

`parent_source_id` is defined in the `Source` schema and **always emitted empty** by the v1 Fingerprint adapter. When real provenance becomes available, the cheapest path is a curated column in `file_metadata.csv` (the file already carries five human-curated columns preserved across runs by `_merge_meta_row`, so the mechanism exists). The adapter would then pass it through with no contract-version change, and PR 2's outputs would gain a `lineage_conditioned` stratum. **No heuristic inference. No path or filename matching.** The field exists so that its absence is visible in the schema rather than discovered later.

---

# B. PR 2 design — emergent domain association

**Goal:** evaluate every domain pair with no authored semantic gating.

## B.1 Domain count — verified

`policies/domain_sig_hash_policies.json` declares **40** domains. `policies/domain_join_key_policies.json` declares **41**; the extra is `view_category_overrides`, the routing coordinator, which emits no records of its own (its output is split into `_model` / `_annotation` partitions).

So **40 emitting domains → 780 unordered pairs**, not 820. The engine must not hardcode either number: it enumerates whatever domains appear in the evidence, and the manifest records the count. The 820 figure in the brief assumed 41.

## B.2 The binary projection problem, and the resolution

Domain-level presence — "does this file have any records in this domain" — is **near-ubiquitous and therefore degenerate**. Every Revit model has text types, line styles, object styles, materials. A contingency table on domain presence would be almost entirely in the `n_both` cell, giving undefined or meaningless association.

The resolution is to project at the **configuration-state** level. For each (`source_id`, `domain`):

**`domain_state_id` = md5 of the sorted, deduplicated list of that source's `config_id` values for that domain** (unknown-mass observations excluded, counted separately).

This is the load-bearing idea, and it has a property that eliminates the Cartesian risk by construction: **the number of distinct states in a domain cannot exceed the number of sources.** At 184 sources, every pair's contingency table is at most 184 × 184 regardless of how many patterns the domain contains. Pattern × pattern expansion is not avoided by a threshold — it is structurally unreachable.

Two projections are derived from the same state, and both are emitted:

**P-STATE (categorical).** `domain_state_id` per (source, domain). Full R×C contingency. Supports bias-corrected Cramér's V. Answers: *do these two domains vary together at all?*

**P-CONFORM (binary).** `deviates = (domain_state_id != corpus_modal_state_id_for_that_domain)`. Fixed 2×2. Supports phi. Answers: *when a file departs from the corpus norm in domain A, does it also depart in domain B?* — which is the actual governance question.

The corpus-modal state is the most frequent `domain_state_id` across sources for that domain; on a tie, the lexicographically smallest, recorded explicitly in `domain_cardinality.csv` as `modal_tie=true`.

Three points of honesty the implementation must preserve:

- A source with **no observations** in a domain is *not* given a state. It is `domain_observed=false` with an empty `domain_state_id`. Pairs are computed **pairwise-complete**: only sources where *both* domains are observed enter that pair's table, and `n_sources_used` is reported per pair. No invented sentinel value (the repo's three-sentinel rule is about identity values, but inventing an `<ABSENT>` category here would silently merge "not installed" with "configured empty").
- **State cardinality is combinatorial, not pattern-driven.** A domain with few patterns can still have near-unique states if each source uses a different subset (measured on a synthetic fixture: 12 patterns → 122 states across 184 sources). The `state_ratio` column in `domain_cardinality.csv` exists to make this visible per domain, and E.1 records the mitigations if it proves widespread.
- **Unknown mass is excluded from the state hash but reported.** `unknown_share` per (source, domain) is carried into `domain_cardinality.csv`. A domain whose states are largely driven by unresolved join hashes is measuring extraction quality, not configuration.
- `P-CONFORM` collapses *how* a file deviates. Two files deviating in opposite directions both read as `deviates=true`. This is stated in the output, not hidden — it is why `P-STATE` is emitted alongside.

## B.3 Metrics — and what is deliberately not shipped

Grounded in the probes above.

| Metric | Projection | Ship in PR 2 | Why |
|---|---|---|---|
| `n_both / n_a_only / n_b_only / n_neither` | P-CONFORM | ✅ | The sufficient statistic. Everything else derives from it. |
| `support` (`n_both`) and `n_sources_used` | both | ✅ | Required to judge every other number. |
| **`phi`** | P-CONFORM | ✅ **primary** | Measured 0.000 / 0.049 / −0.039 under independence at prevalences 0.5 / 0.2 / 0.05; 1.000 coupled. Unbiased and interpretable. |
| **`cramers_v_bc`** (Bergsma-corrected) | P-STATE | ✅ **primary** | Measured 0.000–0.177 under independence vs 1.000 coupled. Handles the high-cardinality domains phi's binarization discards. |
| `perm_p_value`, `perm_null_p95` | both | ✅ | Cardinality-aware calibration. Probe B shows a fixed threshold is wrong. |
| `cooccurrence_rate` | P-CONFORM | ✅ descriptive only | `n_both / n_sources_used`. Labeled descriptive in the schema. |
| `jaccard` | P-CONFORM | ⚠️ descriptive only, explicitly flagged | Measured **0.313 under independence** at prevalence 0.5. Emitted for continuity with existing tooling, with a column comment and a `metric_role=descriptive` marker. **Must not be thresholded on.** |
| `cramers_v` (raw) | P-STATE | ❌ | Reads 1.000 for independent near-unique domains. |
| `mutual_information`, `nmi` | P-STATE | ❌ | NMI reads 0.33–1.00 under independence at this N. Revisit with adjusted MI if a real need appears. |
| `odds_ratio` | P-CONFORM | ❌ | Redundant with phi at 2×2; unstable with zero cells. |

**Redundancy, stated plainly:** at 2×2, Cramér's V equals |phi|, so shipping both from the *same* projection would be duplication. They are not duplicative here because they come from *different* projections — phi from P-CONFORM, corrected V from P-STATE. That is the only reason both are present.

## B.4 Statistical safeguards

- **Minimum sources:** engine refuses to emit association metrics if `n_sources_used < 20` for a pair (contingency counts still emitted, metrics null, `gate_reason=insufficient_sources`). Configurable via `--min-sources`.
- **Minimum support:** a pair needs `n_both >= 5` and both marginals non-degenerate. Configurable.
- **Zero cells:** phi is null when any marginal is zero (division by zero — probe A returned `n/a` for the ubiquitous case, correctly). Never substitute 0.0.
- **Ubiquitous domains:** a domain whose modal state covers ≥ 95% of sources has near-zero variance in P-CONFORM. Emit with `gate_reason=low_variance_domain` and exclude from the edge list, but keep in `domain_cardinality.csv` — "this domain is universally uniform" is a finding, arguably the most valuable governance finding available.
- **Extremely sparse domains:** observed in < 20 sources → excluded from pairing with `gate_reason=sparse_domain`.
- **High-cardinality domains:** `n_distinct_states / n_sources_observed >= 0.9` (near-unique) → P-STATE metrics suppressed with `gate_reason=near_unique_states`; P-CONFORM still valid (everything deviates, so it will be caught by the low-variance gate instead). This is the `loaded_family_types` case.
- **Multiple comparisons:** 780 pairs at α=0.05 yields ~39 false positives by chance. Emit Benjamini–Hochberg FDR-adjusted q-values alongside raw permutation p-values. Do **not** silently filter — emit both and let the consumer choose.
- **Permutation tests: warranted now**, because probe B proved a fixed threshold is invalid and the measured cost is ~60 s for all pairs at 200 permutations. Default `--permutations 200`, `0` to disable.
- **Confidence intervals: defer.** Bootstrap CIs on phi add cost without changing any decision at this stage. Revisit if a downstream consumer needs effect-size ranking rather than screening.
- **Small strata:** any stratum with `n_sources < --min-sources` emits contingency counts with null metrics and `gate_reason=insufficient_sources`. Never dropped silently — a stratum too small to analyse is itself information.

## B.5 Context stratification

Strata computed **only** where the field is populated and the stratum is large enough:

`global` (always) · `governance_role` · `client_label` · `business_center_label` · `discipline_label` · `unit_system` · `software_version` · `used_state` (all vs used, only where `used` observations exist).

Every output row carries `stratum_key` and `stratum_value`; `global` is a row like any other.

**`lineage_conditioned` is emitted as a single row in `warnings.json` stating `status: unavailable, reason: no parent_source_id in evidence.v1`.** It is never computed from paths, filenames, or `lineage_hash`. The outputs must make it impossible to mistake role-stratified association for inheritance-controlled association — a `stratum_key=governance_role` row means "association among Templates", not "association controlling for inheritance".

## B.6 Files to create / modify / leave untouched

**Create:**

    analysis/__init__.py
    analysis/project.py        build_domain_states() -> per (source, domain) state + flags
                               build_conformance()   -> P-CONFORM binary matrix
    analysis/contingency.py    build_pair_tables()   -> the 2x2 and RxC sufficient statistics
    analysis/metrics.py        phi(), cramers_v_bias_corrected(), cooccurrence_rate(),
                               jaccard_descriptive(), benjamini_hochberg()
    analysis/permutation.py    permutation_null()    -> p-value + null p95, seeded
    analysis/stratify.py       iter_strata()         -> (key, value, source_id set)
    analysis/emergent_association.py   CLI entry point, orchestration, artifact writing
    analysis/seed_evaluation.py        positive-control comparison vs static_edges_seed.json
    analysis/io.py             atomic CSV/JSON writers (mirrors archetype/_common conventions)

**Extract into shared use (see section C):**

    analysis/cluster.py        complete_linkage_clusters()  [lifted from cluster_archetype_signals.py]
    analysis/threshold.py      jenks_threshold()            [lifted, decoupled from log(STAGE, ...)]

**Modify:** nothing. Not `tools/`, not `core/`, not `domains/`, not `policies/`, not `contracts/`.

**Leave untouched explicitly:** all of `tools/archetype/` (including `static_edges_seed.json`, which PR 2 reads read-only for evaluation), the whole stage machine, every existing CSV.

`analysis/` imports only from `evidence/` and the stdlib. It must not import from `tools/`, mirroring the existing rule that extraction never imports from `tools/`.

## B.7 Complexity and anti-explosion

**Theoretical.** 40 domains → 780 pairs. State projection is O(observations) ≈ 10⁵. Each pair's contingency is O(n_sources) = 184 with a dict keyed by (state_a, state_b), bounded at 184 distinct keys. Total point-estimate work ≈ 780 × 184 ≈ 1.4 × 10⁵ operations. With S strata, multiply by S (~15 realistic strata values across 7 keys) → ~2 × 10⁶. Permutations multiply by `--permutations`.

**Measured on synthetic data at real dimensions** (41 domains × 184 sources, stdlib, single core — not measured on your corpus):

| Step | Time |
|---|---|
| All 820 pair point estimates | 0.30 s |
| 200-permutation null, all 820 pairs | ~60 s |
| Resident state matrix | ~69 KB |

Expected runtime class: **seconds without permutations, low minutes with them, across all strata.** Memory in the low tens of MB dominated by reading the evidence CSVs, not by the analysis.

**Anti-explosion, concretely.** The bad shape is `for pair: for pattern_a: for pattern_b: for file`. With high-cardinality domains at ~10⁴ patterns this is ~10⁸ per pair and ~10¹¹ overall. It is avoided structurally, not by thresholding:

1. Patterns are collapsed to one `domain_state_id` per (source, domain) **before** any pairing. Cardinality per domain is then bounded by `n_sources`.
2. Pairing operates on the state vector — length `n_sources` — never on pattern lists.
3. The contingency dict has at most `n_sources` non-empty cells, since each source contributes exactly one cell.
4. No step materialises a pattern × pattern structure at any point. There is no threshold to mis-set and no flag that can re-enable the bad path.

The engine never reads `records.csv`, `phase0_identity_items.csv` (48M rows), or any export JSON.

## B.8 Output artifacts

Written to `--out-dir`, all small enough to attach to a review.

| File | Grain | Approx size |
|---|---|---|
| `manifest.json` | run metadata: evidence version, producer, domain/source counts, params, seed, timings | < 5 KB |
| `schema_summary.json` | per-table row counts, column lists, null counts | < 10 KB |
| `domain_cardinality.csv` | one row per domain: `n_sources_observed`, `n_distinct_states`, `modal_state_share`, `modal_tie`, `unknown_share_mean`, `gate_status`, `gate_reason` | ~40 rows |
| `domain_pair_contingency.csv` | one row per (pair, stratum): `n_both/n_a_only/n_b_only/n_neither`, `n_sources_used` | 780 × strata |
| `domain_association_edges.csv` | one row per (pair, stratum): metrics, p/q values, gate status | 780 × strata |
| `seed_edge_evaluation.csv` | one row per authored seed edge + unmatched discovered edges | ~22 + N |
| `timing.json` | per-stage elapsed | < 2 KB |
| `warnings.json` | structured warnings incl. the `lineage_conditioned: unavailable` record | < 20 KB |

`domain_association_edges.csv` fields: `schema_version, run_id, domain_a, domain_b, stratum_key, stratum_value, n_sources_used, n_both, n_a_only, n_b_only, n_neither, support, cooccurrence_rate, jaccard_descriptive, phi, cramers_v_bc, perm_p_value, perm_null_p95, fdr_q_value, metric_role, gate_status, gate_reason`. Domain pairs are emitted with `domain_a < domain_b` lexicographically so the file is stable and self-deduplicating.

## B.9 Seed-edge evaluation (positive controls, not ground truth)

`analysis/seed_evaluation.py` reads `config/archetype/static_edges_seed.json` **read-only** and maps each edge's (`source_domain`, `target_domain`) onto the discovered pair set. It never gates discovery — it runs after, purely as a report.

`seed_edge_evaluation.csv` classifies each row as:

- `seed_rediscovered` — authored edge present with `gate_status=ok` and `fdr_q < 0.05`, with its measured strength and support;
- `seed_weak` — present and measurable but below threshold (records the actual value, so "weak" is auditable);
- `seed_ungated` — pair excluded by a safeguard, with the `gate_reason` (sparse, ubiquitous, insufficient sources) — **a measurement limitation, not a refutation**;
- `seed_absent_from_population` — one or both domains not observed at all;
- `emergent_only` — a high-strength discovered edge with no authored counterpart.

**Failure to rediscover a seed edge never fails the run or the test suite.** The five causes named in the brief map onto the five classifications above, which is the point of classifying rather than scoring. A `seed_ungated` result says the engine could not look; a `seed_weak` result says it looked and found little. Conflating those would be the error.

**Negative controls.** Rather than hand-authoring an implausible edge set, derive them mechanically: sample 22 domain pairs uniformly at random from pairs that are *not* seed edges and not in the top decile of measured strength, and report their strength distribution as `negative_control_summary` in `manifest.json`. If the authored-seed distribution does not separate from this random baseline, that is a calibration signal about the whole approach. This costs nothing and requires no hand-authoring. A deliberately-implausible hand-authored set can be added later if the random baseline proves too weak a contrast.

## B.10 Tests

New `tests/test_emergent_association.py`, `tests/test_emergent_metrics.py`, `tests/test_emergent_projection.py`. All on synthetic fixtures; no corpus required.

**Metric correctness (known-answer):** phi = 0 for independent; phi = 1 for identical; phi = −1 for complementary; corrected V = 1 for a perfect permutation-coupling; corrected V ≈ 0 for independent at 5 × 5; hand-computed 2×2 checked against a literature value.

**Behavioral:**
- independent domains → |phi| < 0.15 and `fdr_q > 0.05` (tolerance set from the measured null: probe A gave |phi| ≤ 0.049 at N=184);
- perfectly coupled → phi = 1.0 and `fdr_q < 0.01`;
- 70%-coupled → phi in [0.6, 0.8] (measured 0.696);
- sparse domain (observed in 10 of 184) → `gate_reason=sparse_domain`, metrics null, **run does not fail**;
- ubiquitous domain (modal share 100%) → `gate_reason=low_variance_domain`, phi null not 0.0;
- near-unique domain (184 distinct states) → P-STATE suppressed with `gate_reason=near_unique_states`, **and specifically asserts the metric is not 1.0** — the exact artifact probe 1 found;
- zero-cell case → phi null, no `ZeroDivisionError`.

**Structural:**
- **no authored gating** — a fixture where two domains have no `static_edges_seed.json` relationship, no shared target, no chain, and no whitelist entry still produces an edge row. Asserts the emergent path cannot be gated.
- **complete enumeration** — with D domains all passing gates, exactly D(D−1)/2 rows appear per stratum.
- **seed file absence** — with `config/archetype/static_edges_seed.json` removed, the engine runs and emits `seed_edge_evaluation.csv` with a header and zero rows plus a warning. Proves the emergent engine has no hard dependency on authored config.
- **stratification** — a fixture where two domains couple only within `governance_role=Template` yields a strong Template-stratum edge and a weak global edge.
- **determinism** — two runs with the same `--seed` are byte-identical apart from timestamps; permutation results reproduce exactly.
- **pairwise-complete handling** — a source missing domain B is excluded from every B-pair and `n_sources_used` reflects it.
- **unknown-mass exclusion** — unknown observations do not alter `domain_state_id` but do appear in `unknown_share_mean`.

---

# C. Existing-code reuse matrix

| Component | Location | Classification | Justification |
|---|---|---|---|
| `_complete_linkage_clusters()` | `cluster_archetype_signals.py:209` | **Extract & refactor** | Fully generic already: takes `(nodes, pair_value_dict, threshold)` with no archetype coupling. Lift verbatim to `analysis/cluster.py`. Needed in PR 3, not PR 2 — extract when first used, not speculatively. |
| `_jenks_threshold_for_values()` | `cluster_archetype_signals.py:440` | **Extract & refactor** | Generic apart from `log(STAGE, …)`. Lift with an injected logger. Note it thresholds *whatever* is passed — the Jaccard choice lives in the caller, so lifting it does not import the prevalence problem. |
| `jenks_breaks()` | `tools/jenks_utils.py:6` | **Reuse unchanged** | Already standalone, stdlib, documented degenerate-case handling. Import directly. |
| `atomic_write_csv()` / `atomic_write_json()` / `read_csv_rows()` / `read_json()` | `archetype/_common.py:49-91` | **Reuse pattern, reimplement** | Trivial (~40 lines) and correct, but `analysis/` must not import from `tools/archetype/` — that would couple the emergent engine to the authored pipeline, the exact thing the architecture forbids. Copy the *convention* into `analysis/io.py`. |
| Graceful-degradation contract (null signal, never error; explicit `n_*_unavailable`) | `compute_cross_domain_cooccurrence.py` docstring; README | **Reuse the design, not the code** | The single best idea in the existing pipeline. Reappears as `gate_status`/`gate_reason` with metrics null rather than zero. |
| Jaccard computation | `compute_cross_domain_cooccurrence.py` | **Conceptually useful, do not reuse as association** | Measured 0.313 under independence. Reimplement as `jaccard_descriptive()` marked `metric_role=descriptive`. Never threshold on it. |
| Support-gated pair enumeration (`--support-min-files` before descending to join_hash pairs) | `compute_cross_domain_cooccurrence.py` | **Reuse the design, not the code** | The correct funnel shape. PR 2 does not need it, because the state projection makes pattern-pair expansion structurally unreachable rather than merely gated. Retain the idea for any future pattern-grain drill-down. |
| Edge aliasing (collapsing `fill_patterns_drafting`/`_model`, the four `dimension_types_*` tick-mark partitions) | `_common.py:116 build_edge_aliases()` | **Conceptually useful, too coupled** | Encodes real knowledge that D-015 partitions are analytically one family. But it is keyed to edge records, not domains. PR 2 treats each partition as its own domain and lets the data show whether they behave as one — which is a better test of the aliasing assumption than inheriting it. Revisit for PR 3. |
| Two-grain coherence validation (`join_hash` classify → `sig_hash` validate) | `validate_archetype_signals.py` | **Conceptually useful, too coupled** | Sound methodology, wired entirely to promoted archetype definitions. The analogue for PR 3 is: do sources sharing a discovered archetype actually share configurations. Not needed in PR 2. |
| Authored pair gating (`shared_target` / `chain` / `whitelist`) | `compute_cross_domain_cooccurrence.py` | **Do not reuse** | Explicitly forbidden in emergent mode; a test asserts it is absent. |
| `governance_question` partitioning | `cluster_archetype_signals.py` | **Do not reuse** | Authored semantic bucket. Emergent clustering (PR 3) must run over one unpartitioned graph. |
| `static_edges_seed.json` | `config/archetype/` | **Do not reuse as input** | Read-only, post-hoc, evaluation only. Never gates discovery. |
| `derive_scope_key()` | `bundle_analysis/common.py:80` | **Not applicable to PR 2** | Sub-domain partitioning for `object_styles_model`, `object_styles_annotation`, `view_category_overrides`, `dimension_types`, `arrowheads`. Note it keys on **pre-split** domain names, so it yields `""` for the D-015 split names in `pattern_presence_file.csv`. PR 2 works at plain `domain` grain; document as a known simplification. |

---

# D. Local-run plan

Both commands are read-only with respect to everything except `--out-dir`, and require no re-extraction.

**Step 1 — build the evidence bundle.** `--segment-root` is the corpus-wide (root) segment directory produced by `run_segment_orchestrator.py`; it must contain `results/records/file_metadata.csv` and `results/analysis/pattern_presence_file.csv`.

```bash
python evidence/adapters/fingerprint_v21.py \
  --segment-root  "<Fingerprint_Data>/segments/<root_segment>" \
  --out-dir       "./evidence_out" \
  --validate \
  --profile
```

**Step 2 — run the emergent association engine.**

```bash
python analysis/emergent_association.py \
  --evidence      "./evidence_out" \
  --out-dir       "./emergent_out" \
  --min-sources   20 \
  --min-support   5 \
  --permutations  200 \
  --seed          20260918 \
  --strata        global,governance_role,client_label,business_center_label,discipline_label,unit_system \
  --seed-edges    config/archetype/static_edges_seed.json \
  --profile
```

Expect Step 1 in seconds and Step 2 in roughly a minute at `--permutations 200` (extrapolated from the synthetic-data timings; your corpus may differ). Drop to `--permutations 0` for a first smoke run.

**What to send back for review** — all small, none containing model content:

1. `emergent_out/manifest.json`
2. `emergent_out/schema_summary.json`
3. `emergent_out/domain_cardinality.csv` (~40 rows — **the most informative single artifact**; it shows immediately which domains are uniform, which are near-unique, and which carry heavy unknown mass)
4. `emergent_out/domain_association_edges.csv` (global stratum alone is fine if the full file is large)
5. `emergent_out/seed_edge_evaluation.csv`
6. `emergent_out/warnings.json`
7. `emergent_out/timing.json`
8. `evidence_out/manifest.json`

`domain_pair_contingency.csv` is the largest artifact and is only needed if a specific pair looks wrong.

**Before either PR is written, one cheap thing is worth running.** The entire PR 2 design rests on the claim that `n_distinct_states` per domain is well below `n_sources` for most domains. If it turns out that nearly every domain is near-unique, P-STATE is largely useless and P-CONFORM carries the whole result — which would change the metric emphasis, though not the architecture. That can be checked with a throwaway script over `pattern_presence_file.csv` alone, well under 50 lines, producing only `domain_cardinality.csv`. I can write that script first if you want the design validated against your real corpus before committing to PR 2's metric set.

---

# E. Risks and open decisions

**E.1 — State cardinality is the one genuine unknown, and it is a larger risk than it first appears.** Dry-running the pre-check script on a synthetic fixture exposed a subtlety I had underweighted: **state cardinality is driven by subset combinatorics, not by pattern cardinality.** A fixture domain with only **12 distinct patterns**, where each source draws 1–4 of them, produced **122 distinct states across 184 sources** (ratio 0.66). Pattern count being low does not make state count low.

The consequence: near-unique state spaces may be the common case rather than the exception, in which case P-STATE and bias-corrected Cramér's V get suppressed for most domains and **P-CONFORM/phi carries the entire result**. The architecture survives this unchanged — P-CONFORM is a 2×2 projection whose cardinality is fixed at 2 and is immune to the effect — but the metric emphasis, and how much of section B.3 is actually load-bearing, would shift substantially.

If the pre-check shows widespread near-uniqueness, two mitigations are available and should be decided then, not now: (a) treat the *modal pattern only* rather than the full pattern set as the domain state, collapsing subset combinatorics at the cost of discarding minority configurations; or (b) define the state over the intersection with `CORPUS_STANDARD` patterns only, which measures standard-adherence rather than full configuration. Both are small changes to `analysis/project.py::build_domain_states()` and neither touches the contract or the engine.

This is cheap to resolve and worth resolving before either PR is written.

**E.2 — Corpus-modal state is a corpus-relative baseline, not a standard.** P-CONFORM measures deviation from what is *most common*, which in a poorly-governed corpus may be itself non-compliant. The output must never call this "conformance to standard". `corpus_classification` in the existing data has the same property and the same caveat. An authored reference model could later supply a true baseline; `compare_reference.py` already has the mechanism. **Decision needed:** ship corpus-modal in PR 2 (recommended — it needs no curation), or block on an authored reference.

**E.3 — Strata will fragment fast.** 184 sources across role × client × discipline × business centre will leave many strata under 20. The design degrades gracefully (counts emitted, metrics null), but the *useful* stratified output may be limited to `governance_role` and possibly `client_label`. This is a property of corpus size, not of the design. Expect the global and role strata to carry most of the signal.

**E.4 — Association is not direction and not causation.** A strong A–B edge says they move together. It cannot say A drives B, or that a third domain drives both. With no lineage, confounding by shared template origin is unresolvable. The outputs are named `association`, not `influence` or `dependency`, deliberately.

**E.5 — D-015 partition independence is an assumption under test.** Treating `dimension_types_linear` and `dimension_types_angular` as separate domains will likely produce very strong edges between them, because they are one family split by architecture. That is *correct behaviour* — and it also means the top of the edge list may be dominated by within-family pairs that are structurally trivial rather than governance-interesting. **Decision needed:** annotate edges with `same_domain_family` (derivable from `policies/cross_domain_alignment_keys.json`) so they can be filtered in review without being hidden. Recommended; cheap.

**E.6 — `used_state` availability is uncertain.** The `used` view requires bundle_analysis output, which per `CLAUDE.md` currently fails at near-100% on `run_type=bundle` runs (the `clear_stale_name_all` / `WinError 5` issue). PR 1 treats `used` as optional and PR 2 will simply have no `used_state` stratum if it is missing. No repair is attempted in either PR. If the All-vs-Used distinction matters to you analytically, that failure becomes a blocker for a later PR and should be scheduled on its own.

**E.7 — New top-level packages.** `evidence/` and `analysis/` sit alongside `core/`, `domains/`, `tools/`. This is deliberate — `analysis/` must not live under `tools/`, or the "no import from `tools/`" boundary becomes unenforceable by convention. **Decision needed:** confirm the top-level placement, and whether this warrants a D-number in `DECISIONS.md`. Recommended: yes, one decision covering the producer/evidence/analysis boundary — not because hashes change (they do not), but because the boundary is the durable architectural commitment.

**E.8 — Two pipelines answering one question.** After PR 2 the repo contains an authored archetype pipeline (inert) and an emergent one (working). Without a clear statement of which answers what, this will confuse the next reader. Recommend a short `analysis/README.md` in PR 2 stating the two-mode architecture, and a pointer from `tools/archetype/README.md`. **This is documentation, not consolidation** — the brief is explicit that both modes are preserved.

**E.9 — Scope discipline.** PR 2 deliberately stops before clustering. `domain_association_edges.csv` is a weighted edge list, so community detection in PR 3 is a small addition. Resist adding it to PR 2: the association numbers need to be reviewed and believed before anything clusters on top of them.

---

## Appendix — Stage 5 (whole-file archetypes) readiness

Verified as requested; not implemented here.

**What already supports file→archetype analysis:** `tools/archetype/assign_archetype_classifications.py` (474 LOC) classifies every file against promoted archetypes, and `cluster_archetype_signals.py` rolls classifications up to file × governance_question × cluster grain. Both are authored-mode: they require `archetype_definitions.json` and partition by `governance_question`.

**What blocks emergent whole-file clustering:** only those two authored dependencies. Nothing structural.

**Confirmed:** the earlier finding holds — emergent whole-file archetypes depend only on the file-level presence matrix. The per-(source, domain) `domain_state_id` vector built in PR 2's `analysis/project.py` **is** the file configuration vector. A source is then a 40-dimensional categorical vector, and sources cluster by similarity over it.

**Minimum later work:** a `analysis/source_archetype.py` computing pairwise source similarity (Hamming or weighted agreement over the 40-dim state vector — 184²/2 = 16,836 pairs, trivial), then reusing the lifted `complete_linkage_clusters()`, emitting neutral `archetype_001…` IDs plus per-cluster domain-agreement profiles. It needs **no new extraction, no new evidence field, and no dependency on PR 2's association output** — only on `analysis/project.py`. That makes it a genuinely small PR 3, and it is the one that most directly answers "six independent failures, or one coherent alternate archetype".
