# Capability Gap — Emergent Standards & Archetype Analysis (Stage 0)

Status date: 2026-09-17
Scope: inventory only. No code changed.
Method: static read of the repository at `2fe6de2`. **No export data, no `Results_v21/` tree, and no `Fingerprint_Data/` root is present in the repo**, so every quantity below is derived from code, policy files and contracts — not measured against a live corpus. Compute figures are explicitly labelled theoretical.

---

## Headline findings

1. **A cross-domain archetype pipeline already exists** — `tools/archetype/`, 10 modules, 5,555 LOC. It already implements Jaccard co-occurrence, containment, a Jenks-derived coupling threshold, complete-linkage signal clustering, file→archetype classification, and a separate `sig_hash`-grain coherence validation pass. This is substantially more than "notes only."

2. **But it is edge-seeded, not data-driven.** It can only see relationships someone wrote down first: 22 hand-authored structural edges in `config/archetype/static_edges_seed.json`, and pair enumeration is *gated* on an authored governance relationship (`shared_target` / `chain` / `whitelist`). It cannot discover that two domains co-vary unless an edge already connects them. This is the single largest divergence from the brief's "do not require authored semantic buckets as an input."

3. **The pipeline cannot currently run end-to-end.** `config/archetype/archetype_definitions.json` — required by stages 3 and 4 and by everything downstream — **is not in the repository** (only `static_edges_seed.json` is). `CLAUDE.md` and `tools/archetype/README.md` both document it as present. The pipeline is parked at the DP1 human-curation gate and the curation artifact was never committed.

4. **No association statistics exist beyond set overlap.** Zero occurrences of Cramér's V, mutual information, normalized MI, correlation over derived metrics, or community detection anywhere in the codebase. Shannon entropy exists (`tools/extractor.py:862`) but only as *within-domain* pattern-share concentration, never as a cross-domain association measure.

5. **No context conditioning exists anywhere.** Every association in the repo is unconditional. Nothing stratifies, residualizes, or conditions on role/project/client.

6. **There is no true lineage in the data, and this is the binding constraint on Stage 3.** `lineage_hash` (`domains/identity.py:313`) is a *self*-identity hash over `central_path`, `central_path_norm`, `filename`, `is_workshared`, `project_title`. It identifies a file; it does **not** link a child to the template or container it came from. No `parent_template`, `source_template`, `created_from` or equivalent exists in any extractor. See Q7.

7. **The evidence contract is roughly 80% present as data and 0% present as a contract.** `pattern_presence_file.csv` is already a file × domain × pattern sparse presence matrix. `phase0_records.csv` is already an Observation table. `file_metadata.csv` is already a Source/context table. What is missing is a *versioned logical model* — consumers hardcode filenames and column names throughout, and `schema_version` is `"2.1"`, the CSV serialization version, not a logical-model version.

---

## A. Existing capability map

Legend: **Exists** / **Partial** / **Missing**

### Identity

| Capability | Status | Reference |
|---|---|---|
| Domain signature generation | Exists | `core/record_v2.py`, per-domain `extract()` in `domains/*.py` |
| `sig_hash` | Exists | inline at extraction; policy-driven reconstruction at `core/sig_hash_builder.py` (`build_sig_hash_from_policy`, `apply_sig_hash_policy_to_record`) |
| Signature-policy loading | Exists | `core/sig_hash_policy.py`; policy at `policies/domain_sig_hash_policies.json` (40 domains), generated from `contracts/domain_identity_keys_v2.json` by `tools/generate_sig_hash_policy.py` |
| Normalization before hashing | Exists | `core/canon.py` (canonicalization + the 3 sentinels), `core/hashing.py` |
| Domain-level identity equivalence | Exists | `join_hash` via `core/join_key_builder.py` + `policies/domain_join_key_policies.json` (41 domains) |
| Name-identity projection (names as a *separate* identity axis) | Exists | `core/name_key_builder.py`, `policies/domain_name_key_policies.json`, `tools/apply_name_key_policy.py`, `tools/name_key_rollup.py` |

The name-identity projection is worth flagging: it gives a ready-made second identity axis (config identity vs. name identity) that the archetype work can use to separate "same behaviour, different name" from "same name, different behaviour" without new extraction.

### Relationship / comparison

| Capability | Status | Reference |
|---|---|---|
| Containment | Exists | `tools/compare_cross_segment.py` (both directions, all-view and used-view), `tools/compare_governance_populations.py` |
| Jaccard | Exists | same; plus `tools/archetype/compute_cross_domain_cooccurrence.py` |
| Reference/target comparison | Exists | `tools/compare_reference.py` (orchestration only; delegates maths to `bundle_analysis/step_compare.py`) |
| Domain join policies | Exists | `core/join_key_policy.py`, `tools/discover_join_policy.py`, `tools/apply_join_policy.py` |
| Cross-role analysis | Partial | role appears as a *segment dimension* (`tools/build_segment_manifest.py` `DIMENSION_CONFIG`) and as disjoint populations (`tools/governance_manifest.py`); role-to-role propagation is expressed as directed `comparison_type` edges in `cross_segment_summary.csv`, consumed by `tools/analyze_promotion_candidates.py` |
| Comparison matrices | Exists | `cross_segment_summary.csv`, `cross_segment_pooled.csv` (N-1 pooled), `project_mean_file_pair_jaccard_matrix.csv` |
| **Cross-*domain* relationships** | **Partial** | only `tools/archetype/` — and only along authored edges. See below. |

### Context

| Variable | Status | Reference |
|---|---|---|
| `export_run_id` / `file_id` | Exists | `tools/extractor.py` `meta_core` |
| `governance_role` | Exists (inferred) | `policies/governance_role_path_patterns.json` — **4 path-substring rules only** (`containers`→Container, `project_templates`+filename `generic`→Generic, `project_templates`→Template, `autodesk docs`→Project), first match wins; hand-overridable in `file_metadata.csv` |
| `project_id` / `project_label` | Exists | `meta_core`, sticky annotation |
| `client_label` | Exists (hand-curated) | `file_metadata.csv` annotation column |
| `business_center_label`, `discipline_label`, `collection_label` | Exists (hand-curated) | same |
| `unit_system` | Exists | derived (`_derive_unit_system`) with sticky override |
| Revit version / build | Exists | `revit_version_number`, `revit_version_name`, `revit_build` |
| `is_workshared` | Exists | `meta_core` |
| `central_path_norm` | Exists | `docs/CENTRAL_PATH_NORM_RULE.md` |
| All vs. Used | Exists | `bundle_analysis` all/used views; `tools/compute_latent_purgeable.py`; surfaced as `all_*`/`used_*` column pairs in `cross_segment_summary.csv` |
| **Template/container → project lineage edge** | **Missing** | no such field is extracted anywhere. `lineage_hash` is not this. |

### Aggregation

| Capability | Status | Reference |
|---|---|---|
| Per-domain / per-file counts | Exists | `pattern_size_records`, `pattern_size_files` in `domain_patterns.csv` |
| Configuration frequency | Exists | `domain_patterns.csv`, `pattern_presence_file.csv` (`pattern_share_pct`) |
| Containment rates | Exists | `cross_segment_summary.csv` |
| Distinct signature counts | Exists | `pattern_diagnostics.csv` (`pattern_count`, HHI, effective clusters, `entropy_index`) |
| Cross-project reuse | Exists | `pattern_reuse_distribution.csv`, `pattern_reuse_summary_by_domain.csv` |
| All-vs-Used differences | Exists | paired `all_*`/`used_*` columns throughout |
| Concentration metrics | Exists | `docs/METRICS.md`; HHI + effective-cluster contracts |
| **Persistence rates** (survives downstream) | **Partial** | derivable from containment + role, not computed as a first-class metric |
| **File × domain × configuration matrix** | **Exists** | `pattern_presence_file.csv` — this is the key reusable aggregate |

### Previous archetype work — detailed status

**Production code: no. Experimental code: yes, and substantial.**

| Module | LOC | Role |
|---|---|---|
| `generate_reference_graph.py` | 299 | Stage 0 — resolve which authored edges have data backing |
| `build_cross_domain_items.py` | 285 | Stage 1 — materialize (file, edge) firings with join hashes |
| `compute_cross_domain_cooccurrence.py` | 305 | Stage 2 — **Jaccard, containment both directions, support, n_both/n_a_only/n_b_only/n_neither** at edge-pair grain; join_hash-pair patterns under a support floor |
| `generate_archetype_candidates.py` | 342 | Stage 3 — emit candidate definitions for human review |
| `assign_archetype_classifications.py` | 474 | Stage 4 — classify every file against *promoted* archetypes |
| `validate_archetype_signals.py` | 458 | Stage 5 — re-resolve at `sig_hash` grain to test within-archetype coherence |
| `cluster_archetype_signals.py` | 801 | **Jenks-derived coupling threshold + complete-linkage clustering** of co-varying signals, then rollup to cluster grain |
| `discover_vfd_edges.py` | 1,353 | dynamic edge discovery from View Filter Definition parameter references (the one module with real test coverage) |
| `prepare_archetype_review.py` | 1,048 | human-review packaging |
| `_common.py` | 190 | shared IO |

What is genuinely reusable, and good:

- The **graceful-degradation contract** is well-specified: an unavailable edge produces a null signal with explicit `n_*_unavailable` counts, never an error. That discipline should survive into any new layer.
- **Complete-linkage** (not single-linkage) clustering, explicitly to prevent chain bridging — a signal joins a cluster only if it is above threshold against *every* existing member.
- **Jaccard rather than containment** for coupling, explicitly reasoned about the asymmetry failure mode where a rare signal that is a strict subset of a common one scores a perfect containment.
- **Jenks natural breaks** (`tools/jenks_utils.py`) to derive the threshold from the data rather than hardcoding it, with a logged fallback.
- **Two-grain methodology**: `join_hash` grain for classification, `sig_hash` grain for coherence validation.
- Stdlib-only. Atomic writes. `--dry-run` on every stage. Row counts logged to stderr.

What blocks reuse as-is:

- **Authored-edge dependency.** 22 seed edges derived by hand from `make_identity_item()` call sites. 41 domains admit 820 unordered pairs; 22 edges cover ~2.7% of the possible relationship space, and only the relationships someone already suspected.
- **Authored pair-eligibility gate.** `compute_cross_domain_cooccurrence.py` skips any pair that is not `shared_target`, `chain`, or explicitly whitelisted. An emergent relationship between two structurally unconnected domains is unreachable by construction.
- **Authored partition for clustering.** `cluster_archetype_signals.py` builds one signal graph *per `governance_question`* — a human-assigned label. Clusters cannot cross governance questions.
- **Missing curation artifact.** `config/archetype/archetype_definitions.json` absent → stages 3–5 and the clustering chain are inert.
- **Largely untested.** A single test file, `tests/test_discover_vfd_edges.py`, dynamically loads three modules (`discover_vfd_edges.py`, `generate_reference_graph.py`, `build_cross_domain_items.py`). The remaining six stage modules — including all of the co-occurrence, classification, validation and clustering logic — have **no test coverage at all**: roughly 3,600 of 5,555 LOC, and specifically every module that produces an analytical conclusion.
- **Not wired into any orchestrator.** No reference from `run_extract_all.py`, `run_segment_orchestrator.py`, `discovery_orchestrator.py`, `corpus_update_runbook.py`, or any `.ps1` runbook. Manual CLI only.
- **Not ratified.** `DECISIONS.md` contains exactly one incidental mention of "archetype" (line 1124, about a BuiltInParameter lookup table). There is no D-number governing this subsystem.

Keyword sweep results, for completeness — `archetype` 15 files, `cluster` 72, `jaccard` 34, `cooccur` 4, `cross_domain` 8, `multi_domain` 3, `latent` 7, `association` 2 (both incidental), `correlation` 2 (both incidental), `entropy` 5 (all within-domain concentration), `community` / `network` (all unrelated — Dynamo runner, repo tooling), `mutual_information` 0, `cramer` 0, `covariance` 0.

---

## B. Architecture recommendation

### Current architecture

    Revit/Dynamo (runner/run_dynamo.py)
      -> *.details.json / *.index.json
        -> tools/run_extract_all.py stage machine
             flatten (T0) -> sig_hash (T0.5) -> discover (T1) -> apply (T2)
             -> placeholders (T2b) -> patterns / authority / split / flat_tables
        -> Results_v21/phase0_v21/{file_metadata, phase0_records,
             phase0_identity_items, identity_items_by_domain/*}.csv
        -> results/analysis/{domain_patterns, pattern_presence_file,
             pattern_diagnostics, authority_patterns}.csv
          -> build_segment_manifest -> run_segment_orchestrator
               -> bundle_analysis (all/used) -> membership_matrix.csv
            -> compare_cross_segment / compare_governance_populations
              -> generate_governance_narrative (deterministic, no LLM)

    tools/archetype/  ── detached, manual, edge-seeded, blocked at DP1
      config/archetype/static_edges_seed.json (22 hand-authored edges)
        -> reference_graph -> cross_domain_items -> cooccurrence
          -> candidates -> [MISSING archetype_definitions.json] -> classify
            -> validate -> cluster_archetype_signals

Every downstream consumer reaches *through* Fingerprint-specific filenames and column names. There are ~40 distinct hardcoded CSV basenames across `tools/`.

### Minimum-change target architecture

Do **not** rewrite. The measurement layer is sound and the aggregates you need already exist. Insert one thin seam and build the new analysis behind it.

    [unchanged extraction + stage machine + segment/bundle pipeline]
        |
        v
    evidence/  (NEW — small, versioned, no Revit, no Fingerprint filenames)
      contract.py    logical model + schema version (evidence.v1)
      adapters/
        fingerprint_v21.py   projects existing CSVs -> evidence.v1
      validate.py    reconciliation + invariants
        |
        v
    analysis/  (NEW — consumes evidence.v1 only)
      aggregate.py   sufficient statistics (file x domain x config presence)
      associate.py   support, Jaccard, Cramer's V, MI/NMI  [Stage 2]
      condition.py   stratified + conditional association    [Stage 3]
      graph.py       weighted graph + community detection    [Stage 4]
      archetype.py   whole-file configuration archetypes     [Stage 5]

    tools/archetype/  -> either retire, or refactor to read evidence.v1
                         and contribute its authored edges as *priors*

Three properties make this a seam rather than a rewrite:

- The adapter is a **projection, not a transformation**. `pattern_presence_file.csv` already *is* the observation grain the analysis wants. The adapter renames and validates; it does not recompute.
- **Nothing upstream changes.** No extractor, no policy, no hash, no existing CSV column. The existing result contract is untouched (see Q17).
- **The analysis layer never imports from `tools/`**, mirroring the existing rule that extraction never imports from `tools/`.

On `tools/archetype/`: do not delete it and do not build on it. Its *methodology* (Jaccard coupling, complete linkage, Jenks thresholds, graceful degradation, two-grain validation) is the best thinking in the repo on this problem and should be lifted into `analysis/`. Its *inputs* (authored edges, authored pair gate, authored governance questions) are exactly what the new work must not inherit. The clean resolution is to treat the 22 seed edges as a **labelled evaluation set**: if a data-driven association engine cannot rediscover most of the 22 known-structural edges, the engine is wrong. That turns a liability into the only ground truth available.

---

## C. Implementation estimate

Relative sizing only. "S" ≈ a focused PR, "M" ≈ a PR with meaningful new logic and tests, "L" ≈ multi-PR.

| Stage | Files / modules | Complexity | Dependencies | Migration risk | Tests needed |
|---|---|---|---|---|---|
| **1. Evidence contract + adapter** | new `evidence/` (~4 files); zero existing files modified | **S–M** | none | **Very low** — additive, read-only | Row/count reconciliation against source CSVs; sentinel round-trip; schema-version pin; a second synthetic producer emitting the same contract |
| **2. Descriptive association** | new `analysis/aggregate.py`, `associate.py` | **M** | Stage 1 | Low | Known-answer fixtures for Jaccard / Cramér's V / MI; degenerate cases (single-value domain, zero-variance, empty intersection); support-floor behaviour; **rediscovery of the 22 seed edges** |
| **3. Context conditioning** | new `analysis/condition.py` | **M–L** | Stage 2; *blocked in part by the lineage gap* | Medium — this is where wrong conclusions get manufactured | Stratified-vs-pooled disagreement (Simpson's paradox fixture); small-stratum suppression; explicit "cannot separate inheritance" assertion where lineage is absent |
| **4. Graph + communities** | new `analysis/graph.py` | **S–M** | Stage 2/3 | Low | Determinism under node-order permutation; stability under resampling; neutral-ID assignment is stable across runs |
| **5. Whole-file archetypes** | new `analysis/archetype.py` | **M** | Stage 1 (only) | Low | Recovery of planted archetypes in synthetic corpora; distinguishing "one coherent alternate" from "N independent deviations" |
| **6. Governance interpretation** | out of scope | — | — | — | — |

Stage 5 depends only on Stage 1, not on Stages 2–4. It can be built in parallel and will likely produce a usable result soonest, because `pattern_presence_file.csv` is already the exact input it needs.

Two additional non-code items that gate real conclusions:

- **Decide the fate of `tools/archetype/`.** Leaving 5,555 LOC of largely untested, unratified, non-running code adjacent to a new subsystem that answers the same question is the main source of future confusion. Either give it a D-number and a test suite, or mark it superseded in `docs/tools_DEPRECATED.md` and lift its methodology.
- **Fix the documentation drift.** `CLAUDE.md` and `tools/archetype/README.md` both document `config/archetype/archetype_definitions.json` as a present, hand-curated file. It is absent.

---

## D. Compute estimate

**All figures below are theoretical.** The repo contains no benchmark results, no profiling output, and no export data. `tools/extractor.py` and `tools/run_segment_orchestrator.py` emit `[patterns_timing]`-style stderr lines at runtime, but no captured timings are committed. The CHANGELOG contains no measured runtimes. Anchors used: the brief's ~3.27M cached rows, ~48M basis items, ~184 files — implying ≈14.7 items/record and ≈17,800 records/file.

### What operates on what

| Step | Operates on | Scale |
|---|---|---|
| RVT extraction | Revit documents | 184 files, in-process in Revit |
| flatten (T0) | raw export JSON | 48M items parsed; files "single-digit MB to well over 100MB" (`tools/run_a_cache.py`) |
| sig_hash (T0.5) | flattened rows | 3.27M records × ~14.7 items |
| apply (T2) | flattened rows | 3.27M records |
| patterns | records | 3.27M records → pattern grain |
| bundle_analysis | `pattern_presence_file.csv` | presence grain |
| compare_cross_segment | `membership_matrix.csv` | segment × segment |
| **proposed Stages 2/4/5** | **`pattern_presence_file.csv`** | **presence grain — see below** |
| proposed Stage 3 | presence grain + context | presence grain |

### Sizing the proposed work

The critical number: `pattern_presence_file.csv` has one row per (file, domain, pattern). With 184 files and 41 domains, and assuming on the order of tens of distinct patterns per (file, domain) cell, this lands in the **10^5 rows** range — roughly **two orders of magnitude smaller than the 3.27M record cache and ~three orders below the 48M basis items**.

Against that input:

- **Domain-grain association.** 41 domains → 820 unordered pairs. Over 184 files with binary presence: ~151,000 cell comparisons. Sub-second; memory in the low MB. A full 41×41 association matrix under any of Jaccard / Cramér's V / MI is **negligible** — comparable to a rounding error against one RVT extraction.
- **Graph construction + community detection.** 41 nodes, ≤820 edges. Instant. Any community algorithm (Louvain, label propagation, or the existing complete-linkage approach) is trivial at this size, and a pure-Python implementation is entirely adequate — no new dependency is justified by scale.
- **Whole-file archetypes.** 184 × P sparse binary matrix, P = distinct corpus-wide patterns. Even at P = 50,000 the sparse representation is the same ~10^5 nonzeros. All-pairs file similarity is 184²/2 = **16,836 pairs**. Trivial.
- **Conditional analysis (Stage 3).** The only stage with real cost risk, and the risk is *statistical*, not computational: 184 files stratified by role × client × discipline × unit system fragments fast. Many strata will have n < 5. The expensive mistake is fitting hundreds of models on strata too small to support them — the cost is wrong answers, not CPU.

### Where a naïve implementation explodes

The danger is not files and not domains. It is **pattern-grain pair enumeration**:

    BAD:  for each of 820 domain pairs:
            for each pattern_a in domain_a (up to ~10^4):
              for each pattern_b in domain_b (up to ~10^4):
                for each of 184 files: ...

High-cardinality domains are the trigger. `loaded_family_types`, `view_filter_definitions`, `dimension_types_linear` (39 allowed items) and `view_category_overrides_model` (21) are the likely offenders. At 10^4 patterns each, one domain pair is 10^8 cells; 820 pairs is 10^11. **That dwarfs the entire existing pipeline.**

The existing `compute_cross_domain_cooccurrence.py` already solves this correctly and the solution should be copied: compute co-occurrence at the *edge/domain* grain first, and only descend to pattern-pair grain for pairs that clear `--support-min-files`, emitting only patterns that themselves clear the floor. That is the computational funnel the brief asks for, already implemented.

The second explosion risk is re-reading `phase0_identity_items.csv` (48M rows) inside any per-pair loop. The mitigation already exists structurally: items are **sharded per domain** at `identity_items_by_domain/<domain>.csv` (`tools/extractor.py:445, 1238-1288`), so a domain-pair join touches two shards, never the full 48M.

### Aggregates worth persisting

Compute once, reuse everywhere:

1. `(file, domain) -> set of pattern_ids` — the sparse presence matrix (already materialized as `pattern_presence_file.csv`).
2. `(file, domain) -> distinct pattern count, dominant pattern, HHI` — already in `file_domain_concentration.csv` and `pattern_diagnostics.csv`.
3. `(domain, pattern) -> file support count` — already `pattern_size_files` in `domain_patterns.csv`.
4. **New:** `(domain_a, domain_b) -> n_both, n_a_only, n_b_only, n_neither` at file grain. This 2×2 contingency table per pair is the sufficient statistic for Jaccard, Cramér's V, MI, and every other measure in the brief. 820 rows. Compute once; every association measure is then a closed-form read off it.
5. **New:** the same 2×2 table per context stratum, for Stage 3.

Item 4 is the single highest-leverage artifact in this whole proposal: it collapses the entire Stage 2 question into an 820-row table that fits in memory and recomputes in seconds.

### Incremental cost, stated plainly

Relative to one full corpus run today (RVT extraction + flatten of 48M items + sig_hash + apply + patterns + per-segment bundle analysis across 74+ bundle runs), Stages 1, 2, 4 and 5 as specified are **a small single-digit percentage at most, and plausibly under 1%** — *provided* they consume `pattern_presence_file.csv` and never re-read the item population. Disk impact is similarly small: an 820-row association matrix, a 41-node graph, and a 184-row file-archetype assignment are kilobytes against a multi-gigabyte cache.

Stage 3's cost depends entirely on design discipline, and its risk is statistical rather than computational.

---

## E. Answers to the 18 questions

**1. Which required capabilities already exist?**
Identity (all), set-based relationship measurement (Jaccard, containment, both directions, all/used views), context metadata (8 of 9 desired variables), aggregation (all except first-class persistence rates), and — unexpectedly — a complete if edge-seeded cross-domain co-occurrence and clustering pipeline. Missing: any association measure beyond set overlap, any context conditioning, any community detection, any lineage edge.

**2. Which existing outputs can be reused unchanged?**
`pattern_presence_file.csv` (the file × domain × pattern presence matrix — reusable as-is and central), `file_metadata.csv` (the context table — reusable as-is), `domain_patterns.csv` (pattern vocabulary + support), `pattern_diagnostics.csv` (per-domain concentration), `phase0_records.csv` (the observation grain), `identity_items_by_domain/*.csv` (per-domain shards, for drill-down only), `membership_matrix.csv` (all/used views). None require modification.

**3. Is there already a sufficiently generic normalized intermediate representation?**
**No — the data is generic, the interface is not.** The rows in `phase0_records.csv` map almost one-to-one onto the brief's Observation model. But there is no versioned logical contract, no producer-independent naming, and ~40 hardcoded CSV basenames across `tools/`. `schema_version = "2.1"` is the CSV serialization version, not a logical-model version. A second producer could not feed this pipeline without reverse-engineering filenames and column semantics from code.

**4. If not, what is the smallest refactor needed to create one?**
An additive `evidence/` package: a logical model (`evidence.v1`), one adapter that projects the existing CSVs onto it, and reconciliation tests. Zero existing files modified. The mapping is close to mechanical:

| evidence.v1 | Source today |
|---|---|
| `source_id` | `export_run_id` |
| `source_role` | `governance_role` |
| `domain` | `domain` |
| `item_id` | `record_pk` |
| `sig_hash` | `sig_hash` |
| `join_key` | `join_hash` |
| `config_id` | `pattern_id` |
| `used_state` | all/used view |
| `count` | `instance_count` / `pattern_size_records` |
| `context_id` | `segment_id` or composite of `file_metadata.csv` dimensions |
| `provenance_ref` | **no source — see Q7** |

**5. Are cross-domain relationships currently computed anywhere?**
Yes, in exactly one place: `tools/archetype/compute_cross_domain_cooccurrence.py`. It computes `n_both`/`n_a_only`/`n_b_only`/`n_neither`, `support_pct`, `jaccard`, and containment in both directions, at edge-pair grain over per-file activation sets — with edge aliasing that correctly collapses domain-family partitions (`fill_patterns_drafting`/`_model`, the four `dimension_types_*` tick-mark partitions) onto canonical edges. But it only enumerates pairs that pass an authored eligibility gate, over an authored 22-edge seed. Nothing outside `tools/archetype/` computes any cross-domain relationship.

**6. Is any archetype/clustering work already present?**
Yes — experimental code, 5,555 LOC, 10 modules, not production. It implements Jenks-derived thresholds, complete-linkage clustering, file→archetype classification and a second-grain coherence validation. Coverage is limited to the three upstream modules exercised by `tests/test_discover_vfd_edges.py`; the six modules that compute co-occurrence, classify, validate and cluster have none (~3,600 LOC). It is unwired (no orchestrator references it), unratified (no D-number), and **currently non-runnable** because `config/archetype/archetype_definitions.json` is absent from the repo despite being documented as present.

**7. Can current containment/provenance information distinguish inheritance from residual association?**
**Partially, and this is the most important limitation in the review.**

What you *can* do: stratify by `governance_role` (Template/Container/Project/Generic), and use directed role-to-role containment from `cross_segment_summary.csv` to establish that a configuration present in a Template is also present in a Project. That supports the population-level claim "this pattern is available to be inherited."

What you *cannot* do: establish that a *specific* project inherited from a *specific* template. No parent/child edge is extracted. `lineage_hash` is a self-identity hash over `central_path`, `central_path_norm`, `filename`, `is_workshared` and `project_title` (`domains/identity.py:236-313`) and its own docstring calls it "heuristic, non-authoritative." `governance_role` itself is inferred from **4 path-substring rules**.

The practical consequence: Stage 3 can do **stratified** association (association within role, within client, within segment) — which is genuinely useful and cheap. It cannot do true inheritance control, because the inheritance graph does not exist in the data. Two domains co-varying inside the Project stratum may still both derive from a shared template that nobody recorded.

This should be stated as an explicit limitation of any Stage 3 output rather than papered over. If real provenance becomes available later — a template GUID, an ACC document lineage, or even a curated `parent_source_id` column in `file_metadata.csv` — the conclusions strengthen materially. A curated column is by far the cheapest path and would slot into `evidence.v1` as `provenance_ref` with no algorithm changes.

**8. Which contextual variables are available today versus merely desirable?**

*Available and reliable (extracted):* `export_run_id`, `project_id`/`project_label`, `revit_version_number`/`_name`/`_build`, `is_workshared`, `central_path`/`central_path_norm`, `unit_system` (derived), All-vs-Used state.

*Available but human-curated* (present only where a curator filled them in; `build_segment_manifest.py` hard-requires several and blocks the whole manifest if any row is missing one): `client_label`, `discipline_label`, `business_center_label`, `collection_label`.

*Available but inferred from 4 path rules:* `governance_role`.

*Desirable and absent:* template/container → project lineage; discipline at record rather than file grain; time (no revision or model-age dimension exists anywhere — worth noting, since "persistent drift" in the brief's governance motivation is implicitly temporal and cannot currently be measured).

**9. Current number of analyzed domains and approximate cardinalities?**
**40 domains** in `policies/domain_sig_hash_policies.json`; **41** in `policies/domain_join_key_policies.json` (the extra is `view_category_overrides`, the routing coordinator). Identity-item cardinality per domain, from the sig-hash policy (allowed / required):

- Largest: `dimension_types_linear` 39/8, `dimension_types_angular` 36/8, `dimension_types_spot_coordinate` 32/12, `dimension_types_spot_elevation` 31/11, `dimension_types_diameter` and `_radial` 26/8, `dimension_types_spot_slope` 22/4, `view_category_overrides_model` 21/3, `identity` 17/1, `view_category_overrides_annotation` 16/3, `loaded_family_types` 12/2, `text_types` 12/7, `units` 11/2.
- Smallest: the five `view_templates_*` partitions and `view_filter_applications_view_templates` at 1/1; `materials` 1/2; `line_patterns` 2/1; `phases` 2/2.

Record-count cardinality per domain is **unmeasurable from the repo** — no data present. The dimension_types family and `loaded_family_types` are the likely high-cardinality domains and therefore the ones to guard against in pair enumeration.

**10. Which domains are single-valued versus multi-valued per source?**
**Single-valued (one record per file):** `identity` (`domains/identity.py:309`, "single-record domain"), `units_doc` (`domains/units.py:503`, "the single document-level units summary record"), `worksets_doc` (`domains/worksets.py:627`, same phrasing). **Near-single:** `browser_organization` — at most 3 records, one per resolvable organization type (`domains/browser_organization.py:6-14`). **All other ~37 domains are multi-valued per source.**

This matters directly for statistics: the 4 single/near-single domains admit true per-file categorical variables and support Cramér's V and MI cleanly. The other 37 are *sets* per file, and for those the natural per-file variable is presence/absence of a given `pattern_id`, or a set-valued summary — not a single categorical value.

**11. Which statistics are appropriate for the actual data types?**

- **`sig_hash` / `join_hash` / `pattern_id` are identities, not measurements.** Never encode them numerically. The brief's warning is correct and should be an enforced invariant.
- **Presence/absence of a configuration in a file** → binary. Use Jaccard, support, lift, phi coefficient, or MI on the 2×2 table.
- **Domain pair co-occurrence** → the 2×2 contingency table (`n_both`, `n_a_only`, `n_b_only`, `n_neither`) is the sufficient statistic. Jaccard, phi, Cramér's V (which for 2×2 equals |phi|) and MI all read off it in closed form.
- **Single-valued categorical domains** (the 4 above) → Cramér's V and normalized MI on the full contingency table are appropriate and meaningful.
- **Multi-valued domains** → treat as set-valued. Jaccard between per-file pattern sets; or binarize per pattern and compute association per (pattern_a, pattern_b) only above a support floor.
- **Derived numeric metrics** (HHI, effective cluster count, `entropy_index`, `pattern_share_pct`, deviation scores) → these *are* continuous and Spearman/Pearson between them across files is legitimate. This is the one place ordinary correlation is defensible.
- **Small-sample caution:** at 184 files, a 2×2 table with an expected cell below ~5 will not support a chi-square-based statistic. Report raw counts alongside every derived measure (as `compare_cross_segment.py` already does) and set an explicit support floor.

**12. Where would a naïve implementation create Cartesian explosions?**
Three places. (a) Pattern × pattern × file across all 820 domain pairs — up to ~10^11 cells; mitigate with the existing domain-grain-first, support-floor-gated funnel. (b) Re-reading the 48M-row item population inside a per-pair loop; mitigate by using the existing per-domain shards. (c) Stage 3 conditional modelling fanned out across every (pair × stratum) combination without a support floor — statistically as well as computationally wasteful.

**13. What aggregates/sufficient statistics should be persisted?**
The five listed in section D. The new, highest-leverage one is the **per-domain-pair 2×2 contingency table at file grain** (820 rows), from which every Stage 2 measure is derivable in closed form, plus the same table per context stratum for Stage 3.

**14. Should the evidence layer use current CSVs, Parquet/Arrow, SQLite/DuckDB, or something else?**
**Keep CSV for now.** Justification from measured usage, not preference:

- The analysis inputs are *aggregates*, not the raw population. `pattern_presence_file.csv` at ~10^5 rows and the 820-row association matrix are trivially handled by `csv.DictReader`. Parquet would optimize a bottleneck that does not exist at the analysis layer.
- The repo has a standing stdlib-only rule for `tools/` with a short, explicit exception list. Introducing Arrow or DuckDB across the new layer would be the largest dependency change in the project's history, justified by no measured need.
- The real I/O pressure is upstream — 48M items, export files "well over 100MB" — and that is already addressed by per-domain sharding, streaming writes, and the `(mtime_ns, size)` change-detection cache in `tools/run_a_cache.py`.
- Columnar storage becomes justified if the corpus grows an order of magnitude, or if Stage 3 starts scanning the item population repeatedly. Neither is true today.

The right move is to make the contract **format-agnostic**: define `evidence.v1` as a logical model with a serialization-independent reader, so a Parquet backend can be added later without touching any analysis code. That preserves the option at near-zero cost and is a stronger architectural position than picking a format now.

**15. How much additional disk/memory/CPU per stage relative to today?**
Theoretical, per section D. Stages 1, 2, 4, 5 combined: **small single-digit percent of a full corpus run at most, plausibly under 1%**, with disk impact in kilobytes-to-megabytes against a multi-gigabyte cache. Stage 3 is design-dependent; bounded by a support floor it stays in the same range, and unbounded it becomes the dominant cost while producing unreliable results.

**16. Which stages could run as optional post-processing without rerunning RVT extraction?**
**All of them.** Every proposed stage consumes `pattern_presence_file.csv`, `domain_patterns.csv` and `file_metadata.csv` — all already on disk after a normal run. Nothing requires Revit, Dynamo, or re-parsing export JSON. This mirrors how `core/sig_hash_builder.py` and `core/name_key_builder.py` already reconstruct hashes analysis-side from exported JSON without re-extraction. The new layer should be a post-processing stage, opt-in, exactly like `patterns` and `authority` are today.

**17. How much could be added without changing the existing Fingerprint result contract?**
**All of it.** The proposed work is purely additive and read-only: no extractor change, no policy change, no hash-composition change, no new sentinel, no modification to any existing CSV column. Under the repo's own rules that means no `DECISIONS.md` hash-semantics entry and no golden-file churn. The only changes to existing files would be documentation (`CLAUDE.md` directory map, `docs/` index) and, if the new layer becomes a stage, one entry in `run_extract_all.py`'s `stage_names`. A new D-number should be raised for the architectural boundary itself — the producer/evidence/analysis separation — not because hashes change, but because that boundary is the durable decision.

**18. What tests would demonstrate reproducibility and source-tool independence?**

*Reproducibility:* byte-identical outputs across two runs on the same input; invariance under row-order permutation of every input CSV; invariance of neutral pattern IDs (`pattern_001`, …) under node relabeling; stability of clusters under resampling, reported as a confidence rather than asserted as exact.

*Source-tool independence* — the decisive test: **a synthetic second producer.** Write a small fixture generator that emits `evidence.v1` directly, with no Fingerprint code in the path, and assert the analysis layer produces correct known-answer results on it. If the analysis passes on synthetic evidence that never touched a Revit export, independence is demonstrated rather than asserted. Complement with a null test (shuffled evidence produces no significant associations) and a planted-archetype test (synthetic corpora with a known alternate configuration system are recovered as one archetype, not as N independent deviations).

*Reconciliation:* row counts, distinct `sig_hash` counts, and distinct file counts must match exactly between `evidence.v1` and the source CSVs, per domain. This is the Stage 1 acceptance criterion.

*Regression against prior work:* run the association engine over the same corpus the 22 seed edges were authored against, and report what fraction it independently rediscovers. Anything below a high fraction indicates an engine problem; anything above suggests the authored edges were an incomplete view — which is the hypothesis this whole programme exists to test.

---

## F. Recommended first PR

**Create the `evidence.v1` contract and the Fingerprint adapter. Change no existing analytical behaviour.**

The repo has the *data* for a generic evidence layer but not the *contract*, so the brief's default preference applies.

Scope:

1. `evidence/contract.py` — the Observation and Source/context logical models, with an explicit `evidence_schema_version` distinct from the CSV `schema_version = "2.1"`. Document `provenance_ref` as a defined-but-unpopulated field, so the lineage gap is visible in the contract rather than discovered later.
2. `evidence/adapters/fingerprint_v21.py` — projects `pattern_presence_file.csv`, `phase0_records.csv`, `domain_patterns.csv` and `file_metadata.csv` onto the contract. Pure projection; no recomputation of any hash or pattern.
3. `evidence/validate.py` — reconciliation (row counts, distinct `sig_hash` counts, distinct file counts per domain must match source exactly) plus contract invariants (no sentinel literal in an identity value; `sig_hash` null iff blocked).
4. `tests/test_evidence_contract.py` — reconciliation on a synthetic corpus, order-permutation invariance, and **a second synthetic producer** emitting `evidence.v1` with no Fingerprint code in the path.

Explicitly out of scope for this PR: any statistic, any clustering, any change to `tools/archetype/`, any change to an existing CSV.

Why this PR first: it is the only step that creates leverage for every later stage while carrying near-zero migration risk, and it forces the lineage gap (Q7) to be named in a schema rather than discovered halfway through Stage 3. It is also the step that makes the design principle real — the long-lived product is a generic engine over normalized configuration evidence, with Revit Fingerprint as its first data source.

**Immediate follow-on, and nearly as valuable:** Stage 5 (whole-file archetypes) depends only on Stage 1, reads a file that already exists, and directly answers the brief's sharpest question — whether a project is six independent standards failures or one coherent alternate operating archetype. It is likely to produce a usable result before Stages 2–4 are finished.

**Also worth doing early, cheaply:** resolve the `tools/archetype/` documentation drift and decide the subsystem's status. Leaving 5,555 LOC of largely untested, non-running code that answers the same question next to a new subsystem is the main avoidable source of future confusion.
