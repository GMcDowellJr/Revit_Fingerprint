# Repository Operational Review

## Executive Assessment

- **Single-contributor project** (90 of 100 commits by one developer; 10 AI-assisted) executing a disciplined, milestone-based refactor from an MVP monolith to a modular domain-driven extractor. The refactor plan (M0–M5) appears structurally complete, though several declared milestones are not reflected in updated documentation.
- **Architecture enforcement is test-backed**, not just documented: sentinel policy, cross-domain import prohibition, and FilteredElementCollector indirection are all validated by automated tests. This is unusually rigorous for a project of this scale.
- **No CI/CD pipeline exists.** There are no GitHub Actions, no build scripts, no `requirements.txt`, no `pyproject.toml`. Testing is entirely local and manual. PR templates exist, suggesting a review process, but there is no automated gate.
- **Dual hash mode migration (legacy → semantic) is incomplete.** Legacy remains the default. All domains compute both hashes, but the switchover has not occurred. This is the largest declared-but-unfinished transition.
- **Phase-2 analysis tooling is partially built.** Join-key discovery and population analysis tools exist and appear functional. Two Phase-2 attribute functions are explicitly `NotImplementedError` stubs. Phase 3–6 exist only as a question map — no implementation signals.
- **Join-key policies for 2 domains are explicitly marked "provisional"**, indicating active empirical discovery that has not yet stabilized.
- **Documentation investment is high relative to codebase size** (~140K markdown vs ~1.2M Python). Decisions, invariants, and contracts are formalized and cross-referenced. This documentation acts as a governance layer.
- **The primary runtime (Dynamo CPython3) is untestable outside Revit.** The 155 unit tests are pure Python and verify contracts, policies, and hash determinism — but never exercise actual Revit API extraction. Integration testing requires a live Revit environment with no evidence it runs regularly.
- **No versioning or release mechanism exists.** No tags, no version strings, no changelog entries beyond an "Unreleased" section. The project has no formal notion of a release.
- **Technical debt is well-signposted** (deprecated files, provisional policies, disabled domains with decision references) rather than hidden. The codebase self-documents its known gaps.

---

## Observed Architecture

### Layered Design (Implemented)

```
Layer 0 — Core (17 modules, pure Python)
  ├── Hashing (MD5, CLR/hashlib dual-runtime)
  ├── Canonicalization (3 sentinels: MISSING, UNREADABLE, NOT_APPLICABLE)
  ├── Contract envelopes & status rollups
  ├── record.v2 schema utilities
  ├── Join-key builder & policy loader (with shape-gating)
  ├── FilteredElementCollector caching wrapper
  └── Timing instrumentation (added PR#127)

Layer 1 — Domain Extractors (14 active + 1 disabled + 1 deprecated)
  ├── Metadata: identity (no hash), units
  ├── Global styles: line_patterns → object_styles → line_styles →
  │   fill_patterns → arrowheads → text_types → dimension_types
  ├── Global filters/phases: view_filter_definitions, phases, phase_filters
  ├── Disabled: phase_graphics (D-013, API limitation)
  └── Contextual: view_filter_applications, view_category_overrides, view_templates

Layer 2 — Context Builder (runner-internal)
  └── Progressive ctx population during sequential domain execution

Layer 3 — Runner (run_dynamo.py, 1301 lines)
  └── Dynamo CPython3 entry point; thin_runner.py for environment control
```

### Key Structural Properties

- **No cross-domain imports.** All inter-domain data flows through the context dictionary (`ctx`), populated progressively by the runner. Verified by automated test.
- **Hard dependency enforcement.** `core/deps.py` provides `require_domain()` which raises `Blocked` exceptions when upstream domains fail. Used for `view_category_overrides` (depends on 3 domains) and `view_templates` (depends on 3 domains).
- **Shape-gating system.** Four domains (arrowheads, dimension_types, identity, object_styles) use conditional join-key composition based on discriminator values. Policies defined in `policies/domain_join_key_policies.json`.
- **Dual hash computation.** Every domain computes both legacy (pipe-delimited with sentinels) and semantic (record.v2 identity-basis) hashes. `REVIT_FINGERPRINT_HASH_MODE` selects which is authoritative. Legacy is default.
- **Split export.** Runner can emit index.json (small, canonical), details.json (full payloads), and legacy bundle. Controlled by environment variables.

### Codebase Scale

| Component | Files | Lines (approx.) |
|-----------|-------|-----------------|
| core/ | 17 | 3,200 |
| domains/ | 18 | 8,300 |
| runner/ | 3 | 1,550 |
| tests/ | 30 | 3,250 |
| tools/ | 40+ | 8,000+ |
| validators/ | 1 | 357 |
| Total Python | 95+ | ~25,000 |

---

## Signals of Intent

### Explicit Roadmap Artifacts

| Source | Signal | Quote / Reference |
|--------|--------|-------------------|
| `REFACTOR.md` | Milestone plan M0–M5 | "M5: Upgrade view templates to behavioral fingerprinting" |
| `README.md` | Status declaration | "Transitioning from MVP to Baseline … not yet a product" |
| `docs/phase2-identity-and-semantic-plan.md` | Three-horizon plan | Near: freeze exporter, design join keys; Intermediate: lock identity, scale pattern; Long: expand semantic insight |
| `docs/analysis-phases-question-map.md` | Six-phase governance ambition | Phase 0 (inventory) through Phase 6 (strategic/predictive) |
| `CHANGELOG.md` | Unreleased section | Lists M4 domains and behavioral view template refactor as pending |
| `policies/domain_join_key_policies.json` | Provisional markers | phase_filters: "treat as provisional"; phases: "provisional pending broader samples" |
| PR templates (`.github/`) | Review process | Verification checklists for Dynamo runs, hash stability, golden comparisons |
| `docs/tools_DEPRECATED.md` | Tool lifecycle management | Explicit deprecation with supersession paths |
| `DECISIONS.md` | 13 architectural decisions | D-001 through D-013, with revision history |
| `INVARIANTS.md` | Non-negotiable rules | Hash determinism, sentinel policy, fail-soft, ordering explicitness |

### Implied Direction

- **Governance tooling ambition.** The `tools/governance/standards_governance_report.py` (16K) and Phase 1–2 analysis runners suggest intent to build a standards compliance workflow, not just an extractor.
- **Multi-project comparison.** Tools like `pairwise_drift.py`, `similarity_compare.py`, and `score_drift.py` imply the system is designed for cross-project fleet analysis.
- **Data-driven policy tuning.** Pareto join-key search and shape-gating inputs suggest join-key policies are derived empirically from real Revit model populations, not guessed.

---

## Implementation vs Intent

### Aligned

| Intent | Evidence of Alignment |
|--------|----------------------|
| Behavior-first hashing (D-001) | All 14 active domains hash behavioral properties; names excluded from hashes (except phases per D-010) |
| Domain isolation (D-011) | Zero cross-domain imports verified by `test_no_direct_filtered_element_collector_in_domains.py` and grep analysis |
| Three-sentinel policy (PR3) | `test_sentinel_policy.py` statically scans all domain source for violations |
| record.v2 contract schema | `validators/record_v2.py` (357 lines) validates all required fields; `test_record_contract_v2.py` exercises it |
| Explicit ordering per domain (D-006) | Runner enforces sequential execution with documented dependency chain |
| Fail-soft (D-005) | Domains emit `<UNREADABLE>`/degraded status rather than crashing; `Blocked` exceptions handled with contract envelopes |
| M0–M3 refactor milestones | Legacy MVP preserved in `legacy/`; core utilities extracted; all original domains modularized |
| M4 milestone (new domains) | view_filter_definitions, phases, phase_filters, phase_graphics all exist |
| M5 milestone (behavioral view templates) | `view_templates.py` (1113 lines) implements full behavioral fingerprinting with dependency resolution |
| Commit discipline | 100 commits follow `feat:/fix:/chore:/refactor:` convention consistently |
| Canonical evidence selector pattern | PRs #106–#119 systematically rolled this out across all 15 domains with per-domain tests |

### Divergent

| Intent | Divergence | Evidence |
|--------|------------|---------|
| Semantic hash mode as preferred direction | Legacy mode remains default | `run_dynamo.py` line ~436: `HASH_MODE = os.environ.get("REVIT_FINGERPRINT_HASH_MODE", "legacy")` |
| CHANGELOG tracks semantic changes | CHANGELOG "Unreleased" section has not been updated for M5 completion, canonical evidence rollout (PRs #106–#127), or timing instrumentation | `CHANGELOG.md` last substantive entry references M4 domains |
| README describes current scope | README lists "9 global + 2 contextual" domains; actual count is 12 global + 2 contextual + 1 disabled | `README.md` lines 36–42 |
| `domain_identity_keys_v2.json` as per-domain key registry | Four domains with join-key policies are missing from identity keys contract: arrowheads, identity, view_category_overrides, view_templates | Cross-reference of `policies/` vs `contracts/` |
| Phase-2 analysis as near-term work | `tools/phase2_analysis/attributes.py` raises `NotImplementedError` for both public functions | Lines 61, 69 |
| PR templates imply review workflow | No CI/CD gates to enforce any of the checklist items (hash stability, golden comparisons, test passage) | No `.github/workflows/` directory |

### Indeterminate

| Area | Why Indeterminate |
|------|-------------------|
| Golden file regression testing | `tests/golden/` directory is empty in the repo; golden files are generated at runtime in Revit. No evidence of how often this runs or whether baselines exist. |
| Revit integration test frequency | `tests/revit/revit_test_runner_pyrevit.py` exists but requires live Revit; no automation evidence |
| Split export adoption | Feature is fully implemented with tests, but no evidence of downstream consumer usage |
| Phase 3–6 governance ambitions | Documented in question map and plan document; zero implementation artifacts exist |
| `view_filters_deprecated.py` cleanup timeline | File is not imported by runner but remains in `domains/` without a removal plan |
| Tool deprecation enforcement | `docs/tools_DEPRECATED.md` marks tools as deprecated but the tools remain fully present and unrestricted |

---

## Workflow & Process Inference

### Development Model

**Trunk-based development with short-lived feature branches.** Every PR contains exactly one feature commit merged into `main`. No long-running branches exist. Prior branches are cleaned after merge. This is a solo-developer workflow that mimics team practices.

### Testing Workflow

**Manual, local execution.** `pytest tests/` runs 155 pure-Python unit tests. No test runner configuration (`pytest.ini`, `pyproject.toml [tool.pytest]`) beyond `conftest.py` adding the repo root to `sys.path`. One test (`test_record_contract_v2.py`) conditionally skips when `FINGERPRINT_JSON_PATH` is unset — this is the only test that validates real extractor output.

### Review Process

**PR templates exist but are unenforceable.** Three templates (generic, feature, patch) define rigorous checklists covering hash stability, golden comparisons, and Dynamo runs. Without CI/CD, compliance is voluntary.

### Operational Knowledge

**Encoded in a PowerShell command reference.** `tools/Powershell Commands.txt` (12K) contains hardcoded Windows paths and real project names (Stantec directory structures). This is the closest artifact to an operator's runbook, but it is not parameterized or automated.

### Probe-Driven Development

**API probes generate evidence for policy decisions.** The `tools/probes/` directory contains 16 probe scripts that inventory Revit API surfaces, with JSON results timestamped 2026-02-04/05. A 53K markdown inventory document synthesizes findings. This suggests a deliberate "measure first, then codify" approach to domain design.

---

## Technical Debt Indicators

### Structural Debt

| Item | Severity | Location | Notes |
|------|----------|----------|-------|
| No CI/CD pipeline | **High** | Absent | 155 tests exist but are never run automatically; PR checklists are unenforceable |
| No package management | **Medium** | Absent | No `requirements.txt`, `pyproject.toml`, or dependency pinning; relies on Dynamo CPython3 built-ins |
| Legacy hash mode still default | **Medium** | `runner/run_dynamo.py` | Semantic mode is documented as preferred but legacy remains authoritative |
| CHANGELOG stale | **Low** | `CHANGELOG.md` | "Unreleased" section does not reflect ~25 PRs of semantic work since M4 |
| README domain count outdated | **Low** | `README.md` | Lists 11 domains; actual count is 17 (14 active + 1 disabled + 1 deprecated + 1 legacy) |

### Code Debt

| Item | Severity | Location | Notes |
|------|----------|----------|-------|
| `view_filters_deprecated.py` retained in `domains/` | **Low** | `domains/view_filters_deprecated.py` (741 lines) | Not imported by runner; no removal timeline |
| `core/canon.py` deprecated alias `sig_val()` | **Low** | `core/canon.py:141` | "Back-compat helpers (deprecated; kept to reduce churn)" |
| Deprecated semantic_items duplication pattern in 6+ domains | **Low** | `text_types.py:124`, `object_styles.py:451`, `line_styles.py:399`, `units.py:291`, `view_category_overrides.py:407`, `dimension_types.py:1416,1443` | Marked "deprecated direction" but code still present |
| Phase-2 attributes.py NotImplementedError stubs | **Medium** | `tools/phase2_analysis/attributes.py:61,69` | Blocks Phase-2 attribute analysis workflow |
| `sigaudit.py` at repo root | **Low** | `/sigaudit.py` | Ad-hoc diagnostic script; should be in tools/ or removed |

### Contract Debt

| Item | Severity | Location | Notes |
|------|----------|----------|-------|
| 4 domains missing from `domain_identity_keys_v2.json` | **Medium** | `contracts/domain_identity_keys_v2.json` | arrowheads, identity, view_category_overrides, view_templates have join-key policies but no identity key registration |
| 2 provisional join-key policies | **Low** | `policies/domain_join_key_policies.json` | phase_filters and phases marked provisional; acceptable if actively being validated |

### Test Debt

| Item | Severity | Location | Notes |
|------|----------|----------|-------|
| 5 core modules without unit tests | **Medium** | `core/collect.py`, `core/rows.py`, `core/canon.py`, `core/context.py`, `core/graphic_overrides.py` | `collect.py` (12K) and `graphic_overrides.py` (21K) are substantial untested modules |
| 11 of 14 active domains have only 1 canonical-selector test | **Medium** | `tests/test_*_canonical_selectors.py` | Tests verify join-key policy compliance but not extraction logic |
| No integration test automation | **High** | `tests/revit/` | Integration harness exists but requires manual Revit execution with no evidence of regular use |
| Empty golden file directory | **Medium** | `tests/golden/` | Golden file infrastructure exists but no baselines are committed |

---

## Operational Risks (Ranked)

### 1. Single Point of Failure — Knowledge Concentration

**Risk:** One developer holds all domain knowledge, operational procedures, and Revit environment context. AI-assisted commits (10%) supplement but do not replace this.

**Evidence:** 90/100 commits by one author. `tools/Powershell Commands.txt` contains hardcoded paths specific to one workstation. No onboarding documentation beyond `CLAUDE.md` (which targets AI assistants, not human developers). PR templates reference verification steps that assume Revit access.

### 2. No Automated Quality Gate

**Risk:** Regressions can merge without detection. Hash stability — the project's core invariant — is unverified by any automated process.

**Evidence:** No `.github/workflows/`, no CI/CD configuration of any kind. 155 unit tests exist but are never triggered by push or PR events. PR template checklists are advisory only.

### 3. Untestable Core Path Without Revit

**Risk:** The actual extraction logic (domain `extract()` functions interacting with the Revit API) cannot be tested outside a live Revit session. Unit tests validate contracts and policies but not the data pipeline.

**Evidence:** All 155 tests mock or avoid Revit API calls. `tests/revit/revit_test_runner_pyrevit.py` requires `__revit__` global. No Revit model files are committed. Golden file directory is empty.

### 4. Stale Documentation Creates False Confidence

**Risk:** Outdated README (wrong domain count), stale CHANGELOG (missing ~25 PRs of work), and incomplete identity-key registration could mislead future contributors or AI assistants.

**Evidence:** README says "9 global + 2 contextual"; actual is 12 + 2. CHANGELOG "Unreleased" does not reflect canonical-evidence rollout, timing instrumentation, or record_id stabilization. Four domains lack identity-key registration despite having policies.

### 5. Legacy Hash Mode Persistence

**Risk:** The longer legacy mode remains default, the harder migration becomes. Downstream consumers (if any) may depend on legacy hash format, creating an implicit compatibility contract.

**Evidence:** `HASH_MODE` defaults to `"legacy"`. All domains compute both hashes. `docs/phase2-identity-and-semantic-plan.md` describes migration path but sets no timeline. No evidence of comparison runs between legacy and semantic hashes.

### 6. Operational Runbook Is Informal

**Risk:** The only operational reference is a text file of PowerShell commands with hardcoded paths. Loss of this implicit knowledge would significantly impair usage.

**Evidence:** `tools/Powershell Commands.txt` (12K) contains real project paths and command sequences. No parameterized scripts, no CLI wrappers, no `--help` documentation on analysis tools.

---

## Highest-Leverage Next Moves

### 1. Add CI to Run `pytest tests/` on Push and PR

**Justification:** 155 tests exist and are pure Python (no Revit dependency). A single GitHub Actions workflow running `pytest tests/` would protect the project's core invariants (hash determinism, sentinel policy, architecture rules, contract compliance) with zero new test authoring required. This is the highest ratio of protection-to-effort available.

**Evidence:** Tests are self-contained (`conftest.py` only adds repo root to sys.path). No external dependencies needed. One conditional test skips gracefully when `FINGERPRINT_JSON_PATH` is unset.

### 2. Update CHANGELOG and README to Reflect Current State

**Justification:** Both documents are materially outdated. The CHANGELOG has not been updated through ~25 PRs of significant work (canonical evidence rollout, record_id stabilization, timing instrumentation, join-key deduplication). The README undercounts domains. Correcting these requires no code changes and directly reduces Risk #4.

**Evidence:** CHANGELOG "Unreleased" references M4 as latest work; actual latest is PR #127 (timing instrumentation). README lists 11 domains; 17 exist in `domains/`.

### 3. Register Missing Domains in `domain_identity_keys_v2.json`

**Justification:** Four domains (arrowheads, identity, view_category_overrides, view_templates) have join-key policies but are absent from the identity-key contract. This gap means `test_record_contract_v2.py` cannot fully validate these domains' records. Filling this gap strengthens the contract validation layer the project already invested in.

**Evidence:** `policies/domain_join_key_policies.json` has entries for all 15 domains. `contracts/domain_identity_keys_v2.json` has entries for only 11.

### 4. Add Unit Tests for `core/collect.py` and `core/graphic_overrides.py`

**Justification:** These are the two largest untested core modules (12K and 21K respectively). `collect.py` is the centralized element collection layer — every domain depends on it. `graphic_overrides.py` is shared by multiple domains. Both can be tested with mocks (no Revit required) since their interfaces are well-defined.

**Evidence:** `test_no_direct_filtered_element_collector_in_domains.py` enforces that all domains use `collect.py`, making it a single point of failure without direct test coverage.

### 5. Set a Decision Date for Legacy → Semantic Hash Mode Switch

**Justification:** The dual hash computation increases code complexity and maintenance burden in every domain. `docs/phase2-identity-and-semantic-plan.md` describes the migration path but sets no timeline. A decision — even "not yet, because X" — would prevent indefinite drift. This need not be a code change; a dated entry in `DECISIONS.md` would suffice.

**Evidence:** All 14 active domains compute both hashes. Legacy remains default. No evidence of systematic comparison between the two modes. The semantic mode is described as "preferred for new integrations" in `CLAUDE.md`.
