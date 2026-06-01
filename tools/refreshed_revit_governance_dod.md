# Refreshed Definition of Done — Revit Standards Governance Narrative Outputs

## Compact version

Done means the summary document gives leadership a clear, evidence-based read of the selected Revit standards landscape without requiring them to understand similarity, containment, bundle, or governance-state mechanics. It must explain what upstream standards content is provided, what is carried into downstream files, what is actively used in projects, what is locally created or modified, and where those signals create governance or onboarding burden. It may identify ratifiable baselines and roll-up candidates, but it must not declare approved standards, assign ownership, or measure compliance unless those decisions exist outside the analysis.

---

## Expanded Definition of Done

### Deliverable

A leadership-facing draft document for a defined analysis scope, such as:

- firmwide corpus,
- client portfolio such as Sutter,
- discipline,
- template family,
- generic / enterprise baseline chain,
- template-to-container-to-project cascade,
- or another generated segment.

The document should be in a polished memo/report format suitable to share as a draft with BIM, Digital Practice, or practice leadership.

It should include:

1. **Executive summary**
   - What the analysis covers.
   - What leadership should understand from it.
   - The main governance, standards, onboarding, or roll-up implication.

2. **Scope and evidence base**
   - Corpus slice used.
   - File counts where available.
   - Segment represented.
   - Governance roles represented: Generic / Enterprise, Template, Container, Project.
   - Explicit note for unavailable/future segments such as project type, business center, and region.

3. **How to read the analysis**
   - Plain-language explanation of configured vocabulary, active use, containment, consistency, divergence, and bundle/standards-shape signals.
   - Explanation of the all/used distinction:
     - all view = full configured vocabulary present in the file,
     - used view = project vocabulary excluding conclusively purgeable / unused records,
     - used/purge interpretation is meaningful primarily for Project targets,
     - Template, Generic, and most Container roles are provided-vocabulary references, not production-use environments.
   - No dependence on leadership understanding Jaccard similarity, DAGs, Jenks, containment math, or bundle mechanics.

4. **Governance cascade findings**
   - Generic / Enterprise → Template / Container / Project, when available.
   - Template → Container.
   - Template → Project.
   - Container → Project.
   - Project all → Project used.
   - Clear distinction between standards being present downstream and standards being actively used.

5. **Governance-state findings**
   The document should surface explicit governance states when outputs are available:

   - `provided_and_used`: upstream content reached projects and is active in delivery.
   - `provided_but_passive`: upstream content is carried downstream but not actively used by project files.
   - `provided_but_missing`: upstream content did not reach the downstream target.
   - `local_active`: downstream/project-created or modified content is actively used and may deserve roll-up review.
   - `local_passive`: local/downstream content exists but is not actively used.
   - `local_unbundled`: local content exists with weak/no bundle evidence.

   If governance-state files are unavailable, the document must say that inherited-but-unused and roll-up findings are inferred only indirectly.

6. **Current-state domain findings**
   - Domains with strong internal consistency.
   - Domains with ratifiable baselines.
   - Domains with ratifiable baselines but active local extensions.
   - Domains with usable common base but meaningful variation.
   - Domains with high divergence, weak containment, or poor downstream propagation.
   - Domains where evidence is insufficient or sparse.

7. **Onboarding / governance implications**
   - For client-specific summaries, identify what a new team member would likely need to learn:
     - common client/project vocabulary,
     - client-specific departures from the wider corpus,
     - domains where local practice is stable,
     - domains where project-to-project variation creates learning burden,
     - domains where “learning the client” means learning several local variants.
   - For firmwide summaries, identify where governance discussion is needed before standards can be formalized.
   - For local-active domains, distinguish full roll-up candidates from approved-list / starter-content / exception-governance candidates.

8. **Boundaries and limitations**
   - State that this is discovery/classification, not compliance tracking.
   - State that the tool identifies evidence of convergence, propagation, active use, passive inheritance, and local creation; it does not decide standards.
   - State whether governance-state counts are unique pattern counts or comparison-state rows. If not explicitly deduplicated, use shares/rankings rather than absolute counts for leadership claims.
   - Call out future segment expansion where applicable.

---

## Completeness Criteria

The document is complete when it answers these questions for the selected scope.

### 1. What is the scope?

The reader can tell whether the document is about:

- the full corpus,
- a client portfolio,
- a discipline,
- Generic / Enterprise baseline propagation,
- a template/coordination/project relationship,
- or another segment.

It should not imply broader coverage than the data supports.

### 2. What is internally consistent?

The document identifies domains where the selected scope has a stable common base.

For client documents, internal consistency is more important than connection to the whole corpus.

### 3. What is provided, carried, and used?

The document explains:

- what upstream content was provided,
- what downstream files carried,
- what project files actively used,
- what was carried passively,
- what upstream content is missing downstream.

Containment should be framed as evidence of reuse or propagation, not automatic proof of governance or use.

### 4. What is created or modified locally?

The document identifies local-active domains where project-created or modified vocabulary is actively used.

It should distinguish:

- roll-up candidates for templates or containers,
- client/discipline playbook candidates,
- approved-list or starter-content governance candidates,
- legitimate project-specific variation.

### 5. Where does the scope diverge from the wider corpus?

The document identifies where the selected segment differs from the broader corpus, but only treats divergence as important when it affects:

- onboarding,
- governance review,
- portability of staff between teams,
- template or container strategy,
- standards negotiation,
- or standards maintenance.

Divergence is not automatically bad.

### 6. What does this imply for onboarding?

For a client-specific document, the output should identify:

- low-friction domains a new team member can likely absorb quickly,
- domains requiring client-specific orientation,
- domains where project-to-project inconsistency may require coaching or reference material,
- and domains where “learning the client” is really learning several local variants.

### 7. What should leadership do with the information?

The document should convert findings into governance questions, not governance decisions.

Examples:

- “Is this a ratifiable baseline?”
- “Is this active local practice a roll-up candidate or a permitted variant?”
- “Should this be treated as a client-specific standard?”
- “Is this variation intentional specialization or unmanaged drift?”
- “Should onboarding material document the local variant?”
- “Should this domain be governed by approved lists rather than full convergence?”

### 8. What is out of scope?

The document explicitly avoids:

- declaring formal standards,
- assigning owners,
- writing standards language,
- measuring compliance,
- labeling project teams as compliant/noncompliant,
- implying corpus divergence is failure,
- or treating unused Template / Generic stock content as bloat.

---

## Quality Standard

The document is useful when a leadership reader can understand the practical implication without needing to inspect CSVs or understand the math.

For the **firmwide version**, useful means leadership can see:

- where the firm already has de facto convergence,
- where baseline standards are plausible but require governance review,
- where active local practice should be reviewed for roll-up,
- where serious governance work is needed,
- and why better standards governance matters.

For the **Sutter/client-specific version**, useful means leadership can see:

- how internally consistent the client portfolio is,
- what common base a new team member can rely on,
- which domains require Sutter-specific orientation,
- where Sutter diverges from the broader corpus,
- whether that divergence represents client identity, local practice, or onboarding burden,
- and which local-active patterns may need documentation, not necessarily standardization.

A done-looking but weak version would simply list domain scores. A useful version explains what those scores and state signals mean for governance, onboarding, and practical decision-making.

---

## Checkpoints

### 1. Data-scope checkpoint

Confirm the selected scope is correctly represented and does not imply unavailable segments.

For current work:

- client and corpus views are in scope,
- Generic / Enterprise baseline propagation should be reported when comparison outputs include it,
- project type, business center, and region are future enhancements,
- corpus-size versus segment-lattice sparsity is a known limitation.

### 2. All/used checkpoint

Confirm that all-view and used-view claims are not conflated.

- All-view = standard/configuration is present.
- Used-view = project target actively exercises non-purgeable vocabulary.
- Template, Generic, and most Container “used” signals are not compliance/use claims.

### 3. Governance-state checkpoint

If governance-state outputs are present, confirm the narrative includes:

- provided and used,
- provided but passive,
- provided but missing,
- local active,
- local passive/unbundled.

If governance-state outputs are absent, the narrative must disclose that limitation.

### 4. Ratification checkpoint

Confirm that “ratifiable baseline” is not overstated as:

- approved standard,
- compliance rule,
- ownership assignment,
- active-use proof,
- or full-domain governance.

If a ratifiable baseline has high local-active share, the document should say that the baseline can be reviewed for ratification while local extensions need separate review.

### 5. Evidence checkpoint

Confirm major claims are traceable to deterministic summaries, governance-state outputs, bundle data, or cross-segment outputs.

The document does not need to expose every metric, but it should not make claims that cannot be traced back.

### 6. Leadership-readability checkpoint

Confirm the document can be read without understanding:

- Jaccard similarity,
- containment math,
- Jenks thresholds,
- DAG roots/branches/leaves,
- differentiators,
- or bundle mechanics.

Technical detail can be summarized, not hidden.

### 7. Boundary checkpoint

Confirm the document ends at discovery, classification, and governance/onboarding implications.

Compliance tracking remains a future use case after governance decisions exist.

---

## Boundaries

This task ends at a leadership-ready **discovery and interpretation document**.

It should not continue into:

- revising the underlying data formats unless explicitly requested,
- rebuilding the analysis pipeline unless explicitly requested,
- generating new segment CSVs unless explicitly requested,
- writing formal standards,
- defining compliance rules,
- assigning standards owners,
- deciding which standards are approved,
- or building the future compliance/deviation dashboard.

For client-specific documents, it also should not imply that divergence from the firmwide corpus is inherently negative. Divergence matters when it affects onboarding, governance clarity, staff portability, or standards maintenance.
