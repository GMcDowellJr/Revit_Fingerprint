# Chunk of DECISIONS.md

- Source relative path: `DECISIONS.md`
- Chunk: 1 of 5
- Original line range: 1-399
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: 8ed07306f5b9f68e40e1373d6eb567f822e92ba18bed964edfc92c80cb0cb774
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| # DECISIONS
     2| 
     3| > **Historical-record notice (2026-08-20):** This append-only decision log may name identities and deployment details that were accurate when recorded. Those references are non-operational and do not define current executable defaults, policy, or supported configuration.
     4| 
     5| This document records **architectural and semantic decisions** that materially affect
     6| system behavior, evolution, or constraints.
     7| 
     8| It exists to:
     9| - prevent re-litigation of settled questions
    10| - make intent explicit
    11| - preserve rationale when context is lost
    12| 
    13| This is **not** a log of implementation details.
    14| If a decision changes hashes, identity rules, or system structure, it belongs here.
    15| 
    16| ---
    17| 
    18| ## Decision Log
    19| 
    20| ### D-001 — Behavior-First Fingerprinting
    21| **Status:** Accepted  
    22| **Date:** 2025-12-17
    23| 
    24| **Decision**  
    25| Fingerprints represent **behavior**, not UI presentation or naming.
    26| 
    27| **Rationale**  
    28| Names, ordering in UI, and cosmetic properties change frequently and are not reliable
    29| signals of functional intent. Behavioral properties are the only stable basis for
    30| standards governance and drift detection.
    31| 
    32| **Consequences**
    33| - Names are metadata only unless explicitly stated otherwise
    34| - Hash changes are meaningful signals, not noise
    35| 
    36| ---
    37| 
    38| ### D-002 — Deterministic, Auditable Hashes
    39| **Status:** Accepted  
    40| **Date:** 2025-12-17
    41| 
    42| **Decision**  
    43| All hashes must be:
    44| - deterministic
    45| - stable across sessions
    46| - derived from an auditable preimage
    47| 
    48| **Rationale**  
    49| Hashes without explainability cannot be trusted or debugged.
    50| Auditability is mandatory for governance and standards enforcement.
    51| 
    52| **Consequences**
    53| - `record_rows` is mandatory for record-based domains
    54| - Debug markers must be explicit when data is unreadable
    55| 
    56| ---
    57| 
    58| ### D-003 — `record_rows` as Canonical Explainability
    59| **Status:** Accepted  
    60| **Date:** 2025-12-17
    61| 
    62| **Decision**  
    63| `record_rows` is the canonical explainability structure for all record-based domains.
    64| 
    65| **Rationale**  
    66| Lists of names or counts are insufficient for traceability.
    67| A stable `(record_key → sig_hash)` mapping enables diffs, audits, and downstream tooling.
    68| 
    69| **Consequences**
    70| - Every record-based domain must emit `record_rows`
    71| - Global hashes are always derived from per-record hashes
    72| 
    73| ---
    74| 
    75| ### D-004 — UniqueId Usage Is Restricted
    76| **Status:** Accepted  
    77| **Date:** 2025-12-17
    78| 
    79| **Decision**  
    80| `UniqueId` is used **only** where element-backed identity is meaningful and persistent.
    81| 
    82| **Rationale**  
    83| Blind use of `UniqueId` causes unnecessary churn and false drift.
    84| Some domains are definition-based, not identity-based.
    85| 
    86| **Consequences**
    87| - Styles, patterns, and definitions avoid `UniqueId` unless identity matters
    88| - Views, view templates, filters, phases may use `UniqueId`
    89| 
    90| ---
    91| 
    92| ### D-005 — Fail-Soft Is Mandatory
    93| **Status:** Accepted  
    94| **Date:** 2025-12-17
    95| 
    96| **Decision**  
    97| Unreadable or inaccessible data must never cause silent collapse.
    98| 
    99| **Rationale**  
   100| Silence hides risk. Explicit failure markers preserve state distinctions and auditability.
   101| 
   102| **Consequences**
   103| - `<Unreadable>` / `<None>` markers are emitted instead of skipping data
   104| - Errors propagate into hashes intentionally
   105| 
   106| ---
   107| 
   108| ### D-006 — Ordering Rules Are Explicit Per Domain
   109| **Status:** Accepted  
   110| **Date:** 2025-12-17
   111| 
   112| **Decision**  
   113| Ordering sensitivity is a **domain decision**, not an implementation accident.
   114| 
   115| **Rationale**  
   116| Some structures (e.g. view filter stacks) are order-dependent; others are not.
   117| Implicit ordering leads to accidental semantic changes.
   118| 
   119| **Consequences**
   120| - Order-sensitive structures preserve order in signatures
   121| - Order-insensitive structures are sorted before hashing
   122| - Each domain must state its ordering behavior
   123| 
   124| ---
   125| 
   126| ### D-007 — Global vs Contextual Domain Split
   127| **Status:** Accepted  
   128| **Date:** 2025-12-17
   129| 
   130| **Decision**  
   131| Globally defined entities are fingerprinted once and referenced elsewhere.
   132| 
   133| **Rationale**  
   134| Duplication of global definitions inside views/templates causes inconsistency and waste.
   135| 
   136| **Consequences**
   137| - Filters, phases, phase filters, phase graphics are global domains
   138| - Views and view templates reference global domains by identity + hash
   139| 
   140| ---
   141| 
   142| ### D-008 — View Templates Are Behavioral, Not Nominal
   143| **Status:** Accepted  
   144| **Date:** 2025-12-17
   145| 
   146| **Decision**  
   147| View templates are fingerprinted by **controlled behavior**, not by name or existence.
   148| 
   149| **Rationale**  
   150| Two templates with the same name can behave differently.
   151| Name-only fingerprints are misleading and unsafe.
   152| 
   153| **Consequences**
   154| - Template hashes are derived from controlled parameters, filters, phase settings, etc.
   155| - Names are metadata only
   156| 
   157| ---
   158| 
   159| ### D-009 — Views Compose Templates + Deltas
   160| **Status:** Accepted  
   161| **Date:** 2025-12-17
   162| 
   163| **Decision**  
   164| A view’s effective behavior is:
   165| - template behavior (if assigned)
   166| - plus view-specific deltas not controlled by the template
   167| 
   168| **Rationale**  
   169| This mirrors actual Revit behavior and avoids double-counting settings.
   170| 
   171| **Consequences**
   172| - Views with templates do not re-hash template-controlled settings
   173| - Views without templates hash full allowlisted behavior
   174| 
   175| ---
   176| 
   177| ### D-010 — Phase Names in Behavioral Hashes
   178| **Status:** Revised
   179| **Date:** 2025-12-17 (revised 2026-01-29)
   180| 
   181| **Decision**
   182| Phase names ARE included in behavioral hashes for cross-project comparability.
   183| Phase UniqueId is used for identity/debug only (document-specific).
   184| 
   185| **Rationale**
   186| UniqueIds are document-specific and cannot be compared across projects.
   187| Phase names provide the semantic link needed for cross-project drift detection.
   188| This supersedes the original decision that treated names as metadata-only.
   189| 
   190| **Consequences**
   191| - Phase name changes ARE considered behavioral changes
   192| - Cross-project comparison uses phase names as the comparability key
   193| - UniqueId remains for within-document identity only
   194| 
   195| ---
   196| 
   197| ### D-011 — Domain-Driven Architecture
   198| **Status:** Accepted  
   199| **Date:** 2025-12-17
   200| 
   201| **Decision**  
   202| The system is structured into:
   203| - Core (pure Python)
   204| - Domain extractors (Revit-aware)
   205| - Context builder
   206| - Host-specific runners
   207| 
   208| **Rationale**  
   209| This enables refactoring, selective execution, and future portability.
   210| 
   211| **Consequences**
   212| - Domains do not import each other
   213| - Cross-domain data flows only through context
   214| 
   215| ---
   216| 
   217| ### D-012 — Markdown Portability Rule
   218| **Status:** Accepted  
   219| **Date:** 2025-12-17
   220| 
   221| **Decision**  
   222| Nested fenced code blocks are forbidden in documentation.
   223| 
   224| **Rationale**  
   225| GitHub Mobile, Obsidian, and chat renderers handle nested fences inconsistently.
   226| 
   227| **Consequences**
   228| - Fenced blocks are used only for whole-file examples
   229| - Indented blocks are used for schemas and inline snippets
   230| 
   231| ---
   232| 
   233| ## D-013 — Phase Graphics Domain Disabled (API Limitation)
   234| 
   235| **Status:** Accepted  
   236| **Date:** 2025-12-18  
   237| **Scope:** `phase_graphics` domain
   238| 
   239| ### Context
   240| The Revit UI exposes *Phase Graphic Overrides* (per-status line styles, colors, patterns).
   241| During implementation, it was unclear whether these overrides were accessible via the
   242| public Revit API.
   243| 
   244| A targeted API probe in Revit 2025 (and consistent with behavior back to 2021) confirmed:
   245| - `PhaseFilter.GetPhaseStatusPresentation` **is available**
   246| - No API access exists for:
   247|   - per-status graphic overrides
   248|   - line style assignments
   249|   - color / pattern overrides
   250| 
   251| Earlier attempts that surfaced `<Unreadable>` values were calling non-existent or unsupported
   252| API members and did not represent real accessible data.
   253| 
   254| ### Decision
   255| The `phase_graphics` domain is **intentionally disabled** at runtime.
   256| 
   257| The system will not emit stub hashes or placeholder signatures for data that cannot be
   258| reliably extracted via the API.
   259| 
   260| ### Rationale
   261| - Avoids misleading fingerprints and false confidence
   262| - Keeps all emitted data verifiable and reproducible
   263| - Maintains a clean separation between:
   264|   - `phase_filters` → presentation (API-supported)
   265|   - `phase_graphics` → not available via API
   266| 
   267| ### Consequences
   268| - Phase graphic overrides are not fingerprinted
   269| - Downstream consumers must not assume graphic override coverage
   270| - Future enablement requires a documented, supported API path or non-API extraction strategy
   271| 
   272| ### Revisit Criteria
   273| Revisit this decision if:
   274| - Autodesk exposes phase graphic overrides via the public API
   275| - A sanctioned non-API extraction mechanism is introduced and approved
   276| 
   277| ---
   278| 
   279| ## D-014 — Hash Mode Migration Timeline
   280| 
   281| **Status:** Accepted
   282| **Date:** 2026-02-07
   283| 
   284| ### Context
   285| The system computes two hashes for every domain: `hash` (legacy pipe-delimited with sentinel
   286| literals) and `hash_v2` (record.v2 identity-basis, no sentinel literals). The `REVIT_FINGERPRINT_HASH_MODE`
   287| environment variable selects which is authoritative. Legacy remains the default.
   288| 
   289| All 14 active domains now compute both hashes. The canonical evidence selector rollout
   290| (PRs #106–#119) established policy-driven join-key composition for all domains, making
   291| semantic mode viable.
   292| 
   293| ### Decision
   294| The legacy hash mode will be maintained as default until the following criteria are met:
   295| 
   296| 1. A comparison run across the current model population confirms `hash` and `hash_v2` produce
   297|    equivalent governance signals (same drift/deviation detection).
   298| 2. All downstream consumers (if any) have been notified of the format change.
   299| 3. The comparison results are documented in this repository.
   300| 
   301| Once criteria are satisfied, `semantic` becomes the default and `legacy` enters a deprecation
   302| period of at least one extraction cycle before removal.
   303| 
   304| ### Rationale
   305| - Dual computation adds complexity to every domain but is necessary for safe migration.
   306| - Setting explicit criteria prevents indefinite deferral while protecting against premature switching.
   307| - The comparison run is the minimum evidence required for confidence.
   308| 
   309| ### Consequences
   310| - Legacy mode remains default until criteria are met.
   311| - No new domains should add legacy hash support — new domains use semantic mode only.
   312| - The comparison run becomes a blocking prerequisite for the switch.
   313| 
   314| ---
   315| 
   316| 
   317| ## D-014 — Hash Mode Migration Timeline (COMPLETED)
   318| 
   319| **Completion Date:** 2026-02-10  
   320| **PR:** #XXX
   321| 
   322| Legacy hash infrastructure removed. All domains now use semantic (record.v2) hashing exclusively.
   323| Comparison run validated equivalence across 50+ sample files on 2026-02-09.
   324| 
   325| No downstream breaking changes: contract schema already supported semantic mode.
   326| 
   327| ---
   328| 
   329| ## D-015 — Domain Family Split Architecture
   330| 
   331| **Status:** Accepted
   332| **Date:** 2026-03-06
   333| 
   334| **Decision**
   335| The four monolithic extractors (`object_styles`, `fill_patterns`, `dimension_types`,
   336| `view_templates`) are split into per-partition domain files, each covering one record
   337| class or ViewType family. The split follows a three-level hierarchy:
   338| 
   339| - **Domain family**: Named grouping (e.g., `object_styles`, `dimension_types`)
   340| - **Domain**: Individual split file (e.g., `object_styles_model`, `dimension_types_linear`)
   341| - **Record class**: The entity type within a domain (e.g., Model categories, Linear shapes)
   342| 
   343| **Rationale**
   344| - Monolithic extractors mixed heterogeneous record structures, making per-class policy
   345|   governance impractical.
   346| - Each split domain can have its own join-key policy tailored to the record class.
   347| - Shape discrimination moves to domain-level filtering rather than within-domain branching.
   348| - Downstream tools and analysis pipelines can target specific record classes directly.
   349| 
   350| **Split mapping**
   351| 
   352| | Old domain | New domains |
   353| |------------|-------------|
   354| | `object_styles` | `object_styles_model`, `object_styles_annotation`, `object_styles_analytical`, `object_styles_imported` |
   355| | `fill_patterns` | `fill_patterns_drafting`, `fill_patterns_model` |
   356| | `dimension_types` | `dimension_types_linear`, `dimension_types_angular`, `dimension_types_radial`, `dimension_types_diameter`, `dimension_types_spot_elevation`, `dimension_types_spot_coordinate`, `dimension_types_spot_slope` |
   357| | `view_templates` | `view_templates_floor_structural_area_plans`, `view_templates_ceiling_plans`, `view_templates_elevations_sections_detail`, `view_templates_renderings_drafting`, `view_templates_schedules` |
   358| 
   359| **Shared helpers**
   360| - `core/dimension_type_helpers.py`: Shape constants, detection, and reading helpers
   361| - `core/vg_sig.py`: VG signature helpers for view_templates split domains
   362| 
   363| **Consequences**
   364| - Each split domain has its own flat join-key policy (no shape_gating in new dimension_types policies — shape discrimination is done at domain-level)
   365| - The `require_domain` dependency chain is updated to reference split domain names
   366| - Tools and analysis configs use split domain names throughout
   367| - No semantic change to hash values within each record class
   368| 
   369| ---
   370| 
   371| ## D-015 — Domain Family Architecture
   372| **Status:** Accepted
   373| **Date:** 2026-03-19
   374| ### Context
   375| Several Revit API classes expose structurally heterogeneous records that were
   376| initially extracted as single monolithic domains. As the corpus grew and governance
   377| questions became more specific, the single-domain approach produced analytically
   378| meaningless blended HHI scores (e.g. a single score for dimension_types mixing
   379| Linear and SpotCoordinate types that share almost no applicable properties).
   380| ### Vocabulary
   381| - **Domain family**: Named grouping of related domains. Policy and BI concept only —
   382|   no code hierarchy. Defined in `policies/cross_domain_alignment_keys.json`.
   383| - **Domain**: The extractable, analyzable unit. One extractor file, one policy entry,
   384|   one sig_hash, one HHI score. All domains are flat peers in the runner.
   385| - **Record class**: Within a single domain, records may fall into classes where
   386|   different properties are applicable to identity. Routed by class discriminator.
   387|   Implemented via `shape_gating` block in join-key policy.
   388| - **Class discriminator**: The identity_item field whose value determines a record's
   389|   record class.
   390| - **Alignment keys**: Fields shared across domains within a family that governance
   391|   expects to be consistent. Defined in `policies/cross_domain_alignment_keys.json`.
   392| ### Decision
   393| Adopt the three-level architecture above. Revit's system family boundary is the
   394| authoritative partition criterion for deciding when to split into separate domains
   395| versus use a record class gate within one domain.
   396| The shape_gating JSON key in join-key policy is retained for backward compatibility
   397| with core/join_key_builder.py. The new vocabulary applies to prose, comments, and
   398| documentation only — not to JSON key names.
   399| ### Affected domains in this branch
```
