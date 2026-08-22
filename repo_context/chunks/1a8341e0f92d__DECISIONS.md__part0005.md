# Chunk of DECISIONS.md

- Source relative path: `DECISIONS.md`
- Chunk: 5 of 5
- Original line range: 1592-1674
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: 8ed07306f5b9f68e40e1373d6eb567f822e92ba18bed964edfc92c80cb0cb774
- Starts inside symbol: no
- Ends inside symbol: no

```
  1592| Every maintained artifact package whose interpretation depends on enterprise
  1593| identity accompanies its outputs with canonical `enterprise_policy.json`.
  1594| Sorted UTF-8 JSON excludes local paths and is published only after primary
  1595| artifacts. Validation and dry-run paths remain non-writing.
  1596| 
  1597| Promotion analysis now emits `reuse_client_pool_is_enterprise`. The
  1598| organization-specific predecessor is removed rather than retained as an alias:
  1599| no maintained external consumer requires it. Classification uses the effective
  1600| policy label, never the enterprise BC bookkeeping token alone. Existing BC-grain
  1601| limitations remain.
  1602| 
  1603| ### Consequences
  1604| - Pre-v2 CSV consumers must explicitly rename the retired field.
  1605| - Policy overrides are reproducible beside identity-aware artifacts.
  1606| - Historical audit text remains historical; maintained prose/examples are neutral.
  1607| 
  1608| ## D-038 — `mapping/` line_patterns Revit mapping utility (model-writing, join_hash-verified)
  1609| 
  1610| ### Status
  1611| Accepted (2026-08-21)
  1612| 
  1613| ### Decision
  1614| Introduce `mapping/`, a new top-level package separate from `core/`/`domains/`/
  1615| `runner/`/`tools/`, that reads the `tools/export_bundle_pattern_detail.py` CSV
  1616| triple (`bundle_pattern_inventory.csv`/`pattern_settings.csv`/`pattern_names.csv`)
  1617| and materializes representative `LinePatternElement` objects in the currently
  1618| open Revit document, for use in a mapping/configuration RVT consumed by
  1619| downstream governance tooling. This is the first behavior in the repository
  1620| that writes to a Revit document rather than only reading it.
  1621| 
  1622| Scope is locked to the `line_patterns` domain only. Every unique
  1623| `(domain="line_patterns", join_hash)` in `bundle_pattern_inventory.csv` is a
  1624| requested configuration; reconstruction from `pattern_settings.csv` blocks
  1625| (never infers) on any incomplete/inconsistent evidence. Verification against
  1626| the requested identity uses `join_hash` (the `line_patterns.join_key.v3`
  1627| policy value, D-017's `line_pattern.segments_norm_hash`-derived scale-invariant
  1628| identity) computed via the existing `core/join_key_builder.py` +
  1629| `policies/domain_join_key_policies.json`, never `sig_hash` (which remains
  1630| `line_pattern.segments_def_hash`-derived, exact-scale identity) -- these answer
  1631| different questions and are not interchangeable. Each requested configuration
  1632| is created inside its own `Autodesk.Revit.DB.Transaction`, read back, and
  1633| re-verified against the requested `join_hash` before commit; any mismatch or
  1634| exception rolls that one transaction back without affecting others. Mapping
  1635| elements are named `MAP__<observed_name>`, with a deterministic
  1636| `MAP__<observed_name>__<short_join_hash>` fallback on a name collision against
  1637| a *different* configuration; an existing nonmatching element is never modified
  1638| or replaced.
  1639| 
  1640| `line_pattern.segments_norm_hash` is computed synthetically by
  1641| `tools/run_extract_all.py`'s private `_append_line_pattern_synthetic_norm_hash()`
  1642| during the flatten stage and is not exposed as an importable function. Rather
  1643| than import it (and couple this Revit-writing utility to that CLI
  1644| orchestrator's machinery), `mapping/line_pattern_reconstruction.py` carries a
  1645| deliberately independent reimplementation of the same per-record algorithm --
  1646| the same "independent reimplementation over import" precedent
  1647| `tools/pattern_id_utils.py` already established for `tools/extractor.py`'s
  1648| private `_stable_pattern_id()`. A cross-check test
  1649| (`tests/test_line_pattern_mapping_reconstruction.py::test_segments_norm_hash_matches_run_extract_all_reference`)
  1650| asserts the two implementations agree over synthetic segment lists.
  1651| 
  1652| ### Rationale
  1653| Existing extraction/analysis hash and join-key semantics are reused verbatim
  1654| (no hash-affecting change to any domain or policy). The only genuinely new
  1655| rules introduced are downstream-only and non-hash-affecting: a Revit
  1656| element-name sanitizer (`mapping/line_pattern_reconstruction.py::sanitize_revit_name`,
  1657| since nothing upstream previously needed to construct Revit-legal names from
  1658| arbitrary observed labels), and a defensive re-application of the existing
  1659| Dot-length-normalization rule (`domains/line_patterns.py` already forces Dot
  1660| segment length to `0.0` at extraction time; this utility re-applies the same
  1661| normalization to defend against a hand-edited/stale CSV, marking the result
  1662| degraded rather than blocked when it has to).
  1663| 
  1664| ### Consequences
  1665| - No existing extraction, join-key, bundle-analysis, or
  1666|   `export_bundle_pattern_detail.py` behavior changes; `mapping/` is
  1667|   purely additive and downstream.
  1668| - `core/`, `domains/`, and `runner/` must never import from `mapping/`
  1669|   (dependency direction stays one-way, same as the existing
  1670|   Core -> Domains -> Context -> Runner rule).
  1671| - A subsequent fill-pattern (or other domain) mapping PR must redo its own
  1672|   domain-specific reconstruction/naming/verification; no shared
  1673|   "materialize-a-domain-into-Revit" abstraction was introduced here to avoid
  1674|   generalizing prematurely from a single domain.
```
