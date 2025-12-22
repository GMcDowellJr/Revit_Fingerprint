# Fingerprint Hashing Rules — Semantic Stability & API Reachability

## Purpose
Define **non-negotiable rules** for computing fingerprints such that:
- Equal standards produce equal hashes
- Hash drift represents **semantic change**, not API reachability, version quirks, or execution order
- Cross-version and cross-context comparisons remain interpretable

These rules are normative and govern all domains and the runner.

---

## Core Principle

> **Unreadable ≠ Different**  
> Unreachable or unreadable data must never influence semantic equality.

Hashing must reflect **what is known**, not **what failed to load**.

---

## 1. Domain Output Contract (Required)

Every domain must return the following logical components:

- **semantic**
  - Deterministic, hashable data
  - Stable across Revit versions and contexts
  - Contains only allowed primitives (see §3)

- **quality**
  - Machine-readable indicators of collection health
  - Examples:
    - unreadable_count
    - skipped_count
    - dependency_missing (bool)
    - warnings[]

- **debug**
  - Human-readable context
  - Names, raw strings, samples
  - Never hashed

### Minimum Required Fields
- `count` — number of semantic records
- `hash` — hash of semantic records, or `None`
- `quality` — object as described above

---

## 2. Hash Scope Rules

### 2.1 Hash Only Semantic Data
- The hash **must be computed exclusively** from `semantic`
- `quality` and `debug` are explicitly excluded

### 2.2 Unreadable Data Policy
- If a value is unreadable:
  - It is **excluded** from `semantic`
  - It is **reported** in `quality`
- No `<UNREADABLE>`, error strings, or exception messages may enter the hash

### 2.3 No Per-Document Conditional Hashing
- Field inclusion must be based on **capability**, not per-element success/failure
- Do not include a field in one file and omit it in another unless gated by an explicit capability rule (see §5)

---

## 3. Allowed Semantic Primitives

Only the following may appear in `semantic`:

- Negative built-in ids (categories, parameters)
- Stable enum values
- Normalized numeric primitives
- Stable upstream hashes (via ctx mapping)
- Fixed sentinel tokens (only for known, declared cases)

### Explicitly Forbidden in Semantic
- Names (localized or user-editable)
- `str(obj)` / `ToString()` outputs
- Exception messages
- Raw API object dumps

#### Allowed exceptions (must be explicit)
Names are forbidden in semantic **unless** a domain explicitly declares a narrowly-scoped exception where:
- No stable, cross-file API identity exists for the records being fingerprinted, and
- The name is treated as part of the **definition/identity** for that domain, and
- The exception is documented in-code (domain module) and in release notes.

Exceptions must never be introduced implicitly.

Names and strings belong in `debug` only when they are not part of an explicitly-declared identity exception.

---

## 4. No `ToString()` Rule

- `ToString()` or implicit stringification is **never allowed** in semantic
- If structured extraction fails:
  - Exclude the value from semantic
  - Record the failure in `quality`
- Debug output may include raw strings for inspection

---

## 5. Capability-Gated Hashing

### 5.1 Capability Declaration
Domains may declare capabilities, e.g.:
- `can_read_rule_evaluator`
- `can_read_phase_graphics`

Capabilities describe **environment support**, not per-element success.

### 5.2 Capability Enforcement
- A field may enter `semantic` **only if the capability is true for the entire run**
- If a capability is false:
  - The field is excluded everywhere
  - Capability state is reported in runner metadata

### 5.3 Cross-Version Comparability
- Hash equality is defined only within the same semantic capability set
- Runner must emit capability vector for interpretation

---

## 6. Dependency Contracts (ctx)

### 6.1 Explicit Dependencies
Domains that rely on upstream hashes must declare:
- `requires = ["line_patterns", "view_filters", ...]`

### 6.2 Missing Dependency Behavior
If a required dependency is missing:
- Domain returns:
  - `status = "blocked"`
  - `hash = None`
  - `quality.dependency_missing = true`
- Runner emits a global note
- No sentinel placeholders in semantic

---

## 7. Order Semantics

Each domain must declare one:

- `semantic_order = "sorted"`  
  (order-insensitive; semantic records sorted before hashing)

- `semantic_order = "preserve"`  
  (order-sensitive; domain must guarantee stable ordering)

Order choice must be justified in code comments.

---

## 8. Numeric Normalization

All numeric values in semantic must define:
- Unit basis (e.g., internal feet)
- Rounding or tolerance
- Representation (string vs numeric)

Normalization rules must be consistent within a domain.

---

## 9. Domain Status States

Every domain must report one status:

- `supported` — full semantic hash emitted
- `partial` — semantic hash emitted, but some data unreadable (reported in quality)
- `blocked` — missing dependencies; no hash
- `unsupported` — API does not expose data; no hash

Only `supported` and `partial` domains may emit a hash.

---

## 10. Runner Global Notes (Required)

Runner must maintain:
- `fingerprint["_notes"] = []`

Notes must include:
- Unsupported domains
- Blocked domains
- Partial domains (unreadables present)
- Capability exclusions affecting hashing

Notes are human-readable and non-hashing.

---

## 11. Phase Graphics Special Rule

- Phase Graphics are **not reachable via the Revit API**
- Domain status is always:
  - `status = "unsupported"`
  - `hash = None`
- Runner must always emit a note:
  - `"phase_graphics: unsupported (Revit API does not expose Phase Graphics)"`

No placeholder or proxy hash is permitted.

---

## 12. Diff Interpretability Requirements

For every emitted hash, domains must also emit:
- `semantic_record_count`
- `semantic_schema_version`

This enables distinguishing:
- count change vs content change
- schema evolution vs data drift

---

## Invariant Summary

- Semantic equality must not depend on API reachability
- Unreadable data is **reported, not hashed**
- Capabilities define what is hashable
- Hashes represent standards, not execution artifacts
- Unsupported domains must be explicit, never implicit

Violations of these rules are considered correctness defects, not implementation details.