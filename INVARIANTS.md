# Fingerprinting Invariants

These rules are non-negotiable unless explicitly versioned.

---

## Hash Semantics

- Hashes must be:
  - deterministic
  - stable across sessions
  - independent of element creation order

- Hash inputs must represent **behavior**, not presentation or naming.

---

## Identity Rules

- `UniqueId` is used only when:
  - the entity is element-backed
  - identity persistence is meaningful (e.g. filters, phases, templates, views)

- Names:
  - are metadata only
  - never included in behavior hashes unless explicitly stated

---

## Phase-2 Buckets

- `semantic_items`: behavior-defining items exported for join-key discovery and Phase-2 comparisons.
- `cosmetic_items`: non-behavioral labels and presentation attributes used for pattern detection.
- `coordination_items`: name-based cross-model resolution contracts (e.g., ByHost lookups) that never participate in behavior hashing or join-key discovery.
- `unknown_items`: file-local noise such as UIDs and ElementIds that are exported for traceability but excluded from behavior and join-key discovery.

Phase-2 invariants:
- `sig_hash` is authoritative and UID-free by contract.
- `identity_basis.items` contains the full behavioral definition and drives `sig_hash`.

---

## record_rows

- Every record-based domain must emit `record_rows`
- `record_rows` is the **canonical explainability layer**

Minimum schema:

    {
      "record_key": "<stable identity>",
      "sig_hash": "<behavior hash>",
      "name": "<optional metadata>"
    }

---

## Fail-Soft Policy

- Unreadable or inaccessible data must:
  - remain represented in the fingerprint
  - emit explicit markers (e.g. `<Unreadable>`, `<None>`)

- Silent collapse of distinct states is forbidden.

---

## Ordering Rules

- Order-sensitive structures (e.g. view filter stack):
  - preserve order in signature generation

- Order-insensitive structures:
  - are sorted before hashing

- Ordering behavior must be explicitly defined per domain.

---

## Refactor Discipline

- Refactors must not change hash semantics unless:
  - explicitly intended
  - clearly documented
  - versioned
