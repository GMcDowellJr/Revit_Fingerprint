# Record Contract v2 (record.v2)

This contract defines the canonical per-record schema for all fingerprint domains that participate in similarity and reporting.

## Invariants (non-negotiable)

- `sig_hash` is the ONLY identity used for similarity.
- No silent fallbacks:
  - if a human-readable label is unavailable, that must be explicit via `label.quality` and/or `status_reasons`.
- Status ordering semantics are preserved: blocked > degraded > ok.
- Unreadable ≠ missing (must be distinguished explicitly).
- Domains MUST NOT invent keys, normalization rules, or sentinel conventions outside this contract.
- Sentinel literals MUST NOT appear in identity values.

## Record schema version

- `schema_version` MUST be `"record.v2"`.

## Record object: required fields

Every emitted record MUST contain:

- `schema_version: "record.v2"`
- `domain: string` (stable key; lowercase snake_case recommended)
- `record_id: string` (domain-local deterministic key; NOT used for similarity)
- `status: "ok" | "degraded" | "blocked"`
- `status_reasons: string[]` (required; may be empty; machine-readable codes only)
- `sig_hash: string | null`
  - MUST be null iff `status == "blocked"`.
  - If non-null, MUST be lowercase 32-hex (MD5) unless the contract updates the format.
- `identity_basis: object`
  - `hash_alg: string` (e.g. "md5_utf8_join_pipe")
  - `item_schema: "identity_items.v1"`
  - `items: IdentityItem[]`
- `identity_quality: "complete" | "incomplete_missing" | "incomplete_unreadable" | "incomplete_unsupported" | "none_blocked"`
- `label: object`
  - `display: string`
  - `quality: LabelQuality` (enum)
  - `provenance: LabelProvenance` (enum)
  - `components: object` (must exist; may be empty)

Optional fields:
- `debug: object` (freeform; MUST NOT be used for identity)

## IdentityItem (identity_basis.items)

Each identity item MUST be:

- `k: string` (contract-owned key registry; per-domain allowed keys enforced)
- `v: string | null` (canonicalized value or null)
- `q: "ok" | "missing" | "unreadable" | "unsupported"`

### Identity bans

- `v` MUST NOT contain sentinel literals such as:
  - "<MISSING>", "<UNREADABLE>", "<NOT_APPLICABLE>", "<LP:UNMAPPED>"
- Absence MUST be represented using `v = null` and `q` (not magic strings).

### Determinism

- Keys `k` MUST be unique within a record.
- Items MUST be sorted lexicographically by `k` before hashing.
- Canonicalization of values MUST be centralized (single implementation).

## Label enums

LabelQuality:
- "human"
- "system"
- "placeholder_missing"
- "placeholder_unreadable"
- "placeholder_unsupported"

LabelProvenance:
- "revit.Name"
- "revit.FamilyName+Name"
- "revit.ViewName"
- "revit.BuiltInEnum"
- "revit.SpecTypeId"
- "computed.path"
- "none"

## sig_hash semantics

### Preimage (authoritative)

Given items sorted by `k`, serialize each item into:

- `k=<k>|q=<q>|v=<v_or_empty>`

Where:
- `v_or_empty` is "" when `v` is null.

Then:
- `sig_hash = make_hash(preimage_strings)` per `identity_basis.hash_alg`.

## identity_quality semantics (authoritative)

- If `status == "blocked"`, then `identity_quality = "none_blocked"`.
- Else compute identity_quality over REQUIRED KEYS only using dominance order from:
  - `contracts/domain_identity_keys_v2.json`

## Domain key registry and minima

Per-domain allowed keys, required keys, and minima/blocking rules are defined in:
- `contracts/domain_identity_keys_v2.json`

Domains MUST:
- emit only keys listed for that domain (plus indexed expansions explicitly permitted)
- block when minima requires blocking
- otherwise degrade with null-marked items and explicit status_reasons

## Reason codes

`status_reasons` entries MUST be machine-readable (no free text).
Recommended prefixes:
- "identity.incomplete:<missing|unreadable|unsupported>:<key>"
- "dependency.<missing|blocked|unresolved>:<domain>"
- "label.placeholder:<missing|unreadable|unsupported>"

## Out of scope

- Similarity algorithm changes
- CSV/comparison logic changes
- UI-dependent naming retrieval assumptions
