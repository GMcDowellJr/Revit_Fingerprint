# Revit Fingerprint workspace instructions

This file bootstraps AI behavior for the Revit Fingerprint repository.

## Use this repo guidance

- Start with `CLAUDE.md` for the high-level architecture, domain rules, and semantic invariants.
- Use `README.md` for project scope, runtime environment, and current work-in-progress status.
- Refer to `DECISIONS.md`, `INVARIANTS.md`, and `CHANGELOG.md` for contract, architecture, and change history rules.
- `REPO_OPERATIONAL_REVIEW.md` contains useful operational context on testing and CI limitations.

## Project structure

- `core/` contains pure Python utilities, hashing, canonicalization, record schema helpers, and shared context logic.
- `domains/` contains one extraction module per domain, with some consolidated extractors emitting partitioned domain families.
- `runner/` contains host-specific entry points for Dynamo CPython3 and lightweight wrappers.
- `policies/` contains join-key policies and alignment key definitions.
- `contracts/` contains record.v2 and identity-key schema contracts.
- `tests/` contains pytest unit coverage and domain-specific test suites.

## Important conventions

- This codebase is behavior-first: hashes represent behavior, not presentation or names.
- Only three sentinels are allowed: `<MISSING>`, `<UNREADABLE>`, and `<NOT_APPLICABLE>`.
- `UniqueId` is used only for element-backed identity where persistence matters.
- Domains must not import other domains.
- Use `core/collect.py` wrappers rather than Revit `FilteredElementCollector` directly in domains.
- Phase names are intentionally included in behavioral hashes (D-010).
- Hashing is semantic-only (`hash_v2`) and legacy signature mode has been removed.
- `record_rows` is the canonical explainability structure for record hashing.

## Test and development commands

- Run the unit test suite with:
  - `pytest tests/ -v`
- Run a focused test file with:
  - `pytest tests/test_hashing_incremental.py`
- Validate exported JSON contract with:
  - `FINGERPRINT_JSON_PATH=/path/to/export.json pytest tests/test_record_contract_v2.py`

## CI context

- Existing GitHub Actions workflow in `.github/workflows/ci.yml` installs `pytest` and runs `pytest tests/ -v` on main branch pushes and PRs.
- There is no pinned dependency file (`requirements.txt` / `pyproject.toml`) in this repo.

## What to avoid

- Do not introduce new semantic hashing behavior without updating `DECISIONS.md` and `CHANGELOG.md`.
- Do not add new sentinel literals beyond the approved three.
- Do not make pure refactor changes appear as semantic changes.
- Do not create cross-domain imports inside `domains/`.

## Helpful quick links

- `CLAUDE.md` — AI assistant guide for this repo
- `README.md` — project overview and scope
- `DECISIONS.md` — architectural and semantic decision history
- `INVARIANTS.md` — non-negotiable rules
- `contracts/record_contract_v2.md` — record schema details
- `docs/join_key_shape_gating.md` — join-key shape gating rules
