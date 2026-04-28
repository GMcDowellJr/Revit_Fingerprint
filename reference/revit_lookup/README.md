# RevitLookup Descriptor Reference

Source: https://github.com/lookup-foundation/RevitLookup/tree/develop/source/RevitLookup/Core/Decomposition/Descriptors
Pinned commit: 2401d82e4da1f95ab2648834597cb29f4842aa5d
Pinned date:   2026-04-28 15:58 UTC
License:       MIT (https://github.com/lookup-foundation/RevitLookup/blob/develop/License.md)

## Purpose

These files are **reference material only** — not executed by the Fingerprint pipeline.

Each descriptor in `Descriptors/` is a C# class that documents the exact Revit API
traversal RevitLookup uses for a given type (WallType, CompoundStructure, etc.).

See `REVIT_LOOKUP_DOMAIN_MAP.md` for the mapping between Fingerprint domains and
the descriptor files that cover the same Revit API surface.

## When to use

- Writing a new domain extractor: check what API calls are available and what
  guard conditions are needed (null checks, type discrimination, etc.)
- Auditing an existing extractor: compare what you call vs what they call
- Investigating an edge case: see how RevitLookup handles it in the wild
- Planning a future domain: check descriptor availability in DescriptorsMap.cs

## Important distinction

RevitLookup descriptors call *everything* for display purposes.
Fingerprint extractors call only configuration-stable signals.
The API traversal is reference; the selection of calls is your judgment.

## Keeping up to date

Re-run `sync_revitlookup_reference.py` from the repo root to refresh.
The script is idempotent — re-running when already at HEAD is a no-op.
Use `--priority-only` to fetch just the descriptors mapped to active domains.
