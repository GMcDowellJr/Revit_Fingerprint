# PATCH PR — Non-breaking only

## Intent
<!-- One sentence: what safety/bugfix is being added? -->

## Invariants
- [ ] No fingerprint behavior changes (hash semantics unchanged)
- [ ] No domain scope changes (no new owned scopes; no hidden influence)
- [ ] No new required inputs
- [ ] Deterministic output preserved

## Change set
<!-- Exact files + what changed. -->
- Files changed:
  - 
- Key edits (high signal):
  - 

## Evidence
<!-- Minimal proof. Prefer before/after logs or JSON key lists. -->
- [ ] Reproduced issue (before): YES/NO
- [ ] Verified fix (after): YES/NO
- Runs:
  - Environment: CPython 3 / Dynamo / Revit version:
  - Model(s):
  - Steps:
- Artifacts:
  - [ ] Output keys unchanged (except expected safety fields like _meta)
  - [ ] Hashes unchanged for same inputs
  - [ ] Errors now surface with traceback / metadata

## Post-conditions
<!-- Must be true after merge; measurable if possible. -->
- 
- 

## Reviewer checklist
- [ ] Diff is tightly scoped
- [ ] Error handling does not mask failures silently
- [ ] Any added metadata is non-breaking for downstream consumers
