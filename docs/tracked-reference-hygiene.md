# Tracked-reference hygiene

Maintained code, runnable examples, runbooks, and active governance documentation
use neutral identities and parameterized or repository-relative paths. Client and
organization identities are not runtime defaults.

## Retained references

The case-insensitive inventory terms may remain only in these classes:

- `CHANGELOG.md` and `DECISIONS.md`: append-only historical records, explicitly
  marked non-operational at the top of each file.
- `audit_results/**`: dated review evidence retained as an archived,
  non-operational artifact. It is not configuration or a runbook.
- `docs/repository-ai-client-reference-review.md`: the historical remediation
  inventory itself; its literal search terms and baseline counts explain what
  was audited.
- `tests/test_repository_data_remediation.py`: literals intentionally under test
  so regressions in tracked-reference hygiene can be detected.
- Tests that mention a removed legacy field name assert that the field is not
  emitted; the identity-bearing spelling is the behavior under test.
- `tools/patterns_analysis/_archive/**`: archived analysis code retained only
  for provenance and outside supported execution paths.

Everything else is expected to be free of the inventoried organization/client
terms. Product names such as ACC, SharePoint, Teams, OneDrive, Dynamo, Graphify,
and LLM remain where they describe supported functionality rather than identity.

## Executable paths

Runbooks resolve the repository from their script location and accept data roots
as parameters with neutral relative defaults. Desktop Connector hydration
requires an explicit manifest. Examples use neutral relative paths or obvious
`path\\to` placeholders; they do not point to a contributor workstation.
