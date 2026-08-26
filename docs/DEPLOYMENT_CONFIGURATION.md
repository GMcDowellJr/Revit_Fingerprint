# Deployment configuration

## Operator path and schema

Set `REVIT_FINGERPRINT_DEPLOYMENT_CONFIG` to an absolute or operator-resolved path
to a deployment-local JSON file. The Dynamo runner is the only environment reader;
it passes that path to `runner.extraction_context.build_extraction_context()`.

There is no repo-tracked entry point that launches `runner/run_dynamo.py` -- it
runs from a pasted Dynamo CPython3 node (`runner/thin_runner.py`) invoked via a
pyRevit "BatchExtract" button, outside any `.ps1`/`.py` script committed here.
`tools/corpus_update_runbook.ps1` and friends only orchestrate *post-export*
analysis (`run_extract_all.py`, `build_segment_manifest.py`, etc.) against
already-exported JSON, so setting this env var there has no effect on
extraction. Set it one of two ways instead:

- Node input: `runner/thin_runner.py`'s `IN[5]` forwards its value straight to
  `REVIT_FINGERPRINT_DEPLOYMENT_CONFIG` before importing `runner.run_dynamo`
  (same pattern as `IN[0]`'s output path). Point it at the local deployment
  config file's path -- never paste the file's *contents* into the Dynamo
  graph itself, since that would check deployment-authored data into an
  executable graph if the graph is ever committed.
- Machine/user environment variable, for any other invocation context.

Either way the JSON file itself stays outside this repository. The
checked-in, closed schema it must follow is:

```json
{
  "schema": "revit_fingerprint.deployment.v1",
  "project_info_shared_parameters": [
    {"key": "project_info.business_center", "name": "<deployment field name>"}
  ]
}
```

Each entry requires a non-blank `project_info.*` key and name. `guid` is optional.
With a GUID, lookup is exact and canonicalized to lowercase hyphenated form. If
`System.Guid` is unavailable extraction fails closed before reading that field;
there is no name fallback. Without a GUID, name lookup is an explicit deployment
choice. Unknown schema fields, built-in collisions, duplicate keys, conflicting
GUIDs, malformed GUIDs, and unregistered keys fail validation before extraction.

Do not check deployment-authored names or GUIDs into source, examples, fixtures,
defaults, or executable graphs.

## Quality and identity signature

An unconfigured optional field is omitted. A configured definition absent from a
document emits `q=unsupported`; a present blank value emits `q=missing`; a readable
value emits `q=ok`; and a read failure emits `q=unreadable`.

Configured `project_info.*` items participate in `identity.sig_hash.v2` in every
quality state. In particular, `project_info.business_center` intentionally
participates. Enabling or disabling a mapping changes the set of serialized
identity items, so optional configuration is a compatibility/migration boundary:
only compare identity hashes from runs with equivalent deployment mapping policy.

## Registering another key

1. Choose a stable, organization-neutral `project_info.*` key.
2. Add it to `domains.identity.allowed_keys` in
   `contracts/domain_identity_keys_v2.json`, with contract notes describing its
   meaning and signature decision.
3. Review and update identity signature-policy notes/version in the same change.
   The current policy hashes every emitted identity item; changing that composition
   requires an explicit signature-schema migration.
4. Validate the contract JSON and tests, then add the deployment-owned name/GUID
   only to the external deployment file.

At runtime `core.deployment_config.load_deployment_config()` loads the maintained
contract and rejects any mapping whose key is not registered.
