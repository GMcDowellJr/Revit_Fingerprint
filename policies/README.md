# Deployment policy data

Checked-in policy files define deterministic defaults and examples. The
`client_sector.csv` file intentionally uses synthetic client labels; it is safe
for tests and demonstrates the required `client_label,sector` schema, but it
cannot classify a deployment's real clients.

For a governance narrative run against real data, provide an approved local
mapping explicitly:

```text
python tools/generate_governance_narrative.py ... --client-sector <approved-client-sector.csv>
```

Keep deployment mappings containing real organization names outside this
repository. A client absent from the selected mapping remains unclassified; it
must not be interpreted as confirmed non-healthcare.

### Optional ProjectInformation shared parameters

The identity extractor has no organization-authored shared parameter enabled by
default. A deployment can supply `project_info_shared_parameters` in the runner
context. Each entry contains a contract-registered `project_info.*` `key`, a
Revit display `name`, and, preferably, a shared-parameter `guid`. For example:

```json
{
  "project_info_shared_parameters": [
    {
      "key": "project_info.business_center",
      "name": "Deployment Business Center",
      "guid": "11111111-2222-4333-8444-555555555555"
    }
  ]
}
```

The example identifiers are synthetic. Deployments must keep their real mapping
in an approved local configuration and register any additional emitted key in
the identity contract and signature policy. With no mapping, optional deployment
fields are not emitted. A configured definition that is absent is emitted as
`unsupported_not_applicable`; a present blank value is `missing`.

### Enterprise identity policy

Governance CLIs load one immutable `enterprise_policy.v1` value. The checked-in
synthetic default is `InternalEnterprise`; the bookkeeping business-center token
is independently fixed at `0000`. Use `--enterprise-policy <local.json>` for a
local file and `--enterprise-label` only as a higher-precedence compatibility
override. Identity-dependent artifact directories receive deterministic
`enterprise_policy.json` provenance only on a writing run; dry runs do not write it.

### Runner deployment configuration

Set `REVIT_FINGERPRINT_DEPLOYMENT_CONFIG` to an approved local JSON file before
launching Dynamo. The runner loads it before any domain executes and passes its
validated `project_info_shared_parameters` mapping through `ctx` to `identity`.
The file schema is `revit_fingerprint.deployment.v1`. Keys must be registered in
`contracts/domain_identity_keys_v2.json`; duplicates, built-in collisions, blank
names, malformed/conflicting GUIDs, and unregistered keys fail the run before
extraction. A configured GUID requires `System.Guid` and never degrades to name
lookup. Omit `guid` to explicitly select display-name lookup.

Quality remains: unconfigured fields are omitted; absent definitions are
`unsupported.not_applicable`; blank values are `missing`; readable values are
`ok`; access failures are `unreadable`. Registered fields participate in the
signature only when listed by `policies/domain_sig_hash_policies.json`.
