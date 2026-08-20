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
