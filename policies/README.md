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
