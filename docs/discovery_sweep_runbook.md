# Discovery sweep runbook

The sweep has equivalent repository-root entry points:

```powershell
.\run_discovery_sweep.ps1 -Run
.\run_discovery_sweep.ps1 -Domains "walls,doors" -Run
.\run_discovery_sweep.ps1 -WhatIf
```

```bash
python run_discovery_sweep.py --run
python run_discovery_sweep.py --domains walls,doors --run
python run_discovery_sweep.py --what-if
```

Both delegate to `tools/discovery_orchestrator.py`; `--force`/`-Force` bypasses
matching cache entries. `ExportsRoot`, `RepoRoot`, `SuggestionsCsv`, `Domains`,
`SkipJoin`, `SkipSig`, `WhatIf`, and `Run` are available in the spelling native
to each shell.

## Execution model

An initial run must be a full sweep. A partial run is rejected until a prior
new-format full snapshot exists. Every policy stage starts with Greedy. Pareto
runs only when Greedy fails the strict gate: status `ok`, full coverage 1.0,
zero full collision and fragmentation, and no sample/full divergence. Discover
and validate run normally; harsh runs only when validate fails that same gate.
Full-population verification is always enabled by the runbook.

`validate` retains its existing meaning: it searches within the governed
`required_fields + optional_items` candidate universe. It does not score only
one frozen selected field set. Discovery classifications are review evidence,
not policy approval; summaries therefore use `governance_status=unratified`.

## Output and history

```text
<ExportsRoot>/diagnostics/discovery_results/
  cache_manifest.json
  summaries/
    join_key_discovery_summary_<timestamp>.csv
    sig_hash_discovery_summary_<timestamp>.csv
    discovery_run_summary_<timestamp>.csv
  domains/<domain>/<run_id>/
    run_manifest.json
    join/ or sig/
    logs/
```

Snapshots, evidence folders, logs, and manifests are immutable. Find the latest
summary by lexically sorting its UTC timestamped filename. Partial runs publish
a new full-state snapshot: requested rows are refreshed or cache-verified and
unrequested rows are inherited without duplicating their evidence.

Provenance is explicit. `fresh` was computed now; `cached` was requested and
fingerprint-verified; `carried_forward` was outside scope and was not checked;
`blocked_refresh_previous_retained` means a requested refresh failed and the
prior result remains. `summary_timestamp` is snapshot age while
`domain_result_timestamp` is evidence age.

## Cache contract

The versioned JSON manifest uses deterministic SHA-256 fingerprints over
domain-scoped records/items, the relevant governed policy block, target,
policy/search mode, shape gate, sampling and search parameters, mandatory full
verification, and the discovery engine semantic version. Canonical JSON makes
mapping order irrelevant. Only entries with result status `ok` are reused.
An `ok` status is not sufficient by itself: every shape-gate row must also pass
the full acceptance gate before the aggregate stage becomes reusable. A blocked
domain run discards cache entries created by any earlier successful stage.
Policy, data, parameter, target/mode/gate, or engine-version changes invalidate
the relevant entry; `--force` bypasses it.

`DISCOVERY_ENGINE_VERSION` is defined in `tools/discovery_orchestrator.py`.
Bump it whenever discovery inputs, evaluation behavior, acceptance logic, or
other result-affecting orchestration semantics change. Do not bump it for a
documentation-only change.

WhatIf computes the plan without publishing summaries or cache changes. It
reports requested domains, likely fingerprint-verified cache hits, initial
Greedy stages, and the intended results root, and explicitly notes that
Pareto/harsh escalation cannot be predicted until evidence exists.
