# Discovery sweep runbook

The supported sweep entry points remain the repository-root wrappers below.
They are the normal way to run discovery across domains; the individual
`tools/discover_join_policy.py` and `tools/discover_hash_policy.py` commands are
stage implementations invoked by the sweep, not replacements for its entry
point.

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

The command prints each subprocess at `START`, streams discovery output to the
console and its immutable log, emits a `still running` heartbeat every 30
seconds when a search is quiet, and prints `DONE` with elapsed time. Pareto can
be computationally expensive; a quiet subprocess is no longer indistinguishable
from a hung run. Press `Ctrl+C` to terminate the active child cleanly.
Before subprocesses start, fingerprint preparation reports its scope. Large
monolithic CSVs are scanned at most once per sweep and retained only as compact
order-independent per-domain hashes; they are not rescanned for every mode.

Both root wrappers delegate to `tools/discovery_orchestrator.py`; the older
`tools/run_discovery_sweep.py` wrapper remains available for backward
compatibility and delegates to the same orchestrator. The orchestrator invokes
`tools/discover_join_policy.py` for join stages and
`tools/discover_hash_policy.py --discovery-target sig` for sig stages.
`--force`/`-Force` bypasses
matching cache entries. `ExportsRoot`, `RepoRoot`, `SuggestionsCsv`, `Domains`,
`SkipJoin`, `SkipSig`, `WhatIf`, and `Run` are available in the spelling native
to each shell. Skipping both targets is rejected as an invalid no-op sweep.

## Execution model

An initial run must be a full sweep. A partial run is rejected until a prior
new-format full snapshot exists. Every policy stage starts with Greedy. Pareto
runs only when Greedy fails the strict gate: status `ok`, full coverage 1.0,
zero full collision and fragmentation, and no sample/full divergence. Discover
and validate run normally; harsh runs only when validate fails that same gate.
For shape-gated domains, every gate must have an accepted validation result;
one passing gate cannot suppress harsh evidence required by another gate.
Full-population verification is always enabled by the runbook.

Per-discriminator discovery rows are auxiliary evidence with
`result_scope=partition_diagnostic`. They remain in the archived stage CSV, but
the orchestrator excludes them from acceptance, cache, Pareto escalation, and
domain-wide summary selection; only global/runtime-policy rows drive those sweep
decisions.

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
`mixed` means the refreshed result combines fresh and cached stages. The
`stage_provenance_json`, `source_run_ids`, and `source_evidence_paths` columns
retain every stage's actual evidence location, including fully cached stages
that originated in different runs.
The run summary aggregates every shape gate conservatively and includes the
exception text when a failed requested refresh retains prior evidence.

## Cache contract

The versioned JSON manifest uses deterministic SHA-256 fingerprints over
domain-scoped records/items, the relevant governed policy block, target,
policy/search mode, shape gate, sampling and search parameters, mandatory full
verification, and the discovery engine semantic version. Canonical JSON makes
mapping order irrelevant. Only entries with result status `ok` are reused.
An `ok` status is not sufficient by itself: every shape-gate row must also pass
the full acceptance gate before the aggregate stage becomes reusable. A blocked
domain run discards cache entries created by any earlier successful stage.
Accepted Pareto evidence is cached independently; an unchanged rerun still
executes a non-accepted Greedy search to establish escalation, but can reuse the
accepted Pareto result instead of repeating that expensive search.
Policy, data, parameter, target/mode/gate, or engine-version changes invalidate
the relevant entry; `--force` bypasses it.

`DISCOVERY_ENGINE_VERSION` is defined in `tools/discovery_orchestrator.py`.
Bump it whenever discovery inputs, evaluation behavior, acceptance logic, or
other result-affecting orchestration semantics change. Do not bump it for a
documentation-only change.

The candidate/runtime scoring separation changed result-affecting evaluation
semantics, so its sweep cache contract is `discovery-sweep-v3`. Evidence cached
under v2 cannot be reused for the corrected discovery results.

WhatIf computes the plan without publishing summaries or cache changes. It
reports requested domains, likely fingerprint-verified cache hits, initial
Greedy stages, and the intended results root, and explicitly notes that
Pareto/harsh escalation cannot be predicted until evidence exists.
