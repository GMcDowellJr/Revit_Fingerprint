#!/usr/bin/env python3
"""Pre-flight sizing for tools/discover_join_policy.py's --sample-size /
--max-candidate-fields / --max-k / --stratify-by knobs.

Those four knobs are currently passed as flat constants applied identically to
every domain, regardless of how much data or how many candidate fields that
domain actually has. This script computes, per domain, from the same flattened
records.csv/identity_items.csv discover_join_policy.py itself reads (no
discovery run required), the measurable quantities those knobs should actually
be a function of:

  N (records_total_domain)     -- population size for the domain
  G (distinct_sig_hash_groups) -- diversity: how many distinct configurations
                                    exist, since fragmentation is detected
                                    per-group (need >=2 sampled records of the
                                    same group to notice a split)
  F (distinct_file_count)      -- volume-imbalance risk: how many files
                                    contribute records, informing whether
                                    --stratify-by file_id is worth using
  n_candidates                 -- distinct populated identity-item keys for
                                    the domain (what --max-candidate-fields
                                    would otherwise cap blindly at a flat 64)
  required_count                -- size of the domain's existing
                                    required_items baseline (from an optional
                                    --policy-json); pareto_search() guarantees
                                    every subset it tries includes these
                                    fields structurally, so this only
                                    consumes max_k budget, not combinatorial
                                    pool budget (see the --max-k section below)
  optional_count                -- size of the domain's existing
                                    optional_items list; unlike required
                                    fields, these DO inflate the harsh/
                                    validate combinatorial pool Pareto
                                    searches extras from

and turns them into concrete suggested flag values plus, optionally, ready-to
run discover_join_policy.py commands.

Sizing rationale (see also the join-key discovery review in this repo's audit
trail / conversation history this tool was built from):

  --sample-size: sized off diversity, not just population size. The project's
    own acceptance criterion is fragmentation == 0
    (docs/phase_2_join-key_discovery.md); fragmentation is only detectable
    when a sample contains multiple records of the SAME sig_hash group, so
    the sample should scale with G (distinct groups), aiming for roughly
    --sample-k-per-group representatives per group, floored so small domains
    just get everything (which _sample_domain_records already does for free
    when sample_size >= population size).

  --max-candidate-fields / --max-k (discover mode): solved JOINTLY against a
    subset-count compute budget, since tools/pareto_joinkey_search.py's cost
    is combinatorial (sum of C(n, i) for i=1..max_k) over the data-observed
    candidate pool alone (no required/optional baseline involved in discover
    mode). Given the domain's actual candidate-field count, this finds the
    largest max_k that fits the budget without trimming fields; only trims
    the candidate pool (keeping the highest most-frequently-populated fields,
    same ranking _pick_candidate_fields already uses) if even --min-k doesn't
    fit within budget at the domain's full field count.

  --max-k (harsh/validate mode): required fields cost nothing combinatorially
    (pareto_search() builds every subset as required_fields + a combination
    of the remaining fields, so representing the baseline is exactly one
    evaluation, not C(pool, required_count)) -- so harsh_max_k =
    required_count + however many EXTRA fields fit the budget at the
    (optional-inflated) extra-field pool. This is structurally always
    feasible: the "extra_k = 0" case (no extras at all, just the required
    baseline) costs exactly 1 regardless of pool size or budget.

  --stratify-by: recommended as 'file_id' whenever a domain's records are
    concentrated in relatively few files (a low F : N ratio) -- this project's
    own governance model (Template/Container files carry a much larger
    configured vocabulary than typical Project files) makes this a real,
    not hypothetical, risk for volume imbalance skewing a pooled sample.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import shlex
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence

try:
    from tools.discover_join_policy import _pick_candidate_fields, _read_csv, _write_csv
    from tools.join_key_discovery.eval import normalize_policy_block
except ModuleNotFoundError:
    from discover_join_policy import _pick_candidate_fields, _read_csv, _write_csv
    from join_key_discovery.eval import normalize_policy_block


# ---------------------------------------------------------------------------
# Pure computation (no file I/O) -- directly unit-testable.
# ---------------------------------------------------------------------------

def compute_domain_stats(
    records: Sequence[Dict[str, str]],
    items: Sequence[Dict[str, str]],
    domain: str,
) -> Dict[str, object]:
    """N/G/F/n_candidates plus a real file-concentration measure for one domain
    from already-loaded records/items rows.

    File concentration is computed as HHI over per-file record-count shares,
    following this repo's own established HHI convention (docs/METRICS.md):
    closed universe (shares sum to 1.0) with an explicit "unknown" bucket for
    blank file_id, rather than silently excluding those records. A plain
    N/F average would be blind to concentration -- 6000 records in 1 file and
    6000 records spread evenly across 6000 files both average to the same
    N/F, but only the former is a real sampling-imbalance risk.
    """
    dom_records = [r for r in records if r.get("domain", "") == domain]
    n = len(dom_records)
    g = len({r.get("sig_hash", "").strip() for r in dom_records if r.get("sig_hash", "").strip()})

    file_counts: Dict[str, int] = {}
    unknown_file_count = 0
    for r in dom_records:
        fid = r.get("file_id", "").strip()
        if fid:
            file_counts[fid] = file_counts.get(fid, 0) + 1
        else:
            unknown_file_count += 1
    f = len(file_counts)
    shares = [c / n for c in file_counts.values()] if n else []
    if unknown_file_count:
        shares.append(unknown_file_count / n)
    file_hhi = sum(s * s for s in shares) if shares else 0.0
    file_effective_cluster_count = (1.0 / file_hhi) if file_hhi > 0 else 0.0

    # Ranked (not just counted) via _pick_candidate_fields(items, 0): same
    # frequency-then-alphabetical ranking discover_join_policy.py's actual
    # scoped_candidates uses, with max_fields=0 (its own "no cap" case) so the
    # FULL ranked list comes back uncapped -- suggest_params_for_domain slices
    # the top --max-candidate-fields itself once that value is known, and can
    # use these actual field NAMES (not just a count) to compute the harsh/
    # validate pool size as a true deduplicated union against required/
    # optional field names, instead of a pessimistic arithmetic sum.
    dom_items = [it for it in items if it.get("domain", "") == domain]
    candidate_field_names_ranked = _pick_candidate_fields(dom_items, 0)
    return {
        "records_total_domain": n,
        "distinct_sig_hash_groups": g,
        "distinct_file_count": f,
        "file_hhi": file_hhi,
        "file_effective_cluster_count": file_effective_cluster_count,
        "candidate_field_count": len(candidate_field_names_ranked),
        "candidate_field_names_ranked": candidate_field_names_ranked,
    }


def suggest_sample_size(n: int, g: int, k_per_group: int = 15, floor: int = 500) -> int:
    """clamp(k_per_group * G, floor, N) -- see module docstring for rationale.

    Falls back to min(n, floor) if the domain has no sig_hash groups at all
    (e.g. every record blocked / sig_hash empty) since group-aware sizing has
    nothing to key off in that case.
    """
    if n <= 0:
        return 0
    if g <= 0:
        return min(n, floor)
    raw = k_per_group * g
    return min(n, max(floor, raw))


def _cumulative_subset_count(n: int, k: int) -> int:
    """Sum of C(n, i) for i=1..k -- the number of Pareto subsets evaluated."""
    if n <= 0 or k <= 0:
        return 0
    return sum(math.comb(n, i) for i in range(1, min(k, n) + 1))


def _cumulative_subset_count_from_zero(n: int, k: int) -> int:
    """Sum of C(n, i) for i=0..k -- includes the i=0 ("no extra fields") case,
    which trivially costs exactly 1 (there is exactly one way to add zero
    extras). Used for the harsh/validate mode EXTRA-field budget:
    tools/pareto_joinkey_search.py's pareto_search() now guarantees required
    fields are present in every subset it tries by CONSTRUCTION (built as
    required_fields + a combination of the remaining fields), not by
    combinatorially selecting them out of the pool -- so representing the
    required baseline costs zero combinatorial budget, and this function
    starts counting from "no extras" rather than "at least one field."
    """
    if k < 0:
        return 0
    return 1 + _cumulative_subset_count(n, k)


def solve_candidate_fields_and_k(n_candidates: int, budget: int = 20000, min_k: int = 2):
    """Jointly size --max-candidate-fields/--max-k against a subset-count budget.

    Prefers keeping every candidate field (n_candidates) and growing max_k as
    large as the budget allows; only trims the candidate pool -- keeping the
    top-ranked fields, same as _pick_candidate_fields's own frequency ranking
    would -- if even min_k doesn't fit the budget at the domain's full field
    count. Returns (suggested_max_candidate_fields, suggested_max_k).
    """
    if n_candidates <= 0:
        return 0, min_k

    max_possible_k = min(n_candidates, 64)
    best_k = 0
    for k in range(1, max_possible_k + 1):
        if _cumulative_subset_count(n_candidates, k) <= budget:
            best_k = k
        else:
            break

    if best_k >= min_k:
        return n_candidates, best_k

    # Even min_k doesn't fit at the full field count -- shrink the candidate
    # pool until min_k does, rather than shrinking k below the caller's floor.
    lo, hi = 1, n_candidates
    best_m = 1
    while lo <= hi:
        mid = (lo + hi) // 2
        if _cumulative_subset_count(mid, min_k) <= budget:
            best_m = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return best_m, min_k


def suggest_params_for_domain(
    stats: Dict[str, object],
    *,
    required_count: int = 0,
    optional_count: int = 0,
    required_field_names: Sequence[str] = (),
    optional_field_names: Sequence[str] = (),
    k_per_group: int = 15,
    sample_floor: int = 500,
    subset_budget: int = 20000,
    min_k: int = 2,
    concentration_ratio_threshold: float = 0.5,
) -> Dict[str, object]:
    """Combine the sizing functions above into one suggestion dict for a domain."""
    n = int(stats["records_total_domain"])
    g = int(stats["distinct_sig_hash_groups"])
    f = int(stats["distinct_file_count"])
    file_effective_cluster_count = float(stats.get("file_effective_cluster_count", f))
    n_candidates = int(stats["candidate_field_count"])
    candidate_field_names_ranked = list(stats.get("candidate_field_names_ranked") or [])

    sample_size = suggest_sample_size(n, g, k_per_group=k_per_group, floor=sample_floor)
    max_candidate_fields, discover_max_k = solve_candidate_fields_and_k(
        n_candidates, budget=subset_budget, min_k=min_k,
    )

    # harsh/validate work_candidates fold in the existing required AND optional
    # baseline UNCONDITIONALLY (req + opt + scoped_candidates for harsh, req +
    # opt for validate -- see tools/discover_join_policy.py's policy_mode
    # branch), regardless of --max-candidate-fields.
    #
    # tools/pareto_joinkey_search.py's pareto_search() (the Callable API
    # discover_join_policy.py actually calls) guarantees required-field
    # inclusion STRUCTURALLY: every subset it tries is built as
    # required_fields + a combination drawn only from the REMAINING
    # (non-required) fields, rather than combinatorially selecting required
    # fields out of the whole pool. That means representing the required
    # baseline costs exactly ONE evaluation (the "zero extra fields" case),
    # not C(pool, required_count) -- required_count no longer inflates the
    # combinatorial search space at all. It only consumes max_k budget:
    # max_extra_k = max_k - required_count is how many EXTRA (non-required)
    # fields the search can still add on top of the guaranteed baseline.
    #
    # Only optional_count still inflates the combinatorial POOL Pareto
    # searches the extras from (policy fields -- possibly including names not
    # even populated in the domain's data -- unconditionally added to
    # work_candidates alongside the data-observed scoped_candidates).
    # discover_join_policy.py's actual work_candidates is a DEDUPLICATED union
    # (_without_excluded -> _dedupe), not a raw concatenation -- optional
    # policy fields normally overlap the observed candidate pool (they were
    # presumably discovered as legitimate identity items in the first place)
    # -- so when the caller supplies the actual field NAMES
    # (candidate_field_names_ranked from compute_domain_stats, plus
    # required_field_names/optional_field_names from the policy JSON), the
    # extra pool is the TRUE deduplicated union size (scoped_candidates ∪
    # optional_items) - required_items, not a pessimistic arithmetic sum.
    # Falls back to max_candidate_fields + optional_count (still a safe, if
    # pessimistic, upper bound) when names aren't available, e.g.
    # suggest_params_for_domain's own direct unit tests that only pass counts.
    if candidate_field_names_ranked and (required_field_names or optional_field_names):
        capped_candidate_names = set(
            candidate_field_names_ranked[:max_candidate_fields] if max_candidate_fields > 0 else candidate_field_names_ranked
        )
        extra_pool_size = len((capped_candidate_names | set(optional_field_names)) - set(required_field_names))
    else:
        extra_pool_size = max_candidate_fields + optional_count

    # Largest extra_k (>=0) affordable at extra_pool_size within budget --
    # always succeeds at extra_k=0 (cost exactly 1) except the degenerate
    # subset_budget < 1 case, so this is structurally always feasible, unlike
    # the old required-count-as-combinatorial-floor model where a large
    # enough required baseline really could make every k unaffordable.
    max_extra_k = 0
    if subset_budget >= 1:
        for k in range(1, min(extra_pool_size, 64) + 1):
            if _cumulative_subset_count_from_zero(extra_pool_size, k) <= subset_budget:
                max_extra_k = k
            else:
                break
    harsh_max_k = required_count + max_extra_k
    harsh_pareto_feasible = subset_budget >= 1

    # Concentration, not just N/F average: file_effective_cluster_count (1/HHI
    # over per-file record-count shares) meaningfully below the actual distinct
    # file count f means a handful of files carry a disproportionate share of
    # this domain's records (e.g. Template/Container files) -- exactly the
    # volume-imbalance risk --stratify-by file_id guards against. An N/F
    # average alone can't distinguish this from perfectly even distribution
    # (6000 records in 1 file and 6000 spread evenly across 6000 files both
    # average to the same N/F). Only relevant when sampling actually happens
    # (sample_size < n) -- with no cap, imbalance can't bias anything.
    sampling_applies = bool(sample_size) and sample_size < n
    stratify_recommended = (
        sampling_applies
        and f > 1
        and file_effective_cluster_count < (f * concentration_ratio_threshold)
    )

    notes: List[str] = []
    if not harsh_pareto_feasible:
        notes.append(
            f"--subset-budget ({subset_budget}) is too small to evaluate even the required-only "
            "baseline candidate; check --subset-budget is a positive value."
        )
    elif required_count and max_extra_k == 0 and extra_pool_size > 0:
        notes.append(
            f"required_items count ({required_count}) plus the harsh/validate extra-field pool "
            f"({extra_pool_size}: --max-candidate-fields {max_candidate_fields} + {optional_count} optional, "
            "deduplicated against required) leaves no room within --subset-budget "
            f"({subset_budget}) to explore any field beyond the required baseline -- harsh/validate "
            f"will only ever evaluate the required-only candidate (--max-k {harsh_max_k}). Raise "
            "--subset-budget or lower --max-candidate-fields/optional_items to allow exploration."
        )
    elif optional_count and extra_pool_size > max_candidate_fields:
        notes.append(
            f"existing optional_items count ({optional_count}) enlarges the harsh/validate extra-field "
            f"pool to {extra_pool_size} (--max-candidate-fields {max_candidate_fields} + {optional_count} "
            "optional, deduplicated against required); harsh/validate can add up to "
            f"{max_extra_k} extra field(s) on top of the {required_count} required field(s) "
            f"(--max-k {harsh_max_k}) within --subset-budget ({subset_budget})."
        )
    if stratify_recommended:
        notes.append(
            "records concentrated in relatively few files for this population size; "
            "consider --stratify-by file_id to avoid a pooled sample being dominated "
            "by a handful of high-volume files (e.g. Template/Container files)."
        )
    if n and sample_size >= n:
        notes.append("population already <= suggested sample size; no sampling cap needed (--sample-size 0 or omit).")

    return {
        "records_total_domain": n,
        "distinct_sig_hash_groups": g,
        "distinct_file_count": f,
        "candidate_field_count": n_candidates,
        "required_items_count": required_count,
        "optional_items_count": optional_count,
        "suggested_sample_size": sample_size,
        "suggested_max_candidate_fields": max_candidate_fields,
        "suggested_max_k_discover": discover_max_k,
        "suggested_max_k_harsh_validate": harsh_max_k,
        "harsh_pareto_feasible": harsh_pareto_feasible,
        "stratify_by_recommended": "file_id" if stratify_recommended else "",
        "notes": "; ".join(notes),
    }


# ---------------------------------------------------------------------------
# CLI / file I/O
# ---------------------------------------------------------------------------

def _resolve_phase0_dir(path: Path) -> Path:
    """Mirror discover_join_policy.py's/discover_hash_policy.py's phase0-dir resolution."""
    if (path / "records.csv").exists() or (path / "phase0_records.csv").exists():
        return path
    records_dir = path / "records"
    if (records_dir / "records.csv").exists():
        return records_dir
    results_records = path / "results" / "records"
    if (results_records / "records.csv").exists():
        return results_records
    nested = path / "phase0_v21"
    if (nested / "records.csv").exists():
        return nested
    return path


def _load_policy_fields(policy_json: Optional[Path]) -> Dict[str, Dict[str, object]]:
    """Per-domain required/optional field NAMES (plus their counts) from a
    join-key policy JSON.

    Both matter for harsh/validate sizing: discover_join_policy.py's
    work_candidates for those modes is req + opt (+ scoped_candidates for
    harsh) -- optional_items inflates the real Pareto candidate pool exactly
    like required_items does, unconditionally and regardless of
    --max-candidate-fields, including policy field names that aren't even
    populated anywhere in the domain's data. The actual NAMES (not just
    counts) let suggest_params_for_domain compute the true deduplicated pool
    size instead of a pessimistic arithmetic sum -- see its own docstring.

    Uses normalize_policy_block() -- the SAME parsing discover_join_policy.py
    itself applies to every policy block before running -- rather than
    reimplementing the alias/precedence rules by hand. A hand-rolled
    `required_items or required_fields` here previously disagreed with
    normalize_policy_block()'s actual precedence (required_fields checked
    FIRST there, required_items here) whenever a policy set both, and had no
    fallback at all for a policy that only specifies `selected_fields` (a
    supported legacy shape normalize_policy_block() falls back to as the
    required baseline). Both mismatches meant a policy could be sized as
    budget-safe here while the emitted command's actual required baseline --
    and therefore its real Pareto search space -- was larger than what was
    ever checked against --subset-budget.
    """
    if not policy_json or not policy_json.exists():
        return {}
    loaded = json.loads(policy_json.read_text(encoding="utf-8"))
    cand = loaded.get("domains") if isinstance(loaded, dict) else {}
    if not isinstance(cand, dict):
        return {}
    out: Dict[str, Dict[str, object]] = {}
    for domain, block in cand.items():
        if not isinstance(block, dict):
            continue
        normalized = normalize_policy_block(block)
        req = normalized["required_fields"]
        opt = normalized["optional_items"]
        out[str(domain)] = {
            "required_count": len(req),
            "optional_count": len(opt),
            "required_fields": list(req),
            "optional_fields": list(opt),
        }
    return out


def _emit_command(domain: str, suggestion: Dict[str, object], phase0_dir: str, policy_json: Optional[str]) -> List[str]:
    """Build ready-to-run discover_join_policy.py command(s) for one domain.

    Returns two commands, not one, whenever the harsh/validate command needs
    different treatment than discover: suggested_max_k_harsh_validate is
    computed from a different pool/budget than suggested_max_k_discover (see
    suggest_params_for_domain's docstring/notes) and commonly differs whenever
    a domain has any required baseline at all, since pareto_search()'s
    structural required-field guarantee (tools/pareto_joinkey_search.py) means
    harsh_max_k = required_count + however many extra fields fit the budget,
    unrelated to discover mode's own data-only field pool. A single combined
    command at the discover value would under-size harsh/validate's search
    space relative to what it can actually represent. harsh_pareto_feasible
    is only False in the degenerate --subset-budget < 1 case; in that (rare)
    case only the harsh/validate command is forced to --search-modes greedy
    instead of the CLI default greedy,pareto -- discover/validate's own
    candidate pools stay small and cheap regardless.
    """
    def _base_parts(max_k: object) -> List[str]:
        # Dynamic values (paths, domain names) are shell-quoted via shlex.quote:
        # an otherwise-valid directory like "/tmp/Revit Results/records" would
        # otherwise split into two arguments when the printed command is
        # actually run, and discover_join_policy.py would fail to find
        # records.csv under the truncated first half.
        parts = [
            "python tools/discover_join_policy.py",
            f"--phase0-dir {shlex.quote(str(phase0_dir))}",
            f"--domains {shlex.quote(domain)}",
            f"--sample-size {suggestion['suggested_sample_size']}",
            f"--max-candidate-fields {suggestion['suggested_max_candidate_fields']}",
            f"--max-k {max_k}",
        ]
        if suggestion.get("stratify_by_recommended"):
            parts.append(f"--stratify-by {shlex.quote(str(suggestion['stratify_by_recommended']))}")
        if policy_json:
            parts.append(f"--policy-json {shlex.quote(str(policy_json))}")
        parts.append("--warn-only")
        return parts

    discover_k = suggestion["suggested_max_k_discover"]
    harsh_k = suggestion["suggested_max_k_harsh_validate"]
    harsh_feasible = suggestion.get("harsh_pareto_feasible", True)

    # Split whenever harsh/validate needs *different* treatment than discover.
    # discover_max_k and harsh_max_k are computed from genuinely different pool/
    # budget reasoning now (harsh_max_k = required_count + however many EXTRA
    # fields fit the budget, unrelated to discover mode's data-only pool), so
    # they commonly differ whenever a domain has any required baseline at all --
    # a single combined command at the smaller value would under-size harsh
    # mode's search space. harsh_pareto_feasible is only False in the
    # degenerate --subset-budget < 1 case (pareto_search()'s structural
    # required-field guarantee means representing the baseline itself never
    # blows the budget), but is still checked so the (rare) infeasible case
    # forces --search-modes greedy on just the harsh/validate command, not
    # discover/validate too (their own candidate pools stay small and cheap
    # regardless).
    if harsh_k == discover_k and harsh_feasible:
        return [" \\\n    ".join(_base_parts(discover_k))]

    discover_cmd = _base_parts(discover_k) + ["--policy-modes discover"]
    harsh_cmd = _base_parts(harsh_k) + ["--policy-modes validate,harsh"]
    if not harsh_feasible:
        harsh_cmd.append("--search-modes greedy")
    return [" \\\n    ".join(discover_cmd), " \\\n    ".join(harsh_cmd)]


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Suggest per-domain --sample-size/--max-candidate-fields/--max-k/--stratify-by "
            "values for tools/discover_join_policy.py, sized from the domain's actual data "
            "(population size, distinct sig_hash groups, distinct files, candidate-field "
            "count) instead of guessing flat constants across every domain."
        )
    )
    ap.add_argument("--phase0-dir", default="results/records", help="Flatten output directory (same resolution as discover_join_policy.py).")
    ap.add_argument("--domains", default=None, help="Optional comma-separated domain allow-list.")
    ap.add_argument("--policy-json", default=None, help="Optional current join-key policy JSON, used to size harsh/validate max_k against each domain's existing required_items count.")
    ap.add_argument("--sample-k-per-group", type=int, default=15, help="Target sampled records per distinct sig_hash group (default 15).")
    ap.add_argument("--sample-floor", type=int, default=500, help="Minimum suggested sample size (default 500).")
    ap.add_argument("--subset-budget", type=int, default=20000, help="Target Pareto subset-evaluation budget per domain/policy-mode/search-mode run (default 20000).")
    ap.add_argument("--min-k", type=int, default=2, help="Minimum acceptable max_k when solving the candidate-fields/max-k tradeoff (default 2).")
    ap.add_argument("--out", default=None, help="Optional CSV output path (default: <phase0-dir>/../diagnostics/discovery_param_suggestions.csv).")
    ap.add_argument("--emit-commands", action="store_true", help="Also print ready-to-run discover_join_policy.py commands per domain.")
    args = ap.parse_args()

    phase0_dir = _resolve_phase0_dir(Path(args.phase0_dir))
    records_path = phase0_dir / "records.csv"
    if not records_path.exists():
        legacy = phase0_dir / "phase0_records.csv"
        if legacy.exists():
            records_path = legacy
        else:
            raise SystemExit(f"records.csv not found under phase0 dir: {phase0_dir}")
    records = _read_csv(records_path)

    items_path = phase0_dir / "identity_items.csv"
    if not items_path.exists():
        legacy_items = phase0_dir / "phase0_identity_items.csv"
        if legacy_items.exists():
            items_path = legacy_items

    # Load items domain-by-domain from per-domain shards when available.
    # Falls back to one monolithic load + in-memory partition when shards
    # don't exist. Mirrors tools/apply_join_policy.py's _get_domain_items()
    # closure (see tools/discover_join_policy.py's identical port for the
    # full rationale on the .complete-sentinel gating and the warning below).
    # Unlike discover_join_policy.py, a missing monolithic file here is not
    # fatal -- compute_domain_stats() degrades gracefully to
    # candidate_field_count=0 for the affected domain(s), same as the
    # pre-port behavior when identity_items.csv was simply absent.
    shard_dir = phase0_dir / "identity_items_by_domain"
    _use_shards = (shard_dir / ".complete").is_file()
    if not _use_shards and shard_dir.is_dir() and any(shard_dir.glob("*.csv")):
        sys.stderr.write(
            "[WARN suggest] identity_items_by_domain/ contains CSV files but .complete sentinel "
            "is absent -- possible interrupted flatten run. Falling back to monolithic "
            "identity_items.csv to avoid silently missing domain items.\n"
        )
    _monolithic_by_domain: Dict[str, List[Dict[str, str]]] = {}
    if not _use_shards and items_path.exists():
        for _row in _read_csv(items_path):
            _d = str(_row.get("domain", "")).strip()
            _monolithic_by_domain.setdefault(_d, []).append(_row)

    def _get_domain_items(domain: str) -> List[Dict[str, str]]:
        if _use_shards:
            _shard = shard_dir / f"{domain}.csv"
            return _read_csv(_shard) if _shard.is_file() else []
        return _monolithic_by_domain.get(domain, [])

    domains = sorted({r.get("domain", "").strip() for r in records if r.get("domain", "").strip()}, key=str.lower)
    if args.domains:
        allow = {d.strip() for d in str(args.domains).split(",") if d.strip()}
        domains = [d for d in domains if d in allow]

    policy_fields = _load_policy_fields(Path(args.policy_json)) if args.policy_json else {}

    rows: List[Dict[str, object]] = []
    for domain in domains:
        stats = compute_domain_stats(records, _get_domain_items(domain), domain)
        domain_fields = policy_fields.get(domain, {})
        suggestion = suggest_params_for_domain(
            stats,
            required_count=domain_fields.get("required_count", 0),
            optional_count=domain_fields.get("optional_count", 0),
            required_field_names=domain_fields.get("required_fields", []),
            optional_field_names=domain_fields.get("optional_fields", []),
            k_per_group=int(args.sample_k_per_group),
            sample_floor=int(args.sample_floor),
            subset_budget=int(args.subset_budget),
            min_k=int(args.min_k),
        )
        row = {"domain": domain, **suggestion}
        rows.append(row)
        print(
            f"[suggest] domain={domain} N={stats['records_total_domain']} "
            f"G={stats['distinct_sig_hash_groups']} F={stats['distinct_file_count']} "
            f"candidates={stats['candidate_field_count']} -> "
            f"sample-size={suggestion['suggested_sample_size']} "
            f"max-candidate-fields={suggestion['suggested_max_candidate_fields']} "
            f"max-k(discover)={suggestion['suggested_max_k_discover']} "
            f"max-k(harsh/validate)={suggestion['suggested_max_k_harsh_validate']} "
            f"stratify-by={suggestion['stratify_by_recommended'] or '(none)'}",
            flush=True,
        )

    out_path = Path(args.out) if args.out else (phase0_dir.parent / "diagnostics" / "discovery_param_suggestions.csv")
    fields = [
        "domain", "records_total_domain", "distinct_sig_hash_groups", "distinct_file_count",
        "candidate_field_count", "required_items_count", "optional_items_count",
        "suggested_sample_size", "suggested_max_candidate_fields",
        "suggested_max_k_discover", "suggested_max_k_harsh_validate",
        "stratify_by_recommended", "notes",
    ]
    _write_csv(out_path, fields, rows)
    print(f"[suggest] wrote {out_path}", flush=True)

    if args.emit_commands:
        print("\n[suggest] ready-to-run commands:\n", flush=True)
        # Pass the RESOLVED phase0_dir, not args.phase0_dir verbatim: when
        # --phase0-dir was given as one of the root forms _resolve_phase0_dir
        # accepts (a repo root containing results/records, a Results_v21 root,
        # etc.), discover_join_policy.py performs no such resolution itself --
        # it reads straight from "<argument>/records.csv" -- so an emitted
        # command built from the original unresolved argument would fail to
        # find the CSVs this very suggestion run just read successfully.
        for row in rows:
            for cmd in _emit_command(str(row["domain"]), row, str(phase0_dir), args.policy_json):
                print(cmd)
                print()


if __name__ == "__main__":
    main()
