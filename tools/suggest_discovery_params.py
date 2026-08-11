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
                                    --policy-json), since validate/harsh mode
                                    search spaces must be able to represent at
                                    least that many fields simultaneously

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

  --max-candidate-fields / --max-k: solved JOINTLY against a subset-count
    compute budget, since tools/pareto_joinkey_search.py's cost is
    combinatorial (sum of C(n, i) for i=1..max_k). Given the domain's actual
    candidate-field count, this finds the largest max_k that fits the budget
    without trimming fields; only trims the candidate pool (keeping the
    highest most-frequently-populated fields, same ranking
    _pick_candidate_fields already uses) if even --min-k doesn't fit within
    budget at the domain's full field count.

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
from pathlib import Path
from typing import Dict, List, Optional, Sequence

try:
    from tools.discover_join_policy import _pick_candidate_fields, _read_csv, _write_csv
except ModuleNotFoundError:
    from discover_join_policy import _pick_candidate_fields, _read_csv, _write_csv


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
    # baseline (req + opt + scoped_candidates / req + opt -- see
    # tools/discover_join_policy.py's policy_mode branch), UNCONDITIONALLY on
    # top of scoped_candidates -- --max-candidate-fields only caps the
    # data-observed scoped_candidates pool, not req/opt, which can include
    # policy field names that aren't even among the domain's populated
    # candidates at all (stale/unpopulated names still count toward the pool
    # Pareto enumerates over). So the real harsh/validate candidate POOL SIZE
    # is max_candidate_fields + required_count + optional_count, not just
    # max_candidate_fields. The discover-mode budget solve above knows about
    # neither.
    #
    # required_count and optional_count matter DIFFERENTLY here, though:
    # optional fields never need to co-occur together in one selected subset
    # -- they're just additional pool members Pareto may combine in at any k
    # -- while required fields do, because discover_join_policy.py's
    # validate-mode frontier filter demands `set(req).issubset(selected)`
    # (harsh mode imposes no such constraint on selected at all). So the
    # minimum k that can represent a USEFUL harsh/validate search is
    # required_count alone, never required_count + optional_count -- treating
    # optional_count as part of that floor (an earlier version of this logic
    # did) incorrectly declared runs infeasible whenever a domain simply had a
    # large optional_items list, even with zero or few required fields, where
    # Pareto could still validly explore small subsets from the larger pool.
    #
    # tools/discover_join_policy.py's actual pareto_search() (the Callable API
    # it calls, not the separate policy-aware CLI path elsewhere in
    # pareto_joinkey_search.py) has no required-fields-aware enumeration --
    # it's plain itertools.combinations(fields, k) for every k up to max_k,
    # blind to which fields are "required"/"optional". Blindly bumping max_k
    # to represent the required baseline without checking the resulting
    # combinatorial cost AT THE LARGER (optional-inflated) POOL can recommend
    # a command that would evaluate billions of subsets.
    #
    # required_count is the absolute minimum k that can represent selecting
    # the whole required baseline as one candidate -- no smaller k can, so
    # checking cost at exactly that k (against the harsh pool size) is the
    # tightest possible feasibility check: if even that's unaffordable, no
    # larger k is either (monotonicity of Sum C(pool, i)), so there's no
    # "smaller headroom" to search for. When it IS affordable (including the
    # required_count == 0 case, which always is), max_k is set to
    # max(discover_max_k, required_count) for a bit of exploration room beyond
    # the bare minimum, re-checked against the same budget since cost still
    # grows with k; if that larger value doesn't fit, it falls back to the
    # minimal-but-verified-affordable required_count instead of
    # discover_max_k (which was never checked against the harsh pool size at
    # all).
    policy_fixed_count = required_count + optional_count  # pool-size inflation, upper-bound fallback
    # discover_join_policy.py's actual work_candidates is a DEDUPLICATED union
    # (_without_excluded -> _dedupe), not a raw concatenation -- required/
    # optional policy fields normally overlap the observed candidate pool
    # (they were presumably discovered as legitimate identity items in the
    # first place), so summing max_candidate_fields + required_count +
    # optional_count arithmetically OVER-counts the true pool size whenever
    # there's overlap, the common case. That over-count can falsely declare
    # an otherwise-affordable Pareto run infeasible (or force an unnecessarily
    # small max_k), pushing --emit-commands to --search-modes greedy when
    # Pareto would actually have fit.
    #
    # When the caller supplies the actual field NAMES (candidate_field_names_
    # ranked from compute_domain_stats, plus required_field_names/
    # optional_field_names from the policy JSON), compute the TRUE
    # deduplicated union size instead of the pessimistic arithmetic sum.
    # Falls back to the arithmetic sum (still a safe, if pessimistic, upper
    # bound -- never underestimates cost) when names aren't available, e.g.
    # suggest_params_for_domain's own direct unit tests that only pass counts.
    if candidate_field_names_ranked and (required_field_names or optional_field_names):
        capped_candidate_names = set(
            candidate_field_names_ranked[:max_candidate_fields] if max_candidate_fields > 0 else candidate_field_names_ranked
        )
        harsh_pool_size = len(capped_candidate_names | set(required_field_names) | set(optional_field_names))
    else:
        harsh_pool_size = max_candidate_fields + policy_fixed_count
    if required_count and _cumulative_subset_count(harsh_pool_size, required_count) > subset_budget:
        # Even the minimum k needed to represent the required baseline is
        # unaffordable at the (optional-inflated) harsh pool -- no larger k
        # works either (monotonicity of Sum C(pool, i)).
        harsh_max_k = discover_max_k
        harsh_pareto_feasible = False
    else:
        candidate_k = max(discover_max_k, required_count)
        if _cumulative_subset_count(harsh_pool_size, candidate_k) <= subset_budget:
            harsh_max_k = candidate_k
        elif required_count:
            # candidate_k (anchored at discover_max_k, which was only ever
            # verified against the smaller discover pool) doesn't fit the
            # larger harsh pool, but the true required-only floor -- already
            # confirmed affordable above -- does. Fall back to it rather than
            # an unverified discover_max_k.
            harsh_max_k = required_count
        else:
            # No required baseline to fall back to (required_count == 0) and
            # discover_max_k itself doesn't fit the optional-inflated pool --
            # shrink k from scratch until it does. Optional-only inflation
            # must still be feasible at SOME (possibly small) k, since
            # optional fields never need to co-occur together in one subset
            # -- only the pool got bigger, not the minimum useful k.
            harsh_max_k = 0
            for k in range(1, min(harsh_pool_size, 64) + 1):
                if _cumulative_subset_count(harsh_pool_size, k) <= subset_budget:
                    harsh_max_k = k
                else:
                    break
        harsh_pareto_feasible = harsh_max_k > 0

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
            f"required_items count ({required_count}) is too large for harsh/validate Pareto search "
            f"to represent within --subset-budget ({subset_budget}) at the harsh/validate pool size "
            f"(--max-candidate-fields {max_candidate_fields} + {required_count} required + {optional_count} "
            f"optional = {harsh_pool_size}) -- pareto_search() has no required/optional-fields-aware "
            "enumeration, so max_k can't be bumped to fit the baseline without a combinatorial blowup. "
            "Use --search-modes greedy for harsh/validate on this domain instead (discover_greedy() has "
            "no such ceiling)."
        )
    elif harsh_max_k > discover_max_k:
        notes.append(
            f"required_items count ({required_count}) exceeds the discover-mode budget max_k "
            f"({discover_max_k}); harsh/validate runs need --max-k >= {harsh_max_k} to be able to "
            "represent the current baseline plus new candidates."
        )
    elif optional_count and harsh_max_k < discover_max_k:
        notes.append(
            f"existing optional_items count ({optional_count}) enlarges the harsh/validate candidate "
            f"pool to {harsh_pool_size} (--max-candidate-fields {max_candidate_fields} + {required_count} "
            f"required + {optional_count} optional); harsh/validate --max-k is capped lower than "
            f"discover's ({harsh_max_k} vs {discover_max_k}) to stay within --subset-budget "
            f"({subset_budget}) at that larger pool -- Pareto can still validly explore subsets up to "
            "that size, just not as large as discover mode's."
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
        req = block.get("required_items") or block.get("required_fields") or []
        opt = block.get("optional_items") or []
        req = req if isinstance(req, list) else []
        opt = opt if isinstance(opt, list) else []
        out[str(domain)] = {
            "required_count": len(req),
            "optional_count": len(opt),
            "required_fields": [str(x) for x in req],
            "optional_fields": [str(x) for x in opt],
        }
    return out


def _emit_command(domain: str, suggestion: Dict[str, object], phase0_dir: str, policy_json: Optional[str]) -> List[str]:
    """Build ready-to-run discover_join_policy.py command(s) for one domain.

    Returns two commands, not one, whenever the harsh/validate command needs
    different treatment than discover: either a different max_k (an existing
    required_items baseline the budget-derived discover value can't represent
    -- rare in practice, since suggest_params_for_domain never lets
    suggested_max_k_harsh_validate exceed suggested_max_k_discover unless
    that's verified budget-safe), or harsh_pareto_feasible is False (the
    required baseline is too large for Pareto's blind combinatorial
    enumeration to represent within budget at ANY max_k -- see
    suggest_params_for_domain's docstring/notes). In the latter case only the
    harsh/validate command is forced to --search-modes greedy instead of the
    CLI default greedy,pareto -- discover/validate's own candidate pools stay
    small and cheap regardless, so there's no need to give up Pareto there too.
    A single combined command that left harsh/validate on the CLI's default
    greedy,pareto in either case would silently under-size its search space,
    or hand out a Pareto invocation that can only ever explore subsets too
    small to contain the full required baseline.
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

    # Split whenever harsh/validate needs *different* treatment than discover --
    # either a different max_k, or (even when the k values match, which is now
    # the common case: suggest_params_for_domain never lets harsh_k exceed
    # discover_k unless it's verified budget-safe) because Pareto is infeasible
    # for the required baseline and only the harsh/validate command should be
    # forced to greedy, not discover/validate too (their own candidate pools
    # stay small and cheap regardless).
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
    items = _read_csv(items_path) if items_path.exists() else []

    domains = sorted({r.get("domain", "").strip() for r in records if r.get("domain", "").strip()}, key=str.lower)
    if args.domains:
        allow = {d.strip() for d in str(args.domains).split(",") if d.strip()}
        domains = [d for d in domains if d in allow]

    policy_fields = _load_policy_fields(Path(args.policy_json)) if args.policy_json else {}

    rows: List[Dict[str, object]] = []
    for domain in domains:
        stats = compute_domain_stats(records, items, domain)
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
