#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
from typing import Dict, List, Sequence

try:
    from tools.join_key_discovery.eval import build_identity_index, normalize_policy_block, score_candidate
    from tools.join_key_discovery.greedy import discover_greedy
except ModuleNotFoundError:
    from join_key_discovery.eval import build_identity_index, normalize_policy_block, score_candidate
    from join_key_discovery.greedy import discover_greedy


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [{str(k): "" if v is None else str(v) for k, v in row.items()} for row in csv.DictReader(f)]


def _pareto_search_adapter(domain_records, identity_index, candidate_fields, cfg):
    try:
        try:
            from tools.pareto_joinkey_search import pareto_search
        except ModuleNotFoundError:
            from pareto_joinkey_search import pareto_search
        return pareto_search(domain_records, identity_index, candidate_fields, cfg)
    except ModuleNotFoundError:
        return {"frontier": [], "chosen": None, "error": "pareto_dependency_missing"}


def _diagnostics_domain_suffix(allow: set) -> str:
    """Filename suffix for diagnostics CSVs when a run is scoped to specific --domains.

    Sequential --emit-commands invocations of discover_join_policy.py (one per
    domain or small domain group, as tools/suggest_discovery_params.py's
    --emit-commands prints) all write to the same fixed diagnostics filenames by
    default, so each later run silently clobbers the previous run's output before
    anyone reads it. Suffixing by the scoped domain set keeps per-domain runs'
    diagnostics side by side. Unscoped (whole-corpus) runs keep the original,
    unsuffixed filenames -- this only changes behavior when --domains is passed.
    """
    if not allow:
        return ""
    scoped = sorted(allow, key=str.lower)
    joined = "_".join(scoped)
    if len(joined) <= 60:
        return "__" + joined
    digest = hashlib.sha1("|".join(scoped).encode("utf-8")).hexdigest()[:10]
    return f"__{len(scoped)}domains_{digest}"


def _write_csv(path: Path, fields: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def _rank_all(records: List[Dict[str, str]], seed: int) -> List[Dict[str, str]]:
    """Deterministically sort every record by seeded hash rank (always sorts -- no
    early-return-for-small-inputs shortcut). Shared sort primitive for
    _sample_domain_records and the stratified top-up logic below, both of which need
    a full, order-consistent ranking (not just "return whatever was passed in
    unsorted") to slice remainders correctly.
    """

    def _rank(row: Dict[str, str]) -> str:
        key = row.get("record_pk", "") or row.get("record_id", "") or row.get("file_id", "")
        return hashlib.sha1(f"{seed}|{key}".encode("utf-8")).hexdigest()

    return sorted(records, key=lambda r: (_rank(r), r.get("record_pk", "")))


def _sample_domain_records(records: List[Dict[str, str]], sample_size: int, seed: int) -> List[Dict[str, str]]:
    if sample_size <= 0 or len(records) <= sample_size:
        return records
    return _rank_all(records, seed)[:sample_size]


def _stratified_sample(
    records: List[Dict[str, str]],
    items: List[Dict[str, str]],
    stratify_key: str,
    sample_size: int,
    seed: int,
) -> List[Dict[str, str]]:
    """Sample records giving equal representation to each unique value of stratify_key.

    Guards against a pooled/unweighted sample being dominated by whichever group
    happens to have the most records (e.g. a handful of Template/Container files
    carrying a much larger configured vocabulary than typical Project files) --
    rare-but-real configurations in small groups are exactly the cases most likely
    to expose a candidate join key's collisions/fragmentation, so they need
    guaranteed representation, not proportional-to-population representation.

    ``stratify_key`` is resolved two ways:
      - ``"file_id"`` (or ``"record_id"``): read directly off each record row
        (these live in records.csv, not identity_items.csv).
      - anything else: treated as an identity-item key and looked up per
        record_pk from ``items`` (e.g. ``lft.family_name``), mirroring the
        domain-attribute grouping tools/discover_hash_policy.py already uses.

    Records the stratifier has no value for (e.g. blank file_id) get an
    unconditionally reserved share up front -- not dropped, and not merely
    given equal-but-unguaranteed odds alongside the real groups -- see the
    reserved_ungrouped comment below for why a hard guarantee, not just fair
    treatment, is needed here.

    For the real groups, takes ceil(remaining_sample_size / n_groups) records
    from each using the same deterministic hash-rank as _sample_domain_records,
    with which GROUPS survive an eventual out[:sample_size] cap decided by a
    seeded rank over the group values themselves (see _group_rank below), not
    group/file name. After the first pass, tops up from groups with surplus
    records, so the total always reaches sample_size when enough records
    exist. Falls back to flat sampling when the key has no coverage.
    """
    if not stratify_key or sample_size <= 0 or len(records) <= sample_size:
        return records

    if stratify_key in ("file_id", "record_id"):
        pk_to_val = {
            r.get("record_pk", "").strip(): r.get(stratify_key, "").strip()
            for r in records
            if r.get("record_pk", "").strip() and r.get(stratify_key, "").strip()
        }
    else:
        pk_to_val = {}
        for it in items:
            if it.get("item_key", "").strip() != stratify_key:
                continue
            pk = it.get("record_pk", "").strip()
            val = it.get("item_value", "").strip()
            if pk and val:
                pk_to_val[pk] = val

    if not pk_to_val:
        return _sample_domain_records(records, sample_size, seed)

    groups: Dict[str, List[Dict[str, str]]] = {}
    ungrouped: List[Dict[str, str]] = []
    for r in records:
        pk = r.get("record_pk", "").strip()
        val = pk_to_val.get(pk)
        if val:
            groups.setdefault(val, []).append(r)
        else:
            ungrouped.append(r)

    n_groups = len(groups)
    if n_groups == 0:
        return _sample_domain_records(records, sample_size, seed)

    # Records the stratifier has no value for (e.g. blank file_id, or a
    # partially-populated identity-item key) get an UNCONDITIONAL reserved
    # share -- computed as though "ungrouped" were one more stratum among
    # n_groups+1 -- rather than being folded into the same seeded
    # out[:sample_size] truncation the real groups below are subject to.
    # Treating them as just another group with equal (but not guaranteed) odds
    # would still let them land at zero purely by chance whenever known groups
    # alone already exceed sample_size (the common case when there are more
    # groups than the sample cap: the old top-up-only handling made this a
    # mathematical certainty, not just a chance outcome, since the top-up pass
    # never ran at all in that regime). For records the stratifier has no data
    # on at all, losing that lottery is a worse failure than any single real
    # group losing it: it can silently drop an entire uncharacterized slice of
    # the population rather than one specific known file/value, so this slice
    # gets a hard guarantee instead of just fair-chance treatment.
    reserved_ungrouped: List[Dict[str, str]] = []
    if ungrouped:
        ungrouped_share = max(1, math.ceil(sample_size / (n_groups + 1)))
        reserved_ungrouped = _sample_domain_records(ungrouped, ungrouped_share, seed)

    sample_size = max(0, sample_size - len(reserved_ungrouped))
    if sample_size <= 0 or n_groups == 0:
        return reserved_ungrouped

    # Rank GROUPS themselves by the same deterministic hash approach
    # _sample_domain_records uses for records, not alphabetically: when
    # n_groups > sample_size (the common case for --stratify-by file_id on a
    # corpus with more files than the sample cap), the final out[:sample_size]
    # truncation below keeps only the groups iterated first. Sorting by group
    # name would make that truncation silently keep only the
    # lexicographically-first groups regardless of seed (e.g. always f0000..
    # f0009 out of 1000 single-record files) -- ranking by seed instead makes
    # which groups survive the cap an actual deterministic-but-unbiased
    # (seeded) choice, not an artifact of naming.
    def _group_rank(val: str) -> str:
        return hashlib.sha1(f"{seed}|group|{val}".encode("utf-8")).hexdigest()

    ranked_group_keys = sorted(groups.keys(), key=_group_rank)

    per_group = max(1, math.ceil(sample_size / n_groups))
    first_pass: Dict[str, List[Dict[str, str]]] = {}
    out: List[Dict[str, str]] = []
    for val in ranked_group_keys:
        sampled = _sample_domain_records(groups[val], per_group, seed)
        first_pass[val] = sampled
        out.extend(sampled)

    # Top up to (the reduced) sample_size: groups with more records than
    # per_group contribute their surplus. Uses _rank_all directly rather than
    # _sample_domain_records(groups[val], len(groups[val]), seed): that call's
    # sample_size == len(records) always hits the "cap isn't binding" early
    # return, which yields records in ORIGINAL (unsorted) order -- inconsistent
    # with first_pass[val], which came from the actual seeded ranking. Slicing
    # an unsorted list at len(first_pass[val]) would return an arbitrary,
    # possibly-overlapping remainder instead of "everything not already taken."
    if len(out) < sample_size:
        surplus: List[Dict[str, str]] = []
        for val in ranked_group_keys:
            all_ranked = _rank_all(groups[val], seed)
            surplus.extend(all_ranked[len(first_pass[val]):])
        out.extend(surplus[: sample_size - len(out)])

    # If the known groups still can't fill sample_size on their own (e.g. a
    # large ungrouped slice dwarfing a handful of small known groups -- the
    # groups above simply don't have enough records to give), pull additional
    # records from the remainder of the ungrouped pool beyond what was already
    # reserved. Still deterministic/seeded via _rank_all (same ranking
    # reserved_ungrouped was sliced from, so this picks up exactly where that
    # slice left off, no overlap).
    if len(out) < sample_size and len(ungrouped) > len(reserved_ungrouped):
        ranked_ungrouped = _rank_all(ungrouped, seed)
        remaining_ungrouped = ranked_ungrouped[len(reserved_ungrouped):]
        out.extend(remaining_ungrouped[: sample_size - len(out)])

    return reserved_ungrouped + out[:sample_size]


def _full_population_verify(
    dom_records_all: List[Dict[str, str]],
    identity_index,
    selected: List[str],
    cfg: Dict[str, object],
    metrics_sample: Dict[str, object],
    divergence_delta: float,
    coverage_drop_threshold: float = 0.05,
):
    """Re-score `selected` against the FULL (unsampled) population and flag divergence
    from the sample-based `metrics_sample`.

    This is a single O(len(dom_records_all)) pass via score_candidate() -- not a
    combinatorial search -- so it stays cheap even though the search that produced
    `selected` ran on a sample for tractability. Without this, a candidate that looks
    fragmentation-free on the sample could be pinned as policy despite fragmenting on
    records the sample never saw.

    Divergence is flagged when any of:
      - the full population shows fragmentation (fragmentation_rate > 0) while the
        sample showed none (fragmentation_rate == 0) -- exactly the "sample said 0,
        population disagrees" failure mode this exists to catch;
      - collision_rate on the full population exceeds the sample's by more than
        `divergence_delta` (absolute);
      - coverage on the full population drops from the sample's by more than
        `coverage_drop_threshold` (absolute) -- a candidate selected because it
        happened to cover every sampled record but is largely absent from the rest
        of the population is not globally applicable (Phase-2's own "Global
        Consistency" principle, docs/phase_2_join-key_discovery.md), even though
        collision/fragmentation alone wouldn't catch it: those metrics are only
        computed over *covered* records, so a coverage collapse can leave both
        unchanged while most of the population silently gets no join key at all.

    Returns (metrics_full, diverges).
    """
    metrics_full = score_candidate(dom_records_all, identity_index, selected, cfg)
    collision_delta = float(metrics_full.get("collision_rate", 1.0)) - float(metrics_sample.get("collision_rate", 1.0))
    frag_sample = float(metrics_sample.get("fragmentation_rate", 1.0))
    frag_full = float(metrics_full.get("fragmentation_rate", 1.0))
    coverage_delta = float(metrics_sample.get("coverage", 0.0)) - float(metrics_full.get("coverage", 0.0))
    diverges = (
        (frag_full > 0.0 and frag_sample == 0.0)
        or (collision_delta > divergence_delta)
        or (coverage_delta > coverage_drop_threshold)
    )
    return metrics_full, diverges


def _pick_candidate_fields(items: List[Dict[str, str]], max_fields: int) -> List[str]:
    counts: Dict[str, int] = {}
    for it in items:
        k = it.get("item_key", "").strip()
        if not k:
            continue
        counts[k] = counts.get(k, 0) + 1
    fields = sorted(counts.keys(), key=lambda k: (-counts[k], k.lower()))
    if max_fields > 0 and len(fields) > max_fields:
        return fields[:max_fields]
    return fields


def _dedupe(items: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        key = str(item).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _without_excluded(items: Sequence[str], excluded: Sequence[str]) -> List[str]:
    excluded_lc = {str(x).strip().lower() for x in excluded if str(x).strip()}
    if not excluded_lc:
        return _dedupe(items)
    return _dedupe([x for x in items if str(x).strip().lower() not in excluded_lc])


def _to_legacy_shape_gating(gates: Dict[str, object]) -> Dict[str, object]:
    if not isinstance(gates, dict) or not gates:
        return {}
    return {
        "discriminator_key": gates.get("discriminator_key"),
        "shape_requirements": gates.get("shape_requirements") if isinstance(gates.get("shape_requirements"), dict) else {},
        "default_shape_behavior": gates.get("default_shape_behavior") or "common_only",
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Discover exploration stage (T1): emit discover/validate/harsh CSVs for PowerBI join-key review.")
    ap.add_argument("--phase0-dir", default="results/records", help="Flatten output directory (default: results/records).")
    ap.add_argument("--out-policy", default=None, help="Optional output policy JSON path. If omitted, no policy JSON is written.")
    ap.add_argument("--policy-json", default=None, help="Current official policy JSON used for validate/harsh constraints.")
    ap.add_argument("--domains", default=None)
    ap.add_argument("--search-modes", default="greedy,pareto", help="Comma-separated discovery engines: greedy,pareto")
    ap.add_argument("--policy-modes", default="discover,validate,harsh", help="Comma-separated policy strictness modes: discover,validate,harsh")
    ap.add_argument("--sample-size", type=int, default=5000)
    ap.add_argument("--sample-seed", type=int, default=17)
    ap.add_argument(
        "--stratify-by",
        default="",
        help=(
            "Field to stratify sampling by so each unique value gets equal representation "
            "regardless of group size, instead of a pooled/unweighted sample that a few "
            "high-volume groups (e.g. Template/Container files with a much larger configured "
            "vocabulary than typical Project files) can dominate. Use 'file_id' for per-file "
            "balance, or a populated identity-item key (e.g. 'lft.family_name') for a "
            "domain-attribute grouping. Falls back to flat sampling when the key has no coverage."
        ),
    )
    ap.add_argument("--max-candidate-fields", type=int, default=64)
    ap.add_argument("--max-k", type=int, default=4, help="Max subset size for Pareto search (validate mode auto-bumps to required count).")
    ap.add_argument("--base-policy", default=None, help="Optional policy to preserve metadata/shape gates when writing out-policy.")
    ap.add_argument("--warn-only", action="store_true")
    ap.add_argument(
        "--no-full-verify",
        action="store_true",
        help=(
            "Skip re-scoring each selected candidate against the FULL (unsampled) domain "
            "population. By default, every selected candidate is re-scored against the full "
            "population (coverage_full/collision_rate_full/fragmentation_rate_full columns) "
            "so a sample-only 'fragmentation=0' finding can't be pinned as policy without ever "
            "being checked against the real corpus -- this re-score is a single O(records) pass "
            "per row, not a combinatorial search, so it's cheap even when the search itself was "
            "sampled for tractability."
        ),
    )
    ap.add_argument(
        "--divergence-collision-delta",
        type=float,
        default=0.01,
        help="Absolute collision_rate_full - collision_rate threshold above which a [discover] WARNING is printed for that row (default 0.01).",
    )
    ap.add_argument(
        "--coverage-drop-threshold",
        type=float,
        default=0.05,
        help=(
            "Absolute coverage - coverage_full drop threshold above which a [discover] WARNING is "
            "printed (default 0.05). Catches a candidate that happened to cover every sampled record "
            "but is largely absent from the rest of the population -- collision_rate/fragmentation_rate "
            "alone won't catch this since both are only computed over covered records."
        ),
    )
    args = ap.parse_args()

    phase0_dir = Path(args.phase0_dir)
    records_path = phase0_dir / "records.csv"
    if not records_path.exists():
        legacy_records_path = phase0_dir / "phase0_records.csv"
        if legacy_records_path.exists():
            records_path = legacy_records_path
    records = _read_csv(records_path)
    items_path = phase0_dir / "identity_items.csv"
    if not items_path.exists():
        legacy_items_path = phase0_dir / "phase0_identity_items.csv"
        if legacy_items_path.exists():
            items_path = legacy_items_path
    items = _read_csv(items_path)

    domains = sorted({r.get("domain", "").strip() for r in records if r.get("domain", "").strip()}, key=str.lower)
    domain_suffix = ""
    if args.domains:
        allow = {d.strip() for d in str(args.domains).split(",") if d.strip()}
        domains = [d for d in domains if d in allow]
        domain_suffix = _diagnostics_domain_suffix(allow)

    search_modes = [m.strip() for m in str(args.search_modes).split(",") if m.strip()]
    policy_modes = [m.strip() for m in str(args.policy_modes).split(",") if m.strip()]

    source_policy = Path(args.policy_json) if args.policy_json else None
    base_policy = Path(args.base_policy) if args.base_policy else None
    policy_source = source_policy if source_policy and source_policy.exists() else base_policy
    base_domains: Dict[str, Dict[str, object]] = {}
    if policy_source and policy_source.exists():
        loaded = json.loads(policy_source.read_text(encoding="utf-8"))
        cand = loaded.get("domains") if isinstance(loaded, dict) else {}
        if isinstance(cand, dict):
            base_domains = {str(k): v for k, v in cand.items() if isinstance(v, dict)}

    policies = {"policy_version": "v21.1", "domains": {}}
    report_rows: List[Dict[str, str]] = []
    failures: List[str] = []

    print(f"[discover] loaded records={len(records)} identity_items={len(items)} domains={len(domains)} policy_modes={policy_modes} search_modes={search_modes}", flush=True)

    stratify_key = str(args.stratify_by or "").strip()
    full_verify = not bool(args.no_full_verify)
    divergence_delta = float(args.divergence_collision_delta)
    coverage_drop_threshold = float(args.coverage_drop_threshold)

    for i, domain in enumerate(domains, start=1):
        dom_records_all = [r for r in records if r.get("domain") == domain]
        dom_items_all = [it for it in items if it.get("domain") == domain]
        if stratify_key:
            dom_records = _stratified_sample(dom_records_all, dom_items_all, stratify_key, int(args.sample_size), int(args.sample_seed))
        else:
            dom_records = _sample_domain_records(dom_records_all, int(args.sample_size), int(args.sample_seed))
        records_total_domain = len(dom_records_all)
        records_sampled_domain = len(dom_records)
        sample_rate = (float(records_sampled_domain) / float(records_total_domain)) if records_total_domain else 0.0
        sampled_pks = {r.get("record_pk", "").strip() for r in dom_records if r.get("record_pk", "").strip()}
        dom_items_sampled = [it for it in dom_items_all if not sampled_pks or it.get("record_pk", "").strip() in sampled_pks]
        candidate_fields = _pick_candidate_fields(dom_items_sampled, int(args.max_candidate_fields))
        if not candidate_fields:
            failures.append(domain)
            continue

        # Built from the FULL (unsampled) item set, not just the sampled subset: a
        # record_pk absent from the sample is simply never looked up when scoring
        # against dom_records (the sample), so this is strictly more complete for
        # the sampled pass too, and is what the full-population verification pass
        # below needs (dom_records_all references record_pks outside the sample).
        identity_index = build_identity_index(dom_items_all)
        existing = base_domains.get(domain, {}) if isinstance(base_domains.get(domain, {}), dict) else {}
        normalized = normalize_policy_block(existing)
        req = normalized["required_fields"]
        opt = normalized["optional_items"]
        excluded = set(normalized["explicitly_excluded_items"])
        gates = normalized["gates"]
        scoped_candidates = _without_excluded(candidate_fields, excluded)

        # Required fields that never appear anywhere in this domain's populated
        # identity items at all -- i.e. genuinely absent from the data, not just
        # unresolved for this particular candidate. Computed once per domain,
        # independent of what greedy/Pareto end up selecting: since
        # discover_greedy() seeds its selection with cfg.gates.required_fields
        # regardless of whether those fields are populated anywhere (see
        # tools/join_key_discovery/greedy.py), and Pareto's own validate-mode
        # fallback does the same (`selected = list(req)` unconditionally when
        # the frontier comes back empty), the returned selected_fields NAMES
        # always trivially satisfy issubset(req) now -- checking selected alone
        # can no longer detect "required field doesn't exist in the data" the
        # way it used to by coincidence (candidate_fields only ever contained
        # fields with at least one populated occurrence).
        #
        # Checked against the FULL population's item keys (dom_items_all), not
        # candidate_fields: candidate_fields is _pick_candidate_fields()'s
        # output over the SAMPLED item set, capped at --max-candidate-fields. A
        # required field populated only on an unsampled record, or simply
        # ranked below the candidate-field cap (a real field present in the
        # data, just not among the top --max-candidate-fields by frequency),
        # would otherwise be wrongly reported as absent from the data entirely.
        all_item_keys_domain = {it.get("item_key", "").strip() for it in dom_items_all if it.get("item_key", "").strip()}
        req_missing_from_data = set(req) - all_item_keys_domain

        for policy_mode in policy_modes:
            if policy_mode == "validate":
                work_candidates = _without_excluded(req + opt, excluded)
            elif policy_mode == "harsh":
                work_candidates = _without_excluded(req + opt + scoped_candidates, excluded)
            else:
                work_candidates = list(scoped_candidates)

            if not work_candidates:
                report_rows.append({
                    "domain": domain,
                    "policy_mode": policy_mode,
                    "search_mode": "n/a",
                    "status": "no_candidates",
                    "reason": "",
                    "records_total_domain": str(records_total_domain),
                    "records_sampled_domain": str(records_sampled_domain),
                    "sample_rate": f"{sample_rate:.6f}",
                    "sample_size_arg": str(args.sample_size),
                    "sample_seed_arg": str(args.sample_seed),
                    "max_candidate_fields_arg": str(args.max_candidate_fields),
                    "max_k_effective": "",
                    "candidate_fields_raw": "|".join(candidate_fields),
                    "candidate_fields_raw_count": str(len(candidate_fields)),
                    "scoped_candidates": "|".join(scoped_candidates),
                    "scoped_candidates_count": str(len(scoped_candidates)),
                    "work_candidates": "|".join(work_candidates),
                    "work_candidates_count": str(len(work_candidates)),
                    "selected_fields": "",
                    "coverage": "0",
                    "collision_rate": "1",
                    "fragmentation_rate": "1",
                    "records_total": "0",
                    "records_covered": "0",
                    "collision_records": "0",
                    "fragmented_sig_count": "0",
                    "join_group_count": "0",
                    "hhi": "0.000000",
                    "effective_cluster_count": "0.000000",
                    "failures_json": "{}",
                    "frontier_size": "0",
                    "fallback_used": "false",
                    "required_count": str(len(req)),
                    "required_fields": "|".join(req),
                    "optional_count": str(len(opt)),
                    "optional_items": "|".join(opt),
                    "excluded_count": str(len(excluded)),
                    "excluded_items": "|".join(sorted(excluded)),
                    "stratify_by": stratify_key,
                    "coverage_full": "0",
                    "collision_rate_full": "1",
                    "fragmentation_rate_full": "1",
                    "records_total_full": str(records_total_domain),
                    "records_covered_full": "0",
                    "collision_records_full": "0",
                    "fragmented_sig_count_full": "0",
                    "join_group_count_full": "0",
                    "hhi_full": "0.000000",
                    "effective_cluster_count_full": "0.000000",
                    "full_verify_status": "skipped_no_selection",
                    "sample_vs_full_diverges": "false",
                })
                continue

            max_k = int(args.max_k)
            if policy_mode == "validate" and req:
                max_k = max(max_k, len(req))
            cfg = {"max_k": max_k, "gates": {"required_fields": req, **gates}}
            for search_mode in search_modes:
                status = "ok"
                selected: List[str] = []
                metrics: Dict[str, object] = {}
                reason = ""
                frontier_size = 0
                fallback_used = False
                if search_mode == "pareto":
                    p = _pareto_search_adapter(dom_records, identity_index, work_candidates, cfg)
                    frontier = p.get("frontier") if isinstance(p.get("frontier"), list) else []
                    if policy_mode == "validate" and req:
                        frontier = [row for row in frontier if set(req).issubset(set(str(row.get("keys", "")).split("|")))]
                    frontier_size = len(frontier)
                    if frontier:
                        chosen = sorted(frontier, key=lambda x: (x.get("collision_rate", 1.0), x.get("coverage_gap", 1.0), x.get("k_count", 99), x.get("keys", "")))[0]
                        selected = [x for x in str(chosen.get("keys", "")).split("|") if x]
                        metrics = chosen.get("metrics", {}) if isinstance(chosen.get("metrics"), dict) else {}
                    elif policy_mode == "validate" and req:
                        selected = list(req)
                        metrics = score_candidate(dom_records, identity_index, selected, cfg)
                        reason = "required_set_fallback"
                        fallback_used = True
                    else:
                        status = "blocked"
                        reason = "no_frontier"
                else:
                    g = discover_greedy(dom_records, identity_index, work_candidates, cfg)
                    selected = [str(x) for x in g.get("selected_fields", []) if str(x).strip()]
                    metrics = g.get("metrics", {}) if isinstance(g.get("metrics"), dict) else {}

                if policy_mode == "validate" and req and (not set(req).issubset(set(selected)) or req_missing_from_data):
                    status = "blocked_missing_required"
                    if req_missing_from_data:
                        reason = "required_fields_absent_from_data:" + ",".join(sorted(req_missing_from_data))

                # Full-population verification: re-score whatever was selected against
                # dom_records_all (not the sample) with the same cfg/gates. This is a
                # single O(records_total_domain) pass, not a combinatorial search, so it
                # stays cheap even though the search itself ran on a sample for
                # tractability. Without this, a sample-only "fragmentation=0" could be
                # pinned as policy despite fragmenting on records the sample never saw.
                full_verify_status = "skipped_no_full_verify_flag"
                metrics_full: Dict[str, object] = {}
                diverges = False
                if selected and full_verify:
                    metrics_full, diverges = _full_population_verify(
                        dom_records_all, identity_index, selected, cfg, metrics, divergence_delta,
                        coverage_drop_threshold=coverage_drop_threshold,
                    )
                    full_verify_status = "ok"
                    if diverges:
                        print(
                            f"[discover] WARNING domain={domain} policy_mode={policy_mode} search_mode={search_mode} "
                            f"sample-based metrics diverge from full population: "
                            f"fragmentation_rate sample={float(metrics.get('fragmentation_rate', 1.0)):.6f} full={float(metrics_full.get('fragmentation_rate', 1.0)):.6f}, "
                            f"collision_rate sample={float(metrics.get('collision_rate', 1.0)):.6f} full={float(metrics_full.get('collision_rate', 1.0)):.6f}, "
                            f"coverage sample={float(metrics.get('coverage', 0.0)):.6f} full={float(metrics_full.get('coverage', 0.0)):.6f} "
                            f"-- do not pin this candidate without review.",
                            flush=True,
                        )
                elif not selected:
                    full_verify_status = "skipped_no_selection"

                report_rows.append({
                    "domain": domain,
                    "policy_mode": policy_mode,
                    "search_mode": search_mode,
                    "status": status,
                    "reason": reason,
                    "records_total_domain": str(records_total_domain),
                    "records_sampled_domain": str(records_sampled_domain),
                    "sample_rate": f"{sample_rate:.6f}",
                    "sample_size_arg": str(args.sample_size),
                    "sample_seed_arg": str(args.sample_seed),
                    "max_candidate_fields_arg": str(args.max_candidate_fields),
                    "max_k_effective": str(max_k),
                    "candidate_fields_raw": "|".join(candidate_fields),
                    "candidate_fields_raw_count": str(len(candidate_fields)),
                    "scoped_candidates": "|".join(scoped_candidates),
                    "scoped_candidates_count": str(len(scoped_candidates)),
                    "work_candidates": "|".join(work_candidates),
                    "work_candidates_count": str(len(work_candidates)),
                    "selected_fields": "|".join(selected),
                    "coverage": f"{float(metrics.get('coverage', 0.0)):.6f}",
                    "collision_rate": f"{float(metrics.get('collision_rate', 1.0)):.6f}",
                    "fragmentation_rate": f"{float(metrics.get('fragmentation_rate', 1.0)):.6f}",
                    "records_total": str(int(metrics.get("records_total", 0) or 0)),
                    "records_covered": str(int(metrics.get("records_covered", 0) or 0)),
                    "collision_records": str(int(metrics.get("collision_records", 0) or 0)),
                    "fragmented_sig_count": str(int(metrics.get("fragmented_sig_count", 0) or 0)),
                    "join_group_count": str(int(metrics.get("join_group_count", 0) or 0)),
                    "hhi": f"{float(metrics.get('hhi', 0.0)):.6f}",
                    "effective_cluster_count": f"{float(metrics.get('effective_cluster_count', 0.0)):.6f}",
                    "failures_json": json.dumps(metrics.get("failures", {}) if isinstance(metrics.get("failures"), dict) else {}, sort_keys=True),
                    "frontier_size": str(frontier_size),
                    "fallback_used": "true" if fallback_used else "false",
                    "required_count": str(len(req)),
                    "required_fields": "|".join(req),
                    "optional_count": str(len(opt)),
                    "optional_items": "|".join(opt),
                    "excluded_count": str(len(excluded)),
                    "excluded_items": "|".join(sorted(excluded)),
                    "stratify_by": stratify_key,
                    "coverage_full": f"{float(metrics_full.get('coverage', 0.0)):.6f}",
                    "collision_rate_full": f"{float(metrics_full.get('collision_rate', 1.0)):.6f}",
                    "fragmentation_rate_full": f"{float(metrics_full.get('fragmentation_rate', 1.0)):.6f}",
                    "records_total_full": str(int(metrics_full.get("records_total", records_total_domain)) if metrics_full else records_total_domain),
                    "records_covered_full": str(int(metrics_full.get("records_covered", 0) or 0)),
                    "collision_records_full": str(int(metrics_full.get("collision_records", 0) or 0)),
                    "fragmented_sig_count_full": str(int(metrics_full.get("fragmented_sig_count", 0) or 0)),
                    "join_group_count_full": str(int(metrics_full.get("join_group_count", 0) or 0)),
                    "hhi_full": f"{float(metrics_full.get('hhi', 0.0)):.6f}",
                    "effective_cluster_count_full": f"{float(metrics_full.get('effective_cluster_count', 0.0)):.6f}",
                    "full_verify_status": full_verify_status,
                    "sample_vs_full_diverges": "true" if diverges else "false",
                })

        # optional compatibility policy JSON generation
        row_for_policy = next((r for r in report_rows if r.get("domain") == domain and r.get("policy_mode") == "validate" and r.get("search_mode") == "pareto" and r.get("status") == "ok"), None)
        if row_for_policy is None:
            row_for_policy = next((r for r in report_rows if r.get("domain") == domain and r.get("policy_mode") == "discover" and r.get("search_mode") == "greedy" and r.get("status") == "ok"), None)
        sel_for_policy = [x for x in (row_for_policy.get("selected_fields", "") if row_for_policy else "").split("|") if x]
        if sel_for_policy:
            policy_row = {
                "policy_id": f"{domain}.join_key.v21",
                "policy_version": "1",
                "selected_fields": sel_for_policy,
                "required_fields": sel_for_policy,
                "required_items": sel_for_policy,
                "optional_items": opt,
                "explicitly_excluded_items": sorted(excluded),
                "gates": gates,
                "method_used": "explore",
                "join_key_schema": str(existing.get("join_key_schema") or f"{domain}.join_key.v21"),
                "hash_alg": str(existing.get("hash_alg") or "md5_utf8_join_pipe"),
                "diagnostics": {
                    "records_total_domain": records_total_domain,
                    "records_sampled_domain": records_sampled_domain,
                    "sample_rate": round(sample_rate, 6),
                    "sample_size_arg": int(args.sample_size),
                    "sample_seed_arg": int(args.sample_seed),
                    "max_candidate_fields_arg": int(args.max_candidate_fields),
                    "stratify_by": stratify_key,
                    "full_verification": {
                        "status": row_for_policy.get("full_verify_status", "") if row_for_policy else "",
                        "coverage_full": row_for_policy.get("coverage_full", "") if row_for_policy else "",
                        "collision_rate_full": row_for_policy.get("collision_rate_full", "") if row_for_policy else "",
                        "fragmentation_rate_full": row_for_policy.get("fragmentation_rate_full", "") if row_for_policy else "",
                        "sample_vs_full_diverges": row_for_policy.get("sample_vs_full_diverges", "") if row_for_policy else "",
                    },
                },
            }
            legacy_shape_gating = _to_legacy_shape_gating(gates)
            if legacy_shape_gating:
                policy_row["shape_gating"] = legacy_shape_gating
            if isinstance(existing.get("notes"), list):
                policy_row["notes"] = existing.get("notes")
            policies["domains"][domain] = policy_row

        print(f"[discover] [{i}/{len(domains)}] domain={domain} explored", flush=True)

    diagnostics_dir = phase0_dir.parent / "diagnostics"
    fields = [
        "domain", "policy_mode", "search_mode", "status", "reason",
        "records_total_domain", "records_sampled_domain", "sample_rate", "sample_size_arg", "sample_seed_arg", "max_candidate_fields_arg", "max_k_effective",
        "candidate_fields_raw", "candidate_fields_raw_count", "scoped_candidates", "scoped_candidates_count", "work_candidates", "work_candidates_count",
        "selected_fields", "coverage", "collision_rate", "fragmentation_rate",
        "records_total", "records_covered", "collision_records", "fragmented_sig_count", "join_group_count", "hhi", "effective_cluster_count", "failures_json", "frontier_size", "fallback_used",
        "required_count", "required_fields", "optional_count", "optional_items", "excluded_count", "excluded_items",
        "stratify_by",
        "coverage_full", "collision_rate_full", "fragmentation_rate_full", "records_total_full", "records_covered_full",
        "collision_records_full", "fragmented_sig_count_full", "join_group_count_full", "hhi_full", "effective_cluster_count_full",
        "full_verify_status", "sample_vs_full_diverges",
    ]
    _write_csv(diagnostics_dir / f"join_key_discovery_exploration{domain_suffix}.csv", fields, sorted(report_rows, key=lambda r: (r.get("domain", ""), r.get("policy_mode", ""), r.get("search_mode", ""))))
    for mode in policy_modes:
        _write_csv(diagnostics_dir / f"join_key_{mode}{domain_suffix}.csv", fields, [r for r in sorted(report_rows, key=lambda r: (r.get("domain", ""), r.get("search_mode", ""))) if r.get("policy_mode") == mode])
        for search_mode in search_modes:
            _write_csv(
                diagnostics_dir / f"join_key_{mode}_{search_mode}{domain_suffix}.csv",
                fields,
                [
                    r
                    for r in sorted(report_rows, key=lambda r: (r.get("domain", ""), r.get("search_mode", "")))
                    if r.get("policy_mode") == mode and r.get("search_mode") == search_mode
                ],
            )

    if args.out_policy:
        out_policy = Path(args.out_policy)
        out_policy.parent.mkdir(parents=True, exist_ok=True)
        out_policy.write_text(json.dumps(policies, indent=2, sort_keys=True), encoding="utf-8")
        print(f"[discover] wrote compatibility policy JSON: {out_policy}", flush=True)
    else:
        print("[discover] policy JSON emission disabled (use diagnostics CSVs for PowerBI exploration)", flush=True)

    if failures:
        print(f"[discover] domains without candidates: {','.join(sorted(failures))}", flush=True)
    if failures and not args.warn_only:
        raise SystemExit(f"Failed to discover policies for domains: {','.join(sorted(failures))}")


if __name__ == "__main__":
    main()
