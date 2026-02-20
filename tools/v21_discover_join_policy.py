#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
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


def _write_csv(path: Path, fields: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def _sample_domain_records(records: List[Dict[str, str]], sample_size: int, seed: int) -> List[Dict[str, str]]:
    if sample_size <= 0 or len(records) <= sample_size:
        return records

    def _rank(row: Dict[str, str]) -> str:
        key = row.get("record_pk", "") or row.get("record_id", "") or row.get("file_id", "")
        return hashlib.sha1(f"{seed}|{key}".encode("utf-8")).hexdigest()

    ranked = sorted(records, key=lambda r: (_rank(r), r.get("record_pk", "")))
    return ranked[:sample_size]


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
    ap.add_argument("--phase0-dir", default="Results_v21/phase0_v21", help="Flatten output directory (default: Results_v21/phase0_v21).")
    ap.add_argument("--out-policy", default=None, help="Optional output policy JSON path. If omitted, no policy JSON is written.")
    ap.add_argument("--policy-json", default=None, help="Current official policy JSON used for validate/harsh constraints.")
    ap.add_argument("--domains", default=None)
    ap.add_argument("--search-modes", default="greedy,pareto", help="Comma-separated discovery engines: greedy,pareto")
    ap.add_argument("--policy-modes", default="discover,validate,harsh", help="Comma-separated policy strictness modes: discover,validate,harsh")
    ap.add_argument("--sample-size", type=int, default=5000)
    ap.add_argument("--sample-seed", type=int, default=17)
    ap.add_argument("--max-candidate-fields", type=int, default=64)
    ap.add_argument("--max-k", type=int, default=4, help="Max subset size for Pareto search (validate mode auto-bumps to required count).")
    ap.add_argument("--base-policy", default=None, help="Optional policy to preserve metadata/shape gates when writing out-policy.")
    ap.add_argument("--warn-only", action="store_true")
    args = ap.parse_args()

    phase0_dir = Path(args.phase0_dir)
    records = _read_csv(phase0_dir / "phase0_records.csv")
    items = _read_csv(phase0_dir / "phase0_identity_items.csv")

    domains = sorted({r.get("domain", "").strip() for r in records if r.get("domain", "").strip()}, key=str.lower)
    if args.domains:
        allow = {d.strip() for d in str(args.domains).split(",") if d.strip()}
        domains = [d for d in domains if d in allow]

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

    for i, domain in enumerate(domains, start=1):
        dom_records_all = [r for r in records if r.get("domain") == domain]
        dom_records = _sample_domain_records(dom_records_all, int(args.sample_size), int(args.sample_seed))
        sampled_pks = {r.get("record_pk", "").strip() for r in dom_records if r.get("record_pk", "").strip()}
        dom_items = [it for it in items if it.get("domain") == domain and (not sampled_pks or it.get("record_pk", "").strip() in sampled_pks)]
        candidate_fields = _pick_candidate_fields(dom_items, int(args.max_candidate_fields))
        if not candidate_fields:
            failures.append(domain)
            continue

        identity_index = build_identity_index(dom_items)
        existing = base_domains.get(domain, {}) if isinstance(base_domains.get(domain, {}), dict) else {}
        normalized = normalize_policy_block(existing)
        req = normalized["required_fields"]
        opt = normalized["optional_items"]
        excluded = set(normalized["explicitly_excluded_items"])
        gates = normalized["gates"]
        scoped_candidates = [f for f in candidate_fields if f not in excluded]

        for policy_mode in policy_modes:
            if policy_mode == "validate":
                work_candidates = _dedupe(req + opt)
            elif policy_mode == "harsh":
                work_candidates = _dedupe(req + opt + scoped_candidates)
            else:
                work_candidates = list(scoped_candidates)

            if not work_candidates:
                report_rows.append({"domain": domain, "policy_mode": policy_mode, "search_mode": "n/a", "status": "no_candidates", "selected_fields": "", "coverage": "0", "collision_rate": "1", "fragmentation_rate": "1", "required_fields": "|".join(req), "optional_items": "|".join(opt), "excluded_items": "|".join(sorted(excluded))})
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
                if search_mode == "pareto":
                    p = _pareto_search_adapter(dom_records, identity_index, work_candidates, cfg)
                    frontier = p.get("frontier") if isinstance(p.get("frontier"), list) else []
                    if policy_mode == "validate" and req:
                        frontier = [row for row in frontier if set(req).issubset(set(str(row.get("keys", "")).split("|")))]
                    if frontier:
                        chosen = sorted(frontier, key=lambda x: (x.get("collision_rate", 1.0), x.get("coverage_gap", 1.0), x.get("k_count", 99), x.get("keys", "")))[0]
                        selected = [x for x in str(chosen.get("keys", "")).split("|") if x]
                        metrics = chosen.get("metrics", {}) if isinstance(chosen.get("metrics"), dict) else {}
                    elif policy_mode == "validate" and req:
                        selected = list(req)
                        metrics = score_candidate(dom_records, identity_index, selected, cfg)
                        reason = "required_set_fallback"
                    else:
                        status = "blocked"
                        reason = "no_frontier"
                else:
                    g = discover_greedy(dom_records, identity_index, work_candidates, cfg)
                    selected = [str(x) for x in g.get("selected_fields", []) if str(x).strip()]
                    metrics = g.get("metrics", {}) if isinstance(g.get("metrics"), dict) else {}

                if policy_mode == "validate" and req and not set(req).issubset(set(selected)):
                    status = "blocked_missing_required"

                report_rows.append({
                    "domain": domain,
                    "policy_mode": policy_mode,
                    "search_mode": search_mode,
                    "status": status,
                    "reason": reason,
                    "selected_fields": "|".join(selected),
                    "coverage": f"{float(metrics.get('coverage', 0.0)):.6f}",
                    "collision_rate": f"{float(metrics.get('collision_rate', 1.0)):.6f}",
                    "fragmentation_rate": f"{float(metrics.get('fragmentation_rate', 1.0)):.6f}",
                    "required_fields": "|".join(req),
                    "optional_items": "|".join(opt),
                    "excluded_items": "|".join(sorted(excluded)),
                })

        # optional compatibility policy JSON generation
        sel_for_policy = [x for x in (next((r.get("selected_fields", "") for r in report_rows if r.get("domain") == domain and r.get("policy_mode") == "validate" and r.get("search_mode") == "pareto" and r.get("status") == "ok"), "") or "").split("|") if x]
        if not sel_for_policy:
            sel_for_policy = [x for x in (next((r.get("selected_fields", "") for r in report_rows if r.get("domain") == domain and r.get("policy_mode") == "discover" and r.get("search_mode") == "greedy" and r.get("status") == "ok"), "") or "").split("|") if x]
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
            }
            legacy_shape_gating = _to_legacy_shape_gating(gates)
            if legacy_shape_gating:
                policy_row["shape_gating"] = legacy_shape_gating
            if isinstance(existing.get("notes"), list):
                policy_row["notes"] = existing.get("notes")
            policies["domains"][domain] = policy_row

        print(f"[discover] [{i}/{len(domains)}] domain={domain} explored", flush=True)

    diagnostics_dir = phase0_dir.parent / "diagnostics"
    fields = ["domain", "policy_mode", "search_mode", "status", "reason", "selected_fields", "coverage", "collision_rate", "fragmentation_rate", "required_fields", "optional_items", "excluded_items"]
    _write_csv(diagnostics_dir / "join_key_discovery_exploration.csv", fields, sorted(report_rows, key=lambda r: (r.get("domain", ""), r.get("policy_mode", ""), r.get("search_mode", ""))))
    for mode in policy_modes:
        _write_csv(diagnostics_dir / f"join_key_{mode}.csv", fields, [r for r in sorted(report_rows, key=lambda r: (r.get("domain", ""), r.get("search_mode", ""))) if r.get("policy_mode") == mode])

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
