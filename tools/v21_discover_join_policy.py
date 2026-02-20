#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List

try:
    from tools.join_key_discovery.eval import build_identity_index
    from tools.join_key_discovery.greedy import discover_greedy
except ModuleNotFoundError:
    from join_key_discovery.eval import build_identity_index
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


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Discover stage (T1): derive per-domain candidate join-key policy from flatten (T0) identity components."
        )
    )
    ap.add_argument("--phase0-dir", default="Results_v21/phase0_v21", help="Flatten output directory (default: Results_v21/phase0_v21).")
    ap.add_argument("--out-policy", default="Results_v21/policies/domain_join_key_policies.v21.json", help="Output policy JSON path.")
    ap.add_argument("--domains", default=None)
    ap.add_argument("--mode", choices=("auto", "greedy", "pareto"), default="auto")
    ap.add_argument("--warn-only", action="store_true")
    args = ap.parse_args()

    phase0_dir = Path(args.phase0_dir)
    records = _read_csv(phase0_dir / "phase0_records.csv")
    items = _read_csv(phase0_dir / "phase0_identity_items.csv")
    identity_index = build_identity_index(items)

    domains = sorted({r.get("domain", "").strip() for r in records if r.get("domain", "").strip()}, key=str.lower)
    if args.domains:
        allow = {d.strip() for d in str(args.domains).split(",") if d.strip()}
        domains = [d for d in domains if d in allow]

    policies = {"policy_version": "v21.1", "domains": {}}
    report_rows: List[Dict[str, str]] = []
    failures: List[str] = []

    for domain in domains:
        dom_records = [r for r in records if r.get("domain") == domain]
        candidate_fields = sorted({it.get("item_key", "").strip() for it in items if it.get("domain") == domain and it.get("item_key", "").strip()}, key=str.lower)
        if not candidate_fields:
            failures.append(domain)
            report_rows.append({"domain": domain, "method_used": "none", "selected_fields": "", "coverage": "0", "collision_rate": "1", "needs_pareto_reason": "no_candidate_fields", "top_alternates": ""})
            continue

        greedy = discover_greedy(dom_records, identity_index, candidate_fields, {"max_k": 4})
        chosen = greedy
        method = "greedy"
        frontier = []
        if args.mode == "pareto" or (args.mode == "auto" and greedy.get("needs_pareto")):
            p = _pareto_search_adapter(dom_records, identity_index, candidate_fields, {"max_k": 4})
            if p.get("chosen"):
                method = "pareto"
                chosen = {
                    "selected_fields": str(p["chosen"]["keys"]).split("|") if p["chosen"].get("keys") else [],
                    "metrics": p["chosen"].get("metrics", {}),
                    "needs_pareto_reasons": greedy.get("needs_pareto_reasons", []),
                }
                frontier = p.get("frontier", [])
            elif p.get("error"):
                greedy.setdefault("needs_pareto_reasons", []).append(str(p.get("error")))

        sel = chosen.get("selected_fields", [])
        if not sel:
            failures.append(domain)
            continue
        policy_id = f"{domain}.join_key.v21"
        policies["domains"][domain] = {
            "policy_id": policy_id,
            "policy_version": "1",
            "selected_fields": sel,
            "required_fields": sel,
            "gates": {},
            "method_used": method,
        }
        metrics = chosen.get("metrics", {})
        alts = [x.get("keys") or "|".join(x.get("selected_fields", [])) for x in frontier[:3]]
        if not alts:
            alts = ["|".join(x.get("selected_fields", [])) for x in greedy.get("top_contenders", [])[:3]]
        report_rows.append({
            "domain": domain,
            "method_used": method,
            "selected_fields": "|".join(sel),
            "coverage": f"{float(metrics.get('coverage', 0.0)):.6f}",
            "collision_rate": f"{float(metrics.get('collision_rate', 1.0)):.6f}",
            "needs_pareto_reason": "|".join(greedy.get("needs_pareto_reasons", [])),
            "top_alternates": " || ".join(a for a in alts if a),
        })

    out_policy = Path(args.out_policy)
    print(f"[discover] using flatten dir: {phase0_dir}")
    print(f"[discover] writing policy: {out_policy}")
    out_policy.parent.mkdir(parents=True, exist_ok=True)
    out_policy.write_text(json.dumps(policies, indent=2, sort_keys=True), encoding="utf-8")

    _write_csv(
        phase0_dir.parent / "diagnostics" / "discovery_report.csv",
        ["domain", "method_used", "selected_fields", "coverage", "collision_rate", "needs_pareto_reason", "top_alternates"],
        sorted(report_rows, key=lambda r: r["domain"].lower()),
    )

    if failures and not args.warn_only:
        raise SystemExit(f"Failed to discover policies for domains: {','.join(sorted(failures))}")


if __name__ == "__main__":
    main()
