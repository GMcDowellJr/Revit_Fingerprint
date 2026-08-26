"""Regenerate the repository-wide discovery candidate inventory.

Static extractor evidence is unioned with keys observed in a flattened corpus.
The output is deterministic and intentionally reports policy roles without
using those roles to decide diagnostic eligibility.
"""
from __future__ import annotations

import argparse, ast, csv, json, sys
from collections import Counter, defaultdict
from pathlib import Path

try:
    from tools.discovery_candidate_eligibility import classify_candidate
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from tools.discovery_candidate_eligibility import classify_candidate


def _policy_roles(path: Path):
    data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    out = {}
    for domain, block in data.get("domains", {}).items():
        for role, names in (("required", block.get("required_items", block.get("required_fields", []))), ("optional", block.get("optional_items", [])), ("excluded", block.get("explicitly_excluded_items", []))):
            for name in names or []: out[(domain, name)] = role
    return out


def _static_items(repo: Path):
    found = {}
    for path in sorted((repo / "domains").glob("*.py")):
        text = path.read_text(encoding="utf-8")
        try: tree = ast.parse(text)
        except SyntaxError: continue
        domain = path.stem
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args: continue
            fn = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, "attr", "")
            if fn not in {"make_identity_item", "identity_item"} or not isinstance(node.args[0], ast.Constant) or not isinstance(node.args[0].value, str): continue
            key = node.args[0].value
            context = "\n".join(text.splitlines()[max(0, node.lineno-8):node.lineno+1])
            layer = "identity"
            if "unknown_items" in context: layer = "phase2.unknown"
            elif "coordination_items" in context: layer = "phase2.coordination"
            elif "cosmetic_items" in context: layer = "phase2.cosmetic"
            found[(domain, key)] = (layer, f"{path.relative_to(repo)}:{node.lineno}")
    return found


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--phase0-dir", type=Path)
    ap.add_argument("--output", type=Path, default=Path("discovery_candidate_inventory.csv"))
    ap.add_argument("--summary", type=Path, default=Path("discovery_candidate_inventory_summary.json"))
    args = ap.parse_args(); repo = Path(__file__).resolve().parents[1]
    evidence = _static_items(repo); observed = defaultdict(lambda: Counter(records=set()))
    if args.phase0_dir:
        mono = args.phase0_dir / "identity_items.csv"
        paths = sorted((args.phase0_dir / "identity_items_by_domain").glob("*.csv")) if (args.phase0_dir / "identity_items_by_domain" / ".complete").exists() else ([mono] if mono.exists() else [])
        for path in paths:
            with path.open(encoding="utf-8", newline="") as f:
                for row in csv.DictReader(f):
                    domain, key = row.get("domain", ""), row.get("item_key", "")
                    if not domain or not key: continue
                    evidence.setdefault((domain, key), (row.get("source_layer") or "other", str(path)))
                    stat = observed[(domain, key)]; stat["rows"] += 1; stat["records"].add(row.get("record_pk", ""))
                    q = (row.get("item_q") or row.get("quality") or row.get("item_value_type") or "").lower()
                    if q in {"ok", "missing", "unreadable"}: stat[q] += 1
    join_roles = _policy_roles(repo / "policies/domain_join_key_policies.json")
    sig_roles = _policy_roles(repo / "policies/domain_sig_hash_policies.json")
    fields = ["domain","item_key","source_layer","source_location","classification","eligible","canonical_item","exclusion_reason","candidate_for_sig_discovery","candidate_for_join_discovery","current_join_policy_role","current_sig_policy_role","current_policy_required","current_policy_optional","current_policy_excluded","observed_record_count","observed_ok_count","observed_missing_count","observed_unreadable_count"]
    rows=[]
    for (domain,key),(layer,location) in sorted(evidence.items(), key=lambda x:(x[0][0].lower(),x[0][1].lower())):
        d=classify_candidate(domain,key); jr=join_roles.get((domain,key),""); sr=sig_roles.get((domain,key),""); stat=observed[(domain,key)]
        rows.append({"domain":domain,"item_key":key,"source_layer":layer,"source_location":location,"classification":d.classification,"eligible":str(d.eligible).lower(),"canonical_item":d.canonical_item,"exclusion_reason":d.reason,"candidate_for_sig_discovery":str(d.eligible).lower(),"candidate_for_join_discovery":str(d.eligible).lower(),"current_join_policy_role":jr,"current_sig_policy_role":sr,"current_policy_required":str("required" in (jr,sr)).lower(),"current_policy_optional":str("optional" in (jr,sr)).lower(),"current_policy_excluded":str("excluded" in (jr,sr)).lower(),"observed_record_count":len(stat["records"]),"observed_ok_count":stat["ok"],"observed_missing_count":stat["missing"],"observed_unreadable_count":stat["unreadable"]})
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with args.output.open("w",encoding="utf-8",newline="") as f: w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)
    summary={"total":len(rows),"eligible":sum(r["eligible"]=="true" for r in rows),"ineligible":sum(r["eligible"]=="false" for r in rows),"by_domain":dict(sorted(Counter(r["domain"] for r in rows).items())),"by_classification":dict(sorted(Counter(r["classification"] for r in rows).items()))}
    args.summary.write_text(json.dumps(summary,indent=2,sort_keys=True)+"\n",encoding="utf-8")

if __name__ == "__main__": main()
