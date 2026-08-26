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


def _constant_strings(scope):
    values = {}
    for node in getattr(scope, "body", []):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            target = node.targets[0] if isinstance(node, ast.Assign) and len(node.targets) == 1 else getattr(node, "target", None)
            value = node.value
            if isinstance(target, ast.Name) and isinstance(value, ast.Constant) and isinstance(value.value, str):
                values[target.id] = value.value
    return values


def _emitted_domain(node, parents, module_constants, tree):
    function = next((p for p in parents[node] if isinstance(p, (ast.FunctionDef, ast.AsyncFunctionDef))), None)
    constants = dict(module_constants)
    if function: constants.update(_constant_strings(function))
    scope = function or next(p for p in parents[node] if isinstance(p, ast.Module))
    domains = set()
    for call in ast.walk(scope):
        if not isinstance(call, ast.Call): continue
        fn = call.func.id if isinstance(call.func, ast.Name) else getattr(call.func, "attr", "")
        if fn not in {"build_record_v2", "blocked_record_v2"}: continue
        value = next((kw.value for kw in call.keywords if kw.arg == "domain"), None)
        if isinstance(value, ast.Constant) and isinstance(value.value, str): domains.add(value.value)
        elif isinstance(value, ast.Name) and value.id in constants: domains.add(constants[value.id])
        elif isinstance(value, ast.Name) and function and value.id in {arg.arg for arg in function.args.args + function.args.kwonlyargs}:
            # Resolve wrapper calls such as _extract_object_styles(...,
            # domain_name="object_styles_model") when the record builder uses
            # the function parameter as its runtime domain.
            for outer in ast.walk(tree):
                if not isinstance(outer, ast.Call): continue
                outer_name = outer.func.id if isinstance(outer.func, ast.Name) else getattr(outer.func, "attr", "")
                if outer_name != function.name: continue
                supplied = next((kw.value for kw in outer.keywords if kw.arg == value.id), None)
                if isinstance(supplied, ast.Constant) and isinstance(supplied.value, str): domains.add(supplied.value)
    return domains


def _module_emitted_domains(tree, parents, module_constants):
    domains = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call): continue
        fn = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, "attr", "")
        if fn in {"build_record_v2", "blocked_record_v2"}:
            domains.update(_emitted_domain(node, parents, module_constants, tree))
    return domains


def _static_items(repo: Path, policy_domains_by_key):
    found = {}
    for path in sorted((repo / "domains").glob("*.py")):
        text = path.read_text(encoding="utf-8")
        try: tree = ast.parse(text)
        except SyntaxError: continue
        parents = defaultdict(list)
        for parent in ast.walk(tree):
            for child in ast.iter_child_nodes(parent): parents[child] = [parent] + parents[parent]
        module_constants = _constant_strings(tree)
        module_domains = _module_emitted_domains(tree, parents, module_constants)
        for node in ast.walk(tree):
            key = None
            if isinstance(node, ast.Call) and node.args:
                fn = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, "attr", "")
                if fn in {"make_identity_item", "identity_item"} and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str): key = node.args[0].value
            elif isinstance(node, ast.Dict):
                for k, value in zip(node.keys, node.values):
                    if isinstance(k, ast.Constant) and k.value == "k" and isinstance(value, ast.Constant) and isinstance(value.value, str): key = value.value
            if not key: continue
            context = "\n".join(text.splitlines()[max(0, node.lineno-8):node.lineno+1])
            layer = "identity"
            if "unknown_items" in context: layer = "phase2.unknown"
            elif "coordination_items" in context: layer = "phase2.coordination"
            elif "cosmetic_items" in context: layer = "phase2.cosmetic"
            domains = set(policy_domains_by_key.get(key, ())) or _emitted_domain(node, parents, module_constants, tree) or module_domains
            if not domains:
                domains = {path.stem}
            for domain in domains:
                found[(domain, key)] = (layer, f"{path.relative_to(repo)}:{node.lineno}")
    return found


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--phase0-dir", type=Path)
    ap.add_argument("--output", type=Path, default=Path("discovery_candidate_inventory.csv"))
    ap.add_argument("--summary", type=Path, default=Path("discovery_candidate_inventory_summary.json"))
    args = ap.parse_args(); repo = Path(__file__).resolve().parents[1]
    join_roles = _policy_roles(repo / "policies/domain_join_key_policies.json")
    sig_roles = _policy_roles(repo / "policies/domain_sig_hash_policies.json")
    policy_domains_by_key = defaultdict(set)
    for domain, key in set(join_roles) | set(sig_roles): policy_domains_by_key[key].add(domain)
    evidence = _static_items(repo, policy_domains_by_key); observed = defaultdict(lambda: Counter(records=set()))
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
    fields = ["domain","item_key","source_layer","source_location","classification","eligible","canonical_item","exclusion_reason","candidate_for_sig_discovery","candidate_for_join_discovery","current_join_policy_role","current_sig_policy_role","current_policy_required","current_policy_optional","current_policy_excluded","observed_record_count","observed_ok_count","observed_missing_count","observed_unreadable_count"]
    rows=[]
    for (domain,key),(layer,location) in sorted(evidence.items(), key=lambda x:(x[0][0].lower(),x[0][1].lower())):
        d=classify_candidate(domain,key); jr=join_roles.get((domain,key),""); sr=sig_roles.get((domain,key),""); stat=observed[(domain,key)]
        rows.append({"domain":domain,"item_key":key,"source_layer":layer,"source_location":location,"classification":d.classification,"eligible":str(d.eligible).lower(),"canonical_item":d.canonical_item,"exclusion_reason":d.reason,"candidate_for_sig_discovery":str(d.eligible).lower(),"candidate_for_join_discovery":str(d.eligible).lower(),"current_join_policy_role":jr,"current_sig_policy_role":sr,"current_policy_required":str("required" in (jr,sr)).lower(),"current_policy_optional":str("optional" in (jr,sr)).lower(),"current_policy_excluded":str("excluded" in (jr,sr)).lower(),"observed_record_count":len(stat["records"]),"observed_ok_count":stat["ok"],"observed_missing_count":stat["missing"],"observed_unreadable_count":stat["unreadable"]})
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with args.output.open("w",encoding="utf-8",newline="") as f: w=csv.DictWriter(f,fieldnames=fields,lineterminator="\n");w.writeheader();w.writerows(rows)
    summary={"total":len(rows),"eligible":sum(r["eligible"]=="true" for r in rows),"ineligible":sum(r["eligible"]=="false" for r in rows),"by_domain":dict(sorted(Counter(r["domain"] for r in rows).items())),"by_classification":dict(sorted(Counter(r["classification"] for r in rows).items()))}
    args.summary.write_text(json.dumps(summary,indent=2,sort_keys=True)+"\n",encoding="utf-8")

if __name__ == "__main__": main()
