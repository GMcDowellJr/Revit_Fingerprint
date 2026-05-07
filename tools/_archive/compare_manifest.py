# -*- coding: utf-8 -*-
"""
Compare two fingerprint runs using the stable manifest surface.

Exit codes:
  0 = equal
  1 = differences found
  2 = invalid input (missing/unreadable/no contract surface)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, Tuple

from core.manifest import build_manifest


def _load_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise ValueError("missing file: {}".format(path))
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _to_manifest(obj: Any) -> Dict[str, Any]:
    if isinstance(obj, dict) and isinstance(obj.get("_manifest", None), dict):
        return obj["_manifest"]
    return build_manifest(obj)


def _diff_manifests(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    diffs: Dict[str, Any] = {}

    def put(k: str, av: Any, bv: Any) -> None:
        if av != bv:
            diffs[k] = {"a": av, "b": bv}

    put("schema_version", a.get("schema_version"), b.get("schema_version"))
    put("hash_mode", a.get("hash_mode"), b.get("hash_mode"))
    put("run_status", a.get("run_status"), b.get("run_status"))

    da = a.get("domains", {}) if isinstance(a.get("domains", None), dict) else {}
    db = b.get("domains", {}) if isinstance(b.get("domains", None), dict) else {}

    names = sorted(set(da.keys()) | set(db.keys()))
    domain_diffs: Dict[str, Any] = {}

    for n in names:
        ea = da.get(n, None)
        eb = db.get(n, None)
        if ea is None:
            domain_diffs[n] = {"added_in_b": True, "b": eb}
            continue
        if eb is None:
            domain_diffs[n] = {"removed_in_b": True, "a": ea}
            continue

        # Compare envelope fields
        ed: Dict[str, Any] = {}
        for k in ("status", "hash", "block_reasons"):
            av = ea.get(k, None) if isinstance(ea, dict) else None
            bv = eb.get(k, None) if isinstance(eb, dict) else None
            if av != bv:
                ed[k] = {"a": av, "b": bv}

        if ed:
            domain_diffs[n] = ed

    if domain_diffs:
        diffs["domains"] = domain_diffs

    return diffs


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("a", help="Path to full payload JSON or manifest JSON")
    ap.add_argument("b", help="Path to full payload JSON or manifest JSON")
    ap.add_argument("--json", dest="as_json", action="store_true", help="Emit machine-readable diff JSON")
    args = ap.parse_args(argv)

    try:
        oa = _load_json(args.a)
        ob = _load_json(args.b)
    except Exception as e:
        sys.stderr.write("ERROR: {}\n".format(str(e)))
        return 2

    try:
        ma = _to_manifest(oa)
        mb = _to_manifest(ob)
    except Exception as e:
        sys.stderr.write("ERROR: cannot build manifest: {}\n".format(str(e)))
        return 2

    diffs = _diff_manifests(ma, mb)
    equal = not bool(diffs)

    if args.as_json:
        sys.stdout.write(json.dumps({"equal": equal, "diff": diffs}, indent=2, sort_keys=True))
        sys.stdout.write("\n")
    else:
        if equal:
            sys.stdout.write("EQUAL\n")
        else:
            sys.stdout.write("DIFF\n")
            # Compact summary
            if "schema_version" in diffs:
                sys.stdout.write("  schema_version: {a} -> {b}\n".format(**diffs["schema_version"]))
            if "hash_mode" in diffs:
                sys.stdout.write("  hash_mode: {a} -> {b}\n".format(**diffs["hash_mode"]))
            if "run_status" in diffs:
                sys.stdout.write("  run_status: {a} -> {b}\n".format(**diffs["run_status"]))

            dd = diffs.get("domains", {})
            if isinstance(dd, dict) and dd:
                for dn in sorted(dd.keys()):
                    d = dd[dn]
                    if "added_in_b" in d:
                        sys.stdout.write("  + domain {0}\n".format(dn))
                        continue
                    if "removed_in_b" in d:
                        sys.stdout.write("  - domain {0}\n".format(dn))
                        continue
                    sys.stdout.write("  * domain {0}\n".format(dn))
                    for k in ("status", "hash", "block_reasons"):
                        if k in d:
                            sys.stdout.write("      {0}: {1} -> {2}\n".format(k, d[k]["a"], d[k]["b"]))

    return 0 if equal else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
