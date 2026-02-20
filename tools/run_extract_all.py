#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from v21_emit import emit_analysis_v21, emit_phase0_v21


def _discover_domains_from_exports(exports_dir: Path) -> List[str]:
    """
    Best-effort discovery of domains from fingerprint JSON exports.
    Assumes domains are top-level keys excluding meta keys (leading underscore) and known non-domain keys.
    Deterministic: returns sorted list.
    """
    exports_dir = Path(exports_dir)
    domains: set[str] = set()

    # Scan a limited set first for speed; fall back to full scan if needed.
    candidates = sorted(exports_dir.glob("*.json"))
    if not candidates:
        return []

    for p in candidates[:200]:
        try:
            with p.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue

        if not isinstance(data, dict):
            continue

        for k, v in data.items():
            if not isinstance(k, str):
                continue
            if k.startswith("_"):
                continue
            if k in ("artifacts",):
                continue
            # Domain payloads are typically dict-like.
            if isinstance(v, dict):
                domains.add(k)

    return sorted(domains, key=lambda s: s.lower())


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _run(cmd: List[str], *, env: Dict[str, str]) -> None:
    start = time.time()
    print(f"[extract_all] RUN: {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True, env=env)
    print(f"[extract_all] DONE ({time.time() - start:.1f}s): {cmd[1] if len(cmd) > 1 else cmd[0]}", flush=True)


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    import csv

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [{str(k): "" if v is None else str(v) for k, v in row.items()} for row in csv.DictReader(f)]


def _emit_join_policy_diagnostics(rows: List[Dict[str, str]], diagnostics_dir: Path, domains: Optional[List[str]] = None) -> List[Dict[str, str]]:
    import csv

    dom_filter = set(domains or [])
    problems: List[Dict[str, str]] = []
    for r in rows:
        dom = str(r.get("domain", "")).strip()
        if dom_filter and dom not in dom_filter:
            continue
        schema = str(r.get("join_key_schema", "")).strip()
        status = str(r.get("join_key_status", "")).strip()
        if schema == "sig_hash_as_join_key.v1" or status != "ok":
            problems.append(
                {
                    "domain": dom,
                    "file_id": str(r.get("file_id", "")),
                    "record_pk": str(r.get("record_pk", "")),
                    "join_key_schema": schema,
                    "join_key_status": status,
                    "reason": "bootstrap_schema" if schema == "sig_hash_as_join_key.v1" else "non_ok_status",
                }
            )
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    out_csv = diagnostics_dir / "join_policy_gate_diagnostics.csv"
    fields = ["domain", "file_id", "record_pk", "join_key_schema", "join_key_status", "reason"]
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in sorted(problems, key=lambda x: (x["domain"], x["file_id"], x["record_pk"])):
            w.writerow(row)
    return problems


def _detect_surfaces(exports_dir: Path) -> Dict[str, int]:
    names = [p.name for p in exports_dir.iterdir() if p.is_file() and p.name.lower().endswith(".json")]
    details = sum(1 for n in names if n.lower().endswith(".details.json"))
    index = sum(1 for n in names if n.lower().endswith(".index.json"))
    legacy = sum(1 for n in names if n.lower().endswith(".legacy.json"))
    plain = sum(
        1
        for n in names
        if n.lower().endswith(".json")
        and not (
            n.lower().endswith(".details.json")
            or n.lower().endswith(".index.json")
            or n.lower().endswith(".legacy.json")
        )
    )
    return {"details": details, "index": index, "legacy": legacy, "plain_json": plain, "total_json": len(names)}


def _merge_index_details(index_fp: Dict[str, Any], details_fp: Dict[str, Any]) -> Dict[str, Any]:
    """Merge index (metadata) and details (domain payloads) into a single fingerprint object."""
    merged = {**index_fp}
    for key, value in details_fp.items():
        # Domain payloads don't start with underscore; index metadata does
        if not key.startswith("_") and key not in merged:
            merged[key] = value
    return merged


def _pick_sample_file(exports_dir: Path) -> Tuple[Optional[Path], Optional[Path]]:
    """Pick sample files for domain inference.

    Returns (index_path, details_path) tuple. Both may be None if no files found.
    For split exports, returns both index and details paths.
    For legacy/plain exports, returns (path, None).
    """
    details = sorted(exports_dir.glob("*.details.json"))
    index = sorted(exports_dir.glob("*.index.json"))

    if index and details:
        # Split export: return first matching pair
        index_by_stem = {p.stem.lower().replace('.index', ''): p for p in index}
        details_by_stem = {p.stem.lower().replace('.details', ''): p for p in details}
        for stem in sorted(index_by_stem.keys()):
            if stem in details_by_stem:
                return (index_by_stem[stem], details_by_stem[stem])
        # Fallback: return first index even without matching details
        return (index[0], details_by_stem.get(index[0].stem.lower().replace('.index', '')))

    if index:
        return (index[0], None)

    if details:
        return (None, details[0])

    plain = sorted([p for p in exports_dir.glob("*.json") if not p.name.lower().endswith(".legacy.json")])
    if plain:
        return (plain[0], None)

    legacy = sorted(exports_dir.glob("*.legacy.json"))
    if legacy:
        return (legacy[0], None)

    return (None, None)


def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise TypeError(f"JSON root must be object: {path}")
    return data


def _infer_domains(exports_dir: Path) -> List[str]:
    """Infer domain names from sample export files.

    Handles split exports by merging index + details for reliable domain discovery.
    """
    index_path, details_path = _pick_sample_file(exports_dir)

    if index_path is None and details_path is None:
        return []

    # Load and potentially merge files
    fp: Dict[str, Any] = {}
    if index_path and details_path:
        # Split export: merge index + details
        sys.stderr.write("[INFO run_extract_all] Found split exports. Merging index + details for domain inference.\n")
        index_fp = _read_json(index_path)
        details_fp = _read_json(details_path)
        fp = _merge_index_details(index_fp, details_fp)
    elif index_path:
        fp = _read_json(index_path)
    elif details_path:
        fp = _read_json(details_path)

    # Try contract first (most reliable)
    c = fp.get("_contract")
    if isinstance(c, dict):
        doms = c.get("domains")
        if isinstance(doms, dict):
            return sorted([str(k) for k in doms.keys()])

    # Try _domains (back-compat surface)
    d = fp.get("_domains")
    if isinstance(d, dict):
        return sorted([str(k) for k in d.keys()])

    # Fallback: scan top-level keys for domain-like payloads
    out: List[str] = []
    for k, v in fp.items():
        if not isinstance(k, str) or k.startswith("_"):
            continue
        if isinstance(v, dict) and (("records" in v) or ("status" in v) or ("domain_version" in v)):
            out.append(k)
    return sorted(out)


def _parse_stage_csv(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [s.strip().lower() for s in str(raw).split(',') if s.strip()]


def _warn_deprecated_alias(flag: str, replacement: str) -> None:
    sys.stderr.write(f"[WARN extract_all] Deprecated alias: use {replacement} instead of {flag}.\n")


def _enforce_policy_gate(rows: List[Dict[str, str]], diagnostics_dir: Path, domains: Optional[List[str]], allow_sig_hash_join_key: bool) -> None:
    problems = _emit_join_policy_diagnostics(rows, diagnostics_dir, domains)
    if problems and not allow_sig_hash_join_key:
        raise SystemExit(
            "Join-policy gate failed: identity-mode join keys detected (join_key_schema=sig_hash_as_join_key.v1 or join_key_status!=ok). "
            "Re-run with --stages flatten,discover,apply,split (or include analyze1/analyze2 with apply), "
            "or use --allow-sig-hash-join-key for degraded exploratory analysis. "
            f"Diagnostics: {diagnostics_dir / 'join_policy_gate_diagnostics.csv'}"
        )
    if problems and allow_sig_hash_join_key:
        sys.stderr.write("\n" + "!" * 80 + "\n")
        sys.stderr.write("[WARN extract_all] --allow-sig-hash-join-key enabled; proceeding with DEGRADED identity-mode clustering (not for governance conclusions).\n")
        sys.stderr.write(f"[WARN extract_all] Diagnostics: {diagnostics_dir / 'join_policy_gate_diagnostics.csv'}\n")
        sys.stderr.write("!" * 80 + "\n\n")


def main() -> None:
    stage_names = ["flatten", "discover", "apply", "split", "analyze1", "analyze2"]
    ap = argparse.ArgumentParser(
        description=(
            "Pipeline orchestrator with explicit stages: flatten (T0), discover (T1), apply (T2), split, analyze1, analyze2. "
            "Default stages are flatten,discover. Apply is opt-in. Identity-mode join schema sig_hash_as_join_key.v1 is degraded and gated by default."
        ),
        epilog=(
            "Examples:\n"
            "  default (draft prep): --stages flatten,discover\n"
            "  operational commit:  --stages flatten,discover,apply\n"
            "  analysis after apply: --stages flatten,discover,apply,split,analyze1,analyze2\n"
            "  degraded exploratory analysis (not governance-grade): add --allow-sig-hash-join-key\n"
            "  matrix reference: docs/extract_stage_matrix.md"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    ap.add_argument("exports_dir", help="Folder containing fingerprint exports (*.details.json / *.index.json).")
    ap.add_argument("--out-root", required=True, help="Output root folder.")
    ap.add_argument("--config", default=None, help="Phase1 config path (required when stage analyze1 is included).")
    ap.add_argument("--seed-baseline", default=None, help="Optional seed baseline fingerprint JSON path for analyze1.")
    ap.add_argument("--domains", default=None, help="Comma list of domains; if omitted, infer from exports.")
    ap.add_argument("--baseline", default=None, help="Baseline export filename for analyze2 dimension_types by-family packet.")
    ap.add_argument("--emit-legacy", action="store_true", help="Also emit legacy phase0/phase1/phase2 artifacts when analyze1/analyze2 run.")
    ap.add_argument("--no-dimtypes-by-family", action="store_true", help="Disable dimension_types by-family packet (default: enabled).")
    ap.add_argument("--stages", default="flatten,discover", help="Comma-separated stages to run. Default: flatten,discover.")
    ap.add_argument("--skip-stages", default="", help="Comma-separated stages to skip from --stages.")
    ap.add_argument("--discover-join-policy", action="store_true", help="Alias for including stage discover.")
    ap.add_argument("--apply-join-policy", action="store_true", help="Alias for including stage apply (operational commit path).")
    ap.add_argument("--join-policy", default=None, help="Policy JSON path used by apply stage.")
    ap.add_argument("--require-join-policy", action=argparse.BooleanOptionalAction, default=True, help="Require policy-mode join keys for split/analyze stages (default: true).")
    ap.add_argument("--strict-join-policy", action="store_true", help="Deprecated alias for --require-join-policy.")
    ap.add_argument("--allow-sig-hash-join-key", action="store_true", help="Allow degraded identity-mode join keys (sig_hash_as_join_key.v1) for exploratory analysis.")
    ap.add_argument("--allow-bootstrap", action="store_true", help="Deprecated alias for --allow-sig-hash-join-key.")
    ap.add_argument("--emit-v21", action="store_true", help="Deprecated alias; v2.1 flatten is default and always emitted.")
    ap.add_argument("--emit-phase0-v21", action="store_true", help="Deprecated alias; v2.1 flatten is default and always emitted.")
    ap.add_argument("--phase0-only", action="store_true", help="Deprecated alias for --stages flatten.")
    ap.add_argument("--phase1-only", action="store_true", help="Deprecated alias for --stages analyze1.")
    ap.add_argument("--phase2-only", action="store_true", help="Deprecated alias for --stages analyze2.")
    ap.add_argument("--split-only", action="store_true", help="Deprecated alias for --stages split.")
    ap.add_argument("--split-domains", nargs="?", const="__ALL__", default=None, help="Domains for split stage; optional CSV. If no value, run all discovered domains.")
    ap.add_argument("--mode", choices=("allpairs", "candidates"), default="allpairs", help="File-level split detection mode.")
    ap.add_argument("--discover-sample-size", type=int, default=5000, help="Max records per domain for discover stage (default: 5000; set <=0 to disable sampling).")
    ap.add_argument("--discover-sample-seed", type=int, default=17, help="Deterministic sampling seed for discover stage (default: 17).")
    ap.add_argument("--discover-max-candidate-fields", type=int, default=64, help="Max candidate fields per domain for discover stage (default: 64; <=0 disables cap).")
    args = ap.parse_args()

    for alias, repl, used in [
        ("--strict-join-policy", "--require-join-policy", args.strict_join_policy),
        ("--allow-bootstrap", "--allow-sig-hash-join-key", args.allow_bootstrap),
        ("--emit-v21", "--stages flatten,discover", args.emit_v21),
        ("--emit-phase0-v21", "--stages flatten,discover", args.emit_phase0_v21),
        ("--phase0-only", "--stages flatten", args.phase0_only),
        ("--phase1-only", "--stages analyze1", args.phase1_only),
        ("--phase2-only", "--stages analyze2", args.phase2_only),
        ("--split-only", "--stages split", args.split_only),
    ]:
        if used:
            _warn_deprecated_alias(alias, repl)

    allow_sig_hash_join_key = args.allow_sig_hash_join_key or args.allow_bootstrap
    require_join_policy = args.require_join_policy or args.strict_join_policy

    selected_stages = _parse_stage_csv(args.stages) or ["flatten", "discover"]
    if args.discover_join_policy and "discover" not in selected_stages:
        selected_stages.append("discover")
    if args.apply_join_policy and "apply" not in selected_stages:
        selected_stages.append("apply")

    only_aliases = [args.phase0_only, args.phase1_only, args.phase2_only, args.split_only]
    if sum(1 for f in only_aliases if f) > 1:
        raise SystemExit("Only one of --phase0-only/--phase1-only/--phase2-only/--split-only may be used.")
    if args.phase0_only:
        selected_stages = ["flatten"]
    elif args.phase1_only:
        selected_stages = ["analyze1"]
    elif args.phase2_only:
        selected_stages = ["analyze2"]
    elif args.split_only:
        selected_stages = ["split"]

    skipped = set(_parse_stage_csv(args.skip_stages))
    for st in selected_stages + list(skipped):
        if st not in stage_names:
            raise SystemExit(f"Unknown stage: {st}. Valid stages: {','.join(stage_names)}")
    selected_stages = [s for s in stage_names if s in selected_stages and s not in skipped]

    plan_msg = " → ".join([s if s in selected_stages else f"({s} skipped)" for s in stage_names])
    if require_join_policy and any(s in selected_stages for s in ("split", "analyze1", "analyze2")) and "apply" not in selected_stages:
        plan_msg += " → (analysis gated: requires policy join keys; include apply stage)"
    print(f"Plan: {plan_msg}")

    exports_dir = Path(args.exports_dir).resolve()
    out_root = Path(args.out_root).resolve()
    phase0_dir = out_root / "phase0_flat"
    phase1_dir = out_root / "phase1_authority"
    phase2_root = out_root / "phase2_domain"
    v21_root = out_root / "Results_v21"
    v21_phase0_dir = v21_root / "phase0_v21"
    v21_analysis_dir = v21_root / "analysis_v21"
    v21_split_root = v21_root / "split_analysis"

    _ensure_dir(out_root)
    surfaces = _detect_surfaces(exports_dir)

    if args.domains and str(args.domains).strip():
        domains = [d.strip() for d in str(args.domains).split(",") if d.strip()]
    else:
        domains = _infer_domains(exports_dir)
    if not domains and any(s in selected_stages for s in ("analyze2",)):
        raise SystemExit("No domains inferred; provide --domains.")

    env = os.environ.copy()
    report: Dict[str, Any] = {"tool": "tools/run_extract_all.py", "exports_dir": str(exports_dir), "out_root": str(out_root), "surfaces": surfaces, "domains": domains, "selected_stages": selected_stages, "commands": [], "notes": []}
    meta_rows: List[Dict[str, str]] = []
    record_rows: List[Dict[str, str]] = []

    if "flatten" in selected_stages:
        print("[extract_all] Stage flatten (T0): emitting flatten outputs...", flush=True)
        _ensure_dir(v21_phase0_dir)
        report["commands"].append({"stage": "flatten", "out": str(v21_phase0_dir)})
        meta_rows, record_rows = emit_phase0_v21(exports_dir, v21_phase0_dir, file_id_mode="basename")
        print(f"[extract_all] Stage flatten complete: rows={len(record_rows)} files={len(meta_rows)} out={v21_phase0_dir}", flush=True)

    if "discover" in selected_stages:
        print("[extract_all] Stage discover (T1): deriving join policy candidates...", flush=True)
        discover_out = Path(args.join_policy).resolve() if args.join_policy else (v21_root / "policies" / "domain_join_key_policies.v21.json")
        cmd_discover = [sys.executable, "tools/v21_discover_join_policy.py", "--phase0-dir", str(v21_phase0_dir), "--out-policy", str(discover_out), "--sample-size", str(args.discover_sample_size), "--sample-seed", str(args.discover_sample_seed), "--max-candidate-fields", str(args.discover_max_candidate_fields)]
        report["commands"].append({"stage": "discover", "cmd": cmd_discover})
        _run(cmd_discover, env=env)
        args.join_policy = str(discover_out)

    if "apply" in selected_stages:
        print("[extract_all] Stage apply (T2): applying join policy to flatten outputs...", flush=True)
        policy_path = Path(args.join_policy).resolve() if args.join_policy else (v21_root / "policies" / "domain_join_key_policies.v21.json").resolve()
        cmd_apply = [sys.executable, "tools/v21_apply_join_policy.py", "--phase0-dir", str(v21_phase0_dir), "--join-policy", str(policy_path)]
        report["commands"].append({"stage": "apply", "cmd": cmd_apply})
        _run(cmd_apply, env=env)

    if any(s in selected_stages for s in ("split", "analyze1", "analyze2")) and require_join_policy:
        phase0_records_csv = v21_phase0_dir / "phase0_records.csv"
        if phase0_records_csv.is_file():
            _enforce_policy_gate(_read_csv_rows(phase0_records_csv), v21_root / "diagnostics", domains, allow_sig_hash_join_key)

    if "analyze1" in selected_stages or "analyze2" in selected_stages:
        phase0_records_csv = v21_phase0_dir / "phase0_records.csv"
        if phase0_records_csv.is_file() and not record_rows:
            record_rows = _read_csv_rows(phase0_records_csv)
        if (v21_phase0_dir / "file_metadata.csv").is_file() and not meta_rows:
            meta_rows = _read_csv_rows(v21_phase0_dir / "file_metadata.csv")
        if meta_rows and record_rows:
            _ensure_dir(v21_analysis_dir)
            analysis_run_id = emit_analysis_v21(meta_rows, record_rows, v21_analysis_dir)
            report["notes"].append(f"analysis_run_id={analysis_run_id}")

    if "analyze1" in selected_stages and args.emit_legacy:
        if not args.config:
            sys.stderr.write("[WARN extract_all] --config not provided; skipping analyze1 legacy outputs.\n")
        else:
            _ensure_dir(phase1_dir)
            cmd1a = [sys.executable, "tools/phase1_domain_authority.py", "--input-dir", str(exports_dir), "--config", str(Path(args.config).resolve()), "--out-dir", str(phase1_dir)]
            if args.seed_baseline:
                sb = Path(args.seed_baseline)
                seed_path = (exports_dir / sb) if not sb.is_absolute() else sb
                cmd1a += ["--seed-baseline", str(seed_path.resolve())]
            _run(cmd1a, env=env)
            _run([sys.executable, "tools/phase1_population_framing.py", "--domain-clusters", str(phase1_dir / "domain_cluster_summary.csv"), "--domain-authority", str(phase1_dir / "domain_authority_summary.csv"), "--run-config", str(Path(args.config).resolve()), "--out", str(phase1_dir / "population_baseline_summary.csv")], env=env)
            _run([sys.executable, "tools/phase1_pairwise_analysis.py", "--baseline-coverage", str(phase1_dir / "baseline_coverage_by_project.csv"), "--out-dir", str(phase1_dir)], env=env)

    if "analyze2" in selected_stages and args.emit_legacy:
        _ensure_dir(phase2_root)
        for dom in domains:
            dom_out = phase2_root / dom
            _ensure_dir(dom_out)
            for mod in ["run_joinhash_label_population", "run_joinhash_parameter_population", "run_candidate_joinkey_simulation", "run_population_stability"]:
                _run([sys.executable, "-m", f"tools.phase2_analysis.{mod}", str(exports_dir), "--domain", dom, "--out", str(dom_out)], env=env)
            if dom == "dimension_types" and not args.no_dimtypes_by_family:
                cmd2e = [sys.executable, "-m", "tools.phase2_analysis.run_dimension_types_by_family", str(exports_dir), "--domain", "dimension_types", "--out", str(dom_out)]
                cmd2e += ["--baseline", str(args.baseline), "--families_from", "baseline"] if args.baseline else ["--families_from", "all"]
                _run(cmd2e, env=env)

    split_domains: List[str] = []
    if "split" in selected_stages:
        if args.split_domains is None or str(args.split_domains) == "__ALL__":
            split_domains = sorted({str(r.get("domain", "")).strip() for r in (record_rows or []) if str(r.get("domain", "")).strip()}, key=lambda s: s.lower())
            if not split_domains:
                split_domains = _discover_domains_from_exports(exports_dir)
        else:
            split_domains = [d.strip() for d in str(args.split_domains).split(",") if d.strip()]

    if split_domains:
        print(f"[extract_all] Stage split: running split detection for {len(split_domains)} domain(s)...", flush=True)
        _ensure_dir(v21_split_root)
        phase0_records_csv = v21_phase0_dir / "phase0_records.csv"
        use_phase0_dir = phase0_records_csv.is_file()
        if use_phase0_dir and require_join_policy:
            _enforce_policy_gate(_read_csv_rows(phase0_records_csv), v21_root / "diagnostics", split_domains, allow_sig_hash_join_key)
        for split_domain in split_domains:
            cmd_split = [sys.executable, "tools/run_split_detection_all.py", str(exports_dir), "--domain", split_domain, "--out-root", str(v21_split_root / split_domain), "--mode", str(args.mode), *(["--phase0-dir", str(v21_phase0_dir)] if use_phase0_dir else []), *(["--allow-sig-hash-join-key"] if allow_sig_hash_join_key else [])]
            report["commands"].append({"stage": "split", "domain": split_domain, "cmd": cmd_split})
            _run(cmd_split, env=env)

    report_path = out_root / "extract_all.report.json"
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, sort_keys=True)
    print(f"Wrote: {report_path}")


if __name__ == "__main__":
    main()
