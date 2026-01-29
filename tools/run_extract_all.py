#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _run(cmd: List[str], *, env: Dict[str, str]) -> None:
    subprocess.run(cmd, check=True, env=env)


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


def _pick_sample_file(exports_dir: Path) -> Optional[Path]:
    # Prefer details, then index, then non-legacy json, then legacy.
    details = sorted(exports_dir.glob("*.details.json"))
    if details:
        return details[0]
    index = sorted(exports_dir.glob("*.index.json"))
    if index:
        return index[0]
    plain = sorted([p for p in exports_dir.glob("*.json") if not p.name.lower().endswith(".legacy.json")])
    if plain:
        return plain[0]
    legacy = sorted(exports_dir.glob("*.legacy.json"))
    if legacy:
        return legacy[0]
    return None


def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise TypeError(f"JSON root must be object: {path}")
    return data


def _infer_domains(exports_dir: Path) -> List[str]:
    sample = _pick_sample_file(exports_dir)
    if sample is None:
        return []
    fp = _read_json(sample)

    c = fp.get("_contract")
    if isinstance(c, dict):
        doms = c.get("domains")
        if isinstance(doms, dict):
            return sorted([str(k) for k in doms.keys()])

    d = fp.get("_domains")
    if isinstance(d, dict):
        return sorted([str(k) for k in d.keys()])

    out: List[str] = []
    for k, v in fp.items():
        if not isinstance(k, str) or k.startswith("_"):
            continue
        if isinstance(v, dict) and (("records" in v) or ("status" in v) or ("domain_version" in v)):
            out.append(k)
    return sorted(out)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="One-shot extractor: Phase 0 (flat tables), Phase 1 (authority), Phase 2 (per-domain packet)."
    )
    ap.add_argument("exports_dir", help="Folder containing fingerprint exports (*.details.json / *.index.json).")
    ap.add_argument("--out-root", required=True, help="Output root folder.")
    ap.add_argument("--config", default=None, help="Phase-1 RunConfig JSON path (required to run Phase-1).")
    ap.add_argument("--seed-baseline", default=None, help="Optional seed baseline fingerprint JSON path for Phase-1.")
    ap.add_argument(
        "--domains",
        default=None,
        help="Comma list of domains. If omitted, infer from sample export.",
    )
    ap.add_argument(
        "--baseline",
        default=None,
        help="Baseline export filename (must exist in exports_dir). Required for dimension_types by-family packet.",
    )
    ap.add_argument("--skip-phase0", action="store_true")
    ap.add_argument("--skip-phase1", action="store_true")
    ap.add_argument("--skip-phase2", action="store_true")
    ap.add_argument(
        "--no-dimtypes-by-family",
        action="store_true",
        help="Disable dimension_types by-family packet (default: enabled).",
    )
    args = ap.parse_args()

    exports_dir = Path(args.exports_dir).resolve()
    out_root = Path(args.out_root).resolve()

    phase0_dir = out_root / "phase0_flat"
    phase1_dir = out_root / "phase1_authority"
    phase2_root = out_root / "phase2_domain"

    _ensure_dir(out_root)
    _ensure_dir(phase2_root)

    surfaces = _detect_surfaces(exports_dir)
    if surfaces.get("legacy", 0) > 0:
        sys.stderr.write("[WARN extract_all] legacy bundle(s) present; extractor will not enable legacy implicitly.\n")

    # Domains
    if args.domains and str(args.domains).strip():
        domains = [d.strip() for d in str(args.domains).split(",") if d.strip()]
    else:
        domains = _infer_domains(exports_dir)

    if not domains and not args.skip_phase2:
        raise SystemExit("No domains inferred; provide --domains.")

    env = os.environ.copy()

    report: Dict[str, Any] = {
        "tool": "tools/run_extract_all.py",
        "exports_dir": str(exports_dir),
        "out_root": str(out_root),
        "surfaces": surfaces,
        "domains": domains,
        "commands": [],
        "notes": [],
    }

    # -------------------------
    # Phase 0
    # -------------------------
    if not args.skip_phase0:
        _ensure_dir(phase0_dir)
        cmd0 = [
            sys.executable,
            "tools/export_to_flat_tables.py",
            "--root_dir",
            str(exports_dir),
            "--out_dir",
            str(phase0_dir),
            "--file_id_mode",
            "basename",
        ]
        report["commands"].append({"phase": "phase0", "cmd": cmd0})
        _run(cmd0, env=env)

    # -------------------------
    # Phase 1
    # -------------------------
    if not args.skip_phase1:
        if not args.config:
            sys.stderr.write("[WARN extract_all] --config not provided; skipping Phase-1.\n")
            report["notes"].append("phase1_skipped_no_config")
        else:
            _ensure_dir(phase1_dir)

            cmd1a = [
                sys.executable,
                "tools/phase1_domain_authority.py",
                "--input-dir",
                str(exports_dir),
                "--config",
                str(Path(args.config).resolve()),
                "--out-dir",
                str(phase1_dir),
            ]
            
            if args.seed_baseline:
                sb = Path(args.seed_baseline)
                # If user provided a relative path or filename, interpret it relative to exports_dir.
                seed_path = (exports_dir / sb) if not sb.is_absolute() else sb
                seed_path = seed_path.resolve()
                if not seed_path.exists():
                    raise SystemExit(
                        f"--seed-baseline not found: {seed_path}\n"
                        f"Tip: pass a full path or a filename that exists under exports_dir: {exports_dir}"
                    )
                cmd1a += ["--seed-baseline", str(seed_path)]

            report["commands"].append({"phase": "phase1", "step": "domain_authority", "cmd": cmd1a})
            _run(cmd1a, env=env)

            cmd1b = [
                sys.executable,
                "tools/phase1_population_framing.py",
                "--domain-clusters",
                str(phase1_dir / "domain_cluster_summary.csv"),
                "--domain-authority",
                str(phase1_dir / "domain_authority_summary.csv"),
                "--run-config",
                str(Path(args.config).resolve()),
                "--out",
                str(phase1_dir / "population_baseline_summary.csv"),
            ]
            report["commands"].append({"phase": "phase1", "step": "population_framing", "cmd": cmd1b})
            _run(cmd1b, env=env)

            cmd1c = [
                sys.executable,
                "tools/phase1_pairwise_analysis.py",
                "--baseline-coverage",
                str(phase1_dir / "baseline_coverage_by_project.csv"),
                "--out-dir",
                str(phase1_dir),
            ]
            report["commands"].append({"phase": "phase1", "step": "pairwise_analysis", "cmd": cmd1c})
            _run(cmd1c, env=env)

    # -------------------------
    # Phase 2 (per-domain packet)
    # -------------------------
    if not args.skip_phase2:
        for dom in domains:
            dom_out = phase2_root / dom
            _ensure_dir(dom_out)

            cmd2a = [
                sys.executable,
                "-m",
                "tools.phase2_analysis.run_joinhash_label_population",
                str(exports_dir),
                "--domain",
                dom,
                "--out",
                str(dom_out),
            ]
            report["commands"].append({"phase": "phase2", "domain": dom, "step": "joinhash_label_population", "cmd": cmd2a})
            _run(cmd2a, env=env)

            cmd2b = [
                sys.executable,
                "-m",
                "tools.phase2_analysis.run_joinhash_parameter_population",
                str(exports_dir),
                "--domain",
                dom,
                "--out",
                str(dom_out),
            ]
            report["commands"].append({"phase": "phase2", "domain": dom, "step": "joinhash_parameter_population", "cmd": cmd2b})
            _run(cmd2b, env=env)

            cmd2c = [
                sys.executable,
                "-m",
                "tools.phase2_analysis.run_candidate_joinkey_simulation",
                str(exports_dir),
                "--domain",
                dom,
                "--out",
                str(dom_out),
            ]
            report["commands"].append({"phase": "phase2", "domain": dom, "step": "candidate_joinkey_simulation", "cmd": cmd2c})
            _run(cmd2c, env=env)

            cmd2d = [
                sys.executable,
                "-m",
                "tools.phase2_analysis.run_population_stability",
                str(exports_dir),
                "--domain",
                dom,
                "--out",
                str(dom_out),
            ]
            report["commands"].append({"phase": "phase2", "domain": dom, "step": "population_stability", "cmd": cmd2d})
            _run(cmd2d, env=env)

            # Dimension types: by-family packet (default ON)
            if dom == "dimension_types" and not args.no_dimtypes_by_family:
                cmd2e = [
                    sys.executable,
                    "-m",
                    "tools.phase2_analysis.run_dimension_types_by_family",
                    str(exports_dir),
                    "--domain",
                    "dimension_types",
                    "--out",
                    str(dom_out),
                ]

                if args.baseline:
                    cmd2e += ["--baseline", str(args.baseline), "--families_from", "baseline"]
                else:
                    # Baseline-free mode: discover families from all exports; skip baseline-anchored steps inside.
                    cmd2e += ["--families_from", "all"]

                report["commands"].append(
                    {"phase": "phase2", "domain": dom, "step": "dimension_types_by_family", "cmd": cmd2e}
                )
                _run(cmd2e, env=env)

    report_path = out_root / "extract_all.report.json"
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, sort_keys=True)

    print(f"Wrote: {report_path}")


if __name__ == "__main__":
    main()
