from __future__ import annotations
import argparse, json, re
from collections import defaultdict
from pathlib import Path
if __package__ in (None, ""):
    import sys
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from common import atomic_write_csv, read_csv_rows
else:
    from .common import atomic_write_csv, read_csv_rows

DOMAINS=["wall_types","floor_types","roof_types","ceiling_types","text_types","dimension_types_linear","dimension_types_angular","dimension_types_radial","dimension_types_diameter","dimension_types_spot_elevation","dimension_types_spot_coordinate","dimension_types_spot_slope","arrowheads","line_patterns","fill_patterns_drafting","fill_patterns_model","line_styles","materials","view_templates"]
COLS=["schema_version","domain","file_id","type_name","type_count","purgeable_count","instance_count","is_sole_type","is_purgeable","matched_reference_name","matched_reference_category","suggested_exclude","suggestion_reasons","excluded","reviewed_by","override_reason"]
DOMAIN_AUTHORITY_COLS=["schema_version","domain","file_id","governance_role","total_count","known_builtin_count","adjusted_total","purgeable_count","purgeable_pct","sole_type_zero_instance_count","domain_authority","authority_reason","gap_threshold_used","gap_threshold_source","override_authority","reviewed_by","override_reason"]


def t(v): return str(v or "").strip().lower() in {"1","true","yes","y","t"}

def lg(vals):
    vals=sorted([v for v in vals if v>0])
    if len(vals)<2:return None
    bg=0;thr=None
    for a,b in zip(vals,vals[1:]):
        g=b-a
        if g>bg: bg=g;thr=(a+b)/2.0
    return thr if bg>=0.30 else None


def _to_int(v):
    s=(v or "").strip()
    try:
        return int(s) if s not in ("", "None", "none") else None
    except (TypeError, ValueError):
        return None


def _load_governance_roles(path: Path | None) -> dict[str, str]:
    if not path or not path.exists():
        return {}
    out = {}
    for r in read_csv_rows(path):
        fid = (r.get("file_id") or "").strip()
        if fid:
            out[fid] = (r.get("governance_role") or "").strip()
    return out


def _load_existing_overrides(path: Path) -> dict[tuple[str, str], dict[str, str]]:
    if not path.exists():
        return {}
    out = {}
    for r in read_csv_rows(path):
        key = ((r.get("domain") or "").strip(), (r.get("file_id") or "").strip())
        ov = (r.get("override_authority") or "").strip()
        rb = (r.get("reviewed_by") or "").strip()
        rs = (r.get("override_reason") or "").strip()
        if key[0] and key[1] and (ov or rb or rs):
            out[key] = {"override_authority": ov, "reviewed_by": rb, "override_reason": rs}
    return out


def _choose_threshold(values: list[int]) -> int | None:
    uniq = sorted(set(values))
    if len(uniq) < 2:
        return None
    median = uniq[len(uniq)//2] if len(uniq) % 2 == 1 else (uniq[len(uniq)//2 - 1] + uniq[len(uniq)//2]) / 2.0
    candidates = []
    for a, b in zip(uniq, uniq[1:]):
        gap = b - a
        mid = (a + b) / 2.0
        candidates.append((gap, abs(mid - median), a))
    max_gap = max(g for g, _, _ in candidates)
    best = [c for c in candidates if c[0] == max_gap]
    best.sort(key=lambda x: (x[1], x[2]))
    return best[0][2]


def compute_placeholder_exclusions(records_csv_path: Path, out_csv_path: Path) -> None:
    """Compatibility API for run_bundle_analysis legacy callsites.

    Produces a file-level exclusions CSV (domain,file_id) using the legacy
    implementation contract.
    """
    if __package__ in (None, ""):
        from placeholder_exclusions_legacy import compute_placeholder_exclusions as _legacy_compute
    else:
        from .placeholder_exclusions_legacy import compute_placeholder_exclusions as _legacy_compute
    _legacy_compute(Path(records_csv_path), Path(out_csv_path))

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--phase0-dir',type=Path,required=True)
    ap.add_argument('--policies-dir',type=Path,required=True)
    ap.add_argument('--out-dir',type=Path,required=True)
    ap.add_argument('--file-metadata-path',type=Path,default=None)
    a=ap.parse_args()
    rows=read_csv_rows(a.phase0_dir/'phase0_records.csv')
    pol=json.loads((a.policies_dir/'placeholder_known_defaults.json').read_text())
    pdom=pol.get('domains',{})
    file_roles = _load_governance_roles(a.file_metadata_path)
    existing_overrides = _load_existing_overrides(a.out_dir/'domain_authority_by_file.csv')
    by=defaultdict(list)
    for r in rows:
        d=(r.get('domain') or '').strip()
        if d in DOMAINS: by[d].append(r)
    all_rows=[]
    all_authority_rows=[]
    a.out_dir.mkdir(parents=True,exist_ok=True)
    for d in DOMAINS:
        drows=by[d]
        per=defaultdict(list)
        for r in drows:
            fid = (r.get("export_run_id") or "").strip() or (r.get("file_id") or "").strip()
            if not fid:
                continue
            role = (file_roles.get(fid, "") or "").strip()
            if role and role not in {"Template", "Container", "unknown"}:
                continue
            per[fid].append(r)
        file_pct={fid:(sum(1 for x in rs if t(x.get('is_purgeable')))/float(len(rs)) if rs else 0.0) for fid,rs in per.items()}
        thr=lg(list(file_pct.values()))
        out=[]
        authority_rows=[]
        cfg=pdom.get(d,{})
        kd=[x.lower() for x in cfg.get('known_defaults',[])]
        kb=[x.lower() for x in cfg.get('known_builtins',[])]
        pats=[re.compile(p) for p in cfg.get('placeholder_patterns',[])]
        for fid,rs in sorted(per.items()):
            tc=len(rs); pc=sum(1 for x in rs if t(x.get('is_purgeable')))
            sole_zero_count=0
            known_builtin_count=0
            for r in rs:
                nm=(r.get('label_display') or '').strip(); nml=nm.lower(); reasons=[]; mrn='';mrc=''
                is_builtin=nml in kb
                if is_builtin:
                    known_builtin_count += 1
                    reasons.append('known_builtin'); mrn=nm; mrc='known_builtins'
                if t(r.get('is_purgeable')): reasons.append('is_purgeable')
                instance_count = _to_int(r.get("instance_count", ""))
                is_sole_raw = ((r.get("is_sole_type_in_category") or r.get("is_sole_type") or "")).strip().lower()
                if is_sole_raw == "true":
                    is_sole_type = True
                elif is_sole_raw == "false":
                    is_sole_type = False
                else:
                    is_sole_type = None
                if is_sole_type is True and instance_count == 0:
                    sole_zero_count += 1
                    reasons.append('sole_type_zero_instances')
                if nml in kd:
                    reasons.append('known_default_name')
                    if not mrn: mrn=nm; mrc='known_defaults'
                if any(p.search(nm) for p in pats):
                    reasons.append('placeholder_pattern')
                    if not mrn: mrc='placeholder_patterns'
                if thr is not None and file_pct.get(fid,0)>thr: reasons.append('above_purgeable_threshold')
                sug=('false' if is_builtin else ('true' if any(x in reasons for x in ['is_purgeable','sole_type_zero_instances','known_default_name','placeholder_pattern']) else 'false'))
                rec={"schema_version":pol.get('schema_version','1.0'),"domain":d,"file_id":fid,"type_name":nm,
                "type_count":str(tc),"purgeable_count":str(pc),"instance_count":("" if instance_count is None else str(instance_count)),"is_sole_type":("" if is_sole_type is None else ("true" if is_sole_type else "false")),
                "is_purgeable":str(r.get('is_purgeable') or ''),"matched_reference_name":mrn,"matched_reference_category":mrc,
                "suggested_exclude":sug,"suggestion_reasons":"|".join(reasons),"excluded":"false","reviewed_by":"","override_reason":""}
                out.append(rec); all_rows.append(rec)
            adjusted_total = tc - known_builtin_count
            authority_rows.append({
                "schema_version": pol.get('schema_version','1.0'),
                "domain": d,
                "file_id": fid,
                "governance_role": (file_roles.get(fid, "") or "unknown"),
                "total_count": str(tc),
                "known_builtin_count": str(known_builtin_count),
                "adjusted_total": str(adjusted_total),
                "purgeable_count": str(pc),
                "purgeable_pct": ("" if tc == 0 else f"{(pc/float(tc)):.6f}"),
                "sole_type_zero_instance_count": str(sole_zero_count),
                "domain_authority": "unknown",
                "authority_reason": "",
                "gap_threshold_used": "",
                "gap_threshold_source": "none",
                "override_authority": "",
                "reviewed_by": "",
                "override_reason": "",
            })

        remaining=[]
        for rec in authority_rows:
            if rec["adjusted_total"] == "1" and rec["sole_type_zero_instance_count"] == "1":
                rec["domain_authority"] = "false"
                rec["authority_reason"] = "sole_placeholder"
                rec["gap_threshold_source"] = "sole_placeholder"
            else:
                remaining.append(rec)
        threshold = _choose_threshold([int(r["adjusted_total"]) for r in remaining]) if remaining else None
        if remaining:
            if threshold is None:
                for rec in remaining:
                    rec["domain_authority"] = "unknown"
                    rec["authority_reason"] = "single_file"
                    rec["gap_threshold_source"] = "none"
            else:
                for rec in remaining:
                    adj = int(rec["adjusted_total"])
                    rec["gap_threshold_used"] = str(threshold)
                    rec["gap_threshold_source"] = "largest_gap"
                    if adj <= threshold:
                        rec["domain_authority"] = "false"
                        rec["authority_reason"] = "below_gap"
                    else:
                        rec["domain_authority"] = "true"
                        rec["authority_reason"] = "above_gap"
        for rec in authority_rows:
            ov = existing_overrides.get((rec["domain"], rec["file_id"]))
            if ov:
                rec.update(ov)
        all_authority_rows.extend(sorted(authority_rows, key=lambda r: (r["domain"], r["file_id"])))

        atomic_write_csv(a.out_dir/f'placeholder_exclusions_{d}.csv',COLS,out)
        atomic_write_csv(a.out_dir/f'domain_authority_by_file_{d}.csv',DOMAIN_AUTHORITY_COLS,sorted(authority_rows, key=lambda r: (r["domain"], r["file_id"])))
    atomic_write_csv(a.out_dir/'placeholder_exclusions_all.csv',COLS,all_rows)
    atomic_write_csv(a.out_dir/'domain_authority_by_file.csv',DOMAIN_AUTHORITY_COLS,sorted(all_authority_rows, key=lambda r: (r["domain"], r["file_id"])))

if __name__=='__main__':
    main()
