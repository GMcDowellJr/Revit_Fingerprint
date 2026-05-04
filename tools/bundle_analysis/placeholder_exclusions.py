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


def t(v): return str(v or "").strip().lower() in {"1","true","yes","y","t"}

def lg(vals):
    vals=sorted([v for v in vals if v>0])
    if len(vals)<2:return None
    bg=0;thr=None
    for a,b in zip(vals,vals[1:]):
        g=b-a
        if g>bg: bg=g;thr=(a+b)/2.0
    return thr if bg>=0.30 else None


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
    a=ap.parse_args()
    rows=read_csv_rows(a.phase0_dir/'phase0_records.csv')
    pol=json.loads((a.policies_dir/'placeholder_known_defaults.json').read_text())
    pdom=pol.get('domains',{})
    by=defaultdict(list)
    for r in rows:
        d=(r.get('domain') or '').strip()
        if d in DOMAINS: by[d].append(r)
    all_rows=[]
    a.out_dir.mkdir(parents=True,exist_ok=True)
    for d in DOMAINS:
        drows=by[d]
        per=defaultdict(list)
        for r in drows:
            fid = (r.get("export_run_id") or "").strip() or (r.get("file_id") or "").strip()
            per[fid].append(r)
        file_pct={fid:(sum(1 for x in rs if t(x.get('is_purgeable')))/float(len(rs)) if rs else 0.0) for fid,rs in per.items()}
        thr=lg(list(file_pct.values()))
        out=[]
        cfg=pdom.get(d,{})
        kd=[x.lower() for x in cfg.get('known_defaults',[])]
        kb=[x.lower() for x in cfg.get('known_builtins',[])]
        pats=[re.compile(p) for p in cfg.get('placeholder_patterns',[])]
        for fid,rs in sorted(per.items()):
            tc=len(rs); pc=sum(1 for x in rs if t(x.get('is_purgeable')))
            for r in rs:
                nm=(r.get('label_display') or '').strip(); nml=nm.lower(); reasons=[]; mrn='';mrc=''
                is_builtin=nml in kb
                if is_builtin: reasons.append('known_builtin'); mrn=nm; mrc='known_builtins'
                if t(r.get('is_purgeable')): reasons.append('is_purgeable')
                instance_count_raw = (r.get("instance_count", "") or "").strip()
                try:
                    instance_count = int(instance_count_raw) if instance_count_raw not in ("", "None", "none") else None
                except (ValueError, TypeError):
                    instance_count = None
                is_sole_raw = ((r.get("is_sole_type_in_category") or r.get("is_sole_type") or "")).strip().lower()
                if is_sole_raw == "true":
                    is_sole_type = True
                elif is_sole_raw == "false":
                    is_sole_type = False
                else:
                    is_sole_type = None
                if is_sole_type is True and instance_count == 0:
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
        atomic_write_csv(a.out_dir/f'placeholder_exclusions_{d}.csv',COLS,out)
    atomic_write_csv(a.out_dir/'placeholder_exclusions_all.csv',COLS,all_rows)

if __name__=='__main__':
    main()
