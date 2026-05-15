from __future__ import annotations
import argparse,csv,hashlib,re,sys
from collections import defaultdict
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict,Iterable,List,Sequence

SEED_ROLES={"Template","Container"}
REQUIRED_COLUMNS={"export_run_id","unit_system","client_label","governance_role"}
MANIFEST_FIELDNAMES=["segment_id","parent_segment_id","segment_level","unit_system","governance_role","client_label","run_type","file_count","export_run_ids","has_seed_file","seed_export_run_ids","population_hash","notes","segment_purpose","segment_label"]
REGISTRY_FIELDNAMES=["segment_id","parent_segment_id","run_type","population_hash","output_folder","status","last_run_utc","notes","segment_purpose","segment_label"]

def _read_csv(path: Path) -> tuple:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = [{str(k): ("" if v is None else str(v)) for k, v in row.items()} for row in reader]
        fieldnames = list(reader.fieldnames or [])
    return fieldnames, rows

def _atomic_write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", newline="", delete=False, dir=str(path.parent), suffix=".tmp") as tmp:
        tmp_path = Path(tmp.name)
        writer = csv.DictWriter(tmp, fieldnames=list(fieldnames));writer.writeheader()
        for row in rows: writer.writerow({name: row.get(name, "") for name in fieldnames})
    tmp_path.replace(path)

def _population_hash(export_run_ids: List[str]) -> str:
    token="|".join(sorted(export_run_ids));return hashlib.sha1(token.encode()).hexdigest()

_UNSAFE_FOLDER_CHARS = re.compile(r'[|/\\:*?"<>\s]+')
def _sanitize_folder(segment_id:str)->str:return _UNSAFE_FOLDER_CHARS.sub("_",segment_id).lower().strip("_")

def _append_note(row,k,v=""):
    note=f"{k}:{v}" if v else k
    if row.get("notes"): row["notes"] += f"|{note}"
    else: row["notes"]=note

def _build_segments(rows:List[Dict[str,str]],min_files:int,enable_cross_org_template_bundles:bool=False,enable_parent_bundle_runs:bool=False)->List[Dict[str,str]]:
    l1=defaultdict(list); l1s=defaultdict(list)
    l2a=defaultdict(list); l2as=defaultdict(list)
    l2b=defaultdict(list); l2bs=defaultdict(list)
    l3=defaultdict(list); l3s=defaultdict(list)
    client_roles=defaultdict(set)
    role_presence_by_l2: Dict[str, set] = defaultdict(set)
    project_presence_by_l2: Dict[str, bool] = defaultdict(bool)
    for row in rows:
        u=(row.get("unit_system") or "").strip(); c=(row.get("client_label") or "").strip(); r=(row.get("governance_role") or "").strip(); e=(row.get("export_run_id") or "").strip()
        if not u or not e: continue
        l1[u].append(e); l2a[(u,r)].append(e)
        l2b[(u,c)].append(e)
        if c:
            l3[(u,r,c)].append(e)
        client_roles[(u,c)].add(r)
        l2_seg_id = f"{u}|{c}"
        if r:
            role_presence_by_l2[l2_seg_id].add(r)
        if r == "Project":
            project_presence_by_l2[l2_seg_id] = True
        if r in SEED_ROLES:
            l1s[u].append(e); l2as[(u,r)].append(e)
            if c:
                l2bs[(u,c)].append(e); l3s[(u,r,c)].append(e)
            else:
                l2bs[(u, c)].append(e)
    m=[]
    def mk(seg,parent,lev,u,r,c,eids,seeds):
        eids=sorted(set(eids)); seeds=sorted(set(seeds))
        m.append({"segment_id":seg,"parent_segment_id":parent,"segment_level":str(lev),"unit_system":u,"governance_role":r,"client_label":c,"run_type":"","file_count":str(len(eids)),"export_run_ids":"|".join(eids),"has_seed_file":"true" if seeds else "false","seed_export_run_ids":"|".join(seeds),"population_hash":_population_hash(eids),"notes":"","segment_purpose":"","segment_label":""})
    for u in sorted(l1): mk(u,"",1,u,"","",l1[u],l1s[u])
    for (u,r) in sorted(l2a): mk(f"{u}|{r}",u,2,u,r,"",l2a[(u,r)],l2as[(u,r)])
    for (u,c) in sorted(l2b):
        mk(f"{u}|{c}",u,2,u,"",c,l2b[(u,c)],l2bs[(u,c)])
    for (u,r,c) in sorted(l3): mk(f"{u}|{r}|{c}",f"{u}|{r}",3,u,r,c,l3[(u,r,c)],l3s[(u,r,c)])
    byid={r["segment_id"]:r for r in m}; kids=defaultdict(list)
    for r in m:
        if r["parent_segment_id"]: kids[r["parent_segment_id"]].append(r)
    # level-3 members per (unit, client) — used to detect children of unit|client segments
    l3_by_uc: Dict[tuple, List[dict]] = defaultdict(list)
    for r in m:
        if r["segment_level"] == "3" and r["unit_system"] and r["client_label"]:
            l3_by_uc[(r["unit_system"], r["client_label"])].append(r)
    # pass3 run type
    for r in m:
        fc=int(r["file_count"]); role=r["governance_role"]
        notes = []
        if fc < min_files:
            notes.append("below_min_files")
        if r["segment_level"] == "2" and r["has_seed_file"] == "true":
            if not role and not project_presence_by_l2.get(r["segment_id"], False):
                notes.append("seed_only")
            elif role and role != "Project":
                notes.append("seed_only")
        if notes:
            r["notes"] = "|".join(notes)
        seg = r["segment_id"]
        lev = r["segment_level"]
        cl = r["client_label"]
        role = r["governance_role"]
        if lev == "2" and not role and cl:
            # unit|client segments: their level-3 children parent to unit|role,
            # not unit|client, so they never appear in kids. Use l3_by_uc instead.
            has = bool(l3_by_uc.get((r["unit_system"], cl)))
        else:
            has = bool(kids.get(seg))
        if has:
            # Determine whether this segment qualifies for bundle analysis despite having children
            is_cross_org_template = (
                enable_cross_org_template_bundles
                and r["segment_level"] == "2"
                and r["governance_role"] == "Template"
                and not r["client_label"]
            )
            is_role_fixed_parent = (
                enable_parent_bundle_runs
                and r["segment_level"] == "2"
                and r["governance_role"] != ""
                and not r["client_label"]
                and fc >= min_files
            )
            if not is_cross_org_template and not is_role_fixed_parent:
                r["run_type"] = "registration"; continue
            # else: fall through to normal bundle/reference assignment below
        if fc>=min_files: r["run_type"]="bundle"
        elif role in {"Template","Container","Generic"}: r["run_type"]="reference"
        elif role=="Project": r["run_type"]="skip"
        elif role == "":
            r["run_type"] = "registration"
        else: r["run_type"]="registration"
    # purpose/label
    def child_span(r):
        cs={c["client_label"] for c in kids.get(r["segment_id"],[]) if c["segment_level"]=="3" and c["client_label"]}
        return "multi_client" if len(cs)>1 else "single_client"
    for r in m:
        pur="insufficient_population" if r["run_type"]=="skip" else ""
        lev,role,rt=int(r["segment_level"]),r["governance_role"],r["run_type"]
        if lev==1: pur="population_denominator"
        elif lev == 2 and r["client_label"] and not role:
            pur = "client_population"
        elif lev==2 and role=="Template":
            if rt=="bundle": pur="cross_template_agreement"
            elif rt in {"registration","reference"}: pur="cross_org_template_pool" if child_span(r)=="multi_client" else "redundant_single_child"
        elif lev==2 and role=="Project": pur="cross_project_practice" if rt=="bundle" else "practiced_standards_corpus"
        elif lev==2 and role=="Container": pur="coordination_corpus"
        elif lev==2 and role=="Generic" and rt=="reference": pur="generic_reference_corpus"
        elif lev==3 and role=="Template" and rt in {"bundle","reference"}: pur="client_standard_anchor"
        elif lev==3 and role=="Project": pur="client_practice" if rt=="bundle" else "insufficient_population"
        elif lev==3 and role=="Container" and rt=="reference": pur="client_coordination"
        elif lev==3 and role=="Generic" and rt=="reference": pur="client_reference"
        r["segment_purpose"]=pur or "population_denominator"
        unit=r["unit_system"].title(); client=r["client_label"]; sid=r["segment_id"]
        templates={"population_denominator":f"All {unit} files","cross_org_template_pool":f"{unit} templates — all organisations (registration only)","cross_template_agreement":f"{unit} templates — cross-template agreement","practiced_standards_corpus":f"{unit} projects — full corpus","cross_project_practice":f"{unit} projects — cross-project practice","coordination_corpus":f"{unit} coordination files","generic_reference_corpus":f"{unit} generic reference","client_population":f"{client} — all roles combined","client_standard_anchor":f"{client} templates — standards as authored","client_practice":f"{client} projects — standards as practiced","client_coordination":f"{client} coordination files","client_reference":f"{client} generic reference","insufficient_population":f"{sid} — below minimum file threshold"}
        r["segment_label"]=templates.get(r["segment_purpose"],sid)
    # pass5 redundant hash
    for r in m:
        if r["run_type"] not in {"bundle", "registration", "reference"}: continue
        _seg = r["segment_id"]
        _lev = r["segment_level"]
        _cl = r["client_label"]
        _role = r["governance_role"]
        if _lev == "2" and not _role and _cl:
            direct_children = l3_by_uc.get((r["unit_system"], _cl), [])
        else:
            direct_children = kids.get(_seg, [])
        matches = [c for c in direct_children if c["population_hash"] == r["population_hash"]]
        if len(direct_children) == 1 and len(matches) == 1:
            ch=matches[0]["segment_id"]; _append_note(r,"redundant_single_child",ch)
            r["run_type"]="registration"; r["segment_purpose"]="redundant_single_child"; r["segment_label"]=f"{r['segment_id']} — same population as {ch}"
    m.sort(key=lambda r:(int(r["segment_level"]),r["segment_id"]))
    return m

# preserve remaining functions from original manually omitted

def _build_registry(manifest_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    registry = []
    assigned_folders: set = set()
    for row in manifest_rows:
        if row["run_type"] not in {"bundle", "reference"}:
            continue
        base = _sanitize_folder(row["segment_id"])
        folder = base
        n = 2
        while folder in assigned_folders:
            folder = f"{base}_{n}"
            n += 1
        assigned_folders.add(folder)
        registry.append({
            "segment_id": row["segment_id"],
            "parent_segment_id": row["parent_segment_id"],
            "run_type": row["run_type"],
            "population_hash": row["population_hash"],
            "output_folder": folder,
            "status": "pending",
            "last_run_utc": "",
            "notes": row.get("notes", ""),
            "segment_purpose": row.get("segment_purpose", ""),
            "segment_label": row.get("segment_label", ""),
        })
    return registry


def _print_summary(
    manifest_path: Path,
    registry_path: Path,
    manifest_rows: List[Dict[str, str]],
    min_files: int,
) -> None:
    bundles = [r for r in manifest_rows if r["run_type"] == "bundle"]
    refs = [r for r in manifest_rows if r["run_type"] == "reference"]
    skips = [r for r in manifest_rows if r["run_type"] == "skip"]
    regs = [r for r in manifest_rows if r["run_type"] == "registration"]

    print(f"Segment manifest written: {manifest_path}")
    print(f"Run registry written: {registry_path}")
    print()
    print(f"Run plan ({len(bundles) + len(refs)} segments):")

    print("\n  Bundle runs:")
    for r in bundles:
        print(f"    {r['segment_label']} [{r['segment_purpose']}]  ({r['segment_id']}, {r['file_count']} files)")

    print("\n  Reference runs:")
    for r in refs:
        print(f"    {r['segment_label']} [{r['segment_purpose']}]  ({r['segment_id']}, {r['file_count']} files)")

    if skips:
        print(f"\n  Skipped (below min_files={min_files}):")
        for r in skips:
            print(f"    {r['segment_label']} [{r['segment_purpose']}]  ({r['segment_id']}, {r['file_count']} files)")

    if regs:
        print("\n  Registration only (hierarchy anchors):")
        for r in regs:
            print(f"    {r['segment_label']} [{r['segment_purpose']}]  ({r['segment_id']}, {r['file_count']} files)")


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build segment_manifest.csv and run_registry.csv from file_metadata.csv.",
    )
    parser.add_argument("--metadata-file", required=True, help="Path to file_metadata.csv")
    parser.add_argument("--out-dir", required=True, help="Directory to write output files")
    parser.add_argument("--min-files", type=int, default=3, help="Minimum file count for a segment (default: 3)")
    parser.add_argument("--enable-cross-org-template-bundles", action="store_true", help="Allow cross-org level-2 Template segments to run as bundle/reference")
    parser.add_argument(
        "--enable-parent-bundle-runs",
        action="store_true",
        help="Allow level-2 role-fixed segments (e.g. imperial|Project) to run bundle analysis even when they have child segments."
    )
    args = parser.parse_args(argv)

    metadata_path = Path(args.metadata_file)
    if not metadata_path.is_file():
        sys.stderr.write(f"[ERROR] --metadata-file not found: {metadata_path}\n")
        return 1

    out_dir = Path(args.out_dir)
    min_files: int = args.min_files

    fieldnames, rows = _read_csv(metadata_path)
    # Validate headers unconditionally — even a header-only file must declare the required columns.
    if not fieldnames:
        sys.stderr.write(f"[WARN] file_metadata.csv is completely empty (no header): {metadata_path}\n")
    else:
        missing_columns = REQUIRED_COLUMNS - set(fieldnames)
        if missing_columns:
            sys.stderr.write(
                f"[ERROR] file_metadata.csv is missing required columns: {sorted(missing_columns)}\n"
            )
            return 1
        if not rows:
            sys.stderr.write(f"[WARN] file_metadata.csv has a valid header but no data rows: {metadata_path}\n")

    skipped_blank_us = sum(1 for r in rows if not (r.get("unit_system") or "").strip())
    if skipped_blank_us:
        sys.stderr.write(f"[WARN] Excluded {skipped_blank_us} row(s) with blank unit_system\n")

    skipped_blank_eid = sum(
        1 for r in rows
        if (r.get("unit_system") or "").strip()      # unit_system present (not already counted above)
        and not (r.get("export_run_id") or "").strip()
    )
    if skipped_blank_eid:
        sys.stderr.write(f"[WARN] Excluded {skipped_blank_eid} row(s) with blank export_run_id\n")

    KNOWN_ROLES = {"Project", "Template", "Container", "Generic", ""}
    unknown_roles = {
        (r.get("governance_role") or "").strip()
        for r in rows
        if (r.get("governance_role") or "").strip() not in KNOWN_ROLES
    }
    for role in sorted(unknown_roles):
        sys.stderr.write(f"[WARN] Unrecognised governance_role value in metadata: '{role}' — rows with this role will create unexpected segments\n")

    manifest_rows = _build_segments(rows, min_files, args.enable_cross_org_template_bundles, args.enable_parent_bundle_runs)

    for r in manifest_rows:
        if r["run_type"] == "bundle" and int(r["file_count"]) < min_files:
            sys.stderr.write(f"[WARN] Bundle below min_files: {r['segment_id']} ({r['file_count']} < {min_files})\n")

    ids = {r["segment_id"] for r in manifest_rows}
    for r in manifest_rows:
        if r["segment_level"] == "3" and r["parent_segment_id"] not in ids:
            sys.stderr.write(f"[WARN] Orphaned level-3 segment missing parent: {r['segment_id']} -> {r['parent_segment_id']}\n")

    bundle_by_hash = defaultdict(list)
    for r in manifest_rows:
        if r["run_type"] == "bundle":
            bundle_by_hash[r["population_hash"]].append(r["segment_id"])
    for pop_hash, segs in bundle_by_hash.items():
        if len(segs) > 1:
            sys.stderr.write(f"[WARN] Duplicate bundle population_hash {pop_hash}: {', '.join(sorted(segs))}\n")

    registry_rows = _build_registry(manifest_rows)

    manifest_path = out_dir / "segment_manifest.csv"
    registry_path = out_dir / "run_registry.csv"

    _atomic_write_csv(manifest_path, MANIFEST_FIELDNAMES, manifest_rows)
    _atomic_write_csv(registry_path, REGISTRY_FIELDNAMES, registry_rows)

    _print_summary(manifest_path, registry_path, manifest_rows, min_files)
    return 0


if __name__ == "__main__":
    sys.exit(main())
