#!/usr/bin/env python3
import csv
from pathlib import Path
from collections import Counter, defaultdict

# --- SET THIS ---
OUT_ROOT = Path(r"C:\Users\gmcdowell\Documents\Fingerprint_Out\domain_families\results_allpairs")
# ---------------

phase0_dir = OUT_ROOT / "Results_v21" / "phase0_v21"
items_csv = phase0_dir / "phase0_identity_items.csv"
records_csv = phase0_dir / "phase0_records.csv"

if not items_csv.exists():
    raise SystemExit(f"Missing: {items_csv}")
if not records_csv.exists():
    raise SystemExit(f"Missing: {records_csv}")

with items_csv.open("r", encoding="utf-8-sig", newline="") as f:
    items = list(csv.DictReader(f))

with records_csv.open("r", encoding="utf-8-sig", newline="") as f:
    records = list(csv.DictReader(f))

if not items:
    raise SystemExit("phase0_identity_items.csv is empty")

headers = list(items[0].keys())
print("== identity_items headers ==")
print(headers)

# Detect schema columns
if "k" in headers:
    key_col, val_col, q_col = "k", "v", "q"
elif "item_key" in headers:
    key_col, val_col, q_col = "item_key", "item_value", "item_value_type"
else:
    raise SystemExit("Could not detect key/value/q columns in identity_items")

print(f"\nDetected key/value/q cols: {key_col}/{val_col}/{q_col}")

# Filter line_patterns rows
lp_items = [r for r in items if (r.get("domain") or "").strip() == "line_patterns"]
print(f"\nline_patterns identity rows: {len(lp_items):,}")

# Segment key checks
seg_rows = [r for r in lp_items if "line_pattern.seg[" in (r.get(key_col) or "")]
segment_rows = [r for r in lp_items if "line_pattern.segment[" in (r.get(key_col) or "")]
print(f"segment key rows (seg[...]): {len(seg_rows):,}")
print(f"segment key rows (segment[...]): {len(segment_rows):,}")

# Segment quality distribution
seg_like = [r for r in lp_items if (".seg[" in (r.get(key_col) or "") or ".segment[" in (r.get(key_col) or ""))]
seg_q = Counter((r.get(q_col) or "").strip() for r in seg_like)
print("\nsegment-row quality breakdown:")
for k, v in seg_q.most_common():
    print(f"  {k or '<blank>'}: {v:,}")

# Norm hash rows + quality
norm_rows = [r for r in lp_items if (r.get(key_col) or "").strip() == "line_pattern.segments_norm_hash"]
norm_q = Counter((r.get(q_col) or "").strip() for r in norm_rows)
print(f"\nsegments_norm_hash rows: {len(norm_rows):,}")
print("segments_norm_hash quality breakdown:")
for k, v in norm_q.most_common():
    print(f"  {k or '<blank>'}: {v:,}")

# Empty-value diagnostic among norm rows
empty_norm = sum(1 for r in norm_rows if not (r.get(val_col) or "").strip())
print(f"segments_norm_hash empty values: {empty_norm:,}")

# Record-level presence checks
lp_record_pks = {(r.get("record_pk") or "").strip() for r in records if (r.get("domain") or "").strip() == "line_patterns"}
norm_record_pks = {(r.get("record_pk") or "").strip() for r in norm_rows if (r.get("record_pk") or "").strip()}
missing_norm_pks = sorted(lp_record_pks - norm_record_pks)

print(f"\nline_patterns records: {len(lp_record_pks):,}")
print(f"line_patterns records with norm hash row: {len(norm_record_pks):,}")
print(f"line_patterns records missing norm hash row: {len(missing_norm_pks):,}")
if missing_norm_pks:
    print("sample missing record_pks:")
    for pk in missing_norm_pks[:10]:
        print(" ", pk)

# For a sample of missing/missing-quality records, inspect their segment rows
def sample_problem_records():
    # Pick records where norm row exists but q != ok OR value blank
    bad = []
    by_pk = defaultdict(list)
    for r in norm_rows:
        by_pk[(r.get("record_pk") or "").strip()].append(r)
    for pk, rows_ in by_pk.items():
        for rr in rows_:
            qv = (rr.get(q_col) or "").strip()
            vv = (rr.get(val_col) or "").strip()
            if qv != "ok" or not vv:
                bad.append(pk)
                break
    return bad

bad_pks = sample_problem_records()
print(f"\nrecords where norm hash is non-ok or blank: {len(bad_pks):,}")
for pk in bad_pks[:5]:
    segs = [r for r in lp_items if (r.get("record_pk") or "").strip() == pk and (".seg[" in (r.get(key_col) or "") or ".segment[" in (r.get(key_col) or ""))]
    print(f"\n--- {pk} ---")
    print(f"segment rows: {len(segs)}")
    # show first 8 segment rows
    for r in segs[:8]:
        print(f"  {r.get(key_col)} | q={r.get(q_col)} | v={r.get(val_col)}")

# join_key_schema check on records
lp_records = [r for r in records if (r.get("domain") or "").strip() == "line_patterns"]
schema_ctr = Counter((r.get("join_key_schema") or "").strip() for r in lp_records)
print("\nline_patterns join_key_schema breakdown:")
for k, v in schema_ctr.most_common():
    print(f"  {k or '<blank>'}: {v:,}")

status_ctr = Counter((r.get("join_key_status") or "").strip() for r in lp_records)
print("\nline_patterns join_key_status breakdown:")
for k, v in status_ctr.most_common():
    print(f"  {k or '<blank>'}: {v:,}")

print("\nDone.")