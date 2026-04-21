#!/usr/bin/env python3
import csv
import hashlib
import re
from collections import defaultdict, Counter
from pathlib import Path

# ---- EDIT THIS ----
PHASE0_ITEMS = Path(r"C:\Users\gmcdowell\Documents\Fingerprint_Out\domain_families\results_allpairs\Results_v21\phase0_v21\phase0_identity_items.csv")
PRECISIONS = [9, 8, 7, 6, 5, 4]
# -------------------

SEG_RE = re.compile(r"^line_pattern\.(?:seg|segment)\[(\d{3})\]\.(kind|length)$")
SEG_COUNT_KEY = "line_pattern.segment_count"

def md5s(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def detect_cols(fieldnames):
    if "k" in fieldnames:
        return "k", "q", "v"
    if "item_key" in fieldnames:
        return "item_key", "item_value_type", "item_value"
    raise RuntimeError(f"Unknown identity item schema columns: {fieldnames}")

def compute_norm_hash_for_group(rows, key_col, q_col, v_col, decimals):
    # rows all belong to one record_pk
    seg_rows = []
    segments = {}
    for r in rows:
        k = (r.get(key_col) or "").strip()
        m = SEG_RE.match(k)
        if not m:
            continue
        seg_rows.append(r)
        idx = int(m.group(1))
        field = m.group(2)
        segments.setdefault(idx, {})
        if field == "kind":
            try:
                segments[idx]["kind"] = int((r.get(v_col) or "").strip())
            except Exception:
                return "missing", ""
        else:
            try:
                segments[idx]["length"] = float((r.get(v_col) or "").strip())
            except Exception:
                return "missing", ""

    # No segment rows: allow deterministic zero-segment case
    if not seg_rows:
        sc = [r for r in rows if (r.get(key_col) or "").strip() == SEG_COUNT_KEY]
        if sc:
            q = (sc[0].get(q_col) or "").strip()
            vv = (sc[0].get(v_col) or "").strip()
            try:
                is_zero = int(vv) == 0
            except Exception:
                is_zero = False
            if q == "ok" and is_zero:
                return "ok", md5s("segment_count=0")
        return "missing", ""

    # Any non-ok segment row => missing
    for r in seg_rows:
        if (r.get(q_col) or "").strip() != "ok":
            return "missing", ""

    # Must have complete pairs
    ordered = []
    for idx in sorted(segments):
        d = segments[idx]
        if "kind" not in d or "length" not in d:
            return "missing", ""
        ordered.append((idx, int(d["kind"]), float(d["length"])))

    non_dot_total = sum(length for _, kind, length in ordered if kind != 2)
    has_non_dot = any(kind != 2 for _, kind, _ in ordered)
    dot_count = sum(1 for _, kind, _ in ordered if kind == 2)
    eff_total = non_dot_total if has_non_dot else float(dot_count)

    tokens = []
    for idx, kind, length in ordered:
        if kind == 2:
            eff_length = 0.0 if has_non_dot else 1.0
        else:
            eff_length = length
        norm = (eff_length / eff_total) if eff_total > 0 else 0.0
        tokens.append(f"seg[{idx:03d}].kind={kind}")
        tokens.append(f"seg[{idx:03d}].norm_length={norm:.{decimals}f}")

    return "ok", md5s("|".join(tokens))

def main():
    if not PHASE0_ITEMS.exists():
        raise SystemExit(f"Missing file: {PHASE0_ITEMS}")

    with PHASE0_ITEMS.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
        if not rows:
            raise SystemExit("CSV has no rows")
        key_col, q_col, v_col = detect_cols(rows[0].keys())

    lp = [r for r in rows if (r.get("domain") or "").strip() == "line_patterns"]
    by_pk = defaultdict(list)
    for r in lp:
        by_pk[(r.get("record_pk") or "").strip()].append(r)

    print(f"line_patterns records: {len(by_pk):,}")
    print(f"source file: {PHASE0_ITEMS}")
    print(f"detected cols: key={key_col}, q={q_col}, v={v_col}")
    print()

    # Baseline missing count independent of precision
    base_status = Counter()
    for pk, grp in by_pk.items():
        st, _ = compute_norm_hash_for_group(grp, key_col, q_col, v_col, 9)
        base_status[st] += 1

    print("status baseline (using current logic):")
    for k, v in base_status.items():
        print(f"  {k}: {v:,}")
    print()

    print("unique normhash counts by precision (ok rows only):")
    for d in PRECISIONS:
        hashes = []
        ok = 0
        missing = 0
        for pk, grp in by_pk.items():
            st, hv = compute_norm_hash_for_group(grp, key_col, q_col, v_col, d)
            if st == "ok":
                ok += 1
                hashes.append(hv)
            else:
                missing += 1
        uniq = len(set(hashes))
        print(f"  decimals={d}: ok={ok:,} missing={missing:,} unique_ok_hashes={uniq:,}")

if __name__ == "__main__":
    main()