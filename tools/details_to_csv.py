import argparse
import csv
import json
from pathlib import Path


def _fmt(v):
    return "" if v is None else v


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--details_json", required=True)
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--prefix", default="details")
    args = ap.parse_args()

    details_path = Path(args.details_json)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    data = json.loads(details_path.read_text(encoding="utf-8"))

    comp_csv = out_dir / f"{args.prefix}_comparisons.csv"
    dom_csv = out_dir / f"{args.prefix}_domains.csv"
    topk_csv = out_dir / f"{args.prefix}_sig_topk.csv"

    # 1) comparisons
    with comp_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "file_a", "file_b",
            "domain_hash_identity_jaccard_value", "domain_hash_identity_jaccard_reason",
            "domain_status_layout_jaccard_value", "domain_status_layout_jaccard_reason",
            "signature_multiset_similarity_value", "signature_multiset_similarity_reason",
            "domains_total", "domains_compared_signatures",
            "domains_undefined_blocked", "domains_undefined_unreadable", "domains_missing",
            "note",
        ])

        for row in data:
            s = row.get("summary", {})
            dhi = s.get("domain_hash_identity_jaccard", {})
            dsl = s.get("domain_status_layout_jaccard", {})
            sms = s.get("signature_multiset_similarity", {})

            w.writerow([
                row.get("file_a"), row.get("file_b"),
                _fmt(dhi.get("value")), _fmt(dhi.get("reason")),
                _fmt(dsl.get("value")), _fmt(dsl.get("reason")),
                _fmt(sms.get("value")), _fmt(sms.get("reason")),
                _fmt(s.get("domains_total")), _fmt(s.get("domains_compared_signatures")),
                _fmt(s.get("domains_undefined_blocked")), _fmt(s.get("domains_undefined_unreadable")), _fmt(s.get("domains_missing")),
                _fmt(s.get("note")),
            ])

    # 2) domains
    with dom_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "file_a", "file_b", "domain",
            "status_a", "status_b", "comparable", "reason",
            "set_jaccard_value", "set_jaccard_reason",
            "multiset_jaccard_value", "multiset_jaccard_reason",
            "a_total", "b_total", "matched", "added_in_b", "removed_from_a", "union_mass",
        ])

        for row in data:
            file_a = row.get("file_a")
            file_b = row.get("file_b")
            for d in row.get("domains", []) or []:
                sj = d.get("set_jaccard", {}) or {}
                mj = d.get("multiset_jaccard", {}) or {}
                w.writerow([
                    file_a, file_b, d.get("domain"),
                    d.get("status_a"), d.get("status_b"), d.get("comparable"), _fmt(d.get("reason")),
                    _fmt(sj.get("value")), _fmt(sj.get("reason")),
                    _fmt(mj.get("value")), _fmt(mj.get("reason")),
                    _fmt(d.get("a_total")), _fmt(d.get("b_total")),
                    _fmt(d.get("matched")), _fmt(d.get("added_in_b")),
                    _fmt(d.get("removed_from_a")), _fmt(d.get("union_mass")),
                ])

    # 3) top-k sig hashes
    with topk_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "file_a", "file_b", "domain",
            "list_type", "rank", "sig_hash",
            "label_display", "label_quality", "label_provenance",
            "count",
        ])

        for row in data:
            file_a = row.get("file_a")
            file_b = row.get("file_b")
            for d in row.get("domains", []) or []:
                domain = d.get("domain")
                label_meta = d.get("sig_label_meta") or {}

                for list_type, key in (("matched", "top_matched"), ("added", "top_added"), ("removed", "top_removed")):
                    items = d.get(key)
                    if items is None:
                        continue
                    # items = [(sig_hash, count), ...]
                    for i, (sig_hash, count) in enumerate(items, start=1):
                        meta = label_meta.get(sig_hash, {}) if isinstance(label_meta, dict) else {}
                        disp = meta.get("display", "") if isinstance(meta, dict) else ""
                        qual = meta.get("quality", "") if isinstance(meta, dict) else ""
                        prov = meta.get("provenance", "") if isinstance(meta, dict) else ""

                        w.writerow([
                            file_a,
                            file_b,
                            domain,
                            list_type,
                            i,
                            sig_hash,
                            disp,
                            qual,
                            prov,
                            count,
                        ])


    print(f"Wrote: {comp_csv}")
    print(f"Wrote: {dom_csv}")
    print(f"Wrote: {topk_csv}")


if __name__ == "__main__":
    main()
