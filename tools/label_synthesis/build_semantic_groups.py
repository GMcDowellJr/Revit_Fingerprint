#!/usr/bin/env python3
"""
Build semantic group labels for selected analysis domains.

This tool reads resolved pattern labels plus representative behavioral properties,
then calls an LLM (one call per pattern) to assign a governance-intent
`semantic_group` label. Results are cached in:

    Results_v21/label_synthesis/label_semantic_groups.json
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

SEMANTIC_GROUPING_DOMAINS = [
    "text_types",
    "arrowheads",
    "line_patterns",
    "line_styles",
    "fill_patterns_drafting",
    "fill_patterns_model",
]

CACHE_SCHEMA_VERSION = "1.0"


def build_grouping_prompt(
    domain: str,
    pattern_label_human: str,
    behavioral_props: dict[str, str],
    peer_group_labels: list[str],
) -> str:
    raise NotImplementedError("Prompt 3 — LLM instruction — not yet written")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _load_analysis_run_id(analysis_dir: Path) -> str:
    manifest = analysis_dir / "analysis_manifest.csv"
    if not manifest.is_file():
        return ""
    rows = _read_csv_rows(manifest)
    if not rows:
        return ""
    return (rows[0].get("analysis_run_id") or "").strip()


def _load_cache(cache_path: Path) -> Dict[str, Any]:
    if not cache_path.is_file():
        return {
            "schema_version": CACHE_SCHEMA_VERSION,
            "analysis_run_id": "",
            "generated_at": "",
            "groups": {},
        }
    with cache_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        return {"schema_version": CACHE_SCHEMA_VERSION, "analysis_run_id": "", "generated_at": "", "groups": {}}
    data.setdefault("schema_version", CACHE_SCHEMA_VERSION)
    data.setdefault("analysis_run_id", "")
    data.setdefault("generated_at", "")
    groups = data.get("groups")
    data["groups"] = groups if isinstance(groups, dict) else {}
    return data


def _save_cache(cache_path: Path, cache: Dict[str, Any]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, sort_keys=True, ensure_ascii=False)


def _load_pattern_rows(analysis_dir: Path, only_domain: Optional[str]) -> Dict[str, List[Dict[str, str]]]:
    domain_patterns_csv = analysis_dir / "domain_patterns.csv"
    if not domain_patterns_csv.is_file():
        raise FileNotFoundError(f"Missing required input: {domain_patterns_csv}")
    rows = _read_csv_rows(domain_patterns_csv)
    out: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    stats: Dict[str, int] = defaultdict(int)
    for row in rows:
        domain = (row.get("domain") or "").strip().lower()
        stats["rows_total"] += 1
        if domain not in SEMANTIC_GROUPING_DOMAINS:
            stats["rows_out_of_scope_domain"] += 1
            continue
        if only_domain and domain != only_domain:
            stats["rows_filtered_by_domain_arg"] += 1
            continue
        pattern_id = (row.get("pattern_id") or "").strip()
        label = (row.get("pattern_label_human") or "").strip()
        source = (row.get("pattern_label_source") or "").strip().lower()
        if not pattern_id:
            stats["rows_skipped_missing_pattern_id"] += 1
            continue
        if source == "missing":
            stats["rows_skipped_missing_source"] += 1
            continue
        if not label:
            stats["rows_skipped_blank_pattern_label_human"] += 1
            continue
        stats["rows_eligible"] += 1
        out[domain].append({
            "pattern_id": pattern_id,
            "pattern_label_human": label,
        })
    for domain in list(out.keys()):
        out[domain] = sorted(out[domain], key=lambda r: r["pattern_id"])
    print(f"[build_semantic_groups] domain_patterns scan stats: {dict(stats)}")
    return out


def _load_pattern_to_record_pk(analysis_dir: Path, domain: str) -> Dict[str, str]:
    membership_csv = analysis_dir / "record_pattern_membership.csv"
    if not membership_csv.is_file():
        raise FileNotFoundError(f"Missing required input: {membership_csv}")
    rows = _read_csv_rows(membership_csv)
    out: Dict[str, str] = {}
    for row in rows:
        if (row.get("domain") or "").strip() != domain:
            continue
        pattern_id = (row.get("pattern_id") or "").strip()
        record_pk = (row.get("record_pk") or "").strip()
        if pattern_id and record_pk and pattern_id not in out:
            out[pattern_id] = record_pk
    return out


def _load_identity_items_by_record(shards_dir: Path, domain: str) -> Optional[Dict[str, Dict[str, str]]]:
    shard_csv = shards_dir / f"{domain}.identity_items.csv"
    if not shard_csv.is_file():
        print(f"[build_semantic_groups] WARN: missing shard for domain '{domain}': {shard_csv}")
        return None
    rows = _read_csv_rows(shard_csv)
    out: Dict[str, Dict[str, str]] = defaultdict(dict)
    for row in rows:
        if (row.get("domain") or "").strip() != domain:
            continue
        record_pk = (row.get("record_pk") or "").strip()
        key = (row.get("k") or "").strip()
        value = (row.get("v") or "").strip()
        quality = (row.get("q") or "").strip()
        if not record_pk or not key or quality != "ok":
            continue
        if value:
            out[record_pk][key] = value
    return out


def _line_pattern_segment_keys(items: Dict[str, str]) -> List[str]:
    keys = [k for k in items.keys() if k.startswith("line_pattern.seg[") and k.endswith("].kind")]
    return sorted(keys)


def _is_nullish(value: str) -> bool:
    v = value.strip().lower()
    return v in {"", "none", "null", "nil", "n/a", "na"}


def _extract_behavioral_props(domain: str, items: Dict[str, str]) -> Dict[str, str]:
    props: Dict[str, str] = {}
    if domain == "text_types":
        for k in [
            "text_type.font",
            "text_type.size_in",
            "text_type.bold",
            "text_type.italic",
            "text_type.color_rgb",
            "text_type.show_border",
            "text_type.background_raw",
        ]:
            if items.get(k):
                props[k] = items[k]
    elif domain == "arrowheads":
        for k in [
            "arrowhead.style",
            "arrowhead.tick_size_in",
            "arrowhead.filled",
            "arrowhead.heavy_end_pen_weight",
        ]:
            if items.get(k):
                props[k] = items[k]
    elif domain == "line_patterns":
        if items.get("line_pattern.segment_count"):
            props["line_pattern.segment_count"] = items["line_pattern.segment_count"]
        for seg_key in _line_pattern_segment_keys(items):
            props[seg_key] = items[seg_key]
    elif domain == "line_styles":
        for k in ["line_style.color.rgb", "line_style.weight.projection"]:
            if items.get(k):
                props[k] = items[k]
        sig_hash = items.get("line_style.pattern_ref.sig_hash", "")
        if _is_nullish(sig_hash):
            props["line pattern"] = "[solid]"
        else:
            props["line pattern"] = sig_hash
    elif domain in {"fill_patterns_drafting", "fill_patterns_model"}:
        for k in ["fill_pattern.is_solid", "fill_pattern.grid_count"]:
            if items.get(k):
                props[k] = items[k]
    return props


def _parse_grouping_response(raw_text: str) -> Dict[str, str]:
    text = raw_text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[-1]
        if text.endswith("```"):
            text = text[: text.rfind("```")]
    try:
        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            raise ValueError("Expected object")
        semantic_group = str(parsed.get("semantic_group", "")).strip()
        confidence = str(parsed.get("confidence", "low")).strip().lower()
        rationale = str(parsed.get("rationale", "")).strip()
        if confidence not in {"high", "medium", "low"}:
            confidence = "low"
        if not semantic_group:
            semantic_group = "__parse_error__"
            confidence = "low"
            rationale = rationale or "Missing semantic_group in LLM response."
        return {
            "semantic_group": semantic_group,
            "confidence": confidence,
            "rationale": rationale,
        }
    except Exception:
        return {
            "semantic_group": "__parse_error__",
            "confidence": "low",
            "rationale": raw_text.strip(),
        }


def _call_grouping_llm(prompt: str) -> str:
    raise NotImplementedError("LLM call wiring for semantic grouping is not implemented yet.")


def build_semantic_groups(
    *,
    out_root: Path,
    domain: Optional[str],
    dry_run: bool,
    force_refresh: bool,
    max_patterns: Optional[int],
) -> None:
    if (out_root / "analysis_v21").is_dir() and (out_root / "phase0_v21").is_dir():
        results_v21 = out_root
    else:
        results_v21 = out_root / "Results_v21"
    analysis_dir = results_v21 / "analysis_v21"
    shards_dir = results_v21 / "phase0_v21" / "identity_items_shards"
    cache_path = results_v21 / "label_synthesis" / "label_semantic_groups.json"
    print(f"[build_semantic_groups] results_v21={results_v21}")
    print(f"[build_semantic_groups] analysis_dir={analysis_dir}")
    print(f"[build_semantic_groups] shards_dir={shards_dir}")
    print(f"[build_semantic_groups] cache_path={cache_path}")

    if domain and domain not in SEMANTIC_GROUPING_DOMAINS:
        raise ValueError(f"--domain must be one of {SEMANTIC_GROUPING_DOMAINS}")

    cache = _load_cache(cache_path)
    cache_groups = cache.get("groups", {})
    if not isinstance(cache_groups, dict):
        cache_groups = {}
        cache["groups"] = cache_groups

    analysis_run_id = _load_analysis_run_id(analysis_dir)
    patterns_by_domain = _load_pattern_rows(analysis_dir, domain)
    if not patterns_by_domain:
        print("[build_semantic_groups] WARN: no eligible patterns found in scope.")
        print("[build_semantic_groups] Check --out-root and ensure domain_patterns.csv has non-missing pattern_label_human/source.")
    for d in SEMANTIC_GROUPING_DOMAINS:
        if domain and d != domain:
            continue
        cache_groups.setdefault(d, {})

    for d, pattern_rows in patterns_by_domain.items():
        if not pattern_rows:
            continue
        print(f"[build_semantic_groups] domain={d} eligible_patterns={len(pattern_rows)}")
        pattern_to_record = _load_pattern_to_record_pk(analysis_dir, d)
        identity_by_record = _load_identity_items_by_record(shards_dir, d)
        if identity_by_record is None:
            continue

        print(f"[build_semantic_groups] domain={d} patterns={len(pattern_rows)}")
        processed = 0
        assigned_this_run: List[str] = []

        for row in pattern_rows:
            pattern_id = row["pattern_id"]
            pattern_label_human = row["pattern_label_human"]
            if not force_refresh and pattern_id in cache_groups[d]:
                continue
            if max_patterns is not None and processed >= max_patterns:
                break

            record_pk = pattern_to_record.get(pattern_id, "")
            identity_items = identity_by_record.get(record_pk, {}) if record_pk else {}
            behavioral_props = _extract_behavioral_props(d, identity_items)
            peer_group_labels = sorted({g for g in assigned_this_run if g})

            if dry_run:
                print("\n--- semantic grouping prompt (dry-run) ---")
                print(json.dumps({
                    "domain": d,
                    "pattern_id": pattern_id,
                    "pattern_label_human": pattern_label_human,
                    "behavioral_props": behavioral_props,
                    "peer_group_labels": peer_group_labels,
                }, indent=2, ensure_ascii=False))
                response_payload = {
                    "semantic_group": "__dry_run__",
                    "confidence": "low",
                    "rationale": "Dry run; LLM call skipped.",
                }
            else:
                try:
                    prompt = build_grouping_prompt(
                        domain=d,
                        pattern_label_human=pattern_label_human,
                        behavioral_props=behavioral_props,
                        peer_group_labels=peer_group_labels,
                    )
                    raw_response = _call_grouping_llm(prompt)
                    response_payload = _parse_grouping_response(raw_response)
                except NotImplementedError as e:
                    response_payload = {
                        "semantic_group": "__parse_error__",
                        "confidence": "low",
                        "rationale": str(e),
                    }

            cache_groups[d][pattern_id] = {
                "semantic_group": response_payload["semantic_group"],
                "confidence": response_payload["confidence"],
                "rationale": response_payload["rationale"],
                "pattern_label_human": pattern_label_human,
                "reviewed": False,
            }
            group_value = response_payload["semantic_group"]
            if group_value and group_value != "__parse_error__":
                assigned_this_run.append(group_value)
            processed += 1

        print(f"[build_semantic_groups] domain={d} processed={processed}")

    cache["schema_version"] = CACHE_SCHEMA_VERSION
    cache["analysis_run_id"] = analysis_run_id
    cache["generated_at"] = _utc_now_iso()
    cache["groups"] = cache_groups
    _save_cache(cache_path, cache)
    print(f"[build_semantic_groups] wrote cache: {cache_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Build semantic group labels for selected pattern domains.")
    ap.add_argument("--out-root", required=True, help="Path containing Results_v21/")
    ap.add_argument("--domain", choices=SEMANTIC_GROUPING_DOMAINS, default=None, help="Optional single domain.")
    ap.add_argument("--dry-run", action="store_true", help="Print prompt inputs; do not call LLM API.")
    ap.add_argument("--force-refresh", action="store_true", help="Regenerate groups even if cached.")
    ap.add_argument("--max-patterns", type=int, default=None, help="Limit patterns processed per domain.")
    args = ap.parse_args()

    build_semantic_groups(
        out_root=Path(args.out_root).resolve(),
        domain=args.domain,
        dry_run=bool(args.dry_run),
        force_refresh=bool(args.force_refresh),
        max_patterns=args.max_patterns,
    )


if __name__ == "__main__":
    main()
