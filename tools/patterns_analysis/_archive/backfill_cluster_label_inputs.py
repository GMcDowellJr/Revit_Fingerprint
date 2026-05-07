from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd


REQUIRED_FILE_LEVEL_FILES = (
    "cluster_summary",
    "file_clustering_report",
)

REQUIRED_INTRADOMAIN_FILES = (
    "intradomain_representative_profiles",
    "intradomain_discriminators",
)


def _file_map(domain_root: Path, domain: str) -> Dict[str, Path]:
    return {
        "cluster_summary": domain_root / "file_level" / f"{domain}.cluster_summary.csv",
        "file_clustering_report": domain_root / "file_level" / f"{domain}.file_clustering_report.json",
        "intradomain_representative_profiles": domain_root
        / "intradomain"
        / f"{domain}.intradomain_representative_profiles.json",
        "intradomain_discriminators": domain_root / "intradomain" / f"{domain}.intradomain_discriminators.csv",
        "cluster_representative_items": domain_root / "intradomain" / f"{domain}.cluster_representative_items.csv",
    }


def _load_json(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _normalize_parts(parts: Any) -> List[str]:
    if not isinstance(parts, list):
        return []
    out: List[str] = []
    for part in parts:
        if part is None:
            continue
        s = str(part).strip()
        if s:
            out.append(s)
    return out


def _common_components_from_paths(paths: List[str]) -> List[str]:
    all_components: List[str] = []
    for path in paths:
        components = str(path).replace("\\", "/").split("/")
        all_components.extend([c.lower() for c in components if c])

    if not all_components:
        return []

    counter = Counter(all_components)
    threshold = len(paths) * 0.5
    common = [comp for comp, count in counter.items() if count >= threshold]
    return sorted(common, key=lambda x: counter[x], reverse=True)


def _extract_cluster_common_path_parts(report: Dict[str, Any]) -> Dict[int, str]:
    """Best-effort extraction of cluster_id -> pipe-delimited common path parts.

    Supports multiple report layouts:
    - cluster entries under report["clusters"]
    - cluster entries under report["cluster_summaries"]
    - nested under report["clustering_parameters"]["clusters"]
    """

    def _extract_parts_from_entry(entry: Dict[str, Any]) -> List[str]:
        metadata_patterns = entry.get("metadata_patterns")
        parts: List[str] = []
        if isinstance(metadata_patterns, dict):
            parts = _normalize_parts(metadata_patterns.get("common_path_parts"))

        if not parts:
            parts = _normalize_parts(entry.get("common_path_parts"))

        if parts:
            return parts

        sample_paths = entry.get("sample_paths")
        if isinstance(metadata_patterns, dict) and not sample_paths:
            sample_paths = metadata_patterns.get("sample_paths")
        if isinstance(sample_paths, list):
            normalized_paths = [str(p) for p in sample_paths if p is not None and str(p).strip()]
            if normalized_paths:
                return _common_components_from_paths(normalized_paths)

        return []

    result: Dict[int, str] = {}

    def _add_cluster_entry(cluster_id_raw: Any, payload: Any) -> None:
        try:
            cluster_id = int(cluster_id_raw)
        except (TypeError, ValueError):
            return

        if isinstance(payload, dict):
            parts = _extract_parts_from_entry(payload)
        else:
            parts = []
        result[cluster_id] = "|".join(parts) if parts else ""

    # Common list-based layouts.
    for candidate in (
        report.get("clusters"),
        report.get("cluster_summaries"),
        (report.get("clustering_parameters") or {}).get("clusters"),
    ):
        if isinstance(candidate, list):
            for entry in candidate:
                if not isinstance(entry, dict):
                    continue
                _add_cluster_entry(entry.get("cluster_id"), entry)

    # Support map-based layouts frequently emitted under clustering_parameters, e.g.
    # {"cluster_metadata_patterns": {"0": {...}, "1": {...}}}
    cp = report.get("clustering_parameters")
    if isinstance(cp, dict):
        for v in cp.values():
            if not isinstance(v, dict):
                continue
            dict_keys = list(v.keys())
            if not dict_keys:
                continue
            numeric_key_share = sum(1 for k in dict_keys if str(k).isdigit()) / float(len(dict_keys))
            if numeric_key_share < 0.8:
                continue
            for cid, payload in v.items():
                _add_cluster_entry(cid, payload)

    # Optionally support a global fallback (same value for all clusters) if present.
    if not result:
        global_parts = _normalize_parts((report.get("clustering_parameters") or {}).get("common_path_parts"))
        if global_parts:
            # Caller will apply this fallback to every cluster_id.
            result[-1] = "|".join(global_parts)

    return result


def _update_cluster_summary(cluster_summary_path: Path, report_path: Path) -> pd.DataFrame:
    summary_df = pd.read_csv(cluster_summary_path)
    report = _load_json(report_path)
    cluster_common_parts = _extract_cluster_common_path_parts(report)

    default_common = cluster_common_parts.get(-1, "")

    parts_by_cluster_id: Dict[int, str] = {
        int(cid): val for cid, val in cluster_common_parts.items() if cid != -1
    }

    if "cluster_id" not in summary_df.columns:
        raise ValueError(f"Missing required 'cluster_id' in {cluster_summary_path}")

    resolved: List[str] = []
    for cid in summary_df["cluster_id"].tolist():
        try:
            key = int(cid)
        except (TypeError, ValueError):
            resolved.append(default_common)
            continue
        resolved.append(parts_by_cluster_id.get(key, default_common))

    original_cols = summary_df.columns.tolist()
    if "common_path_parts" in original_cols:
        summary_df = summary_df.drop(columns=["common_path_parts"])
        original_cols = [c for c in original_cols if c != "common_path_parts"]

    summary_df["common_path_parts"] = resolved
    summary_df = summary_df[original_cols + ["common_path_parts"]]

    # Atomic write: temp file in same directory, then os.replace.
    tmp_fd, tmp_path = tempfile.mkstemp(
        prefix=f"{cluster_summary_path.name}.",
        suffix=".tmp",
        dir=str(cluster_summary_path.parent),
    )
    os.close(tmp_fd)
    try:
        summary_df.to_csv(tmp_path, index=False)
        os.replace(tmp_path, cluster_summary_path)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

    return summary_df


def _build_discriminator_lookup(discriminators_df: pd.DataFrame) -> Dict[str, bool]:
    if "key" not in discriminators_df.columns or "is_discriminator" not in discriminators_df.columns:
        return {}

    lookup: Dict[str, bool] = {}
    for _, row in discriminators_df.iterrows():
        key = str(row["key"])
        raw = row["is_discriminator"]
        if isinstance(raw, bool):
            lookup[key] = raw
        elif pd.isna(raw):
            lookup[key] = False
        else:
            sval = str(raw).strip().lower()
            lookup[key] = sval in {"true", "1", "yes", "y", "t"}
    return lookup


def _build_cluster_representative_items(
    domain: str,
    cluster_summary_df: pd.DataFrame,
    representative_profiles_path: Path,
    discriminators_path: Path,
    out_path: Path,
) -> int:
    profiles = _load_json(representative_profiles_path)
    discriminators_df = pd.read_csv(discriminators_path)
    discriminator_lookup = _build_discriminator_lookup(discriminators_df)

    if "standard_name" not in cluster_summary_df.columns or "cluster_id" not in cluster_summary_df.columns:
        raise ValueError(
            f"cluster_summary for domain '{domain}' missing required columns: standard_name, cluster_id"
        )

    std_to_cluster = (
        cluster_summary_df[["standard_name", "cluster_id"]]
        .drop_duplicates(subset=["standard_name"], keep="first")
        .set_index("standard_name")["cluster_id"]
        .to_dict()
    )

    rows: List[Dict[str, Any]] = []

    for standard_name, payload in profiles.items():
        if standard_name not in std_to_cluster:
            continue

        keys_profile = payload.get("keys_profile") if isinstance(payload, dict) else None
        if not isinstance(keys_profile, dict):
            continue

        cluster_id = int(std_to_cluster[standard_name])

        for key, key_payload in keys_profile.items():
            if not isinstance(key_payload, dict):
                continue
            top_value = key_payload.get("top_value")
            top_share = key_payload.get("top_value_share", 0.0)
            try:
                top_share_float = float(top_share)
            except (TypeError, ValueError):
                top_share_float = 0.0

            rows.append(
                {
                    "cluster_id": cluster_id,
                    "standard_name": str(standard_name),
                    "key": str(key),
                    "top_value": "" if top_value is None else str(top_value),
                    "top_value_share": top_share_float,
                    "is_discriminator": bool(discriminator_lookup.get(str(key), False)),
                }
            )

    out_df = pd.DataFrame(
        rows,
        columns=[
            "cluster_id",
            "standard_name",
            "key",
            "top_value",
            "top_value_share",
            "is_discriminator",
        ],
    )
    out_df.to_csv(out_path, index=False)
    return len(out_df)


def _validate_domain_inputs(domain_root: Path, domain: str) -> Tuple[bool, Dict[str, Path]]:
    file_map = _file_map(domain_root, domain)
    required_keys = REQUIRED_FILE_LEVEL_FILES + REQUIRED_INTRADOMAIN_FILES
    missing = [k for k in required_keys if not file_map[k].exists()]
    if missing:
        missing_paths = ", ".join(str(file_map[m]) for m in missing)
        print(f"[WARN] Skipping domain '{domain}' due to missing inputs: {missing_paths}")
        return False, file_map
    return True, file_map


def _iter_domains(split_root: Path, one_domain: str | None) -> List[str]:
    if one_domain:
        return [one_domain]

    if not split_root.exists():
        return []

    return sorted([p.name for p in split_root.iterdir() if p.is_dir()])


def run_backfill(split_root: Path, domain: str | None = None) -> None:
    domains = _iter_domains(split_root, domain)
    if not domains:
        print(f"[WARN] No domains found under: {split_root}")
        return

    processed = 0
    for dom in domains:
        domain_root = split_root / dom
        ok, file_map = _validate_domain_inputs(domain_root, dom)
        if not ok:
            continue

        try:
            updated_summary_df = _update_cluster_summary(
                cluster_summary_path=file_map["cluster_summary"],
                report_path=file_map["file_clustering_report"],
            )
            item_rows = _build_cluster_representative_items(
                domain=dom,
                cluster_summary_df=updated_summary_df,
                representative_profiles_path=file_map["intradomain_representative_profiles"],
                discriminators_path=file_map["intradomain_discriminators"],
                out_path=file_map["cluster_representative_items"],
            )
            processed += 1
            print(
                f"[INFO] Processed domain '{dom}': updated cluster_summary and wrote "
                f"{file_map['cluster_representative_items'].name} ({item_rows} rows)."
            )
        except Exception as exc:
            print(f"[WARN] Skipping domain '{dom}' due to processing error: {exc}")

    print(f"[INFO] Done. Domains processed: {processed}/{len(domains)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill cluster label inputs from existing split-domain artifacts: "
            "append common_path_parts to cluster_summary and emit cluster_representative_items CSV."
        )
    )
    parser.add_argument(
        "--split-root",
        required=True,
        type=Path,
        help="Path to results/split/",
    )
    parser.add_argument(
        "--domain",
        default=None,
        help="Optional single domain name. If omitted, process all domain directories under --split-root.",
    )

    args = parser.parse_args()
    run_backfill(split_root=args.split_root, domain=args.domain)


if __name__ == "__main__":
    main()
