from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd

TOOLS_ROOT = Path(__file__).resolve().parents[1]
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))

from label_synthesis.label_resolver import _try_synopsis


NOISE_TOKENS = {
    "revit",
    "rvt",
    "projects",
    "project",
    "files",
    "documents",
    "desktop",
    "users",
    "stantec",
    "shared",
    "01",
    "02",
    "03",
    "04",
    "05",
}


def _is_unknown(value: Any) -> bool:
    if value is None or pd.isna(value):
        return True
    sval = str(value).strip()
    return not sval or sval.lower() == "unknown"


def _split_common_path_parts(value: Any) -> list[str]:
    if value is None or pd.isna(value):
        return []
    raw = str(value).strip()
    if not raw:
        return []
    return [part.strip() for part in raw.split("|") if part and part.strip()]


def _first_non_noise(parts: list[str]) -> str | None:
    for part in parts:
        if part.strip().lower() not in NOISE_TOKENS:
            return part.strip()
    return None


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or pd.isna(value):
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y", "t"}


def _clean_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def resolve_provenance_label(
    cluster_id: int,
    likely_region: str,
    likely_office: str,
    common_path_parts: list[str],
    date_range: str | None,
) -> tuple[str, str]:
    """Returns (label, source)."""
    del cluster_id, date_range

    top_path = _first_non_noise(common_path_parts)

    if not _is_unknown(likely_region):
        region = str(likely_region).strip()
        if top_path:
            return f"{region} · {top_path}", "region+path"
        return region, "region"

    if not _is_unknown(likely_office):
        return str(likely_office).strip(), "office"

    if top_path:
        return top_path, "path"

    return "Unknown", "unknown"


def _identity_items_from_representatives(representative_items: list[dict]) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in representative_items:
        top_value = row.get("top_value")
        if top_value is None or pd.isna(top_value):
            continue
        value = str(top_value).strip()
        if not value:
            continue
        key = str(row.get("key", "")).strip()
        if not key:
            continue
        items.append({"k": key, "q": "ok", "v": value})
    return items


def _key_suffix(key: str) -> str:
    if "." not in key:
        return key
    return key.split(".", 1)[1]


def _extract_cluster_id(representative_items: list[dict]) -> int | None:
    for row in representative_items:
        value = row.get("cluster_id")
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def resolve_content_label(
    domain: str,
    standard_name: str,
    representative_items: list[dict],
    discriminators_df: pd.DataFrame,
) -> tuple[str, str]:
    """Returns (label, source)."""
    del standard_name

    identity_items = _identity_items_from_representatives(representative_items)
    synopsis = _try_synopsis(domain, identity_items)
    if synopsis:
        synopsis_text = str(synopsis).strip()
        if synopsis_text:
            return synopsis_text, "synopsis"

    discriminator_keys: set[str] = set()
    if {"key", "is_discriminator"}.issubset(discriminators_df.columns):
        discr_rows = discriminators_df.loc[discriminators_df["is_discriminator"].map(_parse_bool), "key"]
        discriminator_keys = {str(v).strip() for v in discr_rows.tolist() if str(v).strip()}

    disc_rows: list[dict] = []
    for row in representative_items:
        row_is_discriminator = _parse_bool(row.get("is_discriminator"))
        key = str(row.get("key", "")).strip()
        if not row_is_discriminator and key not in discriminator_keys:
            continue
        top_value = row.get("top_value")
        if top_value is None or pd.isna(top_value):
            continue
        top_value_str = str(top_value).strip()
        if not top_value_str or not key:
            continue
        try:
            share = float(row.get("top_value_share", 0.0))
        except (TypeError, ValueError):
            share = 0.0
        disc_rows.append(
            {
                "key": key,
                "top_value": top_value_str,
                "top_value_share": share,
            }
        )

    if disc_rows:
        disc_rows.sort(key=lambda r: r["top_value_share"], reverse=True)
        parts = [f"{_key_suffix(r['key'])}={r['top_value']}" for r in disc_rows[:4]]
        if parts:
            return " · ".join(parts), "discriminators"

    cluster_id = _extract_cluster_id(representative_items)
    if cluster_id is None:
        return "Cluster Unknown", "unknown"
    return f"Cluster {cluster_id}", "unknown"


def _iter_domains(split_root: Path, one_domain: str | None) -> list[str]:
    if one_domain:
        return [one_domain]
    if not split_root.exists():
        return []
    return sorted([p.name for p in split_root.iterdir() if p.is_dir()])


def annotate_cluster_labels(
    split_root: str,
    domain: str,
) -> None:
    split_root_path = Path(split_root)
    domain_root = split_root_path / domain

    summary_path = domain_root / "file_level" / f"{domain}.cluster_summary.csv"
    output_path = domain_root / "file_level" / f"{domain}.cluster_summary_annotated.csv"
    reps_path = domain_root / "intradomain" / f"{domain}.cluster_representative_items.csv"
    discriminators_path = domain_root / "intradomain" / f"{domain}.intradomain_discriminators.csv"

    if not summary_path.exists():
        print(f"[WARN] Skipping domain '{domain}' due to missing cluster summary: {summary_path}")
        return

    summary_df = pd.read_csv(summary_path)

    if reps_path.exists():
        reps_df = pd.read_csv(reps_path)
    else:
        print(f"[WARN] Domain '{domain}': missing representative items ({reps_path}); using unknown content labels.")
        reps_df = pd.DataFrame(columns=["cluster_id", "key", "top_value", "top_value_share", "is_discriminator"])

    if discriminators_path.exists():
        discriminators_df = pd.read_csv(discriminators_path)
    else:
        print(f"[WARN] Domain '{domain}': missing discriminators ({discriminators_path}); using unknown content labels.")
        discriminators_df = pd.DataFrame(columns=["key", "is_discriminator"])

    reps_by_cluster: dict[int, list[dict[str, Any]]] = {}
    if "cluster_id" in reps_df.columns:
        for _, rep_row in reps_df.iterrows():
            try:
                cid = int(rep_row["cluster_id"])
            except (TypeError, ValueError):
                continue
            reps_by_cluster.setdefault(cid, []).append(rep_row.to_dict())

    output_rows: list[dict[str, Any]] = []
    for _, row in summary_df.iterrows():
        row_data = row.to_dict()
        try:
            cluster_id = int(row_data.get("cluster_id"))
        except (TypeError, ValueError):
            cluster_id = -1

        common_parts = _split_common_path_parts(row_data.get("common_path_parts"))
        provenance_label, provenance_source = resolve_provenance_label(
            cluster_id=cluster_id,
            likely_region=_clean_text(row_data.get("likely_region")),
            likely_office=_clean_text(row_data.get("likely_office")),
            common_path_parts=common_parts,
            date_range=None if _is_unknown(row_data.get("date_range")) else str(row_data.get("date_range")),
        )

        rep_items = reps_by_cluster.get(cluster_id, [])
        if not rep_items:
            rep_items = [{"cluster_id": cluster_id}]

        content_label, content_source = resolve_content_label(
            domain=domain,
            standard_name=str(row_data.get("standard_name", "")),
            representative_items=rep_items,
            discriminators_df=discriminators_df,
        )

        row_data["cluster_provenance_label"] = provenance_label
        row_data["cluster_provenance_source"] = provenance_source
        row_data["cluster_content_label"] = content_label
        row_data["cluster_content_source"] = content_source
        output_rows.append(row_data)

    out_df = pd.DataFrame(output_rows)

    tmp_fd, tmp_path = tempfile.mkstemp(
        prefix=f"{output_path.name}.",
        suffix=".tmp",
        dir=str(output_path.parent),
    )
    os.close(tmp_fd)
    try:
        out_df.to_csv(tmp_path, index=False)
        os.replace(tmp_path, output_path)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

    print(f"[INFO] Wrote annotated cluster summary for '{domain}': {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Annotate cluster summaries with provenance/content labels based on "
            "cluster_summary and intradomain representative/discriminator CSVs."
        )
    )
    parser.add_argument(
        "--split-root",
        required=True,
        help="Path to Results_v21/split/",
    )
    parser.add_argument(
        "--domain",
        default=None,
        help="Optional single domain name. If omitted, process all domain directories under --split-root.",
    )

    args = parser.parse_args()

    domains = _iter_domains(Path(args.split_root), args.domain)
    if not domains:
        print(f"[WARN] No domains found under: {args.split_root}")
        return

    processed = 0
    for dom in domains:
        try:
            annotate_cluster_labels(split_root=args.split_root, domain=dom)
            processed += 1
        except Exception as exc:
            print(f"[WARN] Skipping domain '{dom}' due to processing error: {exc}")

    print(f"[INFO] Done. Domains processed: {processed}/{len(domains)}")


if __name__ == "__main__":
    main()
