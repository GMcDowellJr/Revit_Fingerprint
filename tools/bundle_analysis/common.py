from __future__ import annotations

import base64
import csv
import hashlib
import math
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, Iterable, List, Sequence

SCHEMA_VERSION = "2.1"
ROW_KEY_DOMAINS = {"object_styles_model", "object_styles_annotation", "view_category_overrides"}
SHAPE_GATED_DOMAINS = {"dimension_types", "arrowheads"}


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [{str(k): "" if v is None else str(v) for k, v in row.items()} for row in csv.DictReader(f)]


def atomic_write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", newline="", delete=False, dir=str(path.parent), suffix=".tmp") as tmp:
        tmp_path = Path(tmp.name)
        writer = csv.DictWriter(tmp, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})
    tmp_path.replace(path)


def resolve_analysis_run_id(rows: Sequence[Dict[str, str]], explicit: str = "") -> str:
    if explicit:
        return explicit
    run_ids = sorted({(row.get("analysis_run_id", "") or "").strip() for row in rows if row.get("analysis_run_id", "")})
    if len(run_ids) != 1:
        raise ValueError(f"Expected exactly one analysis_run_id in input; found {run_ids}")
    return run_ids[0]


def derive_scope_key(domain: str, pattern_meta: Dict[str, str]) -> str:
    if domain in ROW_KEY_DOMAINS:
        return (pattern_meta.get("pattern_label_human", "") or "").strip()
    if domain in SHAPE_GATED_DOMAINS:
        return (pattern_meta.get("join_key_schema", "") or "").strip()
    return ""


def compute_effective_support(files_total: int, min_support_count: int, min_support_pct: float) -> int:
    pct_threshold = int(math.ceil(files_total * (min_support_pct / 100.0)))
    return max(min_support_count, pct_threshold)


def make_bundle_id(domain: str, scope_key: str, pattern_ids_sorted: Sequence[str]) -> str:
    token = f"{domain}|{scope_key}|{'|'.join(pattern_ids_sorted)}"
    digest = hashlib.sha1(token.encode("utf-8")).digest()
    encoded = base64.b32encode(digest).decode("ascii").lower().rstrip("=")
    return f"bnd_{encoded[:16]}"
