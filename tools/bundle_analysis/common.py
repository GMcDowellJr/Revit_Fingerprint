from __future__ import annotations

import base64
import csv
import hashlib
import math
import time
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, Iterable, List, Optional, Sequence

SCHEMA_VERSION = "2.1"
ROW_KEY_DOMAINS = {"object_styles_model", "object_styles_annotation", "view_category_overrides"}
SHAPE_GATED_DOMAINS = {"dimension_types", "arrowheads"}

# csv.field_size_limit() converts to a C long; on Windows CPython the C long
# is 32-bit so sys.maxsize overflows (same fix already applied in
# tools/run_extract_all.py and tools/run_segment_orchestrator.py -- this
# module's own read_csv_rows() was the one reader in the compare/bundle-
# analysis path that never raised it, which is what let a large pipe-joined
# pattern_id-list cell in file_gap_report.csv (see step_compare.py's
# _GAP_FIELDNAMES) exceed the 131072-byte default and crash on re-read).
# Cap at 2^31-1, which fits everywhere; a 32-bit overflow falls back further.
try:
    csv.field_size_limit(2**31 - 1)
except OverflowError:
    csv.field_size_limit(2**30)


def retry_fs_op(op, *args, attempts: int = 5, delay_seconds: float = 1.0) -> None:
    """Run a filesystem-mutating callable (shutil.move / shutil.rmtree), retrying with
    backoff on OSError (covers Windows' PermissionError / WinError 5 "Access is denied").

    A cloud-synced segments root (OneDrive, etc.) can hold a transient lock on a
    just-written file or folder while its sync client is still uploading/indexing it,
    even though nothing in this process holds a handle -- the name-projection bundle leg
    both writes and immediately relocates dozens of small per-domain files in one pass,
    which is exactly the window this can hit. Retrying after a short pause is the
    standard mitigation; a genuine permissions problem still surfaces once every attempt
    is exhausted. Shared by run_bundle_analysis.py's out_dir/name_all relocation and
    run_segment_orchestrator.py's stale-output pre-clean, both of which mutate the same
    segments-root tree."""
    last_exc: Optional[OSError] = None
    for attempt in range(attempts):
        try:
            op(*args)
            return
        except OSError as exc:
            last_exc = exc
            if attempt < attempts - 1:
                time.sleep(delay_seconds * (attempt + 1))
    raise last_exc


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
        label = (pattern_meta.get("pattern_label_human", "") or "").strip()
        return label.split("|")[0].strip() if "|" in label else label
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
