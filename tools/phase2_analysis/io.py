from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional
import csv
from pathlib import Path
from typing import Set, Tuple


@dataclass(frozen=True)
class ExportFile:
    """One exported fingerprint JSON treated as one authority sample."""

    path: str
    file_id: str
    data: Dict[str, Any]


def _ordered_export_names(root_dir: str) -> List[str]:
    """Return export JSON names in priority order: fingerprint, then plain fallback."""
    names = [
        n
        for n in os.listdir(root_dir)
        if n.lower().endswith(".json") and not n.lower().endswith(".legacy.json")
    ]
    names.sort(key=lambda x: x.lower())

    fingerprints = [n for n in names if n.lower().endswith("__fingerprint.json")]
    plain = [n for n in names if not n.lower().endswith("__fingerprint.json")]
    return fingerprints + plain


def iter_json_paths(root_dir: str) -> Iterator[str]:
    """Yield monolithic export JSON paths in root_dir (non-recursive)."""
    root_dir = os.path.abspath(root_dir)
    if not os.path.isdir(root_dir):
        raise FileNotFoundError(f"Not a directory: {root_dir}")

    names = _ordered_export_names(root_dir)

    for name in names:
        p = os.path.join(root_dir, name)
        if os.path.isfile(p):
            yield p


def load_export_file(path: str, *, file_id: Optional[str] = None) -> ExportFile:
    """Load one export JSON file."""
    if file_id is None:
        file_id = os.path.basename(path)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise TypeError(f"Export JSON root must be an object: {path}")

    return ExportFile(path=os.path.abspath(path), file_id=str(file_id), data=data)


def load_exports(root_dir: str, *, max_files: Optional[int] = None) -> List[ExportFile]:
    """Load all monolithic exports in a directory (each file = one authority sample)."""
    root_dir = os.path.abspath(root_dir)
    if not os.path.isdir(root_dir):
        raise FileNotFoundError(f"Not a directory: {root_dir}")

    names = _ordered_export_names(root_dir)

    exports: List[ExportFile] = []
    for name in names:
        p = os.path.join(root_dir, name)
        if os.path.isfile(p):
            exports.append(load_export_file(p))
            if max_files is not None and len(exports) >= int(max_files):
                break

    return exports


def get_contract(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return the authoritative run contract envelope if present."""
    c = data.get("_contract")
    return c if isinstance(c, dict) else None


def get_domains_map(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return domains map (back-compat pointer) if present."""
    d = data.get("_domains")
    return d if isinstance(d, dict) else None


def get_domain_envelope(data: Dict[str, Any], domain: str) -> Optional[Dict[str, Any]]:
    """Return per-domain envelope from _domains or _contract.domains if present."""
    domain = str(domain)

    dm = get_domains_map(data)
    if isinstance(dm, dict):
        env = dm.get(domain)
        if isinstance(env, dict):
            return env

    c = get_contract(data)
    if isinstance(c, dict):
        doms = c.get("domains")
        if isinstance(doms, dict):
            env = doms.get(domain)
            if isinstance(env, dict):
                return env

    return None


def get_domain_payload(data: Dict[str, Any], domain: str) -> Optional[Dict[str, Any]]:
    """Return the domain payload (legacy surface) if present."""
    d = data.get(str(domain))
    return d if isinstance(d, dict) else None


def get_domain_records(data: Dict[str, Any], domain: str) -> List[Dict[str, Any]]:
    """Extract record.v2 records list from the domain payload.

    Notes:
    - Contract per-domain envelope does not include records in current runner.
    - Domains implementing record.v2 typically expose records under <domain>.records.
    """
    payload = get_domain_payload(data, domain)
    if not isinstance(payload, dict):
        return []
    recs = payload.get("records")
    if not isinstance(recs, list):
        return []
    out: List[Dict[str, Any]] = []
    for r in recs:
        if isinstance(r, dict):
            out.append(r)
    return out


def get_run_provenance(data: Dict[str, Any]) -> Dict[str, Any]:
    """Best-effort extraction of run-level provenance for reporting.

    This is descriptive only; keys may be missing.
    """
    out: Dict[str, Any] = {}

    c = get_contract(data)
    if isinstance(c, dict):
        out["schema_version"] = c.get("schema_version")
        out["run_status"] = c.get("run_status")

    # Runner adds this at top-level.
    out["hash_mode"] = data.get("_hash_mode")

    # If present, include tool version fields without assuming schema.
    for k in ("tool_version", "version", "_tool_version"):
        if k in data:
            out["tool_version"] = data.get(k)
            break

    return out


def _read_csv_rows(path: str) -> Iterator[Dict[str, str]]:
    """Stream rows from a CSV file as dicts (UTF-8)."""
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if isinstance(row, dict):
                yield {str(k): ("" if v is None else str(v)) for k, v in row.items()}


def load_phase0_v21_file_paths(phase0_dir: str) -> Dict[str, str]:
    """Return best-effort file_id/export_run_id -> path for metadata heuristics.

    Uses Results_v21/phase0_v21/file_metadata.csv if present.
    """
    phase0_dir = os.path.abspath(phase0_dir)
    meta_csv = os.path.join(phase0_dir, "file_metadata.csv")
    if not os.path.isfile(meta_csv):
        return {}

    out: Dict[str, str] = {}
    for r in _read_csv_rows(meta_csv):
        # In v2.1, export_run_id is the canonical identifier; interim equals file_id.
        export_run_id = r.get("export_run_id", "").strip()
        file_id = r.get("file_id", "").strip()
        # Prefer normalized central path if present, else raw central path.
        path = (r.get("central_path_norm", "") or r.get("central_path", "") or "").strip()

        key = export_run_id or file_id
        if key:
            out[key] = path or key
    return out


def load_phase0_v21_sig_profiles(phase0_dir: str, domain: str) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
    """Build sig_hash presence profiles per export_run_id from v2.1 Phase0 CSVs.

    Returns:
        file_profiles: Dict[file_id/export_run_id -> {sig_hashes:Set[str], path:str, element_count:int}]
        file_paths: Dict[file_id/export_run_id -> path]
    """
    phase0_dir = os.path.abspath(phase0_dir)
    rec_csv = os.path.join(phase0_dir, "phase0_records.csv")
    if not os.path.isfile(rec_csv):
        raise FileNotFoundError(f"phase0_records.csv not found: {rec_csv}")

    domain = str(domain)
    file_paths = load_phase0_v21_file_paths(phase0_dir)

    sig_sets: Dict[str, Set[str]] = {}
    counts: Dict[str, int] = {}

    for r in _read_csv_rows(rec_csv):
        if r.get("domain", "") != domain:
            continue

        export_run_id = r.get("export_run_id", "").strip()
        file_id = r.get("file_id", "").strip()
        key = export_run_id or file_id
        if not key:
            continue

        sig = r.get("sig_hash", "").strip()
        if sig:
            sig_sets.setdefault(key, set()).add(sig)

        counts[key] = counts.get(key, 0) + 1

    file_profiles: Dict[str, Dict[str, Any]] = {}
    for key in sorted(counts.keys(), key=lambda x: x.lower()):
        file_profiles[key] = {
            "sig_hashes": sig_sets.get(key, set()),
            "path": file_paths.get(key, key),
            "element_count": counts.get(key, 0),
        }
        file_paths.setdefault(key, file_profiles[key]["path"])

    return file_profiles, file_paths


def load_phase0_v21_records_with_identity(
    phase0_dir: str,
    domain: str,
    *,
    allowed_file_ids: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    """Load v2.1 Phase0 records and attach identity_basis.items from phase0_identity_items.csv.

    This supports IDS tools that previously read fingerprint JSON records with:
      - record_id
      - sig_hash
      - identity_basis.items: [{k:<item_key>, v:<item_value>}...]

    Notes:
    - Uses export_run_id+domain+record_pk as the join spine (identity_items grain).
    - allowed_file_ids filters by Phase0 records.file_id (or export_run_id) if provided.
    - item_value is kept as a string (as emitted), which is sufficient for join hash composition.
    """
    phase0_dir = os.path.abspath(phase0_dir)
    rec_csv = os.path.join(phase0_dir, "phase0_records.csv")
    items_csv = os.path.join(phase0_dir, "phase0_identity_items.csv")

    if not os.path.isfile(rec_csv):
        raise FileNotFoundError(f"phase0_records.csv not found: {rec_csv}")
    if not os.path.isfile(items_csv):
        raise FileNotFoundError(f"phase0_identity_items.csv not found: {items_csv}")

    allowed = set([str(x) for x in (allowed_file_ids or set())]) if allowed_file_ids else None

    # Index records by (export_run_id, domain, record_pk)
    idx: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    order: List[Tuple[str, str, str]] = []

    for r in _read_csv_rows(rec_csv):
        if r.get("domain", "") != domain:
            continue

        export_run_id = (r.get("export_run_id", "") or "").strip()
        file_id = (r.get("file_id", "") or "").strip()
        record_pk = (r.get("record_pk", "") or "").strip()
        if not export_run_id or not record_pk:
            continue

        if allowed is not None:
            # Accept either file_id or export_run_id in the allowlist (interim they are the same).
            if (file_id not in allowed) and (export_run_id not in allowed):
                continue

        key = (export_run_id, domain, record_pk)

        rec: Dict[str, Any] = {
            "_file_id": file_id or export_run_id,
            "file_id": file_id or export_run_id,
            "export_run_id": export_run_id,
            "domain": domain,
            "record_pk": record_pk,
            "record_id": (r.get("record_id", "") or "").strip(),
            "sig_hash": (r.get("sig_hash", "") or "").strip(),
            "identity_basis": {"items": []},
        }
        idx[key] = rec
        order.append(key)

    # Attach identity items
    # phase0_identity_items.csv columns:
    # schema_version, export_run_id, domain, record_pk, item_key, item_value, item_value_type, item_role
    for r in _read_csv_rows(items_csv):
        if r.get("domain", "") != domain:
            continue
        export_run_id = (r.get("export_run_id", "") or "").strip()
        record_pk = (r.get("record_pk", "") or "").strip()
        if not export_run_id or not record_pk:
            continue
        key = (export_run_id, domain, record_pk)
        rec = idx.get(key)
        if rec is None:
            continue

        k = (r.get("item_key", "") or "").strip()
        if not k:
            continue
        v = (r.get("item_value", "") or "")
        rec["identity_basis"]["items"].append({"k": k, "v": v})

    # Deterministic: preserve phase0_records order (already stable-sorted in v2.1), fallback to sorted key.
    out: List[Dict[str, Any]] = []
    for k in order:
        if k in idx:
            out.append(idx[k])
    return out
