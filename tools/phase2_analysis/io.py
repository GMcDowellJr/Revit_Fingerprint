from __future__ import annotations

import json
import os
import sys

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple


@dataclass(frozen=True)
class ExportFile:
    """One exported fingerprint JSON treated as one authority sample."""

    path: str
    file_id: str
    data: Dict[str, Any]


def _merge_index_details(index_data: Dict[str, Any], details_data: Dict[str, Any]) -> Dict[str, Any]:
    """Merge index (metadata) and details (domain payloads) into a single fingerprint object."""
    merged = {**index_data}
    for key, value in details_data.items():
        # Domain payloads don't start with underscore; index metadata does
        if not key.startswith("_") and key not in merged:
            merged[key] = value
    return merged


def iter_json_paths(root_dir: str) -> Iterator[str]:
    """Yield export JSON paths in root_dir (non-recursive), split-export safe.

    NOTE: This function yields individual file paths. For split exports (index + details),
    consider using load_exports() instead which properly merges split files.

    Preference order:
      1) *.details.json
      2) *.index.json
      3) *.json excluding *.legacy.json
      4) legacy-only only if nothing else exists (warn loudly)
    """
    root_dir = os.path.abspath(root_dir)
    if not os.path.isdir(root_dir):
        raise FileNotFoundError(f"Not a directory: {root_dir}")

    names = [n for n in os.listdir(root_dir) if n.lower().endswith(".json")]
    names.sort(key=lambda x: x.lower())

    details = [n for n in names if n.lower().endswith(".details.json")]
    index = [n for n in names if n.lower().endswith(".index.json")]
    legacy = [n for n in names if n.lower().endswith(".legacy.json")]

    if details:
        if legacy:
            sys.stderr.write(
                "[WARN phase2.io] Found legacy bundle(s) but ignoring by default (details present).\n"
            )
        chosen = details
    elif index:
        if legacy:
            sys.stderr.write(
                "[WARN phase2.io] Found legacy bundle(s) but ignoring by default (index present).\n"
            )
        sys.stderr.write(
            "[WARN phase2.io] No *.details.json found; falling back to *.index.json "
            "(record-level metrics may be undefined).\n"
        )
        chosen = index
    else:
        chosen = [n for n in names if not n.lower().endswith(".legacy.json")]
        if legacy and not chosen:
            sys.stderr.write(
                "[WARN phase2.io] Only legacy bundle(s) found; Phase-2 analysis may be incomplete/invalid under current defaults.\n"
            )
            chosen = legacy
        else:
            sys.stderr.write(
                "[WARN phase2.io] No split exports found (*.details.json / *.index.json). Falling back to *.json excluding legacy.\n"
            )

    for name in chosen:
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
    """Load all exports in a directory (each file = one authority sample).

    Handles split exports by merging index + details files into single ExportFile objects.
    """
    root_dir = os.path.abspath(root_dir)
    if not os.path.isdir(root_dir):
        raise FileNotFoundError(f"Not a directory: {root_dir}")

    names = [n for n in os.listdir(root_dir) if n.lower().endswith(".json")]
    names.sort(key=lambda x: x.lower())

    details = [n for n in names if n.lower().endswith(".details.json")]
    index = [n for n in names if n.lower().endswith(".index.json")]
    legacy = [n for n in names if n.lower().endswith(".legacy.json")]

    exports: List[ExportFile] = []

    if details and index:
        # SPLIT EXPORT: Merge index + details
        sys.stderr.write("[INFO phase2.io] Found split exports. Merging index + details.\n")
        if legacy:
            sys.stderr.write(
                "[WARN phase2.io] Found legacy bundle(s) but ignoring by default (split exports present).\n"
            )

        # Build stem-to-path mappings
        index_by_stem = {n.lower().replace('.index.json', ''): n for n in index}
        details_by_stem = {n.lower().replace('.details.json', ''): n for n in details}

        for stem in sorted(set(index_by_stem.keys()) | set(details_by_stem.keys())):
            if stem not in index_by_stem:
                sys.stderr.write(f"[WARN phase2.io] Missing index for '{stem}', skipping.\n")
                continue

            index_path = os.path.join(root_dir, index_by_stem[stem])
            with open(index_path, "r", encoding="utf-8") as f:
                index_data = json.load(f)

            if not isinstance(index_data, dict):
                sys.stderr.write(f"[WARN phase2.io] Index JSON root not an object: {index_path}, skipping.\n")
                continue

            details_data: Dict[str, Any] = {}
            if stem in details_by_stem:
                details_path = os.path.join(root_dir, details_by_stem[stem])
                with open(details_path, "r", encoding="utf-8") as f:
                    details_data = json.load(f)
                if not isinstance(details_data, dict):
                    sys.stderr.write(f"[WARN phase2.io] Details JSON root not an object: {details_path}, using index only.\n")
                    details_data = {}

            merged_data = _merge_index_details(index_data, details_data)
            exports.append(ExportFile(
                path=index_path,
                file_id=index_by_stem[stem],
                data=merged_data
            ))

            if max_files is not None and len(exports) >= int(max_files):
                break

    elif details:
        # DETAILS ONLY: Domain payloads without metadata (degraded mode)
        sys.stderr.write(
            "[WARN phase2.io] Found details but no index. Metadata extraction may fail.\n"
        )
        if legacy:
            sys.stderr.write(
                "[WARN phase2.io] Found legacy bundle(s) but ignoring by default (details present).\n"
            )
        for name in details:
            p = os.path.join(root_dir, name)
            exports.append(load_export_file(p, file_id=name.lower().replace('.details.json', '')))
            if max_files is not None and len(exports) >= int(max_files):
                break

    elif index:
        # INDEX ONLY: Metadata but no records (degraded mode)
        sys.stderr.write(
            "[WARN phase2.io] No *.details.json found; using *.index.json "
            "(record-level metrics may be undefined).\n"
        )
        if legacy:
            sys.stderr.write(
                "[WARN phase2.io] Found legacy bundle(s) but ignoring by default (index present).\n"
            )
        for name in index:
            p = os.path.join(root_dir, name)
            exports.append(load_export_file(p, file_id=name.lower().replace('.index.json', '')))
            if max_files is not None and len(exports) >= int(max_files):
                break

    else:
        # FALLBACK: Generic *.json (legacy/monolithic)
        chosen = [n for n in names if not n.lower().endswith(".legacy.json")]
        if legacy and not chosen:
            sys.stderr.write(
                "[WARN phase2.io] Only legacy bundle(s) found; Phase-2 analysis may be incomplete/invalid under current defaults.\n"
            )
            chosen = legacy
        else:
            sys.stderr.write(
                "[WARN phase2.io] No split exports found (*.details.json / *.index.json). Falling back to *.json excluding legacy.\n"
            )

        for name in chosen:
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
