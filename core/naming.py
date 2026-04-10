# -*- coding: utf-8 -*-
"""
Document-derived naming helpers for output files.

Goal:
- Produce filenames that are (a) stable + traceable to the RVT, and (b) filesystem-safe.
- Avoid embedding machine-specific paths.
- Prefer ProjectInformation.Number + ProjectInformation.UniqueId when available.

This module must not throw in normal usage; callers should treat failures as "unknown"
and fall back deterministically.
"""

from __future__ import annotations

import os
import re
import hashlib
from typing import Any, Dict, Optional


_slug_re = re.compile(r"[^A-Za-z0-9._-]+")


def safe_slug(s: Any, *, max_len: int = 80) -> str:
    try:
        s2 = str(s) if s is not None else ""
    except Exception:
        s2 = ""
    s2 = s2.strip()
    if not s2:
        return "unknown"

    s2 = s2.replace(" ", "_")
    s2 = _slug_re.sub("_", s2)
    s2 = re.sub(r"_+", "_", s2).strip("._-")
    if not s2:
        return "unknown"
    if len(s2) > max_len:
        s2 = s2[:max_len].rstrip("._-")
        if not s2:
            return "unknown"
    return s2


def _file_stem_from_doc(doc: Any) -> str:
    # Prefer path basename when present; fall back to Title.
    try:
        p = getattr(doc, "PathName", None)
        if p:
            base = os.path.basename(str(p))
            stem, _ext = os.path.splitext(base)
            if stem:
                return stem
    except Exception:
        pass
    try:
        t = getattr(doc, "Title", None)
        if t:
            return str(t)
    except Exception:
        pass
    return "unknown"


def _project_information(doc: Any) -> Dict[str, Optional[str]]:
    out: Dict[str, Optional[str]] = {"number": None, "name": None, "unique_id": None}
    try:
        pi = getattr(doc, "ProjectInformation", None)
        if pi is None:
            return out
        try:
            out["number"] = str(getattr(pi, "Number", None)) if getattr(pi, "Number", None) else None
        except Exception:
            out["number"] = None
        try:
            out["name"] = str(getattr(pi, "Name", None)) if getattr(pi, "Name", None) else None
        except Exception:
            out["name"] = None
        try:
            out["unique_id"] = str(getattr(pi, "UniqueId", None)) if getattr(pi, "UniqueId", None) else None
        except Exception:
            out["unique_id"] = None
    except Exception:
        return out
    return out


def _short_uid(uid: Optional[str], *, take: int = 12) -> str:
    if not uid:
        return "unknown"
    s = str(uid).strip()
    if not s:
        return "unknown"
    # Revit UniqueId often contains '-' and a trailing element id segment; keep last chunk-ish.
    s = s.replace("{", "").replace("}", "")
    if len(s) <= take:
        return safe_slug(s, max_len=take)
    return safe_slug(s[-take:], max_len=take)


def _doc_identity_seed(doc: Any) -> str:
    """
    Build a document-identity seed for scoping caches/artifacts.

    Priority:
    1) ProjectInformation.UniqueId (document-unique in Revit)
    2) Central/file path + title composite (best-effort fallback)
    3) Title-only fallback

    Explicitly excludes app/version metadata such as VersionBuild because those
    are shared across many documents and are not document-unique.
    """
    pi = _project_information(doc)
    uid = (pi.get("unique_id") or "").strip()
    if uid:
        return "uid:{0}".format(uid)

    path = ""
    title = ""
    try:
        path = str(getattr(doc, "PathName", None) or "").strip()
    except Exception:
        path = ""
    try:
        title = str(getattr(doc, "Title", None) or "").strip()
    except Exception:
        title = ""

    if path or title:
        return "path_title:{0}|{1}".format(path.lower(), title.lower())
    return "title:{0}".format(title.lower())


def _doc_identity_short(doc: Any, *, take: int = 10) -> str:
    seed = _doc_identity_seed(doc)
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()
    return digest[:take]

def derive_doc_key(doc: Any) -> Dict[str, str]:
    """
    Returns identifiers suitable for filenames and indexing.

    Keyed ONLY to the RVT file name (stem), sanitized and truncated.
    """
    file_stem = safe_slug(_file_stem_from_doc(doc), max_len=64)

    # Optional short UID to reduce collisions when files share names
    pi = _project_information(doc)
    pi_uid_short = _short_uid(pi.get("unique_id"), take=8)
    doc_identity_short = _doc_identity_short(doc, take=10)

    key = "fp__rvt-{0}__{1}__{2}".format(file_stem, pi_uid_short, doc_identity_short)

    return {
        "rvt_file_stem": file_stem,
        "projectinfo_uid_short": pi_uid_short,
        "doc_identity_short": doc_identity_short,
        "key": key,
    }

def build_output_filename(
    doc: Any,
    *,
    stamp: Optional[str] = None,
    kind: str = "fingerprint",
    ext: str = "json",
    include_stamp: bool = True,
) -> str:
    """
    Build a filename tied to RVT identity.

    Args:
        doc: Revit document
        stamp: Optional timestamp string
        kind: Logical artifact kind (fingerprint / manifest / features)
        ext: File extension (json)
        include_stamp: If False, suppress timestamp even if provided

    Returns:
        Filesystem-safe filename (no directory component)
    """
    k = derive_doc_key(doc)["key"]
    parts = [k, safe_slug(kind, max_len=24)]

    if include_stamp and stamp:
        parts.append(safe_slug(stamp, max_len=32))

    base = "__".join(parts)
    return "{0}.{1}".format(base, safe_slug(ext, max_len=8))
