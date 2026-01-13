# core/context.py
# Shared document + view context helpers.
#
# PR6 scope:
# - Provide a single authoritative way to read view-scoped properties with:
#   - explicit missing vs unreadable signaling
#   - deterministic output shape
#   - per-run caching by view id
# - Provide explicit "source" markers (HOST/LINK) as scaffolding for link-aware domains.
#
# NOTE: Current repo domains are mostly non-geometry; link/transform helpers are scaffolding
# for future geometry/view-graphics domains.

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from core.canon import canon_id, canon_str, S_MISSING, S_UNREADABLE

try:
    from Autodesk.Revit.DB import BuiltInParameter
except Exception:
    BuiltInParameter = None


@dataclass(frozen=True)
class ViewInfo:
    """
    Deterministic, explainable view context snapshot.

    Fields:
    - view_id: int (or sentinel string)
    - view_uid: str (or sentinel string)
    - view_template_id: int (or sentinel string)
    - phase_id: int (or sentinel string)
    - discipline: str (or sentinel string)
    - source: "HOST" or "LINK"
    - reasons: tuple[str, ...] (explicit missing/unreadable codes)
    """
    view_id: Any
    view_uid: Any
    view_template_id: Any
    phase_id: Any
    discipline: Any
    source: str
    reasons: Tuple[str, ...]


class DocViewContext:
    """
    Shared context object for domains that need consistent view-scoped reads.

    This object is intentionally small:
    - It caches view_info by (source, view_id_int) for the current run.
    - It does not attempt to be a general "run context" or cache collector results.
    """

    def __init__(self, doc: Any):
        self._doc = doc
        self._cache: Dict[Tuple[str, int], ViewInfo] = {}

    def view_info(self, view: Any, *, source: str = "HOST") -> ViewInfo:
        """
        Return a cached ViewInfo for `view`, with explicit reasons for missing/unreadable fields.

        Determinism:
        - Always returns a ViewInfo instance with the same field set.
        - Uses stable sentinels S_MISSING/S_UNREADABLE for missing/unreadable values.
        - `reasons` is a tuple, sorted by insertion order (stable).
        """
        src = str(source).strip().upper() or "HOST"
        if src not in ("HOST", "LINK"):
            src = "HOST"

        view_id_int = None
        try:
            vid = getattr(view, "Id", None)
            view_id_int = int(getattr(vid, "IntegerValue", None))
        except Exception:
            view_id_int = None

        if view_id_int is None:
            # No stable cache key; return uncached, explicit unreadable
            return ViewInfo(
                view_id=S_UNREADABLE,
                view_uid=S_UNREADABLE,
                view_template_id=S_UNREADABLE,
                phase_id=S_UNREADABLE,
                discipline=S_UNREADABLE,
                source=src,
                reasons=("view_id_unreadable",),
            )

        ck = (src, view_id_int)
        cached = self._cache.get(ck)
        if cached is not None:
            return cached

        reasons = []

        # view_id
        view_id_val = view_id_int

        # view_uid
        try:
            vu = canon_str(getattr(view, "UniqueId", None))
            if not vu:
                reasons.append("view_uid_missing")
                vu = S_MISSING
            view_uid_val = vu
        except Exception:
            reasons.append("view_uid_unreadable")
            view_uid_val = S_UNREADABLE

        # view_template_id (note: templates themselves typically have InvalidElementId here)
        try:
            vtid = getattr(view, "ViewTemplateId", None)
            vtid_val = canon_id(vtid)
            if vtid_val == S_MISSING:
                # canon_id returns S_MISSING if id is None/invalid
                reasons.append("view_template_id_missing")
            view_template_id_val = vtid_val
        except Exception:
            reasons.append("view_template_id_unreadable")
            view_template_id_val = S_UNREADABLE

        # phase_id (via VIEW_PHASE parameter when available)
        # Not all views expose phase; treat as missing if not applicable/unset.
        phase_id_val = S_MISSING
        if BuiltInParameter is None:
            # Outside Revit context; cannot read param
            reasons.append("phase_param_api_unavailable")
            phase_id_val = S_UNREADABLE
        else:
            try:
                p = view.get_Parameter(BuiltInParameter.VIEW_PHASE)
                if p is None:
                    reasons.append("phase_param_missing")
                    phase_id_val = S_MISSING
                else:
                    pid = p.AsElementId()
                    phase_id_val = canon_id(pid)
                    if phase_id_val == S_MISSING:
                        reasons.append("phase_id_missing")
            except Exception:
                reasons.append("phase_id_unreadable")
                phase_id_val = S_UNREADABLE

        # discipline (best-effort; value is metadata but may be used for gating)
        # Prefer a stable integer/string, but keep it as a canonical string.
        discipline_val = S_MISSING
        if BuiltInParameter is None:
            # Outside Revit context; cannot read param
            reasons.append("discipline_param_api_unavailable")
            discipline_val = S_UNREADABLE
        else:
            try:
                dp = view.get_Parameter(BuiltInParameter.VIEW_DISCIPLINE)
                if dp is None:
                    reasons.append("discipline_param_missing")
                    discipline_val = S_MISSING
                else:
                    # VIEW_DISCIPLINE is typically integer-coded; keep as string for stable hashing surfaces
                    try:
                        discipline_val = canon_str(dp.AsInteger())
                    except Exception:
                        discipline_val = canon_str(dp.AsValueString())
                    if not discipline_val:
                        reasons.append("discipline_missing")
                        discipline_val = S_MISSING
            except Exception:
                reasons.append("discipline_unreadable")
                discipline_val = S_UNREADABLE

        vi = ViewInfo(
            view_id=view_id_val,
            view_uid=view_uid_val,
            view_template_id=view_template_id_val,
            phase_id=phase_id_val,
            discipline=discipline_val,
            source=src,
            reasons=tuple(reasons),
        )
        self._cache[ck] = vi
        return vi
