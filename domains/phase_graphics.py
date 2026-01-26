# -*- coding: utf-8 -*-
"""
Phase Graphics domain extractor.

Fingerprints phase graphic override settings including:
- Projection and cut line styles
- Line colors
- Surface patterns and colors
- Material overrides

This captures the GLOBAL phase graphic override settings that apply
across the project.

This is a GLOBAL domain - settings are defined once.

Per-record identity: N/A (single global configuration)
Ordering: order-insensitive (settings are unordered)
"""

import os
import sys

# Ensure repo root is importable (so `import core...` works everywhere)
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str

from core.record_v2 import (
    STATUS_OK,
    STATUS_DEGRADED,
    STATUS_BLOCKED,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED_NOT_IMPLEMENTED,
    canonicalize_str,
    make_identity_item,
    serialize_identity_items,
    build_record_v2,
)

from core.phase2 import (
    phase2_sorted_items,
    phase2_join_hash,
)

try:
    from Autodesk.Revit.DB import ElementOnPhaseStatus
except ImportError:
    ElementOnPhaseStatus = None


def _phase2_build_join_key_items():
    """Build Phase-2 join-key IdentityItems (domain-specific, hypothesis-only).

    Join-key hypothesis (reversible):
      - This is a single global configuration record. Use a constant scope key
        to allow cross-file stability checks without inventing per-file identifiers.
    """
    return [
        make_identity_item("phase_graphics.scope", "global", ITEM_Q_OK),
    ]


def extract(doc, ctx=None):
    """
    Extract Phase Graphics override settings from document.

    NOTE: API access to these settings varies by Revit version and may not be exposed.
    This domain currently does not implement a full capture of phase graphic override settings.

    Args:
        doc: Revit Document
        ctx: Context dictionary (unused for this domain)

    Returns:
        Dictionary with legacy hash/signature, plus record.v2 + Phase-2 additive artifacts.
    """
    info = {
        # -------------------------
        # Legacy output (preserve existing behavior)
        # -------------------------
        "hash": None,
        "signature": [],
        "debug_note": "Phase graphic overrides may not be fully exposed in all API versions",

        # -------------------------
        # record.v2 (additive)
        # -------------------------
        "records": [],
        "record_rows": [],
        "hash_v2": None,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
    }

    sig = []

    # Legacy signature/hash: preserve existing behavior (note + statuses_count or error).
    try:
        _ = doc.Settings.Categories  # reachability probe only (no inference)
        statuses = []
        if ElementOnPhaseStatus:
            try:
                statuses = [
                    ("New", ElementOnPhaseStatus.New),
                    ("Existing", ElementOnPhaseStatus.Existing),
                    ("Demolished", ElementOnPhaseStatus.Demolished),
                    ("Temporary", ElementOnPhaseStatus.Temporary),
                ]
            except Exception:
                statuses = []
        sig.append("note=PhaseGraphicsNotFullyImplemented")
        sig.append("statuses_count={}".format(len(statuses)))
        legacy_error = None
    except Exception:
        sig.append("error=CannotAccessPhaseGraphics")
        legacy_error = "CannotAccessPhaseGraphics"

    sig_sorted = sorted(sig)
    info["signature"] = sig_sorted
    info["hash"] = make_hash(sig_sorted) if sig_sorted else None

    # -------------------------
    # record.v2 + Phase-2 (additive, explicit)
    # -------------------------

    # We do not have evidence this domain can be captured via API here, and this extractor
    # does not implement a full capture. Emit explicit unsupported.not_implemented.
    identity_items = []

    # Optional reachability signal: did the legacy probe succeed?
    # Keep this as non-required, and do not treat it as a proxy for actual settings.
    if legacy_error is None:
        probe_v, probe_q = ("ok", ITEM_Q_OK)
    else:
        probe_v, probe_q = (None, ITEM_Q_UNREADABLE)
    identity_items.append(make_identity_item("phase_graphics.api_probe", probe_v, probe_q))

    # Primary explicit statement: capture not implemented (item-level, not legacy sentinel).
    identity_items.append(
        make_identity_item("phase_graphics.overrides", None, ITEM_Q_UNSUPPORTED_NOT_IMPLEMENTED)
    )

    identity_items_sorted = sorted(identity_items, key=lambda it: safe_str(it.get("k", "")))

    status_reasons = []
    any_incomplete = False
    for it in identity_items_sorted:
        q = it.get("q")
        if q != ITEM_Q_OK:
            any_incomplete = True
            status_reasons.append("identity.incomplete:{}:{}".format(q, it.get("k")))

    # Blocked because the primary payload is explicitly not implemented.
    status_v2 = STATUS_BLOCKED
    sig_hash_v2 = None

    rec_v2 = build_record_v2(
        domain="phase_graphics",
        record_id="phase_graphics:global",
        status=status_v2,
        status_reasons=sorted(set(status_reasons)) or ["unsupported.not_implemented"],
        sig_hash=sig_hash_v2,
        identity_items=identity_items_sorted,
        required_qs=(),
        label={
            "display": "Phase Graphics (global)",
            "quality": "system",
            "provenance": "domain.phase_graphics",
            "components": {"scope": "global"},
        },
    )

    # Phase-2 additions (additive, explanatory, reversible)
    join_key_items = _phase2_build_join_key_items()
    join_key_items_sorted = phase2_sorted_items(join_key_items)
    rec_v2["join_key"] = {
        "schema": "phase_graphics.join_key.v1",
        "hash_alg": "md5_utf8_join_pipe",
        "items": join_key_items_sorted,
        "join_hash": phase2_join_hash(join_key_items_sorted),
    }

    # With no extracted settings, all identity items are "unknown" for hypothesis grouping.
    rec_v2["phase2"] = {
        "schema": "phase2.phase_graphics.v1",
        "grouping_basis": "phase2.hypothesis",
        "semantic_items": phase2_sorted_items([]),
        "cosmetic_items": phase2_sorted_items([]),
        "unknown_items": phase2_sorted_items(list(identity_items_sorted or [])),
    }

    info["records"] = [rec_v2]
    info["hash_v2"] = None
    info["debug_v2_blocked"] = True
    info["debug_v2_block_reasons"] = {"unsupported.not_implemented": True}

    return info
