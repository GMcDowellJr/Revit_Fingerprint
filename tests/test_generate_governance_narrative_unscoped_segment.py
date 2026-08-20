"""Tests for _is_unscoped_segment() in tools/generate_governance_narrative.py.

This helper gates every cascade stage in build_cascade() (Group 1 and Group 2).
It has needed two fixes so far:
  - reject bc/collection-scoped segment_ids that client_label/discipline_label
    columns alone can't reveal (PR #350 review, docs/governance_narrative_scope_gap_audit.md B6)
  - accept a trailing/embedded BLANK pipe token as still-unscoped, since
    build_segment_manifest.py's _subset_to_id() emits a literal empty token for
    a selected-but-blank client_label/discipline_label key dimension (e.g.
    "imperial|Template||Shared" for a blank client alongside a real
    business_center_label) -- as distinct from a hidden NON-blank scope token,
    which must still be rejected (PR #350 review, P1).
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from generate_governance_narrative import _is_unscoped_segment  # noqa: E402


def _row(segment_id: str, role: str = "Template", client: str = "", disc: str = ""):
    return {
        "segment_id_a": segment_id,
        "governance_role_a": role,
        "client_label_a": client,
        "discipline_label_a": disc,
    }


def test_genuinely_broadest_segment_is_unscoped():
    assert _is_unscoped_segment(_row("imperial|Template"), "a") is True


def test_trailing_blank_client_token_is_still_unscoped():
    """build_segment_manifest.py's _subset_to_id() emits a literal empty token
    for a selected-but-blank client_label dimension -- "imperial|Generic|" is a
    real, legitimately-generated enterprise segment, not a scoped one."""
    assert _is_unscoped_segment(_row("imperial|Generic|", role="Generic"), "a") is True
    assert _is_unscoped_segment(_row("imperial|Template|"), "a") is True


def test_bc_scoped_segment_is_rejected():
    assert _is_unscoped_segment(_row("imperial|Template|BC_1234"), "a") is False


def test_collection_scoped_segment_is_rejected():
    assert _is_unscoped_segment(_row("imperial|Template|collection:Shared"), "a") is False


def test_blank_client_token_with_real_hidden_scope_value_is_rejected():
    """Blank client (empty token) followed by a REAL bc/collection value must
    still be rejected -- only an all-blank tail is safe."""
    assert _is_unscoped_segment(_row("imperial|Template||Shared"), "a") is False


def test_client_scoped_segment_is_rejected():
    assert _is_unscoped_segment(_row("imperial|Template|ClientAlpha", client="ClientAlpha"), "a") is False


def test_blank_role_rollup_is_rejected():
    """See docs/governance_narrative_scope_gap_audit.md B5 -- a blank-role scope
    rollup like "imperial|BC_2014" must not be treated as unscoped."""
    assert _is_unscoped_segment(_row("imperial|BC_2014", role=""), "a") is False
