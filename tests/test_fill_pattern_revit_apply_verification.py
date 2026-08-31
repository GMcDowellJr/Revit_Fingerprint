# -*- coding: utf-8 -*-
"""Tests for the read/verify half of mapping/fill_pattern_revit_apply.py.

These specific functions (read_target_name, read_fill_pattern_from_element,
verify_element_join_hash) are pure duck-typed attribute access over
whatever object is passed in -- they never reference the module's
guarded Autodesk.Revit.DB imports directly, so they're testable outside
Revit with plain mock objects standing in for FillPatternElement/
FillPattern/FillGrid/Origin. The construction half
(create_and_verify_fill_pattern, _build_fill_grid) DOES require the real
Revit API and is not covered here -- see
docs/fill_pattern_mapping_verification.md for the manual Revit-side
procedure.

Regression coverage for the P1 bug found in PR #443 review: an existing
FillPatternElement found by name could actually belong to the OTHER
partition (fill_patterns_drafting vs fill_patterns_model share one
FillPatternElement namespace within a document) -- verification must read
the element's ACTUAL Target and reject a mismatch, never trust a
caller-asserted expected target.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from mapping.fill_pattern_reconstruction import (
    DOMAIN_DRAFTING,
    DOMAIN_MODEL,
    compute_grids_def_hash,
    compute_join_hash_for_grids,
    ReconstructedGrid,
)
from mapping.fill_pattern_revit_apply import (
    read_fill_pattern_from_element,
    read_target_name,
    verify_element_join_hash,
)


class _MockTarget:
    """Stands in for a pythonnet-wrapped FillPatternTarget enum value."""

    def __init__(self, name):
        self._name = name

    def ToString(self):
        return self._name


class _MockOrigin:
    def __init__(self, u, v):
        self.U = u
        self.V = v


class _MockGrid:
    def __init__(self, angle, u, v, offset, shift):
        self.Angle = angle
        self.Origin = _MockOrigin(u, v)
        self.Offset = offset
        self.Shift = shift


class _MockFillPattern:
    def __init__(self, target_name, grids):
        self.Target = _MockTarget(target_name)
        self._grids = grids

    @property
    def GridCount(self):
        return len(self._grids)

    def GetFillGrids(self):
        return list(self._grids)


class _MockElement:
    def __init__(self, fill_pattern):
        self._fill_pattern = fill_pattern

    def GetFillPattern(self):
        return self._fill_pattern


_SAMPLE_MOCK_GRIDS = [_MockGrid(0.0, 0.0, 0.0, 0.125, 0.0625), _MockGrid(1.5, 0.5, -0.25, 0.25, 0.0)]

_SAMPLE_RECONSTRUCTED_GRIDS = [
    ReconstructedGrid(idx=0, angle=0.0, origin_kind="uv", origin_a=0.0, origin_b=0.0, offset=0.125, shift=0.0625),
    ReconstructedGrid(idx=1, angle=1.5, origin_kind="uv", origin_a=0.5, origin_b=-0.25, offset=0.25, shift=0.0),
]


# ---------------------------------------------------------------------------
# read_target_name
# ---------------------------------------------------------------------------

def test_read_target_name_uses_to_string():
    fp = _MockFillPattern("Drafting", [])
    assert read_target_name(fp) == "Drafting"


def test_read_target_name_none_when_target_missing():
    class _NoTarget:
        pass

    assert read_target_name(_NoTarget()) is None


def test_read_target_name_maps_integer_stringification_to_name():
    # Regression for PR #443 review: confirmed live against a real Dynamo/
    # pythonnet session that FillPatternTarget.ToString() can return the
    # underlying integer ("0"/"1") rather than the enum member name in this
    # environment -- every single real mapping request blocked with
    # target_mismatch:0/1 until this fallback was added (matching the same
    # quirk tools/probes/probe_fill_patterns.py already worked around).
    drafting_fp = _MockFillPattern("0", [])
    model_fp = _MockFillPattern("1", [])
    assert read_target_name(drafting_fp) == "Drafting"
    assert read_target_name(model_fp) == "Model"


# ---------------------------------------------------------------------------
# read_fill_pattern_from_element
# ---------------------------------------------------------------------------

def test_read_fill_pattern_from_element_happy_path():
    fp = _MockFillPattern("Model", _SAMPLE_MOCK_GRIDS)
    element = _MockElement(fp)

    result = read_fill_pattern_from_element(None, element)
    assert result is not None
    target_name, grids = result
    assert target_name == "Model"
    assert len(grids) == 2
    assert grids[0].origin_kind == "uv"
    assert grids[0].angle == 0.0


def test_read_fill_pattern_from_element_none_when_grid_count_mismatch():
    class _MismatchedFP(_MockFillPattern):
        @property
        def GridCount(self):
            return 5  # declared count does not match actual GetFillGrids() length

    mismatched = _MismatchedFP("Drafting", _SAMPLE_MOCK_GRIDS)
    element = _MockElement(mismatched)
    assert read_fill_pattern_from_element(None, element) is None


def test_read_fill_pattern_from_element_none_when_get_fill_pattern_fails():
    class _BrokenElement:
        def GetFillPattern(self):
            raise RuntimeError("boom")

    assert read_fill_pattern_from_element(None, _BrokenElement()) is None


# ---------------------------------------------------------------------------
# verify_element_join_hash -- the P1 regression: target must be read from the
# element, never trusted from the caller's expectation.
# ---------------------------------------------------------------------------

def _domain_policy(domain_name):
    from mapping.fill_pattern_reconstruction import get_fill_pattern_join_key_policy

    return get_fill_pattern_join_key_policy(domain_name)


def test_verify_element_join_hash_rejects_wrong_actual_target():
    # Element is actually Drafting-targeted, but the caller (processing the
    # fill_patterns_model partition) expects "Model" -- this must NOT pass
    # verification just because the grids happen to match.
    grids_def_hash = compute_grids_def_hash(2, _SAMPLE_RECONSTRUCTED_GRIDS)
    requested_join_hash, _, _ = compute_join_hash_for_grids(
        DOMAIN_MODEL, "Model", 2, grids_def_hash, domain_policy=_domain_policy(DOMAIN_MODEL)
    )

    fp = _MockFillPattern("Drafting", _SAMPLE_MOCK_GRIDS)
    element = _MockElement(fp)

    result = verify_element_join_hash(
        None, element, DOMAIN_MODEL, "Model", requested_join_hash, domain_policy=_domain_policy(DOMAIN_MODEL)
    )
    assert result.ok is False
    assert result.reason == "target_mismatch:Drafting"
    assert result.verified_join_hash is None


def test_verify_element_join_hash_accepts_matching_target_and_grids():
    grids_def_hash = compute_grids_def_hash(2, _SAMPLE_RECONSTRUCTED_GRIDS)
    requested_join_hash, _, _ = compute_join_hash_for_grids(
        DOMAIN_DRAFTING, "Drafting", 2, grids_def_hash, domain_policy=_domain_policy(DOMAIN_DRAFTING)
    )

    fp = _MockFillPattern("Drafting", _SAMPLE_MOCK_GRIDS)
    element = _MockElement(fp)

    result = verify_element_join_hash(
        None, element, DOMAIN_DRAFTING, "Drafting", requested_join_hash, domain_policy=_domain_policy(DOMAIN_DRAFTING)
    )
    assert result.ok is True
    assert result.verified_join_hash == requested_join_hash


def test_verify_element_join_hash_accepts_integer_stringified_target():
    # End-to-end regression for the real-environment "0"/"1" stringification
    # quirk: an element whose Target.ToString() returns "1" must still verify
    # successfully against the fill_patterns_model partition's expected
    # "Model" target.
    grids_def_hash = compute_grids_def_hash(2, _SAMPLE_RECONSTRUCTED_GRIDS)
    requested_join_hash, _, _ = compute_join_hash_for_grids(
        DOMAIN_MODEL, "Model", 2, grids_def_hash, domain_policy=_domain_policy(DOMAIN_MODEL)
    )

    fp = _MockFillPattern("1", _SAMPLE_MOCK_GRIDS)
    element = _MockElement(fp)

    result = verify_element_join_hash(
        None, element, DOMAIN_MODEL, "Model", requested_join_hash, domain_policy=_domain_policy(DOMAIN_MODEL)
    )
    assert result.ok is True
    assert result.verified_join_hash == requested_join_hash


def test_verify_element_join_hash_rejects_grid_mismatch_even_with_matching_target():
    grids_def_hash = compute_grids_def_hash(2, _SAMPLE_RECONSTRUCTED_GRIDS)
    requested_join_hash, _, _ = compute_join_hash_for_grids(
        DOMAIN_DRAFTING, "Drafting", 2, grids_def_hash, domain_policy=_domain_policy(DOMAIN_DRAFTING)
    )

    different_grids = [_MockGrid(9.9, 9.9, 9.9, 9.9, 9.9), _MockGrid(1.5, 0.5, -0.25, 0.25, 0.0)]
    fp = _MockFillPattern("Drafting", different_grids)
    element = _MockElement(fp)

    result = verify_element_join_hash(
        None, element, DOMAIN_DRAFTING, "Drafting", requested_join_hash, domain_policy=_domain_policy(DOMAIN_DRAFTING)
    )
    assert result.ok is False
    assert result.reason == "post_creation_identity_mismatch"
