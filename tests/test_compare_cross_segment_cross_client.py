"""Tests for discover_cross_client() / _is_client_only_project_segment() in
tools/compare_cross_segment.py.

Background: cross_client is a purpose-built client-vs-client comparison type
(governance pipeline gap fix). Before this, the only thing feeding the
cross_client_convergence / cross_client_similarity_mean columns downstream in
generate_governance_narrative.py was "sibling_projects" -- an accidental
overload that only pairs Project segments sharing an immediate
parent_segment_id, which a corpus need not ever satisfy for two different
clients. discover_cross_client() pairs each client's own broadest
(client-only-scoped) Project population against every other client's, within
the same unit_system, independent of segment lineage.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from compare_cross_segment import (  # noqa: E402
    _is_client_only_project_segment,
    discover_cross_client,
)


def _seg(role, client="", unit="imperial", discipline="", bc="", collection="", run_type="bundle"):
    return {
        "governance_role": role,
        "client_label": client,
        "unit_system": unit,
        "discipline_label": discipline,
        "business_center_label": bc,
        "collection_label": collection,
        "run_type": run_type,
    }


# ---------------------------------------------------------------------------
# _is_client_only_project_segment()
# ---------------------------------------------------------------------------

def test_is_client_only_project_segment_true_for_bare_client_scope():
    assert _is_client_only_project_segment(_seg("Project", client="Kaiser")) is True


def test_is_client_only_project_segment_false_for_non_project_role():
    assert _is_client_only_project_segment(_seg("Container", client="Kaiser")) is False


def test_is_client_only_project_segment_false_when_client_blank():
    assert _is_client_only_project_segment(_seg("Project", client="")) is False
    assert _is_client_only_project_segment(_seg("Project", client="n/a")) is False


def test_is_client_only_project_segment_false_when_further_scoped_by_discipline():
    assert _is_client_only_project_segment(
        _seg("Project", client="Kaiser", discipline="architectural")
    ) is False


def test_is_client_only_project_segment_false_when_further_scoped_by_bc():
    assert _is_client_only_project_segment(
        _seg("Project", client="Kaiser", bc="BC_1")
    ) is False


def test_is_client_only_project_segment_false_when_further_scoped_by_collection():
    assert _is_client_only_project_segment(
        _seg("Project", client="Kaiser", collection="2014")
    ) is False


# ---------------------------------------------------------------------------
# discover_cross_client()
# ---------------------------------------------------------------------------

def test_discover_cross_client_pairs_distinct_clients_same_unit():
    manifest = {
        "p_kaiser": _seg("Project", client="Kaiser"),
        "p_sutter": _seg("Project", client="Sutter"),
    }
    pairs = discover_cross_client(manifest)
    assert ("p_kaiser", "p_sutter", "cross_client") in pairs
    assert len(pairs) == 1


def test_discover_cross_client_no_pair_across_different_unit_systems():
    manifest = {
        "p_kaiser": _seg("Project", client="Kaiser", unit="imperial"),
        "p_sutter": _seg("Project", client="Sutter", unit="metric"),
    }
    assert discover_cross_client(manifest) == []


def test_discover_cross_client_excludes_discipline_scoped_project_segments():
    """A client's discipline-scoped Project child must not participate --
    only the client's own broadest (client-only) segment is a peer of another
    client's broadest segment; mixing grains would compare a narrower
    population against a broader one."""
    manifest = {
        "p_kaiser": _seg("Project", client="Kaiser"),
        "p_kaiser_arch": _seg("Project", client="Kaiser", discipline="architectural"),
        "p_sutter": _seg("Project", client="Sutter"),
    }
    pairs = discover_cross_client(manifest)
    assert ("p_kaiser", "p_sutter", "cross_client") in pairs
    assert not any("p_kaiser_arch" in (a, b) for a, b, _ in pairs)


def test_discover_cross_client_excludes_non_project_roles():
    manifest = {
        "t_kaiser": _seg("Template", client="Kaiser"),
        "p_sutter": _seg("Project", client="Sutter"),
    }
    assert discover_cross_client(manifest) == []


def test_discover_cross_client_excludes_registration_only_segments():
    manifest = {
        "p_kaiser": _seg("Project", client="Kaiser", run_type="registration"),
        "p_sutter": _seg("Project", client="Sutter"),
    }
    assert discover_cross_client(manifest) == []


def test_discover_cross_client_three_clients_produces_all_pairs():
    manifest = {
        "p_a": _seg("Project", client="A"),
        "p_b": _seg("Project", client="B"),
        "p_c": _seg("Project", client="C"),
    }
    pairs = {(a, b) for a, b, ctype in discover_cross_client(manifest) if ctype == "cross_client"}
    assert pairs == {("p_a", "p_b"), ("p_a", "p_c"), ("p_b", "p_c")}


def test_discover_cross_client_no_self_pair_or_reverse_duplicate():
    manifest = {
        "p_a": _seg("Project", client="A"),
        "p_b": _seg("Project", client="B"),
    }
    pairs = discover_cross_client(manifest)
    assert ("p_b", "p_a", "cross_client") not in pairs
    assert ("p_a", "p_a", "cross_client") not in pairs
