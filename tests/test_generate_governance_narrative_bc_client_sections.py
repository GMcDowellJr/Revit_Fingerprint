"""Tests for render_bc_composition_section() / render_client_bc_distribution_section()
in tools/generate_governance_narrative.py (relationship/topology evidence layer,
Deliverables 4-5)."""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from generate_governance_narrative import (  # noqa: E402
    render_bc_composition_section,
    render_client_bc_distribution_section,
)


def _bc_client_row(bc, client, project_count, project_file_count, pct_bc, pct_client):
    return {
        "business_center_label": bc,
        "client_label": client,
        "project_count": str(project_count),
        "project_file_count": str(project_file_count),
        "percentage_of_bc": f"{pct_bc:.6f}",
        "percentage_of_client": f"{pct_client:.6f}",
    }


def _client_bc_row(client, bc_count, business_centers, project_count, project_file_count):
    return {
        "client_label": client,
        "business_center_count": str(bc_count),
        "business_centers": business_centers,
        "project_count": str(project_count),
        "project_file_count": str(project_file_count),
    }


def test_bc_composition_section_absent_when_no_rows():
    assert render_bc_composition_section([]) is None


def test_bc_composition_section_lists_clients_by_descending_share():
    rows = [
        _bc_client_row("2014", "ClientBeta", 16, 62, 0.446043, 1.0),
        _bc_client_row("2014", "Renown", 1, 41, 0.294964, 1.0),
    ]
    md = render_bc_composition_section(rows)
    assert md is not None
    clientbeta_idx = md.index("ClientBeta")
    renown_idx = md.index("Renown")
    assert clientbeta_idx < renown_idx  # larger percentage_of_bc listed first
    assert "45%" in md  # dominant-client percentage read straight from the row


def test_client_bc_distribution_section_absent_when_no_rows():
    assert render_client_bc_distribution_section([], []) is None


def test_client_bc_distribution_section_renders_per_bc_rows_when_both_matrices_supplied():
    client_bc_rows = [_client_bc_row("ClientBeta", 2, "2014|1779", 20, 100)]
    bc_client_rows = [
        _bc_client_row("2014", "ClientBeta", 16, 60, 0.6, 0.6),
        _bc_client_row("1779", "ClientBeta", 4, 40, 0.4, 0.4),
    ]
    md = render_client_bc_distribution_section(client_bc_rows, bc_client_rows)
    assert md is not None
    assert "60 file(s)" in md  # per-BC file count only available from bc_client_rows
    assert "60%" in md and "40%" in md


def test_client_bc_distribution_falls_back_to_business_centers_list_when_bc_matrix_missing():
    # Both --governance-bc-client-matrix and --governance-client-bc-matrix are
    # independently optional; a caller can supply only the client-vantage
    # rollup. The section must not silently drop the BC breakdown when the
    # supplied artifact (governance_client_bc_matrix.csv's own ordered
    # business_centers column) already carries it.
    client_bc_rows = [_client_bc_row("ClientBeta", 2, "2014|1779", 20, 100)]
    md = render_client_bc_distribution_section(client_bc_rows, [])
    assert md is not None
    assert "2014" in md
    assert "1779" in md
    # No per-BC counts available from this source -- must not fabricate them.
    assert "file(s) (" not in md.split("### ClientBeta")[1]
    assert "Per-BC project/file counts unavailable" in md


def test_client_bc_distribution_no_fallback_bullets_when_business_centers_blank():
    client_bc_rows = [_client_bc_row("ClientBeta", 0, "", 0, 0)]
    md = render_client_bc_distribution_section(client_bc_rows, [])
    assert md is not None
    assert "ClientBeta" in md
    assert "Per-BC project/file counts unavailable" not in md
