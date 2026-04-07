from pathlib import Path

from tools.bundle_analysis.reference_bundle import load_and_validate


def test_load_and_validate_allows_legacy_control_characters(tmp_path: Path) -> None:
    sidecar = tmp_path / "reference_bundle.json"
    sidecar.write_text(
        (
            '{\n'
            '  "reference_bundle_id": "seed-2026-04-07",\n'
            '  "effective_date": "2026-04-07",\n'
            '  "extractor_schema_version": "v21",\n'
            '  "seed_export_run_id": "run\twith-tab",\n'
            '  "domains": {"A": ["P1"]}\n'
            '}\n'
        ),
        encoding="utf-8",
    )

    payload = load_and_validate(tmp_path, "v21")

    assert payload["seed_export_run_id"] == "run\twith-tab"


def test_load_and_validate_allows_raw_newline_in_string(tmp_path: Path) -> None:
    sidecar = tmp_path / "reference_bundle.json"
    sidecar.write_text(
        (
            '{\n'
            '  "reference_bundle_id": "seed-2026-04-07",\n'
            '  "effective_date": "2026-04-07",\n'
            '  "extractor_schema_version": "v21",\n'
            '  "seed_export_run_id": "run\n'
            'with-newline",\n'
            '  "domains": {"A": ["P1"]}\n'
            '}\n'
        ),
        encoding="utf-8",
    )

    payload = load_and_validate(tmp_path, "v21")

    assert payload["seed_export_run_id"] == "run\nwith-newline"
