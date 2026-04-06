from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from typing import Dict, List

if __package__ in (None, ""):
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from common import read_csv_rows
else:
    from .common import read_csv_rows


_REQUIRED_FIELDS = [
    "reference_bundle_id",
    "effective_date",
    "extractor_schema_version",
    "seed_export_run_id",
    "domains",
]


def _validate_effective_date(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Invalid effective_date: expected non-empty ISO 8601 date string")
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"Invalid effective_date: {value!r} is not parseable as ISO 8601 date"
        ) from exc
    return value


def _validate_domains(domains: object) -> Dict[str, List[str]]:
    if not isinstance(domains, dict) or not domains:
        raise ValueError("Invalid domains: expected non-empty object mapping domain -> pattern_id list")

    validated: Dict[str, List[str]] = {}
    for domain, pattern_ids in domains.items():
        if not isinstance(domain, str) or not domain.strip():
            raise ValueError("Invalid domains key: expected non-empty string domain name")
        if not isinstance(pattern_ids, list) or not pattern_ids:
            raise ValueError(f"Invalid domains[{domain!r}]: expected non-empty list of pattern IDs")

        clean_ids: List[str] = []
        for idx, pattern_id in enumerate(pattern_ids):
            if not isinstance(pattern_id, str) or not pattern_id.strip():
                raise ValueError(
                    f"Invalid domains[{domain!r}][{idx}]: expected non-empty string pattern ID"
                )
            clean_ids.append(pattern_id)
        validated[domain] = clean_ids

    return validated


def _validate_bundle(bundle: object, current_schema_version: str) -> Dict[str, object]:
    if not isinstance(bundle, dict):
        raise ValueError("Invalid reference bundle: expected top-level JSON object")

    missing = [field for field in _REQUIRED_FIELDS if field not in bundle]
    if missing:
        raise ValueError(f"Missing required field(s): {', '.join(missing)}")

    reference_bundle_id = bundle.get("reference_bundle_id")
    if not isinstance(reference_bundle_id, str) or not reference_bundle_id.strip():
        raise ValueError("Invalid reference_bundle_id: expected non-empty string")

    seed_export_run_id = bundle.get("seed_export_run_id")
    if not isinstance(seed_export_run_id, str) or not seed_export_run_id.strip():
        raise ValueError("Invalid seed_export_run_id: expected non-empty string")

    effective_date = _validate_effective_date(bundle.get("effective_date"))

    extractor_schema_version = bundle.get("extractor_schema_version")
    if not isinstance(extractor_schema_version, str) or not extractor_schema_version.strip():
        raise ValueError("Invalid extractor_schema_version: expected non-empty string")
    if extractor_schema_version != current_schema_version:
        raise ValueError(
            "Schema version mismatch: "
            f"reference extractor_schema_version={extractor_schema_version!r} "
            f"does not match current schema version {current_schema_version!r}"
        )

    domains = _validate_domains(bundle.get("domains"))

    return {
        "reference_bundle_id": reference_bundle_id,
        "effective_date": effective_date,
        "extractor_schema_version": extractor_schema_version,
        "seed_export_run_id": seed_export_run_id,
        "domains": domains,
    }


def load_and_validate(path: Path, current_schema_version: str) -> Dict[str, object]:
    try:
        raw_bundle = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in reference bundle file {path}: {exc}") from exc
    except OSError as exc:
        raise ValueError(f"Unable to read reference bundle file {path}: {exc}") from exc

    return _validate_bundle(raw_bundle, current_schema_version)


def derive_from_analysis_output(
    analysis_out_dir: Path,
    rvt_path: Path,
    current_schema_version: str,
) -> Dict[str, object]:
    domain_patterns_path = analysis_out_dir / "domain_patterns.csv"
    pattern_presence_path = analysis_out_dir / "pattern_presence_file.csv"

    domain_pattern_rows = read_csv_rows(domain_patterns_path)
    presence_rows = read_csv_rows(pattern_presence_path)

    export_run_ids = sorted(
        {
            (row.get("export_run_id", "") or "").strip()
            for row in presence_rows
            if (row.get("export_run_id", "") or "").strip()
        }
    )
    if len(export_run_ids) != 1:
        raise ValueError(
            "Invalid single-file extraction output: expected exactly one export_run_id in "
            f"{pattern_presence_path}, found {export_run_ids}"
        )
    seed_export_run_id = export_run_ids[0]

    schema_versions = sorted(
        {
            (row.get("schema_version", "") or "").strip()
            for row in (presence_rows + domain_pattern_rows)
            if (row.get("schema_version", "") or "").strip()
        }
    )
    if len(schema_versions) != 1:
        raise ValueError(
            "Invalid extraction schema version in analysis output: expected exactly one schema_version, "
            f"found {schema_versions}"
        )
    extractor_schema_version = schema_versions[0]

    domains: Dict[str, List[str]] = {}
    for row in domain_pattern_rows:
        domain = (row.get("domain", "") or "").strip()
        pattern_id = (row.get("pattern_id", "") or "").strip()
        if not domain or not pattern_id:
            continue
        domains.setdefault(domain, []).append(pattern_id)

    domains = {domain: sorted(set(pattern_ids)) for domain, pattern_ids in sorted(domains.items()) if pattern_ids}

    bundle = {
        "reference_bundle_id": f"{rvt_path.stem}-derived",
        "effective_date": date.today().isoformat(),
        "extractor_schema_version": extractor_schema_version,
        "seed_export_run_id": seed_export_run_id,
        "domains": domains,
    }

    sidecar_path = rvt_path.parent / f"{rvt_path.stem}_reference_bundle.json"
    sidecar_path.write_text(json.dumps(bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        f"[compare] Sidecar reference bundle written to {sidecar_path}. "
        "Review and set reference_bundle_id and effective_date before using as a standalone reference.",
        file=sys.stderr,
    )

    return _validate_bundle(bundle, current_schema_version)
