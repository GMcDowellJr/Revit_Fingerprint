from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, List


def write_sidecar(
    analysis_out_dir: Path,
    seed_export_run_id: str,
    domain_patterns_rows: List[Dict[str, str]],
    schema_version: str,
) -> Path:
    seed_rows = [row for row in domain_patterns_rows if str(row.get("is_seed", "")).strip().lower() == "true"]
    if not seed_rows:
        raise ValueError("Cannot write reference sidecar: no seed-tagged rows were provided.")

    today_iso = date.today().isoformat()
    seed_file_stem = str(seed_rows[0].get("seed_file_stem", "")).strip() or str(seed_export_run_id).strip()
    domains: Dict[str, List[str]] = {}
    for row in seed_rows:
        domain = str(row.get("domain", "")).strip()
        pattern_id = str(row.get("pattern_id", "")).strip()
        if not domain or not pattern_id:
            continue
        domains.setdefault(domain, [])
        if pattern_id not in domains[domain]:
            domains[domain].append(pattern_id)
    domains = {domain: sorted(patterns) for domain, patterns in sorted(domains.items())}
    if not domains:
        raise ValueError("Cannot write reference sidecar: seed rows did not contain any domain/pattern_id pairs.")

    payload = {
        "reference_bundle_id": f"{seed_file_stem}-{today_iso}",
        "effective_date": today_iso,
        "extractor_schema_version": str(schema_version),
        "seed_export_run_id": str(seed_export_run_id),
        "domains": domains,
    }

    analysis_out_dir.mkdir(parents=True, exist_ok=True)
    out_path = analysis_out_dir / "reference_bundle.json"
    with NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(analysis_out_dir), suffix=".tmp") as tmp:
        tmp_path = Path(tmp.name)
        json.dump(payload, tmp, indent=2, sort_keys=True)
        tmp.write("\n")
    tmp_path.replace(out_path)
    return out_path


def load_and_validate(analysis_out_dir: Path, current_schema_version: str) -> Dict[str, object]:
    path = analysis_out_dir / "reference_bundle.json"
    if not path.is_file():
        raise ValueError(f"Missing reference bundle sidecar: {path}")
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid reference bundle JSON shape in {path}: expected object root.")

    required = {
        "reference_bundle_id",
        "effective_date",
        "extractor_schema_version",
        "seed_export_run_id",
        "domains",
    }
    missing = [k for k in sorted(required) if k not in payload]
    if missing:
        raise ValueError(f"Invalid reference bundle: missing required field(s): {missing}")

    effective_date = str(payload.get("effective_date", "")).strip()
    try:
        date.fromisoformat(effective_date)
    except ValueError as exc:
        raise ValueError(f"Invalid reference bundle effective_date (expected ISO 8601 date): {effective_date!r}") from exc

    sidecar_schema = str(payload.get("extractor_schema_version", "")).strip()
    if sidecar_schema != str(current_schema_version):
        raise ValueError(
            "Reference bundle schema mismatch: "
            f"sidecar extractor_schema_version={sidecar_schema!r} "
            f"!= current schema_version={str(current_schema_version)!r}"
        )

    domains = payload.get("domains")
    if not isinstance(domains, dict) or not domains:
        raise ValueError("Invalid reference bundle: domains must be a non-empty object.")
    for domain, pattern_ids in domains.items():
        if not isinstance(domain, str) or not domain.strip():
            raise ValueError("Invalid reference bundle: domain keys must be non-empty strings.")
        if not isinstance(pattern_ids, list) or not pattern_ids:
            raise ValueError(f"Invalid reference bundle: domains[{domain!r}] must be a non-empty list.")
        for pid in pattern_ids:
            if not isinstance(pid, str) or not pid.strip():
                raise ValueError(f"Invalid reference bundle: domains[{domain!r}] contains an empty pattern id.")

    return payload
