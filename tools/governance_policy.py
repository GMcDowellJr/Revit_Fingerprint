"""
governance_policy.py

Generic JSON policy-profile loader for tools/generate_governance_narrative.py's
externalized governance policy: thresholds, domain-governance policy (excluded/
passive-inheritance-risk domains, domain guidance text), client-onboarding
interpretation thresholds, and finding-rule documentation
(policies/governance/*.json).

Mechanical load/fallback/validation only -- this module owns no governance
business content itself. The default threshold VALUES and domain-governance
business logic (TIER_*, PASSIVE_INHERITANCE_RISK_DOMAINS, assign_tier(), etc.)
stay in generate_governance_narrative.py, which owns that logic, matching the
separation of concerns tools/governance_evidence_package.py already
established for the generic package/envelope layer (see that module's own
docstring and docs/governance_evidence_package.md). A profile file that is
present but not valid JSON is a real authoring error and raises; a profile
file that is simply absent from --policy-dir is not an error -- the caller's
own default is used instead, mirroring load_client_sectors()'s existing
absent-file-is-not-an-error convention in generate_governance_narrative.py.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

# Resolved relative to this script's own directory (tools/), not the CWD --
# same convention as generate_governance_narrative.py's _DEFAULT_CLIENT_SECTOR_PATH,
# so existing invocations that don't pass --policy-dir still pick up the
# shipped policy profiles without requiring every caller to learn a new flag.
DEFAULT_POLICY_DIR = Path(__file__).resolve().parent.parent / "policies" / "governance"

THRESHOLDS_FILENAME = "governance_thresholds.json"
DOMAIN_POLICY_FILENAME = "domain_governance_policy.json"
CLIENT_ONBOARDING_FILENAME = "client_onboarding_policy.json"
FINDING_RULES_FILENAME = "finding_rules.json"

_PROFILE_FILENAMES = {
    "thresholds": THRESHOLDS_FILENAME,
    "domain_policy": DOMAIN_POLICY_FILENAME,
    "client_onboarding": CLIENT_ONBOARDING_FILENAME,
    "finding_rules": FINDING_RULES_FILENAME,
}


def _load_profile(policy_dir: Optional[Path], filename: str, default: dict) -> tuple[dict, dict]:
    """Return (effective_profile, load_status).

    load_status is a small dict describing where the effective profile came
    from -- consumed by governance_package_health.json's policy reporting so
    a downstream reader can tell "this run used the shipped default because
    no file was found" apart from "this run used a caller-supplied override".
    """
    if policy_dir is None:
        return default, {"source": "built_in_default", "path": None, "reason": "no_policy_dir"}
    path = Path(policy_dir) / filename
    if not path.exists():
        return default, {"source": "built_in_default", "path": str(path), "reason": "file_not_found"}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload, {"source": "policy_file", "path": str(path), "reason": None}


def load_governance_policy(policy_dir: Optional[Path], defaults: dict) -> dict:
    """Load all four governance policy profiles from policy_dir, falling back
    per-file to the caller-supplied default profile dict when a given file is
    absent (or policy_dir itself is None).

    defaults: {"thresholds": {...}, "domain_policy": {...},
               "client_onboarding": {...}, "finding_rules": {...}}
    -- each a complete default profile dict (including its own profile_id/
    schema_version/notes keys), used verbatim when the corresponding JSON
    file is not found. Passing an incomplete `defaults` dict is a caller
    programming error and raises KeyError, not silently skipped.

    Returns:
      {
        "policy_dir": str | None,
        "profiles": {"thresholds": {...}, "domain_policy": {...},
                      "client_onboarding": {...}, "finding_rules": {...}},
        "load_status": {"thresholds": {...}, ...},  # see _load_profile()
      }
    """
    profiles = {}
    load_status = {}
    for key, filename in _PROFILE_FILENAMES.items():
        profile, status = _load_profile(policy_dir, filename, defaults[key])
        profiles[key] = profile
        load_status[key] = status
    return {
        "policy_dir": str(policy_dir) if policy_dir else None,
        "profiles": profiles,
        "load_status": load_status,
    }
