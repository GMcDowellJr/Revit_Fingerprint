"""Single policy surface for deployment-specific enterprise identity."""
import json
from pathlib import Path

DEFAULT_ENTERPRISE_LABEL = "InternalEnterprise"

def normalize_enterprise_label(value=None):
    label = (value or DEFAULT_ENTERPRISE_LABEL).strip()
    if not label:
        raise ValueError("enterprise label must not be blank")
    return label

def write_enterprise_policy_provenance(out_dir, enterprise_label, source):
    payload = {"schema": "enterprise_policy.v1", "enterprise_label": normalize_enterprise_label(enterprise_label),
               "source": source, "enterprise_business_center_token": "0000"}
    path = Path(out_dir) / "enterprise_policy.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path
