"""Immutable enterprise-identity policy loading and artifact provenance."""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict, Optional, Union

POLICY_SCHEMA = "enterprise_policy.v1"
DEFAULT_ENTERPRISE_LABEL = "InternalEnterprise"
DEFAULT_ENTERPRISE_BC_TOKEN = "0000"


@dataclass(frozen=True)
class EnterprisePolicy:
    """Effective identity boundary shared by governance tools."""

    enterprise_label: str
    normalized_enterprise_label: str
    enterprise_business_center_token: str
    source: str
    policy_path: Optional[str] = None
    schema: str = POLICY_SCHEMA

    def is_enterprise(self, client_label: str) -> bool:
        return (client_label or "").strip().casefold() == self.normalized_enterprise_label

    def provenance(self) -> Dict[str, Any]:
        value: Dict[str, Any] = {
            "schema": self.schema,
            "enterprise_label": self.enterprise_label,
            "enterprise_business_center_token": self.enterprise_business_center_token,
            "source": self.source,
        }
        if self.policy_path:
            value["policy_path"] = self.policy_path
        return value


def _label(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("enterprise label must be a non-blank string")
    return value.strip()


def load_enterprise_policy(
    policy_path: Optional[Union[str, Path]] = None,
    enterprise_label: Optional[str] = None,
) -> EnterprisePolicy:
    """Load default/file policy, then apply the CLI label override (highest precedence)."""
    label = DEFAULT_ENTERPRISE_LABEL
    source = "checked_in_default"
    safe_path = None
    if policy_path:
        path = Path(policy_path).expanduser().resolve()
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("schema") != POLICY_SCHEMA:
            raise ValueError("enterprise policy schema must be {}".format(POLICY_SCHEMA))
        label = _label(payload.get("enterprise_label"))
        token = payload.get("enterprise_business_center_token", DEFAULT_ENTERPRISE_BC_TOKEN)
        if token != DEFAULT_ENTERPRISE_BC_TOKEN:
            raise ValueError("enterprise business-center bookkeeping token must be 0000")
        source, safe_path = "policy_file", path.name
    if enterprise_label is not None:
        label, source = _label(enterprise_label), "cli_override"
    return EnterprisePolicy(label, label.casefold(), DEFAULT_ENTERPRISE_BC_TOKEN, source, safe_path)


def normalize_enterprise_label(value: Optional[str] = None) -> str:
    """Compatibility helper; new code should pass :class:`EnterprisePolicy`."""
    return load_enterprise_policy(enterprise_label=value).enterprise_label if value is not None else DEFAULT_ENTERPRISE_LABEL


def write_enterprise_policy_provenance(out_dir: Union[str, Path], policy: EnterprisePolicy) -> Path:
    """Write deterministic provenance after callers have validated write intent."""
    path = Path(out_dir) / "enterprise_policy.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(policy.provenance(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path
