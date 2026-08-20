"""Dependency-neutral validation for deployment-owned extraction configuration."""
from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Union

CONFIG_SCHEMA = "revit_fingerprint.deployment.v1"
CONFIG_FIELDS = frozenset(("schema", "project_info_shared_parameters"))

# These keys are owned by the extractor and can never be shadowed by a deployment.
PROJECT_INFO_BUILTIN_KEYS = frozenset((
    "project_info.name", "project_info.number", "project_info.status",
    "project_info.address", "project_info.issue_date", "project_info.client_name",
    "project_info.building_name", "project_info.organization_name",
    "project_info.organization_description", "project_info.ifc_building_guid",
    "project_info.ifc_project_guid", "project_info.ifc_site_guid",
))


def validate_project_info_shared_parameters(fields: Any, allowed_keys: Optional[Iterable[str]] = None):
    """Validate and canonically normalize deployment mapping entries."""
    if not isinstance(fields, list):
        raise ValueError("project_info_shared_parameters must be a list")
    allowed = set(allowed_keys) if allowed_keys is not None else None
    seen_keys, seen_guids, normalized = set(), {}, []
    for field in fields:
        if not isinstance(field, dict):
            raise ValueError("each project_info_shared_parameters entry must be an object")
        unknown = set(field) - {"key", "name", "guid"}
        if unknown:
            raise ValueError("unknown project-information mapping fields: {}".format(", ".join(sorted(unknown))))
        key = field.get("key")
        name = field.get("name")
        guid = field.get("guid")
        key = key.strip() if isinstance(key, str) else ""
        name = name.strip() if isinstance(name, str) else ""
        guid = guid.strip() if isinstance(guid, str) else guid
        if not key.startswith("project_info.") or not name:
            raise ValueError("configured fields require a project_info.* key and non-blank name")
        if key in PROJECT_INFO_BUILTIN_KEYS:
            raise ValueError("configured key collides with a built-in field: {}".format(key))
        if allowed is not None and key not in allowed:
            raise ValueError("configured project-information key is not contract-registered: {}".format(key))
        if key in seen_keys:
            raise ValueError("duplicate configured project-information key: {}".format(key))
        canonical_guid = None
        if guid not in (None, ""):
            if not isinstance(guid, str):
                raise ValueError("malformed GUID for configured project-information field: {}".format(key))
            try:
                canonical_guid = str(uuid.UUID(guid))
            except (ValueError, AttributeError, TypeError):
                raise ValueError("malformed GUID for configured project-information field: {}".format(key))
            if canonical_guid in seen_guids and seen_guids[canonical_guid] != key:
                raise ValueError("configured GUID maps to conflicting keys")
            seen_guids[canonical_guid] = key
        seen_keys.add(key)
        normalized.append({"key": key, "name": name, "guid": canonical_guid})
    return normalized


def _identity_allowed_keys(contract_path: Union[str, Path]):
    contract = json.loads(Path(contract_path).read_text(encoding="utf-8"))
    try:
        keys = contract["domains"]["identity"]["allowed_keys"]
    except (KeyError, TypeError):
        raise ValueError("identity contract is malformed")
    if not isinstance(keys, list) or not all(isinstance(key, str) and key for key in keys):
        raise ValueError("identity contract allowed_keys must be a list of non-blank strings")
    return keys


def load_deployment_config(path: Optional[Union[str, Path]], contract_path: Union[str, Path]) -> Dict[str, Any]:
    """Load the closed v1 schema and validate mappings against the identity contract."""
    if not path:
        return {"project_info_shared_parameters": []}
    payload = json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("deployment configuration must be a JSON object")
    unknown = set(payload) - CONFIG_FIELDS
    if unknown:
        raise ValueError("unknown deployment configuration fields: {}".format(", ".join(sorted(unknown))))
    if payload.get("schema") != CONFIG_SCHEMA:
        raise ValueError("deployment configuration schema must be {}".format(CONFIG_SCHEMA))
    if "project_info_shared_parameters" not in payload:
        raise ValueError("deployment configuration requires project_info_shared_parameters")
    fields = validate_project_info_shared_parameters(
        payload["project_info_shared_parameters"], _identity_allowed_keys(contract_path)
    )
    return {"project_info_shared_parameters": fields}
