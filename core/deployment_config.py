"""Deployment-local extraction configuration loader (no checked-in identities)."""
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, Optional, Union

CONFIG_SCHEMA = "revit_fingerprint.deployment.v1"


def load_deployment_config(path: Optional[Union[str, Path]], contract_path: Union[str, Path]) -> Dict[str, Any]:
    """Validate a deployment file against the maintained identity contract."""
    if not path:
        return {"project_info_shared_parameters": []}
    config_path = Path(path).expanduser().resolve()
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    if payload.get("schema") != CONFIG_SCHEMA:
        raise ValueError("deployment configuration schema must be {}".format(CONFIG_SCHEMA))
    fields = payload.get("project_info_shared_parameters", [])
    contract = json.loads(Path(contract_path).read_text(encoding="utf-8"))
    allowed = set(contract["domains"]["identity"]["allowed_keys"])
    from domains.identity import validate_project_info_shared_parameters
    return {"project_info_shared_parameters": validate_project_info_shared_parameters(fields, allowed)}
