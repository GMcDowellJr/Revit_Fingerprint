"""Importable runner context construction; deliberately free of Revit/Dynamo imports."""
from __future__ import annotations

import os
from pathlib import Path

from core.deployment_config import load_deployment_config
from core.join_key_policy import load_join_key_policies
from core.sig_hash_policy import load_sig_hash_policies

DEPLOYMENT_CONFIG_ENV = "REVIT_FINGERPRINT_DEPLOYMENT_CONFIG"


def operator_deployment_config_path(environ=None):
    """Read the deployment path at the single operator/environment boundary."""
    value = (environ if environ is not None else os.environ).get(DEPLOYMENT_CONFIG_ENV, "")
    return str(value).strip() or None


def build_extraction_context(repo_root, deployment_config_path=None):
    """Build and validate context portions required before any domain executes."""
    root = Path(repo_root).expanduser().resolve()
    ctx = {"debug_vg_details": False}
    ctx.update(load_deployment_config(
        deployment_config_path, root / "contracts" / "domain_identity_keys_v2.json"
    ))
    ctx["join_key_policies"] = load_join_key_policies(root / "policies" / "domain_join_key_policies.json")
    ctx["name_key_policies"] = load_join_key_policies(root / "policies" / "domain_name_key_policies.json")
    ctx["sig_hash_policies"] = load_sig_hash_policies(root / "policies" / "domain_sig_hash_policies.json")
    return ctx
