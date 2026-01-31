# -*- coding: utf-8 -*-
"""Join-key policy loader (explicit ctx injection only).

This module is intentionally small and deterministic.

Schema Extension (v2): Shape-Gated Requirements
-----------------------------------------------
Policies can optionally define a `shape_gating` section that specifies
shape-specific required/optional items. This enables domains like
dimension_types to have different join key compositions based on shape.

Example shape_gating section:
{
  "shape_gating": {
    "discriminator_key": "dim_type.shape",
    "shape_requirements": {
      "Linear": {
        "additional_required": ["dim_type.witness_line_control"],
        "additional_optional": []
      },
      "Radial": {
        "additional_required": ["dim_type.center_marks"],
        "additional_optional": []
      }
    },
    "default_shape_behavior": "common_only"
  }
}
"""

import json


def _is_list_of_str(x):
    return isinstance(x, list) and all(isinstance(s, str) for s in x)


def _validate_shape_gating(domain_name, shape_gating):
    """Validate shape_gating section of a policy.

    Args:
        domain_name: Name of domain for error messages
        shape_gating: The shape_gating dict to validate

    Raises:
        ValueError: If shape_gating structure is invalid
    """
    if not isinstance(shape_gating, dict):
        raise ValueError(
            "join-key policy '%s' shape_gating must be an object" % domain_name
        )

    # Required: discriminator_key
    disc_key = shape_gating.get("discriminator_key")
    if not isinstance(disc_key, str) or not disc_key:
        raise ValueError(
            "join-key policy '%s' shape_gating.discriminator_key must be a non-empty string"
            % domain_name
        )

    # Required: shape_requirements
    shape_reqs = shape_gating.get("shape_requirements")
    if not isinstance(shape_reqs, dict):
        raise ValueError(
            "join-key policy '%s' shape_gating.shape_requirements must be an object"
            % domain_name
        )

    # Validate each shape requirement
    for shape_name, shape_req in shape_reqs.items():
        if not isinstance(shape_name, str):
            raise ValueError(
                "join-key policy '%s' shape_gating.shape_requirements keys must be strings"
                % domain_name
            )
        if not isinstance(shape_req, dict):
            raise ValueError(
                "join-key policy '%s' shape_gating.shape_requirements['%s'] must be an object"
                % (domain_name, shape_name)
            )

        # Validate additional_required
        add_req = shape_req.get("additional_required")
        if add_req is not None and not _is_list_of_str(add_req):
            raise ValueError(
                "join-key policy '%s' shape_gating.shape_requirements['%s'].additional_required "
                "must be list[str]" % (domain_name, shape_name)
            )

        # Validate additional_optional
        add_opt = shape_req.get("additional_optional")
        if add_opt is not None and not _is_list_of_str(add_opt):
            raise ValueError(
                "join-key policy '%s' shape_gating.shape_requirements['%s'].additional_optional "
                "must be list[str]" % (domain_name, shape_name)
            )

    # Optional: default_shape_behavior
    default_behavior = shape_gating.get("default_shape_behavior", "common_only")
    valid_behaviors = ("common_only", "block")
    if default_behavior not in valid_behaviors:
        raise ValueError(
            "join-key policy '%s' shape_gating.default_shape_behavior must be one of %s"
            % (domain_name, valid_behaviors)
        )


def load_join_key_policies(policy_path):
    """Load and minimally validate the join-key policy JSON.

    Returns: dict (parsed JSON)
    Raises: Exception on IO/parse/shape errors.
    """
    with open(policy_path, "r") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError("join-key policy file must be a JSON object")

    domains = data.get("domains")
    if not isinstance(domains, dict):
        raise ValueError("join-key policy file must contain a top-level 'domains' object")

    for domain_name, pol in domains.items():
        if not isinstance(domain_name, str):
            raise ValueError("join-key policy domain keys must be strings")
        if not isinstance(pol, dict):
            raise ValueError("join-key policy for '%s' must be an object" % domain_name)

        # Required fields
        if not isinstance(pol.get("join_key_schema"), str):
            raise ValueError("join-key policy '%s' missing join_key_schema" % domain_name)
        if not isinstance(pol.get("hash_alg"), str):
            raise ValueError("join-key policy '%s' missing hash_alg" % domain_name)
        if not _is_list_of_str(pol.get("required_items")):
            raise ValueError("join-key policy '%s' required_items must be list[str]" % domain_name)
        if not _is_list_of_str(pol.get("optional_items")):
            raise ValueError("join-key policy '%s' optional_items must be list[str]" % domain_name)
        if not _is_list_of_str(pol.get("explicitly_excluded_items")):
            raise ValueError("join-key policy '%s' explicitly_excluded_items must be list[str]" % domain_name)

        # Optional fields
        notes = pol.get("notes", [])
        if notes is not None and not _is_list_of_str(notes):
            raise ValueError("join-key policy '%s' notes must be list[str]" % domain_name)

        # Optional: shape_gating (v2 extension)
        shape_gating = pol.get("shape_gating")
        if shape_gating is not None:
            _validate_shape_gating(domain_name, shape_gating)

    return data


def get_domain_join_key_policy(policies, domain_name):
    """Return the per-domain join-key policy or None."""
    if not isinstance(policies, dict):
        return None
    domains = policies.get("domains")
    if not isinstance(domains, dict):
        return None
    pol = domains.get(domain_name)
    return pol if isinstance(pol, dict) else None
