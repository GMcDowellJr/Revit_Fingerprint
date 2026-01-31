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


def validate_domain_join_key_policy(domain_name, pol, *, exported_keys=None):
    """Validate shape-gating semantics and return structured issues.

    Args:
        domain_name: Name of domain for error messages
        pol: Policy dict
        exported_keys: Optional set of exported keys for orphan checks

    Returns:
        list[dict]: Issue dicts with severity, code, message, hint, path
    """
    issues = []

    def add_issue(severity, code, message, hint, path):
        issues.append(
            {
                "severity": severity,
                "code": code,
                "message": message,
                "hint": hint,
                "path": path,
            }
        )

    required_items = list(pol.get("required_items") or [])
    optional_items = list(pol.get("optional_items") or [])

    shape_gating = pol.get("shape_gating")
    if isinstance(shape_gating, dict):
        discriminator_key = shape_gating.get("discriminator_key")
        shape_requirements = shape_gating.get("shape_requirements")

        # Rule A1 — discriminator first required
        if discriminator_key not in required_items:
            add_issue(
                "error",
                "A1_DISCRIMINATOR_FIRST",
                "shape_gating.discriminator_key must appear in required_items",
                "Add discriminator_key to required_items as the first entry.",
                "required_items",
            )
        elif required_items and required_items[0] != discriminator_key:
            add_issue(
                "error",
                "A1_DISCRIMINATOR_FIRST",
                "shape_gating.discriminator_key must be the first required item",
                "Reorder required_items so the discriminator is first.",
                "required_items",
            )

        all_additional_required = []
        all_additional_optional = []

        if isinstance(shape_requirements, dict):
            # Rule A4 — shapes must declare non-empty additional_required
            for shape_name, shape_req in shape_requirements.items():
                if not isinstance(shape_req, dict):
                    add_issue(
                        "error",
                        "A4_SHAPE_REQUIRED_EMPTY",
                        "shape requirement must be an object with additional_required",
                        "Ensure each shape entry is an object with a non-empty additional_required list.",
                        f"shape_gating.shape_requirements.{shape_name}",
                    )
                    continue

                add_req = shape_req.get("additional_required")
                if not _is_list_of_str(add_req) or len(add_req) == 0:
                    add_issue(
                        "error",
                        "A4_SHAPE_REQUIRED_EMPTY",
                        "shape requirement must define a non-empty additional_required list",
                        "Provide at least one required key for this shape or remove the entry.",
                        f"shape_gating.shape_requirements.{shape_name}.additional_required",
                    )
                else:
                    all_additional_required.extend(list(add_req))

                add_opt = shape_req.get("additional_optional")
                if _is_list_of_str(add_opt):
                    all_additional_optional.extend(list(add_opt))

        # Rule A2 — no overlap: common required vs additional_required
        overlap = [k for k in all_additional_required if k in required_items]
        for k in overlap:
            add_issue(
                "error",
                "A2_OVERLAP_COMMON_REQUIRED",
                "additional_required must not overlap required_items",
                "Remove the key from common required_items or from the shape-specific additional_required list.",
                "required_items",
            )

        # Rule A3 — additional_required must exist in optional_items
        for k in all_additional_required:
            if k not in optional_items:
                add_issue(
                    "error",
                    "A3_REQUIRED_NOT_OPTIONAL",
                    "additional_required keys must also appear in optional_items",
                    "Add the key to optional_items so it remains available for scoring/analytics.",
                    "optional_items",
                )

        referenced_keys = set(required_items + optional_items)
        referenced_keys.update(all_additional_required)
        referenced_keys.update(all_additional_optional)
    else:
        referenced_keys = set(required_items + optional_items)

    # Rule A5 — orphaned keys (warning only)
    if exported_keys is not None:
        for k in referenced_keys:
            if k not in exported_keys:
                add_issue(
                    "warning",
                    "A5_ORPHANED_KEY",
                    "policy references a key not present in exported_keys",
                    "Verify the exporter emits this key or remove it from the policy.",
                    f"policy:{k}",
                )

    return issues


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
            issues = validate_domain_join_key_policy(domain_name, pol)
            error_issues = [i for i in issues if i.get("severity") == "error"]
            if error_issues:
                lines = [
                    "%s %s: %s" % (domain_name, i.get("code"), i.get("message"))
                    for i in error_issues
                ]
                raise ValueError("join-key policy validation errors:\n  - " + "\n  - ".join(lines))

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
