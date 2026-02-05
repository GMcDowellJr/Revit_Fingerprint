# -*- coding: utf-8 -*-
"""Join-key builder from policy.

Inputs are explicit (policy + provided field/value sources).
No inference/fallback to uid/name/id when policy excludes them.

Schema Extension (v2): Shape-Gated Requirements
-----------------------------------------------
Policies can define shape-specific required items via `shape_gating`:

{
  "join_key_schema": "dimension_types.join_key.v3",
  "required_items": ["dim_type.accuracy"],  // Common required (all shapes)
  "optional_items": [...],
  "shape_gating": {
    "discriminator_key": "dim_type.shape",  // Key used to determine shape
    "shape_requirements": {
      "Linear": {
        "additional_required": ["dim_type.witness_line_control"],
        "additional_optional": []
      },
      "LinearFixed": {
        "additional_required": ["dim_type.witness_line_control"],
        "additional_optional": []
      },
      "Radial": {
        "additional_required": ["dim_type.center_marks", "dim_type.center_mark_size"],
        "additional_optional": []
      }
    },
    "default_shape_behavior": "common_only"  // "common_only" | "block"
  }
}

Join Matching Algorithm:
1. Extract shape value from discriminator_key
2. Always include common required_items
3. If shape matches a key in shape_requirements:
   - Add additional_required items for that shape
   - Add additional_optional items for that shape
4. If shape doesn't match and default_shape_behavior == "common_only":
   - Only use common required/optional (no shape-specific items)
5. If shape doesn't match and default_shape_behavior == "block":
   - Mark record as blocked (shape not recognized)
"""

import re

from core.record_v2 import ITEM_Q_MISSING, ITEM_Q_OK
from core.phase2 import phase2_join_hash


_RE_BRACKETED_INDEX = re.compile(r"\[(\d+)\]")


def _dedupe_preserve_order(items):
    seen = set()
    out = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _items_to_kqv_map(items):
    """Map k -> (v,q) for identity_items-like dicts."""
    m = {}
    for it in items or []:
        try:
            k = it.get("k")
            if isinstance(k, str) and k not in m:
                m[k] = (it.get("v"), it.get("q"))
        except Exception:
            # Best-effort mapping; caller controls diagnostics.
            continue
    return m


def _infer_indexed_count(kqv, prefix):
    """Infer count for keys like f"{prefix}[###]." by scanning keys."""
    mx = -1
    for k in kqv.keys():
        if not isinstance(k, str):
            continue
        if not k.startswith(prefix + "["):
            continue
        m = _RE_BRACKETED_INDEX.search(k)
        if not m:
            continue
        try:
            idx = int(m.group(1))
            mx = max(mx, idx)
        except Exception:
            continue
    return (mx + 1) if mx >= 0 else 0


def _expand_sequence_key(seq_key, kqv):
    """Expand policy key with [] to indexed keys in order.

Supports:
  - vf.rules[].sig -> vf.rule[000..rule_count-1].sig
  - vfa.stack[].filter_sig_hash -> vfa.stack[000..filter_stack_count-1].filter_sig_hash
"""
    if "[]" not in seq_key:
        return [seq_key]

    # Split like: "vf.rules[].sig" -> head="vf.rules", tail="sig"
    head, tail = seq_key.split("[].", 1)

    # Determine count key + indexed head
    indexed_head = head
    count_key = None

    if head == "vf.rules":
        indexed_head = "vf.rule"
        count_key = "vf.rule_count"
    elif head == "vfa.stack":
        indexed_head = "vfa.stack"
        count_key = "vfa.filter_stack_count"
    else:
        # Generic heuristic: if trailing token endswith 's', drop it.
        if head.endswith("s"):
            indexed_head = head[:-1]

    count = None
    if count_key is not None:
        v, q = kqv.get(count_key, (None, None))
        if q == ITEM_Q_OK:
            try:
                count = int(v)
            except Exception:
                count = None

    if count is None:
        # Infer by scanning existing indexed keys.
        count = _infer_indexed_count(kqv, indexed_head)

    out = []
    for i in range(max(0, int(count))):
        out.append("%s[%03d].%s" % (indexed_head, i, tail))
    return out


def _get_shape_specific_requirements(domain_policy, kqv):
    """Extract shape-specific required/optional items based on discriminator value.

    Args:
        domain_policy: Policy dict with optional shape_gating section
        kqv: Key-value-quality map from identity_items

    Returns:
        tuple: (additional_required, additional_optional, shape_value, shape_matched)
            - additional_required: List of additional required keys for this shape
            - additional_optional: List of additional optional keys for this shape
            - shape_value: The detected shape value (or None)
            - shape_matched: True if shape matched a defined shape_requirement
    """
    shape_gating = (domain_policy or {}).get("shape_gating")
    if not isinstance(shape_gating, dict):
        return [], [], None, False

    discriminator_key = shape_gating.get("discriminator_key")
    if not isinstance(discriminator_key, str):
        return [], [], None, False

    shape_requirements = shape_gating.get("shape_requirements")
    if not isinstance(shape_requirements, dict):
        return [], [], None, False

    # Get shape value from kqv
    shape_v, shape_q = kqv.get(discriminator_key, (None, None))
    if shape_q != ITEM_Q_OK or shape_v is None:
        return [], [], None, False

    shape_value = str(shape_v).strip()
    if not shape_value:
        return [], [], None, False

    # Look up shape-specific requirements
    shape_req = shape_requirements.get(shape_value)
    if not isinstance(shape_req, dict):
        # Shape not found in requirements - use default behavior
        return [], [], shape_value, False

    additional_required = list(shape_req.get("additional_required") or [])
    additional_optional = list(shape_req.get("additional_optional") or [])

    return additional_required, additional_optional, shape_value, True


def build_join_key_from_policy(
    *,
    domain_policy,
    identity_items=None,
    candidate_kqv=None,
    include_optional_items=True,
    emit_keys_used=False,
    hash_optional_items=True,
    preserve_single_def_hash_passthrough=True,
):
    """Build join_key dict from a policy and available value sources.

    Supports shape-gated requirements via the `shape_gating` policy section.

    Args:
        domain_policy: Policy dict with required_items, optional_items, and optional shape_gating
        identity_items: list of {k,q,v} identity items
        candidate_kqv: optional dict k -> (v,q) that can supplement identity_items
        include_optional_items: if True, include optional items in join_key.items
        emit_keys_used: if True, add join_key.keys_used list for hash provenance
        hash_optional_items: if True, include optional items in join_hash computation
        preserve_single_def_hash_passthrough: if True, keep legacy invariant where
            a lone *_def_hash required item is used directly as join_hash

    Returns:
        tuple: (join_key_dict, missing_required_keys)
            - join_key_dict: Dict with schema, hash_alg, items, join_hash, and metadata
            - missing_required_keys: List of required keys that were not found

    Shape-gating behavior:
        1. Always includes common required_items
        2. If shape_gating is defined and shape matches a shape_requirement:
           - Adds additional_required items for that shape
           - Adds additional_optional items for that shape
        3. If shape doesn't match any shape_requirement:
           - Uses common required/optional only (default behavior)
    """
    kqv = _items_to_kqv_map(identity_items)
    if isinstance(candidate_kqv, dict):
        # Candidate values are additive; do not override existing identity values.
        for k, vq in candidate_kqv.items():
            if k not in kqv and isinstance(vq, tuple) and len(vq) == 2:
                kqv[k] = vq

    # Get common required/optional items
    req = list((domain_policy or {}).get("required_items") or [])
    opt = list((domain_policy or {}).get("optional_items") or [])

    # Get shape-specific requirements
    add_req, add_opt, shape_value, shape_matched = _get_shape_specific_requirements(
        domain_policy, kqv
    )
    req = _dedupe_preserve_order(req + add_req)
    opt = _dedupe_preserve_order(opt + add_opt)
    if req:
        req_set = set(req)
        opt = [k for k in opt if k not in req_set]

    missing_required = []
    required_items = []
    optional_items = []

    def emit_key(k):
        v, q = kqv.get(k, (None, None))
        if q is None:
            v, q = (None, ITEM_Q_MISSING)
        return {"k": k, "q": q, "v": v}

    for pol_key in req:
        for k in _expand_sequence_key(pol_key, kqv):
            if k not in kqv:
                missing_required.append(k)
            required_items.append(emit_key(k))

    if include_optional_items:
        for pol_key in opt:
            for k in _expand_sequence_key(pol_key, kqv):
                if k in kqv:
                    optional_items.append(emit_key(k))

    items = required_items + optional_items

    hash_items = items if hash_optional_items else required_items

    # Compute join_hash
    join_hash = None
    if (
        preserve_single_def_hash_passthrough
        and (
        len(hash_items) == 1
        and isinstance(hash_items[0].get("k"), str)
        and hash_items[0]["k"].endswith("_def_hash")
        and isinstance(hash_items[0].get("v"), str)
        and re.match(r"^[0-9a-f]{32}$", hash_items[0]["v"])
        )
    ):
        # Structured-domain invariant: def_hash IS the join_hash
        join_hash = hash_items[0]["v"]
    else:
        join_hash = phase2_join_hash(hash_items)

    keys_used = None
    if emit_keys_used:
        keys = sorted({it.get("k") for it in hash_items if isinstance(it.get("k"), str)})
        keys_used = keys

    join_key = {
        "schema": (domain_policy or {}).get("join_key_schema"),
        "hash_alg": (domain_policy or {}).get("hash_alg"),
        "items": items,
        "join_hash": join_hash,
    }
    if keys_used is not None:
        join_key["keys_used"] = keys_used

    if missing_required:
        join_key["missing_required"] = missing_required

    # Add shape-gating metadata if active
    shape_gating = (domain_policy or {}).get("shape_gating")
    if isinstance(shape_gating, dict) and shape_value is not None:
        join_key["shape_gating"] = {
            "discriminator_key": shape_gating.get("discriminator_key"),
            "shape_value": shape_value,
            "shape_matched": shape_matched,
            "additional_required_keys": add_req if shape_matched else [],
        }

    return join_key, missing_required
