# -*- coding: utf-8 -*-
"""Join-key builder from policy.

Inputs are explicit (policy + provided field/value sources).
No inference/fallback to uid/name/id when policy excludes them.
"""

import re

from core.record_v2 import ITEM_Q_MISSING, ITEM_Q_OK
from core.phase2 import phase2_join_hash


_RE_BRACKETED_INDEX = re.compile(r"\[(\d+)\]")


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


def build_join_key_from_policy(*, domain_policy, identity_items=None, candidate_kqv=None):
    """Build join_key dict from a policy and available value sources.

    - identity_items: list of {k,q,v}
    - candidate_kqv: optional dict k -> (v,q) that can supplement identity_items

    Returns:
      (join_key_dict, missing_required_keys)
    """
    kqv = _items_to_kqv_map(identity_items)
    if isinstance(candidate_kqv, dict):
        # Candidate values are additive; do not override existing identity values.
        for k, vq in candidate_kqv.items():
            if k not in kqv and isinstance(vq, tuple) and len(vq) == 2:
                kqv[k] = vq

    req = list((domain_policy or {}).get("required_items") or [])
    opt = list((domain_policy or {}).get("optional_items") or [])

    missing_required = []
    items = []

    def emit_key(k):
        v, q = kqv.get(k, (None, None))
        if q is None:
            v, q = (None, ITEM_Q_MISSING)
        return {"k": k, "q": q, "v": v}

    for pol_key in req:
        for k in _expand_sequence_key(pol_key, kqv):
            if k not in kqv:
                missing_required.append(k)
            items.append(emit_key(k))

    for pol_key in opt:
        for k in _expand_sequence_key(pol_key, kqv):
            if k in kqv:
                items.append(emit_key(k))

    join_key = {
        "schema": (domain_policy or {}).get("join_key_schema"),
        "hash_alg": (domain_policy or {}).get("hash_alg"),
        "items": items,
        "join_hash": phase2_join_hash(items),
    }
    if missing_required:
        join_key["missing_required"] = missing_required
    return join_key, missing_required
