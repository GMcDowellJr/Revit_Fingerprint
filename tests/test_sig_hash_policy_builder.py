import os

from core.record_v2 import ITEM_Q_MISSING, ITEM_Q_OK, make_identity_item, serialize_identity_items
from core.hashing import make_hash
from core.sig_hash_builder import build_sig_hash_from_policy, apply_sig_hash_policy_to_record
from core.sig_hash_policy import load_sig_hash_policies, get_domain_sig_hash_policy, resolve_sig_hash_keys


def test_generated_sig_hash_policy_loads():
    policies = load_sig_hash_policies(os.path.join("policies", "domain_sig_hash_policies.json"))
    assert policies["version"] == "domain_sig_hash_policies.v1"
    assert get_domain_sig_hash_policy(policies, "line_patterns") is not None


def test_sig_hash_builder_hashes_allowed_items_from_items_list_order_independent():
    policy = {
        "sig_hash_schema": "x.sig_hash.v1",
        "hash_alg": "md5_utf8_join_pipe",
        "allowed_items": ["b", "a"],
        "allowed_item_prefixes": [],
        "required_items": ["a"],
        "minima": {"block_if_any_required_not_ok": True},
    }
    items = [make_identity_item("b", "2", ITEM_Q_OK), make_identity_item("a", "1", ITEM_Q_OK)]
    sig_hash, status, reasons, hash_items = build_sig_hash_from_policy(domain_policy=policy, items=items)
    assert status == "ok"
    assert reasons == []
    assert sig_hash == make_hash(serialize_identity_items(items))
    assert [it["k"] for it in hash_items] == ["b", "a"]


def test_sig_hash_builder_blocks_when_required_not_ok():
    policy = {
        "sig_hash_schema": "x.sig_hash.v1",
        "hash_alg": "md5_utf8_join_pipe",
        "allowed_items": ["a"],
        "allowed_item_prefixes": [],
        "required_items": ["a"],
        "minima": {"block_if_any_required_not_ok": True},
    }
    item = make_identity_item("a", None, ITEM_Q_MISSING)
    sig_hash, status, reasons, hash_items = build_sig_hash_from_policy(
        domain_policy=policy,
        items=[item],
    )
    # Blocked records still produce a hash for traceability (based on available items).
    assert sig_hash == make_hash(serialize_identity_items(hash_items))
    assert status == "blocked"
    assert "identity.incomplete:required_not_ok:a" in reasons


def test_sig_hash_builder_degrades_when_optional_hash_item_not_ok():
    # A non-required item that is still part of the hash preimage (allowed_items)
    # must not be silently invisible to status -- it degrades (never blocks) the
    # record even though only required_items gates the blocked path.
    policy = {
        "sig_hash_schema": "x.sig_hash.v1",
        "hash_alg": "md5_utf8_join_pipe",
        "allowed_items": ["a", "b"],
        "allowed_item_prefixes": [],
        "required_items": ["a"],
        "minima": {"block_if_any_required_not_ok": True},
    }
    items = [
        make_identity_item("a", "1", ITEM_Q_OK),
        make_identity_item("b", None, ITEM_Q_MISSING),
    ]
    sig_hash, status, reasons, hash_items = build_sig_hash_from_policy(domain_policy=policy, items=items)
    assert status == "degraded"
    assert sig_hash is not None
    assert "identity.incomplete:optional_not_ok:b" in reasons
    assert [it["k"] for it in hash_items] == ["a", "b"]


def test_sig_hash_builder_degrades_when_required_not_ok_and_block_disabled():
    policy = {
        "allowed_items": ["a"],
        "allowed_item_prefixes": [],
        "required_items": ["a"],
        "minima": {"block_if_any_required_not_ok": False},
    }
    sig_hash, status, reasons, _ = build_sig_hash_from_policy(domain_policy=policy, items=[{"k": "a", "v": None, "q": "missing"}])
    assert status == "degraded"
    assert sig_hash is not None
    assert "identity.incomplete:required_not_ok:a" in reasons


def test_sig_hash_builder_prefix_and_first_writer_wins():
    policy = {
        "allowed_items": [],
        "allowed_item_prefixes": ["x."],
        "required_items": ["x.a"],
        "minima": {"block_if_any_required_not_ok": True},
    }
    items = [
        {"k": "x.a", "v": "1", "q": "ok"},
        {"k": "x.a", "v": "2", "q": "missing"},
        {"k": "x.b", "v": "3", "q": "ok"},
    ]
    sig_hash, status, _, hash_items = build_sig_hash_from_policy(domain_policy=policy, items=items)
    assert status == "ok"
    assert sig_hash is not None
    assert [it["k"] for it in hash_items] == ["x.a", "x.a", "x.b"]


def test_apply_sig_hash_policy_to_record_uses_items_and_writes_sig_basis():
    policy = {
        "sig_hash_schema": "x.sig_hash.v1",
        "hash_alg": "md5_utf8_join_pipe",
        "allowed_items": ["a"],
        "allowed_item_prefixes": [],
        "required_items": ["a"],
        "minima": {"block_if_any_required_not_ok": True},
    }
    record = {
        "status": "ok",
        "status_reasons": [],
        "items": [{"k": "a", "v": "1", "q": "ok"}],
    }
    out = apply_sig_hash_policy_to_record(record, policy)
    assert out["sig_hash"] == make_hash(serialize_identity_items(record["items"]))
    assert out["sig_basis"]["schema"] == "x.sig_hash.v1"
    assert out["sig_basis"]["keys_used"] == ["a"]
    assert "identity_basis" not in out
    assert "identity_quality" not in out


def test_text_types_sig_hash_excludes_name_includes_behavioral_items():
    # text_type.name was removed from allowed_keys (label-only, not behavioral).
    # Verify: (a) name is excluded from the hash, (b) all 12 behavioral items are hashed,
    # (c) extra non-allowed items in the input are silently filtered out.
    policy = get_domain_sig_hash_policy(load_sig_hash_policies(os.path.join("policies", "domain_sig_hash_policies.json")), "text_types")
    allowed = set(policy["allowed_items"])
    items = [
        {"k": "text_type.name", "v": "Notes-Medium", "q": "ok"},  # not in allowed_items
        {"k": "text_type.font", "v": "Arial", "q": "ok"},
        {"k": "text_type.size_in", "v": "1.000000", "q": "ok"},
        {"k": "text_type.width_factor", "v": "1.000000", "q": "ok"},
        {"k": "text_type.background", "v": "1", "q": "ok"},
        {"k": "text_type.line_weight", "v": "1", "q": "ok"},
        {"k": "text_type.color_rgb", "v": "0-0-0", "q": "ok"},
        {"k": "text_type.show_border", "v": "false", "q": "ok"},
        {"k": "text_type.leader_border_offset_in", "v": "1.000000", "q": "ok"},
        {"k": "text_type.tab_size_in", "v": "1.000000", "q": "ok"},
        {"k": "text_type.bold", "v": "false", "q": "ok"},
        {"k": "text_type.italic", "v": "false", "q": "ok"},
        {"k": "text_type.underline", "v": "false", "q": "ok"},
    ]
    sig_hash, status, _, hash_items = build_sig_hash_from_policy(domain_policy=policy, items=items)
    assert status == "ok"
    hashed_keys = [it["k"] for it in hash_items]
    assert "text_type.name" not in hashed_keys
    assert all(k in allowed for k in hashed_keys)
    behavioral_items = [it for it in items if it["k"] in allowed]
    expected = make_hash(serialize_identity_items(behavioral_items))
    assert sig_hash == expected


def test_object_styles_model_sig_hash_excludes_area9_additions():
    # Area 9 additions (can_add_subcategory/has_material_quantities/is_cuttable/parent_name)
    # register as identity_basis.items via allowed_keys in the registry, but sig_hash_keys
    # pins the policy's sig-hash preimage to the pre-existing set so the analysis-side
    # sig_hash_builder does not widen the hash to include them (open bucket question, see
    # CHANGELOG.md/PR description).
    policy = get_domain_sig_hash_policy(load_sig_hash_policies(os.path.join("policies", "domain_sig_hash_policies.json")), "object_styles_model")
    allowed = set(policy["allowed_items"])
    new_keys = {
        "obj_style.can_add_subcategory",
        "obj_style.has_material_quantities",
        "obj_style.is_cuttable",
        "obj_style.parent_name",
    }
    assert not (new_keys & allowed)

    items = [
        {"k": "obj_style.row_key", "v": "Walls|self", "q": "ok"},
        {"k": "obj_style.weight.projection", "v": "2", "q": "ok"},
        {"k": "obj_style.weight.cut", "v": "3", "q": "ok"},
        {"k": "obj_style.color.rgb", "v": "10-20-30", "q": "ok"},
        {"k": "obj_style.pattern_ref.sig_hash", "v": "a" * 32, "q": "ok"},
        {"k": "obj_style.material_sig_hash", "v": "b" * 32, "q": "ok"},
        {"k": "obj_style.can_add_subcategory", "v": "true", "q": "ok"},
        {"k": "obj_style.has_material_quantities", "v": "false", "q": "ok"},
        {"k": "obj_style.is_cuttable", "v": "false", "q": "ok"},
        {"k": "obj_style.parent_name", "v": None, "q": "missing"},
    ]
    sig_hash, status, _, hash_items = build_sig_hash_from_policy(domain_policy=policy, items=items)
    assert status == "ok"
    hashed_keys = [it["k"] for it in hash_items]
    assert not (new_keys & set(hashed_keys))
    behavioral_items = [it for it in items if it["k"] in allowed]
    expected = make_hash(serialize_identity_items(behavioral_items))
    assert sig_hash == expected


def test_resolve_sig_hash_keys_falls_back_when_no_policies_present():
    result = resolve_sig_hash_keys(None, "wall_types", ["a", "b"], ["a", "b"])
    assert result == ["a", "b"]

    result = resolve_sig_hash_keys({}, "wall_types", ["a", "b"], ["a", "b"])
    assert result == ["a", "b"]


def test_resolve_sig_hash_keys_falls_back_when_domain_not_in_policies():
    policies = {"domains": {"units": {"allowed_items": ["units.spec"]}}}
    result = resolve_sig_hash_keys(policies, "wall_types", ["a", "b"], ["a", "b"])
    assert result == ["a", "b"]


def test_resolve_sig_hash_keys_treats_validated_empty_allowed_items_as_legitimate_not_absence():
    # A validated empty allowed_items (with no prefixes) is a real policy decision --
    # "nothing is behavioral for this domain" -- not a signal to fall back. See P2
    # Codex review finding on the D-040 PR.
    assert resolve_sig_hash_keys({"domains": {"wall_types": {"allowed_items": []}}}, "wall_types", ["a"], ["a"]) == []


def test_resolve_sig_hash_keys_falls_back_when_allowed_items_malformed():
    assert resolve_sig_hash_keys({"domains": {"wall_types": {"allowed_items": None}}}, "wall_types", ["a"], ["a"]) == ["a"]
    assert resolve_sig_hash_keys({"domains": {"wall_types": {"allowed_items": ["a", 1]}}}, "wall_types", ["b"], ["b"]) == ["b"]


def test_resolve_sig_hash_keys_uses_ctx_policy_when_present():
    policies = {"domains": {"wall_types": {"allowed_items": ["wt.layer_count"]}}}
    result = resolve_sig_hash_keys(policies, "wall_types", ["wt.function", "wt.layer_count"], ["wt.function"])
    assert result == ["wt.layer_count"]


def test_resolve_sig_hash_keys_resolves_allowed_item_prefixes_against_candidate_keys():
    # P2 Codex review finding: allowed_item_prefixes must be resolved against the
    # keys actually present on the record (candidate_keys), the same way
    # core/sig_hash_builder.py's _key_allowed() does -- otherwise a domain relying
    # on prefixes (e.g. view_filter_definitions' "vf.rule[") would silently lose
    # those keys the moment it adopted this resolver.
    policies = {
        "domains": {
            "view_filter_definitions": {
                "allowed_items": ["vf.category_id"],
                "allowed_item_prefixes": ["vf.rule["],
            }
        }
    }
    candidate_keys = ["vf.category_id", "vf.rule[0].param", "vf.rule[1].op", "vf.unrelated"]
    result = resolve_sig_hash_keys(policies, "view_filter_definitions", candidate_keys, [])
    assert result == sorted(["vf.category_id", "vf.rule[0].param", "vf.rule[1].op"])


def _shape_gated_policy(applies_to_sig_hash):
    return {
        "sig_hash_schema": "x.sig_hash.v1",
        "hash_alg": "md5_utf8_join_pipe",
        "allowed_items": ["common", "gated_a", "gated_b"],
        "allowed_item_prefixes": [],
        "required_items": ["common"],
        "minima": {"block_if_any_required_not_ok": True},
        "shape_gating": {
            "discriminator_key": "shape",
            "applies_to_sig_hash": applies_to_sig_hash,
            "shape_requirements": {
                "Owner": {
                    "additional_required": ["gated_a"],
                    "additional_optional": [],
                },
                "OtherOwner": {
                    "additional_required": ["gated_b"],
                    "additional_optional": [],
                },
            },
            "default_shape_behavior": "common_only",
        },
    }


def test_sig_hash_builder_shape_gating_excludes_keys_not_owned_by_records_own_shape():
    # D-049 P1 fix: a policy that opts in via shape_gating.applies_to_sig_hash
    # must exclude a gated key from the hash preimage when the record's own
    # discriminator value doesn't own it -- even though the key is present
    # on the record and listed in allowed_items. This is what keeps the
    # analysis-side sig_hash stage consistent with a domain extractor (e.g.
    # domains/arrowheads.py) that now emits every style-specific field
    # unconditionally but still hash-gates by bucket ownership.
    policy = _shape_gated_policy(applies_to_sig_hash=True)
    items = [
        make_identity_item("common", "1", ITEM_Q_OK),
        make_identity_item("shape", "NotOwner", ITEM_Q_OK),
        make_identity_item("gated_a", "2", ITEM_Q_OK),
        make_identity_item("gated_b", "3", ITEM_Q_OK),
    ]
    sig_hash, status, reasons, hash_items = build_sig_hash_from_policy(domain_policy=policy, items=items)
    hashed_keys = [it["k"] for it in hash_items]
    assert hashed_keys == ["common"]
    assert status == "ok"
    assert sig_hash == make_hash(serialize_identity_items([items[0]]))

    # Same items, but the record's own shape now owns gated_a.
    items_owner = [
        make_identity_item("common", "1", ITEM_Q_OK),
        make_identity_item("shape", "Owner", ITEM_Q_OK),
        make_identity_item("gated_a", "2", ITEM_Q_OK),
        make_identity_item("gated_b", "3", ITEM_Q_OK),
    ]
    sig_hash2, status2, _, hash_items2 = build_sig_hash_from_policy(domain_policy=policy, items=items_owner)
    hashed_keys2 = [it["k"] for it in hash_items2]
    assert hashed_keys2 == ["common", "gated_a"]
    assert status2 == "ok"
    assert sig_hash2 != sig_hash


def test_sig_hash_builder_shape_gating_without_opt_in_flag_is_a_no_op():
    # Without applies_to_sig_hash (the default/absent case -- e.g. today's
    # `identity` domain policy), shape_gating must have zero effect: every
    # allowed_items key hashes regardless of the record's shape. This is
    # what keeps domains that haven't opted in (identity) byte-identical to
    # their pre-D-049 analysis-side behavior.
    policy = _shape_gated_policy(applies_to_sig_hash=False)
    items = [
        make_identity_item("common", "1", ITEM_Q_OK),
        make_identity_item("shape", "NotOwner", ITEM_Q_OK),
        make_identity_item("gated_a", "2", ITEM_Q_OK),
        make_identity_item("gated_b", "3", ITEM_Q_OK),
    ]
    _sig_hash, _status, _reasons, hash_items = build_sig_hash_from_policy(domain_policy=policy, items=items)
    assert [it["k"] for it in hash_items] == ["common", "gated_a", "gated_b"]

    policy_no_flag = _shape_gated_policy(applies_to_sig_hash=False)
    del policy_no_flag["shape_gating"]["applies_to_sig_hash"]
    _sig_hash, _status, _reasons, hash_items2 = build_sig_hash_from_policy(domain_policy=policy_no_flag, items=items)
    assert [it["k"] for it in hash_items2] == ["common", "gated_a", "gated_b"]


def test_resolve_sig_hash_keys_real_policy_file_matches_hardcoded_fallbacks():
    """D-039/D-040/D-042 drift guard: for every domain that resolve_sig_hash_keys()
    is actually wired into (wall/floor/roof/ceiling_types, units, worksets,
    worksets_doc, browser_organization, object_styles x4, arrowheads, text_types),
    the real compiled policy's allowed_items must equal what each domain module
    declares as its own hardcoded fallback -- if these two ever diverge, the
    module falls back to a stale value in any ctx lacking sig_hash_policies
    (e.g. a unit test, or a caller that hasn't adopted runner/extraction_context.py
    yet), silently reproducing the exact class of bug D-039 fixed. D-042 (an
    automated PR review finding) was exactly this: object_styles was missing from
    this check's domain list, so this test never actually verified it, and the
    real compiled policy had silently carried obj_style.row_key -- absent from
    _MODEL_SEMANTIC_KEYS/_NON_MODEL_SEMANTIC_KEYS -- with nothing catching it.
    """
    import domains.wall_types as wall_types
    import domains.floor_types as floor_types
    import domains.roof_types as roof_types
    import domains.ceiling_types as ceiling_types
    import domains.units as units
    import domains.worksets as worksets
    import domains.browser_organization as browser_organization
    import domains.object_styles as object_styles
    import domains.arrowheads as arrowheads
    import domains.text_types as text_types

    policies = load_sig_hash_policies(os.path.join("policies", "domain_sig_hash_policies.json"))

    checks = [
        ("wall_types", wall_types._WALL_TYPES_SIG_HASH_KEYS_FALLBACK),
        ("floor_types", floor_types._FLOOR_TYPES_SIG_HASH_KEYS_FALLBACK),
        ("roof_types", roof_types._ROOF_TYPES_SIG_HASH_KEYS_FALLBACK),
        ("ceiling_types", ceiling_types._CEILING_TYPES_SIG_HASH_KEYS_FALLBACK),
        ("units", units.UNITS_SEMANTIC_KEYS),
        ("units_doc", units.UNITS_DOC_SEMANTIC_KEYS),
        ("worksets", worksets.WORKSETS_SEMANTIC_KEYS),
        ("worksets_doc", worksets.WORKSETS_DOC_SEMANTIC_KEYS),
        ("browser_organization", browser_organization.BROWSER_ORGANIZATION_SEMANTIC_KEYS),
        ("object_styles_model", object_styles._MODEL_SEMANTIC_KEYS),
        ("object_styles_annotation", object_styles._NON_MODEL_SEMANTIC_KEYS),
        ("object_styles_analytical", object_styles._NON_MODEL_SEMANTIC_KEYS),
        ("object_styles_imported", object_styles._NON_MODEL_SEMANTIC_KEYS),
        ("arrowheads", arrowheads._ARROWHEADS_SIG_HASH_KEYS_FALLBACK),
        ("text_types", text_types.TEXT_TYPE_SEMANTIC_KEYS_FALLBACK),
    ]
    mismatches = {}
    for domain, fallback in checks:
        pol = get_domain_sig_hash_policy(policies, domain)
        allowed = set(pol["allowed_items"])
        if allowed != set(fallback):
            mismatches[domain] = {"policy": sorted(allowed), "fallback": sorted(set(fallback))}

    assert not mismatches, "fallback key set diverged from compiled policy: {}".format(mismatches)
