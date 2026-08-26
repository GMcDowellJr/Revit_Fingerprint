# Chunk of domains/fill_patterns.py

- Source relative path: `domains/fill_patterns.py`
- Chunk: 4 of 8
- Original line range: 933-1038
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: extract_drafting
- Source SHA-256: 30da073fc127a2ee2c9133e6348b0a2099f02ec5ae001d02fcf0ce69a1287358
- Starts inside symbol: extract_drafting
- Ends inside symbol: no

```
   933|         sig_hash_v2 = None if status_v2 == STATUS_BLOCKED else make_hash(sig_preimage_v2)
   934| 
   935|         # Selector-only phase2 semantic surface: no duplicated k/q/v evidence.
   936|         semantic_keys = sorted({it.get("k") for it in identity_items_v2_sorted if isinstance(it.get("k"), str)})
   937|         phase2_payload.pop("semantic_items", None)
   938|         phase2_payload["semantic_keys"] = semantic_keys
   939| 
   940|         # Policy-driven join_key from canonical evidence (identity_basis.items) only.
   941|         # Optional keys stay in identity evidence for future exploration but are not hashed by default.
   942|         pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
   943|         join_key, _missing = build_join_key_from_policy(
   944|             domain_policy=pol,
   945|             identity_items=identity_items_v2_sorted,
   946|             include_optional_items=False,
   947|             emit_keys_used=True,
   948|             hash_optional_items=False,
   949|             emit_items=False,
   950|             emit_selectors=True,
   951|         )
   952| 
   953|         # Canonical Name Identity Projection (PR1): second, independent join_hash variant
   954|         # keyed off this record's own label.display-backing item (fill_pattern.name).
   955|         # fill_pattern.name is not a member of identity_items_v2_sorted -- it lives only in
   956|         # the phase2 cosmetic bucket. Widened items list used only for this call;
   957|         # identity_basis.items/sig_hash/join_key above are unaffected.
   958|         fp_name_v, fp_name_q = canonicalize_str(name)
   959|         name_key_items = identity_items_v2_sorted + [
   960|             make_identity_item("fill_pattern.name", fp_name_v, fp_name_q)
   961|         ]
   962|         name_key_pol = get_domain_join_key_policy((ctx or {}).get("name_key_policies"), DOMAIN_NAME)
   963|         name_key, name_key_missing = build_join_key_from_policy(
   964|             domain_policy=name_key_pol,
   965|             identity_items=name_key_items,
   966|             include_optional_items=False,
   967|             emit_keys_used=True,
   968|             hash_optional_items=False,
   969|             emit_items=False,
   970|             emit_selectors=True,
   971|         )
   972|         name_key["status"] = compute_projection_status(name_key_pol, name_key_missing)
   973| 
   974|         rec_v2 = build_record_v2(
   975|             domain=DOMAIN_NAME,
   976|             record_id=safe_str(name) if safe_str(name) else safe_str(e.Id.IntegerValue),
   977|             status=status_v2,
   978|             status_reasons=sorted(set(status_reasons_v2)),
   979|             sig_hash=sig_hash_v2,
   980|             identity_items=identity_items_v2_sorted,
   981|             required_qs=required_qs,
   982|             label={
   983|                 "display": safe_str(name),
   984|                 "quality": "human",
   985|                 "provenance": "revit.FillPatternElement.Name",
   986|             },
   987|             debug={
   988|                 "sig_preimage_sample": sig_preimage_v2[:6],
   989|                 "uid_excluded_from_sig": True,
   990|             },
   991|         )
   992|         _ip, _ip_q = purge_lookup(getattr(getattr(e, "Id", None), "IntegerValue", None), ctx)
   993|         rec_v2["is_purgeable"] = _ip
   994|         rec_v2["is_purgeable_q"] = _ip_q
   995|         rec_v2["join_key"] = join_key
   996|         rec_v2["join_key_name_identity"] = name_key
   997|         rec_v2["phase2"] = phase2_payload
   998|         rec_v2["sig_basis"] = {
   999|             "schema": "{}.sig_basis.v1".format(DOMAIN_NAME),
  1000|             "keys_used": semantic_keys,
  1001|         }
  1002| 
  1003|         # Keep legacy record additive payload aligned with record.v2 selectors.
  1004|         rec["join_key"] = join_key
  1005|         rec["phase2"] = phase2_payload
  1006| 
  1007|         v2_records.append(rec_v2)
  1008|         if sig_hash_v2 is not None:
  1009|             v2_sig_hashes.append(sig_hash_v2)
  1010|             if uid:
  1011|                 uid_to_hash_v2[uid] = sig_hash_v2
  1012|             id_to_value[safe_str(e.Id.IntegerValue)] = sig_hash_v2
  1013| 
  1014|         info["debug_kept"] += 1
  1015| 
  1016|         info["names"] = sorted(set(names))
  1017|     info["count"] = len(info["names"])
  1018|     info["records"] = v2_records
  1019| 
  1020|     # v2 finalize: block domain hash if any record is blocked
  1021|     info["signature_hashes_v2"] = sorted(v2_sig_hashes)
  1022|     if any((r or {}).get("status") == STATUS_BLOCKED for r in (v2_records or [])):
  1023|         info["hash_v2"] = None
  1024|     else:
  1025|         info["hash_v2"] = make_hash(info["signature_hashes_v2"])
  1026| 
  1027|     # Context mapping:
  1028|     # - UID lookup preserves existing dependency contract for readers.
  1029|     # - ElementId lookup and special values provide producer-side symbolic resolution.
  1030|     _export_fill_pattern_ctx(ctx, uid_to_hash_v2, id_to_value)
  1031| 
  1032|     info["record_rows"] = [{
  1033|         "record_key": safe_str(r.get("record_id", "")),
  1034|         "sig_hash":   safe_str(r.get("sig_hash", "")),
  1035|         "name":       safe_str((r.get("label", {}) or {}).get("display", "")),
  1036|     } for r in v2_records if isinstance(r, dict)]
  1037| 
  1038|     return info
```
