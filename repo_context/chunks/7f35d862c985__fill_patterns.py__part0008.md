# Chunk of domains/fill_patterns.py

- Source relative path: `domains/fill_patterns.py`
- Chunk: 8 of 8
- Original line range: 1840-1945
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: extract_model
- Source SHA-256: 30da073fc127a2ee2c9133e6348b0a2099f02ec5ae001d02fcf0ce69a1287358
- Starts inside symbol: extract_model
- Ends inside symbol: no

```
  1840|         sig_hash_v2 = None if status_v2 == STATUS_BLOCKED else make_hash(sig_preimage_v2)
  1841| 
  1842|         # Selector-only phase2 semantic surface: no duplicated k/q/v evidence.
  1843|         semantic_keys = sorted({it.get("k") for it in identity_items_v2_sorted if isinstance(it.get("k"), str)})
  1844|         phase2_payload.pop("semantic_items", None)
  1845|         phase2_payload["semantic_keys"] = semantic_keys
  1846| 
  1847|         # Policy-driven join_key from canonical evidence (identity_basis.items) only.
  1848|         # Optional keys stay in identity evidence for future exploration but are not hashed by default.
  1849|         pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
  1850|         join_key, _missing = build_join_key_from_policy(
  1851|             domain_policy=pol,
  1852|             identity_items=identity_items_v2_sorted,
  1853|             include_optional_items=False,
  1854|             emit_keys_used=True,
  1855|             hash_optional_items=False,
  1856|             emit_items=False,
  1857|             emit_selectors=True,
  1858|         )
  1859| 
  1860|         # Canonical Name Identity Projection (PR1): second, independent join_hash variant
  1861|         # keyed off this record's own label.display-backing item (fill_pattern.name).
  1862|         # fill_pattern.name is not a member of identity_items_v2_sorted -- it lives only in
  1863|         # the phase2 cosmetic bucket. Widened items list used only for this call;
  1864|         # identity_basis.items/sig_hash/join_key above are unaffected.
  1865|         fp_name_v, fp_name_q = canonicalize_str(name)
  1866|         name_key_items = identity_items_v2_sorted + [
  1867|             make_identity_item("fill_pattern.name", fp_name_v, fp_name_q)
  1868|         ]
  1869|         name_key_pol = get_domain_join_key_policy((ctx or {}).get("name_key_policies"), DOMAIN_NAME)
  1870|         name_key, name_key_missing = build_join_key_from_policy(
  1871|             domain_policy=name_key_pol,
  1872|             identity_items=name_key_items,
  1873|             include_optional_items=False,
  1874|             emit_keys_used=True,
  1875|             hash_optional_items=False,
  1876|             emit_items=False,
  1877|             emit_selectors=True,
  1878|         )
  1879|         name_key["status"] = compute_projection_status(name_key_pol, name_key_missing)
  1880| 
  1881|         rec_v2 = build_record_v2(
  1882|             domain=DOMAIN_NAME,
  1883|             record_id=safe_str(name) if safe_str(name) else safe_str(e.Id.IntegerValue),
  1884|             status=status_v2,
  1885|             status_reasons=sorted(set(status_reasons_v2)),
  1886|             sig_hash=sig_hash_v2,
  1887|             identity_items=identity_items_v2_sorted,
  1888|             required_qs=required_qs,
  1889|             label={
  1890|                 "display": safe_str(name),
  1891|                 "quality": "human",
  1892|                 "provenance": "revit.FillPatternElement.Name",
  1893|             },
  1894|             debug={
  1895|                 "sig_preimage_sample": sig_preimage_v2[:6],
  1896|                 "uid_excluded_from_sig": True,
  1897|             },
  1898|         )
  1899|         _ip, _ip_q = purge_lookup(getattr(getattr(e, "Id", None), "IntegerValue", None), ctx)
  1900|         rec_v2["is_purgeable"] = _ip
  1901|         rec_v2["is_purgeable_q"] = _ip_q
  1902|         rec_v2["join_key"] = join_key
  1903|         rec_v2["join_key_name_identity"] = name_key
  1904|         rec_v2["phase2"] = phase2_payload
  1905|         rec_v2["sig_basis"] = {
  1906|             "schema": "{}.sig_basis.v1".format(DOMAIN_NAME),
  1907|             "keys_used": semantic_keys,
  1908|         }
  1909| 
  1910|         # Keep legacy record additive payload aligned with record.v2 selectors.
  1911|         rec["join_key"] = join_key
  1912|         rec["phase2"] = phase2_payload
  1913| 
  1914|         v2_records.append(rec_v2)
  1915|         if sig_hash_v2 is not None:
  1916|             v2_sig_hashes.append(sig_hash_v2)
  1917|             if uid:
  1918|                 uid_to_hash_v2[uid] = sig_hash_v2
  1919|             id_to_value[safe_str(e.Id.IntegerValue)] = sig_hash_v2
  1920| 
  1921|         info["debug_kept"] += 1
  1922| 
  1923|         info["names"] = sorted(set(names))
  1924|     info["count"] = len(info["names"])
  1925|     info["records"] = v2_records
  1926| 
  1927|     # v2 finalize: block domain hash if any record is blocked
  1928|     info["signature_hashes_v2"] = sorted(v2_sig_hashes)
  1929|     if any((r or {}).get("status") == STATUS_BLOCKED for r in (v2_records or [])):
  1930|         info["hash_v2"] = None
  1931|     else:
  1932|         info["hash_v2"] = make_hash(info["signature_hashes_v2"])
  1933| 
  1934|     # Context mapping:
  1935|     # - UID lookup preserves existing dependency contract for readers.
  1936|     # - ElementId lookup and special values provide producer-side symbolic resolution.
  1937|     _export_fill_pattern_ctx(ctx, uid_to_hash_v2, id_to_value)
  1938| 
  1939|     info["record_rows"] = [{
  1940|         "record_key": safe_str(r.get("record_id", "")),
  1941|         "sig_hash":   safe_str(r.get("sig_hash", "")),
  1942|         "name":       safe_str((r.get("label", {}) or {}).get("display", "")),
  1943|     } for r in v2_records if isinstance(r, dict)]
  1944| 
  1945|     return info
```
