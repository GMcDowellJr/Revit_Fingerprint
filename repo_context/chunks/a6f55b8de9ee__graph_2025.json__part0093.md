# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 93 of 216
- Original line range: 35881-36280
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 35881|       "evidence": [
 35882|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 35883|       ],
 35884|       "source_url": "https://www.revitapidocs.com/2025/5571b4c3-08f4-c300-5d4e-90d405b1cb52.htm",
 35885|       "dll_signature_verified": true,
 35886|       "dll_relationship_scope": "declared",
 35887|       "dll_semantic_verified": null,
 35888|       "dll_verified_status": "signature_verified_declared",
 35889|       "revitlookup_referenced": null,
 35890|       "revitlookup_requires_document_context": null
 35891|     },
 35892|     {
 35893|       "source": "Autodesk.Revit.DB.CurtainGrid",
 35894|       "target": null,
 35895|       "member_name": "GetVGridLineIds",
 35896|       "member_kind": "method",
 35897|       "edge_type": "RETURNS_ELEMENT_IDS",
 35898|       "confidence": "unknown_reference",
 35899|       "confidence_tier": "unverified_reference",
 35900|       "target_resolution": "none",
 35901|       "evidence": [
 35902|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 35903|       ],
 35904|       "source_url": "https://www.revitapidocs.com/2025/23e62335-0db9-9b7d-079b-255ca5944c8e.htm",
 35905|       "dll_signature_verified": true,
 35906|       "dll_relationship_scope": "declared",
 35907|       "dll_semantic_verified": null,
 35908|       "dll_verified_status": "signature_verified_declared",
 35909|       "revitlookup_referenced": null,
 35910|       "revitlookup_requires_document_context": null
 35911|     },
 35912|     {
 35913|       "source": "Autodesk.Revit.DB.CurtainGridLine",
 35914|       "target": "Autodesk.Revit.DB.ElementSet",
 35915|       "member_name": "AddMullions",
 35916|       "member_kind": "method",
 35917|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35918|       "confidence": "direct_return_type",
 35919|       "confidence_tier": "unverified_reference",
 35920|       "target_resolution": "exact",
 35921|       "evidence": [
 35922|         "return type 'ElementSet' directly names a Revit DB object type"
 35923|       ],
 35924|       "source_url": "https://www.revitapidocs.com/2025/c6a78e90-5ce1-5bf3-cb82-f3de750e0255.htm",
 35925|       "dll_signature_verified": true,
 35926|       "dll_relationship_scope": "declared",
 35927|       "dll_semantic_verified": null,
 35928|       "dll_verified_status": "signature_verified_declared",
 35929|       "revitlookup_referenced": null,
 35930|       "revitlookup_requires_document_context": null
 35931|     },
 35932|     {
 35933|       "source": "Autodesk.Revit.DB.CurtainGridSet",
 35934|       "target": "Autodesk.Revit.DB.CurtainGridSetIterator",
 35935|       "member_name": "ForwardIterator",
 35936|       "member_kind": "method",
 35937|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35938|       "confidence": "direct_return_type",
 35939|       "confidence_tier": "unverified_reference",
 35940|       "target_resolution": "exact",
 35941|       "evidence": [
 35942|         "return type 'CurtainGridSetIterator' directly names a Revit DB object type"
 35943|       ],
 35944|       "source_url": "https://www.revitapidocs.com/2025/dd2e6123-4be9-162d-ee79-2d576c7e30ff.htm",
 35945|       "dll_signature_verified": true,
 35946|       "dll_relationship_scope": "declared",
 35947|       "dll_semantic_verified": null,
 35948|       "dll_verified_status": "signature_verified_declared",
 35949|       "revitlookup_referenced": null,
 35950|       "revitlookup_requires_document_context": null
 35951|     },
 35952|     {
 35953|       "source": "Autodesk.Revit.DB.CurtainGridSet",
 35954|       "target": "Autodesk.Revit.DB.CurtainGridSetIterator",
 35955|       "member_name": "ReverseIterator",
 35956|       "member_kind": "method",
 35957|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35958|       "confidence": "direct_return_type",
 35959|       "confidence_tier": "unverified_reference",
 35960|       "target_resolution": "exact",
 35961|       "evidence": [
 35962|         "return type 'CurtainGridSetIterator' directly names a Revit DB object type"
 35963|       ],
 35964|       "source_url": "https://www.revitapidocs.com/2025/86a0df75-ba39-6c56-a11d-62cafb423a0a.htm",
 35965|       "dll_signature_verified": true,
 35966|       "dll_relationship_scope": "declared",
 35967|       "dll_semantic_verified": null,
 35968|       "dll_verified_status": "signature_verified_declared",
 35969|       "revitlookup_referenced": null,
 35970|       "revitlookup_requires_document_context": null
 35971|     },
 35972|     {
 35973|       "source": "Autodesk.Revit.DB.CurtainSystem",
 35974|       "target": "Autodesk.Revit.DB.CurtainGridSet",
 35975|       "member_name": "CurtainGrids",
 35976|       "member_kind": "property",
 35977|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35978|       "confidence": "direct_return_type",
 35979|       "confidence_tier": "unverified_reference",
 35980|       "target_resolution": "exact",
 35981|       "evidence": [
 35982|         "return type 'CurtainGridSet' directly names a Revit DB object type"
 35983|       ],
 35984|       "source_url": "https://www.revitapidocs.com/2025/b19b2f13-c741-a233-34ba-26415acacce3.htm",
 35985|       "dll_signature_verified": true,
 35986|       "dll_relationship_scope": "declared",
 35987|       "dll_semantic_verified": null,
 35988|       "dll_verified_status": "signature_verified_declared",
 35989|       "revitlookup_referenced": null,
 35990|       "revitlookup_requires_document_context": null
 35991|     },
 35992|     {
 35993|       "source": "Autodesk.Revit.DB.CurtainSystem",
 35994|       "target": "Autodesk.Revit.DB.CurtainSystemType",
 35995|       "member_name": "CurtainSystemType",
 35996|       "member_kind": "property",
 35997|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35998|       "confidence": "direct_return_type",
 35999|       "confidence_tier": "unverified_reference",
 36000|       "target_resolution": "exact",
 36001|       "evidence": [
 36002|         "return type 'CurtainSystemType' directly names a Revit DB object type"
 36003|       ],
 36004|       "source_url": "https://www.revitapidocs.com/2025/128f8a8f-3c3b-3b60-9b72-3dd7ad7ebca7.htm",
 36005|       "dll_signature_verified": true,
 36006|       "dll_relationship_scope": "declared",
 36007|       "dll_semantic_verified": null,
 36008|       "dll_verified_status": "signature_verified_declared",
 36009|       "revitlookup_referenced": null,
 36010|       "revitlookup_requires_document_context": null
 36011|     },
 36012|     {
 36013|       "source": "Autodesk.Revit.DB.Curve",
 36014|       "target": "Autodesk.Revit.DB.Reference",
 36015|       "member_name": "Reference",
 36016|       "member_kind": "property",
 36017|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36018|       "confidence": "direct_return_type",
 36019|       "confidence_tier": "unverified_reference",
 36020|       "target_resolution": "exact",
 36021|       "evidence": [
 36022|         "return type 'Reference' directly names a Revit DB object type"
 36023|       ],
 36024|       "source_url": "https://www.revitapidocs.com/2025/d5e10517-24fa-4627-43be-8981746d30c8.htm",
 36025|       "dll_signature_verified": true,
 36026|       "dll_relationship_scope": "declared",
 36027|       "dll_semantic_verified": null,
 36028|       "dll_verified_status": "signature_verified_declared",
 36029|       "revitlookup_referenced": null,
 36030|       "revitlookup_requires_document_context": null
 36031|     },
 36032|     {
 36033|       "source": "Autodesk.Revit.DB.Curve",
 36034|       "target": null,
 36035|       "member_name": "ComputeNormalizedParameter",
 36036|       "member_kind": "method",
 36037|       "edge_type": "HAS_PARAMETER",
 36038|       "confidence": "name_only_candidate",
 36039|       "confidence_tier": "likely",
 36040|       "target_resolution": "none",
 36041|       "evidence": [
 36042|         "member name 'ComputeNormalizedParameter' matches keyword pattern /Parameter/ but return type 'double' gives no type-level confirmation"
 36043|       ],
 36044|       "source_url": "https://www.revitapidocs.com/2025/d42c45a0-7525-aab6-2527-16148dd6dcc1.htm",
 36045|       "dll_signature_verified": true,
 36046|       "dll_relationship_scope": "declared",
 36047|       "dll_semantic_verified": null,
 36048|       "dll_verified_status": "signature_verified_declared",
 36049|       "revitlookup_referenced": null,
 36050|       "revitlookup_requires_document_context": null
 36051|     },
 36052|     {
 36053|       "source": "Autodesk.Revit.DB.Curve",
 36054|       "target": null,
 36055|       "member_name": "ComputeRawParameter",
 36056|       "member_kind": "method",
 36057|       "edge_type": "HAS_PARAMETER",
 36058|       "confidence": "name_only_candidate",
 36059|       "confidence_tier": "likely",
 36060|       "target_resolution": "none",
 36061|       "evidence": [
 36062|         "member name 'ComputeRawParameter' matches keyword pattern /Parameter/ but return type 'double' gives no type-level confirmation"
 36063|       ],
 36064|       "source_url": "https://www.revitapidocs.com/2025/ac00deb9-9e8d-6bcb-60ac-b6f6a7520ea2.htm",
 36065|       "dll_signature_verified": true,
 36066|       "dll_relationship_scope": "declared",
 36067|       "dll_semantic_verified": null,
 36068|       "dll_verified_status": "signature_verified_declared",
 36069|       "revitlookup_referenced": null,
 36070|       "revitlookup_requires_document_context": null
 36071|     },
 36072|     {
 36073|       "source": "Autodesk.Revit.DB.Curve",
 36074|       "target": null,
 36075|       "member_name": "GetEndParameter",
 36076|       "member_kind": "method",
 36077|       "edge_type": "HAS_PARAMETER",
 36078|       "confidence": "name_only_candidate",
 36079|       "confidence_tier": "likely",
 36080|       "target_resolution": "none",
 36081|       "evidence": [
 36082|         "member name 'GetEndParameter' matches keyword pattern /Parameter/ but return type 'double' gives no type-level confirmation"
 36083|       ],
 36084|       "source_url": "https://www.revitapidocs.com/2025/0f4b2c25-35f8-4e3c-c71a-0d41fb6935ce.htm",
 36085|       "dll_signature_verified": true,
 36086|       "dll_relationship_scope": "declared",
 36087|       "dll_semantic_verified": null,
 36088|       "dll_verified_status": "signature_verified_declared",
 36089|       "revitlookup_referenced": true,
 36090|       "revitlookup_requires_document_context": false
 36091|     },
 36092|     {
 36093|       "source": "Autodesk.Revit.DB.Curve",
 36094|       "target": "Autodesk.Revit.DB.Reference",
 36095|       "member_name": "GetEndPointReference",
 36096|       "member_kind": "method",
 36097|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36098|       "confidence": "direct_return_type",
 36099|       "confidence_tier": "unverified_reference",
 36100|       "target_resolution": "exact",
 36101|       "evidence": [
 36102|         "return type 'Reference' directly names a Revit DB object type"
 36103|       ],
 36104|       "source_url": "https://www.revitapidocs.com/2025/5619a3fd-38e1-fb56-7286-2e5f33a3b2b8.htm",
 36105|       "dll_signature_verified": true,
 36106|       "dll_relationship_scope": "declared",
 36107|       "dll_semantic_verified": null,
 36108|       "dll_verified_status": "signature_verified_declared",
 36109|       "revitlookup_referenced": true,
 36110|       "revitlookup_requires_document_context": false
 36111|     },
 36112|     {
 36113|       "source": "Autodesk.Revit.DB.Curve",
 36114|       "target": "Autodesk.Revit.DB.IntersectionResult",
 36115|       "member_name": "Project",
 36116|       "member_kind": "method",
 36117|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36118|       "confidence": "direct_return_type",
 36119|       "confidence_tier": "unverified_reference",
 36120|       "target_resolution": "exact",
 36121|       "evidence": [
 36122|         "return type 'IntersectionResult' directly names a Revit DB object type"
 36123|       ],
 36124|       "source_url": "https://www.revitapidocs.com/2025/b87fc3e4-ea25-2a75-5b5a-53065b099d2a.htm",
 36125|       "dll_signature_verified": true,
 36126|       "dll_relationship_scope": "declared",
 36127|       "dll_semantic_verified": null,
 36128|       "dll_verified_status": "signature_verified_declared",
 36129|       "revitlookup_referenced": null,
 36130|       "revitlookup_requires_document_context": null
 36131|     },
 36132|     {
 36133|       "source": "Autodesk.Revit.DB.Curve",
 36134|       "target": "Autodesk.Revit.DB.GraphicsStyle",
 36135|       "member_name": "SetGraphicsStyleId",
 36136|       "member_kind": "method",
 36137|       "edge_type": "REFERENCES",
 36138|       "confidence": "name_only_candidate",
 36139|       "confidence_tier": "likely",
 36140|       "target_resolution": "exact",
 36141|       "evidence": [
 36142|         "member name 'SetGraphicsStyleId' matches keyword pattern /GraphicsStyle/ but return type 'void' gives no type-level confirmation"
 36143|       ],
 36144|       "source_url": "https://www.revitapidocs.com/2025/bd71365d-d2f2-2758-c220-a2a5c71cc6e4.htm",
 36145|       "dll_signature_verified": true,
 36146|       "dll_relationship_scope": "declared",
 36147|       "dll_semantic_verified": null,
 36148|       "dll_verified_status": "signature_verified_declared",
 36149|       "revitlookup_referenced": null,
 36150|       "revitlookup_requires_document_context": null
 36151|     },
 36152|     {
 36153|       "source": "Autodesk.Revit.DB.CurveArrArray",
 36154|       "target": "Autodesk.Revit.DB.CurveArrArrayIterator",
 36155|       "member_name": "ForwardIterator",
 36156|       "member_kind": "method",
 36157|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36158|       "confidence": "direct_return_type",
 36159|       "confidence_tier": "unverified_reference",
 36160|       "target_resolution": "exact",
 36161|       "evidence": [
 36162|         "return type 'CurveArrArrayIterator' directly names a Revit DB object type"
 36163|       ],
 36164|       "source_url": "https://www.revitapidocs.com/2025/e603edf6-91d6-2598-307a-2b70e6239de5.htm",
 36165|       "dll_signature_verified": true,
 36166|       "dll_relationship_scope": "declared",
 36167|       "dll_semantic_verified": null,
 36168|       "dll_verified_status": "signature_verified_declared",
 36169|       "revitlookup_referenced": null,
 36170|       "revitlookup_requires_document_context": null
 36171|     },
 36172|     {
 36173|       "source": "Autodesk.Revit.DB.CurveArrArray",
 36174|       "target": "Autodesk.Revit.DB.CurveArrArrayIterator",
 36175|       "member_name": "ReverseIterator",
 36176|       "member_kind": "method",
 36177|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36178|       "confidence": "direct_return_type",
 36179|       "confidence_tier": "unverified_reference",
 36180|       "target_resolution": "exact",
 36181|       "evidence": [
 36182|         "return type 'CurveArrArrayIterator' directly names a Revit DB object type"
 36183|       ],
 36184|       "source_url": "https://www.revitapidocs.com/2025/1dff1c5c-571a-ba8a-c0d1-41b914f2301b.htm",
 36185|       "dll_signature_verified": true,
 36186|       "dll_relationship_scope": "declared",
 36187|       "dll_semantic_verified": null,
 36188|       "dll_verified_status": "signature_verified_declared",
 36189|       "revitlookup_referenced": null,
 36190|       "revitlookup_requires_document_context": null
 36191|     },
 36192|     {
 36193|       "source": "Autodesk.Revit.DB.CurveArray",
 36194|       "target": "Autodesk.Revit.DB.CurveArrayIterator",
 36195|       "member_name": "ForwardIterator",
 36196|       "member_kind": "method",
 36197|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36198|       "confidence": "direct_return_type",
 36199|       "confidence_tier": "unverified_reference",
 36200|       "target_resolution": "exact",
 36201|       "evidence": [
 36202|         "return type 'CurveArrayIterator' directly names a Revit DB object type"
 36203|       ],
 36204|       "source_url": "https://www.revitapidocs.com/2025/4ff143f0-2f34-acbe-f938-077d4f945758.htm",
 36205|       "dll_signature_verified": true,
 36206|       "dll_relationship_scope": "declared",
 36207|       "dll_semantic_verified": null,
 36208|       "dll_verified_status": "signature_verified_declared",
 36209|       "revitlookup_referenced": null,
 36210|       "revitlookup_requires_document_context": null
 36211|     },
 36212|     {
 36213|       "source": "Autodesk.Revit.DB.CurveArray",
 36214|       "target": "Autodesk.Revit.DB.CurveArrayIterator",
 36215|       "member_name": "ReverseIterator",
 36216|       "member_kind": "method",
 36217|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36218|       "confidence": "direct_return_type",
 36219|       "confidence_tier": "unverified_reference",
 36220|       "target_resolution": "exact",
 36221|       "evidence": [
 36222|         "return type 'CurveArrayIterator' directly names a Revit DB object type"
 36223|       ],
 36224|       "source_url": "https://www.revitapidocs.com/2025/ccc74210-38ce-3cf2-2174-73389f1f3228.htm",
 36225|       "dll_signature_verified": true,
 36226|       "dll_relationship_scope": "declared",
 36227|       "dll_semantic_verified": null,
 36228|       "dll_verified_status": "signature_verified_declared",
 36229|       "revitlookup_referenced": null,
 36230|       "revitlookup_requires_document_context": null
 36231|     },
 36232|     {
 36233|       "source": "Autodesk.Revit.DB.CurveByPoints",
 36234|       "target": "Autodesk.Revit.DB.SketchPlane",
 36235|       "member_name": "SketchPlane",
 36236|       "member_kind": "property",
 36237|       "edge_type": "REFERENCES",
 36238|       "confidence": "direct_return_type",
 36239|       "confidence_tier": "core",
 36240|       "target_resolution": "exact",
 36241|       "evidence": [
 36242|         "return type 'SketchPlane' directly names a Revit DB object type"
 36243|       ],
 36244|       "source_url": "https://www.revitapidocs.com/2025/94ff6c7e-7596-0ee9-357a-fc91e1ba8547.htm",
 36245|       "dll_signature_verified": true,
 36246|       "dll_relationship_scope": "declared",
 36247|       "dll_semantic_verified": null,
 36248|       "dll_verified_status": "signature_verified_declared",
 36249|       "revitlookup_referenced": null,
 36250|       "revitlookup_requires_document_context": null
 36251|     },
 36252|     {
 36253|       "source": "Autodesk.Revit.DB.CurveByPoints",
 36254|       "target": "Autodesk.Revit.DB.GraphicsStyle",
 36255|       "member_name": "Subcategory",
 36256|       "member_kind": "property",
 36257|       "edge_type": "REFERENCES",
 36258|       "confidence": "direct_return_type",
 36259|       "confidence_tier": "core",
 36260|       "target_resolution": "exact",
 36261|       "evidence": [
 36262|         "return type 'GraphicsStyle' directly names a Revit DB object type"
 36263|       ],
 36264|       "source_url": "https://www.revitapidocs.com/2025/cbdc4354-9a30-6223-14a3-3f7ba66da2d8.htm",
 36265|       "dll_signature_verified": true,
 36266|       "dll_relationship_scope": "declared",
 36267|       "dll_semantic_verified": null,
 36268|       "dll_verified_status": "signature_verified_declared",
 36269|       "revitlookup_referenced": null,
 36270|       "revitlookup_requires_document_context": null
 36271|     },
 36272|     {
 36273|       "source": "Autodesk.Revit.DB.CurveByPoints",
 36274|       "target": "Autodesk.Revit.DB.ReferencePointArray",
 36275|       "member_name": "GetPoints",
 36276|       "member_kind": "method",
 36277|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36278|       "confidence": "direct_return_type",
 36279|       "confidence_tier": "unverified_reference",
 36280|       "target_resolution": "exact",
```
