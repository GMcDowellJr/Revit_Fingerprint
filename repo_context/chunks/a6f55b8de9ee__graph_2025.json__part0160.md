# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 160 of 216
- Original line range: 62011-62410
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 62011|       "source": "Autodesk.Revit.DB.View",
 62012|       "target": "Autodesk.Revit.DB.OverrideGraphicSettings",
 62013|       "member_name": "GetCategoryOverrides",
 62014|       "member_kind": "method",
 62015|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 62016|       "confidence": "direct_return_type",
 62017|       "confidence_tier": "unverified_reference",
 62018|       "target_resolution": "exact",
 62019|       "evidence": [
 62020|         "member name 'GetCategoryOverrides' matches keyword pattern /Category/ implying target 'Category', but the actual return type 'OverrideGraphicSettings' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 62021|         "return type 'OverrideGraphicSettings' directly names a Revit DB object type"
 62022|       ],
 62023|       "source_url": "https://www.revitapidocs.com/2025/ed267b82-56be-6e3b-0c6d-4de7df1ed312.htm",
 62024|       "dll_signature_verified": true,
 62025|       "dll_relationship_scope": "declared",
 62026|       "dll_semantic_verified": null,
 62027|       "dll_verified_status": "signature_verified_declared",
 62028|       "revitlookup_referenced": true,
 62029|       "revitlookup_requires_document_context": true
 62030|     },
 62031|     {
 62032|       "source": "Autodesk.Revit.DB.View",
 62033|       "target": null,
 62034|       "member_name": "GetColorFillSchemeId",
 62035|       "member_kind": "method",
 62036|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 62037|       "confidence": "unknown_reference",
 62038|       "confidence_tier": "unverified_reference",
 62039|       "target_resolution": "none",
 62040|       "evidence": [
 62041|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 62042|       ],
 62043|       "source_url": "https://www.revitapidocs.com/2025/c504d70c-ab68-2db1-95be-73e821ee3587.htm",
 62044|       "dll_signature_verified": true,
 62045|       "dll_relationship_scope": "declared",
 62046|       "dll_semantic_verified": null,
 62047|       "dll_verified_status": "signature_verified_declared",
 62048|       "revitlookup_referenced": true,
 62049|       "revitlookup_requires_document_context": true
 62050|     },
 62051|     {
 62052|       "source": "Autodesk.Revit.DB.View",
 62053|       "target": "Autodesk.Revit.DB.ViewCropRegionShapeManager",
 62054|       "member_name": "GetCropRegionShapeManager",
 62055|       "member_kind": "method",
 62056|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 62057|       "confidence": "direct_return_type",
 62058|       "confidence_tier": "unverified_reference",
 62059|       "target_resolution": "exact",
 62060|       "evidence": [
 62061|         "return type 'ViewCropRegionShapeManager' directly names a Revit DB object type"
 62062|       ],
 62063|       "source_url": "https://www.revitapidocs.com/2025/e2f53728-9b72-227a-f585-9dccf6d79d9f.htm",
 62064|       "dll_signature_verified": true,
 62065|       "dll_relationship_scope": "declared",
 62066|       "dll_semantic_verified": null,
 62067|       "dll_verified_status": "signature_verified_declared",
 62068|       "revitlookup_referenced": null,
 62069|       "revitlookup_requires_document_context": null
 62070|     },
 62071|     {
 62072|       "source": "Autodesk.Revit.DB.View",
 62073|       "target": "Autodesk.Revit.DB.ViewCropRegionShapeManager",
 62074|       "member_name": "GetCropRegionShapeManagerForReferenceCallout",
 62075|       "member_kind": "method",
 62076|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 62077|       "confidence": "direct_return_type",
 62078|       "confidence_tier": "unverified_reference",
 62079|       "target_resolution": "exact",
 62080|       "evidence": [
 62081|         "return type 'ViewCropRegionShapeManager' directly names a Revit DB object type"
 62082|       ],
 62083|       "source_url": "https://www.revitapidocs.com/2025/248f20e0-9735-5733-2c8a-6b871bb17d3b.htm",
 62084|       "dll_signature_verified": true,
 62085|       "dll_relationship_scope": "declared",
 62086|       "dll_semantic_verified": null,
 62087|       "dll_verified_status": "signature_verified_declared",
 62088|       "revitlookup_referenced": null,
 62089|       "revitlookup_requires_document_context": null
 62090|     },
 62091|     {
 62092|       "source": "Autodesk.Revit.DB.View",
 62093|       "target": null,
 62094|       "member_name": "GetDependentViewIds",
 62095|       "member_kind": "method",
 62096|       "edge_type": "DEPENDS_ON",
 62097|       "confidence": "elementid_collection_with_strong_name",
 62098|       "confidence_tier": "core",
 62099|       "target_resolution": "none",
 62100|       "evidence": [
 62101|         "member name 'GetDependentViewIds' matches keyword pattern /^GetDependent|Dependent/"
 62102|       ],
 62103|       "source_url": "https://www.revitapidocs.com/2025/07ba2649-c343-a59b-5bf2-00f3d8f24d10.htm",
 62104|       "dll_signature_verified": true,
 62105|       "dll_relationship_scope": "declared",
 62106|       "dll_semantic_verified": null,
 62107|       "dll_verified_status": "signature_verified_declared",
 62108|       "revitlookup_referenced": null,
 62109|       "revitlookup_requires_document_context": null
 62110|     },
 62111|     {
 62112|       "source": "Autodesk.Revit.DB.View",
 62113|       "target": "Autodesk.Revit.DB.ViewDisplayDepthCueing",
 62114|       "member_name": "GetDepthCueing",
 62115|       "member_kind": "method",
 62116|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 62117|       "confidence": "direct_return_type",
 62118|       "confidence_tier": "unverified_reference",
 62119|       "target_resolution": "exact",
 62120|       "evidence": [
 62121|         "return type 'ViewDisplayDepthCueing' directly names a Revit DB object type"
 62122|       ],
 62123|       "source_url": "https://www.revitapidocs.com/2025/e60cb801-5a19-1fd7-6a44-0751cf87f162.htm",
 62124|       "dll_signature_verified": true,
 62125|       "dll_relationship_scope": "declared",
 62126|       "dll_semantic_verified": null,
 62127|       "dll_verified_status": "signature_verified_declared",
 62128|       "revitlookup_referenced": null,
 62129|       "revitlookup_requires_document_context": null
 62130|     },
 62131|     {
 62132|       "source": "Autodesk.Revit.DB.View",
 62133|       "target": "Autodesk.Revit.DB.DirectContext3D.DirectContext3DHandleOverrides",
 62134|       "member_name": "GetDirectContext3DHandleOverrides",
 62135|       "member_kind": "method",
 62136|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 62137|       "confidence": "direct_return_type",
 62138|       "confidence_tier": "unverified_reference",
 62139|       "target_resolution": "short_name_fallback",
 62140|       "evidence": [
 62141|         "return type 'DirectContext3DHandleOverrides' directly names a Revit DB object type"
 62142|       ],
 62143|       "source_url": "https://www.revitapidocs.com/2025/c3671906-2049-5bb7-c9b9-b22e1c813509.htm",
 62144|       "dll_signature_verified": true,
 62145|       "dll_relationship_scope": "declared",
 62146|       "dll_semantic_verified": null,
 62147|       "dll_verified_status": "signature_verified_declared",
 62148|       "revitlookup_referenced": null,
 62149|       "revitlookup_requires_document_context": null
 62150|     },
 62151|     {
 62152|       "source": "Autodesk.Revit.DB.View",
 62153|       "target": "Autodesk.Revit.DB.OverrideGraphicSettings",
 62154|       "member_name": "GetElementOverrides",
 62155|       "member_kind": "method",
 62156|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 62157|       "confidence": "direct_return_type",
 62158|       "confidence_tier": "unverified_reference",
 62159|       "target_resolution": "exact",
 62160|       "evidence": [
 62161|         "return type 'OverrideGraphicSettings' directly names a Revit DB object type"
 62162|       ],
 62163|       "source_url": "https://www.revitapidocs.com/2025/af0a58fc-7bdf-02fb-4a20-944ffbe9057b.htm",
 62164|       "dll_signature_verified": true,
 62165|       "dll_relationship_scope": "declared",
 62166|       "dll_semantic_verified": null,
 62167|       "dll_verified_status": "signature_verified_declared",
 62168|       "revitlookup_referenced": null,
 62169|       "revitlookup_requires_document_context": null
 62170|     },
 62171|     {
 62172|       "source": "Autodesk.Revit.DB.View",
 62173|       "target": "Autodesk.Revit.DB.OverrideGraphicSettings",
 62174|       "member_name": "GetFilterOverrides",
 62175|       "member_kind": "method",
 62176|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 62177|       "confidence": "direct_return_type",
 62178|       "confidence_tier": "unverified_reference",
 62179|       "target_resolution": "exact",
 62180|       "evidence": [
 62181|         "return type 'OverrideGraphicSettings' directly names a Revit DB object type"
 62182|       ],
 62183|       "source_url": "https://www.revitapidocs.com/2025/764e7bf7-a852-95a3-4183-dc52a1caccaf.htm",
 62184|       "dll_signature_verified": true,
 62185|       "dll_relationship_scope": "declared",
 62186|       "dll_semantic_verified": null,
 62187|       "dll_verified_status": "signature_verified_declared",
 62188|       "revitlookup_referenced": true,
 62189|       "revitlookup_requires_document_context": false
 62190|     },
 62191|     {
 62192|       "source": "Autodesk.Revit.DB.View",
 62193|       "target": null,
 62194|       "member_name": "GetFilters",
 62195|       "member_kind": "method",
 62196|       "edge_type": "RETURNS_ELEMENT_IDS",
 62197|       "confidence": "unknown_reference",
 62198|       "confidence_tier": "unverified_reference",
 62199|       "target_resolution": "none",
 62200|       "evidence": [
 62201|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 62202|       ],
 62203|       "source_url": "https://www.revitapidocs.com/2025/a5b3aee3-222c-7e8d-fb63-2034ed8871a0.htm",
 62204|       "dll_signature_verified": true,
 62205|       "dll_relationship_scope": "declared",
 62206|       "dll_semantic_verified": null,
 62207|       "dll_verified_status": "signature_verified_declared",
 62208|       "revitlookup_referenced": null,
 62209|       "revitlookup_requires_document_context": null
 62210|     },
 62211|     {
 62212|       "source": "Autodesk.Revit.DB.View",
 62213|       "target": "Autodesk.Revit.DB.RevitLinkGraphicsSettings",
 62214|       "member_name": "GetLinkOverrides",
 62215|       "member_kind": "method",
 62216|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 62217|       "confidence": "direct_return_type",
 62218|       "confidence_tier": "unverified_reference",
 62219|       "target_resolution": "exact",
 62220|       "evidence": [
 62221|         "return type 'RevitLinkGraphicsSettings' directly names a Revit DB object type"
 62222|       ],
 62223|       "source_url": "https://www.revitapidocs.com/2025/f31cba64-9778-a432-4856-fe597f3daf84.htm",
 62224|       "dll_signature_verified": true,
 62225|       "dll_relationship_scope": "declared",
 62226|       "dll_semantic_verified": null,
 62227|       "dll_verified_status": "signature_verified_declared",
 62228|       "revitlookup_referenced": null,
 62229|       "revitlookup_requires_document_context": null
 62230|     },
 62231|     {
 62232|       "source": "Autodesk.Revit.DB.View",
 62233|       "target": "Autodesk.Revit.DB.TransformWithBoundary",
 62234|       "member_name": "GetModelToProjectionTransforms",
 62235|       "member_kind": "method",
 62236|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 62237|       "confidence": "needs_runtime_validation",
 62238|       "confidence_tier": "needs_validation",
 62239|       "target_resolution": "exact",
 62240|       "evidence": [
 62241|         "return type 'IList < TransformWithBoundary >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 62242|       ],
 62243|       "source_url": "https://www.revitapidocs.com/2025/593acdf3-9c82-d12a-4fc3-f15a636fd3d9.htm",
 62244|       "dll_signature_verified": true,
 62245|       "dll_relationship_scope": "declared",
 62246|       "dll_semantic_verified": null,
 62247|       "dll_verified_status": "signature_verified_declared",
 62248|       "revitlookup_referenced": null,
 62249|       "revitlookup_requires_document_context": null
 62250|     },
 62251|     {
 62252|       "source": "Autodesk.Revit.DB.View",
 62253|       "target": "Autodesk.Revit.DB.View",
 62254|       "member_name": "GetNonControlledTemplateParameterIds",
 62255|       "member_kind": "method",
 62256|       "edge_type": "CONTROLLED_BY_TEMPLATE",
 62257|       "confidence": "elementid_collection_with_strong_name",
 62258|       "confidence_tier": "core",
 62259|       "target_resolution": "exact",
 62260|       "evidence": [
 62261|         "member name 'GetNonControlledTemplateParameterIds' matches keyword pattern /Template/"
 62262|       ],
 62263|       "source_url": "https://www.revitapidocs.com/2025/a34bb9cf-9a1d-a3e7-b04e-78bca30ecf4e.htm",
 62264|       "dll_signature_verified": true,
 62265|       "dll_relationship_scope": "declared",
 62266|       "dll_semantic_verified": null,
 62267|       "dll_verified_status": "signature_verified_declared",
 62268|       "revitlookup_referenced": null,
 62269|       "revitlookup_requires_document_context": null
 62270|     },
 62271|     {
 62272|       "source": "Autodesk.Revit.DB.View",
 62273|       "target": null,
 62274|       "member_name": "GetOrderedFilters",
 62275|       "member_kind": "method",
 62276|       "edge_type": "RETURNS_ELEMENT_IDS",
 62277|       "confidence": "unknown_reference",
 62278|       "confidence_tier": "unverified_reference",
 62279|       "target_resolution": "none",
 62280|       "evidence": [
 62281|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 62282|       ],
 62283|       "source_url": "https://www.revitapidocs.com/2025/0de769ec-9c15-435e-95dc-6dcf439bbf15.htm",
 62284|       "dll_signature_verified": true,
 62285|       "dll_relationship_scope": "declared",
 62286|       "dll_semantic_verified": null,
 62287|       "dll_verified_status": "signature_verified_declared",
 62288|       "revitlookup_referenced": null,
 62289|       "revitlookup_requires_document_context": null
 62290|     },
 62291|     {
 62292|       "source": "Autodesk.Revit.DB.View",
 62293|       "target": "Autodesk.Revit.DB.ViewSheet",
 62294|       "member_name": "GetPlacementOnSheetStatus",
 62295|       "member_kind": "method",
 62296|       "edge_type": "PLACED_ON_SHEET",
 62297|       "confidence": "name_only_candidate",
 62298|       "confidence_tier": "likely",
 62299|       "target_resolution": "exact",
 62300|       "evidence": [
 62301|         "member name 'GetPlacementOnSheetStatus' matches keyword pattern /Sheet/ but return type 'ViewPlacementOnSheetStatus' gives no type-level confirmation"
 62302|       ],
 62303|       "source_url": "https://www.revitapidocs.com/2025/bc769050-18b4-e147-b1ac-753c11b62c70.htm",
 62304|       "dll_signature_verified": true,
 62305|       "dll_relationship_scope": "declared",
 62306|       "dll_semantic_verified": null,
 62307|       "dll_verified_status": "signature_verified_declared",
 62308|       "revitlookup_referenced": null,
 62309|       "revitlookup_requires_document_context": null
 62310|     },
 62311|     {
 62312|       "source": "Autodesk.Revit.DB.View",
 62313|       "target": "Autodesk.Revit.DB.PointClouds.PointCloudOverrides",
 62314|       "member_name": "GetPointCloudOverrides",
 62315|       "member_kind": "method",
 62316|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 62317|       "confidence": "direct_return_type",
 62318|       "confidence_tier": "unverified_reference",
 62319|       "target_resolution": "short_name_fallback",
 62320|       "evidence": [
 62321|         "return type 'PointCloudOverrides' directly names a Revit DB object type"
 62322|       ],
 62323|       "source_url": "https://www.revitapidocs.com/2025/08c94e04-8c83-4d28-dccb-ddccec1487d9.htm",
 62324|       "dll_signature_verified": true,
 62325|       "dll_relationship_scope": "declared",
 62326|       "dll_semantic_verified": null,
 62327|       "dll_verified_status": "signature_verified_declared",
 62328|       "revitlookup_referenced": null,
 62329|       "revitlookup_requires_document_context": null
 62330|     },
 62331|     {
 62332|       "source": "Autodesk.Revit.DB.View",
 62333|       "target": null,
 62334|       "member_name": "GetPrimaryViewId",
 62335|       "member_kind": "method",
 62336|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 62337|       "confidence": "unknown_reference",
 62338|       "confidence_tier": "unverified_reference",
 62339|       "target_resolution": "none",
 62340|       "evidence": [
 62341|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 62342|       ],
 62343|       "source_url": "https://www.revitapidocs.com/2025/3ef30e6a-73e8-2d80-7f76-00fe1f42fed7.htm",
 62344|       "dll_signature_verified": true,
 62345|       "dll_relationship_scope": "declared",
 62346|       "dll_semantic_verified": null,
 62347|       "dll_verified_status": "signature_verified_declared",
 62348|       "revitlookup_referenced": null,
 62349|       "revitlookup_requires_document_context": null
 62350|     },
 62351|     {
 62352|       "source": "Autodesk.Revit.DB.View",
 62353|       "target": null,
 62354|       "member_name": "GetReferenceCallouts",
 62355|       "member_kind": "method",
 62356|       "edge_type": "RETURNS_ELEMENT_IDS",
 62357|       "confidence": "unknown_reference",
 62358|       "confidence_tier": "unverified_reference",
 62359|       "target_resolution": "none",
 62360|       "evidence": [
 62361|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 62362|       ],
 62363|       "source_url": "https://www.revitapidocs.com/2025/00cf51d8-dfcd-3bb3-91e8-91b49d0c4f0d.htm",
 62364|       "dll_signature_verified": true,
 62365|       "dll_relationship_scope": "declared",
 62366|       "dll_semantic_verified": null,
 62367|       "dll_verified_status": "signature_verified_declared",
 62368|       "revitlookup_referenced": null,
 62369|       "revitlookup_requires_document_context": null
 62370|     },
 62371|     {
 62372|       "source": "Autodesk.Revit.DB.View",
 62373|       "target": null,
 62374|       "member_name": "GetReferenceElevations",
 62375|       "member_kind": "method",
 62376|       "edge_type": "RETURNS_ELEMENT_IDS",
 62377|       "confidence": "unknown_reference",
 62378|       "confidence_tier": "unverified_reference",
 62379|       "target_resolution": "none",
 62380|       "evidence": [
 62381|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 62382|       ],
 62383|       "source_url": "https://www.revitapidocs.com/2025/a029f626-b771-176c-09e6-8a58437c6efe.htm",
 62384|       "dll_signature_verified": true,
 62385|       "dll_relationship_scope": "declared",
 62386|       "dll_semantic_verified": null,
 62387|       "dll_verified_status": "signature_verified_declared",
 62388|       "revitlookup_referenced": null,
 62389|       "revitlookup_requires_document_context": null
 62390|     },
 62391|     {
 62392|       "source": "Autodesk.Revit.DB.View",
 62393|       "target": null,
 62394|       "member_name": "GetReferenceSections",
 62395|       "member_kind": "method",
 62396|       "edge_type": "RETURNS_ELEMENT_IDS",
 62397|       "confidence": "unknown_reference",
 62398|       "confidence_tier": "unverified_reference",
 62399|       "target_resolution": "none",
 62400|       "evidence": [
 62401|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 62402|       ],
 62403|       "source_url": "https://www.revitapidocs.com/2025/8ea511ab-7bca-bb8b-4eb0-1c1bacfc03f7.htm",
 62404|       "dll_signature_verified": true,
 62405|       "dll_relationship_scope": "declared",
 62406|       "dll_semantic_verified": null,
 62407|       "dll_verified_status": "signature_verified_declared",
 62408|       "revitlookup_referenced": null,
 62409|       "revitlookup_requires_document_context": null
 62410|     },
```
