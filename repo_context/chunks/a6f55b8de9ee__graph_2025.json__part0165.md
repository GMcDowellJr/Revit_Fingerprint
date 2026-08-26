# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 165 of 216
- Original line range: 63961-64360
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 63961|       "confidence_tier": "core",
 63962|       "target_resolution": "none",
 63963|       "evidence": [
 63964|         "member name 'GetAllRevisionIds' matches keyword pattern /^GetAll/"
 63965|       ],
 63966|       "source_url": "https://www.revitapidocs.com/2025/e6f4e79f-c076-8085-5288-6e0b5a431177.htm",
 63967|       "dll_signature_verified": true,
 63968|       "dll_relationship_scope": "declared",
 63969|       "dll_semantic_verified": null,
 63970|       "dll_verified_status": "signature_verified_declared",
 63971|       "revitlookup_referenced": null,
 63972|       "revitlookup_requires_document_context": null
 63973|     },
 63974|     {
 63975|       "source": "Autodesk.Revit.DB.ViewSheet",
 63976|       "target": null,
 63977|       "member_name": "GetAllViewports",
 63978|       "member_kind": "method",
 63979|       "edge_type": "RETURNS_ELEMENT_IDS",
 63980|       "confidence": "elementid_collection_with_strong_name",
 63981|       "confidence_tier": "core",
 63982|       "target_resolution": "none",
 63983|       "evidence": [
 63984|         "member name 'GetAllViewports' matches keyword pattern /^GetAll/"
 63985|       ],
 63986|       "source_url": "https://www.revitapidocs.com/2025/8bec6dbb-2487-d3b3-e664-62ec31a2185c.htm",
 63987|       "dll_signature_verified": true,
 63988|       "dll_relationship_scope": "declared",
 63989|       "dll_semantic_verified": null,
 63990|       "dll_verified_status": "signature_verified_declared",
 63991|       "revitlookup_referenced": null,
 63992|       "revitlookup_requires_document_context": null
 63993|     },
 63994|     {
 63995|       "source": "Autodesk.Revit.DB.ViewSheet",
 63996|       "target": null,
 63997|       "member_name": "GetCurrentRevision",
 63998|       "member_kind": "method",
 63999|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 64000|       "confidence": "unknown_reference",
 64001|       "confidence_tier": "unverified_reference",
 64002|       "target_resolution": "none",
 64003|       "evidence": [
 64004|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 64005|       ],
 64006|       "source_url": "https://www.revitapidocs.com/2025/9184aa1a-1f29-2f1f-9557-bafb176a4aa0.htm",
 64007|       "dll_signature_verified": true,
 64008|       "dll_relationship_scope": "declared",
 64009|       "dll_semantic_verified": null,
 64010|       "dll_verified_status": "signature_verified_declared",
 64011|       "revitlookup_referenced": null,
 64012|       "revitlookup_requires_document_context": null
 64013|     },
 64014|     {
 64015|       "source": "Autodesk.Revit.DB.ViewSheet",
 64016|       "target": "Autodesk.Revit.DB.ViewSheet",
 64017|       "member_name": "GetRevisionCloudNumberOnSheet",
 64018|       "member_kind": "method",
 64019|       "edge_type": "PLACED_ON_SHEET",
 64020|       "confidence": "name_only_candidate",
 64021|       "confidence_tier": "likely",
 64022|       "target_resolution": "exact",
 64023|       "evidence": [
 64024|         "member name 'GetRevisionCloudNumberOnSheet' matches keyword pattern /Sheet/ but return type 'string' gives no type-level confirmation"
 64025|       ],
 64026|       "source_url": "https://www.revitapidocs.com/2025/677a2864-349f-42e6-481a-2c3cd55f8081.htm",
 64027|       "dll_signature_verified": true,
 64028|       "dll_relationship_scope": "declared",
 64029|       "dll_semantic_verified": null,
 64030|       "dll_verified_status": "signature_verified_declared",
 64031|       "revitlookup_referenced": null,
 64032|       "revitlookup_requires_document_context": null
 64033|     },
 64034|     {
 64035|       "source": "Autodesk.Revit.DB.ViewSheet",
 64036|       "target": "Autodesk.Revit.DB.ViewSheet",
 64037|       "member_name": "GetRevisionNumberOnSheet",
 64038|       "member_kind": "method",
 64039|       "edge_type": "PLACED_ON_SHEET",
 64040|       "confidence": "name_only_candidate",
 64041|       "confidence_tier": "likely",
 64042|       "target_resolution": "exact",
 64043|       "evidence": [
 64044|         "member name 'GetRevisionNumberOnSheet' matches keyword pattern /Sheet/ but return type 'string' gives no type-level confirmation"
 64045|       ],
 64046|       "source_url": "https://www.revitapidocs.com/2025/cf2ea568-81ba-40e6-c29c-e0c0138ffbe7.htm",
 64047|       "dll_signature_verified": true,
 64048|       "dll_relationship_scope": "declared",
 64049|       "dll_semantic_verified": null,
 64050|       "dll_verified_status": "signature_verified_declared",
 64051|       "revitlookup_referenced": null,
 64052|       "revitlookup_requires_document_context": null
 64053|     },
 64054|     {
 64055|       "source": "Autodesk.Revit.DB.ViewSheetSet",
 64056|       "target": "Autodesk.Revit.DB.ViewSheet",
 64057|       "member_name": "SheetOrganizationId",
 64058|       "member_kind": "property",
 64059|       "edge_type": "PLACED_ON_SHEET",
 64060|       "confidence": "elementid_with_strong_name",
 64061|       "confidence_tier": "core",
 64062|       "target_resolution": "exact",
 64063|       "evidence": [
 64064|         "member name 'SheetOrganizationId' matches keyword pattern /Sheet/"
 64065|       ],
 64066|       "source_url": "https://www.revitapidocs.com/2025/a28d4e62-17bc-96c2-4ebb-d6a9b96afb1d.htm",
 64067|       "dll_signature_verified": true,
 64068|       "dll_relationship_scope": "declared",
 64069|       "dll_semantic_verified": null,
 64070|       "dll_verified_status": "signature_verified_declared",
 64071|       "revitlookup_referenced": null,
 64072|       "revitlookup_requires_document_context": null
 64073|     },
 64074|     {
 64075|       "source": "Autodesk.Revit.DB.ViewSheetSet",
 64076|       "target": null,
 64077|       "member_name": "ViewOrganizationId",
 64078|       "member_kind": "property",
 64079|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 64080|       "confidence": "unknown_reference",
 64081|       "confidence_tier": "unverified_reference",
 64082|       "target_resolution": "none",
 64083|       "evidence": [
 64084|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 64085|       ],
 64086|       "source_url": "https://www.revitapidocs.com/2025/f8d31451-ddb8-06e3-6c5f-87dff0132645.htm",
 64087|       "dll_signature_verified": true,
 64088|       "dll_relationship_scope": "declared",
 64089|       "dll_semantic_verified": null,
 64090|       "dll_verified_status": "signature_verified_declared",
 64091|       "revitlookup_referenced": null,
 64092|       "revitlookup_requires_document_context": null
 64093|     },
 64094|     {
 64095|       "source": "Autodesk.Revit.DB.ViewSheetSet",
 64096|       "target": "Autodesk.Revit.DB.ViewSet",
 64097|       "member_name": "Views",
 64098|       "member_kind": "property",
 64099|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 64100|       "confidence": "direct_return_type",
 64101|       "confidence_tier": "unverified_reference",
 64102|       "target_resolution": "exact",
 64103|       "evidence": [
 64104|         "return type 'ViewSet' directly names a Revit DB object type"
 64105|       ],
 64106|       "source_url": "https://www.revitapidocs.com/2025/b6c69e58-9579-9df2-6adc-8f8ce44a633c.htm",
 64107|       "dll_signature_verified": true,
 64108|       "dll_relationship_scope": "declared",
 64109|       "dll_semantic_verified": null,
 64110|       "dll_verified_status": "signature_verified_declared",
 64111|       "revitlookup_referenced": null,
 64112|       "revitlookup_requires_document_context": null
 64113|     },
 64114|     {
 64115|       "source": "Autodesk.Revit.DB.ViewSheetSetting",
 64116|       "target": "Autodesk.Revit.DB.ViewSet",
 64117|       "member_name": "AvailableViews",
 64118|       "member_kind": "property",
 64119|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 64120|       "confidence": "direct_return_type",
 64121|       "confidence_tier": "unverified_reference",
 64122|       "target_resolution": "exact",
 64123|       "evidence": [
 64124|         "return type 'ViewSet' directly names a Revit DB object type"
 64125|       ],
 64126|       "source_url": "https://www.revitapidocs.com/2025/7dfeb893-68af-9d54-b92a-789733c01104.htm",
 64127|       "dll_signature_verified": true,
 64128|       "dll_relationship_scope": "declared",
 64129|       "dll_semantic_verified": null,
 64130|       "dll_verified_status": "signature_verified_declared",
 64131|       "revitlookup_referenced": null,
 64132|       "revitlookup_requires_document_context": null
 64133|     },
 64134|     {
 64135|       "source": "Autodesk.Revit.DB.ViewSheetSetting",
 64136|       "target": "Autodesk.Revit.DB.IViewSheetSet",
 64137|       "member_name": "CurrentViewSheetSet",
 64138|       "member_kind": "property",
 64139|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 64140|       "confidence": "direct_return_type",
 64141|       "confidence_tier": "unverified_reference",
 64142|       "target_resolution": "exact",
 64143|       "evidence": [
 64144|         "member name 'CurrentViewSheetSet' matches keyword pattern /Sheet/ implying target 'ViewSheet', but the actual return type 'IViewSheetSet' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 64145|         "return type 'IViewSheetSet' directly names a Revit DB object type"
 64146|       ],
 64147|       "source_url": "https://www.revitapidocs.com/2025/e0e891ee-2ff1-9097-9faa-6e3c7db51e81.htm",
 64148|       "dll_signature_verified": true,
 64149|       "dll_relationship_scope": "declared",
 64150|       "dll_semantic_verified": null,
 64151|       "dll_verified_status": "signature_verified_declared",
 64152|       "revitlookup_referenced": null,
 64153|       "revitlookup_requires_document_context": null
 64154|     },
 64155|     {
 64156|       "source": "Autodesk.Revit.DB.ViewSheetSetting",
 64157|       "target": "Autodesk.Revit.DB.InSessionViewSheetSet",
 64158|       "member_name": "InSession",
 64159|       "member_kind": "property",
 64160|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 64161|       "confidence": "direct_return_type",
 64162|       "confidence_tier": "unverified_reference",
 64163|       "target_resolution": "exact",
 64164|       "evidence": [
 64165|         "return type 'InSessionViewSheetSet' directly names a Revit DB object type"
 64166|       ],
 64167|       "source_url": "https://www.revitapidocs.com/2025/499cf452-867e-e131-b39d-493875215872.htm",
 64168|       "dll_signature_verified": true,
 64169|       "dll_relationship_scope": "declared",
 64170|       "dll_semantic_verified": null,
 64171|       "dll_verified_status": "signature_verified_declared",
 64172|       "revitlookup_referenced": null,
 64173|       "revitlookup_requires_document_context": null
 64174|     },
 64175|     {
 64176|       "source": "Autodesk.Revit.DB.Wall",
 64177|       "target": "Autodesk.Revit.DB.CurtainGrid",
 64178|       "member_name": "CurtainGrid",
 64179|       "member_kind": "property",
 64180|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 64181|       "confidence": "direct_return_type",
 64182|       "confidence_tier": "unverified_reference",
 64183|       "target_resolution": "exact",
 64184|       "evidence": [
 64185|         "return type 'CurtainGrid' directly names a Revit DB object type"
 64186|       ],
 64187|       "source_url": "https://www.revitapidocs.com/2025/7d46f445-4739-1877-c9de-e04787d7257b.htm",
 64188|       "dll_signature_verified": true,
 64189|       "dll_relationship_scope": "declared",
 64190|       "dll_semantic_verified": null,
 64191|       "dll_verified_status": "signature_verified_declared",
 64192|       "revitlookup_referenced": null,
 64193|       "revitlookup_requires_document_context": null
 64194|     },
 64195|     {
 64196|       "source": "Autodesk.Revit.DB.Wall",
 64197|       "target": "Autodesk.Revit.DB.Sketch",
 64198|       "member_name": "SketchId",
 64199|       "member_kind": "property",
 64200|       "edge_type": "DEPENDS_ON",
 64201|       "confidence": "elementid_with_strong_name",
 64202|       "confidence_tier": "core",
 64203|       "target_resolution": "exact",
 64204|       "evidence": [
 64205|         "member name 'SketchId' matches keyword pattern /Sketch(Id)?$/"
 64206|       ],
 64207|       "source_url": "https://www.revitapidocs.com/2025/cbe78025-7b87-340c-3fe9-d0019a6e7382.htm",
 64208|       "dll_signature_verified": true,
 64209|       "dll_relationship_scope": "declared",
 64210|       "dll_semantic_verified": null,
 64211|       "dll_verified_status": "signature_verified_declared",
 64212|       "revitlookup_referenced": null,
 64213|       "revitlookup_requires_document_context": null
 64214|     },
 64215|     {
 64216|       "source": "Autodesk.Revit.DB.Wall",
 64217|       "target": null,
 64218|       "member_name": "StackedWallOwnerId",
 64219|       "member_kind": "property",
 64220|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 64221|       "confidence": "unknown_reference",
 64222|       "confidence_tier": "unverified_reference",
 64223|       "target_resolution": "none",
 64224|       "evidence": [
 64225|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 64226|       ],
 64227|       "source_url": "https://www.revitapidocs.com/2025/b140a527-a790-9397-51c8-1dae171fda6b.htm",
 64228|       "dll_signature_verified": true,
 64229|       "dll_relationship_scope": "declared",
 64230|       "dll_semantic_verified": null,
 64231|       "dll_verified_status": "signature_verified_declared",
 64232|       "revitlookup_referenced": null,
 64233|       "revitlookup_requires_document_context": null
 64234|     },
 64235|     {
 64236|       "source": "Autodesk.Revit.DB.Wall",
 64237|       "target": "Autodesk.Revit.DB.WallType",
 64238|       "member_name": "WallType",
 64239|       "member_kind": "property",
 64240|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 64241|       "confidence": "direct_return_type",
 64242|       "confidence_tier": "unverified_reference",
 64243|       "target_resolution": "exact",
 64244|       "evidence": [
 64245|         "return type 'WallType' directly names a Revit DB object type"
 64246|       ],
 64247|       "source_url": "https://www.revitapidocs.com/2025/0c0d155f-5b2c-c09f-d7d4-41a8600560eb.htm",
 64248|       "dll_signature_verified": true,
 64249|       "dll_relationship_scope": "declared",
 64250|       "dll_semantic_verified": null,
 64251|       "dll_verified_status": "signature_verified_declared",
 64252|       "revitlookup_referenced": null,
 64253|       "revitlookup_requires_document_context": null
 64254|     },
 64255|     {
 64256|       "source": "Autodesk.Revit.DB.Wall",
 64257|       "target": "Autodesk.Revit.DB.Sketch",
 64258|       "member_name": "CanHaveProfileSketch",
 64259|       "member_kind": "method",
 64260|       "edge_type": "DEPENDS_ON",
 64261|       "confidence": "name_only_candidate",
 64262|       "confidence_tier": "likely",
 64263|       "target_resolution": "exact",
 64264|       "evidence": [
 64265|         "member name 'CanHaveProfileSketch' matches keyword pattern /Sketch(Id)?$/ but return type 'bool' gives no type-level confirmation"
 64266|       ],
 64267|       "source_url": "https://www.revitapidocs.com/2025/63ddfb69-5168-af0a-224c-4608ddd2352c.htm",
 64268|       "dll_signature_verified": true,
 64269|       "dll_relationship_scope": "declared",
 64270|       "dll_semantic_verified": null,
 64271|       "dll_verified_status": "signature_verified_declared",
 64272|       "revitlookup_referenced": null,
 64273|       "revitlookup_requires_document_context": null
 64274|     },
 64275|     {
 64276|       "source": "Autodesk.Revit.DB.Wall",
 64277|       "target": null,
 64278|       "member_name": "GetStackedWallMemberIds",
 64279|       "member_kind": "method",
 64280|       "edge_type": "RETURNS_ELEMENT_IDS",
 64281|       "confidence": "unknown_reference",
 64282|       "confidence_tier": "unverified_reference",
 64283|       "target_resolution": "none",
 64284|       "evidence": [
 64285|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 64286|       ],
 64287|       "source_url": "https://www.revitapidocs.com/2025/66ec8d4e-25cd-1e14-3b25-d9d3a0f5cce9.htm",
 64288|       "dll_signature_verified": true,
 64289|       "dll_relationship_scope": "declared",
 64290|       "dll_semantic_verified": null,
 64291|       "dll_verified_status": "signature_verified_declared",
 64292|       "revitlookup_referenced": null,
 64293|       "revitlookup_requires_document_context": null
 64294|     },
 64295|     {
 64296|       "source": "Autodesk.Revit.DB.Wall",
 64297|       "target": null,
 64298|       "member_name": "GetWrappingLocationAsCurveParameter",
 64299|       "member_kind": "method",
 64300|       "edge_type": "HAS_PARAMETER",
 64301|       "confidence": "name_only_candidate",
 64302|       "confidence_tier": "likely",
 64303|       "target_resolution": "none",
 64304|       "evidence": [
 64305|         "member name 'GetWrappingLocationAsCurveParameter' matches keyword pattern /Parameter/ but return type 'double' gives no type-level confirmation"
 64306|       ],
 64307|       "source_url": "https://www.revitapidocs.com/2025/4755c9ba-cbb5-e2f8-703d-eeb0f1cd463d.htm",
 64308|       "dll_signature_verified": true,
 64309|       "dll_relationship_scope": "declared",
 64310|       "dll_semantic_verified": null,
 64311|       "dll_verified_status": "signature_verified_declared",
 64312|       "revitlookup_referenced": null,
 64313|       "revitlookup_requires_document_context": null
 64314|     },
 64315|     {
 64316|       "source": "Autodesk.Revit.DB.Wall",
 64317|       "target": "Autodesk.Revit.DB.Reference",
 64318|       "member_name": "GetWrappingLocationAsReferences",
 64319|       "member_kind": "method",
 64320|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 64321|       "confidence": "needs_runtime_validation",
 64322|       "confidence_tier": "needs_validation",
 64323|       "target_resolution": "exact",
 64324|       "evidence": [
 64325|         "return type 'IList < Reference >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 64326|       ],
 64327|       "source_url": "https://www.revitapidocs.com/2025/f8a0e79e-7e80-2b41-2d33-86371ff85fb8.htm",
 64328|       "dll_signature_verified": true,
 64329|       "dll_relationship_scope": "declared",
 64330|       "dll_semantic_verified": null,
 64331|       "dll_verified_status": "signature_verified_declared",
 64332|       "revitlookup_referenced": null,
 64333|       "revitlookup_requires_document_context": null
 64334|     },
 64335|     {
 64336|       "source": "Autodesk.Revit.DB.Wall",
 64337|       "target": "Autodesk.Revit.DB.Sketch",
 64338|       "member_name": "RemoveProfileSketch",
 64339|       "member_kind": "method",
 64340|       "edge_type": "DEPENDS_ON",
 64341|       "confidence": "name_only_candidate",
 64342|       "confidence_tier": "likely",
 64343|       "target_resolution": "exact",
 64344|       "evidence": [
 64345|         "member name 'RemoveProfileSketch' matches keyword pattern /Sketch(Id)?$/ but return type 'void' gives no type-level confirmation"
 64346|       ],
 64347|       "source_url": "https://www.revitapidocs.com/2025/7bd6782b-b088-9a3e-c5b6-3609c83f0070.htm",
 64348|       "dll_signature_verified": true,
 64349|       "dll_relationship_scope": "declared",
 64350|       "dll_semantic_verified": null,
 64351|       "dll_verified_status": "signature_verified_declared",
 64352|       "revitlookup_referenced": null,
 64353|       "revitlookup_requires_document_context": null
 64354|     },
 64355|     {
 64356|       "source": "Autodesk.Revit.DB.WallFoundation",
 64357|       "target": null,
 64358|       "member_name": "WallId",
 64359|       "member_kind": "property",
 64360|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
```
