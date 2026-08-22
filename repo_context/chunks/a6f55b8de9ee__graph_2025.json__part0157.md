# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 157 of 216
- Original line range: 60841-61240
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 60841|       "source_url": "https://www.revitapidocs.com/2025/b52fe304-95a0-77c8-4b4c-e3c18c16677d.htm",
 60842|       "dll_signature_verified": true,
 60843|       "dll_relationship_scope": "declared",
 60844|       "dll_semantic_verified": null,
 60845|       "dll_verified_status": "signature_verified_declared",
 60846|       "revitlookup_referenced": null,
 60847|       "revitlookup_requires_document_context": null
 60848|     },
 60849|     {
 60850|       "source": "Autodesk.Revit.DB.TessellatedShapeBuilder",
 60851|       "target": "Autodesk.Revit.DB.TessellatedShapeBuilderResult",
 60852|       "member_name": "GetBuildResult",
 60853|       "member_kind": "method",
 60854|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 60855|       "confidence": "direct_return_type",
 60856|       "confidence_tier": "unverified_reference",
 60857|       "target_resolution": "exact",
 60858|       "evidence": [
 60859|         "return type 'TessellatedShapeBuilderResult' directly names a Revit DB object type"
 60860|       ],
 60861|       "source_url": "https://www.revitapidocs.com/2025/136e8763-4156-4ffe-0fcc-45af9dbb6c14.htm",
 60862|       "dll_signature_verified": true,
 60863|       "dll_relationship_scope": "declared",
 60864|       "dll_semantic_verified": null,
 60865|       "dll_verified_status": "signature_verified_declared",
 60866|       "revitlookup_referenced": null,
 60867|       "revitlookup_requires_document_context": null
 60868|     },
 60869|     {
 60870|       "source": "Autodesk.Revit.DB.TessellatedShapeBuilderResult",
 60871|       "target": "Autodesk.Revit.DB.TessellatedBuildIssue",
 60872|       "member_name": "GetIssuesForFaceSet",
 60873|       "member_kind": "method",
 60874|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 60875|       "confidence": "needs_runtime_validation",
 60876|       "confidence_tier": "needs_validation",
 60877|       "target_resolution": "exact",
 60878|       "evidence": [
 60879|         "return type 'IList < TessellatedBuildIssue >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 60880|       ],
 60881|       "source_url": "https://www.revitapidocs.com/2025/9063460c-2dd8-a00e-6519-8733117870cb.htm",
 60882|       "dll_signature_verified": true,
 60883|       "dll_relationship_scope": "declared",
 60884|       "dll_semantic_verified": null,
 60885|       "dll_verified_status": "signature_verified_declared",
 60886|       "revitlookup_referenced": null,
 60887|       "revitlookup_requires_document_context": null
 60888|     },
 60889|     {
 60890|       "source": "Autodesk.Revit.DB.TextElement",
 60891|       "target": "Autodesk.Revit.DB.TextElementType",
 60892|       "member_name": "Symbol",
 60893|       "member_kind": "property",
 60894|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 60895|       "confidence": "direct_return_type",
 60896|       "confidence_tier": "unverified_reference",
 60897|       "target_resolution": "exact",
 60898|       "evidence": [
 60899|         "member name 'Symbol' matches keyword pattern /^Symbol$/ implying target 'FamilySymbol', but the actual return type 'TextElementType' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 60900|         "return type 'TextElementType' directly names a Revit DB object type"
 60901|       ],
 60902|       "source_url": "https://www.revitapidocs.com/2025/6e9bdd6f-14ef-94c6-cd94-ad735d18885f.htm",
 60903|       "dll_signature_verified": true,
 60904|       "dll_relationship_scope": "declared",
 60905|       "dll_semantic_verified": null,
 60906|       "dll_verified_status": "signature_verified_declared",
 60907|       "revitlookup_referenced": null,
 60908|       "revitlookup_requires_document_context": null
 60909|     },
 60910|     {
 60911|       "source": "Autodesk.Revit.DB.TextNode",
 60912|       "target": "Autodesk.Revit.DB.FormattedText",
 60913|       "member_name": "GetFormattedText",
 60914|       "member_kind": "method",
 60915|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 60916|       "confidence": "direct_return_type",
 60917|       "confidence_tier": "unverified_reference",
 60918|       "target_resolution": "exact",
 60919|       "evidence": [
 60920|         "return type 'FormattedText' directly names a Revit DB object type"
 60921|       ],
 60922|       "source_url": "https://www.revitapidocs.com/2025/1786c219-a864-1444-9338-f33daddb16e3.htm",
 60923|       "dll_signature_verified": true,
 60924|       "dll_relationship_scope": "declared",
 60925|       "dll_semantic_verified": null,
 60926|       "dll_verified_status": "signature_verified_declared",
 60927|       "revitlookup_referenced": null,
 60928|       "revitlookup_requires_document_context": null
 60929|     },
 60930|     {
 60931|       "source": "Autodesk.Revit.DB.TextNote",
 60932|       "target": "Autodesk.Revit.DB.TextNoteType",
 60933|       "member_name": "TextNoteType",
 60934|       "member_kind": "property",
 60935|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 60936|       "confidence": "direct_return_type",
 60937|       "confidence_tier": "unverified_reference",
 60938|       "target_resolution": "exact",
 60939|       "evidence": [
 60940|         "return type 'TextNoteType' directly names a Revit DB object type"
 60941|       ],
 60942|       "source_url": "https://www.revitapidocs.com/2025/e0b08dfc-834c-6dbf-c66c-fc640860d36f.htm",
 60943|       "dll_signature_verified": true,
 60944|       "dll_relationship_scope": "declared",
 60945|       "dll_semantic_verified": null,
 60946|       "dll_verified_status": "signature_verified_declared",
 60947|       "revitlookup_referenced": null,
 60948|       "revitlookup_requires_document_context": null
 60949|     },
 60950|     {
 60951|       "source": "Autodesk.Revit.DB.TextNote",
 60952|       "target": "Autodesk.Revit.DB.Leader",
 60953|       "member_name": "AddLeader",
 60954|       "member_kind": "method",
 60955|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 60956|       "confidence": "direct_return_type",
 60957|       "confidence_tier": "unverified_reference",
 60958|       "target_resolution": "exact",
 60959|       "evidence": [
 60960|         "return type 'Leader' directly names a Revit DB object type"
 60961|       ],
 60962|       "source_url": "https://www.revitapidocs.com/2025/c3cc0373-2963-3512-b308-7058d803d267.htm",
 60963|       "dll_signature_verified": true,
 60964|       "dll_relationship_scope": "declared",
 60965|       "dll_semantic_verified": null,
 60966|       "dll_verified_status": "signature_verified_declared",
 60967|       "revitlookup_referenced": null,
 60968|       "revitlookup_requires_document_context": null
 60969|     },
 60970|     {
 60971|       "source": "Autodesk.Revit.DB.TextNote",
 60972|       "target": "Autodesk.Revit.DB.FormattedText",
 60973|       "member_name": "GetFormattedText",
 60974|       "member_kind": "method",
 60975|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 60976|       "confidence": "direct_return_type",
 60977|       "confidence_tier": "unverified_reference",
 60978|       "target_resolution": "exact",
 60979|       "evidence": [
 60980|         "return type 'FormattedText' directly names a Revit DB object type"
 60981|       ],
 60982|       "source_url": "https://www.revitapidocs.com/2025/a1e3b2d7-0d56-55f9-6b30-1c5269dc39e0.htm",
 60983|       "dll_signature_verified": true,
 60984|       "dll_relationship_scope": "declared",
 60985|       "dll_semantic_verified": null,
 60986|       "dll_verified_status": "signature_verified_declared",
 60987|       "revitlookup_referenced": null,
 60988|       "revitlookup_requires_document_context": null
 60989|     },
 60990|     {
 60991|       "source": "Autodesk.Revit.DB.TextNote",
 60992|       "target": "Autodesk.Revit.DB.Leader",
 60993|       "member_name": "GetLeaders",
 60994|       "member_kind": "method",
 60995|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 60996|       "confidence": "needs_runtime_validation",
 60997|       "confidence_tier": "needs_validation",
 60998|       "target_resolution": "exact",
 60999|       "evidence": [
 61000|         "return type 'IList < Leader >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 61001|       ],
 61002|       "source_url": "https://www.revitapidocs.com/2025/be3005ca-94ae-cfd6-3990-db01308dfa96.htm",
 61003|       "dll_signature_verified": true,
 61004|       "dll_relationship_scope": "declared",
 61005|       "dll_semantic_verified": null,
 61006|       "dll_verified_status": "signature_verified_declared",
 61007|       "revitlookup_referenced": null,
 61008|       "revitlookup_requires_document_context": null
 61009|     },
 61010|     {
 61011|       "source": "Autodesk.Revit.DB.TextNoteOptions",
 61012|       "target": null,
 61013|       "member_name": "TypeId",
 61014|       "member_kind": "property",
 61015|       "edge_type": "TYPE_OF",
 61016|       "confidence": "elementid_with_strong_name",
 61017|       "confidence_tier": "core",
 61018|       "target_resolution": "none",
 61019|       "evidence": [
 61020|         "member name 'TypeId' matches keyword pattern /^(Type|TypeId|GetTypeId)$/"
 61021|       ],
 61022|       "source_url": "https://www.revitapidocs.com/2025/4eef8585-be67-b6af-7d32-885fd49511da.htm",
 61023|       "dll_signature_verified": true,
 61024|       "dll_relationship_scope": "declared",
 61025|       "dll_semantic_verified": null,
 61026|       "dll_verified_status": "signature_verified_declared",
 61027|       "revitlookup_referenced": null,
 61028|       "revitlookup_requires_document_context": null
 61029|     },
 61030|     {
 61031|       "source": "Autodesk.Revit.DB.ThermalAsset",
 61032|       "target": "Autodesk.Revit.DB.Material",
 61033|       "member_name": "ThermalMaterialType",
 61034|       "member_kind": "property",
 61035|       "edge_type": "USES_MATERIAL",
 61036|       "confidence": "name_only_candidate",
 61037|       "confidence_tier": "likely",
 61038|       "target_resolution": "exact",
 61039|       "evidence": [
 61040|         "member name 'ThermalMaterialType' matches keyword pattern /Material/ but return type 'ThermalMaterialType' gives no type-level confirmation"
 61041|       ],
 61042|       "source_url": "https://www.revitapidocs.com/2025/bd8af449-c7ef-7e95-a82e-eb6c5104cd5f.htm",
 61043|       "dll_signature_verified": true,
 61044|       "dll_relationship_scope": "declared",
 61045|       "dll_semantic_verified": null,
 61046|       "dll_verified_status": "signature_verified_declared",
 61047|       "revitlookup_referenced": null,
 61048|       "revitlookup_requires_document_context": null
 61049|     },
 61050|     {
 61051|       "source": "Autodesk.Revit.DB.TilePatterns",
 61052|       "target": "Autodesk.Revit.DB.TilePattern",
 61053|       "member_name": "GetTilePattern",
 61054|       "member_kind": "method",
 61055|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61056|       "confidence": "direct_return_type",
 61057|       "confidence_tier": "unverified_reference",
 61058|       "target_resolution": "exact",
 61059|       "evidence": [
 61060|         "return type 'TilePattern' directly names a Revit DB object type"
 61061|       ],
 61062|       "source_url": "https://www.revitapidocs.com/2025/291efd44-998c-12be-8dfc-87716e67f143.htm",
 61063|       "dll_signature_verified": true,
 61064|       "dll_relationship_scope": "declared",
 61065|       "dll_semantic_verified": null,
 61066|       "dll_verified_status": "signature_verified_declared",
 61067|       "revitlookup_referenced": null,
 61068|       "revitlookup_requires_document_context": null
 61069|     },
 61070|     {
 61071|       "source": "Autodesk.Revit.DB.Toposolid",
 61072|       "target": null,
 61073|       "member_name": "HostTopoId",
 61074|       "member_kind": "property",
 61075|       "edge_type": "HOSTED_BY",
 61076|       "confidence": "elementid_with_strong_name",
 61077|       "confidence_tier": "core",
 61078|       "target_resolution": "none",
 61079|       "evidence": [
 61080|         "member name 'HostTopoId' matches keyword pattern /^GetHosted|Host/"
 61081|       ],
 61082|       "source_url": "https://www.revitapidocs.com/2025/d16ba5b3-da97-b262-3dc8-5de0a9816f74.htm",
 61083|       "dll_signature_verified": true,
 61084|       "dll_relationship_scope": "declared",
 61085|       "dll_semantic_verified": null,
 61086|       "dll_verified_status": "signature_verified_declared",
 61087|       "revitlookup_referenced": null,
 61088|       "revitlookup_requires_document_context": null
 61089|     },
 61090|     {
 61091|       "source": "Autodesk.Revit.DB.Toposolid",
 61092|       "target": "Autodesk.Revit.DB.Sketch",
 61093|       "member_name": "SketchId",
 61094|       "member_kind": "property",
 61095|       "edge_type": "DEPENDS_ON",
 61096|       "confidence": "elementid_with_strong_name",
 61097|       "confidence_tier": "core",
 61098|       "target_resolution": "exact",
 61099|       "evidence": [
 61100|         "member name 'SketchId' matches keyword pattern /Sketch(Id)?$/"
 61101|       ],
 61102|       "source_url": "https://www.revitapidocs.com/2025/e2fc2a37-4ad1-a77f-9454-a7ccd6ea6e9d.htm",
 61103|       "dll_signature_verified": true,
 61104|       "dll_relationship_scope": "declared",
 61105|       "dll_semantic_verified": null,
 61106|       "dll_verified_status": "signature_verified_declared",
 61107|       "revitlookup_referenced": null,
 61108|       "revitlookup_requires_document_context": null
 61109|     },
 61110|     {
 61111|       "source": "Autodesk.Revit.DB.Toposolid",
 61112|       "target": "Autodesk.Revit.DB.IntersectingElementData",
 61113|       "member_name": "GetIntersectingElementData",
 61114|       "member_kind": "method",
 61115|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61116|       "confidence": "needs_runtime_validation",
 61117|       "confidence_tier": "needs_validation",
 61118|       "target_resolution": "exact",
 61119|       "evidence": [
 61120|         "return type 'IList < IntersectingElementData >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 61121|       ],
 61122|       "source_url": "https://www.revitapidocs.com/2025/e9c2d73c-4672-8f85-43c5-29d780b82183.htm",
 61123|       "dll_signature_verified": true,
 61124|       "dll_relationship_scope": "declared",
 61125|       "dll_semantic_verified": null,
 61126|       "dll_verified_status": "signature_verified_declared",
 61127|       "revitlookup_referenced": null,
 61128|       "revitlookup_requires_document_context": null
 61129|     },
 61130|     {
 61131|       "source": "Autodesk.Revit.DB.Toposolid",
 61132|       "target": "Autodesk.Revit.DB.SlabShapeEditor",
 61133|       "member_name": "GetSlabShapeEditor",
 61134|       "member_kind": "method",
 61135|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61136|       "confidence": "direct_return_type",
 61137|       "confidence_tier": "unverified_reference",
 61138|       "target_resolution": "exact",
 61139|       "evidence": [
 61140|         "return type 'SlabShapeEditor' directly names a Revit DB object type"
 61141|       ],
 61142|       "source_url": "https://www.revitapidocs.com/2025/4b7c441c-9f4f-4756-14bc-5fc387043c3e.htm",
 61143|       "dll_signature_verified": true,
 61144|       "dll_relationship_scope": "declared",
 61145|       "dll_semantic_verified": null,
 61146|       "dll_verified_status": "signature_verified_declared",
 61147|       "revitlookup_referenced": null,
 61148|       "revitlookup_requires_document_context": null
 61149|     },
 61150|     {
 61151|       "source": "Autodesk.Revit.DB.Toposolid",
 61152|       "target": null,
 61153|       "member_name": "GetSubDivisionIds",
 61154|       "member_kind": "method",
 61155|       "edge_type": "RETURNS_ELEMENT_IDS",
 61156|       "confidence": "unknown_reference",
 61157|       "confidence_tier": "unverified_reference",
 61158|       "target_resolution": "none",
 61159|       "evidence": [
 61160|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 61161|       ],
 61162|       "source_url": "https://www.revitapidocs.com/2025/0a8cf5c3-61c8-b255-06e7-bb42a9097788.htm",
 61163|       "dll_signature_verified": true,
 61164|       "dll_relationship_scope": "declared",
 61165|       "dll_semantic_verified": null,
 61166|       "dll_verified_status": "signature_verified_declared",
 61167|       "revitlookup_referenced": null,
 61168|       "revitlookup_requires_document_context": null
 61169|     },
 61170|     {
 61171|       "source": "Autodesk.Revit.DB.Toposolid",
 61172|       "target": null,
 61173|       "member_name": "Split",
 61174|       "member_kind": "method",
 61175|       "edge_type": "RETURNS_ELEMENT_IDS",
 61176|       "confidence": "unknown_reference",
 61177|       "confidence_tier": "unverified_reference",
 61178|       "target_resolution": "none",
 61179|       "evidence": [
 61180|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 61181|       ],
 61182|       "source_url": "https://www.revitapidocs.com/2025/a7760dc6-515d-6ac2-6164-d0581c7d6dee.htm",
 61183|       "dll_signature_verified": true,
 61184|       "dll_relationship_scope": "declared",
 61185|       "dll_semantic_verified": null,
 61186|       "dll_verified_status": "signature_verified_declared",
 61187|       "revitlookup_referenced": null,
 61188|       "revitlookup_requires_document_context": null
 61189|     },
 61190|     {
 61191|       "source": "Autodesk.Revit.DB.ToposolidType",
 61192|       "target": "Autodesk.Revit.DB.ContourSetting",
 61193|       "member_name": "GetContourSetting",
 61194|       "member_kind": "method",
 61195|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61196|       "confidence": "direct_return_type",
 61197|       "confidence_tier": "unverified_reference",
 61198|       "target_resolution": "exact",
 61199|       "evidence": [
 61200|         "return type 'ContourSetting' directly names a Revit DB object type"
 61201|       ],
 61202|       "source_url": "https://www.revitapidocs.com/2025/916e164a-0d63-6d1d-790b-08303219d9b9.htm",
 61203|       "dll_signature_verified": true,
 61204|       "dll_relationship_scope": "declared",
 61205|       "dll_semantic_verified": null,
 61206|       "dll_verified_status": "signature_verified_declared",
 61207|       "revitlookup_referenced": null,
 61208|       "revitlookup_requires_document_context": null
 61209|     },
 61210|     {
 61211|       "source": "Autodesk.Revit.DB.Transaction",
 61212|       "target": "Autodesk.Revit.DB.FailureHandlingOptions",
 61213|       "member_name": "GetFailureHandlingOptions",
 61214|       "member_kind": "method",
 61215|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61216|       "confidence": "direct_return_type",
 61217|       "confidence_tier": "unverified_reference",
 61218|       "target_resolution": "exact",
 61219|       "evidence": [
 61220|         "return type 'FailureHandlingOptions' directly names a Revit DB object type"
 61221|       ],
 61222|       "source_url": "https://www.revitapidocs.com/2025/f306f808-a753-1585-18ef-57d65e76fad4.htm",
 61223|       "dll_signature_verified": true,
 61224|       "dll_relationship_scope": "declared",
 61225|       "dll_semantic_verified": null,
 61226|       "dll_verified_status": "signature_verified_declared",
 61227|       "revitlookup_referenced": null,
 61228|       "revitlookup_requires_document_context": null
 61229|     },
 61230|     {
 61231|       "source": "Autodesk.Revit.DB.TransactWithCentralOptions",
 61232|       "target": "Autodesk.Revit.DB.ICentralLockedCallback",
 61233|       "member_name": "GetLockCallback",
 61234|       "member_kind": "method",
 61235|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61236|       "confidence": "direct_return_type",
 61237|       "confidence_tier": "unverified_reference",
 61238|       "target_resolution": "exact",
 61239|       "evidence": [
 61240|         "return type 'ICentralLockedCallback' directly names a Revit DB object type"
```
