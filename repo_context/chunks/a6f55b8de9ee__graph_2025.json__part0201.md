# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 201 of 216
- Original line range: 78001-78400
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 78001|       "dll_relationship_scope": "declared",
 78002|       "dll_semantic_verified": null,
 78003|       "dll_verified_status": "signature_verified_declared",
 78004|       "revitlookup_referenced": null,
 78005|       "revitlookup_requires_document_context": null
 78006|     },
 78007|     {
 78008|       "source": "Autodesk.Revit.DB.Structure.BoundaryConditions",
 78009|       "target": null,
 78010|       "member_name": "HostElementId",
 78011|       "member_kind": "property",
 78012|       "edge_type": "HOSTED_BY",
 78013|       "confidence": "elementid_with_strong_name",
 78014|       "confidence_tier": "core",
 78015|       "target_resolution": "none",
 78016|       "evidence": [
 78017|         "member name 'HostElementId' matches keyword pattern /^GetHosted|Host/"
 78018|       ],
 78019|       "source_url": "https://www.revitapidocs.com/2025/9e930666-e2e2-dcff-d441-b85daac15e00.htm",
 78020|       "dll_signature_verified": true,
 78021|       "dll_relationship_scope": "declared",
 78022|       "dll_semantic_verified": null,
 78023|       "dll_verified_status": "signature_verified_declared",
 78024|       "revitlookup_referenced": null,
 78025|       "revitlookup_requires_document_context": null
 78026|     },
 78027|     {
 78028|       "source": "Autodesk.Revit.DB.Structure.CodeCheckingParameterServiceData",
 78029|       "target": "Autodesk.Revit.DB.Document",
 78030|       "member_name": "Document",
 78031|       "member_kind": "property",
 78032|       "edge_type": "REFERENCES",
 78033|       "confidence": "direct_return_type",
 78034|       "confidence_tier": "core",
 78035|       "target_resolution": "exact",
 78036|       "evidence": [
 78037|         "return type 'Document' directly names a Revit DB object type"
 78038|       ],
 78039|       "source_url": "https://www.revitapidocs.com/2025/b0cf1924-926e-2eba-1093-467c6084f1c6.htm",
 78040|       "dll_signature_verified": true,
 78041|       "dll_relationship_scope": "declared",
 78042|       "dll_semantic_verified": null,
 78043|       "dll_verified_status": "signature_verified_declared",
 78044|       "revitlookup_referenced": null,
 78045|       "revitlookup_requires_document_context": null
 78046|     },
 78047|     {
 78048|       "source": "Autodesk.Revit.DB.Structure.CodeCheckingParameterServiceData",
 78049|       "target": null,
 78050|       "member_name": "GetCurrentElements",
 78051|       "member_kind": "method",
 78052|       "edge_type": "RETURNS_ELEMENT_IDS",
 78053|       "confidence": "unknown_reference",
 78054|       "confidence_tier": "unverified_reference",
 78055|       "target_resolution": "none",
 78056|       "evidence": [
 78057|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 78058|       ],
 78059|       "source_url": "https://www.revitapidocs.com/2025/b262877a-d1a8-145a-2aff-861e62304944.htm",
 78060|       "dll_signature_verified": true,
 78061|       "dll_relationship_scope": "declared",
 78062|       "dll_semantic_verified": null,
 78063|       "dll_verified_status": "signature_verified_declared",
 78064|       "revitlookup_referenced": null,
 78065|       "revitlookup_requires_document_context": null
 78066|     },
 78067|     {
 78068|       "source": "Autodesk.Revit.DB.Structure.FabricArea",
 78069|       "target": "Autodesk.Revit.DB.Structure.FabricAreaType",
 78070|       "member_name": "FabricAreaType",
 78071|       "member_kind": "property",
 78072|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 78073|       "confidence": "direct_return_type",
 78074|       "confidence_tier": "unverified_reference",
 78075|       "target_resolution": "short_name_fallback",
 78076|       "evidence": [
 78077|         "return type 'FabricAreaType' directly names a Revit DB object type"
 78078|       ],
 78079|       "source_url": "https://www.revitapidocs.com/2025/132f328f-c6e9-685e-c7e0-a24eaae6c234.htm",
 78080|       "dll_signature_verified": true,
 78081|       "dll_relationship_scope": "declared",
 78082|       "dll_semantic_verified": null,
 78083|       "dll_verified_status": "signature_verified_declared",
 78084|       "revitlookup_referenced": null,
 78085|       "revitlookup_requires_document_context": null
 78086|     },
 78087|     {
 78088|       "source": "Autodesk.Revit.DB.Structure.FabricArea",
 78089|       "target": "Autodesk.Revit.DB.ViewSheet",
 78090|       "member_name": "FabricSheetTypeId",
 78091|       "member_kind": "property",
 78092|       "edge_type": "PLACED_ON_SHEET",
 78093|       "confidence": "elementid_with_strong_name",
 78094|       "confidence_tier": "core",
 78095|       "target_resolution": "exact",
 78096|       "evidence": [
 78097|         "member name 'FabricSheetTypeId' matches keyword pattern /Sheet/"
 78098|       ],
 78099|       "source_url": "https://www.revitapidocs.com/2025/1cf0d022-cc11-723b-88ee-6521b3700b29.htm",
 78100|       "dll_signature_verified": true,
 78101|       "dll_relationship_scope": "declared",
 78102|       "dll_semantic_verified": null,
 78103|       "dll_verified_status": "signature_verified_declared",
 78104|       "revitlookup_referenced": null,
 78105|       "revitlookup_requires_document_context": null
 78106|     },
 78107|     {
 78108|       "source": "Autodesk.Revit.DB.Structure.FabricArea",
 78109|       "target": null,
 78110|       "member_name": "HostId",
 78111|       "member_kind": "property",
 78112|       "edge_type": "HOSTED_BY",
 78113|       "confidence": "elementid_with_strong_name",
 78114|       "confidence_tier": "core",
 78115|       "target_resolution": "none",
 78116|       "evidence": [
 78117|         "member name 'HostId' matches keyword pattern /^GetHosted|Host/"
 78118|       ],
 78119|       "source_url": "https://www.revitapidocs.com/2025/f61a779d-6368-ac14-4d9b-e038eafa27ed.htm",
 78120|       "dll_signature_verified": true,
 78121|       "dll_relationship_scope": "declared",
 78122|       "dll_semantic_verified": null,
 78123|       "dll_verified_status": "signature_verified_declared",
 78124|       "revitlookup_referenced": null,
 78125|       "revitlookup_requires_document_context": null
 78126|     },
 78127|     {
 78128|       "source": "Autodesk.Revit.DB.Structure.FabricArea",
 78129|       "target": "Autodesk.Revit.DB.ViewSheet",
 78130|       "member_name": "MajorSheetAlignment",
 78131|       "member_kind": "property",
 78132|       "edge_type": "PLACED_ON_SHEET",
 78133|       "confidence": "name_only_candidate",
 78134|       "confidence_tier": "likely",
 78135|       "target_resolution": "exact",
 78136|       "evidence": [
 78137|         "member name 'MajorSheetAlignment' matches keyword pattern /Sheet/ but return type 'FabricSheetAlignment' gives no type-level confirmation"
 78138|       ],
 78139|       "source_url": "https://www.revitapidocs.com/2025/cc8ee7a1-806a-c522-112f-e38fec71a708.htm",
 78140|       "dll_signature_verified": true,
 78141|       "dll_relationship_scope": "declared",
 78142|       "dll_semantic_verified": null,
 78143|       "dll_verified_status": "signature_verified_declared",
 78144|       "revitlookup_referenced": null,
 78145|       "revitlookup_requires_document_context": null
 78146|     },
 78147|     {
 78148|       "source": "Autodesk.Revit.DB.Structure.FabricArea",
 78149|       "target": "Autodesk.Revit.DB.ViewSheet",
 78150|       "member_name": "MinorSheetAlignment",
 78151|       "member_kind": "property",
 78152|       "edge_type": "PLACED_ON_SHEET",
 78153|       "confidence": "name_only_candidate",
 78154|       "confidence_tier": "likely",
 78155|       "target_resolution": "exact",
 78156|       "evidence": [
 78157|         "member name 'MinorSheetAlignment' matches keyword pattern /Sheet/ but return type 'FabricSheetAlignment' gives no type-level confirmation"
 78158|       ],
 78159|       "source_url": "https://www.revitapidocs.com/2025/95451caf-0948-3236-be43-892771a42c26.htm",
 78160|       "dll_signature_verified": true,
 78161|       "dll_relationship_scope": "declared",
 78162|       "dll_semantic_verified": null,
 78163|       "dll_verified_status": "signature_verified_declared",
 78164|       "revitlookup_referenced": null,
 78165|       "revitlookup_requires_document_context": null
 78166|     },
 78167|     {
 78168|       "source": "Autodesk.Revit.DB.Structure.FabricArea",
 78169|       "target": "Autodesk.Revit.DB.Sketch",
 78170|       "member_name": "SketchId",
 78171|       "member_kind": "property",
 78172|       "edge_type": "DEPENDS_ON",
 78173|       "confidence": "elementid_with_strong_name",
 78174|       "confidence_tier": "core",
 78175|       "target_resolution": "exact",
 78176|       "evidence": [
 78177|         "member name 'SketchId' matches keyword pattern /Sketch(Id)?$/"
 78178|       ],
 78179|       "source_url": "https://www.revitapidocs.com/2025/54174aa5-f45a-c7f5-5b65-5a3a9ea3af95.htm",
 78180|       "dll_signature_verified": true,
 78181|       "dll_relationship_scope": "declared",
 78182|       "dll_semantic_verified": null,
 78183|       "dll_verified_status": "signature_verified_declared",
 78184|       "revitlookup_referenced": null,
 78185|       "revitlookup_requires_document_context": null
 78186|     },
 78187|     {
 78188|       "source": "Autodesk.Revit.DB.Structure.FabricArea",
 78189|       "target": null,
 78190|       "member_name": "TagViewId",
 78191|       "member_kind": "property",
 78192|       "edge_type": "TAGS_ELEMENT",
 78193|       "confidence": "elementid_with_strong_name",
 78194|       "confidence_tier": "core",
 78195|       "target_resolution": "none",
 78196|       "evidence": [
 78197|         "member name 'TagViewId' matches keyword pattern /^GetTagged|Tag(ged)?/"
 78198|       ],
 78199|       "source_url": "https://www.revitapidocs.com/2025/8ab10b80-d3ee-199f-b87d-8356a85d0109.htm",
 78200|       "dll_signature_verified": true,
 78201|       "dll_relationship_scope": "declared",
 78202|       "dll_semantic_verified": null,
 78203|       "dll_verified_status": "signature_verified_declared",
 78204|       "revitlookup_referenced": null,
 78205|       "revitlookup_requires_document_context": null
 78206|     },
 78207|     {
 78208|       "source": "Autodesk.Revit.DB.Structure.FabricArea",
 78209|       "target": null,
 78210|       "member_name": "GetBoundaryCurveIds",
 78211|       "member_kind": "method",
 78212|       "edge_type": "RETURNS_ELEMENT_IDS",
 78213|       "confidence": "unknown_reference",
 78214|       "confidence_tier": "unverified_reference",
 78215|       "target_resolution": "none",
 78216|       "evidence": [
 78217|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 78218|       ],
 78219|       "source_url": "https://www.revitapidocs.com/2025/eae59f38-12a2-87cb-a153-642aa88d6c55.htm",
 78220|       "dll_signature_verified": true,
 78221|       "dll_relationship_scope": "declared",
 78222|       "dll_semantic_verified": null,
 78223|       "dll_verified_status": "signature_verified_declared",
 78224|       "revitlookup_referenced": null,
 78225|       "revitlookup_requires_document_context": null
 78226|     },
 78227|     {
 78228|       "source": "Autodesk.Revit.DB.Structure.FabricArea",
 78229|       "target": "Autodesk.Revit.DB.ViewSheet",
 78230|       "member_name": "GetFabricSheetElementIds",
 78231|       "member_kind": "method",
 78232|       "edge_type": "PLACED_ON_SHEET",
 78233|       "confidence": "elementid_collection_with_strong_name",
 78234|       "confidence_tier": "core",
 78235|       "target_resolution": "exact",
 78236|       "evidence": [
 78237|         "member name 'GetFabricSheetElementIds' matches keyword pattern /Sheet/"
 78238|       ],
 78239|       "source_url": "https://www.revitapidocs.com/2025/7c1f56f7-0dd8-d0b6-c49e-b70c3e34bafd.htm",
 78240|       "dll_signature_verified": true,
 78241|       "dll_relationship_scope": "declared",
 78242|       "dll_semantic_verified": null,
 78243|       "dll_verified_status": "signature_verified_declared",
 78244|       "revitlookup_referenced": null,
 78245|       "revitlookup_requires_document_context": null
 78246|     },
 78247|     {
 78248|       "source": "Autodesk.Revit.DB.Structure.FabricArea",
 78249|       "target": "Autodesk.Revit.DB.Structure.FabricRoundingManager",
 78250|       "member_name": "GetReinforcementRoundingManager",
 78251|       "member_kind": "method",
 78252|       "edge_type": "REFERENCES",
 78253|       "confidence": "direct_return_type",
 78254|       "confidence_tier": "core",
 78255|       "target_resolution": "short_name_fallback",
 78256|       "evidence": [
 78257|         "return type 'FabricRoundingManager' directly names a Revit DB object type"
 78258|       ],
 78259|       "source_url": "https://www.revitapidocs.com/2025/6dc448b9-1be0-95f7-a38d-c540a440c5e3.htm",
 78260|       "dll_signature_verified": true,
 78261|       "dll_relationship_scope": "declared",
 78262|       "dll_semantic_verified": null,
 78263|       "dll_verified_status": "signature_verified_declared",
 78264|       "revitlookup_referenced": null,
 78265|       "revitlookup_requires_document_context": null
 78266|     },
 78267|     {
 78268|       "source": "Autodesk.Revit.DB.Structure.FabricArea",
 78269|       "target": "Autodesk.Revit.DB.ViewSheet",
 78270|       "member_name": "GetTotalSheetMass",
 78271|       "member_kind": "method",
 78272|       "edge_type": "PLACED_ON_SHEET",
 78273|       "confidence": "name_only_candidate",
 78274|       "confidence_tier": "likely",
 78275|       "target_resolution": "exact",
 78276|       "evidence": [
 78277|         "member name 'GetTotalSheetMass' matches keyword pattern /Sheet/ but return type 'double' gives no type-level confirmation"
 78278|       ],
 78279|       "source_url": "https://www.revitapidocs.com/2025/53bbfc7e-3255-6e68-7339-5dd39af0d75b.htm",
 78280|       "dll_signature_verified": true,
 78281|       "dll_relationship_scope": "declared",
 78282|       "dll_semantic_verified": null,
 78283|       "dll_verified_status": "signature_verified_declared",
 78284|       "revitlookup_referenced": null,
 78285|       "revitlookup_requires_document_context": null
 78286|     },
 78287|     {
 78288|       "source": "Autodesk.Revit.DB.Structure.FabricArea",
 78289|       "target": null,
 78290|       "member_name": "GetValidViewsForTags",
 78291|       "member_kind": "method",
 78292|       "edge_type": "TAGS_ELEMENT",
 78293|       "confidence": "elementid_collection_with_strong_name",
 78294|       "confidence_tier": "core",
 78295|       "target_resolution": "none",
 78296|       "evidence": [
 78297|         "member name 'GetValidViewsForTags' matches keyword pattern /^GetTagged|Tag(ged)?/"
 78298|       ],
 78299|       "source_url": "https://www.revitapidocs.com/2025/be0c10c4-f8b0-0149-0a73-2879cdec5076.htm",
 78300|       "dll_signature_verified": true,
 78301|       "dll_relationship_scope": "declared",
 78302|       "dll_semantic_verified": null,
 78303|       "dll_verified_status": "signature_verified_declared",
 78304|       "revitlookup_referenced": null,
 78305|       "revitlookup_requires_document_context": null
 78306|     },
 78307|     {
 78308|       "source": "Autodesk.Revit.DB.Structure.FabricArea",
 78309|       "target": null,
 78310|       "member_name": "RemoveFabricReinforcementSystem",
 78311|       "member_kind": "method",
 78312|       "edge_type": "RETURNS_ELEMENT_IDS",
 78313|       "confidence": "unknown_reference",
 78314|       "confidence_tier": "unverified_reference",
 78315|       "target_resolution": "none",
 78316|       "evidence": [
 78317|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 78318|       ],
 78319|       "source_url": "https://www.revitapidocs.com/2025/f3972463-e0b1-6998-a12e-20ee51fbd6f0.htm",
 78320|       "dll_signature_verified": true,
 78321|       "dll_relationship_scope": "declared",
 78322|       "dll_semantic_verified": null,
 78323|       "dll_verified_status": "signature_verified_declared",
 78324|       "revitlookup_referenced": null,
 78325|       "revitlookup_requires_document_context": null
 78326|     },
 78327|     {
 78328|       "source": "Autodesk.Revit.DB.Structure.FabricSheet",
 78329|       "target": "Autodesk.Revit.DB.ViewSheet",
 78330|       "member_name": "CutSheetMass",
 78331|       "member_kind": "property",
 78332|       "edge_type": "PLACED_ON_SHEET",
 78333|       "confidence": "name_only_candidate",
 78334|       "confidence_tier": "likely",
 78335|       "target_resolution": "exact",
 78336|       "evidence": [
 78337|         "member name 'CutSheetMass' matches keyword pattern /Sheet/ but return type 'double' gives no type-level confirmation"
 78338|       ],
 78339|       "source_url": "https://www.revitapidocs.com/2025/31122be4-75b6-ed8d-6067-dc17a47cbf77.htm",
 78340|       "dll_signature_verified": true,
 78341|       "dll_relationship_scope": "declared",
 78342|       "dll_semantic_verified": null,
 78343|       "dll_verified_status": "signature_verified_declared",
 78344|       "revitlookup_referenced": null,
 78345|       "revitlookup_requires_document_context": null
 78346|     },
 78347|     {
 78348|       "source": "Autodesk.Revit.DB.Structure.FabricSheet",
 78349|       "target": null,
 78350|       "member_name": "FabricAreaOwnerId",
 78351|       "member_kind": "property",
 78352|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 78353|       "confidence": "unknown_reference",
 78354|       "confidence_tier": "unverified_reference",
 78355|       "target_resolution": "none",
 78356|       "evidence": [
 78357|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 78358|       ],
 78359|       "source_url": "https://www.revitapidocs.com/2025/927f2625-193d-208a-9bb4-4cc3234a6bfe.htm",
 78360|       "dll_signature_verified": true,
 78361|       "dll_relationship_scope": "declared",
 78362|       "dll_semantic_verified": null,
 78363|       "dll_verified_status": "signature_verified_declared",
 78364|       "revitlookup_referenced": null,
 78365|       "revitlookup_requires_document_context": null
 78366|     },
 78367|     {
 78368|       "source": "Autodesk.Revit.DB.Structure.FabricSheet",
 78369|       "target": null,
 78370|       "member_name": "FabricHostReference",
 78371|       "member_kind": "property",
 78372|       "edge_type": "HOSTED_BY",
 78373|       "confidence": "name_only_candidate",
 78374|       "confidence_tier": "likely",
 78375|       "target_resolution": "none",
 78376|       "evidence": [
 78377|         "member name 'FabricHostReference' matches keyword pattern /^GetHosted|Host/ but return type 'FabricHostReference' gives no type-level confirmation"
 78378|       ],
 78379|       "source_url": "https://www.revitapidocs.com/2025/e7b4d579-4362-b7b4-3aeb-4edd50c366e2.htm",
 78380|       "dll_signature_verified": true,
 78381|       "dll_relationship_scope": "declared",
 78382|       "dll_semantic_verified": null,
 78383|       "dll_verified_status": "signature_verified_declared",
 78384|       "revitlookup_referenced": null,
 78385|       "revitlookup_requires_document_context": null
 78386|     },
 78387|     {
 78388|       "source": "Autodesk.Revit.DB.Structure.FabricSheet",
 78389|       "target": null,
 78390|       "member_name": "HostId",
 78391|       "member_kind": "property",
 78392|       "edge_type": "HOSTED_BY",
 78393|       "confidence": "elementid_with_strong_name",
 78394|       "confidence_tier": "core",
 78395|       "target_resolution": "none",
 78396|       "evidence": [
 78397|         "member name 'HostId' matches keyword pattern /^GetHosted|Host/"
 78398|       ],
 78399|       "source_url": "https://www.revitapidocs.com/2025/f1c5db9c-4dfa-6f9e-3248-056b1460442a.htm",
 78400|       "dll_signature_verified": true,
```
