# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 214 of 216
- Original line range: 83071-83470
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 83071|       "revitlookup_requires_document_context": null
 83072|     },
 83073|     {
 83074|       "source": "Autodesk.Revit.DB.Structure.StructuralConnectionHandlerType",
 83075|       "target": null,
 83076|       "member_name": "FindGenericConnectionType",
 83077|       "member_kind": "method",
 83078|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 83079|       "confidence": "unknown_reference",
 83080|       "confidence_tier": "unverified_reference",
 83081|       "target_resolution": "none",
 83082|       "evidence": [
 83083|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 83084|       ],
 83085|       "source_url": "https://www.revitapidocs.com/2025/83298a62-78c9-8182-f742-a4aeff747bec.htm",
 83086|       "dll_signature_verified": true,
 83087|       "dll_relationship_scope": "declared",
 83088|       "dll_semantic_verified": null,
 83089|       "dll_verified_status": "signature_verified_declared",
 83090|       "revitlookup_referenced": null,
 83091|       "revitlookup_requires_document_context": null
 83092|     },
 83093|     {
 83094|       "source": "Autodesk.Revit.DB.Structure.StructuralConnectionHandlerType",
 83095|       "target": null,
 83096|       "member_name": "GetDefaultConnectionHandlerType",
 83097|       "member_kind": "method",
 83098|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 83099|       "confidence": "unknown_reference",
 83100|       "confidence_tier": "unverified_reference",
 83101|       "target_resolution": "none",
 83102|       "evidence": [
 83103|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 83104|       ],
 83105|       "source_url": "https://www.revitapidocs.com/2025/f9b606a4-2656-dca7-8f8e-469630bd151e.htm",
 83106|       "dll_signature_verified": true,
 83107|       "dll_relationship_scope": "declared",
 83108|       "dll_semantic_verified": null,
 83109|       "dll_verified_status": "signature_verified_declared",
 83110|       "revitlookup_referenced": null,
 83111|       "revitlookup_requires_document_context": null
 83112|     },
 83113|     {
 83114|       "source": "Autodesk.Revit.DB.Structure.StructuralConnectionType",
 83115|       "target": null,
 83116|       "member_name": "GetAllStructuralConnectionTypeIds",
 83117|       "member_kind": "method",
 83118|       "edge_type": "RETURNS_ELEMENT_IDS",
 83119|       "confidence": "name_only_candidate",
 83120|       "confidence_tier": "likely",
 83121|       "target_resolution": "none",
 83122|       "evidence": [
 83123|         "member name 'GetAllStructuralConnectionTypeIds' matches keyword pattern /^GetAll/ but return type 'void' gives no type-level confirmation"
 83124|       ],
 83125|       "source_url": "https://www.revitapidocs.com/2025/33908a35-a8d8-dfe1-abd2-a59eaaa77045.htm",
 83126|       "dll_signature_verified": true,
 83127|       "dll_relationship_scope": "declared",
 83128|       "dll_semantic_verified": null,
 83129|       "dll_verified_status": "signature_verified_declared",
 83130|       "revitlookup_referenced": null,
 83131|       "revitlookup_requires_document_context": null
 83132|     },
 83133|     {
 83134|       "source": "Autodesk.Revit.DB.Structure.StructuralConnectionType",
 83135|       "target": null,
 83136|       "member_name": "GetFamilySymbolId",
 83137|       "member_kind": "method",
 83138|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 83139|       "confidence": "unknown_reference",
 83140|       "confidence_tier": "unverified_reference",
 83141|       "target_resolution": "none",
 83142|       "evidence": [
 83143|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 83144|       ],
 83145|       "source_url": "https://www.revitapidocs.com/2025/33ed961b-ba63-84c8-4cea-499ad8c2efc3.htm",
 83146|       "dll_signature_verified": true,
 83147|       "dll_relationship_scope": "declared",
 83148|       "dll_semantic_verified": null,
 83149|       "dll_verified_status": "signature_verified_declared",
 83150|       "revitlookup_referenced": null,
 83151|       "revitlookup_requires_document_context": null
 83152|     },
 83153|     {
 83154|       "source": "Autodesk.Revit.DB.Structure.StructuralFramingUtils",
 83155|       "target": "Autodesk.Revit.DB.Reference",
 83156|       "member_name": "GetEndReference",
 83157|       "member_kind": "method",
 83158|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 83159|       "confidence": "direct_return_type",
 83160|       "confidence_tier": "unverified_reference",
 83161|       "target_resolution": "exact",
 83162|       "evidence": [
 83163|         "return type 'Reference' directly names a Revit DB object type"
 83164|       ],
 83165|       "source_url": "https://www.revitapidocs.com/2025/0d5d008a-5317-357f-e4d4-46d8a745a494.htm",
 83166|       "dll_signature_verified": true,
 83167|       "dll_relationship_scope": "declared",
 83168|       "dll_semantic_verified": null,
 83169|       "dll_verified_status": "signature_verified_declared",
 83170|       "revitlookup_referenced": null,
 83171|       "revitlookup_requires_document_context": null
 83172|     },
 83173|     {
 83174|       "source": "Autodesk.Revit.DB.Structure.StructuralMaterialTypeFilter",
 83175|       "target": "Autodesk.Revit.DB.Material",
 83176|       "member_name": "StructuralMaterialType",
 83177|       "member_kind": "property",
 83178|       "edge_type": "USES_MATERIAL",
 83179|       "confidence": "name_only_candidate",
 83180|       "confidence_tier": "likely",
 83181|       "target_resolution": "exact",
 83182|       "evidence": [
 83183|         "member name 'StructuralMaterialType' matches keyword pattern /Material/ but return type 'StructuralMaterialType' gives no type-level confirmation"
 83184|       ],
 83185|       "source_url": "https://www.revitapidocs.com/2025/ea0c3f86-529c-71c1-e86e-dc1abc7efa95.htm",
 83186|       "dll_signature_verified": true,
 83187|       "dll_relationship_scope": "declared",
 83188|       "dll_semantic_verified": null,
 83189|       "dll_verified_status": "signature_verified_declared",
 83190|       "revitlookup_referenced": null,
 83191|       "revitlookup_requires_document_context": null
 83192|     },
 83193|     {
 83194|       "source": "Autodesk.Revit.DB.Structure.StructuralSectionsServiceData",
 83195|       "target": "Autodesk.Revit.DB.Document",
 83196|       "member_name": "Document",
 83197|       "member_kind": "property",
 83198|       "edge_type": "REFERENCES",
 83199|       "confidence": "direct_return_type",
 83200|       "confidence_tier": "core",
 83201|       "target_resolution": "exact",
 83202|       "evidence": [
 83203|         "return type 'Document' directly names a Revit DB object type"
 83204|       ],
 83205|       "source_url": "https://www.revitapidocs.com/2025/14dad60e-6e26-1927-7d62-ec8645c02fa8.htm",
 83206|       "dll_signature_verified": true,
 83207|       "dll_relationship_scope": "declared",
 83208|       "dll_semantic_verified": null,
 83209|       "dll_verified_status": "signature_verified_declared",
 83210|       "revitlookup_referenced": null,
 83211|       "revitlookup_requires_document_context": null
 83212|     },
 83213|     {
 83214|       "source": "Autodesk.Revit.DB.Structure.StructuralSectionsServiceData",
 83215|       "target": null,
 83216|       "member_name": "GetCurrentElements",
 83217|       "member_kind": "method",
 83218|       "edge_type": "RETURNS_ELEMENT_IDS",
 83219|       "confidence": "unknown_reference",
 83220|       "confidence_tier": "unverified_reference",
 83221|       "target_resolution": "none",
 83222|       "evidence": [
 83223|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 83224|       ],
 83225|       "source_url": "https://www.revitapidocs.com/2025/5f274e00-ed2f-38a6-6a25-aaffa3968520.htm",
 83226|       "dll_signature_verified": true,
 83227|       "dll_relationship_scope": "declared",
 83228|       "dll_semantic_verified": null,
 83229|       "dll_verified_status": "signature_verified_declared",
 83230|       "revitlookup_referenced": null,
 83231|       "revitlookup_requires_document_context": null
 83232|     },
 83233|     {
 83234|       "source": "Autodesk.Revit.DB.Structure.StructuralSettings",
 83235|       "target": null,
 83236|       "member_name": "BoundaryConditionFamilySymbolFixed",
 83237|       "member_kind": "property",
 83238|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 83239|       "confidence": "unknown_reference",
 83240|       "confidence_tier": "unverified_reference",
 83241|       "target_resolution": "none",
 83242|       "evidence": [
 83243|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 83244|       ],
 83245|       "source_url": "https://www.revitapidocs.com/2025/64993be8-d46b-527f-67c5-5f9d63240b3c.htm",
 83246|       "dll_signature_verified": true,
 83247|       "dll_relationship_scope": "declared",
 83248|       "dll_semantic_verified": null,
 83249|       "dll_verified_status": "signature_verified_declared",
 83250|       "revitlookup_referenced": null,
 83251|       "revitlookup_requires_document_context": null
 83252|     },
 83253|     {
 83254|       "source": "Autodesk.Revit.DB.Structure.StructuralSettings",
 83255|       "target": null,
 83256|       "member_name": "BoundaryConditionFamilySymbolPinned",
 83257|       "member_kind": "property",
 83258|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 83259|       "confidence": "unknown_reference",
 83260|       "confidence_tier": "unverified_reference",
 83261|       "target_resolution": "none",
 83262|       "evidence": [
 83263|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 83264|       ],
 83265|       "source_url": "https://www.revitapidocs.com/2025/d8839f2b-4024-baf7-bf62-6a792f633097.htm",
 83266|       "dll_signature_verified": true,
 83267|       "dll_relationship_scope": "declared",
 83268|       "dll_semantic_verified": null,
 83269|       "dll_verified_status": "signature_verified_declared",
 83270|       "revitlookup_referenced": null,
 83271|       "revitlookup_requires_document_context": null
 83272|     },
 83273|     {
 83274|       "source": "Autodesk.Revit.DB.Structure.StructuralSettings",
 83275|       "target": null,
 83276|       "member_name": "BoundaryConditionFamilySymbolRoller",
 83277|       "member_kind": "property",
 83278|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 83279|       "confidence": "unknown_reference",
 83280|       "confidence_tier": "unverified_reference",
 83281|       "target_resolution": "none",
 83282|       "evidence": [
 83283|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 83284|       ],
 83285|       "source_url": "https://www.revitapidocs.com/2025/75e1056d-07de-cfe4-6bb3-5a549bcbbe69.htm",
 83286|       "dll_signature_verified": true,
 83287|       "dll_relationship_scope": "declared",
 83288|       "dll_semantic_verified": null,
 83289|       "dll_verified_status": "signature_verified_declared",
 83290|       "revitlookup_referenced": null,
 83291|       "revitlookup_requires_document_context": null
 83292|     },
 83293|     {
 83294|       "source": "Autodesk.Revit.DB.Structure.StructuralSettings",
 83295|       "target": null,
 83296|       "member_name": "BoundaryConditionFamilySymbolUserDefined",
 83297|       "member_kind": "property",
 83298|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 83299|       "confidence": "unknown_reference",
 83300|       "confidence_tier": "unverified_reference",
 83301|       "target_resolution": "none",
 83302|       "evidence": [
 83303|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 83304|       ],
 83305|       "source_url": "https://www.revitapidocs.com/2025/fe698c55-f209-acd4-afd1-14223f7be0f4.htm",
 83306|       "dll_signature_verified": true,
 83307|       "dll_relationship_scope": "declared",
 83308|       "dll_semantic_verified": null,
 83309|       "dll_verified_status": "signature_verified_declared",
 83310|       "revitlookup_referenced": null,
 83311|       "revitlookup_requires_document_context": null
 83312|     },
 83313|     {
 83314|       "source": "Autodesk.Revit.DB.Structure.StructuralSettings",
 83315|       "target": null,
 83316|       "member_name": "BraceAboveSymbol",
 83317|       "member_kind": "property",
 83318|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 83319|       "confidence": "unknown_reference",
 83320|       "confidence_tier": "unverified_reference",
 83321|       "target_resolution": "none",
 83322|       "evidence": [
 83323|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 83324|       ],
 83325|       "source_url": "https://www.revitapidocs.com/2025/c62a04e2-84cd-1458-3ad8-0fe9410d5778.htm",
 83326|       "dll_signature_verified": true,
 83327|       "dll_relationship_scope": "declared",
 83328|       "dll_semantic_verified": null,
 83329|       "dll_verified_status": "signature_verified_declared",
 83330|       "revitlookup_referenced": null,
 83331|       "revitlookup_requires_document_context": null
 83332|     },
 83333|     {
 83334|       "source": "Autodesk.Revit.DB.Structure.StructuralSettings",
 83335|       "target": null,
 83336|       "member_name": "BraceBelowSymbol",
 83337|       "member_kind": "property",
 83338|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 83339|       "confidence": "unknown_reference",
 83340|       "confidence_tier": "unverified_reference",
 83341|       "target_resolution": "none",
 83342|       "evidence": [
 83343|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 83344|       ],
 83345|       "source_url": "https://www.revitapidocs.com/2025/825c1d61-be16-df37-2f08-06e6651e8203.htm",
 83346|       "dll_signature_verified": true,
 83347|       "dll_relationship_scope": "declared",
 83348|       "dll_semantic_verified": null,
 83349|       "dll_verified_status": "signature_verified_declared",
 83350|       "revitlookup_referenced": null,
 83351|       "revitlookup_requires_document_context": null
 83352|     },
 83353|     {
 83354|       "source": "Autodesk.Revit.DB.Structure.StructuralSettings",
 83355|       "target": null,
 83356|       "member_name": "KickerBraceSymbol",
 83357|       "member_kind": "property",
 83358|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 83359|       "confidence": "unknown_reference",
 83360|       "confidence_tier": "unverified_reference",
 83361|       "target_resolution": "none",
 83362|       "evidence": [
 83363|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 83364|       ],
 83365|       "source_url": "https://www.revitapidocs.com/2025/ed543557-7917-0666-a9ae-3beb9f33fb6a.htm",
 83366|       "dll_signature_verified": true,
 83367|       "dll_relationship_scope": "declared",
 83368|       "dll_semantic_verified": null,
 83369|       "dll_verified_status": "signature_verified_declared",
 83370|       "revitlookup_referenced": null,
 83371|       "revitlookup_requires_document_context": null
 83372|     },
 83373|     {
 83374|       "source": "Autodesk.Revit.DB.Structure.Truss",
 83375|       "target": null,
 83376|       "member_name": "Members",
 83377|       "member_kind": "property",
 83378|       "edge_type": "RETURNS_ELEMENT_IDS",
 83379|       "confidence": "unknown_reference",
 83380|       "confidence_tier": "unverified_reference",
 83381|       "target_resolution": "none",
 83382|       "evidence": [
 83383|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 83384|       ],
 83385|       "source_url": "https://www.revitapidocs.com/2025/ac8c1ca8-0d19-706e-403b-3be82c6f082f.htm",
 83386|       "dll_signature_verified": true,
 83387|       "dll_relationship_scope": "declared",
 83388|       "dll_semantic_verified": null,
 83389|       "dll_verified_status": "signature_verified_declared",
 83390|       "revitlookup_referenced": null,
 83391|       "revitlookup_requires_document_context": null
 83392|     },
 83393|     {
 83394|       "source": "Autodesk.Revit.DB.Structure.Truss",
 83395|       "target": "Autodesk.Revit.DB.Structure.TrussType",
 83396|       "member_name": "TrussType",
 83397|       "member_kind": "property",
 83398|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 83399|       "confidence": "direct_return_type",
 83400|       "confidence_tier": "unverified_reference",
 83401|       "target_resolution": "short_name_fallback",
 83402|       "evidence": [
 83403|         "return type 'TrussType' directly names a Revit DB object type"
 83404|       ],
 83405|       "source_url": "https://www.revitapidocs.com/2025/e5aac783-7724-bc13-3d49-002edf50ef66.htm",
 83406|       "dll_signature_verified": true,
 83407|       "dll_relationship_scope": "declared",
 83408|       "dll_semantic_verified": null,
 83409|       "dll_verified_status": "signature_verified_declared",
 83410|       "revitlookup_referenced": null,
 83411|       "revitlookup_requires_document_context": null
 83412|     },
 83413|     {
 83414|       "source": "Autodesk.Revit.DB.Structure.Truss",
 83415|       "target": "Autodesk.Revit.DB.Structure.TrussMemberInfo",
 83416|       "member_name": "GetTrussMemberInfo",
 83417|       "member_kind": "method",
 83418|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 83419|       "confidence": "direct_return_type",
 83420|       "confidence_tier": "unverified_reference",
 83421|       "target_resolution": "short_name_fallback",
 83422|       "evidence": [
 83423|         "return type 'TrussMemberInfo' directly names a Revit DB object type"
 83424|       ],
 83425|       "source_url": "https://www.revitapidocs.com/2025/885bcdab-10c1-5857-29db-3b95fe43be6d.htm",
 83426|       "dll_signature_verified": true,
 83427|       "dll_relationship_scope": "declared",
 83428|       "dll_semantic_verified": null,
 83429|       "dll_verified_status": "signature_verified_declared",
 83430|       "revitlookup_referenced": null,
 83431|       "revitlookup_requires_document_context": null
 83432|     },
 83433|     {
 83434|       "source": "Autodesk.Revit.DB.Structure.TrussMemberInfo",
 83435|       "target": null,
 83436|       "member_name": "hostTrussId",
 83437|       "member_kind": "property",
 83438|       "edge_type": "HOSTED_BY",
 83439|       "confidence": "elementid_with_strong_name",
 83440|       "confidence_tier": "core",
 83441|       "target_resolution": "none",
 83442|       "evidence": [
 83443|         "member name 'hostTrussId' matches keyword pattern /^GetHosted|Host/"
 83444|       ],
 83445|       "source_url": "https://www.revitapidocs.com/2025/108d4d33-4879-c6a0-7773-f645f88692a5.htm",
 83446|       "dll_signature_verified": true,
 83447|       "dll_relationship_scope": "declared",
 83448|       "dll_semantic_verified": null,
 83449|       "dll_verified_status": "signature_verified_declared",
 83450|       "revitlookup_referenced": null,
 83451|       "revitlookup_requires_document_context": null
 83452|     },
 83453|     {
 83454|       "source": "Autodesk.Revit.DB.Structure.StructuralSections.StructuralElementDefinitionData",
 83455|       "target": "Autodesk.Revit.DB.Structure.StructuralSections.StructuralSection",
 83456|       "member_name": "Section",
 83457|       "member_kind": "property",
 83458|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 83459|       "confidence": "direct_return_type",
 83460|       "confidence_tier": "unverified_reference",
 83461|       "target_resolution": "short_name_fallback",
 83462|       "evidence": [
 83463|         "return type 'StructuralSection' directly names a Revit DB object type"
 83464|       ],
 83465|       "source_url": "https://www.revitapidocs.com/2025/703a784b-7136-8c4a-b99b-5b0b9b0b66b1.htm",
 83466|       "dll_signature_verified": true,
 83467|       "dll_relationship_scope": "declared",
 83468|       "dll_semantic_verified": null,
 83469|       "dll_verified_status": "signature_verified_declared",
 83470|       "revitlookup_referenced": null,
```
