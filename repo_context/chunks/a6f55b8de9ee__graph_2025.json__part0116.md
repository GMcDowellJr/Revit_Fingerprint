# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 116 of 216
- Original line range: 44851-45250
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 44851|       "dll_semantic_verified": null,
 44852|       "dll_verified_status": "signature_verified_declared",
 44853|       "revitlookup_referenced": null,
 44854|       "revitlookup_requires_document_context": null
 44855|     },
 44856|     {
 44857|       "source": "Autodesk.Revit.DB.FamilyInstance",
 44858|       "target": "Autodesk.Revit.DB.Material",
 44859|       "member_name": "StructuralMaterialId",
 44860|       "member_kind": "property",
 44861|       "edge_type": "USES_MATERIAL",
 44862|       "confidence": "elementid_with_strong_name",
 44863|       "confidence_tier": "core",
 44864|       "target_resolution": "exact",
 44865|       "evidence": [
 44866|         "member name 'StructuralMaterialId' matches keyword pattern /Material/"
 44867|       ],
 44868|       "source_url": "https://www.revitapidocs.com/2025/856b95a1-38c9-4d61-59cd-2844f7348984.htm",
 44869|       "dll_signature_verified": true,
 44870|       "dll_relationship_scope": "declared",
 44871|       "dll_semantic_verified": null,
 44872|       "dll_verified_status": "signature_verified_declared",
 44873|       "revitlookup_referenced": null,
 44874|       "revitlookup_requires_document_context": null
 44875|     },
 44876|     {
 44877|       "source": "Autodesk.Revit.DB.FamilyInstance",
 44878|       "target": "Autodesk.Revit.DB.Material",
 44879|       "member_name": "StructuralMaterialType",
 44880|       "member_kind": "property",
 44881|       "edge_type": "USES_MATERIAL",
 44882|       "confidence": "name_only_candidate",
 44883|       "confidence_tier": "likely",
 44884|       "target_resolution": "exact",
 44885|       "evidence": [
 44886|         "member name 'StructuralMaterialType' matches keyword pattern /Material/ but return type 'StructuralMaterialType' gives no type-level confirmation"
 44887|       ],
 44888|       "source_url": "https://www.revitapidocs.com/2025/042b7922-53d9-d0ee-2f57-ce32cf5c5e4e.htm",
 44889|       "dll_signature_verified": true,
 44890|       "dll_relationship_scope": "declared",
 44891|       "dll_semantic_verified": null,
 44892|       "dll_verified_status": "signature_verified_declared",
 44893|       "revitlookup_referenced": null,
 44894|       "revitlookup_requires_document_context": null
 44895|     },
 44896|     {
 44897|       "source": "Autodesk.Revit.DB.FamilyInstance",
 44898|       "target": "Autodesk.Revit.DB.Element",
 44899|       "member_name": "SuperComponent",
 44900|       "member_kind": "property",
 44901|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 44902|       "confidence": "direct_return_type",
 44903|       "confidence_tier": "unverified_reference",
 44904|       "target_resolution": "exact",
 44905|       "evidence": [
 44906|         "return type 'Element' directly names a Revit DB object type"
 44907|       ],
 44908|       "source_url": "https://www.revitapidocs.com/2025/1dcf3123-c2ea-867a-7b9a-73173343121e.htm",
 44909|       "dll_signature_verified": true,
 44910|       "dll_relationship_scope": "declared",
 44911|       "dll_semantic_verified": null,
 44912|       "dll_verified_status": "signature_verified_declared",
 44913|       "revitlookup_referenced": null,
 44914|       "revitlookup_requires_document_context": null
 44915|     },
 44916|     {
 44917|       "source": "Autodesk.Revit.DB.FamilyInstance",
 44918|       "target": "Autodesk.Revit.DB.FamilySymbol",
 44919|       "member_name": "Symbol",
 44920|       "member_kind": "property",
 44921|       "edge_type": "INSTANCE_OF",
 44922|       "confidence": "direct_return_type",
 44923|       "confidence_tier": "core",
 44924|       "target_resolution": "exact",
 44925|       "evidence": [
 44926|         "return type 'FamilySymbol' directly names a Revit DB object type"
 44927|       ],
 44928|       "source_url": "https://www.revitapidocs.com/2025/4157fff5-cde3-cbb7-1df8-03f77d64712f.htm",
 44929|       "dll_signature_verified": true,
 44930|       "dll_relationship_scope": "declared",
 44931|       "dll_semantic_verified": null,
 44932|       "dll_verified_status": "signature_verified_declared",
 44933|       "revitlookup_referenced": null,
 44934|       "revitlookup_requires_document_context": null
 44935|     },
 44936|     {
 44937|       "source": "Autodesk.Revit.DB.FamilyInstance",
 44938|       "target": "Autodesk.Revit.DB.Architecture.Room",
 44939|       "member_name": "ToRoom",
 44940|       "member_kind": "property",
 44941|       "edge_type": "REFERENCES",
 44942|       "confidence": "direct_return_type",
 44943|       "confidence_tier": "core",
 44944|       "target_resolution": "exact",
 44945|       "evidence": [
 44946|         "return type 'Room' directly names a Revit DB object type"
 44947|       ],
 44948|       "source_url": "https://www.revitapidocs.com/2025/939e9c7b-072a-7be9-105f-64e1aa1f3a97.htm",
 44949|       "dll_signature_verified": true,
 44950|       "dll_relationship_scope": "declared",
 44951|       "dll_semantic_verified": null,
 44952|       "dll_verified_status": "signature_verified_declared",
 44953|       "revitlookup_referenced": true,
 44954|       "revitlookup_requires_document_context": true
 44955|     },
 44956|     {
 44957|       "source": "Autodesk.Revit.DB.FamilyInstance",
 44958|       "target": "Autodesk.Revit.DB.Architecture.Room",
 44959|       "member_name": "FlipFromToRoom",
 44960|       "member_kind": "method",
 44961|       "edge_type": "REFERENCES",
 44962|       "confidence": "name_only_candidate",
 44963|       "confidence_tier": "likely",
 44964|       "target_resolution": "exact",
 44965|       "evidence": [
 44966|         "member name 'FlipFromToRoom' matches keyword pattern /Room/ but return type 'void' gives no type-level confirmation"
 44967|       ],
 44968|       "source_url": "https://www.revitapidocs.com/2025/ae1158c1-1fb0-0558-0ea4-e1cf76bb8a1e.htm",
 44969|       "dll_signature_verified": true,
 44970|       "dll_relationship_scope": "declared",
 44971|       "dll_semantic_verified": null,
 44972|       "dll_verified_status": "signature_verified_declared",
 44973|       "revitlookup_referenced": null,
 44974|       "revitlookup_requires_document_context": null
 44975|     },
 44976|     {
 44977|       "source": "Autodesk.Revit.DB.FamilyInstance",
 44978|       "target": null,
 44979|       "member_name": "GetCopingIds",
 44980|       "member_kind": "method",
 44981|       "edge_type": "RETURNS_ELEMENT_IDS",
 44982|       "confidence": "unknown_reference",
 44983|       "confidence_tier": "unverified_reference",
 44984|       "target_resolution": "none",
 44985|       "evidence": [
 44986|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 44987|       ],
 44988|       "source_url": "https://www.revitapidocs.com/2025/6886b519-4a44-373f-59ab-4ceee51dd096.htm",
 44989|       "dll_signature_verified": true,
 44990|       "dll_relationship_scope": "declared",
 44991|       "dll_semantic_verified": null,
 44992|       "dll_verified_status": "signature_verified_declared",
 44993|       "revitlookup_referenced": null,
 44994|       "revitlookup_requires_document_context": null
 44995|     },
 44996|     {
 44997|       "source": "Autodesk.Revit.DB.FamilyInstance",
 44998|       "target": "Autodesk.Revit.DB.FamilyPointPlacementReference",
 44999|       "member_name": "GetFamilyPointPlacementReferences",
 45000|       "member_kind": "method",
 45001|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45002|       "confidence": "needs_runtime_validation",
 45003|       "confidence_tier": "needs_validation",
 45004|       "target_resolution": "exact",
 45005|       "evidence": [
 45006|         "return type 'IList < FamilyPointPlacementReference >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 45007|       ],
 45008|       "source_url": "https://www.revitapidocs.com/2025/59db15da-7e87-a85f-bacf-e8a636d17022.htm",
 45009|       "dll_signature_verified": true,
 45010|       "dll_relationship_scope": "declared",
 45011|       "dll_semantic_verified": null,
 45012|       "dll_verified_status": "signature_verified_declared",
 45013|       "revitlookup_referenced": null,
 45014|       "revitlookup_requires_document_context": null
 45015|     },
 45016|     {
 45017|       "source": "Autodesk.Revit.DB.FamilyInstance",
 45018|       "target": "Autodesk.Revit.DB.Reference",
 45019|       "member_name": "GetReferenceByName",
 45020|       "member_kind": "method",
 45021|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45022|       "confidence": "direct_return_type",
 45023|       "confidence_tier": "unverified_reference",
 45024|       "target_resolution": "exact",
 45025|       "evidence": [
 45026|         "return type 'Reference' directly names a Revit DB object type"
 45027|       ],
 45028|       "source_url": "https://www.revitapidocs.com/2025/d44a95cc-f2c7-1fa9-9180-fefed6d70ed6.htm",
 45029|       "dll_signature_verified": true,
 45030|       "dll_relationship_scope": "declared",
 45031|       "dll_semantic_verified": null,
 45032|       "dll_verified_status": "signature_verified_declared",
 45033|       "revitlookup_referenced": null,
 45034|       "revitlookup_requires_document_context": null
 45035|     },
 45036|     {
 45037|       "source": "Autodesk.Revit.DB.FamilyInstance",
 45038|       "target": "Autodesk.Revit.DB.Reference",
 45039|       "member_name": "GetReferences",
 45040|       "member_kind": "method",
 45041|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45042|       "confidence": "needs_runtime_validation",
 45043|       "confidence_tier": "needs_validation",
 45044|       "target_resolution": "exact",
 45045|       "evidence": [
 45046|         "return type 'IList < Reference >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 45047|       ],
 45048|       "source_url": "https://www.revitapidocs.com/2025/a8a7dc74-db8e-a7b6-a9c8-869397cca6b4.htm",
 45049|       "dll_signature_verified": true,
 45050|       "dll_relationship_scope": "declared",
 45051|       "dll_semantic_verified": null,
 45052|       "dll_verified_status": "signature_verified_declared",
 45053|       "revitlookup_referenced": true,
 45054|       "revitlookup_requires_document_context": true
 45055|     },
 45056|     {
 45057|       "source": "Autodesk.Revit.DB.FamilyInstance",
 45058|       "target": null,
 45059|       "member_name": "GetSubComponentIds",
 45060|       "member_kind": "method",
 45061|       "edge_type": "RETURNS_ELEMENT_IDS",
 45062|       "confidence": "unknown_reference",
 45063|       "confidence_tier": "unverified_reference",
 45064|       "target_resolution": "none",
 45065|       "evidence": [
 45066|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 45067|       ],
 45068|       "source_url": "https://www.revitapidocs.com/2025/be37702c-1dcd-bc14-aa35-45f06f20210a.htm",
 45069|       "dll_signature_verified": true,
 45070|       "dll_relationship_scope": "declared",
 45071|       "dll_semantic_verified": null,
 45072|       "dll_verified_status": "signature_verified_declared",
 45073|       "revitlookup_referenced": null,
 45074|       "revitlookup_requires_document_context": null
 45075|     },
 45076|     {
 45077|       "source": "Autodesk.Revit.DB.FamilyInstance",
 45078|       "target": "Autodesk.Revit.DB.SweptProfile",
 45079|       "member_name": "GetSweptProfile",
 45080|       "member_kind": "method",
 45081|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45082|       "confidence": "direct_return_type",
 45083|       "confidence_tier": "unverified_reference",
 45084|       "target_resolution": "exact",
 45085|       "evidence": [
 45086|         "return type 'SweptProfile' directly names a Revit DB object type"
 45087|       ],
 45088|       "source_url": "https://www.revitapidocs.com/2025/9bc9e2db-5ef1-8264-1426-01f4a6081844.htm",
 45089|       "dll_signature_verified": true,
 45090|       "dll_relationship_scope": "declared",
 45091|       "dll_semantic_verified": null,
 45092|       "dll_verified_status": "signature_verified_declared",
 45093|       "revitlookup_referenced": null,
 45094|       "revitlookup_requires_document_context": null
 45095|     },
 45096|     {
 45097|       "source": "Autodesk.Revit.DB.FamilyInstance",
 45098|       "target": null,
 45099|       "member_name": "Split",
 45100|       "member_kind": "method",
 45101|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 45102|       "confidence": "unknown_reference",
 45103|       "confidence_tier": "unverified_reference",
 45104|       "target_resolution": "none",
 45105|       "evidence": [
 45106|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 45107|       ],
 45108|       "source_url": "https://www.revitapidocs.com/2025/8f32a065-ba3c-79c7-8141-63183b4cdece.htm",
 45109|       "dll_signature_verified": true,
 45110|       "dll_relationship_scope": "declared",
 45111|       "dll_semantic_verified": null,
 45112|       "dll_verified_status": "signature_verified_declared",
 45113|       "revitlookup_referenced": null,
 45114|       "revitlookup_requires_document_context": null
 45115|     },
 45116|     {
 45117|       "source": "Autodesk.Revit.DB.FamilyInstanceFilter",
 45118|       "target": null,
 45119|       "member_name": "FamilySymbolId",
 45120|       "member_kind": "property",
 45121|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 45122|       "confidence": "unknown_reference",
 45123|       "confidence_tier": "unverified_reference",
 45124|       "target_resolution": "none",
 45125|       "evidence": [
 45126|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 45127|       ],
 45128|       "source_url": "https://www.revitapidocs.com/2025/ca671e94-c095-4577-12e6-efa5918cc5f6.htm",
 45129|       "dll_signature_verified": true,
 45130|       "dll_relationship_scope": "declared",
 45131|       "dll_semantic_verified": null,
 45132|       "dll_verified_status": "signature_verified_declared",
 45133|       "revitlookup_referenced": null,
 45134|       "revitlookup_requires_document_context": null
 45135|     },
 45136|     {
 45137|       "source": "Autodesk.Revit.DB.FamilyManager",
 45138|       "target": "Autodesk.Revit.DB.FamilyType",
 45139|       "member_name": "CurrentType",
 45140|       "member_kind": "property",
 45141|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45142|       "confidence": "direct_return_type",
 45143|       "confidence_tier": "unverified_reference",
 45144|       "target_resolution": "exact",
 45145|       "evidence": [
 45146|         "return type 'FamilyType' directly names a Revit DB object type"
 45147|       ],
 45148|       "source_url": "https://www.revitapidocs.com/2025/3d37cd81-48ba-4011-82bc-dbb7ae14b270.htm",
 45149|       "dll_signature_verified": true,
 45150|       "dll_relationship_scope": "declared",
 45151|       "dll_semantic_verified": null,
 45152|       "dll_verified_status": "signature_verified_declared",
 45153|       "revitlookup_referenced": null,
 45154|       "revitlookup_requires_document_context": null
 45155|     },
 45156|     {
 45157|       "source": "Autodesk.Revit.DB.FamilyManager",
 45158|       "target": "Autodesk.Revit.DB.FamilyParameterSet",
 45159|       "member_name": "Parameters",
 45160|       "member_kind": "property",
 45161|       "edge_type": "HAS_PARAMETER",
 45162|       "confidence": "direct_return_type",
 45163|       "confidence_tier": "core",
 45164|       "target_resolution": "exact",
 45165|       "evidence": [
 45166|         "return type 'FamilyParameterSet' directly names a Revit DB object type"
 45167|       ],
 45168|       "source_url": "https://www.revitapidocs.com/2025/bef4c199-44d9-63b9-80e7-1a6b20a1062a.htm",
 45169|       "dll_signature_verified": true,
 45170|       "dll_relationship_scope": "declared",
 45171|       "dll_semantic_verified": null,
 45172|       "dll_verified_status": "signature_verified_declared",
 45173|       "revitlookup_referenced": null,
 45174|       "revitlookup_requires_document_context": null
 45175|     },
 45176|     {
 45177|       "source": "Autodesk.Revit.DB.FamilyManager",
 45178|       "target": "Autodesk.Revit.DB.FamilyTypeSet",
 45179|       "member_name": "Types",
 45180|       "member_kind": "property",
 45181|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45182|       "confidence": "direct_return_type",
 45183|       "confidence_tier": "unverified_reference",
 45184|       "target_resolution": "exact",
 45185|       "evidence": [
 45186|         "return type 'FamilyTypeSet' directly names a Revit DB object type"
 45187|       ],
 45188|       "source_url": "https://www.revitapidocs.com/2025/048fbdd1-313f-209e-3046-25c8872bf04e.htm",
 45189|       "dll_signature_verified": true,
 45190|       "dll_relationship_scope": "declared",
 45191|       "dll_semantic_verified": null,
 45192|       "dll_verified_status": "signature_verified_declared",
 45193|       "revitlookup_referenced": null,
 45194|       "revitlookup_requires_document_context": null
 45195|     },
 45196|     {
 45197|       "source": "Autodesk.Revit.DB.FamilyManager",
 45198|       "target": "Autodesk.Revit.DB.FamilyParameter",
 45199|       "member_name": "AddParameter",
 45200|       "member_kind": "method",
 45201|       "edge_type": "HAS_PARAMETER",
 45202|       "confidence": "direct_return_type",
 45203|       "confidence_tier": "core",
 45204|       "target_resolution": "exact",
 45205|       "evidence": [
 45206|         "return type 'FamilyParameter' directly names a Revit DB object type"
 45207|       ],
 45208|       "source_url": "https://www.revitapidocs.com/2025/bff507b1-caa3-bf4c-f7f1-c56cade391f8.htm",
 45209|       "dll_signature_verified": true,
 45210|       "dll_relationship_scope": "declared",
 45211|       "dll_semantic_verified": null,
 45212|       "dll_verified_status": "signature_verified_declared",
 45213|       "revitlookup_referenced": null,
 45214|       "revitlookup_requires_document_context": null
 45215|     },
 45216|     {
 45217|       "source": "Autodesk.Revit.DB.FamilyManager",
 45218|       "target": "Autodesk.Revit.DB.FamilyParameter",
 45219|       "member_name": "AddParameter",
 45220|       "member_kind": "method",
 45221|       "edge_type": "HAS_PARAMETER",
 45222|       "confidence": "direct_return_type",
 45223|       "confidence_tier": "core",
 45224|       "target_resolution": "exact",
 45225|       "evidence": [
 45226|         "return type 'FamilyParameter' directly names a Revit DB object type"
 45227|       ],
 45228|       "source_url": "https://www.revitapidocs.com/2025/8425ca9a-9db2-d06a-7540-bc8e686a7566.htm",
 45229|       "dll_signature_verified": true,
 45230|       "dll_relationship_scope": "declared",
 45231|       "dll_semantic_verified": null,
 45232|       "dll_verified_status": "signature_verified_declared",
 45233|       "revitlookup_referenced": null,
 45234|       "revitlookup_requires_document_context": null
 45235|     },
 45236|     {
 45237|       "source": "Autodesk.Revit.DB.FamilyManager",
 45238|       "target": "Autodesk.Revit.DB.FamilyParameter",
 45239|       "member_name": "AddParameter",
 45240|       "member_kind": "method",
 45241|       "edge_type": "HAS_PARAMETER",
 45242|       "confidence": "direct_return_type",
 45243|       "confidence_tier": "core",
 45244|       "target_resolution": "exact",
 45245|       "evidence": [
 45246|         "return type 'FamilyParameter' directly names a Revit DB object type"
 45247|       ],
 45248|       "source_url": "https://www.revitapidocs.com/2025/3ac89d60-4b71-694f-002f-125d2e6565fc.htm",
 45249|       "dll_signature_verified": true,
 45250|       "dll_relationship_scope": "declared",
```
