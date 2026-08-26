# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 152 of 216
- Original line range: 58891-59290
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 58891|       "target": null,
 58892|       "member_name": "SolidTag",
 58893|       "member_kind": "property",
 58894|       "edge_type": "TAGS_ELEMENT",
 58895|       "confidence": "name_only_candidate",
 58896|       "confidence_tier": "likely",
 58897|       "target_resolution": "none",
 58898|       "evidence": [
 58899|         "member name 'SolidTag' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'int' gives no type-level confirmation"
 58900|       ],
 58901|       "source_url": "https://www.revitapidocs.com/2025/7957dff1-2d44-352f-47b1-d68d1f3bf15d.htm",
 58902|       "dll_signature_verified": true,
 58903|       "dll_relationship_scope": "declared",
 58904|       "dll_semantic_verified": null,
 58905|       "dll_verified_status": "signature_verified_declared",
 58906|       "revitlookup_referenced": null,
 58907|       "revitlookup_requires_document_context": null
 58908|     },
 58909|     {
 58910|       "source": "Autodesk.Revit.DB.SolidOptions",
 58911|       "target": "Autodesk.Revit.DB.GraphicsStyle",
 58912|       "member_name": "GraphicsStyleId",
 58913|       "member_kind": "property",
 58914|       "edge_type": "REFERENCES",
 58915|       "confidence": "elementid_with_strong_name",
 58916|       "confidence_tier": "core",
 58917|       "target_resolution": "exact",
 58918|       "evidence": [
 58919|         "member name 'GraphicsStyleId' matches keyword pattern /GraphicsStyle/"
 58920|       ],
 58921|       "source_url": "https://www.revitapidocs.com/2025/0943fdf4-e5c6-b0f8-fb00-54e982ff560f.htm",
 58922|       "dll_signature_verified": true,
 58923|       "dll_relationship_scope": "declared",
 58924|       "dll_semantic_verified": null,
 58925|       "dll_verified_status": "signature_verified_declared",
 58926|       "revitlookup_referenced": null,
 58927|       "revitlookup_requires_document_context": null
 58928|     },
 58929|     {
 58930|       "source": "Autodesk.Revit.DB.SolidOptions",
 58931|       "target": "Autodesk.Revit.DB.Material",
 58932|       "member_name": "MaterialId",
 58933|       "member_kind": "property",
 58934|       "edge_type": "USES_MATERIAL",
 58935|       "confidence": "elementid_with_strong_name",
 58936|       "confidence_tier": "core",
 58937|       "target_resolution": "exact",
 58938|       "evidence": [
 58939|         "member name 'MaterialId' matches keyword pattern /Material/"
 58940|       ],
 58941|       "source_url": "https://www.revitapidocs.com/2025/c82e435c-071d-fd93-4b09-e629823dcbdc.htm",
 58942|       "dll_signature_verified": true,
 58943|       "dll_relationship_scope": "declared",
 58944|       "dll_semantic_verified": null,
 58945|       "dll_verified_status": "signature_verified_declared",
 58946|       "revitlookup_referenced": null,
 58947|       "revitlookup_requires_document_context": null
 58948|     },
 58949|     {
 58950|       "source": "Autodesk.Revit.DB.SolidOrShellTessellationControls",
 58951|       "target": "Autodesk.Revit.DB.Level",
 58952|       "member_name": "LevelOfDetail",
 58953|       "member_kind": "property",
 58954|       "edge_type": "ASSIGNED_TO_LEVEL",
 58955|       "confidence": "name_only_candidate",
 58956|       "confidence_tier": "likely",
 58957|       "target_resolution": "exact",
 58958|       "evidence": [
 58959|         "member name 'LevelOfDetail' matches keyword pattern /Level/ but return type 'double' gives no type-level confirmation"
 58960|       ],
 58961|       "source_url": "https://www.revitapidocs.com/2025/c7975423-7bec-c45d-f0a1-e4edb8d82657.htm",
 58962|       "dll_signature_verified": true,
 58963|       "dll_relationship_scope": "declared",
 58964|       "dll_semantic_verified": null,
 58965|       "dll_verified_status": "signature_verified_declared",
 58966|       "revitlookup_referenced": null,
 58967|       "revitlookup_requires_document_context": null
 58968|     },
 58969|     {
 58970|       "source": "Autodesk.Revit.DB.SolidOrShellTessellationControls",
 58971|       "target": "Autodesk.Revit.DB.Level",
 58972|       "member_name": "DisableLevelOfDetail",
 58973|       "member_kind": "method",
 58974|       "edge_type": "ASSIGNED_TO_LEVEL",
 58975|       "confidence": "name_only_candidate",
 58976|       "confidence_tier": "likely",
 58977|       "target_resolution": "exact",
 58978|       "evidence": [
 58979|         "member name 'DisableLevelOfDetail' matches keyword pattern /Level/ but return type 'void' gives no type-level confirmation"
 58980|       ],
 58981|       "source_url": "https://www.revitapidocs.com/2025/cfe32d33-bd03-812f-9ec3-a1833277c399.htm",
 58982|       "dll_signature_verified": true,
 58983|       "dll_relationship_scope": "declared",
 58984|       "dll_semantic_verified": null,
 58985|       "dll_verified_status": "signature_verified_declared",
 58986|       "revitlookup_referenced": null,
 58987|       "revitlookup_requires_document_context": null
 58988|     },
 58989|     {
 58990|       "source": "Autodesk.Revit.DB.SolidOrShellTessellationControls",
 58991|       "target": "Autodesk.Revit.DB.Level",
 58992|       "member_name": "UseLevelOfDetail",
 58993|       "member_kind": "method",
 58994|       "edge_type": "ASSIGNED_TO_LEVEL",
 58995|       "confidence": "name_only_candidate",
 58996|       "confidence_tier": "likely",
 58997|       "target_resolution": "exact",
 58998|       "evidence": [
 58999|         "member name 'UseLevelOfDetail' matches keyword pattern /Level/ but return type 'bool' gives no type-level confirmation"
 59000|       ],
 59001|       "source_url": "https://www.revitapidocs.com/2025/9458bc5a-30cc-b52a-a04a-159ad9066c9e.htm",
 59002|       "dll_signature_verified": true,
 59003|       "dll_relationship_scope": "declared",
 59004|       "dll_semantic_verified": null,
 59005|       "dll_verified_status": "signature_verified_declared",
 59006|       "revitlookup_referenced": null,
 59007|       "revitlookup_requires_document_context": null
 59008|     },
 59009|     {
 59010|       "source": "Autodesk.Revit.DB.SolidSolidCutUtils",
 59011|       "target": null,
 59012|       "member_name": "GetCuttingSolids",
 59013|       "member_kind": "method",
 59014|       "edge_type": "RETURNS_ELEMENT_IDS",
 59015|       "confidence": "unknown_reference",
 59016|       "confidence_tier": "unverified_reference",
 59017|       "target_resolution": "none",
 59018|       "evidence": [
 59019|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 59020|       ],
 59021|       "source_url": "https://www.revitapidocs.com/2025/ca21c6c1-7689-1a1e-f370-219eba29b8ff.htm",
 59022|       "dll_signature_verified": true,
 59023|       "dll_relationship_scope": "declared",
 59024|       "dll_semantic_verified": null,
 59025|       "dll_verified_status": "signature_verified_declared",
 59026|       "revitlookup_referenced": null,
 59027|       "revitlookup_requires_document_context": null
 59028|     },
 59029|     {
 59030|       "source": "Autodesk.Revit.DB.SolidSolidCutUtils",
 59031|       "target": null,
 59032|       "member_name": "GetSolidsBeingCut",
 59033|       "member_kind": "method",
 59034|       "edge_type": "RETURNS_ELEMENT_IDS",
 59035|       "confidence": "unknown_reference",
 59036|       "confidence_tier": "unverified_reference",
 59037|       "target_resolution": "none",
 59038|       "evidence": [
 59039|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 59040|       ],
 59041|       "source_url": "https://www.revitapidocs.com/2025/4b8c8902-2a32-b99c-aa0e-e3abd79d9073.htm",
 59042|       "dll_signature_verified": true,
 59043|       "dll_relationship_scope": "declared",
 59044|       "dll_semantic_verified": null,
 59045|       "dll_verified_status": "signature_verified_declared",
 59046|       "revitlookup_referenced": null,
 59047|       "revitlookup_requires_document_context": null
 59048|     },
 59049|     {
 59050|       "source": "Autodesk.Revit.DB.SolidUtils",
 59051|       "target": "Autodesk.Revit.DB.EdgeEndPoint",
 59052|       "member_name": "FindAllEdgeEndPointsAtVertex",
 59053|       "member_kind": "method",
 59054|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 59055|       "confidence": "needs_runtime_validation",
 59056|       "confidence_tier": "needs_validation",
 59057|       "target_resolution": "exact",
 59058|       "evidence": [
 59059|         "return type 'IList < EdgeEndPoint >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 59060|       ],
 59061|       "source_url": "https://www.revitapidocs.com/2025/4a7c822c-3be0-52b6-cdca-3cd6496759c5.htm",
 59062|       "dll_signature_verified": true,
 59063|       "dll_relationship_scope": "declared",
 59064|       "dll_semantic_verified": null,
 59065|       "dll_verified_status": "signature_verified_declared",
 59066|       "revitlookup_referenced": null,
 59067|       "revitlookup_requires_document_context": null
 59068|     },
 59069|     {
 59070|       "source": "Autodesk.Revit.DB.SolidUtils",
 59071|       "target": "Autodesk.Revit.DB.TriangulatedSolidOrShell",
 59072|       "member_name": "TessellateSolidOrShell",
 59073|       "member_kind": "method",
 59074|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 59075|       "confidence": "direct_return_type",
 59076|       "confidence_tier": "unverified_reference",
 59077|       "target_resolution": "exact",
 59078|       "evidence": [
 59079|         "return type 'TriangulatedSolidOrShell' directly names a Revit DB object type"
 59080|       ],
 59081|       "source_url": "https://www.revitapidocs.com/2025/d856e5f0-2e26-f01a-2996-9fbc0ad1c03e.htm",
 59082|       "dll_signature_verified": true,
 59083|       "dll_relationship_scope": "declared",
 59084|       "dll_semantic_verified": null,
 59085|       "dll_verified_status": "signature_verified_declared",
 59086|       "revitlookup_referenced": null,
 59087|       "revitlookup_requires_document_context": null
 59088|     },
 59089|     {
 59090|       "source": "Autodesk.Revit.DB.SpatialElement",
 59091|       "target": "Autodesk.Revit.DB.Level",
 59092|       "member_name": "Level",
 59093|       "member_kind": "property",
 59094|       "edge_type": "ASSIGNED_TO_LEVEL",
 59095|       "confidence": "direct_return_type",
 59096|       "confidence_tier": "core",
 59097|       "target_resolution": "exact",
 59098|       "evidence": [
 59099|         "return type 'Level' directly names a Revit DB object type"
 59100|       ],
 59101|       "source_url": "https://www.revitapidocs.com/2025/af1207d3-5875-3866-a259-159f8d3af4b6.htm",
 59102|       "dll_signature_verified": true,
 59103|       "dll_relationship_scope": "declared",
 59104|       "dll_semantic_verified": null,
 59105|       "dll_verified_status": "signature_verified_declared",
 59106|       "revitlookup_referenced": null,
 59107|       "revitlookup_requires_document_context": null
 59108|     },
 59109|     {
 59110|       "source": "Autodesk.Revit.DB.SpatialElement",
 59111|       "target": "Autodesk.Revit.DB.Location",
 59112|       "member_name": "Location",
 59113|       "member_kind": "property",
 59114|       "edge_type": "REFERENCES",
 59115|       "confidence": "direct_return_type",
 59116|       "confidence_tier": "core",
 59117|       "target_resolution": "exact",
 59118|       "evidence": [
 59119|         "return type 'Location' directly names a Revit DB object type"
 59120|       ],
 59121|       "source_url": "https://www.revitapidocs.com/2025/63092169-7a50-9b92-d886-f741adc211ec.htm",
 59122|       "dll_signature_verified": true,
 59123|       "dll_relationship_scope": "declared",
 59124|       "dll_semantic_verified": null,
 59125|       "dll_verified_status": "signature_verified_declared",
 59126|       "revitlookup_referenced": null,
 59127|       "revitlookup_requires_document_context": null
 59128|     },
 59129|     {
 59130|       "source": "Autodesk.Revit.DB.SpatialElement",
 59131|       "target": "Autodesk.Revit.DB.SpatialElementDomainData",
 59132|       "member_name": "GetSpatialElementDomainData",
 59133|       "member_kind": "method",
 59134|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 59135|       "confidence": "direct_return_type",
 59136|       "confidence_tier": "unverified_reference",
 59137|       "target_resolution": "exact",
 59138|       "evidence": [
 59139|         "return type 'SpatialElementDomainData' directly names a Revit DB object type"
 59140|       ],
 59141|       "source_url": "https://www.revitapidocs.com/2025/b116ed4e-6d61-3c00-74af-3689e47fe6d0.htm",
 59142|       "dll_signature_verified": true,
 59143|       "dll_relationship_scope": "declared",
 59144|       "dll_semantic_verified": null,
 59145|       "dll_verified_status": "signature_verified_declared",
 59146|       "revitlookup_referenced": null,
 59147|       "revitlookup_requires_document_context": null
 59148|     },
 59149|     {
 59150|       "source": "Autodesk.Revit.DB.SpatialElementBoundarySubface",
 59151|       "target": null,
 59152|       "member_name": "SpatialBoundaryElement",
 59153|       "member_kind": "property",
 59154|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 59155|       "confidence": "unknown_reference",
 59156|       "confidence_tier": "unverified_reference",
 59157|       "target_resolution": "none",
 59158|       "evidence": [
 59159|         "return type is 'LinkElementId', an ID wrapper, but member name gives no strong hint of the target type"
 59160|       ],
 59161|       "source_url": "https://www.revitapidocs.com/2025/332b209f-bf7d-7ebd-fc63-0463aaf89550.htm",
 59162|       "dll_signature_verified": true,
 59163|       "dll_relationship_scope": "declared",
 59164|       "dll_semantic_verified": null,
 59165|       "dll_verified_status": "signature_verified_declared",
 59166|       "revitlookup_referenced": null,
 59167|       "revitlookup_requires_document_context": null
 59168|     },
 59169|     {
 59170|       "source": "Autodesk.Revit.DB.SpatialElementGeometryCalculator",
 59171|       "target": "Autodesk.Revit.DB.SpatialElementGeometryResults",
 59172|       "member_name": "CalculateSpatialElementGeometry",
 59173|       "member_kind": "method",
 59174|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 59175|       "confidence": "direct_return_type",
 59176|       "confidence_tier": "unverified_reference",
 59177|       "target_resolution": "exact",
 59178|       "evidence": [
 59179|         "return type 'SpatialElementGeometryResults' directly names a Revit DB object type"
 59180|       ],
 59181|       "source_url": "https://www.revitapidocs.com/2025/6dbe1057-76ed-c8e4-1d05-95cf65aa18e7.htm",
 59182|       "dll_signature_verified": true,
 59183|       "dll_relationship_scope": "declared",
 59184|       "dll_semantic_verified": null,
 59185|       "dll_verified_status": "signature_verified_declared",
 59186|       "revitlookup_referenced": null,
 59187|       "revitlookup_requires_document_context": null
 59188|     },
 59189|     {
 59190|       "source": "Autodesk.Revit.DB.SpatialElementGeometryCalculator",
 59191|       "target": "Autodesk.Revit.DB.SpatialElementBoundaryOptions",
 59192|       "member_name": "GetOptions",
 59193|       "member_kind": "method",
 59194|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 59195|       "confidence": "direct_return_type",
 59196|       "confidence_tier": "unverified_reference",
 59197|       "target_resolution": "exact",
 59198|       "evidence": [
 59199|         "return type 'SpatialElementBoundaryOptions' directly names a Revit DB object type"
 59200|       ],
 59201|       "source_url": "https://www.revitapidocs.com/2025/32e23dc2-66e7-2b76-3da2-e997279855c0.htm",
 59202|       "dll_signature_verified": true,
 59203|       "dll_relationship_scope": "declared",
 59204|       "dll_semantic_verified": null,
 59205|       "dll_verified_status": "signature_verified_declared",
 59206|       "revitlookup_referenced": null,
 59207|       "revitlookup_requires_document_context": null
 59208|     },
 59209|     {
 59210|       "source": "Autodesk.Revit.DB.SpatialElementGeometryCalculator",
 59211|       "target": "Autodesk.Revit.DB.Architecture.Room",
 59212|       "member_name": "IsRoomOrSpace",
 59213|       "member_kind": "method",
 59214|       "edge_type": "REFERENCES",
 59215|       "confidence": "name_only_candidate",
 59216|       "confidence_tier": "likely",
 59217|       "target_resolution": "exact",
 59218|       "evidence": [
 59219|         "member name 'IsRoomOrSpace' matches keyword pattern /Room/ but return type 'bool' gives no type-level confirmation"
 59220|       ],
 59221|       "source_url": "https://www.revitapidocs.com/2025/0393222f-7881-594f-095e-57618ea04048.htm",
 59222|       "dll_signature_verified": true,
 59223|       "dll_relationship_scope": "declared",
 59224|       "dll_semantic_verified": null,
 59225|       "dll_verified_status": "signature_verified_declared",
 59226|       "revitlookup_referenced": null,
 59227|       "revitlookup_requires_document_context": null
 59228|     },
 59229|     {
 59230|       "source": "Autodesk.Revit.DB.SpatialElementGeometryResults",
 59231|       "target": "Autodesk.Revit.DB.SpatialElementBoundarySubface",
 59232|       "member_name": "GetBoundaryFaceInfo",
 59233|       "member_kind": "method",
 59234|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 59235|       "confidence": "needs_runtime_validation",
 59236|       "confidence_tier": "needs_validation",
 59237|       "target_resolution": "exact",
 59238|       "evidence": [
 59239|         "return type 'IList < SpatialElementBoundarySubface >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 59240|       ],
 59241|       "source_url": "https://www.revitapidocs.com/2025/85c61cf8-daa1-8aae-76c3-de8f100e9102.htm",
 59242|       "dll_signature_verified": true,
 59243|       "dll_relationship_scope": "declared",
 59244|       "dll_semantic_verified": null,
 59245|       "dll_verified_status": "signature_verified_declared",
 59246|       "revitlookup_referenced": null,
 59247|       "revitlookup_requires_document_context": null
 59248|     },
 59249|     {
 59250|       "source": "Autodesk.Revit.DB.SpatialElementTag",
 59251|       "target": null,
 59252|       "member_name": "IsTaggingLink",
 59253|       "member_kind": "property",
 59254|       "edge_type": "TAGS_ELEMENT",
 59255|       "confidence": "name_only_candidate",
 59256|       "confidence_tier": "likely",
 59257|       "target_resolution": "none",
 59258|       "evidence": [
 59259|         "member name 'IsTaggingLink' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'bool' gives no type-level confirmation"
 59260|       ],
 59261|       "source_url": "https://www.revitapidocs.com/2025/75f8aca2-6dbe-c124-02fa-001620407989.htm",
 59262|       "dll_signature_verified": true,
 59263|       "dll_relationship_scope": "declared",
 59264|       "dll_semantic_verified": null,
 59265|       "dll_verified_status": "signature_verified_declared",
 59266|       "revitlookup_referenced": null,
 59267|       "revitlookup_requires_document_context": null
 59268|     },
 59269|     {
 59270|       "source": "Autodesk.Revit.DB.SpatialElementTag",
 59271|       "target": "Autodesk.Revit.DB.Location",
 59272|       "member_name": "Location",
 59273|       "member_kind": "property",
 59274|       "edge_type": "REFERENCES",
 59275|       "confidence": "direct_return_type",
 59276|       "confidence_tier": "core",
 59277|       "target_resolution": "exact",
 59278|       "evidence": [
 59279|         "return type 'Location' directly names a Revit DB object type"
 59280|       ],
 59281|       "source_url": "https://www.revitapidocs.com/2025/ac42ff31-d480-8b0e-4735-b5eb6ee1d53e.htm",
 59282|       "dll_signature_verified": true,
 59283|       "dll_relationship_scope": "declared",
 59284|       "dll_semantic_verified": null,
 59285|       "dll_verified_status": "signature_verified_declared",
 59286|       "revitlookup_referenced": null,
 59287|       "revitlookup_requires_document_context": null
 59288|     },
 59289|     {
 59290|       "source": "Autodesk.Revit.DB.SpatialElementTag",
```
