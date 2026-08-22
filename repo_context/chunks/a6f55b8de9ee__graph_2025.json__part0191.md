# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 191 of 216
- Original line range: 74101-74500
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 74101|       "dll_semantic_verified": null,
 74102|       "dll_verified_status": "signature_verified_declared",
 74103|       "revitlookup_referenced": null,
 74104|       "revitlookup_requires_document_context": null
 74105|     },
 74106|     {
 74107|       "source": "Autodesk.Revit.DB.Fabrication.FabricationNetworkChangeService",
 74108|       "target": "Autodesk.Revit.DB.Fabrication.FabricationPartSizeMap",
 74109|       "member_name": "GetMapOfAllSizesForStraights",
 74110|       "member_kind": "method",
 74111|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74112|       "confidence": "needs_runtime_validation",
 74113|       "confidence_tier": "needs_validation",
 74114|       "target_resolution": "short_name_fallback",
 74115|       "evidence": [
 74116|         "return type 'ISet < FabricationPartSizeMap >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 74117|       ],
 74118|       "source_url": "https://www.revitapidocs.com/2025/34ceb348-135f-4349-b04d-814763d3bff7.htm",
 74119|       "dll_signature_verified": true,
 74120|       "dll_relationship_scope": "declared",
 74121|       "dll_semantic_verified": null,
 74122|       "dll_verified_status": "signature_verified_declared",
 74123|       "revitlookup_referenced": null,
 74124|       "revitlookup_requires_document_context": null
 74125|     },
 74126|     {
 74127|       "source": "Autodesk.Revit.DB.Fabrication.FabricationNetworkChangeService",
 74128|       "target": null,
 74129|       "member_name": "GetStraightsThatWereNotChanged",
 74130|       "member_kind": "method",
 74131|       "edge_type": "RETURNS_ELEMENT_IDS",
 74132|       "confidence": "unknown_reference",
 74133|       "confidence_tier": "unverified_reference",
 74134|       "target_resolution": "none",
 74135|       "evidence": [
 74136|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 74137|       ],
 74138|       "source_url": "https://www.revitapidocs.com/2025/644c47d9-806b-cd68-bf3e-0f8997c89f50.htm",
 74139|       "dll_signature_verified": true,
 74140|       "dll_relationship_scope": "declared",
 74141|       "dll_semantic_verified": null,
 74142|       "dll_verified_status": "signature_verified_declared",
 74143|       "revitlookup_referenced": null,
 74144|       "revitlookup_requires_document_context": null
 74145|     },
 74146|     {
 74147|       "source": "Autodesk.Revit.DB.IFC.ExporterIFC",
 74148|       "target": "Autodesk.Revit.DB.Level",
 74149|       "member_name": "SpaceBoundaryLevel",
 74150|       "member_kind": "property",
 74151|       "edge_type": "ASSIGNED_TO_LEVEL",
 74152|       "confidence": "name_only_candidate",
 74153|       "confidence_tier": "likely",
 74154|       "target_resolution": "exact",
 74155|       "evidence": [
 74156|         "member name 'SpaceBoundaryLevel' matches keyword pattern /Level/ but return type 'int' gives no type-level confirmation"
 74157|       ],
 74158|       "source_url": "https://www.revitapidocs.com/2025/0beb0795-6270-5141-16df-e51e95acfa73.htm",
 74159|       "dll_signature_verified": true,
 74160|       "dll_relationship_scope": "declared",
 74161|       "dll_semantic_verified": null,
 74162|       "dll_verified_status": "signature_verified_declared",
 74163|       "revitlookup_referenced": null,
 74164|       "revitlookup_requires_document_context": null
 74165|     },
 74166|     {
 74167|       "source": "Autodesk.Revit.DB.IFC.ExporterIFC",
 74168|       "target": "Autodesk.Revit.DB.IFC.IFCFile",
 74169|       "member_name": "GetFile",
 74170|       "member_kind": "method",
 74171|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74172|       "confidence": "direct_return_type",
 74173|       "confidence_tier": "unverified_reference",
 74174|       "target_resolution": "short_name_fallback",
 74175|       "evidence": [
 74176|         "return type 'IFCFile' directly names a Revit DB object type"
 74177|       ],
 74178|       "source_url": "https://www.revitapidocs.com/2025/1baac5bf-ee32-4d1c-0ba3-6193124c0d9c.htm",
 74179|       "dll_signature_verified": true,
 74180|       "dll_relationship_scope": "declared",
 74181|       "dll_semantic_verified": null,
 74182|       "dll_verified_status": "signature_verified_declared",
 74183|       "revitlookup_referenced": null,
 74184|       "revitlookup_requires_document_context": null
 74185|     },
 74186|     {
 74187|       "source": "Autodesk.Revit.DB.IFC.ExporterIFC",
 74188|       "target": null,
 74189|       "member_name": "GetHostObjects",
 74190|       "member_kind": "method",
 74191|       "edge_type": "HOSTED_BY",
 74192|       "confidence": "name_only_candidate",
 74193|       "confidence_tier": "likely",
 74194|       "target_resolution": "none",
 74195|       "evidence": [
 74196|         "member name 'GetHostObjects' matches keyword pattern /^GetHosted|Host/ but return type 'IList < IDictionary < ElementId , IFCAnyHandle >>' gives no type-level confirmation"
 74197|       ],
 74198|       "source_url": "https://www.revitapidocs.com/2025/39ace44e-26a7-e530-2dc2-737a1e3f1479.htm",
 74199|       "dll_signature_verified": true,
 74200|       "dll_relationship_scope": "declared",
 74201|       "dll_semantic_verified": null,
 74202|       "dll_verified_status": "signature_verified_declared",
 74203|       "revitlookup_referenced": null,
 74204|       "revitlookup_requires_document_context": null
 74205|     },
 74206|     {
 74207|       "source": "Autodesk.Revit.DB.IFC.ExporterIFC",
 74208|       "target": "Autodesk.Revit.DB.IFC.IFCLevelInfo",
 74209|       "member_name": "GetLevelInfo",
 74210|       "member_kind": "method",
 74211|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74212|       "confidence": "direct_return_type",
 74213|       "confidence_tier": "unverified_reference",
 74214|       "target_resolution": "short_name_fallback",
 74215|       "evidence": [
 74216|         "member name 'GetLevelInfo' matches keyword pattern /Level/ implying target 'Level', but the actual return type 'IFCLevelInfo' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 74217|         "return type 'IFCLevelInfo' directly names a Revit DB object type"
 74218|       ],
 74219|       "source_url": "https://www.revitapidocs.com/2025/c404ab36-866c-8ac8-a8b1-c60d963791ed.htm",
 74220|       "dll_signature_verified": true,
 74221|       "dll_relationship_scope": "declared",
 74222|       "dll_semantic_verified": null,
 74223|       "dll_verified_status": "signature_verified_declared",
 74224|       "revitlookup_referenced": null,
 74225|       "revitlookup_requires_document_context": null
 74226|     },
 74227|     {
 74228|       "source": "Autodesk.Revit.DB.IFC.ExporterIFC",
 74229|       "target": "Autodesk.Revit.DB.Level",
 74230|       "member_name": "GetLevelInfos",
 74231|       "member_kind": "method",
 74232|       "edge_type": "ASSIGNED_TO_LEVEL",
 74233|       "confidence": "name_only_candidate",
 74234|       "confidence_tier": "likely",
 74235|       "target_resolution": "exact",
 74236|       "evidence": [
 74237|         "member name 'GetLevelInfos' matches keyword pattern /Level/ but return type 'IDictionary < ElementId , IFCLevelInfo >' gives no type-level confirmation"
 74238|       ],
 74239|       "source_url": "https://www.revitapidocs.com/2025/c7f1c52a-a0d0-cc15-4a08-1c476bb7509b.htm",
 74240|       "dll_signature_verified": true,
 74241|       "dll_relationship_scope": "declared",
 74242|       "dll_semantic_verified": null,
 74243|       "dll_verified_status": "signature_verified_declared",
 74244|       "revitlookup_referenced": null,
 74245|       "revitlookup_requires_document_context": null
 74246|     },
 74247|     {
 74248|       "source": "Autodesk.Revit.DB.IFC.ExporterIFC",
 74249|       "target": "Autodesk.Revit.DB.Material",
 74250|       "member_name": "GetMaterialIdForCurrentExportState",
 74251|       "member_kind": "method",
 74252|       "edge_type": "USES_MATERIAL",
 74253|       "confidence": "elementid_with_strong_name",
 74254|       "confidence_tier": "core",
 74255|       "target_resolution": "exact",
 74256|       "evidence": [
 74257|         "member name 'GetMaterialIdForCurrentExportState' matches keyword pattern /Material/"
 74258|       ],
 74259|       "source_url": "https://www.revitapidocs.com/2025/ea78908e-959b-dca9-06a2-abce0c4cef70.htm",
 74260|       "dll_signature_verified": true,
 74261|       "dll_relationship_scope": "declared",
 74262|       "dll_semantic_verified": null,
 74263|       "dll_verified_status": "signature_verified_declared",
 74264|       "revitlookup_referenced": null,
 74265|       "revitlookup_requires_document_context": null
 74266|     },
 74267|     {
 74268|       "source": "Autodesk.Revit.DB.IFC.ExporterIFC",
 74269|       "target": "Autodesk.Revit.DB.Material",
 74270|       "member_name": "SetMaterialIdForCurrentExportState",
 74271|       "member_kind": "method",
 74272|       "edge_type": "USES_MATERIAL",
 74273|       "confidence": "name_only_candidate",
 74274|       "confidence_tier": "likely",
 74275|       "target_resolution": "exact",
 74276|       "evidence": [
 74277|         "member name 'SetMaterialIdForCurrentExportState' matches keyword pattern /Material/ but return type 'void' gives no type-level confirmation"
 74278|       ],
 74279|       "source_url": "https://www.revitapidocs.com/2025/af494e73-5135-bd2b-8d71-389fa5be3ec7.htm",
 74280|       "dll_signature_verified": true,
 74281|       "dll_relationship_scope": "declared",
 74282|       "dll_semantic_verified": null,
 74283|       "dll_verified_status": "signature_verified_declared",
 74284|       "revitlookup_referenced": null,
 74285|       "revitlookup_requires_document_context": null
 74286|     },
 74287|     {
 74288|       "source": "Autodesk.Revit.DB.IFC.ExporterIFCUtils",
 74289|       "target": "Autodesk.Revit.DB.IFC.HostObjectSubcomponentInfo",
 74290|       "member_name": "ComputeSubcomponents",
 74291|       "member_kind": "method",
 74292|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74293|       "confidence": "needs_runtime_validation",
 74294|       "confidence_tier": "needs_validation",
 74295|       "target_resolution": "short_name_fallback",
 74296|       "evidence": [
 74297|         "return type 'IList < HostObjectSubcomponentInfo >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 74298|       ],
 74299|       "source_url": "https://www.revitapidocs.com/2025/47104d1f-d0d6-4903-5d16-e5f807e3acd0.htm",
 74300|       "dll_signature_verified": true,
 74301|       "dll_relationship_scope": "declared",
 74302|       "dll_semantic_verified": null,
 74303|       "dll_verified_status": "signature_verified_declared",
 74304|       "revitlookup_referenced": null,
 74305|       "revitlookup_requires_document_context": null
 74306|     },
 74307|     {
 74308|       "source": "Autodesk.Revit.DB.IFC.ExporterIFCUtils",
 74309|       "target": "Autodesk.Revit.DB.FamilyInstance",
 74310|       "member_name": "GetAttachedColumns",
 74311|       "member_kind": "method",
 74312|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74313|       "confidence": "needs_runtime_validation",
 74314|       "confidence_tier": "needs_validation",
 74315|       "target_resolution": "exact",
 74316|       "evidence": [
 74317|         "return type 'IList < FamilyInstance >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 74318|       ],
 74319|       "source_url": "https://www.revitapidocs.com/2025/d377b274-2327-08f8-8dad-859ff541903a.htm",
 74320|       "dll_signature_verified": true,
 74321|       "dll_relationship_scope": "declared",
 74322|       "dll_semantic_verified": null,
 74323|       "dll_verified_status": "signature_verified_declared",
 74324|       "revitlookup_referenced": null,
 74325|       "revitlookup_requires_document_context": null
 74326|     },
 74327|     {
 74328|       "source": "Autodesk.Revit.DB.IFC.ExporterIFCUtils",
 74329|       "target": "Autodesk.Revit.DB.IFC.IFCConnectedWallData",
 74330|       "member_name": "GetConnectedWalls",
 74331|       "member_kind": "method",
 74332|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74333|       "confidence": "needs_runtime_validation",
 74334|       "confidence_tier": "needs_validation",
 74335|       "target_resolution": "short_name_fallback",
 74336|       "evidence": [
 74337|         "return type 'IList < IFCConnectedWallData >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 74338|       ],
 74339|       "source_url": "https://www.revitapidocs.com/2025/d2199e0e-f7f0-0c4b-cf62-d51773f95d02.htm",
 74340|       "dll_signature_verified": true,
 74341|       "dll_relationship_scope": "declared",
 74342|       "dll_semantic_verified": null,
 74343|       "dll_verified_status": "signature_verified_declared",
 74344|       "revitlookup_referenced": null,
 74345|       "revitlookup_requires_document_context": null
 74346|     },
 74347|     {
 74348|       "source": "Autodesk.Revit.DB.IFC.ExporterIFCUtils",
 74349|       "target": "Autodesk.Revit.DB.Arc",
 74350|       "member_name": "GetDoor2DArcsFromFamily",
 74351|       "member_kind": "method",
 74352|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74353|       "confidence": "needs_runtime_validation",
 74354|       "confidence_tier": "needs_validation",
 74355|       "target_resolution": "exact",
 74356|       "evidence": [
 74357|         "return type 'IList < Arc >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 74358|       ],
 74359|       "source_url": "https://www.revitapidocs.com/2025/dde73d45-46a1-4789-7c63-5523ef16a6de.htm",
 74360|       "dll_signature_verified": true,
 74361|       "dll_relationship_scope": "declared",
 74362|       "dll_semantic_verified": null,
 74363|       "dll_verified_status": "signature_verified_declared",
 74364|       "revitlookup_referenced": null,
 74365|       "revitlookup_requires_document_context": null
 74366|     },
 74367|     {
 74368|       "source": "Autodesk.Revit.DB.IFC.ExporterIFCUtils",
 74369|       "target": "Autodesk.Revit.DB.Category",
 74370|       "member_name": "GetIFCClassNameByCategory",
 74371|       "member_kind": "method",
 74372|       "edge_type": "HAS_CATEGORY",
 74373|       "confidence": "name_only_candidate",
 74374|       "confidence_tier": "likely",
 74375|       "target_resolution": "exact",
 74376|       "evidence": [
 74377|         "member name 'GetIFCClassNameByCategory' matches keyword pattern /Category/ but return type '[' gives no type-level confirmation"
 74378|       ],
 74379|       "source_url": "https://www.revitapidocs.com/2025/7fff2d3a-4175-f2be-4ccc-2f6c5768b38b.htm",
 74380|       "dll_signature_verified": false,
 74381|       "dll_relationship_scope": "declared",
 74382|       "dll_semantic_verified": null,
 74383|       "dll_verified_status": "signature_mismatch",
 74384|       "revitlookup_referenced": null,
 74385|       "revitlookup_requires_document_context": null
 74386|     },
 74387|     {
 74388|       "source": "Autodesk.Revit.DB.IFC.ExporterIFCUtils",
 74389|       "target": null,
 74390|       "member_name": "GetLegacyCurtainSubElements",
 74391|       "member_kind": "method",
 74392|       "edge_type": "RETURNS_ELEMENT_IDS",
 74393|       "confidence": "unknown_reference",
 74394|       "confidence_tier": "unverified_reference",
 74395|       "target_resolution": "none",
 74396|       "evidence": [
 74397|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 74398|       ],
 74399|       "source_url": "https://www.revitapidocs.com/2025/898e687e-5b3a-d2ff-3118-79cf964af1c8.htm",
 74400|       "dll_signature_verified": true,
 74401|       "dll_relationship_scope": "declared",
 74402|       "dll_semantic_verified": null,
 74403|       "dll_verified_status": "signature_verified_declared",
 74404|       "revitlookup_referenced": null,
 74405|       "revitlookup_requires_document_context": null
 74406|     },
 74407|     {
 74408|       "source": "Autodesk.Revit.DB.IFC.ExporterIFCUtils",
 74409|       "target": "Autodesk.Revit.DB.IFC.IFCLegacyStairOrRamp",
 74410|       "member_name": "GetLegacyStairOrRampComponents",
 74411|       "member_kind": "method",
 74412|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74413|       "confidence": "direct_return_type",
 74414|       "confidence_tier": "unverified_reference",
 74415|       "target_resolution": "short_name_fallback",
 74416|       "evidence": [
 74417|         "return type 'IFCLegacyStairOrRamp' directly names a Revit DB object type"
 74418|       ],
 74419|       "source_url": "https://www.revitapidocs.com/2025/a0aa692b-ea27-7b8a-ab52-11d14943f269.htm",
 74420|       "dll_signature_verified": true,
 74421|       "dll_relationship_scope": "declared",
 74422|       "dll_semantic_verified": null,
 74423|       "dll_verified_status": "signature_verified_declared",
 74424|       "revitlookup_referenced": null,
 74425|       "revitlookup_requires_document_context": null
 74426|     },
 74427|     {
 74428|       "source": "Autodesk.Revit.DB.IFC.ExporterIFCUtils",
 74429|       "target": "Autodesk.Revit.DB.Level",
 74430|       "member_name": "GetLevelIdByHeight",
 74431|       "member_kind": "method",
 74432|       "edge_type": "ASSIGNED_TO_LEVEL",
 74433|       "confidence": "elementid_with_strong_name",
 74434|       "confidence_tier": "core",
 74435|       "target_resolution": "exact",
 74436|       "evidence": [
 74437|         "member name 'GetLevelIdByHeight' matches keyword pattern /Level/"
 74438|       ],
 74439|       "source_url": "https://www.revitapidocs.com/2025/b6142529-84ac-41b9-f0d5-ac0105e430c2.htm",
 74440|       "dll_signature_verified": true,
 74441|       "dll_relationship_scope": "declared",
 74442|       "dll_semantic_verified": null,
 74443|       "dll_verified_status": "signature_verified_declared",
 74444|       "revitlookup_referenced": null,
 74445|       "revitlookup_requires_document_context": null
 74446|     },
 74447|     {
 74448|       "source": "Autodesk.Revit.DB.IFC.ExporterIFCUtils",
 74449|       "target": "Autodesk.Revit.DB.IFC.IFCOpeningData",
 74450|       "member_name": "GetOpeningData",
 74451|       "member_kind": "method",
 74452|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74453|       "confidence": "needs_runtime_validation",
 74454|       "confidence_tier": "needs_validation",
 74455|       "target_resolution": "short_name_fallback",
 74456|       "evidence": [
 74457|         "return type 'IList < IFCOpeningData >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 74458|       ],
 74459|       "source_url": "https://www.revitapidocs.com/2025/4059efa7-87bd-5cb3-0b15-106aeb39f8e2.htm",
 74460|       "dll_signature_verified": true,
 74461|       "dll_relationship_scope": "declared",
 74462|       "dll_semantic_verified": null,
 74463|       "dll_verified_status": "signature_verified_declared",
 74464|       "revitlookup_referenced": null,
 74465|       "revitlookup_requires_document_context": null
 74466|     },
 74467|     {
 74468|       "source": "Autodesk.Revit.DB.IFC.ExporterIFCUtils",
 74469|       "target": "Autodesk.Revit.DB.FamilySymbol",
 74470|       "member_name": "GetOriginalSymbol",
 74471|       "member_kind": "method",
 74472|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74473|       "confidence": "direct_return_type",
 74474|       "confidence_tier": "unverified_reference",
 74475|       "target_resolution": "exact",
 74476|       "evidence": [
 74477|         "return type 'FamilySymbol' directly names a Revit DB object type"
 74478|       ],
 74479|       "source_url": "https://www.revitapidocs.com/2025/816f6d7b-d42b-2ba3-11fd-145649805ad1.htm",
 74480|       "dll_signature_verified": true,
 74481|       "dll_relationship_scope": "declared",
 74482|       "dll_semantic_verified": null,
 74483|       "dll_verified_status": "signature_verified_declared",
 74484|       "revitlookup_referenced": null,
 74485|       "revitlookup_requires_document_context": null
 74486|     },
 74487|     {
 74488|       "source": "Autodesk.Revit.DB.IFC.ExporterIFCUtils",
 74489|       "target": "Autodesk.Revit.DB.IFC.RoofComponents",
 74490|       "member_name": "GetRoofComponents",
 74491|       "member_kind": "method",
 74492|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74493|       "confidence": "direct_return_type",
 74494|       "confidence_tier": "unverified_reference",
 74495|       "target_resolution": "short_name_fallback",
 74496|       "evidence": [
 74497|         "return type 'RoofComponents' directly names a Revit DB object type"
 74498|       ],
 74499|       "source_url": "https://www.revitapidocs.com/2025/5f69588b-fe06-ca39-cf72-145e580d839b.htm",
 74500|       "dll_signature_verified": true,
```
