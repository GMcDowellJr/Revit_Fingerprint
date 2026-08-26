# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 180 of 216
- Original line range: 69811-70210
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 69811|       ],
 69812|       "source_url": "https://www.revitapidocs.com/2025/2e8c09ca-d8b6-d586-a8b0-b221d84ef593.htm",
 69813|       "dll_signature_verified": true,
 69814|       "dll_relationship_scope": "declared",
 69815|       "dll_semantic_verified": null,
 69816|       "dll_verified_status": "signature_verified_declared",
 69817|       "revitlookup_referenced": null,
 69818|       "revitlookup_requires_document_context": null
 69819|     },
 69820|     {
 69821|       "source": "Autodesk.Revit.DB.Electrical.AnalyticalPowerSourceData",
 69822|       "target": null,
 69823|       "member_name": "Voltage",
 69824|       "member_kind": "method",
 69825|       "edge_type": "TAGS_ELEMENT",
 69826|       "confidence": "name_only_candidate",
 69827|       "confidence_tier": "likely",
 69828|       "target_resolution": "none",
 69829|       "evidence": [
 69830|         "member name 'Voltage' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type '[' gives no type-level confirmation"
 69831|       ],
 69832|       "source_url": "https://www.revitapidocs.com/2025/fc7a9c1a-6a5d-3103-fbe0-87f7f4888c6e.htm",
 69833|       "dll_signature_verified": false,
 69834|       "dll_relationship_scope": null,
 69835|       "dll_semantic_verified": null,
 69836|       "dll_verified_status": "member_not_found",
 69837|       "revitlookup_referenced": null,
 69838|       "revitlookup_requires_document_context": null
 69839|     },
 69840|     {
 69841|       "source": "Autodesk.Revit.DB.Electrical.AnalyticalTransferSwitchData",
 69842|       "target": null,
 69843|       "member_name": "Voltage",
 69844|       "member_kind": "method",
 69845|       "edge_type": "TAGS_ELEMENT",
 69846|       "confidence": "name_only_candidate",
 69847|       "confidence_tier": "likely",
 69848|       "target_resolution": "none",
 69849|       "evidence": [
 69850|         "member name 'Voltage' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type '[' gives no type-level confirmation"
 69851|       ],
 69852|       "source_url": "https://www.revitapidocs.com/2025/85f3cc07-2cdd-a26e-04e0-6b2a87871d14.htm",
 69853|       "dll_signature_verified": false,
 69854|       "dll_relationship_scope": null,
 69855|       "dll_semantic_verified": null,
 69856|       "dll_verified_status": "member_not_found",
 69857|       "revitlookup_referenced": null,
 69858|       "revitlookup_requires_document_context": null
 69859|     },
 69860|     {
 69861|       "source": "Autodesk.Revit.DB.Electrical.AnalyticalTransformerData",
 69862|       "target": null,
 69863|       "member_name": "SecondaryDistributionSystem",
 69864|       "member_kind": "property",
 69865|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 69866|       "confidence": "unknown_reference",
 69867|       "confidence_tier": "unverified_reference",
 69868|       "target_resolution": "none",
 69869|       "evidence": [
 69870|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 69871|       ],
 69872|       "source_url": "https://www.revitapidocs.com/2025/df6261db-4ad4-7d00-9fd2-d3ebdc988351.htm",
 69873|       "dll_signature_verified": true,
 69874|       "dll_relationship_scope": "declared",
 69875|       "dll_semantic_verified": null,
 69876|       "dll_verified_status": "signature_verified_declared",
 69877|       "revitlookup_referenced": null,
 69878|       "revitlookup_requires_document_context": null
 69879|     },
 69880|     {
 69881|       "source": "Autodesk.Revit.DB.Electrical.AreaBasedLoadBoundaryLineData",
 69882|       "target": "Autodesk.Revit.DB.Level",
 69883|       "member_name": "BottomLevelId",
 69884|       "member_kind": "property",
 69885|       "edge_type": "ASSIGNED_TO_LEVEL",
 69886|       "confidence": "elementid_with_strong_name",
 69887|       "confidence_tier": "core",
 69888|       "target_resolution": "exact",
 69889|       "evidence": [
 69890|         "member name 'BottomLevelId' matches keyword pattern /Level/"
 69891|       ],
 69892|       "source_url": "https://www.revitapidocs.com/2025/e9807384-17dd-09c0-c03b-d9ff2bfbe8c0.htm",
 69893|       "dll_signature_verified": true,
 69894|       "dll_relationship_scope": "declared",
 69895|       "dll_semantic_verified": null,
 69896|       "dll_verified_status": "signature_verified_declared",
 69897|       "revitlookup_referenced": null,
 69898|       "revitlookup_requires_document_context": null
 69899|     },
 69900|     {
 69901|       "source": "Autodesk.Revit.DB.Electrical.AreaBasedLoadBoundaryLineData",
 69902|       "target": "Autodesk.Revit.DB.Level",
 69903|       "member_name": "TopLevelId",
 69904|       "member_kind": "property",
 69905|       "edge_type": "ASSIGNED_TO_LEVEL",
 69906|       "confidence": "elementid_with_strong_name",
 69907|       "confidence_tier": "core",
 69908|       "target_resolution": "exact",
 69909|       "evidence": [
 69910|         "member name 'TopLevelId' matches keyword pattern /Level/"
 69911|       ],
 69912|       "source_url": "https://www.revitapidocs.com/2025/90187c6f-2765-840d-1bc1-909cebc9143d.htm",
 69913|       "dll_signature_verified": true,
 69914|       "dll_relationship_scope": "declared",
 69915|       "dll_semantic_verified": null,
 69916|       "dll_verified_status": "signature_verified_declared",
 69917|       "revitlookup_referenced": null,
 69918|       "revitlookup_requires_document_context": null
 69919|     },
 69920|     {
 69921|       "source": "Autodesk.Revit.DB.Electrical.AreaBasedLoadBoundaryLineData",
 69922|       "target": "Autodesk.Revit.DB.Level",
 69923|       "member_name": "GetLevelIdsInRange",
 69924|       "member_kind": "method",
 69925|       "edge_type": "ASSIGNED_TO_LEVEL",
 69926|       "confidence": "elementid_collection_with_strong_name",
 69927|       "confidence_tier": "core",
 69928|       "target_resolution": "exact",
 69929|       "evidence": [
 69930|         "member name 'GetLevelIdsInRange' matches keyword pattern /Level/"
 69931|       ],
 69932|       "source_url": "https://www.revitapidocs.com/2025/cc6beeb3-928d-4220-7b5f-a3f1b14c344c.htm",
 69933|       "dll_signature_verified": true,
 69934|       "dll_relationship_scope": "declared",
 69935|       "dll_semantic_verified": null,
 69936|       "dll_verified_status": "signature_verified_declared",
 69937|       "revitlookup_referenced": null,
 69938|       "revitlookup_requires_document_context": null
 69939|     },
 69940|     {
 69941|       "source": "Autodesk.Revit.DB.Electrical.AreaBasedLoadBoundaryLineData",
 69942|       "target": "Autodesk.Revit.DB.Level",
 69943|       "member_name": "IsLevelWithinRange",
 69944|       "member_kind": "method",
 69945|       "edge_type": "ASSIGNED_TO_LEVEL",
 69946|       "confidence": "name_only_candidate",
 69947|       "confidence_tier": "likely",
 69948|       "target_resolution": "exact",
 69949|       "evidence": [
 69950|         "member name 'IsLevelWithinRange' matches keyword pattern /Level/ but return type 'bool' gives no type-level confirmation"
 69951|       ],
 69952|       "source_url": "https://www.revitapidocs.com/2025/b32cd564-681a-c6c6-4c74-cc708b3d31c4.htm",
 69953|       "dll_signature_verified": true,
 69954|       "dll_relationship_scope": "declared",
 69955|       "dll_semantic_verified": null,
 69956|       "dll_verified_status": "signature_verified_declared",
 69957|       "revitlookup_referenced": null,
 69958|       "revitlookup_requires_document_context": null
 69959|     },
 69960|     {
 69961|       "source": "Autodesk.Revit.DB.Electrical.AreaBasedLoadData",
 69962|       "target": null,
 69963|       "member_name": "AreaBasedLoadType",
 69964|       "member_kind": "property",
 69965|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 69966|       "confidence": "unknown_reference",
 69967|       "confidence_tier": "unverified_reference",
 69968|       "target_resolution": "none",
 69969|       "evidence": [
 69970|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 69971|       ],
 69972|       "source_url": "https://www.revitapidocs.com/2025/e9f29bc9-9f9b-4c7f-579f-fc5b5516e369.htm",
 69973|       "dll_signature_verified": true,
 69974|       "dll_relationship_scope": "declared",
 69975|       "dll_semantic_verified": null,
 69976|       "dll_verified_status": "signature_verified_declared",
 69977|       "revitlookup_referenced": null,
 69978|       "revitlookup_requires_document_context": null
 69979|     },
 69980|     {
 69981|       "source": "Autodesk.Revit.DB.Electrical.AreaBasedLoadData",
 69982|       "target": "Autodesk.Revit.DB.Phase",
 69983|       "member_name": "ConnectedPhases",
 69984|       "member_kind": "property",
 69985|       "edge_type": "ASSIGNED_TO_PHASE",
 69986|       "confidence": "name_only_candidate",
 69987|       "confidence_tier": "likely",
 69988|       "target_resolution": "exact",
 69989|       "evidence": [
 69990|         "member name 'ConnectedPhases' matches keyword pattern /Phase/ but return type 'ElectricalConnectedPhases' gives no type-level confirmation"
 69991|       ],
 69992|       "source_url": "https://www.revitapidocs.com/2025/07685400-54cb-a60d-c5c9-45ced44f1021.htm",
 69993|       "dll_signature_verified": true,
 69994|       "dll_relationship_scope": "declared",
 69995|       "dll_semantic_verified": null,
 69996|       "dll_verified_status": "signature_verified_declared",
 69997|       "revitlookup_referenced": null,
 69998|       "revitlookup_requires_document_context": null
 69999|     },
 70000|     {
 70001|       "source": "Autodesk.Revit.DB.Electrical.AreaBasedLoadData",
 70002|       "target": null,
 70003|       "member_name": "LoadClassification",
 70004|       "member_kind": "property",
 70005|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 70006|       "confidence": "unknown_reference",
 70007|       "confidence_tier": "unverified_reference",
 70008|       "target_resolution": "none",
 70009|       "evidence": [
 70010|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 70011|       ],
 70012|       "source_url": "https://www.revitapidocs.com/2025/2df5488b-0748-3c63-ba50-564bf3e9e4f1.htm",
 70013|       "dll_signature_verified": true,
 70014|       "dll_relationship_scope": "declared",
 70015|       "dll_semantic_verified": null,
 70016|       "dll_verified_status": "signature_verified_declared",
 70017|       "revitlookup_referenced": null,
 70018|       "revitlookup_requires_document_context": null
 70019|     },
 70020|     {
 70021|       "source": "Autodesk.Revit.DB.Electrical.AreaBasedLoadData",
 70022|       "target": "Autodesk.Revit.DB.Phase",
 70023|       "member_name": "PhasesNumber",
 70024|       "member_kind": "property",
 70025|       "edge_type": "ASSIGNED_TO_PHASE",
 70026|       "confidence": "name_only_candidate",
 70027|       "confidence_tier": "likely",
 70028|       "target_resolution": "exact",
 70029|       "evidence": [
 70030|         "member name 'PhasesNumber' matches keyword pattern /Phase/ but return type 'int' gives no type-level confirmation"
 70031|       ],
 70032|       "source_url": "https://www.revitapidocs.com/2025/d63e643c-917f-ca52-5d3a-af59cb7b5343.htm",
 70033|       "dll_signature_verified": true,
 70034|       "dll_relationship_scope": "declared",
 70035|       "dll_semantic_verified": null,
 70036|       "dll_verified_status": "signature_verified_declared",
 70037|       "revitlookup_referenced": null,
 70038|       "revitlookup_requires_document_context": null
 70039|     },
 70040|     {
 70041|       "source": "Autodesk.Revit.DB.Electrical.AreaBasedLoadData",
 70042|       "target": null,
 70043|       "member_name": "Voltage",
 70044|       "member_kind": "property",
 70045|       "edge_type": "TAGS_ELEMENT",
 70046|       "confidence": "name_only_candidate",
 70047|       "confidence_tier": "likely",
 70048|       "target_resolution": "none",
 70049|       "evidence": [
 70050|         "member name 'Voltage' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'double' gives no type-level confirmation"
 70051|       ],
 70052|       "source_url": "https://www.revitapidocs.com/2025/8709fbe4-24ac-79b1-cd15-f3a96d511b6c.htm",
 70053|       "dll_signature_verified": true,
 70054|       "dll_relationship_scope": "declared",
 70055|       "dll_semantic_verified": null,
 70056|       "dll_verified_status": "signature_verified_declared",
 70057|       "revitlookup_referenced": null,
 70058|       "revitlookup_requires_document_context": null
 70059|     },
 70060|     {
 70061|       "source": "Autodesk.Revit.DB.Electrical.AreaBasedLoadData",
 70062|       "target": null,
 70063|       "member_name": "GetElectricalLoadAreas",
 70064|       "member_kind": "method",
 70065|       "edge_type": "RETURNS_ELEMENT_IDS",
 70066|       "confidence": "unknown_reference",
 70067|       "confidence_tier": "unverified_reference",
 70068|       "target_resolution": "none",
 70069|       "evidence": [
 70070|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 70071|       ],
 70072|       "source_url": "https://www.revitapidocs.com/2025/05745395-b4f0-44ec-8732-93a4147b60b0.htm",
 70073|       "dll_signature_verified": true,
 70074|       "dll_relationship_scope": "declared",
 70075|       "dll_semantic_verified": null,
 70076|       "dll_verified_status": "signature_verified_declared",
 70077|       "revitlookup_referenced": null,
 70078|       "revitlookup_requires_document_context": null
 70079|     },
 70080|     {
 70081|       "source": "Autodesk.Revit.DB.Electrical.AreaBasedLoadData",
 70082|       "target": null,
 70083|       "member_name": "GetUpstreamNodeId",
 70084|       "member_kind": "method",
 70085|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 70086|       "confidence": "unknown_reference",
 70087|       "confidence_tier": "unverified_reference",
 70088|       "target_resolution": "none",
 70089|       "evidence": [
 70090|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 70091|       ],
 70092|       "source_url": "https://www.revitapidocs.com/2025/36df9aff-cc37-9dff-85d0-15f265198a76.htm",
 70093|       "dll_signature_verified": true,
 70094|       "dll_relationship_scope": "declared",
 70095|       "dll_semantic_verified": null,
 70096|       "dll_verified_status": "signature_verified_declared",
 70097|       "revitlookup_referenced": null,
 70098|       "revitlookup_requires_document_context": null
 70099|     },
 70100|     {
 70101|       "source": "Autodesk.Revit.DB.Electrical.AreaBasedLoadType",
 70102|       "target": null,
 70103|       "member_name": "LoadClassification",
 70104|       "member_kind": "property",
 70105|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 70106|       "confidence": "unknown_reference",
 70107|       "confidence_tier": "unverified_reference",
 70108|       "target_resolution": "none",
 70109|       "evidence": [
 70110|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 70111|       ],
 70112|       "source_url": "https://www.revitapidocs.com/2025/fd08a05b-e697-a519-60f8-e40a888e5bcb.htm",
 70113|       "dll_signature_verified": true,
 70114|       "dll_relationship_scope": "declared",
 70115|       "dll_semantic_verified": null,
 70116|       "dll_verified_status": "signature_verified_declared",
 70117|       "revitlookup_referenced": null,
 70118|       "revitlookup_requires_document_context": null
 70119|     },
 70120|     {
 70121|       "source": "Autodesk.Revit.DB.Electrical.CableTrayConduitBase",
 70122|       "target": null,
 70123|       "member_name": "RunId",
 70124|       "member_kind": "property",
 70125|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 70126|       "confidence": "unknown_reference",
 70127|       "confidence_tier": "unverified_reference",
 70128|       "target_resolution": "none",
 70129|       "evidence": [
 70130|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 70131|       ],
 70132|       "source_url": "https://www.revitapidocs.com/2025/6e83aa34-591d-a91f-cebc-1bf7e28d2e6c.htm",
 70133|       "dll_signature_verified": true,
 70134|       "dll_relationship_scope": "declared",
 70135|       "dll_semantic_verified": null,
 70136|       "dll_verified_status": "signature_verified_declared",
 70137|       "revitlookup_referenced": null,
 70138|       "revitlookup_requires_document_context": null
 70139|     },
 70140|     {
 70141|       "source": "Autodesk.Revit.DB.Electrical.CableTrayConduitBase",
 70142|       "target": "Autodesk.Revit.DB.Level",
 70143|       "member_name": "IsValidLevelId",
 70144|       "member_kind": "method",
 70145|       "edge_type": "ASSIGNED_TO_LEVEL",
 70146|       "confidence": "name_only_candidate",
 70147|       "confidence_tier": "likely",
 70148|       "target_resolution": "exact",
 70149|       "evidence": [
 70150|         "member name 'IsValidLevelId' matches keyword pattern /Level/ but return type 'bool' gives no type-level confirmation"
 70151|       ],
 70152|       "source_url": "https://www.revitapidocs.com/2025/152e60e9-3c1e-fbf6-17f9-98b774547694.htm",
 70153|       "dll_signature_verified": true,
 70154|       "dll_relationship_scope": "declared",
 70155|       "dll_semantic_verified": null,
 70156|       "dll_verified_status": "signature_verified_declared",
 70157|       "revitlookup_referenced": null,
 70158|       "revitlookup_requires_document_context": null
 70159|     },
 70160|     {
 70161|       "source": "Autodesk.Revit.DB.Electrical.CableTraySizeIterator",
 70162|       "target": "Autodesk.Revit.DB.MEPSize",
 70163|       "member_name": "Current",
 70164|       "member_kind": "property",
 70165|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70166|       "confidence": "direct_return_type",
 70167|       "confidence_tier": "unverified_reference",
 70168|       "target_resolution": "exact",
 70169|       "evidence": [
 70170|         "return type 'MEPSize' directly names a Revit DB object type"
 70171|       ],
 70172|       "source_url": "https://www.revitapidocs.com/2025/4630e1ba-2b2f-05c9-1006-8adc936ea769.htm",
 70173|       "dll_signature_verified": true,
 70174|       "dll_relationship_scope": "declared",
 70175|       "dll_semantic_verified": null,
 70176|       "dll_verified_status": "signature_verified_declared",
 70177|       "revitlookup_referenced": null,
 70178|       "revitlookup_requires_document_context": null
 70179|     },
 70180|     {
 70181|       "source": "Autodesk.Revit.DB.Electrical.CableTraySizeIterator",
 70182|       "target": "Autodesk.Revit.DB.MEPSize",
 70183|       "member_name": "GetCurrent",
 70184|       "member_kind": "method",
 70185|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70186|       "confidence": "direct_return_type",
 70187|       "confidence_tier": "unverified_reference",
 70188|       "target_resolution": "exact",
 70189|       "evidence": [
 70190|         "return type 'MEPSize' directly names a Revit DB object type"
 70191|       ],
 70192|       "source_url": "https://www.revitapidocs.com/2025/9685cd25-4b38-fc59-5acb-240d6e9f9ee2.htm",
 70193|       "dll_signature_verified": true,
 70194|       "dll_relationship_scope": "declared",
 70195|       "dll_semantic_verified": null,
 70196|       "dll_verified_status": "signature_verified_declared",
 70197|       "revitlookup_referenced": null,
 70198|       "revitlookup_requires_document_context": null
 70199|     },
 70200|     {
 70201|       "source": "Autodesk.Revit.DB.Electrical.CableTraySizes",
 70202|       "target": "Autodesk.Revit.DB.Electrical.CableTraySizeIterator",
 70203|       "member_name": "GetCableTraySizesIterator",
 70204|       "member_kind": "method",
 70205|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70206|       "confidence": "direct_return_type",
 70207|       "confidence_tier": "unverified_reference",
 70208|       "target_resolution": "short_name_fallback",
 70209|       "evidence": [
 70210|         "return type 'CableTraySizeIterator' directly names a Revit DB object type"
```
