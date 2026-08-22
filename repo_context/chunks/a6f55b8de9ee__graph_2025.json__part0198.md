# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 198 of 216
- Original line range: 76831-77230
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 76831|       "member_kind": "method",
 76832|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76833|       "confidence": "direct_return_type",
 76834|       "confidence_tier": "unverified_reference",
 76835|       "target_resolution": "short_name_fallback",
 76836|       "evidence": [
 76837|         "return type 'FluidTemperature' directly names a Revit DB object type"
 76838|       ],
 76839|       "source_url": "https://www.revitapidocs.com/2025/c9aa98b4-430f-d035-4e32-a0c55392c6f6.htm",
 76840|       "dll_signature_verified": true,
 76841|       "dll_relationship_scope": "declared",
 76842|       "dll_semantic_verified": null,
 76843|       "dll_verified_status": "signature_verified_declared",
 76844|       "revitlookup_referenced": null,
 76845|       "revitlookup_requires_document_context": null
 76846|     },
 76847|     {
 76848|       "source": "Autodesk.Revit.DB.Plumbing.IPipeFittingAndAccessoryPressureDropServer",
 76849|       "target": "Autodesk.Revit.DB.ExtensibleStorage.Schema",
 76850|       "member_name": "GetDataSchema",
 76851|       "member_kind": "method",
 76852|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76853|       "confidence": "direct_return_type",
 76854|       "confidence_tier": "unverified_reference",
 76855|       "target_resolution": "exact",
 76856|       "evidence": [
 76857|         "return type 'Schema' directly names a Revit DB object type"
 76858|       ],
 76859|       "source_url": "https://www.revitapidocs.com/2025/11d7e6cd-5e1e-c717-735f-eeb383df7e2a.htm",
 76860|       "dll_signature_verified": true,
 76861|       "dll_relationship_scope": "declared",
 76862|       "dll_semantic_verified": null,
 76863|       "dll_verified_status": "signature_verified_declared",
 76864|       "revitlookup_referenced": null,
 76865|       "revitlookup_requires_document_context": null
 76866|     },
 76867|     {
 76868|       "source": "Autodesk.Revit.DB.Plumbing.Pipe",
 76869|       "target": "Autodesk.Revit.DB.Plumbing.PipeSegment",
 76870|       "member_name": "PipeSegment",
 76871|       "member_kind": "property",
 76872|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76873|       "confidence": "direct_return_type",
 76874|       "confidence_tier": "unverified_reference",
 76875|       "target_resolution": "short_name_fallback",
 76876|       "evidence": [
 76877|         "return type 'PipeSegment' directly names a Revit DB object type"
 76878|       ],
 76879|       "source_url": "https://www.revitapidocs.com/2025/bc1e5201-3ba2-0e18-54bd-4377afc656fe.htm",
 76880|       "dll_signature_verified": true,
 76881|       "dll_relationship_scope": "declared",
 76882|       "dll_semantic_verified": null,
 76883|       "dll_verified_status": "signature_verified_declared",
 76884|       "revitlookup_referenced": null,
 76885|       "revitlookup_requires_document_context": null
 76886|     },
 76887|     {
 76888|       "source": "Autodesk.Revit.DB.Plumbing.Pipe",
 76889|       "target": "Autodesk.Revit.DB.Plumbing.PipeType",
 76890|       "member_name": "PipeType",
 76891|       "member_kind": "property",
 76892|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76893|       "confidence": "direct_return_type",
 76894|       "confidence_tier": "unverified_reference",
 76895|       "target_resolution": "short_name_fallback",
 76896|       "evidence": [
 76897|         "return type 'PipeType' directly names a Revit DB object type"
 76898|       ],
 76899|       "source_url": "https://www.revitapidocs.com/2025/cd80f438-0548-0863-b483-06cc678bd2b3.htm",
 76900|       "dll_signature_verified": true,
 76901|       "dll_relationship_scope": "declared",
 76902|       "dll_semantic_verified": null,
 76903|       "dll_verified_status": "signature_verified_declared",
 76904|       "revitlookup_referenced": null,
 76905|       "revitlookup_requires_document_context": null
 76906|     },
 76907|     {
 76908|       "source": "Autodesk.Revit.DB.Plumbing.PipeFittingAndAccessoryData",
 76909|       "target": "Autodesk.Revit.DB.Plumbing.PipeFittingAndAccessoryConnectorData",
 76910|       "member_name": "GetAllConnectorData",
 76911|       "member_kind": "method",
 76912|       "edge_type": "RETURNS_ELEMENT_IDS",
 76913|       "confidence": "needs_runtime_validation",
 76914|       "confidence_tier": "needs_validation",
 76915|       "target_resolution": "short_name_fallback",
 76916|       "evidence": [
 76917|         "return type 'IList < PipeFittingAndAccessoryConnectorData >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 76918|       ],
 76919|       "source_url": "https://www.revitapidocs.com/2025/66c865d8-12fe-cc4c-cbdc-674c62d5f528.htm",
 76920|       "dll_signature_verified": true,
 76921|       "dll_relationship_scope": "declared",
 76922|       "dll_semantic_verified": null,
 76923|       "dll_verified_status": "signature_verified_declared",
 76924|       "revitlookup_referenced": null,
 76925|       "revitlookup_requires_document_context": null
 76926|     },
 76927|     {
 76928|       "source": "Autodesk.Revit.DB.Plumbing.PipeFittingAndAccessoryData",
 76929|       "target": "Autodesk.Revit.DB.ExtensibleStorage.Entity",
 76930|       "member_name": "GetEntity",
 76931|       "member_kind": "method",
 76932|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76933|       "confidence": "direct_return_type",
 76934|       "confidence_tier": "unverified_reference",
 76935|       "target_resolution": "short_name_fallback",
 76936|       "evidence": [
 76937|         "return type 'Entity' directly names a Revit DB object type"
 76938|       ],
 76939|       "source_url": "https://www.revitapidocs.com/2025/8fbbfc26-4995-ae39-f25d-5020635f9161.htm",
 76940|       "dll_signature_verified": true,
 76941|       "dll_relationship_scope": "declared",
 76942|       "dll_semantic_verified": null,
 76943|       "dll_verified_status": "signature_verified_declared",
 76944|       "revitlookup_referenced": null,
 76945|       "revitlookup_requires_document_context": null
 76946|     },
 76947|     {
 76948|       "source": "Autodesk.Revit.DB.Plumbing.PipeFittingAndAccessoryData",
 76949|       "target": null,
 76950|       "member_name": "GetFamilyInstanceId",
 76951|       "member_kind": "method",
 76952|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 76953|       "confidence": "unknown_reference",
 76954|       "confidence_tier": "unverified_reference",
 76955|       "target_resolution": "none",
 76956|       "evidence": [
 76957|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 76958|       ],
 76959|       "source_url": "https://www.revitapidocs.com/2025/d62954b2-55dc-6f8c-8ceb-1528b90806a3.htm",
 76960|       "dll_signature_verified": true,
 76961|       "dll_relationship_scope": "declared",
 76962|       "dll_semantic_verified": null,
 76963|       "dll_verified_status": "signature_verified_declared",
 76964|       "revitlookup_referenced": null,
 76965|       "revitlookup_requires_document_context": null
 76966|     },
 76967|     {
 76968|       "source": "Autodesk.Revit.DB.Plumbing.PipeFittingAndAccessoryPressureDropData",
 76969|       "target": "Autodesk.Revit.DB.Plumbing.PipeFittingAndAccessoryData",
 76970|       "member_name": "GetPipeFittingAndAccessoryData",
 76971|       "member_kind": "method",
 76972|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76973|       "confidence": "direct_return_type",
 76974|       "confidence_tier": "unverified_reference",
 76975|       "target_resolution": "short_name_fallback",
 76976|       "evidence": [
 76977|         "return type 'PipeFittingAndAccessoryData' directly names a Revit DB object type"
 76978|       ],
 76979|       "source_url": "https://www.revitapidocs.com/2025/46547a49-5faa-8311-2ff5-706cfdac3ac5.htm",
 76980|       "dll_signature_verified": true,
 76981|       "dll_relationship_scope": "declared",
 76982|       "dll_semantic_verified": null,
 76983|       "dll_verified_status": "signature_verified_declared",
 76984|       "revitlookup_referenced": null,
 76985|       "revitlookup_requires_document_context": null
 76986|     },
 76987|     {
 76988|       "source": "Autodesk.Revit.DB.Plumbing.PipeFittingAndAccessoryPressureDropData",
 76989|       "target": "Autodesk.Revit.DB.Plumbing.PipeFittingAndAccessoryPressureDropItem",
 76990|       "member_name": "GetPresureDropItems",
 76991|       "member_kind": "method",
 76992|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76993|       "confidence": "needs_runtime_validation",
 76994|       "confidence_tier": "needs_validation",
 76995|       "target_resolution": "short_name_fallback",
 76996|       "evidence": [
 76997|         "return type 'IList < PipeFittingAndAccessoryPressureDropItem >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 76998|       ],
 76999|       "source_url": "https://www.revitapidocs.com/2025/64c763c1-c558-88f8-1330-12c0c9f215f8.htm",
 77000|       "dll_signature_verified": true,
 77001|       "dll_relationship_scope": "declared",
 77002|       "dll_semantic_verified": null,
 77003|       "dll_verified_status": "signature_verified_declared",
 77004|       "revitlookup_referenced": null,
 77005|       "revitlookup_requires_document_context": null
 77006|     },
 77007|     {
 77008|       "source": "Autodesk.Revit.DB.Plumbing.PipePressureDropData",
 77009|       "target": "Autodesk.Revit.DB.Category",
 77010|       "member_name": "CategoryId",
 77011|       "member_kind": "property",
 77012|       "edge_type": "HAS_CATEGORY",
 77013|       "confidence": "elementid_with_strong_name",
 77014|       "confidence_tier": "core",
 77015|       "target_resolution": "exact",
 77016|       "evidence": [
 77017|         "member name 'CategoryId' matches keyword pattern /Category/"
 77018|       ],
 77019|       "source_url": "https://www.revitapidocs.com/2025/8b101e05-e4b0-429e-1c6a-8ef2d8682581.htm",
 77020|       "dll_signature_verified": true,
 77021|       "dll_relationship_scope": "declared",
 77022|       "dll_semantic_verified": null,
 77023|       "dll_verified_status": "signature_verified_declared",
 77024|       "revitlookup_referenced": null,
 77025|       "revitlookup_requires_document_context": null
 77026|     },
 77027|     {
 77028|       "source": "Autodesk.Revit.DB.Plumbing.PipePressureDropData",
 77029|       "target": "Autodesk.Revit.DB.Level",
 77030|       "member_name": "KLevel",
 77031|       "member_kind": "property",
 77032|       "edge_type": "ASSIGNED_TO_LEVEL",
 77033|       "confidence": "name_only_candidate",
 77034|       "confidence_tier": "likely",
 77035|       "target_resolution": "exact",
 77036|       "evidence": [
 77037|         "member name 'KLevel' matches keyword pattern /Level/ but return type 'SystemCalculationLevel' gives no type-level confirmation"
 77038|       ],
 77039|       "source_url": "https://www.revitapidocs.com/2025/c39ca077-b584-142a-343d-2ee14dcf80be.htm",
 77040|       "dll_signature_verified": true,
 77041|       "dll_relationship_scope": "declared",
 77042|       "dll_semantic_verified": null,
 77043|       "dll_verified_status": "signature_verified_declared",
 77044|       "revitlookup_referenced": null,
 77045|       "revitlookup_requires_document_context": null
 77046|     },
 77047|     {
 77048|       "source": "Autodesk.Revit.DB.Plumbing.PipeScheduleType",
 77049|       "target": null,
 77050|       "member_name": "GetPipeScheduleId",
 77051|       "member_kind": "method",
 77052|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 77053|       "confidence": "unknown_reference",
 77054|       "confidence_tier": "unverified_reference",
 77055|       "target_resolution": "none",
 77056|       "evidence": [
 77057|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 77058|       ],
 77059|       "source_url": "https://www.revitapidocs.com/2025/72fa88cd-5c96-bab5-e7f7-76ecd42c7e9f.htm",
 77060|       "dll_signature_verified": true,
 77061|       "dll_relationship_scope": "declared",
 77062|       "dll_semantic_verified": null,
 77063|       "dll_verified_status": "signature_verified_declared",
 77064|       "revitlookup_referenced": null,
 77065|       "revitlookup_requires_document_context": null
 77066|     },
 77067|     {
 77068|       "source": "Autodesk.Revit.DB.Plumbing.PipeSegment",
 77069|       "target": null,
 77070|       "member_name": "ScheduleTypeId",
 77071|       "member_kind": "property",
 77072|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 77073|       "confidence": "unknown_reference",
 77074|       "confidence_tier": "unverified_reference",
 77075|       "target_resolution": "none",
 77076|       "evidence": [
 77077|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 77078|       ],
 77079|       "source_url": "https://www.revitapidocs.com/2025/c0475662-4896-fbd8-5431-c61952f62960.htm",
 77080|       "dll_signature_verified": true,
 77081|       "dll_relationship_scope": "declared",
 77082|       "dll_semantic_verified": null,
 77083|       "dll_verified_status": "signature_verified_declared",
 77084|       "revitlookup_referenced": null,
 77085|       "revitlookup_requires_document_context": null
 77086|     },
 77087|     {
 77088|       "source": "Autodesk.Revit.DB.Plumbing.PipeSettings",
 77089|       "target": "Autodesk.Revit.DB.MEPCalculationServerInfo",
 77090|       "member_name": "GetFlowConvertionServerInfo",
 77091|       "member_kind": "method",
 77092|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 77093|       "confidence": "direct_return_type",
 77094|       "confidence_tier": "unverified_reference",
 77095|       "target_resolution": "exact",
 77096|       "evidence": [
 77097|         "return type 'MEPCalculationServerInfo' directly names a Revit DB object type"
 77098|       ],
 77099|       "source_url": "https://www.revitapidocs.com/2025/9ab99425-32ed-3b7b-2353-2d76cc364468.htm",
 77100|       "dll_signature_verified": true,
 77101|       "dll_relationship_scope": "declared",
 77102|       "dll_semantic_verified": null,
 77103|       "dll_verified_status": "signature_verified_declared",
 77104|       "revitlookup_referenced": null,
 77105|       "revitlookup_requires_document_context": null
 77106|     },
 77107|     {
 77108|       "source": "Autodesk.Revit.DB.Plumbing.PipingSystem",
 77109|       "target": "Autodesk.Revit.DB.Connector",
 77110|       "member_name": "BaseEquipmentConnector",
 77111|       "member_kind": "property",
 77112|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 77113|       "confidence": "direct_return_type",
 77114|       "confidence_tier": "unverified_reference",
 77115|       "target_resolution": "exact",
 77116|       "evidence": [
 77117|         "return type 'Connector' directly names a Revit DB object type"
 77118|       ],
 77119|       "source_url": "https://www.revitapidocs.com/2025/15ef1a11-38c9-04ca-a75d-92d63f129047.htm",
 77120|       "dll_signature_verified": true,
 77121|       "dll_relationship_scope": "declared",
 77122|       "dll_semantic_verified": null,
 77123|       "dll_verified_status": "signature_verified_declared",
 77124|       "revitlookup_referenced": null,
 77125|       "revitlookup_requires_document_context": null
 77126|     },
 77127|     {
 77128|       "source": "Autodesk.Revit.DB.Plumbing.PipingSystem",
 77129|       "target": "Autodesk.Revit.DB.ElementSet",
 77130|       "member_name": "PipingNetwork",
 77131|       "member_kind": "property",
 77132|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 77133|       "confidence": "direct_return_type",
 77134|       "confidence_tier": "unverified_reference",
 77135|       "target_resolution": "exact",
 77136|       "evidence": [
 77137|         "return type 'ElementSet' directly names a Revit DB object type"
 77138|       ],
 77139|       "source_url": "https://www.revitapidocs.com/2025/fd5c6d0f-3c77-6afd-a752-5ee7c7b8e1a1.htm",
 77140|       "dll_signature_verified": true,
 77141|       "dll_relationship_scope": "declared",
 77142|       "dll_semantic_verified": null,
 77143|       "dll_verified_status": "signature_verified_declared",
 77144|       "revitlookup_referenced": null,
 77145|       "revitlookup_requires_document_context": null
 77146|     },
 77147|     {
 77148|       "source": "Autodesk.Revit.DB.Plumbing.PipingSystem",
 77149|       "target": null,
 77150|       "member_name": "GetPumpSets",
 77151|       "member_kind": "method",
 77152|       "edge_type": "RETURNS_ELEMENT_IDS",
 77153|       "confidence": "unknown_reference",
 77154|       "confidence_tier": "unverified_reference",
 77155|       "target_resolution": "none",
 77156|       "evidence": [
 77157|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 77158|       ],
 77159|       "source_url": "https://www.revitapidocs.com/2025/2a46ea1c-73e9-a258-4ad4-239a9011f7e9.htm",
 77160|       "dll_signature_verified": true,
 77161|       "dll_relationship_scope": "declared",
 77162|       "dll_semantic_verified": null,
 77163|       "dll_verified_status": "signature_verified_declared",
 77164|       "revitlookup_referenced": null,
 77165|       "revitlookup_requires_document_context": null
 77166|     },
 77167|     {
 77168|       "source": "Autodesk.Revit.DB.Plumbing.PipingSystemType",
 77169|       "target": null,
 77170|       "member_name": "FluidType",
 77171|       "member_kind": "property",
 77172|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 77173|       "confidence": "unknown_reference",
 77174|       "confidence_tier": "unverified_reference",
 77175|       "target_resolution": "none",
 77176|       "evidence": [
 77177|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 77178|       ],
 77179|       "source_url": "https://www.revitapidocs.com/2025/c1c50fad-9320-831c-2675-4ab23f720837.htm",
 77180|       "dll_signature_verified": true,
 77181|       "dll_relationship_scope": "declared",
 77182|       "dll_semantic_verified": null,
 77183|       "dll_verified_status": "signature_verified_declared",
 77184|       "revitlookup_referenced": null,
 77185|       "revitlookup_requires_document_context": null
 77186|     },
 77187|     {
 77188|       "source": "Autodesk.Revit.DB.Plumbing.PlumbingUtils",
 77189|       "target": null,
 77190|       "member_name": "BreakCurve",
 77191|       "member_kind": "method",
 77192|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 77193|       "confidence": "unknown_reference",
 77194|       "confidence_tier": "unverified_reference",
 77195|       "target_resolution": "none",
 77196|       "evidence": [
 77197|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 77198|       ],
 77199|       "source_url": "https://www.revitapidocs.com/2025/3c302b80-d1f8-0e17-154a-b809cad2e545.htm",
 77200|       "dll_signature_verified": true,
 77201|       "dll_relationship_scope": "declared",
 77202|       "dll_semantic_verified": null,
 77203|       "dll_verified_status": "signature_verified_declared",
 77204|       "revitlookup_referenced": null,
 77205|       "revitlookup_requires_document_context": null
 77206|     },
 77207|     {
 77208|       "source": "Autodesk.Revit.DB.Plumbing.PlumbingUtils",
 77209|       "target": null,
 77210|       "member_name": "ConvertPipePlaceholders",
 77211|       "member_kind": "method",
 77212|       "edge_type": "RETURNS_ELEMENT_IDS",
 77213|       "confidence": "unknown_reference",
 77214|       "confidence_tier": "unverified_reference",
 77215|       "target_resolution": "none",
 77216|       "evidence": [
 77217|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 77218|       ],
 77219|       "source_url": "https://www.revitapidocs.com/2025/de0f8319-1219-c564-c06c-bc256c0ed9b2.htm",
 77220|       "dll_signature_verified": true,
 77221|       "dll_relationship_scope": "declared",
 77222|       "dll_semantic_verified": null,
 77223|       "dll_verified_status": "signature_verified_declared",
 77224|       "revitlookup_referenced": null,
 77225|       "revitlookup_requires_document_context": null
 77226|     },
 77227|     {
 77228|       "source": "Autodesk.Revit.DB.PointClouds.PointCloudOverrides",
 77229|       "target": "Autodesk.Revit.DB.PointClouds.PointCloudOverrideSettings",
 77230|       "member_name": "GetPointCloudRegionOverrideSettings",
```
