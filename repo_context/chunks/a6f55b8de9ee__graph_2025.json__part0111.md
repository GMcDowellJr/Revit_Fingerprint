# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 111 of 216
- Original line range: 42901-43300
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 42901|       "confidence_tier": "unverified_reference",
 42902|       "target_resolution": "exact",
 42903|       "evidence": [
 42904|         "return type 'FabricationConfigurationInfo' directly names a Revit DB object type"
 42905|       ],
 42906|       "source_url": "https://www.revitapidocs.com/2025/3912030b-b1af-8856-2ace-f6ceeb369cec.htm",
 42907|       "dll_signature_verified": true,
 42908|       "dll_relationship_scope": "declared",
 42909|       "dll_semantic_verified": null,
 42910|       "dll_verified_status": "signature_verified_declared",
 42911|       "revitlookup_referenced": null,
 42912|       "revitlookup_requires_document_context": null
 42913|     },
 42914|     {
 42915|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42916|       "target": null,
 42917|       "member_name": "GetFabricationConnectorGroup",
 42918|       "member_kind": "method",
 42919|       "edge_type": "MEMBER_OF_GROUP",
 42920|       "confidence": "name_only_candidate",
 42921|       "confidence_tier": "likely",
 42922|       "target_resolution": "none",
 42923|       "evidence": [
 42924|         "member name 'GetFabricationConnectorGroup' matches keyword pattern /^GetMember|Group/ but return type 'string' gives no type-level confirmation"
 42925|       ],
 42926|       "source_url": "https://www.revitapidocs.com/2025/23aa9453-cc47-939b-c691-341f9b8bf3a6.htm",
 42927|       "dll_signature_verified": true,
 42928|       "dll_relationship_scope": "declared",
 42929|       "dll_semantic_verified": null,
 42930|       "dll_verified_status": "signature_verified_declared",
 42931|       "revitlookup_referenced": null,
 42932|       "revitlookup_requires_document_context": null
 42933|     },
 42934|     {
 42935|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42936|       "target": null,
 42937|       "member_name": "GetInsulationSpecificationGroup",
 42938|       "member_kind": "method",
 42939|       "edge_type": "MEMBER_OF_GROUP",
 42940|       "confidence": "name_only_candidate",
 42941|       "confidence_tier": "likely",
 42942|       "target_resolution": "none",
 42943|       "evidence": [
 42944|         "member name 'GetInsulationSpecificationGroup' matches keyword pattern /^GetMember|Group/ but return type 'string' gives no type-level confirmation"
 42945|       ],
 42946|       "source_url": "https://www.revitapidocs.com/2025/dec6f45a-0b84-46c9-37a6-8dc9dbdc024e.htm",
 42947|       "dll_signature_verified": true,
 42948|       "dll_relationship_scope": "declared",
 42949|       "dll_semantic_verified": null,
 42950|       "dll_verified_status": "signature_verified_declared",
 42951|       "revitlookup_referenced": null,
 42952|       "revitlookup_requires_document_context": null
 42953|     },
 42954|     {
 42955|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42956|       "target": "Autodesk.Revit.DB.FabricationItemFolder",
 42957|       "member_name": "GetItemFolders",
 42958|       "member_kind": "method",
 42959|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 42960|       "confidence": "needs_runtime_validation",
 42961|       "confidence_tier": "needs_validation",
 42962|       "target_resolution": "exact",
 42963|       "evidence": [
 42964|         "return type 'IList < FabricationItemFolder >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 42965|       ],
 42966|       "source_url": "https://www.revitapidocs.com/2025/d283e079-1113-f185-e9c0-d40fada72391.htm",
 42967|       "dll_signature_verified": true,
 42968|       "dll_relationship_scope": "declared",
 42969|       "dll_semantic_verified": null,
 42970|       "dll_verified_status": "signature_verified_declared",
 42971|       "revitlookup_referenced": null,
 42972|       "revitlookup_requires_document_context": null
 42973|     },
 42974|     {
 42975|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42976|       "target": "Autodesk.Revit.DB.Material",
 42977|       "member_name": "GetMaterialAbbreviation",
 42978|       "member_kind": "method",
 42979|       "edge_type": "USES_MATERIAL",
 42980|       "confidence": "name_only_candidate",
 42981|       "confidence_tier": "likely",
 42982|       "target_resolution": "exact",
 42983|       "evidence": [
 42984|         "member name 'GetMaterialAbbreviation' matches keyword pattern /Material/ but return type 'string' gives no type-level confirmation"
 42985|       ],
 42986|       "source_url": "https://www.revitapidocs.com/2025/fc7879dd-dd2c-71f5-429b-b640d4ac20be.htm",
 42987|       "dll_signature_verified": true,
 42988|       "dll_relationship_scope": "declared",
 42989|       "dll_semantic_verified": null,
 42990|       "dll_verified_status": "signature_verified_declared",
 42991|       "revitlookup_referenced": null,
 42992|       "revitlookup_requires_document_context": null
 42993|     },
 42994|     {
 42995|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42996|       "target": "Autodesk.Revit.DB.Material",
 42997|       "member_name": "GetMaterialByGUID",
 42998|       "member_kind": "method",
 42999|       "edge_type": "USES_MATERIAL",
 43000|       "confidence": "name_only_candidate",
 43001|       "confidence_tier": "likely",
 43002|       "target_resolution": "exact",
 43003|       "evidence": [
 43004|         "member name 'GetMaterialByGUID' matches keyword pattern /Material/ but return type 'int' gives no type-level confirmation"
 43005|       ],
 43006|       "source_url": "https://www.revitapidocs.com/2025/a2bebe19-1cb3-7bfc-c19f-d17ee5e614b8.htm",
 43007|       "dll_signature_verified": true,
 43008|       "dll_relationship_scope": "declared",
 43009|       "dll_semantic_verified": null,
 43010|       "dll_verified_status": "signature_verified_declared",
 43011|       "revitlookup_referenced": null,
 43012|       "revitlookup_requires_document_context": null
 43013|     },
 43014|     {
 43015|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 43016|       "target": "Autodesk.Revit.DB.Material",
 43017|       "member_name": "GetMaterialGaugeByGUID",
 43018|       "member_kind": "method",
 43019|       "edge_type": "USES_MATERIAL",
 43020|       "confidence": "name_only_candidate",
 43021|       "confidence_tier": "likely",
 43022|       "target_resolution": "exact",
 43023|       "evidence": [
 43024|         "member name 'GetMaterialGaugeByGUID' matches keyword pattern /Material/ but return type 'int' gives no type-level confirmation"
 43025|       ],
 43026|       "source_url": "https://www.revitapidocs.com/2025/0dcb90fe-5bce-a212-05b0-b107060dd381.htm",
 43027|       "dll_signature_verified": true,
 43028|       "dll_relationship_scope": "declared",
 43029|       "dll_semantic_verified": null,
 43030|       "dll_verified_status": "signature_verified_declared",
 43031|       "revitlookup_referenced": null,
 43032|       "revitlookup_requires_document_context": null
 43033|     },
 43034|     {
 43035|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 43036|       "target": "Autodesk.Revit.DB.Material",
 43037|       "member_name": "GetMaterialGaugeGUID",
 43038|       "member_kind": "method",
 43039|       "edge_type": "USES_MATERIAL",
 43040|       "confidence": "name_only_candidate",
 43041|       "confidence_tier": "likely",
 43042|       "target_resolution": "exact",
 43043|       "evidence": [
 43044|         "member name 'GetMaterialGaugeGUID' matches keyword pattern /Material/ but return type 'Guid' gives no type-level confirmation"
 43045|       ],
 43046|       "source_url": "https://www.revitapidocs.com/2025/8a8c3761-d5ee-15a5-abc0-659d62845c0c.htm",
 43047|       "dll_signature_verified": true,
 43048|       "dll_relationship_scope": "declared",
 43049|       "dll_semantic_verified": null,
 43050|       "dll_verified_status": "signature_verified_declared",
 43051|       "revitlookup_referenced": null,
 43052|       "revitlookup_requires_document_context": null
 43053|     },
 43054|     {
 43055|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 43056|       "target": "Autodesk.Revit.DB.Material",
 43057|       "member_name": "GetMaterialGroup",
 43058|       "member_kind": "method",
 43059|       "edge_type": "USES_MATERIAL",
 43060|       "confidence": "name_only_candidate",
 43061|       "confidence_tier": "likely",
 43062|       "target_resolution": "exact",
 43063|       "evidence": [
 43064|         "member name 'GetMaterialGroup' matches keyword pattern /Material/ but return type 'string' gives no type-level confirmation"
 43065|       ],
 43066|       "source_url": "https://www.revitapidocs.com/2025/51b9427d-cfef-30be-45cf-fb282212a9a5.htm",
 43067|       "dll_signature_verified": true,
 43068|       "dll_relationship_scope": "declared",
 43069|       "dll_semantic_verified": null,
 43070|       "dll_verified_status": "signature_verified_declared",
 43071|       "revitlookup_referenced": null,
 43072|       "revitlookup_requires_document_context": null
 43073|     },
 43074|     {
 43075|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 43076|       "target": "Autodesk.Revit.DB.Material",
 43077|       "member_name": "GetMaterialGUID",
 43078|       "member_kind": "method",
 43079|       "edge_type": "USES_MATERIAL",
 43080|       "confidence": "name_only_candidate",
 43081|       "confidence_tier": "likely",
 43082|       "target_resolution": "exact",
 43083|       "evidence": [
 43084|         "member name 'GetMaterialGUID' matches keyword pattern /Material/ but return type 'Guid' gives no type-level confirmation"
 43085|       ],
 43086|       "source_url": "https://www.revitapidocs.com/2025/b394287b-bfa2-2fc9-e457-6afb7fbf064c.htm",
 43087|       "dll_signature_verified": true,
 43088|       "dll_relationship_scope": "declared",
 43089|       "dll_semantic_verified": null,
 43090|       "dll_verified_status": "signature_verified_declared",
 43091|       "revitlookup_referenced": null,
 43092|       "revitlookup_requires_document_context": null
 43093|     },
 43094|     {
 43095|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 43096|       "target": "Autodesk.Revit.DB.Material",
 43097|       "member_name": "GetMaterialName",
 43098|       "member_kind": "method",
 43099|       "edge_type": "USES_MATERIAL",
 43100|       "confidence": "name_only_candidate",
 43101|       "confidence_tier": "likely",
 43102|       "target_resolution": "exact",
 43103|       "evidence": [
 43104|         "member name 'GetMaterialName' matches keyword pattern /Material/ but return type 'string' gives no type-level confirmation"
 43105|       ],
 43106|       "source_url": "https://www.revitapidocs.com/2025/828e75ce-67c4-be63-65a0-d547c2541d21.htm",
 43107|       "dll_signature_verified": true,
 43108|       "dll_relationship_scope": "declared",
 43109|       "dll_semantic_verified": null,
 43110|       "dll_verified_status": "signature_verified_declared",
 43111|       "revitlookup_referenced": null,
 43112|       "revitlookup_requires_document_context": null
 43113|     },
 43114|     {
 43115|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 43116|       "target": "Autodesk.Revit.DB.FabricationService",
 43117|       "member_name": "GetService",
 43118|       "member_kind": "method",
 43119|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 43120|       "confidence": "direct_return_type",
 43121|       "confidence_tier": "unverified_reference",
 43122|       "target_resolution": "exact",
 43123|       "evidence": [
 43124|         "return type 'FabricationService' directly names a Revit DB object type"
 43125|       ],
 43126|       "source_url": "https://www.revitapidocs.com/2025/428c7eb4-2dec-4038-3dd4-4dd2a1fd85bd.htm",
 43127|       "dll_signature_verified": true,
 43128|       "dll_relationship_scope": "declared",
 43129|       "dll_semantic_verified": null,
 43130|       "dll_verified_status": "signature_verified_declared",
 43131|       "revitlookup_referenced": null,
 43132|       "revitlookup_requires_document_context": null
 43133|     },
 43134|     {
 43135|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 43136|       "target": null,
 43137|       "member_name": "GetSpecificationGroup",
 43138|       "member_kind": "method",
 43139|       "edge_type": "MEMBER_OF_GROUP",
 43140|       "confidence": "name_only_candidate",
 43141|       "confidence_tier": "likely",
 43142|       "target_resolution": "none",
 43143|       "evidence": [
 43144|         "member name 'GetSpecificationGroup' matches keyword pattern /^GetMember|Group/ but return type 'string' gives no type-level confirmation"
 43145|       ],
 43146|       "source_url": "https://www.revitapidocs.com/2025/b3c66f8d-7aa8-ad2c-bfeb-6e69b7eb12b5.htm",
 43147|       "dll_signature_verified": true,
 43148|       "dll_relationship_scope": "declared",
 43149|       "dll_semantic_verified": null,
 43150|       "dll_verified_status": "signature_verified_declared",
 43151|       "revitlookup_referenced": null,
 43152|       "revitlookup_requires_document_context": null
 43153|     },
 43154|     {
 43155|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 43156|       "target": null,
 43157|       "member_name": "GetUpdatedStraightsFromValidateConnections",
 43158|       "member_kind": "method",
 43159|       "edge_type": "RETURNS_ELEMENT_IDS",
 43160|       "confidence": "unknown_reference",
 43161|       "confidence_tier": "unverified_reference",
 43162|       "target_resolution": "none",
 43163|       "evidence": [
 43164|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 43165|       ],
 43166|       "source_url": "https://www.revitapidocs.com/2025/710c220d-b82b-413b-7491-e9d633359713.htm",
 43167|       "dll_signature_verified": true,
 43168|       "dll_relationship_scope": "declared",
 43169|       "dll_semantic_verified": null,
 43170|       "dll_verified_status": "signature_verified_declared",
 43171|       "revitlookup_referenced": null,
 43172|       "revitlookup_requires_document_context": null
 43173|     },
 43174|     {
 43175|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 43176|       "target": "Autodesk.Revit.DB.FabricationItemFile",
 43177|       "member_name": "LoadItemFiles",
 43178|       "member_kind": "method",
 43179|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 43180|       "confidence": "needs_runtime_validation",
 43181|       "confidence_tier": "needs_validation",
 43182|       "target_resolution": "exact",
 43183|       "evidence": [
 43184|         "return type 'IList < FabricationItemFile >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 43185|       ],
 43186|       "source_url": "https://www.revitapidocs.com/2025/d7aa286a-d55d-46da-b654-0c175f433abc.htm",
 43187|       "dll_signature_verified": true,
 43188|       "dll_relationship_scope": "declared",
 43189|       "dll_semantic_verified": null,
 43190|       "dll_verified_status": "signature_verified_declared",
 43191|       "revitlookup_referenced": null,
 43192|       "revitlookup_requires_document_context": null
 43193|     },
 43194|     {
 43195|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 43196|       "target": "Autodesk.Revit.DB.Material",
 43197|       "member_name": "LocateMaterial",
 43198|       "member_kind": "method",
 43199|       "edge_type": "USES_MATERIAL",
 43200|       "confidence": "name_only_candidate",
 43201|       "confidence_tier": "likely",
 43202|       "target_resolution": "exact",
 43203|       "evidence": [
 43204|         "member name 'LocateMaterial' matches keyword pattern /Material/ but return type 'int' gives no type-level confirmation"
 43205|       ],
 43206|       "source_url": "https://www.revitapidocs.com/2025/d3466136-f845-f453-9456-9981cd4d4fdd.htm",
 43207|       "dll_signature_verified": true,
 43208|       "dll_relationship_scope": "declared",
 43209|       "dll_semantic_verified": null,
 43210|       "dll_verified_status": "signature_verified_declared",
 43211|       "revitlookup_referenced": null,
 43212|       "revitlookup_requires_document_context": null
 43213|     },
 43214|     {
 43215|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 43216|       "target": "Autodesk.Revit.DB.ConfigurationReloadInfo",
 43217|       "member_name": "ReloadConfiguration",
 43218|       "member_kind": "method",
 43219|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 43220|       "confidence": "direct_return_type",
 43221|       "confidence_tier": "unverified_reference",
 43222|       "target_resolution": "exact",
 43223|       "evidence": [
 43224|         "return type 'ConfigurationReloadInfo' directly names a Revit DB object type"
 43225|       ],
 43226|       "source_url": "https://www.revitapidocs.com/2025/4a40d755-029f-5a44-b2a4-b4bb749eae52.htm",
 43227|       "dll_signature_verified": true,
 43228|       "dll_relationship_scope": "declared",
 43229|       "dll_semantic_verified": null,
 43230|       "dll_verified_status": "signature_verified_declared",
 43231|       "revitlookup_referenced": null,
 43232|       "revitlookup_requires_document_context": null
 43233|     },
 43234|     {
 43235|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 43236|       "target": "Autodesk.Revit.DB.ConnectionValidationInfo",
 43237|       "member_name": "ValidateConnectionsForAllFabricationParts",
 43238|       "member_kind": "method",
 43239|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 43240|       "confidence": "direct_return_type",
 43241|       "confidence_tier": "unverified_reference",
 43242|       "target_resolution": "exact",
 43243|       "evidence": [
 43244|         "return type 'ConnectionValidationInfo' directly names a Revit DB object type"
 43245|       ],
 43246|       "source_url": "https://www.revitapidocs.com/2025/c514bccb-d434-8ea3-b0da-d2cd3a4d617d.htm",
 43247|       "dll_signature_verified": true,
 43248|       "dll_relationship_scope": "declared",
 43249|       "dll_semantic_verified": null,
 43250|       "dll_verified_status": "signature_verified_declared",
 43251|       "revitlookup_referenced": null,
 43252|       "revitlookup_requires_document_context": null
 43253|     },
 43254|     {
 43255|       "source": "Autodesk.Revit.DB.FabricationConfigurationInfo",
 43256|       "target": "Autodesk.Revit.DB.FabricationConfigurationInfo",
 43257|       "member_name": "GetAllFabricationConfigurations",
 43258|       "member_kind": "method",
 43259|       "edge_type": "RETURNS_ELEMENT_IDS",
 43260|       "confidence": "needs_runtime_validation",
 43261|       "confidence_tier": "needs_validation",
 43262|       "target_resolution": "exact",
 43263|       "evidence": [
 43264|         "return type 'IList < FabricationConfigurationInfo >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 43265|       ],
 43266|       "source_url": "https://www.revitapidocs.com/2025/3450b8a8-63ac-543c-1813-1b30b0987a1d.htm",
 43267|       "dll_signature_verified": true,
 43268|       "dll_relationship_scope": "declared",
 43269|       "dll_semantic_verified": null,
 43270|       "dll_verified_status": "signature_verified_declared",
 43271|       "revitlookup_referenced": null,
 43272|       "revitlookup_requires_document_context": null
 43273|     },
 43274|     {
 43275|       "source": "Autodesk.Revit.DB.FabricationDimensionDefinition",
 43276|       "target": null,
 43277|       "member_name": "Type",
 43278|       "member_kind": "property",
 43279|       "edge_type": "TYPE_OF",
 43280|       "confidence": "name_only_candidate",
 43281|       "confidence_tier": "likely",
 43282|       "target_resolution": "none",
 43283|       "evidence": [
 43284|         "member name 'Type' matches keyword pattern /^(Type|TypeId|GetTypeId)$/ but return type 'FabricationDimensionType' gives no type-level confirmation"
 43285|       ],
 43286|       "source_url": "https://www.revitapidocs.com/2025/fa5d34d4-d177-fda6-3a96-b557ce970910.htm",
 43287|       "dll_signature_verified": true,
 43288|       "dll_relationship_scope": "declared",
 43289|       "dll_semantic_verified": null,
 43290|       "dll_verified_status": "signature_verified_declared",
 43291|       "revitlookup_referenced": null,
 43292|       "revitlookup_requires_document_context": null
 43293|     },
 43294|     {
 43295|       "source": "Autodesk.Revit.DB.FabricationHostedInfo",
 43296|       "target": null,
 43297|       "member_name": "HostId",
 43298|       "member_kind": "property",
 43299|       "edge_type": "HOSTED_BY",
 43300|       "confidence": "elementid_with_strong_name",
```
