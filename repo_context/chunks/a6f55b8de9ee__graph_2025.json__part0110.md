# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 110 of 216
- Original line range: 42511-42910
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 42511|       "revitlookup_referenced": null,
 42512|       "revitlookup_requires_document_context": null
 42513|     },
 42514|     {
 42515|       "source": "Autodesk.Revit.DB.ExtrusionAnalyzer",
 42516|       "target": null,
 42517|       "member_name": "EndParameter",
 42518|       "member_kind": "property",
 42519|       "edge_type": "HAS_PARAMETER",
 42520|       "confidence": "name_only_candidate",
 42521|       "confidence_tier": "likely",
 42522|       "target_resolution": "none",
 42523|       "evidence": [
 42524|         "member name 'EndParameter' matches keyword pattern /Parameter/ but return type 'double' gives no type-level confirmation"
 42525|       ],
 42526|       "source_url": "https://www.revitapidocs.com/2025/bebcd422-84f8-2086-d130-ef04abab4d64.htm",
 42527|       "dll_signature_verified": true,
 42528|       "dll_relationship_scope": "declared",
 42529|       "dll_semantic_verified": null,
 42530|       "dll_verified_status": "signature_verified_declared",
 42531|       "revitlookup_referenced": null,
 42532|       "revitlookup_requires_document_context": null
 42533|     },
 42534|     {
 42535|       "source": "Autodesk.Revit.DB.ExtrusionAnalyzer",
 42536|       "target": null,
 42537|       "member_name": "StartParameter",
 42538|       "member_kind": "property",
 42539|       "edge_type": "HAS_PARAMETER",
 42540|       "confidence": "name_only_candidate",
 42541|       "confidence_tier": "likely",
 42542|       "target_resolution": "none",
 42543|       "evidence": [
 42544|         "member name 'StartParameter' matches keyword pattern /Parameter/ but return type 'double' gives no type-level confirmation"
 42545|       ],
 42546|       "source_url": "https://www.revitapidocs.com/2025/7d4e2c44-7021-7d94-de0f-e964447b17bb.htm",
 42547|       "dll_signature_verified": true,
 42548|       "dll_relationship_scope": "declared",
 42549|       "dll_semantic_verified": null,
 42550|       "dll_verified_status": "signature_verified_declared",
 42551|       "revitlookup_referenced": null,
 42552|       "revitlookup_requires_document_context": null
 42553|     },
 42554|     {
 42555|       "source": "Autodesk.Revit.DB.ExtrusionRoof",
 42556|       "target": "Autodesk.Revit.DB.CurtainGridSet",
 42557|       "member_name": "CurtainGrids",
 42558|       "member_kind": "property",
 42559|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 42560|       "confidence": "direct_return_type",
 42561|       "confidence_tier": "unverified_reference",
 42562|       "target_resolution": "exact",
 42563|       "evidence": [
 42564|         "return type 'CurtainGridSet' directly names a Revit DB object type"
 42565|       ],
 42566|       "source_url": "https://www.revitapidocs.com/2025/39baec89-9d0d-ba88-6485-ac8b76ccb6d0.htm",
 42567|       "dll_signature_verified": true,
 42568|       "dll_relationship_scope": "declared",
 42569|       "dll_semantic_verified": null,
 42570|       "dll_verified_status": "signature_verified_declared",
 42571|       "revitlookup_referenced": null,
 42572|       "revitlookup_requires_document_context": null
 42573|     },
 42574|     {
 42575|       "source": "Autodesk.Revit.DB.ExtrusionRoof",
 42576|       "target": "Autodesk.Revit.DB.ModelCurveArray",
 42577|       "member_name": "GetProfile",
 42578|       "member_kind": "method",
 42579|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 42580|       "confidence": "direct_return_type",
 42581|       "confidence_tier": "unverified_reference",
 42582|       "target_resolution": "exact",
 42583|       "evidence": [
 42584|         "return type 'ModelCurveArray' directly names a Revit DB object type"
 42585|       ],
 42586|       "source_url": "https://www.revitapidocs.com/2025/0cea252b-0609-0ba9-460a-67dc0cca6e20.htm",
 42587|       "dll_signature_verified": true,
 42588|       "dll_relationship_scope": "declared",
 42589|       "dll_semantic_verified": null,
 42590|       "dll_verified_status": "signature_verified_declared",
 42591|       "revitlookup_referenced": null,
 42592|       "revitlookup_requires_document_context": null
 42593|     },
 42594|     {
 42595|       "source": "Autodesk.Revit.DB.FabricationAncillaryUsage",
 42596|       "target": null,
 42597|       "member_name": "Type",
 42598|       "member_kind": "property",
 42599|       "edge_type": "TYPE_OF",
 42600|       "confidence": "name_only_candidate",
 42601|       "confidence_tier": "likely",
 42602|       "target_resolution": "none",
 42603|       "evidence": [
 42604|         "member name 'Type' matches keyword pattern /^(Type|TypeId|GetTypeId)$/ but return type 'FabricationAncillaryType' gives no type-level confirmation"
 42605|       ],
 42606|       "source_url": "https://www.revitapidocs.com/2025/8aaecf29-576c-4c8a-5023-3d02082b23a4.htm",
 42607|       "dll_signature_verified": true,
 42608|       "dll_relationship_scope": "declared",
 42609|       "dll_semantic_verified": null,
 42610|       "dll_verified_status": "signature_verified_declared",
 42611|       "revitlookup_referenced": null,
 42612|       "revitlookup_requires_document_context": null
 42613|     },
 42614|     {
 42615|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42616|       "target": null,
 42617|       "member_name": "GetAllDampers",
 42618|       "member_kind": "method",
 42619|       "edge_type": "RETURNS_ELEMENT_IDS",
 42620|       "confidence": "name_only_candidate",
 42621|       "confidence_tier": "likely",
 42622|       "target_resolution": "none",
 42623|       "evidence": [
 42624|         "member name 'GetAllDampers' matches keyword pattern /^GetAll/ but return type 'IList < int >' gives no type-level confirmation"
 42625|       ],
 42626|       "source_url": "https://www.revitapidocs.com/2025/b856c588-2a9d-fd34-4d4d-21ba8fcc1343.htm",
 42627|       "dll_signature_verified": true,
 42628|       "dll_relationship_scope": "declared",
 42629|       "dll_semantic_verified": null,
 42630|       "dll_verified_status": "signature_verified_declared",
 42631|       "revitlookup_referenced": null,
 42632|       "revitlookup_requires_document_context": null
 42633|     },
 42634|     {
 42635|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42636|       "target": null,
 42637|       "member_name": "GetAllFabricationConnectorDefinitions",
 42638|       "member_kind": "method",
 42639|       "edge_type": "RETURNS_ELEMENT_IDS",
 42640|       "confidence": "name_only_candidate",
 42641|       "confidence_tier": "likely",
 42642|       "target_resolution": "none",
 42643|       "evidence": [
 42644|         "member name 'GetAllFabricationConnectorDefinitions' matches keyword pattern /^GetAll/ but return type 'IList < int >' gives no type-level confirmation"
 42645|       ],
 42646|       "source_url": "https://www.revitapidocs.com/2025/d694f14a-7afc-1f01-334a-94dd21985835.htm",
 42647|       "dll_signature_verified": true,
 42648|       "dll_relationship_scope": "declared",
 42649|       "dll_semantic_verified": null,
 42650|       "dll_verified_status": "signature_verified_declared",
 42651|       "revitlookup_referenced": null,
 42652|       "revitlookup_requires_document_context": null
 42653|     },
 42654|     {
 42655|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42656|       "target": null,
 42657|       "member_name": "GetAllInsulationSpecifications",
 42658|       "member_kind": "method",
 42659|       "edge_type": "RETURNS_ELEMENT_IDS",
 42660|       "confidence": "name_only_candidate",
 42661|       "confidence_tier": "likely",
 42662|       "target_resolution": "none",
 42663|       "evidence": [
 42664|         "member name 'GetAllInsulationSpecifications' matches keyword pattern /^GetAll/ but return type 'IList < int >' gives no type-level confirmation"
 42665|       ],
 42666|       "source_url": "https://www.revitapidocs.com/2025/daaeb400-b013-36e5-f4f1-d697b668a712.htm",
 42667|       "dll_signature_verified": true,
 42668|       "dll_relationship_scope": "declared",
 42669|       "dll_semantic_verified": null,
 42670|       "dll_verified_status": "signature_verified_declared",
 42671|       "revitlookup_referenced": null,
 42672|       "revitlookup_requires_document_context": null
 42673|     },
 42674|     {
 42675|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42676|       "target": "Autodesk.Revit.DB.FabricationItemFile",
 42677|       "member_name": "GetAllLoadedItemFiles",
 42678|       "member_kind": "method",
 42679|       "edge_type": "RETURNS_ELEMENT_IDS",
 42680|       "confidence": "needs_runtime_validation",
 42681|       "confidence_tier": "needs_validation",
 42682|       "target_resolution": "exact",
 42683|       "evidence": [
 42684|         "return type 'IList < FabricationItemFile >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 42685|       ],
 42686|       "source_url": "https://www.revitapidocs.com/2025/2bc97834-4739-5bb2-83d5-c3ae296250ad.htm",
 42687|       "dll_signature_verified": true,
 42688|       "dll_relationship_scope": "declared",
 42689|       "dll_semantic_verified": null,
 42690|       "dll_verified_status": "signature_verified_declared",
 42691|       "revitlookup_referenced": null,
 42692|       "revitlookup_requires_document_context": null
 42693|     },
 42694|     {
 42695|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42696|       "target": "Autodesk.Revit.DB.FabricationService",
 42697|       "member_name": "GetAllLoadedServices",
 42698|       "member_kind": "method",
 42699|       "edge_type": "RETURNS_ELEMENT_IDS",
 42700|       "confidence": "needs_runtime_validation",
 42701|       "confidence_tier": "needs_validation",
 42702|       "target_resolution": "exact",
 42703|       "evidence": [
 42704|         "return type 'IList < FabricationService >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 42705|       ],
 42706|       "source_url": "https://www.revitapidocs.com/2025/834e34e3-1656-e0f4-b993-735712a3dba7.htm",
 42707|       "dll_signature_verified": true,
 42708|       "dll_relationship_scope": "declared",
 42709|       "dll_semantic_verified": null,
 42710|       "dll_verified_status": "signature_verified_declared",
 42711|       "revitlookup_referenced": null,
 42712|       "revitlookup_requires_document_context": null
 42713|     },
 42714|     {
 42715|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42716|       "target": "Autodesk.Revit.DB.Material",
 42717|       "member_name": "GetAllMaterials",
 42718|       "member_kind": "method",
 42719|       "edge_type": "USES_MATERIAL",
 42720|       "confidence": "name_only_candidate",
 42721|       "confidence_tier": "likely",
 42722|       "target_resolution": "exact",
 42723|       "evidence": [
 42724|         "member name 'GetAllMaterials' matches keyword pattern /Material/ but return type 'IList < int >' gives no type-level confirmation"
 42725|       ],
 42726|       "source_url": "https://www.revitapidocs.com/2025/f8e1ab05-467f-6ee6-3103-052d27e8af74.htm",
 42727|       "dll_signature_verified": true,
 42728|       "dll_relationship_scope": "declared",
 42729|       "dll_semantic_verified": null,
 42730|       "dll_verified_status": "signature_verified_declared",
 42731|       "revitlookup_referenced": null,
 42732|       "revitlookup_requires_document_context": null
 42733|     },
 42734|     {
 42735|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42736|       "target": null,
 42737|       "member_name": "GetAllPartCustomData",
 42738|       "member_kind": "method",
 42739|       "edge_type": "RETURNS_ELEMENT_IDS",
 42740|       "confidence": "name_only_candidate",
 42741|       "confidence_tier": "likely",
 42742|       "target_resolution": "none",
 42743|       "evidence": [
 42744|         "member name 'GetAllPartCustomData' matches keyword pattern /^GetAll/ but return type 'IList < int >' gives no type-level confirmation"
 42745|       ],
 42746|       "source_url": "https://www.revitapidocs.com/2025/19f74ecb-a0c6-5541-126d-9191b9b6db4a.htm",
 42747|       "dll_signature_verified": true,
 42748|       "dll_relationship_scope": "declared",
 42749|       "dll_semantic_verified": null,
 42750|       "dll_verified_status": "signature_verified_declared",
 42751|       "revitlookup_referenced": null,
 42752|       "revitlookup_requires_document_context": null
 42753|     },
 42754|     {
 42755|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42756|       "target": null,
 42757|       "member_name": "GetAllPartStatuses",
 42758|       "member_kind": "method",
 42759|       "edge_type": "RETURNS_ELEMENT_IDS",
 42760|       "confidence": "name_only_candidate",
 42761|       "confidence_tier": "likely",
 42762|       "target_resolution": "none",
 42763|       "evidence": [
 42764|         "member name 'GetAllPartStatuses' matches keyword pattern /^GetAll/ but return type 'IList < int >' gives no type-level confirmation"
 42765|       ],
 42766|       "source_url": "https://www.revitapidocs.com/2025/0cd93d49-9295-cd01-dd68-3a7abe616690.htm",
 42767|       "dll_signature_verified": true,
 42768|       "dll_relationship_scope": "declared",
 42769|       "dll_semantic_verified": null,
 42770|       "dll_verified_status": "signature_verified_declared",
 42771|       "revitlookup_referenced": null,
 42772|       "revitlookup_requires_document_context": null
 42773|     },
 42774|     {
 42775|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42776|       "target": "Autodesk.Revit.DB.FabricationService",
 42777|       "member_name": "GetAllServices",
 42778|       "member_kind": "method",
 42779|       "edge_type": "RETURNS_ELEMENT_IDS",
 42780|       "confidence": "needs_runtime_validation",
 42781|       "confidence_tier": "needs_validation",
 42782|       "target_resolution": "exact",
 42783|       "evidence": [
 42784|         "return type 'IList < FabricationService >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 42785|       ],
 42786|       "source_url": "https://www.revitapidocs.com/2025/ce53c061-fce3-80f7-ca41-80846e3b2159.htm",
 42787|       "dll_signature_verified": true,
 42788|       "dll_relationship_scope": "declared",
 42789|       "dll_semantic_verified": null,
 42790|       "dll_verified_status": "signature_verified_declared",
 42791|       "revitlookup_referenced": null,
 42792|       "revitlookup_requires_document_context": null
 42793|     },
 42794|     {
 42795|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42796|       "target": null,
 42797|       "member_name": "GetAllSpecifications",
 42798|       "member_kind": "method",
 42799|       "edge_type": "RETURNS_ELEMENT_IDS",
 42800|       "confidence": "name_only_candidate",
 42801|       "confidence_tier": "likely",
 42802|       "target_resolution": "none",
 42803|       "evidence": [
 42804|         "member name 'GetAllSpecifications' matches keyword pattern /^GetAll/ but return type 'IList < int >' gives no type-level confirmation"
 42805|       ],
 42806|       "source_url": "https://www.revitapidocs.com/2025/736406c3-c459-2fad-b966-7dc37f339b65.htm",
 42807|       "dll_signature_verified": true,
 42808|       "dll_relationship_scope": "declared",
 42809|       "dll_semantic_verified": null,
 42810|       "dll_verified_status": "signature_verified_declared",
 42811|       "revitlookup_referenced": null,
 42812|       "revitlookup_requires_document_context": null
 42813|     },
 42814|     {
 42815|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42816|       "target": "Autodesk.Revit.DB.FabricationItemFile",
 42817|       "member_name": "GetAllUsedItemFiles",
 42818|       "member_kind": "method",
 42819|       "edge_type": "RETURNS_ELEMENT_IDS",
 42820|       "confidence": "needs_runtime_validation",
 42821|       "confidence_tier": "needs_validation",
 42822|       "target_resolution": "exact",
 42823|       "evidence": [
 42824|         "return type 'IList < FabricationItemFile >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 42825|       ],
 42826|       "source_url": "https://www.revitapidocs.com/2025/0c62a934-d041-14d6-dc54-bd99b8a67111.htm",
 42827|       "dll_signature_verified": true,
 42828|       "dll_relationship_scope": "declared",
 42829|       "dll_semantic_verified": null,
 42830|       "dll_verified_status": "signature_verified_declared",
 42831|       "revitlookup_referenced": null,
 42832|       "revitlookup_requires_document_context": null
 42833|     },
 42834|     {
 42835|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42836|       "target": "Autodesk.Revit.DB.FabricationService",
 42837|       "member_name": "GetAllUsedServices",
 42838|       "member_kind": "method",
 42839|       "edge_type": "RETURNS_ELEMENT_IDS",
 42840|       "confidence": "needs_runtime_validation",
 42841|       "confidence_tier": "needs_validation",
 42842|       "target_resolution": "exact",
 42843|       "evidence": [
 42844|         "return type 'IList < FabricationService >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 42845|       ],
 42846|       "source_url": "https://www.revitapidocs.com/2025/4a7b444f-9a43-5188-5ee4-d13debe21eec.htm",
 42847|       "dll_signature_verified": true,
 42848|       "dll_relationship_scope": "declared",
 42849|       "dll_semantic_verified": null,
 42850|       "dll_verified_status": "signature_verified_declared",
 42851|       "revitlookup_referenced": null,
 42852|       "revitlookup_requires_document_context": null
 42853|     },
 42854|     {
 42855|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42856|       "target": null,
 42857|       "member_name": "GetAncillaryGroup",
 42858|       "member_kind": "method",
 42859|       "edge_type": "MEMBER_OF_GROUP",
 42860|       "confidence": "name_only_candidate",
 42861|       "confidence_tier": "likely",
 42862|       "target_resolution": "none",
 42863|       "evidence": [
 42864|         "member name 'GetAncillaryGroup' matches keyword pattern /^GetMember|Group/ but return type 'string' gives no type-level confirmation"
 42865|       ],
 42866|       "source_url": "https://www.revitapidocs.com/2025/881dd623-3c7d-59e1-b19d-9f56e5f6d45a.htm",
 42867|       "dll_signature_verified": true,
 42868|       "dll_relationship_scope": "declared",
 42869|       "dll_semantic_verified": null,
 42870|       "dll_verified_status": "signature_verified_declared",
 42871|       "revitlookup_referenced": null,
 42872|       "revitlookup_requires_document_context": null
 42873|     },
 42874|     {
 42875|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42876|       "target": null,
 42877|       "member_name": "GetAncillaryGroupName",
 42878|       "member_kind": "method",
 42879|       "edge_type": "MEMBER_OF_GROUP",
 42880|       "confidence": "name_only_candidate",
 42881|       "confidence_tier": "likely",
 42882|       "target_resolution": "none",
 42883|       "evidence": [
 42884|         "member name 'GetAncillaryGroupName' matches keyword pattern /^GetMember|Group/ but return type 'string' gives no type-level confirmation"
 42885|       ],
 42886|       "source_url": "https://www.revitapidocs.com/2025/2367e7e5-4c90-0473-cddc-0281093af660.htm",
 42887|       "dll_signature_verified": true,
 42888|       "dll_relationship_scope": "declared",
 42889|       "dll_semantic_verified": null,
 42890|       "dll_verified_status": "signature_verified_declared",
 42891|       "revitlookup_referenced": null,
 42892|       "revitlookup_requires_document_context": null
 42893|     },
 42894|     {
 42895|       "source": "Autodesk.Revit.DB.FabricationConfiguration",
 42896|       "target": "Autodesk.Revit.DB.FabricationConfigurationInfo",
 42897|       "member_name": "GetFabricationConfigurationInfo",
 42898|       "member_kind": "method",
 42899|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 42900|       "confidence": "direct_return_type",
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
```
