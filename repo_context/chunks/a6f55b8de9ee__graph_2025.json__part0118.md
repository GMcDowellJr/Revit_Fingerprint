# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 118 of 216
- Original line range: 45631-46030
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 45631|       "dll_semantic_verified": null,
 45632|       "dll_verified_status": "signature_verified_declared",
 45633|       "revitlookup_referenced": null,
 45634|       "revitlookup_requires_document_context": null
 45635|     },
 45636|     {
 45637|       "source": "Autodesk.Revit.DB.FamilyParameterSet",
 45638|       "target": "Autodesk.Revit.DB.FamilyParameterSetIterator",
 45639|       "member_name": "ForwardIterator",
 45640|       "member_kind": "method",
 45641|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45642|       "confidence": "direct_return_type",
 45643|       "confidence_tier": "unverified_reference",
 45644|       "target_resolution": "exact",
 45645|       "evidence": [
 45646|         "return type 'FamilyParameterSetIterator' directly names a Revit DB object type"
 45647|       ],
 45648|       "source_url": "https://www.revitapidocs.com/2025/048f14e4-2760-8c6d-fdbe-98f6ed97ccb7.htm",
 45649|       "dll_signature_verified": true,
 45650|       "dll_relationship_scope": "declared",
 45651|       "dll_semantic_verified": null,
 45652|       "dll_verified_status": "signature_verified_declared",
 45653|       "revitlookup_referenced": null,
 45654|       "revitlookup_requires_document_context": null
 45655|     },
 45656|     {
 45657|       "source": "Autodesk.Revit.DB.FamilyParameterSet",
 45658|       "target": "Autodesk.Revit.DB.FamilyParameterSetIterator",
 45659|       "member_name": "ReverseIterator",
 45660|       "member_kind": "method",
 45661|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45662|       "confidence": "direct_return_type",
 45663|       "confidence_tier": "unverified_reference",
 45664|       "target_resolution": "exact",
 45665|       "evidence": [
 45666|         "return type 'FamilyParameterSetIterator' directly names a Revit DB object type"
 45667|       ],
 45668|       "source_url": "https://www.revitapidocs.com/2025/462072a5-96d2-37fe-45f8-6ccda997e2c3.htm",
 45669|       "dll_signature_verified": true,
 45670|       "dll_relationship_scope": "declared",
 45671|       "dll_semantic_verified": null,
 45672|       "dll_verified_status": "signature_verified_declared",
 45673|       "revitlookup_referenced": null,
 45674|       "revitlookup_requires_document_context": null
 45675|     },
 45676|     {
 45677|       "source": "Autodesk.Revit.DB.FamilyPointPlacementReference",
 45678|       "target": "Autodesk.Revit.DB.Reference",
 45679|       "member_name": "PointReference",
 45680|       "member_kind": "property",
 45681|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45682|       "confidence": "direct_return_type",
 45683|       "confidence_tier": "unverified_reference",
 45684|       "target_resolution": "exact",
 45685|       "evidence": [
 45686|         "return type 'Reference' directly names a Revit DB object type",
 45687|         "docs text contains relationship phrase: 'depends on'"
 45688|       ],
 45689|       "source_url": "https://www.revitapidocs.com/2025/874e99bf-9567-929d-f67f-4cc2e8358d20.htm",
 45690|       "dll_signature_verified": true,
 45691|       "dll_relationship_scope": "declared",
 45692|       "dll_semantic_verified": null,
 45693|       "dll_verified_status": "signature_verified_declared",
 45694|       "revitlookup_referenced": null,
 45695|       "revitlookup_requires_document_context": null
 45696|     },
 45697|     {
 45698|       "source": "Autodesk.Revit.DB.FamilySizeTable",
 45699|       "target": "Autodesk.Revit.DB.FamilySizeTableColumn",
 45700|       "member_name": "GetColumnHeader",
 45701|       "member_kind": "method",
 45702|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45703|       "confidence": "direct_return_type",
 45704|       "confidence_tier": "unverified_reference",
 45705|       "target_resolution": "exact",
 45706|       "evidence": [
 45707|         "return type 'FamilySizeTableColumn' directly names a Revit DB object type"
 45708|       ],
 45709|       "source_url": "https://www.revitapidocs.com/2025/47c651f5-1306-1f1a-157c-be56737a1b16.htm",
 45710|       "dll_signature_verified": true,
 45711|       "dll_relationship_scope": "declared",
 45712|       "dll_semantic_verified": null,
 45713|       "dll_verified_status": "signature_verified_declared",
 45714|       "revitlookup_referenced": true,
 45715|       "revitlookup_requires_document_context": false
 45716|     },
 45717|     {
 45718|       "source": "Autodesk.Revit.DB.FamilySizeTableManager",
 45719|       "target": null,
 45720|       "member_name": "GetAllSizeTableNames",
 45721|       "member_kind": "method",
 45722|       "edge_type": "RETURNS_ELEMENT_IDS",
 45723|       "confidence": "name_only_candidate",
 45724|       "confidence_tier": "likely",
 45725|       "target_resolution": "none",
 45726|       "evidence": [
 45727|         "member name 'GetAllSizeTableNames' matches keyword pattern /^GetAll/ but return type 'IList < string >' gives no type-level confirmation"
 45728|       ],
 45729|       "source_url": "https://www.revitapidocs.com/2025/e043b624-d464-d0f3-e1c6-3a5bdaaa3238.htm",
 45730|       "dll_signature_verified": true,
 45731|       "dll_relationship_scope": "declared",
 45732|       "dll_semantic_verified": null,
 45733|       "dll_verified_status": "signature_verified_declared",
 45734|       "revitlookup_referenced": null,
 45735|       "revitlookup_requires_document_context": null
 45736|     },
 45737|     {
 45738|       "source": "Autodesk.Revit.DB.FamilySizeTableManager",
 45739|       "target": "Autodesk.Revit.DB.FamilySizeTable",
 45740|       "member_name": "GetSizeTable",
 45741|       "member_kind": "method",
 45742|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45743|       "confidence": "direct_return_type",
 45744|       "confidence_tier": "unverified_reference",
 45745|       "target_resolution": "exact",
 45746|       "evidence": [
 45747|         "return type 'FamilySizeTable' directly names a Revit DB object type"
 45748|       ],
 45749|       "source_url": "https://www.revitapidocs.com/2025/e2fb9f86-e444-67b6-c830-4037206636b4.htm",
 45750|       "dll_signature_verified": true,
 45751|       "dll_relationship_scope": "declared",
 45752|       "dll_semantic_verified": null,
 45753|       "dll_verified_status": "signature_verified_declared",
 45754|       "revitlookup_referenced": true,
 45755|       "revitlookup_requires_document_context": false
 45756|     },
 45757|     {
 45758|       "source": "Autodesk.Revit.DB.FamilySymbol",
 45759|       "target": "Autodesk.Revit.DB.Family",
 45760|       "member_name": "Family",
 45761|       "member_kind": "property",
 45762|       "edge_type": "BELONGS_TO_FAMILY",
 45763|       "confidence": "direct_return_type",
 45764|       "confidence_tier": "core",
 45765|       "target_resolution": "exact",
 45766|       "evidence": [
 45767|         "return type 'Family' directly names a Revit DB object type"
 45768|       ],
 45769|       "source_url": "https://www.revitapidocs.com/2025/14272fe6-5fc3-f053-b4c6-2f44d05786f2.htm",
 45770|       "dll_signature_verified": true,
 45771|       "dll_relationship_scope": "declared",
 45772|       "dll_semantic_verified": null,
 45773|       "dll_verified_status": "signature_verified_declared",
 45774|       "revitlookup_referenced": null,
 45775|       "revitlookup_requires_document_context": null
 45776|     },
 45777|     {
 45778|       "source": "Autodesk.Revit.DB.FamilySymbol",
 45779|       "target": "Autodesk.Revit.DB.Material",
 45780|       "member_name": "StructuralMaterialType",
 45781|       "member_kind": "property",
 45782|       "edge_type": "USES_MATERIAL",
 45783|       "confidence": "name_only_candidate",
 45784|       "confidence_tier": "likely",
 45785|       "target_resolution": "exact",
 45786|       "evidence": [
 45787|         "member name 'StructuralMaterialType' matches keyword pattern /Material/ but return type 'StructuralMaterialType' gives no type-level confirmation"
 45788|       ],
 45789|       "source_url": "https://www.revitapidocs.com/2025/e3e95cd1-6fb6-6c08-b8e7-4e5267d7339e.htm",
 45790|       "dll_signature_verified": true,
 45791|       "dll_relationship_scope": "declared",
 45792|       "dll_semantic_verified": null,
 45793|       "dll_verified_status": "signature_verified_declared",
 45794|       "revitlookup_referenced": null,
 45795|       "revitlookup_requires_document_context": null
 45796|     },
 45797|     {
 45798|       "source": "Autodesk.Revit.DB.FamilySymbol",
 45799|       "target": "Autodesk.Revit.DB.FamilyPointLocation",
 45800|       "member_name": "GetFamilyPointLocations",
 45801|       "member_kind": "method",
 45802|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45803|       "confidence": "needs_runtime_validation",
 45804|       "confidence_tier": "needs_validation",
 45805|       "target_resolution": "exact",
 45806|       "evidence": [
 45807|         "return type 'IList < FamilyPointLocation >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 45808|       ],
 45809|       "source_url": "https://www.revitapidocs.com/2025/b25b0bf1-6c01-846b-8dd2-9576e629d13c.htm",
 45810|       "dll_signature_verified": true,
 45811|       "dll_relationship_scope": "declared",
 45812|       "dll_semantic_verified": null,
 45813|       "dll_verified_status": "signature_verified_declared",
 45814|       "revitlookup_referenced": null,
 45815|       "revitlookup_requires_document_context": null
 45816|     },
 45817|     {
 45818|       "source": "Autodesk.Revit.DB.FamilySymbol",
 45819|       "target": "Autodesk.Revit.DB.Structure.StructuralSections.StructuralSection",
 45820|       "member_name": "GetStructuralSection",
 45821|       "member_kind": "method",
 45822|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45823|       "confidence": "direct_return_type",
 45824|       "confidence_tier": "unverified_reference",
 45825|       "target_resolution": "short_name_fallback",
 45826|       "evidence": [
 45827|         "return type 'StructuralSection' directly names a Revit DB object type"
 45828|       ],
 45829|       "source_url": "https://www.revitapidocs.com/2025/99fb2804-0763-d6d1-13e7-7f49ff85fb68.htm",
 45830|       "dll_signature_verified": true,
 45831|       "dll_relationship_scope": "declared",
 45832|       "dll_semantic_verified": null,
 45833|       "dll_verified_status": "signature_verified_declared",
 45834|       "revitlookup_referenced": null,
 45835|       "revitlookup_requires_document_context": null
 45836|     },
 45837|     {
 45838|       "source": "Autodesk.Revit.DB.FamilySymbol",
 45839|       "target": "Autodesk.Revit.DB.FamilyThermalProperties",
 45840|       "member_name": "GetThermalProperties",
 45841|       "member_kind": "method",
 45842|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45843|       "confidence": "direct_return_type",
 45844|       "confidence_tier": "unverified_reference",
 45845|       "target_resolution": "exact",
 45846|       "evidence": [
 45847|         "return type 'FamilyThermalProperties' directly names a Revit DB object type"
 45848|       ],
 45849|       "source_url": "https://www.revitapidocs.com/2025/ee19d344-addf-af6e-255b-725103cf2bd7.htm",
 45850|       "dll_signature_verified": true,
 45851|       "dll_relationship_scope": "declared",
 45852|       "dll_semantic_verified": null,
 45853|       "dll_verified_status": "signature_verified_declared",
 45854|       "revitlookup_referenced": null,
 45855|       "revitlookup_requires_document_context": null
 45856|     },
 45857|     {
 45858|       "source": "Autodesk.Revit.DB.FamilySymbolFilter",
 45859|       "target": null,
 45860|       "member_name": "FamilyId",
 45861|       "member_kind": "property",
 45862|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 45863|       "confidence": "unknown_reference",
 45864|       "confidence_tier": "unverified_reference",
 45865|       "target_resolution": "none",
 45866|       "evidence": [
 45867|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 45868|       ],
 45869|       "source_url": "https://www.revitapidocs.com/2025/8dad7be1-1e6e-4080-aca6-f2e738a6f6c1.htm",
 45870|       "dll_signature_verified": true,
 45871|       "dll_relationship_scope": "declared",
 45872|       "dll_semantic_verified": null,
 45873|       "dll_verified_status": "signature_verified_declared",
 45874|       "revitlookup_referenced": null,
 45875|       "revitlookup_requires_document_context": null
 45876|     },
 45877|     {
 45878|       "source": "Autodesk.Revit.DB.FamilySymbolProfile",
 45879|       "target": "Autodesk.Revit.DB.FamilySymbol",
 45880|       "member_name": "Profile",
 45881|       "member_kind": "property",
 45882|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45883|       "confidence": "direct_return_type",
 45884|       "confidence_tier": "unverified_reference",
 45885|       "target_resolution": "exact",
 45886|       "evidence": [
 45887|         "return type 'FamilySymbol' directly names a Revit DB object type"
 45888|       ],
 45889|       "source_url": "https://www.revitapidocs.com/2025/15a89854-bc42-7e79-097c-979220a3ad27.htm",
 45890|       "dll_signature_verified": true,
 45891|       "dll_relationship_scope": "declared",
 45892|       "dll_semantic_verified": null,
 45893|       "dll_verified_status": "signature_verified_declared",
 45894|       "revitlookup_referenced": null,
 45895|       "revitlookup_requires_document_context": null
 45896|     },
 45897|     {
 45898|       "source": "Autodesk.Revit.DB.FamilyType",
 45899|       "target": null,
 45900|       "member_name": "AsElementId",
 45901|       "member_kind": "method",
 45902|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 45903|       "confidence": "unknown_reference",
 45904|       "confidence_tier": "unverified_reference",
 45905|       "target_resolution": "none",
 45906|       "evidence": [
 45907|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 45908|       ],
 45909|       "source_url": "https://www.revitapidocs.com/2025/8b31dc65-e397-7e9e-97b8-ed83854c619f.htm",
 45910|       "dll_signature_verified": true,
 45911|       "dll_relationship_scope": "declared",
 45912|       "dll_semantic_verified": null,
 45913|       "dll_verified_status": "signature_verified_declared",
 45914|       "revitlookup_referenced": null,
 45915|       "revitlookup_requires_document_context": null
 45916|     },
 45917|     {
 45918|       "source": "Autodesk.Revit.DB.FamilyTypeSet",
 45919|       "target": "Autodesk.Revit.DB.FamilyTypeSetIterator",
 45920|       "member_name": "ForwardIterator",
 45921|       "member_kind": "method",
 45922|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45923|       "confidence": "direct_return_type",
 45924|       "confidence_tier": "unverified_reference",
 45925|       "target_resolution": "exact",
 45926|       "evidence": [
 45927|         "return type 'FamilyTypeSetIterator' directly names a Revit DB object type"
 45928|       ],
 45929|       "source_url": "https://www.revitapidocs.com/2025/fc8efada-122c-e709-9b60-cd748a4f5fef.htm",
 45930|       "dll_signature_verified": true,
 45931|       "dll_relationship_scope": "declared",
 45932|       "dll_semantic_verified": null,
 45933|       "dll_verified_status": "signature_verified_declared",
 45934|       "revitlookup_referenced": null,
 45935|       "revitlookup_requires_document_context": null
 45936|     },
 45937|     {
 45938|       "source": "Autodesk.Revit.DB.FamilyTypeSet",
 45939|       "target": "Autodesk.Revit.DB.FamilyTypeSetIterator",
 45940|       "member_name": "ReverseIterator",
 45941|       "member_kind": "method",
 45942|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45943|       "confidence": "direct_return_type",
 45944|       "confidence_tier": "unverified_reference",
 45945|       "target_resolution": "exact",
 45946|       "evidence": [
 45947|         "return type 'FamilyTypeSetIterator' directly names a Revit DB object type"
 45948|       ],
 45949|       "source_url": "https://www.revitapidocs.com/2025/39c6ba67-1469-a905-ae18-aaaa1bc615ea.htm",
 45950|       "dll_signature_verified": true,
 45951|       "dll_relationship_scope": "declared",
 45952|       "dll_semantic_verified": null,
 45953|       "dll_verified_status": "signature_verified_declared",
 45954|       "revitlookup_referenced": null,
 45955|       "revitlookup_requires_document_context": null
 45956|     },
 45957|     {
 45958|       "source": "Autodesk.Revit.DB.FamilyUtils",
 45959|       "target": null,
 45960|       "member_name": "ConvertFamilyToFaceHostBased",
 45961|       "member_kind": "method",
 45962|       "edge_type": "HOSTED_BY",
 45963|       "confidence": "name_only_candidate",
 45964|       "confidence_tier": "likely",
 45965|       "target_resolution": "none",
 45966|       "evidence": [
 45967|         "member name 'ConvertFamilyToFaceHostBased' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 45968|       ],
 45969|       "source_url": "https://www.revitapidocs.com/2025/a834b134-c57e-c062-a044-3b5f677537c0.htm",
 45970|       "dll_signature_verified": true,
 45971|       "dll_relationship_scope": "declared",
 45972|       "dll_semantic_verified": null,
 45973|       "dll_verified_status": "signature_verified_declared",
 45974|       "revitlookup_referenced": null,
 45975|       "revitlookup_requires_document_context": null
 45976|     },
 45977|     {
 45978|       "source": "Autodesk.Revit.DB.FamilyUtils",
 45979|       "target": null,
 45980|       "member_name": "FamilyCanConvertToFaceHostBased",
 45981|       "member_kind": "method",
 45982|       "edge_type": "HOSTED_BY",
 45983|       "confidence": "name_only_candidate",
 45984|       "confidence_tier": "likely",
 45985|       "target_resolution": "none",
 45986|       "evidence": [
 45987|         "member name 'FamilyCanConvertToFaceHostBased' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 45988|       ],
 45989|       "source_url": "https://www.revitapidocs.com/2025/624b1f01-0d87-d1a3-192c-620916279406.htm",
 45990|       "dll_signature_verified": true,
 45991|       "dll_relationship_scope": "declared",
 45992|       "dll_semantic_verified": null,
 45993|       "dll_verified_status": "signature_verified_declared",
 45994|       "revitlookup_referenced": null,
 45995|       "revitlookup_requires_document_context": null
 45996|     },
 45997|     {
 45998|       "source": "Autodesk.Revit.DB.FamilyUtils",
 45999|       "target": null,
 46000|       "member_name": "GetProfileSymbols",
 46001|       "member_kind": "method",
 46002|       "edge_type": "RETURNS_ELEMENT_IDS",
 46003|       "confidence": "unknown_reference",
 46004|       "confidence_tier": "unverified_reference",
 46005|       "target_resolution": "none",
 46006|       "evidence": [
 46007|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 46008|       ],
 46009|       "source_url": "https://www.revitapidocs.com/2025/804d7710-829a-4ad9-13ab-fbb5650bfe77.htm",
 46010|       "dll_signature_verified": true,
 46011|       "dll_relationship_scope": "declared",
 46012|       "dll_semantic_verified": null,
 46013|       "dll_verified_status": "signature_verified_declared",
 46014|       "revitlookup_referenced": null,
 46015|       "revitlookup_requires_document_context": null
 46016|     },
 46017|     {
 46018|       "source": "Autodesk.Revit.DB.FBXExportOptions",
 46019|       "target": "Autodesk.Revit.DB.Level",
 46020|       "member_name": "LevelsOfDetailValue",
 46021|       "member_kind": "property",
 46022|       "edge_type": "ASSIGNED_TO_LEVEL",
 46023|       "confidence": "name_only_candidate",
 46024|       "confidence_tier": "likely",
 46025|       "target_resolution": "exact",
 46026|       "evidence": [
 46027|         "member name 'LevelsOfDetailValue' matches keyword pattern /Level/ but return type 'int' gives no type-level confirmation"
 46028|       ],
 46029|       "source_url": "https://www.revitapidocs.com/2025/0e1686f0-8a05-eeb3-dd44-9d26e0ca8a09.htm",
 46030|       "dll_signature_verified": true,
```
