# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 195 of 216
- Original line range: 75661-76060
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 75661|       "dll_relationship_scope": "declared",
 75662|       "dll_semantic_verified": null,
 75663|       "dll_verified_status": "signature_verified_declared",
 75664|       "revitlookup_referenced": null,
 75665|       "revitlookup_requires_document_context": null
 75666|     },
 75667|     {
 75668|       "source": "Autodesk.Revit.DB.Mechanical.Duct",
 75669|       "target": "Autodesk.Revit.DB.Mechanical.DuctType",
 75670|       "member_name": "DuctType",
 75671|       "member_kind": "property",
 75672|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75673|       "confidence": "direct_return_type",
 75674|       "confidence_tier": "unverified_reference",
 75675|       "target_resolution": "short_name_fallback",
 75676|       "evidence": [
 75677|         "return type 'DuctType' directly names a Revit DB object type"
 75678|       ],
 75679|       "source_url": "https://www.revitapidocs.com/2025/50115563-4651-8c1d-18e2-01fe46b2185d.htm",
 75680|       "dll_signature_verified": true,
 75681|       "dll_relationship_scope": "declared",
 75682|       "dll_semantic_verified": null,
 75683|       "dll_verified_status": "signature_verified_declared",
 75684|       "revitlookup_referenced": null,
 75685|       "revitlookup_requires_document_context": null
 75686|     },
 75687|     {
 75688|       "source": "Autodesk.Revit.DB.Mechanical.DuctFittingAndAccessoryData",
 75689|       "target": "Autodesk.Revit.DB.Mechanical.DuctFittingAndAccessoryConnectorData",
 75690|       "member_name": "GetAllConnectorData",
 75691|       "member_kind": "method",
 75692|       "edge_type": "RETURNS_ELEMENT_IDS",
 75693|       "confidence": "needs_runtime_validation",
 75694|       "confidence_tier": "needs_validation",
 75695|       "target_resolution": "short_name_fallback",
 75696|       "evidence": [
 75697|         "return type 'IList < DuctFittingAndAccessoryConnectorData >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 75698|       ],
 75699|       "source_url": "https://www.revitapidocs.com/2025/6a6fd6cc-325d-4d44-6e08-309cdc81ef42.htm",
 75700|       "dll_signature_verified": true,
 75701|       "dll_relationship_scope": "declared",
 75702|       "dll_semantic_verified": null,
 75703|       "dll_verified_status": "signature_verified_declared",
 75704|       "revitlookup_referenced": null,
 75705|       "revitlookup_requires_document_context": null
 75706|     },
 75707|     {
 75708|       "source": "Autodesk.Revit.DB.Mechanical.DuctFittingAndAccessoryData",
 75709|       "target": "Autodesk.Revit.DB.ExtensibleStorage.Entity",
 75710|       "member_name": "GetEntity",
 75711|       "member_kind": "method",
 75712|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75713|       "confidence": "direct_return_type",
 75714|       "confidence_tier": "unverified_reference",
 75715|       "target_resolution": "short_name_fallback",
 75716|       "evidence": [
 75717|         "return type 'Entity' directly names a Revit DB object type"
 75718|       ],
 75719|       "source_url": "https://www.revitapidocs.com/2025/c1e1344a-74d7-fd84-877f-e4513270e61c.htm",
 75720|       "dll_signature_verified": true,
 75721|       "dll_relationship_scope": "declared",
 75722|       "dll_semantic_verified": null,
 75723|       "dll_verified_status": "signature_verified_declared",
 75724|       "revitlookup_referenced": null,
 75725|       "revitlookup_requires_document_context": null
 75726|     },
 75727|     {
 75728|       "source": "Autodesk.Revit.DB.Mechanical.DuctFittingAndAccessoryData",
 75729|       "target": null,
 75730|       "member_name": "GetFamilyInstanceId",
 75731|       "member_kind": "method",
 75732|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 75733|       "confidence": "unknown_reference",
 75734|       "confidence_tier": "unverified_reference",
 75735|       "target_resolution": "none",
 75736|       "evidence": [
 75737|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 75738|       ],
 75739|       "source_url": "https://www.revitapidocs.com/2025/b219f66a-497c-b7ca-a1fa-6cf36287b7a4.htm",
 75740|       "dll_signature_verified": true,
 75741|       "dll_relationship_scope": "declared",
 75742|       "dll_semantic_verified": null,
 75743|       "dll_verified_status": "signature_verified_declared",
 75744|       "revitlookup_referenced": null,
 75745|       "revitlookup_requires_document_context": null
 75746|     },
 75747|     {
 75748|       "source": "Autodesk.Revit.DB.Mechanical.DuctFittingAndAccessoryPressureDropData",
 75749|       "target": "Autodesk.Revit.DB.Mechanical.DuctFittingAndAccessoryData",
 75750|       "member_name": "GetDuctFittingAndAccessoryData",
 75751|       "member_kind": "method",
 75752|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75753|       "confidence": "direct_return_type",
 75754|       "confidence_tier": "unverified_reference",
 75755|       "target_resolution": "short_name_fallback",
 75756|       "evidence": [
 75757|         "return type 'DuctFittingAndAccessoryData' directly names a Revit DB object type"
 75758|       ],
 75759|       "source_url": "https://www.revitapidocs.com/2025/c4cf6998-009a-cf51-535d-53c61177fb6e.htm",
 75760|       "dll_signature_verified": true,
 75761|       "dll_relationship_scope": "declared",
 75762|       "dll_semantic_verified": null,
 75763|       "dll_verified_status": "signature_verified_declared",
 75764|       "revitlookup_referenced": null,
 75765|       "revitlookup_requires_document_context": null
 75766|     },
 75767|     {
 75768|       "source": "Autodesk.Revit.DB.Mechanical.DuctFittingAndAccessoryPressureDropData",
 75769|       "target": "Autodesk.Revit.DB.Mechanical.DuctFittingAndAccessoryPressureDropItem",
 75770|       "member_name": "GetPresureDropItems",
 75771|       "member_kind": "method",
 75772|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75773|       "confidence": "needs_runtime_validation",
 75774|       "confidence_tier": "needs_validation",
 75775|       "target_resolution": "short_name_fallback",
 75776|       "evidence": [
 75777|         "return type 'IList < DuctFittingAndAccessoryPressureDropItem >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 75778|       ],
 75779|       "source_url": "https://www.revitapidocs.com/2025/3d16af11-f577-0496-4281-733064dc330a.htm",
 75780|       "dll_signature_verified": true,
 75781|       "dll_relationship_scope": "declared",
 75782|       "dll_semantic_verified": null,
 75783|       "dll_verified_status": "signature_verified_declared",
 75784|       "revitlookup_referenced": null,
 75785|       "revitlookup_requires_document_context": null
 75786|     },
 75787|     {
 75788|       "source": "Autodesk.Revit.DB.Mechanical.DuctPressureDropData",
 75789|       "target": "Autodesk.Revit.DB.Category",
 75790|       "member_name": "CategoryId",
 75791|       "member_kind": "property",
 75792|       "edge_type": "HAS_CATEGORY",
 75793|       "confidence": "elementid_with_strong_name",
 75794|       "confidence_tier": "core",
 75795|       "target_resolution": "exact",
 75796|       "evidence": [
 75797|         "member name 'CategoryId' matches keyword pattern /Category/"
 75798|       ],
 75799|       "source_url": "https://www.revitapidocs.com/2025/facb8291-71f9-a9b5-4abf-6389a5401136.htm",
 75800|       "dll_signature_verified": true,
 75801|       "dll_relationship_scope": "declared",
 75802|       "dll_semantic_verified": null,
 75803|       "dll_verified_status": "signature_verified_declared",
 75804|       "revitlookup_referenced": null,
 75805|       "revitlookup_requires_document_context": null
 75806|     },
 75807|     {
 75808|       "source": "Autodesk.Revit.DB.Mechanical.DuctPressureDropData",
 75809|       "target": "Autodesk.Revit.DB.Level",
 75810|       "member_name": "Level",
 75811|       "member_kind": "property",
 75812|       "edge_type": "ASSIGNED_TO_LEVEL",
 75813|       "confidence": "name_only_candidate",
 75814|       "confidence_tier": "likely",
 75815|       "target_resolution": "exact",
 75816|       "evidence": [
 75817|         "member name 'Level' matches keyword pattern /Level/ but return type 'SystemCalculationLevel' gives no type-level confirmation"
 75818|       ],
 75819|       "source_url": "https://www.revitapidocs.com/2025/4c461eeb-7be6-86a9-d81d-9f35873c0a78.htm",
 75820|       "dll_signature_verified": true,
 75821|       "dll_relationship_scope": "declared",
 75822|       "dll_semantic_verified": null,
 75823|       "dll_verified_status": "signature_verified_declared",
 75824|       "revitlookup_referenced": null,
 75825|       "revitlookup_requires_document_context": null
 75826|     },
 75827|     {
 75828|       "source": "Autodesk.Revit.DB.Mechanical.DuctSettings",
 75829|       "target": "Autodesk.Revit.DB.MEPCalculationServerInfo",
 75830|       "member_name": "GetPressLossCalculationServerInfo",
 75831|       "member_kind": "method",
 75832|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75833|       "confidence": "direct_return_type",
 75834|       "confidence_tier": "unverified_reference",
 75835|       "target_resolution": "exact",
 75836|       "evidence": [
 75837|         "return type 'MEPCalculationServerInfo' directly names a Revit DB object type"
 75838|       ],
 75839|       "source_url": "https://www.revitapidocs.com/2025/301d0233-9033-7ed8-9a66-aaa55c63ddfd.htm",
 75840|       "dll_signature_verified": true,
 75841|       "dll_relationship_scope": "declared",
 75842|       "dll_semantic_verified": null,
 75843|       "dll_verified_status": "signature_verified_declared",
 75844|       "revitlookup_referenced": null,
 75845|       "revitlookup_requires_document_context": null
 75846|     },
 75847|     {
 75848|       "source": "Autodesk.Revit.DB.Mechanical.DuctSizeIterator",
 75849|       "target": "Autodesk.Revit.DB.MEPSize",
 75850|       "member_name": "Current",
 75851|       "member_kind": "property",
 75852|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75853|       "confidence": "direct_return_type",
 75854|       "confidence_tier": "unverified_reference",
 75855|       "target_resolution": "exact",
 75856|       "evidence": [
 75857|         "return type 'MEPSize' directly names a Revit DB object type"
 75858|       ],
 75859|       "source_url": "https://www.revitapidocs.com/2025/d1620fdc-e262-fc9e-16e9-dc1e7f2b03dc.htm",
 75860|       "dll_signature_verified": true,
 75861|       "dll_relationship_scope": "declared",
 75862|       "dll_semantic_verified": null,
 75863|       "dll_verified_status": "signature_verified_declared",
 75864|       "revitlookup_referenced": null,
 75865|       "revitlookup_requires_document_context": null
 75866|     },
 75867|     {
 75868|       "source": "Autodesk.Revit.DB.Mechanical.DuctSizeIterator",
 75869|       "target": "Autodesk.Revit.DB.MEPSize",
 75870|       "member_name": "GetCurrent",
 75871|       "member_kind": "method",
 75872|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75873|       "confidence": "direct_return_type",
 75874|       "confidence_tier": "unverified_reference",
 75875|       "target_resolution": "exact",
 75876|       "evidence": [
 75877|         "return type 'MEPSize' directly names a Revit DB object type"
 75878|       ],
 75879|       "source_url": "https://www.revitapidocs.com/2025/a9b33b14-5bdd-cdf1-4964-47014ec49703.htm",
 75880|       "dll_signature_verified": true,
 75881|       "dll_relationship_scope": "declared",
 75882|       "dll_semantic_verified": null,
 75883|       "dll_verified_status": "signature_verified_declared",
 75884|       "revitlookup_referenced": null,
 75885|       "revitlookup_requires_document_context": null
 75886|     },
 75887|     {
 75888|       "source": "Autodesk.Revit.DB.Mechanical.DuctSizes",
 75889|       "target": "Autodesk.Revit.DB.Mechanical.DuctSizeIterator",
 75890|       "member_name": "GetDuctSizeIterator",
 75891|       "member_kind": "method",
 75892|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75893|       "confidence": "direct_return_type",
 75894|       "confidence_tier": "unverified_reference",
 75895|       "target_resolution": "short_name_fallback",
 75896|       "evidence": [
 75897|         "return type 'DuctSizeIterator' directly names a Revit DB object type"
 75898|       ],
 75899|       "source_url": "https://www.revitapidocs.com/2025/af6c24af-10d9-6ad7-19f9-7bac13bbc9e2.htm",
 75900|       "dll_signature_verified": true,
 75901|       "dll_relationship_scope": "declared",
 75902|       "dll_semantic_verified": null,
 75903|       "dll_verified_status": "signature_verified_declared",
 75904|       "revitlookup_referenced": null,
 75905|       "revitlookup_requires_document_context": null
 75906|     },
 75907|     {
 75908|       "source": "Autodesk.Revit.DB.Mechanical.DuctSizeSettings",
 75909|       "target": "Autodesk.Revit.DB.Mechanical.DuctSizeSettingIterator",
 75910|       "member_name": "GetDuctSizeSettingIterator",
 75911|       "member_kind": "method",
 75912|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75913|       "confidence": "direct_return_type",
 75914|       "confidence_tier": "unverified_reference",
 75915|       "target_resolution": "short_name_fallback",
 75916|       "evidence": [
 75917|         "return type 'DuctSizeSettingIterator' directly names a Revit DB object type"
 75918|       ],
 75919|       "source_url": "https://www.revitapidocs.com/2025/124c6b6d-9730-696e-1e96-66c3159512aa.htm",
 75920|       "dll_signature_verified": true,
 75921|       "dll_relationship_scope": "declared",
 75922|       "dll_semantic_verified": null,
 75923|       "dll_verified_status": "signature_verified_declared",
 75924|       "revitlookup_referenced": null,
 75925|       "revitlookup_requires_document_context": null
 75926|     },
 75927|     {
 75928|       "source": "Autodesk.Revit.DB.Mechanical.FlexDuct",
 75929|       "target": "Autodesk.Revit.DB.Mechanical.FlexDuctType",
 75930|       "member_name": "FlexDuctType",
 75931|       "member_kind": "property",
 75932|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75933|       "confidence": "direct_return_type",
 75934|       "confidence_tier": "unverified_reference",
 75935|       "target_resolution": "short_name_fallback",
 75936|       "evidence": [
 75937|         "return type 'FlexDuctType' directly names a Revit DB object type"
 75938|       ],
 75939|       "source_url": "https://www.revitapidocs.com/2025/67ccc28b-dcf5-aa69-44c1-5a4da62e260a.htm",
 75940|       "dll_signature_verified": true,
 75941|       "dll_relationship_scope": "declared",
 75942|       "dll_semantic_verified": null,
 75943|       "dll_verified_status": "signature_verified_declared",
 75944|       "revitlookup_referenced": null,
 75945|       "revitlookup_requires_document_context": null
 75946|     },
 75947|     {
 75948|       "source": "Autodesk.Revit.DB.Mechanical.IDuctFittingAndAccessoryPressureDropServer",
 75949|       "target": "Autodesk.Revit.DB.ExtensibleStorage.Schema",
 75950|       "member_name": "GetDataSchema",
 75951|       "member_kind": "method",
 75952|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75953|       "confidence": "direct_return_type",
 75954|       "confidence_tier": "unverified_reference",
 75955|       "target_resolution": "exact",
 75956|       "evidence": [
 75957|         "return type 'Schema' directly names a Revit DB object type"
 75958|       ],
 75959|       "source_url": "https://www.revitapidocs.com/2025/f7f4eb89-44d4-adef-99f5-4220c310eed6.htm",
 75960|       "dll_signature_verified": true,
 75961|       "dll_relationship_scope": "declared",
 75962|       "dll_semantic_verified": null,
 75963|       "dll_verified_status": "signature_verified_declared",
 75964|       "revitlookup_referenced": null,
 75965|       "revitlookup_requires_document_context": null
 75966|     },
 75967|     {
 75968|       "source": "Autodesk.Revit.DB.Mechanical.MechanicalEquipmentSet",
 75969|       "target": null,
 75970|       "member_name": "GetMembers",
 75971|       "member_kind": "method",
 75972|       "edge_type": "MEMBER_OF_GROUP",
 75973|       "confidence": "elementid_collection_with_strong_name",
 75974|       "confidence_tier": "core",
 75975|       "target_resolution": "none",
 75976|       "evidence": [
 75977|         "member name 'GetMembers' matches keyword pattern /^GetMember|Group/"
 75978|       ],
 75979|       "source_url": "https://www.revitapidocs.com/2025/6190ec28-6fa3-439e-3d32-11f5f330d819.htm",
 75980|       "dll_signature_verified": true,
 75981|       "dll_relationship_scope": "declared",
 75982|       "dll_semantic_verified": null,
 75983|       "dll_verified_status": "signature_verified_declared",
 75984|       "revitlookup_referenced": null,
 75985|       "revitlookup_requires_document_context": null
 75986|     },
 75987|     {
 75988|       "source": "Autodesk.Revit.DB.Mechanical.MechanicalSystem",
 75989|       "target": "Autodesk.Revit.DB.Connector",
 75990|       "member_name": "BaseEquipmentConnector",
 75991|       "member_kind": "property",
 75992|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75993|       "confidence": "direct_return_type",
 75994|       "confidence_tier": "unverified_reference",
 75995|       "target_resolution": "exact",
 75996|       "evidence": [
 75997|         "return type 'Connector' directly names a Revit DB object type"
 75998|       ],
 75999|       "source_url": "https://www.revitapidocs.com/2025/0aa90e49-871d-89e5-4af0-c20472df9729.htm",
 76000|       "dll_signature_verified": true,
 76001|       "dll_relationship_scope": "declared",
 76002|       "dll_semantic_verified": null,
 76003|       "dll_verified_status": "signature_verified_declared",
 76004|       "revitlookup_referenced": null,
 76005|       "revitlookup_requires_document_context": null
 76006|     },
 76007|     {
 76008|       "source": "Autodesk.Revit.DB.Mechanical.MechanicalSystem",
 76009|       "target": "Autodesk.Revit.DB.ElementSet",
 76010|       "member_name": "DuctNetwork",
 76011|       "member_kind": "property",
 76012|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76013|       "confidence": "direct_return_type",
 76014|       "confidence_tier": "unverified_reference",
 76015|       "target_resolution": "exact",
 76016|       "evidence": [
 76017|         "return type 'ElementSet' directly names a Revit DB object type"
 76018|       ],
 76019|       "source_url": "https://www.revitapidocs.com/2025/93b81f99-e92e-d6ab-f459-eeb71717e809.htm",
 76020|       "dll_signature_verified": true,
 76021|       "dll_relationship_scope": "declared",
 76022|       "dll_semantic_verified": null,
 76023|       "dll_verified_status": "signature_verified_declared",
 76024|       "revitlookup_referenced": null,
 76025|       "revitlookup_requires_document_context": null
 76026|     },
 76027|     {
 76028|       "source": "Autodesk.Revit.DB.Mechanical.MechanicalUtils",
 76029|       "target": null,
 76030|       "member_name": "BreakCurve",
 76031|       "member_kind": "method",
 76032|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 76033|       "confidence": "unknown_reference",
 76034|       "confidence_tier": "unverified_reference",
 76035|       "target_resolution": "none",
 76036|       "evidence": [
 76037|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 76038|       ],
 76039|       "source_url": "https://www.revitapidocs.com/2025/baeec9be-b43d-d378-31b9-453432d44bfb.htm",
 76040|       "dll_signature_verified": true,
 76041|       "dll_relationship_scope": "declared",
 76042|       "dll_semantic_verified": null,
 76043|       "dll_verified_status": "signature_verified_declared",
 76044|       "revitlookup_referenced": null,
 76045|       "revitlookup_requires_document_context": null
 76046|     },
 76047|     {
 76048|       "source": "Autodesk.Revit.DB.Mechanical.MechanicalUtils",
 76049|       "target": null,
 76050|       "member_name": "ConvertDuctPlaceholders",
 76051|       "member_kind": "method",
 76052|       "edge_type": "RETURNS_ELEMENT_IDS",
 76053|       "confidence": "unknown_reference",
 76054|       "confidence_tier": "unverified_reference",
 76055|       "target_resolution": "none",
 76056|       "evidence": [
 76057|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 76058|       ],
 76059|       "source_url": "https://www.revitapidocs.com/2025/8305d265-b824-98d7-2084-8a8eb0c49208.htm",
 76060|       "dll_signature_verified": true,
```
