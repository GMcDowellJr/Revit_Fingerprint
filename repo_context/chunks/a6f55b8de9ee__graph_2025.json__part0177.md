# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 177 of 216
- Original line range: 68641-69040
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 68641|       "member_kind": "property",
 68642|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 68643|       "confidence": "unknown_reference",
 68644|       "confidence_tier": "unverified_reference",
 68645|       "target_resolution": "none",
 68646|       "evidence": [
 68647|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 68648|       ],
 68649|       "source_url": "https://www.revitapidocs.com/2025/b6454a01-0ce3-b32d-8c7b-7359644386b3.htm",
 68650|       "dll_signature_verified": true,
 68651|       "dll_relationship_scope": "declared",
 68652|       "dll_semantic_verified": null,
 68653|       "dll_verified_status": "signature_verified_declared",
 68654|       "revitlookup_referenced": null,
 68655|       "revitlookup_requires_document_context": null
 68656|     },
 68657|     {
 68658|       "source": "Autodesk.Revit.DB.Architecture.StairsLanding",
 68659|       "target": null,
 68660|       "member_name": "GetAllSupports",
 68661|       "member_kind": "method",
 68662|       "edge_type": "RETURNS_ELEMENT_IDS",
 68663|       "confidence": "elementid_collection_with_strong_name",
 68664|       "confidence_tier": "core",
 68665|       "target_resolution": "none",
 68666|       "evidence": [
 68667|         "member name 'GetAllSupports' matches keyword pattern /^GetAll/"
 68668|       ],
 68669|       "source_url": "https://www.revitapidocs.com/2025/0aeb598f-bc73-c12a-20bd-29911c89a1bd.htm",
 68670|       "dll_signature_verified": true,
 68671|       "dll_relationship_scope": "declared",
 68672|       "dll_semantic_verified": null,
 68673|       "dll_verified_status": "signature_verified_declared",
 68674|       "revitlookup_referenced": null,
 68675|       "revitlookup_requires_document_context": null
 68676|     },
 68677|     {
 68678|       "source": "Autodesk.Revit.DB.Architecture.StairsLanding",
 68679|       "target": "Autodesk.Revit.DB.Architecture.StairsComponentConnection",
 68680|       "member_name": "GetConnections",
 68681|       "member_kind": "method",
 68682|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 68683|       "confidence": "needs_runtime_validation",
 68684|       "confidence_tier": "needs_validation",
 68685|       "target_resolution": "short_name_fallback",
 68686|       "evidence": [
 68687|         "return type 'IList < StairsComponentConnection >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 68688|       ],
 68689|       "source_url": "https://www.revitapidocs.com/2025/0a80d27c-a959-a712-6298-de5410502ed8.htm",
 68690|       "dll_signature_verified": true,
 68691|       "dll_relationship_scope": "declared",
 68692|       "dll_semantic_verified": null,
 68693|       "dll_verified_status": "signature_verified_declared",
 68694|       "revitlookup_referenced": null,
 68695|       "revitlookup_requires_document_context": null
 68696|     },
 68697|     {
 68698|       "source": "Autodesk.Revit.DB.Architecture.StairsLanding",
 68699|       "target": "Autodesk.Revit.DB.Architecture.Stairs",
 68700|       "member_name": "GetStairs",
 68701|       "member_kind": "method",
 68702|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 68703|       "confidence": "direct_return_type",
 68704|       "confidence_tier": "unverified_reference",
 68705|       "target_resolution": "short_name_fallback",
 68706|       "evidence": [
 68707|         "return type 'Stairs' directly names a Revit DB object type"
 68708|       ],
 68709|       "source_url": "https://www.revitapidocs.com/2025/b3f42d91-f8ea-ea09-fe74-d4fc9798343c.htm",
 68710|       "dll_signature_verified": true,
 68711|       "dll_relationship_scope": "declared",
 68712|       "dll_semantic_verified": null,
 68713|       "dll_verified_status": "signature_verified_declared",
 68714|       "revitlookup_referenced": null,
 68715|       "revitlookup_requires_document_context": null
 68716|     },
 68717|     {
 68718|       "source": "Autodesk.Revit.DB.Architecture.StairsPath",
 68719|       "target": null,
 68720|       "member_name": "StairsId",
 68721|       "member_kind": "property",
 68722|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 68723|       "confidence": "unknown_reference",
 68724|       "confidence_tier": "unverified_reference",
 68725|       "target_resolution": "none",
 68726|       "evidence": [
 68727|         "return type is 'LinkElementId', an ID wrapper, but member name gives no strong hint of the target type"
 68728|       ],
 68729|       "source_url": "https://www.revitapidocs.com/2025/63edfbf5-ef20-7a6a-5fea-208ce5cf4e20.htm",
 68730|       "dll_signature_verified": true,
 68731|       "dll_relationship_scope": "declared",
 68732|       "dll_semantic_verified": null,
 68733|       "dll_verified_status": "signature_verified_declared",
 68734|       "revitlookup_referenced": null,
 68735|       "revitlookup_requires_document_context": null
 68736|     },
 68737|     {
 68738|       "source": "Autodesk.Revit.DB.Architecture.StairsPathType",
 68739|       "target": null,
 68740|       "member_name": "ArrowheadTypeId",
 68741|       "member_kind": "property",
 68742|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 68743|       "confidence": "unknown_reference",
 68744|       "confidence_tier": "unverified_reference",
 68745|       "target_resolution": "none",
 68746|       "evidence": [
 68747|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 68748|       ],
 68749|       "source_url": "https://www.revitapidocs.com/2025/3c6c3cb9-9fa0-ecbf-c6a3-1ef77d783708.htm",
 68750|       "dll_signature_verified": true,
 68751|       "dll_relationship_scope": "declared",
 68752|       "dll_semantic_verified": null,
 68753|       "dll_verified_status": "signature_verified_declared",
 68754|       "revitlookup_referenced": null,
 68755|       "revitlookup_requires_document_context": null
 68756|     },
 68757|     {
 68758|       "source": "Autodesk.Revit.DB.Architecture.StairsPathType",
 68759|       "target": null,
 68760|       "member_name": "StartSymbolTypeId",
 68761|       "member_kind": "property",
 68762|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 68763|       "confidence": "unknown_reference",
 68764|       "confidence_tier": "unverified_reference",
 68765|       "target_resolution": "none",
 68766|       "evidence": [
 68767|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 68768|       ],
 68769|       "source_url": "https://www.revitapidocs.com/2025/9d3eb283-4348-8330-99f9-6440074e7aaf.htm",
 68770|       "dll_signature_verified": true,
 68771|       "dll_relationship_scope": "declared",
 68772|       "dll_semantic_verified": null,
 68773|       "dll_verified_status": "signature_verified_declared",
 68774|       "revitlookup_referenced": null,
 68775|       "revitlookup_requires_document_context": null
 68776|     },
 68777|     {
 68778|       "source": "Autodesk.Revit.DB.Architecture.StairsRun",
 68779|       "target": null,
 68780|       "member_name": "GetAllSupports",
 68781|       "member_kind": "method",
 68782|       "edge_type": "RETURNS_ELEMENT_IDS",
 68783|       "confidence": "elementid_collection_with_strong_name",
 68784|       "confidence_tier": "core",
 68785|       "target_resolution": "none",
 68786|       "evidence": [
 68787|         "member name 'GetAllSupports' matches keyword pattern /^GetAll/",
 68788|         "docs text contains relationship phrase: 'hosted by'"
 68789|       ],
 68790|       "source_url": "https://www.revitapidocs.com/2025/a860f418-9de2-3600-5105-a48be8544a41.htm",
 68791|       "dll_signature_verified": true,
 68792|       "dll_relationship_scope": "declared",
 68793|       "dll_semantic_verified": null,
 68794|       "dll_verified_status": "signature_verified_declared",
 68795|       "revitlookup_referenced": null,
 68796|       "revitlookup_requires_document_context": null
 68797|     },
 68798|     {
 68799|       "source": "Autodesk.Revit.DB.Architecture.StairsRun",
 68800|       "target": "Autodesk.Revit.DB.Architecture.StairsComponentConnection",
 68801|       "member_name": "GetConnections",
 68802|       "member_kind": "method",
 68803|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 68804|       "confidence": "needs_runtime_validation",
 68805|       "confidence_tier": "needs_validation",
 68806|       "target_resolution": "short_name_fallback",
 68807|       "evidence": [
 68808|         "return type 'IList < StairsComponentConnection >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 68809|       ],
 68810|       "source_url": "https://www.revitapidocs.com/2025/db619f61-5444-37f9-1057-97d35caf5eed.htm",
 68811|       "dll_signature_verified": true,
 68812|       "dll_relationship_scope": "declared",
 68813|       "dll_semantic_verified": null,
 68814|       "dll_verified_status": "signature_verified_declared",
 68815|       "revitlookup_referenced": null,
 68816|       "revitlookup_requires_document_context": null
 68817|     },
 68818|     {
 68819|       "source": "Autodesk.Revit.DB.Architecture.StairsRun",
 68820|       "target": null,
 68821|       "member_name": "GetLeftSupports",
 68822|       "member_kind": "method",
 68823|       "edge_type": "RETURNS_ELEMENT_IDS",
 68824|       "confidence": "unknown_reference",
 68825|       "confidence_tier": "unverified_reference",
 68826|       "target_resolution": "none",
 68827|       "evidence": [
 68828|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 68829|       ],
 68830|       "source_url": "https://www.revitapidocs.com/2025/030d4d7a-26c7-c8ec-a924-d15fa74938d3.htm",
 68831|       "dll_signature_verified": true,
 68832|       "dll_relationship_scope": "declared",
 68833|       "dll_semantic_verified": null,
 68834|       "dll_verified_status": "signature_verified_declared",
 68835|       "revitlookup_referenced": null,
 68836|       "revitlookup_requires_document_context": null
 68837|     },
 68838|     {
 68839|       "source": "Autodesk.Revit.DB.Architecture.StairsRun",
 68840|       "target": "Autodesk.Revit.DB.Reference",
 68841|       "member_name": "GetNumberSystemReference",
 68842|       "member_kind": "method",
 68843|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 68844|       "confidence": "direct_return_type",
 68845|       "confidence_tier": "unverified_reference",
 68846|       "target_resolution": "exact",
 68847|       "evidence": [
 68848|         "return type 'Reference' directly names a Revit DB object type"
 68849|       ],
 68850|       "source_url": "https://www.revitapidocs.com/2025/37d8b848-520b-47d1-5cad-f5c4e41c5979.htm",
 68851|       "dll_signature_verified": true,
 68852|       "dll_relationship_scope": "declared",
 68853|       "dll_semantic_verified": null,
 68854|       "dll_verified_status": "signature_verified_declared",
 68855|       "revitlookup_referenced": null,
 68856|       "revitlookup_requires_document_context": null
 68857|     },
 68858|     {
 68859|       "source": "Autodesk.Revit.DB.Architecture.StairsRun",
 68860|       "target": null,
 68861|       "member_name": "GetRightSupports",
 68862|       "member_kind": "method",
 68863|       "edge_type": "RETURNS_ELEMENT_IDS",
 68864|       "confidence": "unknown_reference",
 68865|       "confidence_tier": "unverified_reference",
 68866|       "target_resolution": "none",
 68867|       "evidence": [
 68868|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 68869|       ],
 68870|       "source_url": "https://www.revitapidocs.com/2025/d28ad844-a878-5953-df59-5f7f092207ea.htm",
 68871|       "dll_signature_verified": true,
 68872|       "dll_relationship_scope": "declared",
 68873|       "dll_semantic_verified": null,
 68874|       "dll_verified_status": "signature_verified_declared",
 68875|       "revitlookup_referenced": null,
 68876|       "revitlookup_requires_document_context": null
 68877|     },
 68878|     {
 68879|       "source": "Autodesk.Revit.DB.Architecture.StairsRun",
 68880|       "target": "Autodesk.Revit.DB.Architecture.Stairs",
 68881|       "member_name": "GetStairs",
 68882|       "member_kind": "method",
 68883|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 68884|       "confidence": "direct_return_type",
 68885|       "confidence_tier": "unverified_reference",
 68886|       "target_resolution": "short_name_fallback",
 68887|       "evidence": [
 68888|         "return type 'Stairs' directly names a Revit DB object type"
 68889|       ],
 68890|       "source_url": "https://www.revitapidocs.com/2025/70bc6ef7-27a5-b1a6-aada-7ffedfbf0260.htm",
 68891|       "dll_signature_verified": true,
 68892|       "dll_relationship_scope": "declared",
 68893|       "dll_semantic_verified": null,
 68894|       "dll_verified_status": "signature_verified_declared",
 68895|       "revitlookup_referenced": null,
 68896|       "revitlookup_requires_document_context": null
 68897|     },
 68898|     {
 68899|       "source": "Autodesk.Revit.DB.Architecture.StairsRunType",
 68900|       "target": "Autodesk.Revit.DB.Material",
 68901|       "member_name": "MaterialId",
 68902|       "member_kind": "property",
 68903|       "edge_type": "USES_MATERIAL",
 68904|       "confidence": "elementid_with_strong_name",
 68905|       "confidence_tier": "core",
 68906|       "target_resolution": "exact",
 68907|       "evidence": [
 68908|         "member name 'MaterialId' matches keyword pattern /Material/"
 68909|       ],
 68910|       "source_url": "https://www.revitapidocs.com/2025/f8b6de5f-2765-ad23-a271-ce65005b79e0.htm",
 68911|       "dll_signature_verified": true,
 68912|       "dll_relationship_scope": "declared",
 68913|       "dll_semantic_verified": null,
 68914|       "dll_verified_status": "signature_verified_declared",
 68915|       "revitlookup_referenced": null,
 68916|       "revitlookup_requires_document_context": null
 68917|     },
 68918|     {
 68919|       "source": "Autodesk.Revit.DB.Architecture.StairsRunType",
 68920|       "target": null,
 68921|       "member_name": "NosingProfile",
 68922|       "member_kind": "property",
 68923|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 68924|       "confidence": "unknown_reference",
 68925|       "confidence_tier": "unverified_reference",
 68926|       "target_resolution": "none",
 68927|       "evidence": [
 68928|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 68929|       ],
 68930|       "source_url": "https://www.revitapidocs.com/2025/5f4c8bb3-da54-fb00-387a-2e7c13800840.htm",
 68931|       "dll_signature_verified": true,
 68932|       "dll_relationship_scope": "declared",
 68933|       "dll_semantic_verified": null,
 68934|       "dll_verified_status": "signature_verified_declared",
 68935|       "revitlookup_referenced": null,
 68936|       "revitlookup_requires_document_context": null
 68937|     },
 68938|     {
 68939|       "source": "Autodesk.Revit.DB.Architecture.StairsRunType",
 68940|       "target": null,
 68941|       "member_name": "RiserProfile",
 68942|       "member_kind": "property",
 68943|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 68944|       "confidence": "unknown_reference",
 68945|       "confidence_tier": "unverified_reference",
 68946|       "target_resolution": "none",
 68947|       "evidence": [
 68948|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 68949|       ],
 68950|       "source_url": "https://www.revitapidocs.com/2025/f9c01688-2700-8144-47f3-a41896ca071a.htm",
 68951|       "dll_signature_verified": true,
 68952|       "dll_relationship_scope": "declared",
 68953|       "dll_semantic_verified": null,
 68954|       "dll_verified_status": "signature_verified_declared",
 68955|       "revitlookup_referenced": null,
 68956|       "revitlookup_requires_document_context": null
 68957|     },
 68958|     {
 68959|       "source": "Autodesk.Revit.DB.Architecture.StairsRunType",
 68960|       "target": null,
 68961|       "member_name": "TreadProfile",
 68962|       "member_kind": "property",
 68963|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 68964|       "confidence": "unknown_reference",
 68965|       "confidence_tier": "unverified_reference",
 68966|       "target_resolution": "none",
 68967|       "evidence": [
 68968|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 68969|       ],
 68970|       "source_url": "https://www.revitapidocs.com/2025/9d40bd4c-480a-3cf6-6b48-f81dfd2e7ad9.htm",
 68971|       "dll_signature_verified": true,
 68972|       "dll_relationship_scope": "declared",
 68973|       "dll_semantic_verified": null,
 68974|       "dll_verified_status": "signature_verified_declared",
 68975|       "revitlookup_referenced": null,
 68976|       "revitlookup_requires_document_context": null
 68977|     },
 68978|     {
 68979|       "source": "Autodesk.Revit.DB.Architecture.StairsType",
 68980|       "target": null,
 68981|       "member_name": "LandingType",
 68982|       "member_kind": "property",
 68983|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 68984|       "confidence": "unknown_reference",
 68985|       "confidence_tier": "unverified_reference",
 68986|       "target_resolution": "none",
 68987|       "evidence": [
 68988|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 68989|       ],
 68990|       "source_url": "https://www.revitapidocs.com/2025/65d623c8-e4af-4b8b-092e-62602d11ecc2.htm",
 68991|       "dll_signature_verified": true,
 68992|       "dll_relationship_scope": "declared",
 68993|       "dll_semantic_verified": null,
 68994|       "dll_verified_status": "signature_verified_declared",
 68995|       "revitlookup_referenced": null,
 68996|       "revitlookup_requires_document_context": null
 68997|     },
 68998|     {
 68999|       "source": "Autodesk.Revit.DB.Architecture.StairsType",
 69000|       "target": null,
 69001|       "member_name": "LeftSideSupportType",
 69002|       "member_kind": "property",
 69003|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 69004|       "confidence": "unknown_reference",
 69005|       "confidence_tier": "unverified_reference",
 69006|       "target_resolution": "none",
 69007|       "evidence": [
 69008|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 69009|       ],
 69010|       "source_url": "https://www.revitapidocs.com/2025/937a6510-30b8-2a23-3c26-687fa8daa8f3.htm",
 69011|       "dll_signature_verified": true,
 69012|       "dll_relationship_scope": "declared",
 69013|       "dll_semantic_verified": null,
 69014|       "dll_verified_status": "signature_verified_declared",
 69015|       "revitlookup_referenced": null,
 69016|       "revitlookup_requires_document_context": null
 69017|     },
 69018|     {
 69019|       "source": "Autodesk.Revit.DB.Architecture.StairsType",
 69020|       "target": null,
 69021|       "member_name": "MiddleSupportType",
 69022|       "member_kind": "property",
 69023|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 69024|       "confidence": "unknown_reference",
 69025|       "confidence_tier": "unverified_reference",
 69026|       "target_resolution": "none",
 69027|       "evidence": [
 69028|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 69029|       ],
 69030|       "source_url": "https://www.revitapidocs.com/2025/be91b3a0-047b-bf7f-27e4-3a22c1930735.htm",
 69031|       "dll_signature_verified": true,
 69032|       "dll_relationship_scope": "declared",
 69033|       "dll_semantic_verified": null,
 69034|       "dll_verified_status": "signature_verified_declared",
 69035|       "revitlookup_referenced": null,
 69036|       "revitlookup_requires_document_context": null
 69037|     },
 69038|     {
 69039|       "source": "Autodesk.Revit.DB.Architecture.StairsType",
 69040|       "target": null,
```
