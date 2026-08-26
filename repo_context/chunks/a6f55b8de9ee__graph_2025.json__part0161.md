# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 161 of 216
- Original line range: 62401-62800
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 62401|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 62402|       ],
 62403|       "source_url": "https://www.revitapidocs.com/2025/8ea511ab-7bca-bb8b-4eb0-1c1bacfc03f7.htm",
 62404|       "dll_signature_verified": true,
 62405|       "dll_relationship_scope": "declared",
 62406|       "dll_semantic_verified": null,
 62407|       "dll_verified_status": "signature_verified_declared",
 62408|       "revitlookup_referenced": null,
 62409|       "revitlookup_requires_document_context": null
 62410|     },
 62411|     {
 62412|       "source": "Autodesk.Revit.DB.View",
 62413|       "target": "Autodesk.Revit.DB.ViewDisplaySketchyLines",
 62414|       "member_name": "GetSketchyLines",
 62415|       "member_kind": "method",
 62416|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 62417|       "confidence": "direct_return_type",
 62418|       "confidence_tier": "unverified_reference",
 62419|       "target_resolution": "exact",
 62420|       "evidence": [
 62421|         "return type 'ViewDisplaySketchyLines' directly names a Revit DB object type"
 62422|       ],
 62423|       "source_url": "https://www.revitapidocs.com/2025/62ec5bed-dfb7-0782-924b-71e12a82756d.htm",
 62424|       "dll_signature_verified": true,
 62425|       "dll_relationship_scope": "declared",
 62426|       "dll_semantic_verified": null,
 62427|       "dll_verified_status": "signature_verified_declared",
 62428|       "revitlookup_referenced": null,
 62429|       "revitlookup_requires_document_context": null
 62430|     },
 62431|     {
 62432|       "source": "Autodesk.Revit.DB.View",
 62433|       "target": "Autodesk.Revit.DB.View",
 62434|       "member_name": "GetTemplateParameterIds",
 62435|       "member_kind": "method",
 62436|       "edge_type": "CONTROLLED_BY_TEMPLATE",
 62437|       "confidence": "elementid_collection_with_strong_name",
 62438|       "confidence_tier": "core",
 62439|       "target_resolution": "exact",
 62440|       "evidence": [
 62441|         "member name 'GetTemplateParameterIds' matches keyword pattern /Template/"
 62442|       ],
 62443|       "source_url": "https://www.revitapidocs.com/2025/64761c8c-ed01-65b6-2b05-ebd7b02acd77.htm",
 62444|       "dll_signature_verified": true,
 62445|       "dll_relationship_scope": "declared",
 62446|       "dll_semantic_verified": null,
 62447|       "dll_verified_status": "signature_verified_declared",
 62448|       "revitlookup_referenced": null,
 62449|       "revitlookup_requires_document_context": null
 62450|     },
 62451|     {
 62452|       "source": "Autodesk.Revit.DB.View",
 62453|       "target": null,
 62454|       "member_name": "GetTemporaryViewPropertiesId",
 62455|       "member_kind": "method",
 62456|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 62457|       "confidence": "unknown_reference",
 62458|       "confidence_tier": "unverified_reference",
 62459|       "target_resolution": "none",
 62460|       "evidence": [
 62461|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 62462|       ],
 62463|       "source_url": "https://www.revitapidocs.com/2025/1fa3e6e9-9a09-2ffa-25e1-302ada24bb12.htm",
 62464|       "dll_signature_verified": true,
 62465|       "dll_relationship_scope": "declared",
 62466|       "dll_semantic_verified": null,
 62467|       "dll_verified_status": "signature_verified_declared",
 62468|       "revitlookup_referenced": null,
 62469|       "revitlookup_requires_document_context": null
 62470|     },
 62471|     {
 62472|       "source": "Autodesk.Revit.DB.View",
 62473|       "target": "Autodesk.Revit.DB.ViewDisplayModel",
 62474|       "member_name": "GetViewDisplayModel",
 62475|       "member_kind": "method",
 62476|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 62477|       "confidence": "direct_return_type",
 62478|       "confidence_tier": "unverified_reference",
 62479|       "target_resolution": "exact",
 62480|       "evidence": [
 62481|         "return type 'ViewDisplayModel' directly names a Revit DB object type"
 62482|       ],
 62483|       "source_url": "https://www.revitapidocs.com/2025/df7cfd06-5282-912b-25f0-e47128a2c62a.htm",
 62484|       "dll_signature_verified": true,
 62485|       "dll_relationship_scope": "declared",
 62486|       "dll_semantic_verified": null,
 62487|       "dll_verified_status": "signature_verified_declared",
 62488|       "revitlookup_referenced": null,
 62489|       "revitlookup_requires_document_context": null
 62490|     },
 62491|     {
 62492|       "source": "Autodesk.Revit.DB.View",
 62493|       "target": "Autodesk.Revit.DB.Workset",
 62494|       "member_name": "GetWorksetVisibility",
 62495|       "member_kind": "method",
 62496|       "edge_type": "OWNED_BY_WORKSET",
 62497|       "confidence": "name_only_candidate",
 62498|       "confidence_tier": "likely",
 62499|       "target_resolution": "exact",
 62500|       "evidence": [
 62501|         "member name 'GetWorksetVisibility' matches keyword pattern /Workset/ but return type 'WorksetVisibility' gives no type-level confirmation"
 62502|       ],
 62503|       "source_url": "https://www.revitapidocs.com/2025/1c37557b-9bd4-12e2-dffb-c3a25cf9a375.htm",
 62504|       "dll_signature_verified": true,
 62505|       "dll_relationship_scope": "declared",
 62506|       "dll_semantic_verified": null,
 62507|       "dll_verified_status": "signature_verified_declared",
 62508|       "revitlookup_referenced": true,
 62509|       "revitlookup_requires_document_context": true
 62510|     },
 62511|     {
 62512|       "source": "Autodesk.Revit.DB.View",
 62513|       "target": "Autodesk.Revit.DB.Level",
 62514|       "member_name": "HasDetailLevel",
 62515|       "member_kind": "method",
 62516|       "edge_type": "ASSIGNED_TO_LEVEL",
 62517|       "confidence": "name_only_candidate",
 62518|       "confidence_tier": "likely",
 62519|       "target_resolution": "exact",
 62520|       "evidence": [
 62521|         "member name 'HasDetailLevel' matches keyword pattern /Level/ but return type 'bool' gives no type-level confirmation"
 62522|       ],
 62523|       "source_url": "https://www.revitapidocs.com/2025/b9f63f88-a18d-5b96-3338-f55685e2a229.htm",
 62524|       "dll_signature_verified": true,
 62525|       "dll_relationship_scope": "declared",
 62526|       "dll_semantic_verified": null,
 62527|       "dll_verified_status": "signature_verified_declared",
 62528|       "revitlookup_referenced": null,
 62529|       "revitlookup_requires_document_context": null
 62530|     },
 62531|     {
 62532|       "source": "Autodesk.Revit.DB.View",
 62533|       "target": "Autodesk.Revit.DB.Category",
 62534|       "member_name": "HideCategoryTemporary",
 62535|       "member_kind": "method",
 62536|       "edge_type": "HAS_CATEGORY",
 62537|       "confidence": "name_only_candidate",
 62538|       "confidence_tier": "likely",
 62539|       "target_resolution": "exact",
 62540|       "evidence": [
 62541|         "member name 'HideCategoryTemporary' matches keyword pattern /Category/ but return type 'void' gives no type-level confirmation"
 62542|       ],
 62543|       "source_url": "https://www.revitapidocs.com/2025/26684015-0635-cb13-d5a9-f6a6f9b0780a.htm",
 62544|       "dll_signature_verified": true,
 62545|       "dll_relationship_scope": "declared",
 62546|       "dll_semantic_verified": null,
 62547|       "dll_verified_status": "signature_verified_declared",
 62548|       "revitlookup_referenced": null,
 62549|       "revitlookup_requires_document_context": null
 62550|     },
 62551|     {
 62552|       "source": "Autodesk.Revit.DB.View",
 62553|       "target": "Autodesk.Revit.DB.Category",
 62554|       "member_name": "IsCategoryOverridable",
 62555|       "member_kind": "method",
 62556|       "edge_type": "HAS_CATEGORY",
 62557|       "confidence": "name_only_candidate",
 62558|       "confidence_tier": "likely",
 62559|       "target_resolution": "exact",
 62560|       "evidence": [
 62561|         "member name 'IsCategoryOverridable' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 62562|       ],
 62563|       "source_url": "https://www.revitapidocs.com/2025/054346ac-ee60-2969-cdc6-8c2c17324abb.htm",
 62564|       "dll_signature_verified": true,
 62565|       "dll_relationship_scope": "declared",
 62566|       "dll_semantic_verified": null,
 62567|       "dll_verified_status": "signature_verified_declared",
 62568|       "revitlookup_referenced": true,
 62569|       "revitlookup_requires_document_context": true
 62570|     },
 62571|     {
 62572|       "source": "Autodesk.Revit.DB.View",
 62573|       "target": "Autodesk.Revit.DB.Category",
 62574|       "member_name": "IsolateCategoryTemporary",
 62575|       "member_kind": "method",
 62576|       "edge_type": "HAS_CATEGORY",
 62577|       "confidence": "name_only_candidate",
 62578|       "confidence_tier": "likely",
 62579|       "target_resolution": "exact",
 62580|       "evidence": [
 62581|         "member name 'IsolateCategoryTemporary' matches keyword pattern /Category/ but return type 'void' gives no type-level confirmation"
 62582|       ],
 62583|       "source_url": "https://www.revitapidocs.com/2025/8bbe17bd-c0f3-d5c5-d94e-a2e04fc42b51.htm",
 62584|       "dll_signature_verified": true,
 62585|       "dll_relationship_scope": "declared",
 62586|       "dll_semantic_verified": null,
 62587|       "dll_verified_status": "signature_verified_declared",
 62588|       "revitlookup_referenced": null,
 62589|       "revitlookup_requires_document_context": null
 62590|     },
 62591|     {
 62592|       "source": "Autodesk.Revit.DB.View",
 62593|       "target": "Autodesk.Revit.DB.View",
 62594|       "member_name": "IsValidViewTemplate",
 62595|       "member_kind": "method",
 62596|       "edge_type": "CONTROLLED_BY_TEMPLATE",
 62597|       "confidence": "docs_semantic_hint",
 62598|       "confidence_tier": "core",
 62599|       "target_resolution": "exact",
 62600|       "evidence": [
 62601|         "member name 'IsValidViewTemplate' matches keyword pattern /Template/ but return type 'bool' gives no type-level confirmation",
 62602|         "docs text contains relationship phrase: 'template for'"
 62603|       ],
 62604|       "source_url": "https://www.revitapidocs.com/2025/53cfc8e6-8004-4164-9b1c-79bbc187450f.htm",
 62605|       "dll_signature_verified": true,
 62606|       "dll_relationship_scope": "declared",
 62607|       "dll_semantic_verified": null,
 62608|       "dll_verified_status": "signature_verified_declared",
 62609|       "revitlookup_referenced": true,
 62610|       "revitlookup_requires_document_context": true
 62611|     },
 62612|     {
 62613|       "source": "Autodesk.Revit.DB.View",
 62614|       "target": "Autodesk.Revit.DB.View",
 62615|       "member_name": "IsViewValidForTemplateCreation",
 62616|       "member_kind": "method",
 62617|       "edge_type": "CONTROLLED_BY_TEMPLATE",
 62618|       "confidence": "name_only_candidate",
 62619|       "confidence_tier": "likely",
 62620|       "target_resolution": "exact",
 62621|       "evidence": [
 62622|         "member name 'IsViewValidForTemplateCreation' matches keyword pattern /Template/ but return type 'bool' gives no type-level confirmation"
 62623|       ],
 62624|       "source_url": "https://www.revitapidocs.com/2025/8de8549b-ded8-94e1-434f-4883afc77028.htm",
 62625|       "dll_signature_verified": true,
 62626|       "dll_relationship_scope": "declared",
 62627|       "dll_semantic_verified": null,
 62628|       "dll_verified_status": "signature_verified_declared",
 62629|       "revitlookup_referenced": null,
 62630|       "revitlookup_requires_document_context": null
 62631|     },
 62632|     {
 62633|       "source": "Autodesk.Revit.DB.View",
 62634|       "target": "Autodesk.Revit.DB.Workset",
 62635|       "member_name": "IsWorksetVisible",
 62636|       "member_kind": "method",
 62637|       "edge_type": "OWNED_BY_WORKSET",
 62638|       "confidence": "name_only_candidate",
 62639|       "confidence_tier": "likely",
 62640|       "target_resolution": "exact",
 62641|       "evidence": [
 62642|         "member name 'IsWorksetVisible' matches keyword pattern /Workset/ but return type 'bool' gives no type-level confirmation"
 62643|       ],
 62644|       "source_url": "https://www.revitapidocs.com/2025/b3eb5b95-5d39-5f77-ef86-b2db00d247cd.htm",
 62645|       "dll_signature_verified": true,
 62646|       "dll_relationship_scope": "declared",
 62647|       "dll_semantic_verified": null,
 62648|       "dll_verified_status": "signature_verified_declared",
 62649|       "revitlookup_referenced": true,
 62650|       "revitlookup_requires_document_context": true
 62651|     },
 62652|     {
 62653|       "source": "Autodesk.Revit.DB.View",
 62654|       "target": "Autodesk.Revit.DB.Category",
 62655|       "member_name": "SetCategoryHidden",
 62656|       "member_kind": "method",
 62657|       "edge_type": "HAS_CATEGORY",
 62658|       "confidence": "name_only_candidate",
 62659|       "confidence_tier": "likely",
 62660|       "target_resolution": "exact",
 62661|       "evidence": [
 62662|         "member name 'SetCategoryHidden' matches keyword pattern /Category/ but return type 'void' gives no type-level confirmation"
 62663|       ],
 62664|       "source_url": "https://www.revitapidocs.com/2025/87a1e1e2-ee81-1a73-19d7-895b1fa10158.htm",
 62665|       "dll_signature_verified": true,
 62666|       "dll_relationship_scope": "declared",
 62667|       "dll_semantic_verified": null,
 62668|       "dll_verified_status": "signature_verified_declared",
 62669|       "revitlookup_referenced": null,
 62670|       "revitlookup_requires_document_context": null
 62671|     },
 62672|     {
 62673|       "source": "Autodesk.Revit.DB.View",
 62674|       "target": "Autodesk.Revit.DB.Category",
 62675|       "member_name": "SetCategoryOverrides",
 62676|       "member_kind": "method",
 62677|       "edge_type": "HAS_CATEGORY",
 62678|       "confidence": "name_only_candidate",
 62679|       "confidence_tier": "likely",
 62680|       "target_resolution": "exact",
 62681|       "evidence": [
 62682|         "member name 'SetCategoryOverrides' matches keyword pattern /Category/ but return type 'void' gives no type-level confirmation"
 62683|       ],
 62684|       "source_url": "https://www.revitapidocs.com/2025/ee90e635-7a78-3d14-9159-23a87f1655cc.htm",
 62685|       "dll_signature_verified": true,
 62686|       "dll_relationship_scope": "declared",
 62687|       "dll_semantic_verified": null,
 62688|       "dll_verified_status": "signature_verified_declared",
 62689|       "revitlookup_referenced": null,
 62690|       "revitlookup_requires_document_context": null
 62691|     },
 62692|     {
 62693|       "source": "Autodesk.Revit.DB.View",
 62694|       "target": "Autodesk.Revit.DB.View",
 62695|       "member_name": "SetNonControlledTemplateParameterIds",
 62696|       "member_kind": "method",
 62697|       "edge_type": "CONTROLLED_BY_TEMPLATE",
 62698|       "confidence": "name_only_candidate",
 62699|       "confidence_tier": "likely",
 62700|       "target_resolution": "exact",
 62701|       "evidence": [
 62702|         "member name 'SetNonControlledTemplateParameterIds' matches keyword pattern /Template/ but return type 'void' gives no type-level confirmation"
 62703|       ],
 62704|       "source_url": "https://www.revitapidocs.com/2025/15617b2e-89a4-ddbe-5f93-7855c2994d79.htm",
 62705|       "dll_signature_verified": true,
 62706|       "dll_relationship_scope": "declared",
 62707|       "dll_semantic_verified": null,
 62708|       "dll_verified_status": "signature_verified_declared",
 62709|       "revitlookup_referenced": null,
 62710|       "revitlookup_requires_document_context": null
 62711|     },
 62712|     {
 62713|       "source": "Autodesk.Revit.DB.View",
 62714|       "target": "Autodesk.Revit.DB.Workset",
 62715|       "member_name": "SetWorksetVisibility",
 62716|       "member_kind": "method",
 62717|       "edge_type": "OWNED_BY_WORKSET",
 62718|       "confidence": "name_only_candidate",
 62719|       "confidence_tier": "likely",
 62720|       "target_resolution": "exact",
 62721|       "evidence": [
 62722|         "member name 'SetWorksetVisibility' matches keyword pattern /Workset/ but return type 'void' gives no type-level confirmation"
 62723|       ],
 62724|       "source_url": "https://www.revitapidocs.com/2025/fa6d4b89-a703-80ee-26a6-e88aa89b96a5.htm",
 62725|       "dll_signature_verified": true,
 62726|       "dll_relationship_scope": "declared",
 62727|       "dll_semantic_verified": null,
 62728|       "dll_verified_status": "signature_verified_declared",
 62729|       "revitlookup_referenced": null,
 62730|       "revitlookup_requires_document_context": null
 62731|     },
 62732|     {
 62733|       "source": "Autodesk.Revit.DB.View",
 62734|       "target": "Autodesk.Revit.DB.Category",
 62735|       "member_name": "SupportedColorFillCategoryIds",
 62736|       "member_kind": "method",
 62737|       "edge_type": "HAS_CATEGORY",
 62738|       "confidence": "elementid_collection_with_strong_name",
 62739|       "confidence_tier": "core",
 62740|       "target_resolution": "exact",
 62741|       "evidence": [
 62742|         "member name 'SupportedColorFillCategoryIds' matches keyword pattern /Category/"
 62743|       ],
 62744|       "source_url": "https://www.revitapidocs.com/2025/84197491-81de-0713-06bf-fa7073419485.htm",
 62745|       "dll_signature_verified": true,
 62746|       "dll_relationship_scope": "declared",
 62747|       "dll_semantic_verified": null,
 62748|       "dll_verified_status": "signature_verified_declared",
 62749|       "revitlookup_referenced": null,
 62750|       "revitlookup_requires_document_context": null
 62751|     },
 62752|     {
 62753|       "source": "Autodesk.Revit.DB.View3D",
 62754|       "target": "Autodesk.Revit.DB.Level",
 62755|       "member_name": "GetLevelsThatShowGrids",
 62756|       "member_kind": "method",
 62757|       "edge_type": "ASSIGNED_TO_LEVEL",
 62758|       "confidence": "elementid_collection_with_strong_name",
 62759|       "confidence_tier": "core",
 62760|       "target_resolution": "exact",
 62761|       "evidence": [
 62762|         "member name 'GetLevelsThatShowGrids' matches keyword pattern /Level/"
 62763|       ],
 62764|       "source_url": "https://www.revitapidocs.com/2025/7e0d9d35-1b5a-90f6-ed28-8356f17cd7e1.htm",
 62765|       "dll_signature_verified": true,
 62766|       "dll_relationship_scope": "declared",
 62767|       "dll_semantic_verified": null,
 62768|       "dll_verified_status": "signature_verified_declared",
 62769|       "revitlookup_referenced": null,
 62770|       "revitlookup_requires_document_context": null
 62771|     },
 62772|     {
 62773|       "source": "Autodesk.Revit.DB.View3D",
 62774|       "target": "Autodesk.Revit.DB.ViewOrientation3D",
 62775|       "member_name": "GetOrientation",
 62776|       "member_kind": "method",
 62777|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 62778|       "confidence": "direct_return_type",
 62779|       "confidence_tier": "unverified_reference",
 62780|       "target_resolution": "exact",
 62781|       "evidence": [
 62782|         "return type 'ViewOrientation3D' directly names a Revit DB object type"
 62783|       ],
 62784|       "source_url": "https://www.revitapidocs.com/2025/58374361-7bfa-aceb-ac4e-bc74024dd657.htm",
 62785|       "dll_signature_verified": true,
 62786|       "dll_relationship_scope": "declared",
 62787|       "dll_semantic_verified": null,
 62788|       "dll_verified_status": "signature_verified_declared",
 62789|       "revitlookup_referenced": null,
 62790|       "revitlookup_requires_document_context": null
 62791|     },
 62792|     {
 62793|       "source": "Autodesk.Revit.DB.View3D",
 62794|       "target": "Autodesk.Revit.DB.RenderingSettings",
 62795|       "member_name": "GetRenderingSettings",
 62796|       "member_kind": "method",
 62797|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 62798|       "confidence": "direct_return_type",
 62799|       "confidence_tier": "unverified_reference",
 62800|       "target_resolution": "exact",
```
