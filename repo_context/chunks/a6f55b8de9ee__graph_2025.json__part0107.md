# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 107 of 216
- Original line range: 41341-41740
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 41341|       "confidence_tier": "unverified_reference",
 41342|       "target_resolution": "exact",
 41343|       "evidence": [
 41344|         "return type 'InternalDefinition' directly names a Revit DB object type"
 41345|       ],
 41346|       "source_url": "https://www.revitapidocs.com/2025/d2184f58-82a5-472f-4cae-64cbaeeb36c9.htm",
 41347|       "dll_signature_verified": true,
 41348|       "dll_relationship_scope": "declared",
 41349|       "dll_semantic_verified": null,
 41350|       "dll_verified_status": "signature_verified_declared",
 41351|       "revitlookup_referenced": null,
 41352|       "revitlookup_requires_document_context": null
 41353|     },
 41354|     {
 41355|       "source": "Autodesk.Revit.DB.EvaluatedParameter",
 41356|       "target": "Autodesk.Revit.DB.ParameterValue",
 41357|       "member_name": "Value",
 41358|       "member_kind": "property",
 41359|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 41360|       "confidence": "direct_return_type",
 41361|       "confidence_tier": "unverified_reference",
 41362|       "target_resolution": "exact",
 41363|       "evidence": [
 41364|         "return type 'ParameterValue' directly names a Revit DB object type"
 41365|       ],
 41366|       "source_url": "https://www.revitapidocs.com/2025/9e5cc692-1507-6780-afa4-78076c8096cc.htm",
 41367|       "dll_signature_verified": true,
 41368|       "dll_relationship_scope": "declared",
 41369|       "dll_semantic_verified": null,
 41370|       "dll_verified_status": "signature_verified_declared",
 41371|       "revitlookup_referenced": null,
 41372|       "revitlookup_requires_document_context": null
 41373|     },
 41374|     {
 41375|       "source": "Autodesk.Revit.DB.ExclusionFilter",
 41376|       "target": null,
 41377|       "member_name": "GetIdsToExclude",
 41378|       "member_kind": "method",
 41379|       "edge_type": "RETURNS_ELEMENT_IDS",
 41380|       "confidence": "unknown_reference",
 41381|       "confidence_tier": "unverified_reference",
 41382|       "target_resolution": "none",
 41383|       "evidence": [
 41384|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 41385|       ],
 41386|       "source_url": "https://www.revitapidocs.com/2025/f8562b02-0777-e3da-e783-2cb6c04c9253.htm",
 41387|       "dll_signature_verified": true,
 41388|       "dll_relationship_scope": "declared",
 41389|       "dll_semantic_verified": null,
 41390|       "dll_verified_status": "signature_verified_declared",
 41391|       "revitlookup_referenced": null,
 41392|       "revitlookup_requires_document_context": null
 41393|     },
 41394|     {
 41395|       "source": "Autodesk.Revit.DB.ExportDGNSettings",
 41396|       "target": "Autodesk.Revit.DB.DGNExportOptions",
 41397|       "member_name": "GetDGNExportOptions",
 41398|       "member_kind": "method",
 41399|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 41400|       "confidence": "direct_return_type",
 41401|       "confidence_tier": "unverified_reference",
 41402|       "target_resolution": "exact",
 41403|       "evidence": [
 41404|         "return type 'DGNExportOptions' directly names a Revit DB object type"
 41405|       ],
 41406|       "source_url": "https://www.revitapidocs.com/2025/bdbe8341-e660-3e86-0cb5-03bfac3927f7.htm",
 41407|       "dll_signature_verified": true,
 41408|       "dll_relationship_scope": "declared",
 41409|       "dll_semantic_verified": null,
 41410|       "dll_verified_status": "signature_verified_declared",
 41411|       "revitlookup_referenced": null,
 41412|       "revitlookup_requires_document_context": null
 41413|     },
 41414|     {
 41415|       "source": "Autodesk.Revit.DB.ExportDWGSettings",
 41416|       "target": "Autodesk.Revit.DB.DWGExportOptions",
 41417|       "member_name": "GetDWGExportOptions",
 41418|       "member_kind": "method",
 41419|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 41420|       "confidence": "direct_return_type",
 41421|       "confidence_tier": "unverified_reference",
 41422|       "target_resolution": "exact",
 41423|       "evidence": [
 41424|         "return type 'DWGExportOptions' directly names a Revit DB object type"
 41425|       ],
 41426|       "source_url": "https://www.revitapidocs.com/2025/d5372a5c-b19a-03a8-4a2b-5f3617acec61.htm",
 41427|       "dll_signature_verified": true,
 41428|       "dll_relationship_scope": "declared",
 41429|       "dll_semantic_verified": null,
 41430|       "dll_verified_status": "signature_verified_declared",
 41431|       "revitlookup_referenced": null,
 41432|       "revitlookup_requires_document_context": null
 41433|     },
 41434|     {
 41435|       "source": "Autodesk.Revit.DB.ExportDWGSettings",
 41436|       "target": "Autodesk.Revit.DB.DXFExportOptions",
 41437|       "member_name": "GetDXFExportOptions",
 41438|       "member_kind": "method",
 41439|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 41440|       "confidence": "direct_return_type",
 41441|       "confidence_tier": "unverified_reference",
 41442|       "target_resolution": "exact",
 41443|       "evidence": [
 41444|         "return type 'DXFExportOptions' directly names a Revit DB object type"
 41445|       ],
 41446|       "source_url": "https://www.revitapidocs.com/2025/c678af47-2536-4301-8167-85d1aacf604e.htm",
 41447|       "dll_signature_verified": true,
 41448|       "dll_relationship_scope": "declared",
 41449|       "dll_semantic_verified": null,
 41450|       "dll_verified_status": "signature_verified_declared",
 41451|       "revitlookup_referenced": null,
 41452|       "revitlookup_requires_document_context": null
 41453|     },
 41454|     {
 41455|       "source": "Autodesk.Revit.DB.ExportFontTable",
 41456|       "target": "Autodesk.Revit.DB.ExportFontInfo",
 41457|       "member_name": "GetExportFontInfo",
 41458|       "member_kind": "method",
 41459|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 41460|       "confidence": "direct_return_type",
 41461|       "confidence_tier": "unverified_reference",
 41462|       "target_resolution": "exact",
 41463|       "evidence": [
 41464|         "return type 'ExportFontInfo' directly names a Revit DB object type"
 41465|       ],
 41466|       "source_url": "https://www.revitapidocs.com/2025/fd36a0ee-28b7-9521-c90d-3b27f8e0bec0.htm",
 41467|       "dll_signature_verified": true,
 41468|       "dll_relationship_scope": "declared",
 41469|       "dll_semantic_verified": null,
 41470|       "dll_verified_status": "signature_verified_declared",
 41471|       "revitlookup_referenced": null,
 41472|       "revitlookup_requires_document_context": null
 41473|     },
 41474|     {
 41475|       "source": "Autodesk.Revit.DB.ExportFontTable",
 41476|       "target": "Autodesk.Revit.DB.ExportFontTableIterator",
 41477|       "member_name": "GetFontTableIterator",
 41478|       "member_kind": "method",
 41479|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 41480|       "confidence": "direct_return_type",
 41481|       "confidence_tier": "unverified_reference",
 41482|       "target_resolution": "exact",
 41483|       "evidence": [
 41484|         "return type 'ExportFontTableIterator' directly names a Revit DB object type"
 41485|       ],
 41486|       "source_url": "https://www.revitapidocs.com/2025/306f098d-8847-c938-ccb0-f941d233d252.htm",
 41487|       "dll_signature_verified": true,
 41488|       "dll_relationship_scope": "declared",
 41489|       "dll_semantic_verified": null,
 41490|       "dll_verified_status": "signature_verified_declared",
 41491|       "revitlookup_referenced": null,
 41492|       "revitlookup_requires_document_context": null
 41493|     },
 41494|     {
 41495|       "source": "Autodesk.Revit.DB.ExportFontTable",
 41496|       "target": "Autodesk.Revit.DB.ExportFontKey",
 41497|       "member_name": "GetKeys",
 41498|       "member_kind": "method",
 41499|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 41500|       "confidence": "needs_runtime_validation",
 41501|       "confidence_tier": "needs_validation",
 41502|       "target_resolution": "exact",
 41503|       "evidence": [
 41504|         "return type 'IList < ExportFontKey >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 41505|       ],
 41506|       "source_url": "https://www.revitapidocs.com/2025/9dae03bf-c7d8-a10b-329d-f6fb01c23d30.htm",
 41507|       "dll_signature_verified": true,
 41508|       "dll_relationship_scope": "declared",
 41509|       "dll_semantic_verified": null,
 41510|       "dll_verified_status": "signature_verified_declared",
 41511|       "revitlookup_referenced": null,
 41512|       "revitlookup_requires_document_context": null
 41513|     },
 41514|     {
 41515|       "source": "Autodesk.Revit.DB.ExportFontTable",
 41516|       "target": "Autodesk.Revit.DB.ExportFontInfo",
 41517|       "member_name": "GetValues",
 41518|       "member_kind": "method",
 41519|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 41520|       "confidence": "needs_runtime_validation",
 41521|       "confidence_tier": "needs_validation",
 41522|       "target_resolution": "exact",
 41523|       "evidence": [
 41524|         "return type 'IList < ExportFontInfo >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 41525|       ],
 41526|       "source_url": "https://www.revitapidocs.com/2025/d469e2a4-dc15-61e6-4af4-17b6010b093b.htm",
 41527|       "dll_signature_verified": true,
 41528|       "dll_relationship_scope": "declared",
 41529|       "dll_semantic_verified": null,
 41530|       "dll_verified_status": "signature_verified_declared",
 41531|       "revitlookup_referenced": null,
 41532|       "revitlookup_requires_document_context": null
 41533|     },
 41534|     {
 41535|       "source": "Autodesk.Revit.DB.ExportIFCCategoryKey",
 41536|       "target": "Autodesk.Revit.DB.Category",
 41537|       "member_name": "CategoryName",
 41538|       "member_kind": "property",
 41539|       "edge_type": "HAS_CATEGORY",
 41540|       "confidence": "name_only_candidate",
 41541|       "confidence_tier": "likely",
 41542|       "target_resolution": "exact",
 41543|       "evidence": [
 41544|         "member name 'CategoryName' matches keyword pattern /Category/ but return type 'string' gives no type-level confirmation"
 41545|       ],
 41546|       "source_url": "https://www.revitapidocs.com/2025/34faa912-01f2-0324-7a68-873235658c8f.htm",
 41547|       "dll_signature_verified": true,
 41548|       "dll_relationship_scope": "declared",
 41549|       "dll_semantic_verified": null,
 41550|       "dll_verified_status": "signature_verified_declared",
 41551|       "revitlookup_referenced": null,
 41552|       "revitlookup_requires_document_context": null
 41553|     },
 41554|     {
 41555|       "source": "Autodesk.Revit.DB.ExportIFCCategoryKey",
 41556|       "target": "Autodesk.Revit.DB.Category",
 41557|       "member_name": "CustomSubCategoryId",
 41558|       "member_kind": "property",
 41559|       "edge_type": "HAS_CATEGORY",
 41560|       "confidence": "name_only_candidate",
 41561|       "confidence_tier": "likely",
 41562|       "target_resolution": "exact",
 41563|       "evidence": [
 41564|         "member name 'CustomSubCategoryId' matches keyword pattern /Category/ but return type 'CustomSubCategoryId' gives no type-level confirmation"
 41565|       ],
 41566|       "source_url": "https://www.revitapidocs.com/2025/fea9560c-4118-4e9e-589c-19fef670a580.htm",
 41567|       "dll_signature_verified": true,
 41568|       "dll_relationship_scope": "declared",
 41569|       "dll_semantic_verified": null,
 41570|       "dll_verified_status": "signature_verified_declared",
 41571|       "revitlookup_referenced": null,
 41572|       "revitlookup_requires_document_context": null
 41573|     },
 41574|     {
 41575|       "source": "Autodesk.Revit.DB.ExportIFCCategoryKey",
 41576|       "target": "Autodesk.Revit.DB.Category",
 41577|       "member_name": "SubCategoryName",
 41578|       "member_kind": "property",
 41579|       "edge_type": "HAS_CATEGORY",
 41580|       "confidence": "name_only_candidate",
 41581|       "confidence_tier": "likely",
 41582|       "target_resolution": "exact",
 41583|       "evidence": [
 41584|         "member name 'SubCategoryName' matches keyword pattern /Category/ but return type 'string' gives no type-level confirmation"
 41585|       ],
 41586|       "source_url": "https://www.revitapidocs.com/2025/996deb18-3046-4b94-4f82-a8c37d2c7521.htm",
 41587|       "dll_signature_verified": true,
 41588|       "dll_relationship_scope": "declared",
 41589|       "dll_semantic_verified": null,
 41590|       "dll_verified_status": "signature_verified_declared",
 41591|       "revitlookup_referenced": null,
 41592|       "revitlookup_requires_document_context": null
 41593|     },
 41594|     {
 41595|       "source": "Autodesk.Revit.DB.ExportLayerInfo",
 41596|       "target": "Autodesk.Revit.DB.Category",
 41597|       "member_name": "CategoryType",
 41598|       "member_kind": "property",
 41599|       "edge_type": "HAS_CATEGORY",
 41600|       "confidence": "name_only_candidate",
 41601|       "confidence_tier": "likely",
 41602|       "target_resolution": "exact",
 41603|       "evidence": [
 41604|         "member name 'CategoryType' matches keyword pattern /Category/ but return type 'LayerCategoryType' gives no type-level confirmation"
 41605|       ],
 41606|       "source_url": "https://www.revitapidocs.com/2025/e04b3dca-a39a-80f6-cd2d-970a2d53accf.htm",
 41607|       "dll_signature_verified": true,
 41608|       "dll_relationship_scope": "declared",
 41609|       "dll_semantic_verified": null,
 41610|       "dll_verified_status": "signature_verified_declared",
 41611|       "revitlookup_referenced": null,
 41612|       "revitlookup_requires_document_context": null
 41613|     },
 41614|     {
 41615|       "source": "Autodesk.Revit.DB.ExportLayerInfo",
 41616|       "target": "Autodesk.Revit.DB.LayerModifier",
 41617|       "member_name": "GetCutLayerModifiers",
 41618|       "member_kind": "method",
 41619|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 41620|       "confidence": "needs_runtime_validation",
 41621|       "confidence_tier": "needs_validation",
 41622|       "target_resolution": "exact",
 41623|       "evidence": [
 41624|         "return type 'IList < LayerModifier >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 41625|       ],
 41626|       "source_url": "https://www.revitapidocs.com/2025/70a80f11-165c-14ce-fcea-d19dba6591ee.htm",
 41627|       "dll_signature_verified": true,
 41628|       "dll_relationship_scope": "declared",
 41629|       "dll_semantic_verified": null,
 41630|       "dll_verified_status": "signature_verified_declared",
 41631|       "revitlookup_referenced": null,
 41632|       "revitlookup_requires_document_context": null
 41633|     },
 41634|     {
 41635|       "source": "Autodesk.Revit.DB.ExportLayerInfo",
 41636|       "target": "Autodesk.Revit.DB.LayerModifier",
 41637|       "member_name": "GetLayerModifiers",
 41638|       "member_kind": "method",
 41639|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 41640|       "confidence": "needs_runtime_validation",
 41641|       "confidence_tier": "needs_validation",
 41642|       "target_resolution": "exact",
 41643|       "evidence": [
 41644|         "return type 'IList < LayerModifier >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 41645|       ],
 41646|       "source_url": "https://www.revitapidocs.com/2025/3d752980-c4ea-6a3b-3708-c00ae5c377e5.htm",
 41647|       "dll_signature_verified": true,
 41648|       "dll_relationship_scope": "declared",
 41649|       "dll_semantic_verified": null,
 41650|       "dll_verified_status": "signature_verified_declared",
 41651|       "revitlookup_referenced": null,
 41652|       "revitlookup_requires_document_context": null
 41653|     },
 41654|     {
 41655|       "source": "Autodesk.Revit.DB.ExportLayerKey",
 41656|       "target": "Autodesk.Revit.DB.Category",
 41657|       "member_name": "CategoryName",
 41658|       "member_kind": "property",
 41659|       "edge_type": "HAS_CATEGORY",
 41660|       "confidence": "name_only_candidate",
 41661|       "confidence_tier": "likely",
 41662|       "target_resolution": "exact",
 41663|       "evidence": [
 41664|         "member name 'CategoryName' matches keyword pattern /Category/ but return type 'string' gives no type-level confirmation"
 41665|       ],
 41666|       "source_url": "https://www.revitapidocs.com/2025/9c22f632-e952-d80c-4bd1-98c57edd0654.htm",
 41667|       "dll_signature_verified": true,
 41668|       "dll_relationship_scope": "declared",
 41669|       "dll_semantic_verified": null,
 41670|       "dll_verified_status": "signature_verified_declared",
 41671|       "revitlookup_referenced": null,
 41672|       "revitlookup_requires_document_context": null
 41673|     },
 41674|     {
 41675|       "source": "Autodesk.Revit.DB.ExportLayerKey",
 41676|       "target": "Autodesk.Revit.DB.Category",
 41677|       "member_name": "SubCategoryName",
 41678|       "member_kind": "property",
 41679|       "edge_type": "HAS_CATEGORY",
 41680|       "confidence": "name_only_candidate",
 41681|       "confidence_tier": "likely",
 41682|       "target_resolution": "exact",
 41683|       "evidence": [
 41684|         "member name 'SubCategoryName' matches keyword pattern /Category/ but return type 'string' gives no type-level confirmation"
 41685|       ],
 41686|       "source_url": "https://www.revitapidocs.com/2025/72d1407d-44a9-70c7-3b16-14efca9fe31a.htm",
 41687|       "dll_signature_verified": true,
 41688|       "dll_relationship_scope": "declared",
 41689|       "dll_semantic_verified": null,
 41690|       "dll_verified_status": "signature_verified_declared",
 41691|       "revitlookup_referenced": null,
 41692|       "revitlookup_requires_document_context": null
 41693|     },
 41694|     {
 41695|       "source": "Autodesk.Revit.DB.ExportLayerTable",
 41696|       "target": "Autodesk.Revit.DB.ModifierType",
 41697|       "member_name": "GetAvaliableLayerModifierTypes",
 41698|       "member_kind": "method",
 41699|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 41700|       "confidence": "needs_runtime_validation",
 41701|       "confidence_tier": "needs_validation",
 41702|       "target_resolution": "exact",
 41703|       "evidence": [
 41704|         "return type 'IList < ModifierType >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 41705|       ],
 41706|       "source_url": "https://www.revitapidocs.com/2025/688f2403-1d4b-2498-8365-c5480fb9a080.htm",
 41707|       "dll_signature_verified": true,
 41708|       "dll_relationship_scope": "declared",
 41709|       "dll_semantic_verified": null,
 41710|       "dll_verified_status": "signature_verified_declared",
 41711|       "revitlookup_referenced": null,
 41712|       "revitlookup_requires_document_context": null
 41713|     },
 41714|     {
 41715|       "source": "Autodesk.Revit.DB.ExportLayerTable",
 41716|       "target": "Autodesk.Revit.DB.ExportLayerInfo",
 41717|       "member_name": "GetExportLayerInfo",
 41718|       "member_kind": "method",
 41719|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 41720|       "confidence": "direct_return_type",
 41721|       "confidence_tier": "unverified_reference",
 41722|       "target_resolution": "exact",
 41723|       "evidence": [
 41724|         "return type 'ExportLayerInfo' directly names a Revit DB object type"
 41725|       ],
 41726|       "source_url": "https://www.revitapidocs.com/2025/9f41769c-080a-620e-2d68-828b27aa3565.htm",
 41727|       "dll_signature_verified": true,
 41728|       "dll_relationship_scope": "declared",
 41729|       "dll_semantic_verified": null,
 41730|       "dll_verified_status": "signature_verified_declared",
 41731|       "revitlookup_referenced": null,
 41732|       "revitlookup_requires_document_context": null
 41733|     },
 41734|     {
 41735|       "source": "Autodesk.Revit.DB.ExportLayerTable",
 41736|       "target": "Autodesk.Revit.DB.ExportLayerKey",
 41737|       "member_name": "GetKeys",
 41738|       "member_kind": "method",
 41739|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 41740|       "confidence": "needs_runtime_validation",
```
