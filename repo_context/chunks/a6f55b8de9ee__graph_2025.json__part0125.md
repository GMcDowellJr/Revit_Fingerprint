# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 125 of 216
- Original line range: 48361-48760
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 48361|     },
 48362|     {
 48363|       "source": "Autodesk.Revit.DB.HostObjAttributes",
 48364|       "target": "Autodesk.Revit.DB.CompoundStructure",
 48365|       "member_name": "GetCompoundStructure",
 48366|       "member_kind": "method",
 48367|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 48368|       "confidence": "direct_return_type",
 48369|       "confidence_tier": "unverified_reference",
 48370|       "target_resolution": "exact",
 48371|       "evidence": [
 48372|         "return type 'CompoundStructure' directly names a Revit DB object type"
 48373|       ],
 48374|       "source_url": "https://www.revitapidocs.com/2025/a1ec47e5-c552-944d-2152-74d7bd3f2a31.htm",
 48375|       "dll_signature_verified": true,
 48376|       "dll_relationship_scope": "declared",
 48377|       "dll_semantic_verified": null,
 48378|       "dll_verified_status": "signature_verified_declared",
 48379|       "revitlookup_referenced": null,
 48380|       "revitlookup_requires_document_context": null
 48381|     },
 48382|     {
 48383|       "source": "Autodesk.Revit.DB.HostObject",
 48384|       "target": null,
 48385|       "member_name": "FindInserts",
 48386|       "member_kind": "method",
 48387|       "edge_type": "RETURNS_ELEMENT_IDS",
 48388|       "confidence": "unknown_reference",
 48389|       "confidence_tier": "unverified_reference",
 48390|       "target_resolution": "none",
 48391|       "evidence": [
 48392|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 48393|       ],
 48394|       "source_url": "https://www.revitapidocs.com/2025/58990230-38cb-3af7-fd25-96ed3215a43d.htm",
 48395|       "dll_signature_verified": true,
 48396|       "dll_relationship_scope": "declared",
 48397|       "dll_semantic_verified": null,
 48398|       "dll_verified_status": "signature_verified_declared",
 48399|       "revitlookup_referenced": true,
 48400|       "revitlookup_requires_document_context": false
 48401|     },
 48402|     {
 48403|       "source": "Autodesk.Revit.DB.HostObjectUtils",
 48404|       "target": "Autodesk.Revit.DB.Reference",
 48405|       "member_name": "GetBottomFaces",
 48406|       "member_kind": "method",
 48407|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 48408|       "confidence": "needs_runtime_validation",
 48409|       "confidence_tier": "needs_validation",
 48410|       "target_resolution": "exact",
 48411|       "evidence": [
 48412|         "return type 'IList < Reference >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 48413|       ],
 48414|       "source_url": "https://www.revitapidocs.com/2025/34737312-04d0-3550-6a42-5020c4ea2284.htm",
 48415|       "dll_signature_verified": true,
 48416|       "dll_relationship_scope": "declared",
 48417|       "dll_semantic_verified": null,
 48418|       "dll_verified_status": "signature_verified_declared",
 48419|       "revitlookup_referenced": null,
 48420|       "revitlookup_requires_document_context": null
 48421|     },
 48422|     {
 48423|       "source": "Autodesk.Revit.DB.HostObjectUtils",
 48424|       "target": "Autodesk.Revit.DB.Reference",
 48425|       "member_name": "GetSideFaces",
 48426|       "member_kind": "method",
 48427|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 48428|       "confidence": "needs_runtime_validation",
 48429|       "confidence_tier": "needs_validation",
 48430|       "target_resolution": "exact",
 48431|       "evidence": [
 48432|         "return type 'IList < Reference >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 48433|       ],
 48434|       "source_url": "https://www.revitapidocs.com/2025/589b9363-c2cc-52d9-6ba1-fc8e8f912b27.htm",
 48435|       "dll_signature_verified": true,
 48436|       "dll_relationship_scope": "declared",
 48437|       "dll_semantic_verified": null,
 48438|       "dll_verified_status": "signature_verified_declared",
 48439|       "revitlookup_referenced": null,
 48440|       "revitlookup_requires_document_context": null
 48441|     },
 48442|     {
 48443|       "source": "Autodesk.Revit.DB.HostObjectUtils",
 48444|       "target": "Autodesk.Revit.DB.Reference",
 48445|       "member_name": "GetTopFaces",
 48446|       "member_kind": "method",
 48447|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 48448|       "confidence": "needs_runtime_validation",
 48449|       "confidence_tier": "needs_validation",
 48450|       "target_resolution": "exact",
 48451|       "evidence": [
 48452|         "return type 'IList < Reference >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 48453|       ],
 48454|       "source_url": "https://www.revitapidocs.com/2025/de3ad895-337e-06f7-b1bb-edfb4fe2f35d.htm",
 48455|       "dll_signature_verified": true,
 48456|       "dll_relationship_scope": "declared",
 48457|       "dll_semantic_verified": null,
 48458|       "dll_verified_status": "signature_verified_declared",
 48459|       "revitlookup_referenced": null,
 48460|       "revitlookup_requires_document_context": null
 48461|     },
 48462|     {
 48463|       "source": "Autodesk.Revit.DB.IExportContext",
 48464|       "target": "Autodesk.Revit.DB.Material",
 48465|       "member_name": "OnMaterial",
 48466|       "member_kind": "method",
 48467|       "edge_type": "USES_MATERIAL",
 48468|       "confidence": "name_only_candidate",
 48469|       "confidence_tier": "likely",
 48470|       "target_resolution": "exact",
 48471|       "evidence": [
 48472|         "member name 'OnMaterial' matches keyword pattern /Material/ but return type 'void' gives no type-level confirmation"
 48473|       ],
 48474|       "source_url": "https://www.revitapidocs.com/2025/9d2dc6b3-21a7-5362-2bf5-2cb11b42c2d4.htm",
 48475|       "dll_signature_verified": true,
 48476|       "dll_relationship_scope": "declared",
 48477|       "dll_semantic_verified": null,
 48478|       "dll_verified_status": "signature_verified_declared",
 48479|       "revitlookup_referenced": null,
 48480|       "revitlookup_requires_document_context": null
 48481|     },
 48482|     {
 48483|       "source": "Autodesk.Revit.DB.IFCCategoryTemplate",
 48484|       "target": "Autodesk.Revit.DB.Category",
 48485|       "member_name": "GetCategoryMappingTable",
 48486|       "member_kind": "method",
 48487|       "edge_type": "HAS_CATEGORY",
 48488|       "confidence": "name_only_candidate",
 48489|       "confidence_tier": "likely",
 48490|       "target_resolution": "exact",
 48491|       "evidence": [
 48492|         "member name 'GetCategoryMappingTable' matches keyword pattern /Category/ but return type 'IDictionary < ExportIFCCategoryKey , ExportIFCCategoryInfo >' gives no type-level confirmation"
 48493|       ],
 48494|       "source_url": "https://www.revitapidocs.com/2025/cf72fe8f-b5f6-18cd-7150-9c40ea216a54.htm",
 48495|       "dll_signature_verified": true,
 48496|       "dll_relationship_scope": "declared",
 48497|       "dll_semantic_verified": null,
 48498|       "dll_verified_status": "signature_verified_declared",
 48499|       "revitlookup_referenced": null,
 48500|       "revitlookup_requires_document_context": null
 48501|     },
 48502|     {
 48503|       "source": "Autodesk.Revit.DB.IFCCategoryTemplate",
 48504|       "target": "Autodesk.Revit.DB.ExportIFCCategoryInfo",
 48505|       "member_name": "GetMappingInfoById",
 48506|       "member_kind": "method",
 48507|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 48508|       "confidence": "direct_return_type",
 48509|       "confidence_tier": "unverified_reference",
 48510|       "target_resolution": "exact",
 48511|       "evidence": [
 48512|         "return type 'ExportIFCCategoryInfo' directly names a Revit DB object type"
 48513|       ],
 48514|       "source_url": "https://www.revitapidocs.com/2025/9304b014-4d75-7ace-6a31-bea372c9f5d7.htm",
 48515|       "dll_signature_verified": true,
 48516|       "dll_relationship_scope": "declared",
 48517|       "dll_semantic_verified": null,
 48518|       "dll_verified_status": "signature_verified_declared",
 48519|       "revitlookup_referenced": null,
 48520|       "revitlookup_requires_document_context": null
 48521|     },
 48522|     {
 48523|       "source": "Autodesk.Revit.DB.IFCCategoryTemplate",
 48524|       "target": "Autodesk.Revit.DB.View",
 48525|       "member_name": "ResetActiveTemplate",
 48526|       "member_kind": "method",
 48527|       "edge_type": "CONTROLLED_BY_TEMPLATE",
 48528|       "confidence": "name_only_candidate",
 48529|       "confidence_tier": "likely",
 48530|       "target_resolution": "exact",
 48531|       "evidence": [
 48532|         "member name 'ResetActiveTemplate' matches keyword pattern /Template/ but return type 'void' gives no type-level confirmation"
 48533|       ],
 48534|       "source_url": "https://www.revitapidocs.com/2025/02e9d474-8d59-fe69-1818-64a1d414e036.htm",
 48535|       "dll_signature_verified": true,
 48536|       "dll_relationship_scope": "declared",
 48537|       "dll_semantic_verified": null,
 48538|       "dll_verified_status": "signature_verified_declared",
 48539|       "revitlookup_referenced": null,
 48540|       "revitlookup_requires_document_context": null
 48541|     },
 48542|     {
 48543|       "source": "Autodesk.Revit.DB.IFCCategoryTemplate",
 48544|       "target": "Autodesk.Revit.DB.ExportIFCCategoryInfo",
 48545|       "member_name": "ResetCategoryToDefault",
 48546|       "member_kind": "method",
 48547|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 48548|       "confidence": "direct_return_type",
 48549|       "confidence_tier": "unverified_reference",
 48550|       "target_resolution": "exact",
 48551|       "evidence": [
 48552|         "member name 'ResetCategoryToDefault' matches keyword pattern /Category/ implying target 'Category', but the actual return type 'ExportIFCCategoryInfo' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 48553|         "return type 'ExportIFCCategoryInfo' directly names a Revit DB object type"
 48554|       ],
 48555|       "source_url": "https://www.revitapidocs.com/2025/c16d57a7-f360-8776-dce8-860b85396906.htm",
 48556|       "dll_signature_verified": true,
 48557|       "dll_relationship_scope": "declared",
 48558|       "dll_semantic_verified": null,
 48559|       "dll_verified_status": "signature_verified_declared",
 48560|       "revitlookup_referenced": null,
 48561|       "revitlookup_requires_document_context": null
 48562|     },
 48563|     {
 48564|       "source": "Autodesk.Revit.DB.IFCCategoryTemplate",
 48565|       "target": "Autodesk.Revit.DB.View",
 48566|       "member_name": "SetActiveTemplate",
 48567|       "member_kind": "method",
 48568|       "edge_type": "CONTROLLED_BY_TEMPLATE",
 48569|       "confidence": "name_only_candidate",
 48570|       "confidence_tier": "likely",
 48571|       "target_resolution": "exact",
 48572|       "evidence": [
 48573|         "member name 'SetActiveTemplate' matches keyword pattern /Template/ but return type 'void' gives no type-level confirmation"
 48574|       ],
 48575|       "source_url": "https://www.revitapidocs.com/2025/9c435b89-7518-5c40-41f1-98fdf2998a14.htm",
 48576|       "dll_signature_verified": true,
 48577|       "dll_relationship_scope": "declared",
 48578|       "dll_semantic_verified": null,
 48579|       "dll_verified_status": "signature_verified_declared",
 48580|       "revitlookup_referenced": null,
 48581|       "revitlookup_requires_document_context": null
 48582|     },
 48583|     {
 48584|       "source": "Autodesk.Revit.DB.IFCCategoryTemplate",
 48585|       "target": "Autodesk.Revit.DB.Category",
 48586|       "member_name": "SetMappingInfo(IDictionary<ExportIFCCategoryKey, ExportIFCCategoryInfo>)",
 48587|       "member_kind": "method",
 48588|       "edge_type": "HAS_CATEGORY",
 48589|       "confidence": "name_only_candidate",
 48590|       "confidence_tier": "likely",
 48591|       "target_resolution": "exact",
 48592|       "evidence": [
 48593|         "member name 'SetMappingInfo(IDictionary<ExportIFCCategoryKey, ExportIFCCategoryInfo>)' matches keyword pattern /Category/ but return type 'void' gives no type-level confirmation"
 48594|       ],
 48595|       "source_url": "https://www.revitapidocs.com/2025/1db0660b-0844-dbc6-45ad-1a834c7393b6.htm",
 48596|       "dll_signature_verified": false,
 48597|       "dll_relationship_scope": null,
 48598|       "dll_semantic_verified": null,
 48599|       "dll_verified_status": "member_not_found",
 48600|       "revitlookup_referenced": null,
 48601|       "revitlookup_requires_document_context": null
 48602|     },
 48603|     {
 48604|       "source": "Autodesk.Revit.DB.IFCCategoryTemplate",
 48605|       "target": "Autodesk.Revit.DB.Category",
 48606|       "member_name": "SetMappingInfo(ExportIFCCategoryKey, ExportIFCCategoryInfo)",
 48607|       "member_kind": "method",
 48608|       "edge_type": "HAS_CATEGORY",
 48609|       "confidence": "name_only_candidate",
 48610|       "confidence_tier": "likely",
 48611|       "target_resolution": "exact",
 48612|       "evidence": [
 48613|         "member name 'SetMappingInfo(ExportIFCCategoryKey, ExportIFCCategoryInfo)' matches keyword pattern /Category/ but return type 'void' gives no type-level confirmation"
 48614|       ],
 48615|       "source_url": "https://www.revitapidocs.com/2025/23a63f03-b0dc-bc89-ec3b-0822cf1c9120.htm",
 48616|       "dll_signature_verified": false,
 48617|       "dll_relationship_scope": null,
 48618|       "dll_semantic_verified": null,
 48619|       "dll_verified_status": "member_not_found",
 48620|       "revitlookup_referenced": null,
 48621|       "revitlookup_requires_document_context": null
 48622|     },
 48623|     {
 48624|       "source": "Autodesk.Revit.DB.IFCCategoryTemplate",
 48625|       "target": "Autodesk.Revit.DB.Category",
 48626|       "member_name": "UpdateCategoryList",
 48627|       "member_kind": "method",
 48628|       "edge_type": "HAS_CATEGORY",
 48629|       "confidence": "name_only_candidate",
 48630|       "confidence_tier": "likely",
 48631|       "target_resolution": "exact",
 48632|       "evidence": [
 48633|         "member name 'UpdateCategoryList' matches keyword pattern /Category/ but return type 'void' gives no type-level confirmation"
 48634|       ],
 48635|       "source_url": "https://www.revitapidocs.com/2025/086a1260-b82d-ae84-7288-e5af0887ebf8.htm",
 48636|       "dll_signature_verified": true,
 48637|       "dll_relationship_scope": "declared",
 48638|       "dll_semantic_verified": null,
 48639|       "dll_verified_status": "signature_verified_declared",
 48640|       "revitlookup_referenced": null,
 48641|       "revitlookup_requires_document_context": null
 48642|     },
 48643|     {
 48644|       "source": "Autodesk.Revit.DB.IFCExportOptions",
 48645|       "target": null,
 48646|       "member_name": "FilterViewId",
 48647|       "member_kind": "property",
 48648|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 48649|       "confidence": "unknown_reference",
 48650|       "confidence_tier": "unverified_reference",
 48651|       "target_resolution": "none",
 48652|       "evidence": [
 48653|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 48654|       ],
 48655|       "source_url": "https://www.revitapidocs.com/2025/927884ac-60b2-fe93-faac-8212d26ebd6a.htm",
 48656|       "dll_signature_verified": true,
 48657|       "dll_relationship_scope": "declared",
 48658|       "dll_semantic_verified": null,
 48659|       "dll_verified_status": "signature_verified_declared",
 48660|       "revitlookup_referenced": null,
 48661|       "revitlookup_requires_document_context": null
 48662|     },
 48663|     {
 48664|       "source": "Autodesk.Revit.DB.IFCExportOptions",
 48665|       "target": "Autodesk.Revit.DB.Level",
 48666|       "member_name": "SpaceBoundaryLevel",
 48667|       "member_kind": "property",
 48668|       "edge_type": "ASSIGNED_TO_LEVEL",
 48669|       "confidence": "name_only_candidate",
 48670|       "confidence_tier": "likely",
 48671|       "target_resolution": "exact",
 48672|       "evidence": [
 48673|         "member name 'SpaceBoundaryLevel' matches keyword pattern /Level/ but return type 'int' gives no type-level confirmation"
 48674|       ],
 48675|       "source_url": "https://www.revitapidocs.com/2025/d9076483-6224-f329-c2e2-a0ea87e7a6fe.htm",
 48676|       "dll_signature_verified": true,
 48677|       "dll_relationship_scope": "declared",
 48678|       "dll_semantic_verified": null,
 48679|       "dll_verified_status": "signature_verified_declared",
 48680|       "revitlookup_referenced": null,
 48681|       "revitlookup_requires_document_context": null
 48682|     },
 48683|     {
 48684|       "source": "Autodesk.Revit.DB.ImageExportOptions",
 48685|       "target": "Autodesk.Revit.DB.ViewSheet",
 48686|       "member_name": "GetViewsAndSheets",
 48687|       "member_kind": "method",
 48688|       "edge_type": "PLACED_ON_SHEET",
 48689|       "confidence": "elementid_collection_with_strong_name",
 48690|       "confidence_tier": "core",
 48691|       "target_resolution": "exact",
 48692|       "evidence": [
 48693|         "member name 'GetViewsAndSheets' matches keyword pattern /Sheet/"
 48694|       ],
 48695|       "source_url": "https://www.revitapidocs.com/2025/b5e34c47-572e-8901-4b37-6ad86153a67f.htm",
 48696|       "dll_signature_verified": true,
 48697|       "dll_relationship_scope": "declared",
 48698|       "dll_semantic_verified": null,
 48699|       "dll_verified_status": "signature_verified_declared",
 48700|       "revitlookup_referenced": null,
 48701|       "revitlookup_requires_document_context": null
 48702|     },
 48703|     {
 48704|       "source": "Autodesk.Revit.DB.ImageExportOptions",
 48705|       "target": "Autodesk.Revit.DB.ViewSheet",
 48706|       "member_name": "SetViewsAndSheets",
 48707|       "member_kind": "method",
 48708|       "edge_type": "PLACED_ON_SHEET",
 48709|       "confidence": "name_only_candidate",
 48710|       "confidence_tier": "likely",
 48711|       "target_resolution": "exact",
 48712|       "evidence": [
 48713|         "member name 'SetViewsAndSheets' matches keyword pattern /Sheet/ but return type 'void' gives no type-level confirmation"
 48714|       ],
 48715|       "source_url": "https://www.revitapidocs.com/2025/f9ac839c-2722-2249-1be8-5033601b948f.htm",
 48716|       "dll_signature_verified": true,
 48717|       "dll_relationship_scope": "declared",
 48718|       "dll_semantic_verified": null,
 48719|       "dll_verified_status": "signature_verified_declared",
 48720|       "revitlookup_referenced": null,
 48721|       "revitlookup_requires_document_context": null
 48722|     },
 48723|     {
 48724|       "source": "Autodesk.Revit.DB.ImageView",
 48725|       "target": null,
 48726|       "member_name": "ImageInstanceId",
 48727|       "member_kind": "property",
 48728|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 48729|       "confidence": "unknown_reference",
 48730|       "confidence_tier": "unverified_reference",
 48731|       "target_resolution": "none",
 48732|       "evidence": [
 48733|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 48734|       ],
 48735|       "source_url": "https://www.revitapidocs.com/2025/31e60b48-a972-80fc-89ed-559068c49dec.htm",
 48736|       "dll_signature_verified": true,
 48737|       "dll_relationship_scope": "declared",
 48738|       "dll_semantic_verified": null,
 48739|       "dll_verified_status": "signature_verified_declared",
 48740|       "revitlookup_referenced": null,
 48741|       "revitlookup_requires_document_context": null
 48742|     },
 48743|     {
 48744|       "source": "Autodesk.Revit.DB.ImportInstance",
 48745|       "target": "Autodesk.Revit.DB.FamilyElementVisibility",
 48746|       "member_name": "GetVisibility",
 48747|       "member_kind": "method",
 48748|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 48749|       "confidence": "direct_return_type",
 48750|       "confidence_tier": "unverified_reference",
 48751|       "target_resolution": "exact",
 48752|       "evidence": [
 48753|         "return type 'FamilyElementVisibility' directly names a Revit DB object type"
 48754|       ],
 48755|       "source_url": "https://www.revitapidocs.com/2025/08de9969-d4c6-1893-ce64-59e5692da23f.htm",
 48756|       "dll_signature_verified": true,
 48757|       "dll_relationship_scope": "declared",
 48758|       "dll_semantic_verified": null,
 48759|       "dll_verified_status": "signature_verified_declared",
 48760|       "revitlookup_referenced": null,
```
