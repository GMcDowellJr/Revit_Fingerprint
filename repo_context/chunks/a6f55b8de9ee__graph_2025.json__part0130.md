# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 130 of 216
- Original line range: 50311-50710
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 50311|       "confidence": "unknown_reference",
 50312|       "confidence_tier": "unverified_reference",
 50313|       "target_resolution": "none",
 50314|       "evidence": [
 50315|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 50316|       ],
 50317|       "source_url": "https://www.revitapidocs.com/2025/4d0358df-6aaa-53b0-ebe0-365cba628f03.htm",
 50318|       "dll_signature_verified": true,
 50319|       "dll_relationship_scope": "declared",
 50320|       "dll_semantic_verified": null,
 50321|       "dll_verified_status": "signature_verified_declared",
 50322|       "revitlookup_referenced": null,
 50323|       "revitlookup_requires_document_context": null
 50324|     },
 50325|     {
 50326|       "source": "Autodesk.Revit.DB.LinkLoadResult",
 50327|       "target": null,
 50328|       "member_name": "ElementId",
 50329|       "member_kind": "property",
 50330|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 50331|       "confidence": "unknown_reference",
 50332|       "confidence_tier": "unverified_reference",
 50333|       "target_resolution": "none",
 50334|       "evidence": [
 50335|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 50336|       ],
 50337|       "source_url": "https://www.revitapidocs.com/2025/fbbd2c3a-435f-faa2-4284-4cf29b6fb1a2.htm",
 50338|       "dll_signature_verified": true,
 50339|       "dll_relationship_scope": "declared",
 50340|       "dll_semantic_verified": null,
 50341|       "dll_verified_status": "signature_verified_declared",
 50342|       "revitlookup_referenced": null,
 50343|       "revitlookup_requires_document_context": null
 50344|     },
 50345|     {
 50346|       "source": "Autodesk.Revit.DB.LinkLoadResult",
 50347|       "target": "Autodesk.Revit.DB.ExternalResourceReference",
 50348|       "member_name": "GetExternalResourceReference",
 50349|       "member_kind": "method",
 50350|       "edge_type": "REFERENCES",
 50351|       "confidence": "direct_return_type",
 50352|       "confidence_tier": "core",
 50353|       "target_resolution": "exact",
 50354|       "evidence": [
 50355|         "return type 'ExternalResourceReference' directly names a Revit DB object type"
 50356|       ],
 50357|       "source_url": "https://www.revitapidocs.com/2025/d2b5e2f8-f3b6-04bf-2a0e-8112998848a3.htm",
 50358|       "dll_signature_verified": true,
 50359|       "dll_relationship_scope": "declared",
 50360|       "dll_semantic_verified": null,
 50361|       "dll_verified_status": "signature_verified_declared",
 50362|       "revitlookup_referenced": null,
 50363|       "revitlookup_requires_document_context": null
 50364|     },
 50365|     {
 50366|       "source": "Autodesk.Revit.DB.LinkLoadResult",
 50367|       "target": "Autodesk.Revit.DB.ExternalResourceReference",
 50368|       "member_name": "GetExternalResourceReferencesFromFailedLoads",
 50369|       "member_kind": "method",
 50370|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 50371|       "confidence": "needs_runtime_validation",
 50372|       "confidence_tier": "needs_validation",
 50373|       "target_resolution": "exact",
 50374|       "evidence": [
 50375|         "return type 'IList < ExternalResourceReference >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 50376|       ],
 50377|       "source_url": "https://www.revitapidocs.com/2025/c80085bc-0123-6dc6-69ab-9cc2510d33d2.htm",
 50378|       "dll_signature_verified": true,
 50379|       "dll_relationship_scope": "declared",
 50380|       "dll_semantic_verified": null,
 50381|       "dll_verified_status": "signature_verified_declared",
 50382|       "revitlookup_referenced": null,
 50383|       "revitlookup_requires_document_context": null
 50384|     },
 50385|     {
 50386|       "source": "Autodesk.Revit.DB.LinkNode",
 50387|       "target": null,
 50388|       "member_name": "SymbolId",
 50389|       "member_kind": "property",
 50390|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 50391|       "confidence": "unknown_reference",
 50392|       "confidence_tier": "unverified_reference",
 50393|       "target_resolution": "none",
 50394|       "evidence": [
 50395|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 50396|       ],
 50397|       "source_url": "https://www.revitapidocs.com/2025/b12f6f45-a9c7-792c-cc45-2ecea09ad38f.htm",
 50398|       "dll_signature_verified": true,
 50399|       "dll_relationship_scope": "declared",
 50400|       "dll_semantic_verified": null,
 50401|       "dll_verified_status": "signature_verified_declared",
 50402|       "revitlookup_referenced": null,
 50403|       "revitlookup_requires_document_context": null
 50404|     },
 50405|     {
 50406|       "source": "Autodesk.Revit.DB.LinkNode",
 50407|       "target": "Autodesk.Revit.DB.Document",
 50408|       "member_name": "GetDocument",
 50409|       "member_kind": "method",
 50410|       "edge_type": "REFERENCES",
 50411|       "confidence": "direct_return_type",
 50412|       "confidence_tier": "core",
 50413|       "target_resolution": "exact",
 50414|       "evidence": [
 50415|         "return type 'Document' directly names a Revit DB object type"
 50416|       ],
 50417|       "source_url": "https://www.revitapidocs.com/2025/cdb19e4d-ab0b-c0ed-1402-c7178022071e.htm",
 50418|       "dll_signature_verified": true,
 50419|       "dll_relationship_scope": "declared",
 50420|       "dll_semantic_verified": null,
 50421|       "dll_verified_status": "signature_verified_declared",
 50422|       "revitlookup_referenced": null,
 50423|       "revitlookup_requires_document_context": null
 50424|     },
 50425|     {
 50426|       "source": "Autodesk.Revit.DB.MassInstanceUtils",
 50427|       "target": "Autodesk.Revit.DB.Level",
 50428|       "member_name": "AddMassLevelDataToMassInstance",
 50429|       "member_kind": "method",
 50430|       "edge_type": "ASSIGNED_TO_LEVEL",
 50431|       "confidence": "elementid_with_strong_name",
 50432|       "confidence_tier": "core",
 50433|       "target_resolution": "exact",
 50434|       "evidence": [
 50435|         "member name 'AddMassLevelDataToMassInstance' matches keyword pattern /Level/"
 50436|       ],
 50437|       "source_url": "https://www.revitapidocs.com/2025/fe3b251b-2677-094d-7e72-77fea0f49f24.htm",
 50438|       "dll_signature_verified": true,
 50439|       "dll_relationship_scope": "declared",
 50440|       "dll_semantic_verified": null,
 50441|       "dll_verified_status": "signature_verified_declared",
 50442|       "revitlookup_referenced": null,
 50443|       "revitlookup_requires_document_context": null
 50444|     },
 50445|     {
 50446|       "source": "Autodesk.Revit.DB.MassInstanceUtils",
 50447|       "target": null,
 50448|       "member_name": "GetJoinedElementIds",
 50449|       "member_kind": "method",
 50450|       "edge_type": "RETURNS_ELEMENT_IDS",
 50451|       "confidence": "unknown_reference",
 50452|       "confidence_tier": "unverified_reference",
 50453|       "target_resolution": "none",
 50454|       "evidence": [
 50455|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 50456|       ],
 50457|       "source_url": "https://www.revitapidocs.com/2025/19706a09-b90f-2078-cd66-488413989b5e.htm",
 50458|       "dll_signature_verified": true,
 50459|       "dll_relationship_scope": "declared",
 50460|       "dll_semantic_verified": null,
 50461|       "dll_verified_status": "signature_verified_declared",
 50462|       "revitlookup_referenced": null,
 50463|       "revitlookup_requires_document_context": null
 50464|     },
 50465|     {
 50466|       "source": "Autodesk.Revit.DB.MassInstanceUtils",
 50467|       "target": "Autodesk.Revit.DB.Level",
 50468|       "member_name": "GetMassLevelDataIds",
 50469|       "member_kind": "method",
 50470|       "edge_type": "ASSIGNED_TO_LEVEL",
 50471|       "confidence": "elementid_collection_with_strong_name",
 50472|       "confidence_tier": "core",
 50473|       "target_resolution": "exact",
 50474|       "evidence": [
 50475|         "member name 'GetMassLevelDataIds' matches keyword pattern /Level/"
 50476|       ],
 50477|       "source_url": "https://www.revitapidocs.com/2025/244c26d6-da7c-c754-3a00-4be63d59a704.htm",
 50478|       "dll_signature_verified": true,
 50479|       "dll_relationship_scope": "declared",
 50480|       "dll_semantic_verified": null,
 50481|       "dll_verified_status": "signature_verified_declared",
 50482|       "revitlookup_referenced": null,
 50483|       "revitlookup_requires_document_context": null
 50484|     },
 50485|     {
 50486|       "source": "Autodesk.Revit.DB.MassInstanceUtils",
 50487|       "target": "Autodesk.Revit.DB.Level",
 50488|       "member_name": "GetMassLevelIds",
 50489|       "member_kind": "method",
 50490|       "edge_type": "ASSIGNED_TO_LEVEL",
 50491|       "confidence": "elementid_collection_with_strong_name",
 50492|       "confidence_tier": "core",
 50493|       "target_resolution": "exact",
 50494|       "evidence": [
 50495|         "member name 'GetMassLevelIds' matches keyword pattern /Level/"
 50496|       ],
 50497|       "source_url": "https://www.revitapidocs.com/2025/627c83e6-6620-1296-9614-30d62042e062.htm",
 50498|       "dll_signature_verified": true,
 50499|       "dll_relationship_scope": "declared",
 50500|       "dll_semantic_verified": null,
 50501|       "dll_verified_status": "signature_verified_declared",
 50502|       "revitlookup_referenced": null,
 50503|       "revitlookup_requires_document_context": null
 50504|     },
 50505|     {
 50506|       "source": "Autodesk.Revit.DB.MassInstanceUtils",
 50507|       "target": "Autodesk.Revit.DB.Level",
 50508|       "member_name": "RemoveMassLevelDataFromMassInstance",
 50509|       "member_kind": "method",
 50510|       "edge_type": "ASSIGNED_TO_LEVEL",
 50511|       "confidence": "name_only_candidate",
 50512|       "confidence_tier": "likely",
 50513|       "target_resolution": "exact",
 50514|       "evidence": [
 50515|         "member name 'RemoveMassLevelDataFromMassInstance' matches keyword pattern /Level/ but return type 'void' gives no type-level confirmation"
 50516|       ],
 50517|       "source_url": "https://www.revitapidocs.com/2025/92218dd5-d331-c33a-abb2-d6f9956f9204.htm",
 50518|       "dll_signature_verified": true,
 50519|       "dll_relationship_scope": "declared",
 50520|       "dll_semantic_verified": null,
 50521|       "dll_verified_status": "signature_verified_declared",
 50522|       "revitlookup_referenced": null,
 50523|       "revitlookup_requires_document_context": null
 50524|     },
 50525|     {
 50526|       "source": "Autodesk.Revit.DB.Material",
 50527|       "target": null,
 50528|       "member_name": "AppearanceAssetId",
 50529|       "member_kind": "property",
 50530|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 50531|       "confidence": "unknown_reference",
 50532|       "confidence_tier": "unverified_reference",
 50533|       "target_resolution": "none",
 50534|       "evidence": [
 50535|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 50536|       ],
 50537|       "source_url": "https://www.revitapidocs.com/2025/d02d0677-341a-8d1a-d3eb-35ff82f01695.htm",
 50538|       "dll_signature_verified": true,
 50539|       "dll_relationship_scope": "declared",
 50540|       "dll_semantic_verified": null,
 50541|       "dll_verified_status": "signature_verified_declared",
 50542|       "revitlookup_referenced": null,
 50543|       "revitlookup_requires_document_context": null
 50544|     },
 50545|     {
 50546|       "source": "Autodesk.Revit.DB.Material",
 50547|       "target": null,
 50548|       "member_name": "CutBackgroundPatternId",
 50549|       "member_kind": "property",
 50550|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 50551|       "confidence": "unknown_reference",
 50552|       "confidence_tier": "unverified_reference",
 50553|       "target_resolution": "none",
 50554|       "evidence": [
 50555|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 50556|       ],
 50557|       "source_url": "https://www.revitapidocs.com/2025/290d0d15-afd4-b333-ff39-8d46481b1b06.htm",
 50558|       "dll_signature_verified": true,
 50559|       "dll_relationship_scope": "declared",
 50560|       "dll_semantic_verified": null,
 50561|       "dll_verified_status": "signature_verified_declared",
 50562|       "revitlookup_referenced": null,
 50563|       "revitlookup_requires_document_context": null
 50564|     },
 50565|     {
 50566|       "source": "Autodesk.Revit.DB.Material",
 50567|       "target": null,
 50568|       "member_name": "CutForegroundPatternId",
 50569|       "member_kind": "property",
 50570|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 50571|       "confidence": "unknown_reference",
 50572|       "confidence_tier": "unverified_reference",
 50573|       "target_resolution": "none",
 50574|       "evidence": [
 50575|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 50576|       ],
 50577|       "source_url": "https://www.revitapidocs.com/2025/9f03bba4-bdb1-5a2e-065e-c1d6ac04bb95.htm",
 50578|       "dll_signature_verified": true,
 50579|       "dll_relationship_scope": "declared",
 50580|       "dll_semantic_verified": null,
 50581|       "dll_verified_status": "signature_verified_declared",
 50582|       "revitlookup_referenced": null,
 50583|       "revitlookup_requires_document_context": null
 50584|     },
 50585|     {
 50586|       "source": "Autodesk.Revit.DB.Material",
 50587|       "target": "Autodesk.Revit.DB.Material",
 50588|       "member_name": "MaterialCategory",
 50589|       "member_kind": "property",
 50590|       "edge_type": "USES_MATERIAL",
 50591|       "confidence": "name_only_candidate",
 50592|       "confidence_tier": "likely",
 50593|       "target_resolution": "exact",
 50594|       "evidence": [
 50595|         "member name 'MaterialCategory' matches keyword pattern /Material/ but return type 'string' gives no type-level confirmation"
 50596|       ],
 50597|       "source_url": "https://www.revitapidocs.com/2025/75828584-b04c-7f5e-3eb1-353bca44cdec.htm",
 50598|       "dll_signature_verified": true,
 50599|       "dll_relationship_scope": "declared",
 50600|       "dll_semantic_verified": null,
 50601|       "dll_verified_status": "signature_verified_declared",
 50602|       "revitlookup_referenced": null,
 50603|       "revitlookup_requires_document_context": null
 50604|     },
 50605|     {
 50606|       "source": "Autodesk.Revit.DB.Material",
 50607|       "target": "Autodesk.Revit.DB.Material",
 50608|       "member_name": "MaterialClass",
 50609|       "member_kind": "property",
 50610|       "edge_type": "USES_MATERIAL",
 50611|       "confidence": "name_only_candidate",
 50612|       "confidence_tier": "likely",
 50613|       "target_resolution": "exact",
 50614|       "evidence": [
 50615|         "member name 'MaterialClass' matches keyword pattern /Material/ but return type 'string' gives no type-level confirmation"
 50616|       ],
 50617|       "source_url": "https://www.revitapidocs.com/2025/ad8d658d-736a-f89c-981a-555f0d69c2e5.htm",
 50618|       "dll_signature_verified": true,
 50619|       "dll_relationship_scope": "declared",
 50620|       "dll_semantic_verified": null,
 50621|       "dll_verified_status": "signature_verified_declared",
 50622|       "revitlookup_referenced": null,
 50623|       "revitlookup_requires_document_context": null
 50624|     },
 50625|     {
 50626|       "source": "Autodesk.Revit.DB.Material",
 50627|       "target": null,
 50628|       "member_name": "StructuralAssetId",
 50629|       "member_kind": "property",
 50630|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 50631|       "confidence": "unknown_reference",
 50632|       "confidence_tier": "unverified_reference",
 50633|       "target_resolution": "none",
 50634|       "evidence": [
 50635|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 50636|       ],
 50637|       "source_url": "https://www.revitapidocs.com/2025/9b6f4d74-4c3b-06cc-874c-176758d305db.htm",
 50638|       "dll_signature_verified": true,
 50639|       "dll_relationship_scope": "declared",
 50640|       "dll_semantic_verified": null,
 50641|       "dll_verified_status": "signature_verified_declared",
 50642|       "revitlookup_referenced": null,
 50643|       "revitlookup_requires_document_context": null
 50644|     },
 50645|     {
 50646|       "source": "Autodesk.Revit.DB.Material",
 50647|       "target": null,
 50648|       "member_name": "SurfaceBackgroundPatternId",
 50649|       "member_kind": "property",
 50650|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 50651|       "confidence": "unknown_reference",
 50652|       "confidence_tier": "unverified_reference",
 50653|       "target_resolution": "none",
 50654|       "evidence": [
 50655|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 50656|       ],
 50657|       "source_url": "https://www.revitapidocs.com/2025/6b7f71e4-7d89-ab30-3eda-65ca8bc038e2.htm",
 50658|       "dll_signature_verified": true,
 50659|       "dll_relationship_scope": "declared",
 50660|       "dll_semantic_verified": null,
 50661|       "dll_verified_status": "signature_verified_declared",
 50662|       "revitlookup_referenced": null,
 50663|       "revitlookup_requires_document_context": null
 50664|     },
 50665|     {
 50666|       "source": "Autodesk.Revit.DB.Material",
 50667|       "target": null,
 50668|       "member_name": "SurfaceForegroundPatternId",
 50669|       "member_kind": "property",
 50670|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 50671|       "confidence": "unknown_reference",
 50672|       "confidence_tier": "unverified_reference",
 50673|       "target_resolution": "none",
 50674|       "evidence": [
 50675|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 50676|       ],
 50677|       "source_url": "https://www.revitapidocs.com/2025/dc5602cc-54d3-2be0-bd3c-4dd9efc14010.htm",
 50678|       "dll_signature_verified": true,
 50679|       "dll_relationship_scope": "declared",
 50680|       "dll_semantic_verified": null,
 50681|       "dll_verified_status": "signature_verified_declared",
 50682|       "revitlookup_referenced": null,
 50683|       "revitlookup_requires_document_context": null
 50684|     },
 50685|     {
 50686|       "source": "Autodesk.Revit.DB.Material",
 50687|       "target": null,
 50688|       "member_name": "ThermalAssetId",
 50689|       "member_kind": "property",
 50690|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 50691|       "confidence": "unknown_reference",
 50692|       "confidence_tier": "unverified_reference",
 50693|       "target_resolution": "none",
 50694|       "evidence": [
 50695|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 50696|       ],
 50697|       "source_url": "https://www.revitapidocs.com/2025/76db3e11-fdaa-14f1-58c6-6f66ed70e0bd.htm",
 50698|       "dll_signature_verified": true,
 50699|       "dll_relationship_scope": "declared",
 50700|       "dll_semantic_verified": null,
 50701|       "dll_verified_status": "signature_verified_declared",
 50702|       "revitlookup_referenced": null,
 50703|       "revitlookup_requires_document_context": null
 50704|     },
 50705|     {
 50706|       "source": "Autodesk.Revit.DB.Material",
 50707|       "target": "Autodesk.Revit.DB.Material",
 50708|       "member_name": "ClearMaterialAspect",
 50709|       "member_kind": "method",
 50710|       "edge_type": "USES_MATERIAL",
```
