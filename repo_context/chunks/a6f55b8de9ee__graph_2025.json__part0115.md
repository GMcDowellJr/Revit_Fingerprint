# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 115 of 216
- Original line range: 44461-44860
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 44461|       "confidence": "unknown_reference",
 44462|       "confidence_tier": "unverified_reference",
 44463|       "target_resolution": "none",
 44464|       "evidence": [
 44465|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 44466|       ],
 44467|       "source_url": "https://www.revitapidocs.com/2025/3f01f592-4d76-4347-23d9-31becc3c54c0.htm",
 44468|       "dll_signature_verified": true,
 44469|       "dll_relationship_scope": "declared",
 44470|       "dll_semantic_verified": null,
 44471|       "dll_verified_status": "signature_verified_declared",
 44472|       "revitlookup_referenced": null,
 44473|       "revitlookup_requires_document_context": null
 44474|     },
 44475|     {
 44476|       "source": "Autodesk.Revit.DB.FailuresAccessor",
 44477|       "target": "Autodesk.Revit.DB.Document",
 44478|       "member_name": "GetDocument",
 44479|       "member_kind": "method",
 44480|       "edge_type": "REFERENCES",
 44481|       "confidence": "direct_return_type",
 44482|       "confidence_tier": "core",
 44483|       "target_resolution": "exact",
 44484|       "evidence": [
 44485|         "return type 'Document' directly names a Revit DB object type"
 44486|       ],
 44487|       "source_url": "https://www.revitapidocs.com/2025/f19901b5-9cba-bdbb-10a6-4ced16d2605f.htm",
 44488|       "dll_signature_verified": true,
 44489|       "dll_relationship_scope": "declared",
 44490|       "dll_semantic_verified": null,
 44491|       "dll_verified_status": "signature_verified_declared",
 44492|       "revitlookup_referenced": null,
 44493|       "revitlookup_requires_document_context": null
 44494|     },
 44495|     {
 44496|       "source": "Autodesk.Revit.DB.FailuresAccessor",
 44497|       "target": "Autodesk.Revit.DB.FailureHandlingOptions",
 44498|       "member_name": "GetFailureHandlingOptions",
 44499|       "member_kind": "method",
 44500|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 44501|       "confidence": "direct_return_type",
 44502|       "confidence_tier": "unverified_reference",
 44503|       "target_resolution": "exact",
 44504|       "evidence": [
 44505|         "return type 'FailureHandlingOptions' directly names a Revit DB object type"
 44506|       ],
 44507|       "source_url": "https://www.revitapidocs.com/2025/eff2181d-b026-4c4b-b14c-e9c62bbbc01c.htm",
 44508|       "dll_signature_verified": true,
 44509|       "dll_relationship_scope": "declared",
 44510|       "dll_semantic_verified": null,
 44511|       "dll_verified_status": "signature_verified_declared",
 44512|       "revitlookup_referenced": null,
 44513|       "revitlookup_requires_document_context": null
 44514|     },
 44515|     {
 44516|       "source": "Autodesk.Revit.DB.FailuresAccessor",
 44517|       "target": "Autodesk.Revit.DB.FailureMessageAccessor",
 44518|       "member_name": "GetFailureMessages",
 44519|       "member_kind": "method",
 44520|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 44521|       "confidence": "needs_runtime_validation",
 44522|       "confidence_tier": "needs_validation",
 44523|       "target_resolution": "exact",
 44524|       "evidence": [
 44525|         "return type 'IList < FailureMessageAccessor >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 44526|       ],
 44527|       "source_url": "https://www.revitapidocs.com/2025/f8f03cd4-a151-91c6-4569-24597604cc81.htm",
 44528|       "dll_signature_verified": true,
 44529|       "dll_relationship_scope": "declared",
 44530|       "dll_semantic_verified": null,
 44531|       "dll_verified_status": "signature_verified_declared",
 44532|       "revitlookup_referenced": null,
 44533|       "revitlookup_requires_document_context": null
 44534|     },
 44535|     {
 44536|       "source": "Autodesk.Revit.DB.FailuresAccessor",
 44537|       "target": "Autodesk.Revit.DB.FailureMessageAccessor",
 44538|       "member_name": "GetFailureMessages",
 44539|       "member_kind": "method",
 44540|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 44541|       "confidence": "needs_runtime_validation",
 44542|       "confidence_tier": "needs_validation",
 44543|       "target_resolution": "exact",
 44544|       "evidence": [
 44545|         "return type 'IList < FailureMessageAccessor >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 44546|       ],
 44547|       "source_url": "https://www.revitapidocs.com/2025/1a24ee05-1057-4638-0b15-1a0f0ef0c21d.htm",
 44548|       "dll_signature_verified": true,
 44549|       "dll_relationship_scope": "declared",
 44550|       "dll_semantic_verified": null,
 44551|       "dll_verified_status": "signature_verified_declared",
 44552|       "revitlookup_referenced": null,
 44553|       "revitlookup_requires_document_context": null
 44554|     },
 44555|     {
 44556|       "source": "Autodesk.Revit.DB.Family",
 44557|       "target": "Autodesk.Revit.DB.Category",
 44558|       "member_name": "FamilyCategory",
 44559|       "member_kind": "property",
 44560|       "edge_type": "HAS_CATEGORY",
 44561|       "confidence": "direct_return_type",
 44562|       "confidence_tier": "core",
 44563|       "target_resolution": "exact",
 44564|       "evidence": [
 44565|         "return type 'Category' directly names a Revit DB object type"
 44566|       ],
 44567|       "source_url": "https://www.revitapidocs.com/2025/e00c2b8b-b92d-526b-11b6-71c7e1d5d1b7.htm",
 44568|       "dll_signature_verified": true,
 44569|       "dll_relationship_scope": "declared",
 44570|       "dll_semantic_verified": null,
 44571|       "dll_verified_status": "signature_verified_declared",
 44572|       "revitlookup_referenced": null,
 44573|       "revitlookup_requires_document_context": null
 44574|     },
 44575|     {
 44576|       "source": "Autodesk.Revit.DB.Family",
 44577|       "target": "Autodesk.Revit.DB.Category",
 44578|       "member_name": "FamilyCategoryId",
 44579|       "member_kind": "property",
 44580|       "edge_type": "HAS_CATEGORY",
 44581|       "confidence": "elementid_with_strong_name",
 44582|       "confidence_tier": "core",
 44583|       "target_resolution": "exact",
 44584|       "evidence": [
 44585|         "member name 'FamilyCategoryId' matches keyword pattern /Category/"
 44586|       ],
 44587|       "source_url": "https://www.revitapidocs.com/2025/c1e0b7fa-8ea0-b6f6-a300-4c3e231bdb95.htm",
 44588|       "dll_signature_verified": true,
 44589|       "dll_relationship_scope": "declared",
 44590|       "dll_semantic_verified": null,
 44591|       "dll_verified_status": "signature_verified_declared",
 44592|       "revitlookup_referenced": null,
 44593|       "revitlookup_requires_document_context": null
 44594|     },
 44595|     {
 44596|       "source": "Autodesk.Revit.DB.Family",
 44597|       "target": "Autodesk.Revit.DB.Material",
 44598|       "member_name": "StructuralMaterialType",
 44599|       "member_kind": "property",
 44600|       "edge_type": "USES_MATERIAL",
 44601|       "confidence": "name_only_candidate",
 44602|       "confidence_tier": "likely",
 44603|       "target_resolution": "exact",
 44604|       "evidence": [
 44605|         "member name 'StructuralMaterialType' matches keyword pattern /Material/ but return type 'StructuralMaterialType' gives no type-level confirmation"
 44606|       ],
 44607|       "source_url": "https://www.revitapidocs.com/2025/b8770014-b218-1b8d-c996-d722828429f4.htm",
 44608|       "dll_signature_verified": true,
 44609|       "dll_relationship_scope": "declared",
 44610|       "dll_semantic_verified": null,
 44611|       "dll_verified_status": "signature_verified_declared",
 44612|       "revitlookup_referenced": null,
 44613|       "revitlookup_requires_document_context": null
 44614|     },
 44615|     {
 44616|       "source": "Autodesk.Revit.DB.Family",
 44617|       "target": null,
 44618|       "member_name": "GetFamilySymbolIds",
 44619|       "member_kind": "method",
 44620|       "edge_type": "RETURNS_ELEMENT_IDS",
 44621|       "confidence": "unknown_reference",
 44622|       "confidence_tier": "unverified_reference",
 44623|       "target_resolution": "none",
 44624|       "evidence": [
 44625|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 44626|       ],
 44627|       "source_url": "https://www.revitapidocs.com/2025/8989a269-c516-0ace-5365-864a61df1103.htm",
 44628|       "dll_signature_verified": true,
 44629|       "dll_relationship_scope": "declared",
 44630|       "dll_semantic_verified": null,
 44631|       "dll_verified_status": "signature_verified_declared",
 44632|       "revitlookup_referenced": null,
 44633|       "revitlookup_requires_document_context": null
 44634|     },
 44635|     {
 44636|       "source": "Autodesk.Revit.DB.Family",
 44637|       "target": null,
 44638|       "member_name": "GetFamilyTypeParameterValues",
 44639|       "member_kind": "method",
 44640|       "edge_type": "HAS_PARAMETER",
 44641|       "confidence": "elementid_collection_with_strong_name",
 44642|       "confidence_tier": "core",
 44643|       "target_resolution": "none",
 44644|       "evidence": [
 44645|         "member name 'GetFamilyTypeParameterValues' matches keyword pattern /Parameter/"
 44646|       ],
 44647|       "source_url": "https://www.revitapidocs.com/2025/a9c8ff23-17ec-1b87-e58a-4be589217766.htm",
 44648|       "dll_signature_verified": true,
 44649|       "dll_relationship_scope": "declared",
 44650|       "dll_semantic_verified": null,
 44651|       "dll_verified_status": "signature_verified_declared",
 44652|       "revitlookup_referenced": null,
 44653|       "revitlookup_requires_document_context": null
 44654|     },
 44655|     {
 44656|       "source": "Autodesk.Revit.DB.Family",
 44657|       "target": "Autodesk.Revit.DB.Category",
 44658|       "member_name": "IsAppropriateCategoryId",
 44659|       "member_kind": "method",
 44660|       "edge_type": "HAS_CATEGORY",
 44661|       "confidence": "name_only_candidate",
 44662|       "confidence_tier": "likely",
 44663|       "target_resolution": "exact",
 44664|       "evidence": [
 44665|         "member name 'IsAppropriateCategoryId' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 44666|       ],
 44667|       "source_url": "https://www.revitapidocs.com/2025/430baffa-8a85-0745-5c37-72ae41386b74.htm",
 44668|       "dll_signature_verified": true,
 44669|       "dll_relationship_scope": "declared",
 44670|       "dll_semantic_verified": null,
 44671|       "dll_verified_status": "signature_verified_declared",
 44672|       "revitlookup_referenced": null,
 44673|       "revitlookup_requires_document_context": null
 44674|     },
 44675|     {
 44676|       "source": "Autodesk.Revit.DB.FamilyInstance",
 44677|       "target": "Autodesk.Revit.DB.IExtension",
 44678|       "member_name": "ExtensionUtility",
 44679|       "member_kind": "property",
 44680|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 44681|       "confidence": "direct_return_type",
 44682|       "confidence_tier": "unverified_reference",
 44683|       "target_resolution": "exact",
 44684|       "evidence": [
 44685|         "return type 'IExtension' directly names a Revit DB object type"
 44686|       ],
 44687|       "source_url": "https://www.revitapidocs.com/2025/2ff87911-3c17-babc-781d-e2c68e62d4e9.htm",
 44688|       "dll_signature_verified": true,
 44689|       "dll_relationship_scope": "declared",
 44690|       "dll_semantic_verified": null,
 44691|       "dll_verified_status": "signature_verified_declared",
 44692|       "revitlookup_referenced": null,
 44693|       "revitlookup_requires_document_context": null
 44694|     },
 44695|     {
 44696|       "source": "Autodesk.Revit.DB.FamilyInstance",
 44697|       "target": "Autodesk.Revit.DB.Architecture.Room",
 44698|       "member_name": "FromRoom",
 44699|       "member_kind": "property",
 44700|       "edge_type": "REFERENCES",
 44701|       "confidence": "direct_return_type",
 44702|       "confidence_tier": "core",
 44703|       "target_resolution": "exact",
 44704|       "evidence": [
 44705|         "return type 'Room' directly names a Revit DB object type"
 44706|       ],
 44707|       "source_url": "https://www.revitapidocs.com/2025/d6658841-da29-ead4-049b-3036cbd4951a.htm",
 44708|       "dll_signature_verified": true,
 44709|       "dll_relationship_scope": "declared",
 44710|       "dll_semantic_verified": null,
 44711|       "dll_verified_status": "signature_verified_declared",
 44712|       "revitlookup_referenced": true,
 44713|       "revitlookup_requires_document_context": true
 44714|     },
 44715|     {
 44716|       "source": "Autodesk.Revit.DB.FamilyInstance",
 44717|       "target": "Autodesk.Revit.DB.Element",
 44718|       "member_name": "Host",
 44719|       "member_kind": "property",
 44720|       "edge_type": "HOSTED_BY",
 44721|       "confidence": "direct_return_type",
 44722|       "confidence_tier": "core",
 44723|       "target_resolution": "exact",
 44724|       "evidence": [
 44725|         "return type 'Element' directly names a Revit DB object type"
 44726|       ],
 44727|       "source_url": "https://www.revitapidocs.com/2025/69f30141-bd3b-8bdd-7a63-6353d4d495f9.htm",
 44728|       "dll_signature_verified": true,
 44729|       "dll_relationship_scope": "declared",
 44730|       "dll_semantic_verified": null,
 44731|       "dll_verified_status": "signature_verified_declared",
 44732|       "revitlookup_referenced": null,
 44733|       "revitlookup_requires_document_context": null
 44734|     },
 44735|     {
 44736|       "source": "Autodesk.Revit.DB.FamilyInstance",
 44737|       "target": "Autodesk.Revit.DB.Reference",
 44738|       "member_name": "HostFace",
 44739|       "member_kind": "property",
 44740|       "edge_type": "HOSTED_BY",
 44741|       "confidence": "direct_return_type",
 44742|       "confidence_tier": "core",
 44743|       "target_resolution": "exact",
 44744|       "evidence": [
 44745|         "return type 'Reference' directly names a Revit DB object type"
 44746|       ],
 44747|       "source_url": "https://www.revitapidocs.com/2025/e795508b-bb6a-4f76-e282-57aa6f7074e5.htm",
 44748|       "dll_signature_verified": true,
 44749|       "dll_relationship_scope": "declared",
 44750|       "dll_semantic_verified": null,
 44751|       "dll_verified_status": "signature_verified_declared",
 44752|       "revitlookup_referenced": null,
 44753|       "revitlookup_requires_document_context": null
 44754|     },
 44755|     {
 44756|       "source": "Autodesk.Revit.DB.FamilyInstance",
 44757|       "target": null,
 44758|       "member_name": "HostParameter",
 44759|       "member_kind": "property",
 44760|       "edge_type": "HOSTED_BY",
 44761|       "confidence": "docs_semantic_hint",
 44762|       "confidence_tier": "core",
 44763|       "target_resolution": "none",
 44764|       "evidence": [
 44765|         "member name 'HostParameter' matches keyword pattern /^GetHosted|Host/ but return type 'double' gives no type-level confirmation",
 44766|         "docs text contains relationship phrase: 'is hosted by'"
 44767|       ],
 44768|       "source_url": "https://www.revitapidocs.com/2025/bcf1a885-5015-0b87-dfe8-9109d499f4e7.htm",
 44769|       "dll_signature_verified": true,
 44770|       "dll_relationship_scope": "declared",
 44771|       "dll_semantic_verified": null,
 44772|       "dll_verified_status": "signature_verified_declared",
 44773|       "revitlookup_referenced": null,
 44774|       "revitlookup_requires_document_context": null
 44775|     },
 44776|     {
 44777|       "source": "Autodesk.Revit.DB.FamilyInstance",
 44778|       "target": "Autodesk.Revit.DB.Location",
 44779|       "member_name": "Location",
 44780|       "member_kind": "property",
 44781|       "edge_type": "REFERENCES",
 44782|       "confidence": "direct_return_type",
 44783|       "confidence_tier": "core",
 44784|       "target_resolution": "exact",
 44785|       "evidence": [
 44786|         "return type 'Location' directly names a Revit DB object type"
 44787|       ],
 44788|       "source_url": "https://www.revitapidocs.com/2025/847ff799-9b1b-0982-f55a-7273c55b196d.htm",
 44789|       "dll_signature_verified": true,
 44790|       "dll_relationship_scope": "declared",
 44791|       "dll_semantic_verified": null,
 44792|       "dll_verified_status": "signature_verified_declared",
 44793|       "revitlookup_referenced": null,
 44794|       "revitlookup_requires_document_context": null
 44795|     },
 44796|     {
 44797|       "source": "Autodesk.Revit.DB.FamilyInstance",
 44798|       "target": "Autodesk.Revit.DB.MEPModel",
 44799|       "member_name": "MEPModel",
 44800|       "member_kind": "property",
 44801|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 44802|       "confidence": "direct_return_type",
 44803|       "confidence_tier": "unverified_reference",
 44804|       "target_resolution": "exact",
 44805|       "evidence": [
 44806|         "return type 'MEPModel' directly names a Revit DB object type"
 44807|       ],
 44808|       "source_url": "https://www.revitapidocs.com/2025/34173003-db39-bfa9-fa59-f7b5ac8da794.htm",
 44809|       "dll_signature_verified": true,
 44810|       "dll_relationship_scope": "declared",
 44811|       "dll_semantic_verified": null,
 44812|       "dll_verified_status": "signature_verified_declared",
 44813|       "revitlookup_referenced": null,
 44814|       "revitlookup_requires_document_context": null
 44815|     },
 44816|     {
 44817|       "source": "Autodesk.Revit.DB.FamilyInstance",
 44818|       "target": "Autodesk.Revit.DB.Architecture.Room",
 44819|       "member_name": "Room",
 44820|       "member_kind": "property",
 44821|       "edge_type": "REFERENCES",
 44822|       "confidence": "direct_return_type",
 44823|       "confidence_tier": "core",
 44824|       "target_resolution": "exact",
 44825|       "evidence": [
 44826|         "return type 'Room' directly names a Revit DB object type"
 44827|       ],
 44828|       "source_url": "https://www.revitapidocs.com/2025/37944e7a-f298-9c25-20bb-9c0c1da46f41.htm",
 44829|       "dll_signature_verified": true,
 44830|       "dll_relationship_scope": "declared",
 44831|       "dll_semantic_verified": null,
 44832|       "dll_verified_status": "signature_verified_declared",
 44833|       "revitlookup_referenced": true,
 44834|       "revitlookup_requires_document_context": true
 44835|     },
 44836|     {
 44837|       "source": "Autodesk.Revit.DB.FamilyInstance",
 44838|       "target": "Autodesk.Revit.DB.Mechanical.Space",
 44839|       "member_name": "Space",
 44840|       "member_kind": "property",
 44841|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 44842|       "confidence": "direct_return_type",
 44843|       "confidence_tier": "unverified_reference",
 44844|       "target_resolution": "short_name_fallback",
 44845|       "evidence": [
 44846|         "return type 'Space' directly names a Revit DB object type"
 44847|       ],
 44848|       "source_url": "https://www.revitapidocs.com/2025/3c81b4e4-de4b-44df-8f80-d90c60976dec.htm",
 44849|       "dll_signature_verified": true,
 44850|       "dll_relationship_scope": "declared",
 44851|       "dll_semantic_verified": null,
 44852|       "dll_verified_status": "signature_verified_declared",
 44853|       "revitlookup_referenced": null,
 44854|       "revitlookup_requires_document_context": null
 44855|     },
 44856|     {
 44857|       "source": "Autodesk.Revit.DB.FamilyInstance",
 44858|       "target": "Autodesk.Revit.DB.Material",
 44859|       "member_name": "StructuralMaterialId",
 44860|       "member_kind": "property",
```
