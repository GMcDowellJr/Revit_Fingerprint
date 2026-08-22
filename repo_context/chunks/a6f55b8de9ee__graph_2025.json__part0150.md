# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 150 of 216
- Original line range: 58111-58510
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 58111|       "target": "Autodesk.Revit.DB.TableCellCombinedParameterData",
 58112|       "member_name": "GetCombinedParameters",
 58113|       "member_kind": "method",
 58114|       "edge_type": "HAS_PARAMETER",
 58115|       "confidence": "needs_runtime_validation",
 58116|       "confidence_tier": "needs_validation",
 58117|       "target_resolution": "exact",
 58118|       "evidence": [
 58119|         "return type 'IList < TableCellCombinedParameterData >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 58120|       ],
 58121|       "source_url": "https://www.revitapidocs.com/2025/fe772ceb-b239-4da3-e3c3-5fb4a42d1f88.htm",
 58122|       "dll_signature_verified": true,
 58123|       "dll_relationship_scope": "declared",
 58124|       "dll_semantic_verified": null,
 58125|       "dll_verified_status": "signature_verified_declared",
 58126|       "revitlookup_referenced": null,
 58127|       "revitlookup_requires_document_context": null
 58128|     },
 58129|     {
 58130|       "source": "Autodesk.Revit.DB.ScheduleField",
 58131|       "target": "Autodesk.Revit.DB.CustomFieldData",
 58132|       "member_name": "GetCustomFieldData",
 58133|       "member_kind": "method",
 58134|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58135|       "confidence": "direct_return_type",
 58136|       "confidence_tier": "unverified_reference",
 58137|       "target_resolution": "exact",
 58138|       "evidence": [
 58139|         "return type 'CustomFieldData' directly names a Revit DB object type"
 58140|       ],
 58141|       "source_url": "https://www.revitapidocs.com/2025/44e67b30-ddb3-474a-7f66-34fc7b901ed9.htm",
 58142|       "dll_signature_verified": true,
 58143|       "dll_relationship_scope": "declared",
 58144|       "dll_semantic_verified": null,
 58145|       "dll_verified_status": "signature_verified_declared",
 58146|       "revitlookup_referenced": null,
 58147|       "revitlookup_requires_document_context": null
 58148|     },
 58149|     {
 58150|       "source": "Autodesk.Revit.DB.ScheduleField",
 58151|       "target": "Autodesk.Revit.DB.SchedulableField",
 58152|       "member_name": "GetSchedulableField",
 58153|       "member_kind": "method",
 58154|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58155|       "confidence": "direct_return_type",
 58156|       "confidence_tier": "unverified_reference",
 58157|       "target_resolution": "exact",
 58158|       "evidence": [
 58159|         "return type 'SchedulableField' directly names a Revit DB object type"
 58160|       ],
 58161|       "source_url": "https://www.revitapidocs.com/2025/cf6a6ae1-a693-a35b-3051-b34475ea574c.htm",
 58162|       "dll_signature_verified": true,
 58163|       "dll_relationship_scope": "declared",
 58164|       "dll_semantic_verified": null,
 58165|       "dll_verified_status": "signature_verified_declared",
 58166|       "revitlookup_referenced": null,
 58167|       "revitlookup_requires_document_context": null
 58168|     },
 58169|     {
 58170|       "source": "Autodesk.Revit.DB.ScheduleField",
 58171|       "target": "Autodesk.Revit.DB.TableCellStyle",
 58172|       "member_name": "GetStyle",
 58173|       "member_kind": "method",
 58174|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58175|       "confidence": "direct_return_type",
 58176|       "confidence_tier": "unverified_reference",
 58177|       "target_resolution": "exact",
 58178|       "evidence": [
 58179|         "return type 'TableCellStyle' directly names a Revit DB object type"
 58180|       ],
 58181|       "source_url": "https://www.revitapidocs.com/2025/9f52cffd-3219-fc71-df91-0302a56cc299.htm",
 58182|       "dll_signature_verified": true,
 58183|       "dll_relationship_scope": "declared",
 58184|       "dll_semantic_verified": null,
 58185|       "dll_verified_status": "signature_verified_declared",
 58186|       "revitlookup_referenced": null,
 58187|       "revitlookup_requires_document_context": null
 58188|     },
 58189|     {
 58190|       "source": "Autodesk.Revit.DB.ScheduleField",
 58191|       "target": null,
 58192|       "member_name": "IsValidCombinedParameters",
 58193|       "member_kind": "method",
 58194|       "edge_type": "HAS_PARAMETER",
 58195|       "confidence": "name_only_candidate",
 58196|       "confidence_tier": "likely",
 58197|       "target_resolution": "none",
 58198|       "evidence": [
 58199|         "member name 'IsValidCombinedParameters' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 58200|       ],
 58201|       "source_url": "https://www.revitapidocs.com/2025/a8021755-2f5d-719b-23d5-a613ec5957a6.htm",
 58202|       "dll_signature_verified": true,
 58203|       "dll_relationship_scope": "declared",
 58204|       "dll_semantic_verified": null,
 58205|       "dll_verified_status": "signature_verified_declared",
 58206|       "revitlookup_referenced": null,
 58207|       "revitlookup_requires_document_context": null
 58208|     },
 58209|     {
 58210|       "source": "Autodesk.Revit.DB.ScheduleField",
 58211|       "target": null,
 58212|       "member_name": "SetCombinedParameters",
 58213|       "member_kind": "method",
 58214|       "edge_type": "HAS_PARAMETER",
 58215|       "confidence": "name_only_candidate",
 58216|       "confidence_tier": "likely",
 58217|       "target_resolution": "none",
 58218|       "evidence": [
 58219|         "member name 'SetCombinedParameters' matches keyword pattern /Parameter/ but return type 'void' gives no type-level confirmation"
 58220|       ],
 58221|       "source_url": "https://www.revitapidocs.com/2025/b216f232-52b8-fbff-a0f7-649834dd213e.htm",
 58222|       "dll_signature_verified": true,
 58223|       "dll_relationship_scope": "declared",
 58224|       "dll_semantic_verified": null,
 58225|       "dll_verified_status": "signature_verified_declared",
 58226|       "revitlookup_referenced": null,
 58227|       "revitlookup_requires_document_context": null
 58228|     },
 58229|     {
 58230|       "source": "Autodesk.Revit.DB.ScheduleFieldId",
 58231|       "target": "Autodesk.Revit.DB.ScheduleFieldId",
 58232|       "member_name": "InvalidScheduleFieldId",
 58233|       "member_kind": "property",
 58234|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58235|       "confidence": "direct_return_type",
 58236|       "confidence_tier": "unverified_reference",
 58237|       "target_resolution": "exact",
 58238|       "evidence": [
 58239|         "return type 'ScheduleFieldId' directly names a Revit DB object type"
 58240|       ],
 58241|       "source_url": "https://www.revitapidocs.com/2025/c85dab45-d373-be75-c2d7-da14eded967c.htm",
 58242|       "dll_signature_verified": true,
 58243|       "dll_relationship_scope": "declared",
 58244|       "dll_semantic_verified": null,
 58245|       "dll_verified_status": "signature_verified_declared",
 58246|       "revitlookup_referenced": null,
 58247|       "revitlookup_requires_document_context": null
 58248|     },
 58249|     {
 58250|       "source": "Autodesk.Revit.DB.ScheduleFilter",
 58251|       "target": "Autodesk.Revit.DB.ScheduleFieldId",
 58252|       "member_name": "FieldId",
 58253|       "member_kind": "property",
 58254|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58255|       "confidence": "direct_return_type",
 58256|       "confidence_tier": "unverified_reference",
 58257|       "target_resolution": "exact",
 58258|       "evidence": [
 58259|         "return type 'ScheduleFieldId' directly names a Revit DB object type"
 58260|       ],
 58261|       "source_url": "https://www.revitapidocs.com/2025/c11c4781-9acd-baf9-692d-93bf4ab9c86e.htm",
 58262|       "dll_signature_verified": true,
 58263|       "dll_relationship_scope": "declared",
 58264|       "dll_semantic_verified": null,
 58265|       "dll_verified_status": "signature_verified_declared",
 58266|       "revitlookup_referenced": null,
 58267|       "revitlookup_requires_document_context": null
 58268|     },
 58269|     {
 58270|       "source": "Autodesk.Revit.DB.ScheduleFilter",
 58271|       "target": null,
 58272|       "member_name": "GetElementIdValue",
 58273|       "member_kind": "method",
 58274|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 58275|       "confidence": "unknown_reference",
 58276|       "confidence_tier": "unverified_reference",
 58277|       "target_resolution": "none",
 58278|       "evidence": [
 58279|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 58280|       ],
 58281|       "source_url": "https://www.revitapidocs.com/2025/3ee4da28-2a21-f9a3-0b58-03286ec21bfc.htm",
 58282|       "dll_signature_verified": true,
 58283|       "dll_relationship_scope": "declared",
 58284|       "dll_semantic_verified": null,
 58285|       "dll_verified_status": "signature_verified_declared",
 58286|       "revitlookup_referenced": null,
 58287|       "revitlookup_requires_document_context": null
 58288|     },
 58289|     {
 58290|       "source": "Autodesk.Revit.DB.ScheduleSheetInstance",
 58291|       "target": null,
 58292|       "member_name": "ScheduleId",
 58293|       "member_kind": "property",
 58294|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 58295|       "confidence": "unknown_reference",
 58296|       "confidence_tier": "unverified_reference",
 58297|       "target_resolution": "none",
 58298|       "evidence": [
 58299|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 58300|       ],
 58301|       "source_url": "https://www.revitapidocs.com/2025/368b680f-2b6e-9269-1f64-fa8e7e09b2cc.htm",
 58302|       "dll_signature_verified": true,
 58303|       "dll_relationship_scope": "declared",
 58304|       "dll_semantic_verified": null,
 58305|       "dll_verified_status": "signature_verified_declared",
 58306|       "revitlookup_referenced": null,
 58307|       "revitlookup_requires_document_context": null
 58308|     },
 58309|     {
 58310|       "source": "Autodesk.Revit.DB.ScheduleSortGroupField",
 58311|       "target": "Autodesk.Revit.DB.ScheduleFieldId",
 58312|       "member_name": "FieldId",
 58313|       "member_kind": "property",
 58314|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58315|       "confidence": "direct_return_type",
 58316|       "confidence_tier": "unverified_reference",
 58317|       "target_resolution": "exact",
 58318|       "evidence": [
 58319|         "return type 'ScheduleFieldId' directly names a Revit DB object type"
 58320|       ],
 58321|       "source_url": "https://www.revitapidocs.com/2025/3ba02d83-443a-2e7f-fa12-6ce9f30e531a.htm",
 58322|       "dll_signature_verified": true,
 58323|       "dll_relationship_scope": "declared",
 58324|       "dll_semantic_verified": null,
 58325|       "dll_verified_status": "signature_verified_declared",
 58326|       "revitlookup_referenced": null,
 58327|       "revitlookup_requires_document_context": null
 58328|     },
 58329|     {
 58330|       "source": "Autodesk.Revit.DB.Segment",
 58331|       "target": "Autodesk.Revit.DB.Material",
 58332|       "member_name": "MaterialId",
 58333|       "member_kind": "property",
 58334|       "edge_type": "USES_MATERIAL",
 58335|       "confidence": "elementid_with_strong_name",
 58336|       "confidence_tier": "core",
 58337|       "target_resolution": "exact",
 58338|       "evidence": [
 58339|         "member name 'MaterialId' matches keyword pattern /Material/"
 58340|       ],
 58341|       "source_url": "https://www.revitapidocs.com/2025/bede9529-90c6-a4ad-0ca6-c74115d80a82.htm",
 58342|       "dll_signature_verified": true,
 58343|       "dll_relationship_scope": "declared",
 58344|       "dll_semantic_verified": null,
 58345|       "dll_verified_status": "signature_verified_declared",
 58346|       "revitlookup_referenced": null,
 58347|       "revitlookup_requires_document_context": null
 58348|     },
 58349|     {
 58350|       "source": "Autodesk.Revit.DB.Segment",
 58351|       "target": "Autodesk.Revit.DB.MEPSize",
 58352|       "member_name": "GetSizes",
 58353|       "member_kind": "method",
 58354|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58355|       "confidence": "needs_runtime_validation",
 58356|       "confidence_tier": "needs_validation",
 58357|       "target_resolution": "exact",
 58358|       "evidence": [
 58359|         "return type 'ICollection < MEPSize >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 58360|       ],
 58361|       "source_url": "https://www.revitapidocs.com/2025/9e4a93cb-f0bb-565b-25ee-4aab0a36e4f0.htm",
 58362|       "dll_signature_verified": true,
 58363|       "dll_relationship_scope": "declared",
 58364|       "dll_semantic_verified": null,
 58365|       "dll_verified_status": "signature_verified_declared",
 58366|       "revitlookup_referenced": null,
 58367|       "revitlookup_requires_document_context": null
 58368|     },
 58369|     {
 58370|       "source": "Autodesk.Revit.DB.SelectionFilterElement",
 58371|       "target": null,
 58372|       "member_name": "GetElementIds",
 58373|       "member_kind": "method",
 58374|       "edge_type": "RETURNS_ELEMENT_IDS",
 58375|       "confidence": "unknown_reference",
 58376|       "confidence_tier": "unverified_reference",
 58377|       "target_resolution": "none",
 58378|       "evidence": [
 58379|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 58380|       ],
 58381|       "source_url": "https://www.revitapidocs.com/2025/42b70399-d151-8a68-8df6-546492c7091d.htm",
 58382|       "dll_signature_verified": true,
 58383|       "dll_relationship_scope": "declared",
 58384|       "dll_semantic_verified": null,
 58385|       "dll_verified_status": "signature_verified_declared",
 58386|       "revitlookup_referenced": null,
 58387|       "revitlookup_requires_document_context": null
 58388|     },
 58389|     {
 58390|       "source": "Autodesk.Revit.DB.Settings",
 58391|       "target": "Autodesk.Revit.DB.Categories",
 58392|       "member_name": "Categories",
 58393|       "member_kind": "property",
 58394|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58395|       "confidence": "direct_return_type",
 58396|       "confidence_tier": "unverified_reference",
 58397|       "target_resolution": "exact",
 58398|       "evidence": [
 58399|         "return type 'Categories' directly names a Revit DB object type"
 58400|       ],
 58401|       "source_url": "https://www.revitapidocs.com/2025/66cb17a0-83b1-3aa7-ee9b-42f5d8dafd25.htm",
 58402|       "dll_signature_verified": true,
 58403|       "dll_relationship_scope": "declared",
 58404|       "dll_semantic_verified": null,
 58405|       "dll_verified_status": "signature_verified_declared",
 58406|       "revitlookup_referenced": null,
 58407|       "revitlookup_requires_document_context": null
 58408|     },
 58409|     {
 58410|       "source": "Autodesk.Revit.DB.Settings",
 58411|       "target": "Autodesk.Revit.DB.Electrical.ElectricalSetting",
 58412|       "member_name": "ElectricalSetting",
 58413|       "member_kind": "property",
 58414|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58415|       "confidence": "direct_return_type",
 58416|       "confidence_tier": "unverified_reference",
 58417|       "target_resolution": "short_name_fallback",
 58418|       "evidence": [
 58419|         "return type 'ElectricalSetting' directly names a Revit DB object type"
 58420|       ],
 58421|       "source_url": "https://www.revitapidocs.com/2025/9bbcc232-2cc1-ebeb-2390-677322054a38.htm",
 58422|       "dll_signature_verified": true,
 58423|       "dll_relationship_scope": "declared",
 58424|       "dll_semantic_verified": null,
 58425|       "dll_verified_status": "signature_verified_declared",
 58426|       "revitlookup_referenced": null,
 58427|       "revitlookup_requires_document_context": null
 58428|     },
 58429|     {
 58430|       "source": "Autodesk.Revit.DB.Settings",
 58431|       "target": "Autodesk.Revit.DB.TilePatterns",
 58432|       "member_name": "TilePatterns",
 58433|       "member_kind": "property",
 58434|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58435|       "confidence": "direct_return_type",
 58436|       "confidence_tier": "unverified_reference",
 58437|       "target_resolution": "exact",
 58438|       "evidence": [
 58439|         "return type 'TilePatterns' directly names a Revit DB object type"
 58440|       ],
 58441|       "source_url": "https://www.revitapidocs.com/2025/42ea5612-6dbf-657a-b8b9-6cb64bdd5ff2.htm",
 58442|       "dll_signature_verified": true,
 58443|       "dll_relationship_scope": "declared",
 58444|       "dll_semantic_verified": null,
 58445|       "dll_verified_status": "signature_verified_declared",
 58446|       "revitlookup_referenced": null,
 58447|       "revitlookup_requires_document_context": null
 58448|     },
 58449|     {
 58450|       "source": "Autodesk.Revit.DB.SharedParameterApplicableRule",
 58451|       "target": null,
 58452|       "member_name": "ParameterName",
 58453|       "member_kind": "property",
 58454|       "edge_type": "HAS_PARAMETER",
 58455|       "confidence": "name_only_candidate",
 58456|       "confidence_tier": "likely",
 58457|       "target_resolution": "none",
 58458|       "evidence": [
 58459|         "member name 'ParameterName' matches keyword pattern /Parameter/ but return type 'string' gives no type-level confirmation"
 58460|       ],
 58461|       "source_url": "https://www.revitapidocs.com/2025/2392a8da-efd4-3571-4cc6-cae5e05d3d02.htm",
 58462|       "dll_signature_verified": true,
 58463|       "dll_relationship_scope": "declared",
 58464|       "dll_semantic_verified": null,
 58465|       "dll_verified_status": "signature_verified_declared",
 58466|       "revitlookup_referenced": null,
 58467|       "revitlookup_requires_document_context": null
 58468|     },
 58469|     {
 58470|       "source": "Autodesk.Revit.DB.Sketch",
 58471|       "target": null,
 58472|       "member_name": "OwnerId",
 58473|       "member_kind": "property",
 58474|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 58475|       "confidence": "unknown_reference",
 58476|       "confidence_tier": "unverified_reference",
 58477|       "target_resolution": "none",
 58478|       "evidence": [
 58479|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 58480|       ],
 58481|       "source_url": "https://www.revitapidocs.com/2025/68849b64-a5c0-ad3c-b11a-59b80bfcc0da.htm",
 58482|       "dll_signature_verified": true,
 58483|       "dll_relationship_scope": "declared",
 58484|       "dll_semantic_verified": null,
 58485|       "dll_verified_status": "signature_verified_declared",
 58486|       "revitlookup_referenced": null,
 58487|       "revitlookup_requires_document_context": null
 58488|     },
 58489|     {
 58490|       "source": "Autodesk.Revit.DB.Sketch",
 58491|       "target": "Autodesk.Revit.DB.SketchPlane",
 58492|       "member_name": "SketchPlane",
 58493|       "member_kind": "property",
 58494|       "edge_type": "REFERENCES",
 58495|       "confidence": "direct_return_type",
 58496|       "confidence_tier": "core",
 58497|       "target_resolution": "exact",
 58498|       "evidence": [
 58499|         "return type 'SketchPlane' directly names a Revit DB object type"
 58500|       ],
 58501|       "source_url": "https://www.revitapidocs.com/2025/d463203b-c692-7217-09fc-eb4110509145.htm",
 58502|       "dll_signature_verified": true,
 58503|       "dll_relationship_scope": "declared",
 58504|       "dll_semantic_verified": null,
 58505|       "dll_verified_status": "signature_verified_declared",
 58506|       "revitlookup_referenced": null,
 58507|       "revitlookup_requires_document_context": null
 58508|     },
 58509|     {
 58510|       "source": "Autodesk.Revit.DB.Sketch",
```
