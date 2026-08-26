# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 148 of 216
- Original line range: 57331-57730
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 57331|       "target": "Autodesk.Revit.DB.ScheduleField",
 57332|       "member_name": "AddField",
 57333|       "member_kind": "method",
 57334|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57335|       "confidence": "direct_return_type",
 57336|       "confidence_tier": "unverified_reference",
 57337|       "target_resolution": "exact",
 57338|       "evidence": [
 57339|         "return type 'ScheduleField' directly names a Revit DB object type"
 57340|       ],
 57341|       "source_url": "https://www.revitapidocs.com/2025/2e28946e-7264-977b-c868-996b4a839048.htm",
 57342|       "dll_signature_verified": true,
 57343|       "dll_relationship_scope": "declared",
 57344|       "dll_semantic_verified": null,
 57345|       "dll_verified_status": "signature_verified_declared",
 57346|       "revitlookup_referenced": null,
 57347|       "revitlookup_requires_document_context": null
 57348|     },
 57349|     {
 57350|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57351|       "target": "Autodesk.Revit.DB.ScheduleField",
 57352|       "member_name": "AddField",
 57353|       "member_kind": "method",
 57354|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57355|       "confidence": "direct_return_type",
 57356|       "confidence_tier": "unverified_reference",
 57357|       "target_resolution": "exact",
 57358|       "evidence": [
 57359|         "return type 'ScheduleField' directly names a Revit DB object type"
 57360|       ],
 57361|       "source_url": "https://www.revitapidocs.com/2025/35b2f716-edd7-8d1b-1c16-dd52be928be7.htm",
 57362|       "dll_signature_verified": true,
 57363|       "dll_relationship_scope": "declared",
 57364|       "dll_semantic_verified": null,
 57365|       "dll_verified_status": "signature_verified_declared",
 57366|       "revitlookup_referenced": null,
 57367|       "revitlookup_requires_document_context": null
 57368|     },
 57369|     {
 57370|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57371|       "target": "Autodesk.Revit.DB.ScheduleField",
 57372|       "member_name": "AddField",
 57373|       "member_kind": "method",
 57374|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57375|       "confidence": "direct_return_type",
 57376|       "confidence_tier": "unverified_reference",
 57377|       "target_resolution": "exact",
 57378|       "evidence": [
 57379|         "return type 'ScheduleField' directly names a Revit DB object type"
 57380|       ],
 57381|       "source_url": "https://www.revitapidocs.com/2025/bf7f777f-c9c1-5037-afeb-ab291bb24197.htm",
 57382|       "dll_signature_verified": true,
 57383|       "dll_relationship_scope": "declared",
 57384|       "dll_semantic_verified": null,
 57385|       "dll_verified_status": "signature_verified_declared",
 57386|       "revitlookup_referenced": null,
 57387|       "revitlookup_requires_document_context": null
 57388|     },
 57389|     {
 57390|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57391|       "target": null,
 57392|       "member_name": "AddSortGroupField",
 57393|       "member_kind": "method",
 57394|       "edge_type": "MEMBER_OF_GROUP",
 57395|       "confidence": "name_only_candidate",
 57396|       "confidence_tier": "likely",
 57397|       "target_resolution": "none",
 57398|       "evidence": [
 57399|         "member name 'AddSortGroupField' matches keyword pattern /^GetMember|Group/ but return type 'void' gives no type-level confirmation"
 57400|       ],
 57401|       "source_url": "https://www.revitapidocs.com/2025/dec080e9-104d-1e1b-b436-b9d60a322815.htm",
 57402|       "dll_signature_verified": true,
 57403|       "dll_relationship_scope": "declared",
 57404|       "dll_semantic_verified": null,
 57405|       "dll_verified_status": "signature_verified_declared",
 57406|       "revitlookup_referenced": null,
 57407|       "revitlookup_requires_document_context": null
 57408|     },
 57409|     {
 57410|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57411|       "target": null,
 57412|       "member_name": "CanFilterByGlobalParameters",
 57413|       "member_kind": "method",
 57414|       "edge_type": "HAS_PARAMETER",
 57415|       "confidence": "name_only_candidate",
 57416|       "confidence_tier": "likely",
 57417|       "target_resolution": "none",
 57418|       "evidence": [
 57419|         "member name 'CanFilterByGlobalParameters' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 57420|       ],
 57421|       "source_url": "https://www.revitapidocs.com/2025/0ab1f8d7-489c-ac18-d262-be359377e523.htm",
 57422|       "dll_signature_verified": true,
 57423|       "dll_relationship_scope": "declared",
 57424|       "dll_semantic_verified": null,
 57425|       "dll_verified_status": "signature_verified_declared",
 57426|       "revitlookup_referenced": true,
 57427|       "revitlookup_requires_document_context": false
 57428|     },
 57429|     {
 57430|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57431|       "target": null,
 57432|       "member_name": "CanFilterByParameterExistence",
 57433|       "member_kind": "method",
 57434|       "edge_type": "HAS_PARAMETER",
 57435|       "confidence": "name_only_candidate",
 57436|       "confidence_tier": "likely",
 57437|       "target_resolution": "none",
 57438|       "evidence": [
 57439|         "member name 'CanFilterByParameterExistence' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 57440|       ],
 57441|       "source_url": "https://www.revitapidocs.com/2025/76000e8b-6c6a-fe38-379c-0a6ee7332c90.htm",
 57442|       "dll_signature_verified": true,
 57443|       "dll_relationship_scope": "declared",
 57444|       "dll_semantic_verified": null,
 57445|       "dll_verified_status": "signature_verified_declared",
 57446|       "revitlookup_referenced": true,
 57447|       "revitlookup_requires_document_context": false
 57448|     },
 57449|     {
 57450|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57451|       "target": null,
 57452|       "member_name": "ClearSortGroupFields",
 57453|       "member_kind": "method",
 57454|       "edge_type": "MEMBER_OF_GROUP",
 57455|       "confidence": "name_only_candidate",
 57456|       "confidence_tier": "likely",
 57457|       "target_resolution": "none",
 57458|       "evidence": [
 57459|         "member name 'ClearSortGroupFields' matches keyword pattern /^GetMember|Group/ but return type 'void' gives no type-level confirmation"
 57460|       ],
 57461|       "source_url": "https://www.revitapidocs.com/2025/645645fa-52b8-b747-e3d4-b5428962a4bc.htm",
 57462|       "dll_signature_verified": true,
 57463|       "dll_relationship_scope": "declared",
 57464|       "dll_semantic_verified": null,
 57465|       "dll_verified_status": "signature_verified_declared",
 57466|       "revitlookup_referenced": null,
 57467|       "revitlookup_requires_document_context": null
 57468|     },
 57469|     {
 57470|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57471|       "target": "Autodesk.Revit.DB.ScheduleField",
 57472|       "member_name": "GetField",
 57473|       "member_kind": "method",
 57474|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57475|       "confidence": "direct_return_type",
 57476|       "confidence_tier": "unverified_reference",
 57477|       "target_resolution": "exact",
 57478|       "evidence": [
 57479|         "return type 'ScheduleField' directly names a Revit DB object type"
 57480|       ],
 57481|       "source_url": "https://www.revitapidocs.com/2025/7c22bcc4-c5be-daea-fb4c-77ef3a7773ab.htm",
 57482|       "dll_signature_verified": true,
 57483|       "dll_relationship_scope": "declared",
 57484|       "dll_semantic_verified": null,
 57485|       "dll_verified_status": "signature_verified_declared",
 57486|       "revitlookup_referenced": true,
 57487|       "revitlookup_requires_document_context": false
 57488|     },
 57489|     {
 57490|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57491|       "target": "Autodesk.Revit.DB.ScheduleField",
 57492|       "member_name": "GetField",
 57493|       "member_kind": "method",
 57494|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57495|       "confidence": "direct_return_type",
 57496|       "confidence_tier": "unverified_reference",
 57497|       "target_resolution": "exact",
 57498|       "evidence": [
 57499|         "return type 'ScheduleField' directly names a Revit DB object type"
 57500|       ],
 57501|       "source_url": "https://www.revitapidocs.com/2025/3507e623-c9a5-a82b-5c44-15756cdc0c3a.htm",
 57502|       "dll_signature_verified": true,
 57503|       "dll_relationship_scope": "declared",
 57504|       "dll_semantic_verified": null,
 57505|       "dll_verified_status": "signature_verified_declared",
 57506|       "revitlookup_referenced": true,
 57507|       "revitlookup_requires_document_context": false
 57508|     },
 57509|     {
 57510|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57511|       "target": "Autodesk.Revit.DB.ScheduleFieldId",
 57512|       "member_name": "GetFieldId",
 57513|       "member_kind": "method",
 57514|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57515|       "confidence": "direct_return_type",
 57516|       "confidence_tier": "unverified_reference",
 57517|       "target_resolution": "exact",
 57518|       "evidence": [
 57519|         "return type 'ScheduleFieldId' directly names a Revit DB object type"
 57520|       ],
 57521|       "source_url": "https://www.revitapidocs.com/2025/c48bf97b-6d47-7b55-6844-91f9c1da85e0.htm",
 57522|       "dll_signature_verified": true,
 57523|       "dll_relationship_scope": "declared",
 57524|       "dll_semantic_verified": null,
 57525|       "dll_verified_status": "signature_verified_declared",
 57526|       "revitlookup_referenced": true,
 57527|       "revitlookup_requires_document_context": false
 57528|     },
 57529|     {
 57530|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57531|       "target": "Autodesk.Revit.DB.ScheduleFieldId",
 57532|       "member_name": "GetFieldOrder",
 57533|       "member_kind": "method",
 57534|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57535|       "confidence": "needs_runtime_validation",
 57536|       "confidence_tier": "needs_validation",
 57537|       "target_resolution": "exact",
 57538|       "evidence": [
 57539|         "return type 'IList < ScheduleFieldId >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 57540|       ],
 57541|       "source_url": "https://www.revitapidocs.com/2025/3549fb1c-fea6-5b64-a8a9-700337a3907e.htm",
 57542|       "dll_signature_verified": true,
 57543|       "dll_relationship_scope": "declared",
 57544|       "dll_semantic_verified": null,
 57545|       "dll_verified_status": "signature_verified_declared",
 57546|       "revitlookup_referenced": null,
 57547|       "revitlookup_requires_document_context": null
 57548|     },
 57549|     {
 57550|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57551|       "target": "Autodesk.Revit.DB.ScheduleFilter",
 57552|       "member_name": "GetFilter",
 57553|       "member_kind": "method",
 57554|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57555|       "confidence": "direct_return_type",
 57556|       "confidence_tier": "unverified_reference",
 57557|       "target_resolution": "exact",
 57558|       "evidence": [
 57559|         "return type 'ScheduleFilter' directly names a Revit DB object type"
 57560|       ],
 57561|       "source_url": "https://www.revitapidocs.com/2025/bfd1a738-9b18-d4dd-f487-eac4566e8484.htm",
 57562|       "dll_signature_verified": true,
 57563|       "dll_relationship_scope": "declared",
 57564|       "dll_semantic_verified": null,
 57565|       "dll_verified_status": "signature_verified_declared",
 57566|       "revitlookup_referenced": true,
 57567|       "revitlookup_requires_document_context": false
 57568|     },
 57569|     {
 57570|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57571|       "target": "Autodesk.Revit.DB.ScheduleFilter",
 57572|       "member_name": "GetFilters",
 57573|       "member_kind": "method",
 57574|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57575|       "confidence": "needs_runtime_validation",
 57576|       "confidence_tier": "needs_validation",
 57577|       "target_resolution": "exact",
 57578|       "evidence": [
 57579|         "return type 'IList < ScheduleFilter >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 57580|       ],
 57581|       "source_url": "https://www.revitapidocs.com/2025/6db26e07-dd4c-d1b8-1bae-621f14330af5.htm",
 57582|       "dll_signature_verified": true,
 57583|       "dll_relationship_scope": "declared",
 57584|       "dll_semantic_verified": null,
 57585|       "dll_verified_status": "signature_verified_declared",
 57586|       "revitlookup_referenced": null,
 57587|       "revitlookup_requires_document_context": null
 57588|     },
 57589|     {
 57590|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57591|       "target": "Autodesk.Revit.DB.SchedulableField",
 57592|       "member_name": "GetSchedulableFields",
 57593|       "member_kind": "method",
 57594|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57595|       "confidence": "needs_runtime_validation",
 57596|       "confidence_tier": "needs_validation",
 57597|       "target_resolution": "exact",
 57598|       "evidence": [
 57599|         "return type 'IList < SchedulableField >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 57600|       ],
 57601|       "source_url": "https://www.revitapidocs.com/2025/66f9648b-dd64-6ff5-ef89-4f1d6b5c4a23.htm",
 57602|       "dll_signature_verified": true,
 57603|       "dll_relationship_scope": "declared",
 57604|       "dll_semantic_verified": null,
 57605|       "dll_verified_status": "signature_verified_declared",
 57606|       "revitlookup_referenced": null,
 57607|       "revitlookup_requires_document_context": null
 57608|     },
 57609|     {
 57610|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57611|       "target": "Autodesk.Revit.DB.ScheduleSortGroupField",
 57612|       "member_name": "GetSortGroupField",
 57613|       "member_kind": "method",
 57614|       "edge_type": "MEMBER_OF_GROUP",
 57615|       "confidence": "direct_return_type",
 57616|       "confidence_tier": "core",
 57617|       "target_resolution": "exact",
 57618|       "evidence": [
 57619|         "return type 'ScheduleSortGroupField' directly names a Revit DB object type"
 57620|       ],
 57621|       "source_url": "https://www.revitapidocs.com/2025/169f1ab2-6b87-9e27-ae4d-ec36bc463f44.htm",
 57622|       "dll_signature_verified": true,
 57623|       "dll_relationship_scope": "declared",
 57624|       "dll_semantic_verified": null,
 57625|       "dll_verified_status": "signature_verified_declared",
 57626|       "revitlookup_referenced": true,
 57627|       "revitlookup_requires_document_context": false
 57628|     },
 57629|     {
 57630|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57631|       "target": null,
 57632|       "member_name": "GetSortGroupFieldCount",
 57633|       "member_kind": "method",
 57634|       "edge_type": "MEMBER_OF_GROUP",
 57635|       "confidence": "name_only_candidate",
 57636|       "confidence_tier": "likely",
 57637|       "target_resolution": "none",
 57638|       "evidence": [
 57639|         "member name 'GetSortGroupFieldCount' matches keyword pattern /^GetMember|Group/ but return type 'int' gives no type-level confirmation"
 57640|       ],
 57641|       "source_url": "https://www.revitapidocs.com/2025/e962c669-7aa3-a6d0-3857-82e1fd536c72.htm",
 57642|       "dll_signature_verified": true,
 57643|       "dll_relationship_scope": "declared",
 57644|       "dll_semantic_verified": null,
 57645|       "dll_verified_status": "signature_verified_declared",
 57646|       "revitlookup_referenced": null,
 57647|       "revitlookup_requires_document_context": null
 57648|     },
 57649|     {
 57650|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57651|       "target": "Autodesk.Revit.DB.ScheduleSortGroupField",
 57652|       "member_name": "GetSortGroupFields",
 57653|       "member_kind": "method",
 57654|       "edge_type": "MEMBER_OF_GROUP",
 57655|       "confidence": "needs_runtime_validation",
 57656|       "confidence_tier": "needs_validation",
 57657|       "target_resolution": "exact",
 57658|       "evidence": [
 57659|         "return type 'IList < ScheduleSortGroupField >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 57660|       ],
 57661|       "source_url": "https://www.revitapidocs.com/2025/20e6ace5-fa95-7705-d8d7-2eb86196e134.htm",
 57662|       "dll_signature_verified": true,
 57663|       "dll_relationship_scope": "declared",
 57664|       "dll_semantic_verified": null,
 57665|       "dll_verified_status": "signature_verified_declared",
 57666|       "revitlookup_referenced": null,
 57667|       "revitlookup_requires_document_context": null
 57668|     },
 57669|     {
 57670|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57671|       "target": null,
 57672|       "member_name": "GetValidCategoriesForEmbeddedSchedule",
 57673|       "member_kind": "method",
 57674|       "edge_type": "RETURNS_ELEMENT_IDS",
 57675|       "confidence": "unknown_reference",
 57676|       "confidence_tier": "unverified_reference",
 57677|       "target_resolution": "none",
 57678|       "evidence": [
 57679|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 57680|       ],
 57681|       "source_url": "https://www.revitapidocs.com/2025/ad3de021-c17d-6a5f-a196-cdf6b43e11ba.htm",
 57682|       "dll_signature_verified": true,
 57683|       "dll_relationship_scope": "declared",
 57684|       "dll_semantic_verified": null,
 57685|       "dll_verified_status": "signature_verified_declared",
 57686|       "revitlookup_referenced": null,
 57687|       "revitlookup_requires_document_context": null
 57688|     },
 57689|     {
 57690|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57691|       "target": "Autodesk.Revit.DB.ScheduleField",
 57692|       "member_name": "InsertCombinedParameterField",
 57693|       "member_kind": "method",
 57694|       "edge_type": "HAS_PARAMETER",
 57695|       "confidence": "direct_return_type",
 57696|       "confidence_tier": "core",
 57697|       "target_resolution": "exact",
 57698|       "evidence": [
 57699|         "return type 'ScheduleField' directly names a Revit DB object type"
 57700|       ],
 57701|       "source_url": "https://www.revitapidocs.com/2025/f33ac062-b861-dd2e-93b4-32f2124151ff.htm",
 57702|       "dll_signature_verified": true,
 57703|       "dll_relationship_scope": "declared",
 57704|       "dll_semantic_verified": null,
 57705|       "dll_verified_status": "signature_verified_declared",
 57706|       "revitlookup_referenced": null,
 57707|       "revitlookup_requires_document_context": null
 57708|     },
 57709|     {
 57710|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57711|       "target": "Autodesk.Revit.DB.ScheduleField",
 57712|       "member_name": "InsertField",
 57713|       "member_kind": "method",
 57714|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57715|       "confidence": "direct_return_type",
 57716|       "confidence_tier": "unverified_reference",
 57717|       "target_resolution": "exact",
 57718|       "evidence": [
 57719|         "return type 'ScheduleField' directly names a Revit DB object type"
 57720|       ],
 57721|       "source_url": "https://www.revitapidocs.com/2025/57376e72-79c9-da97-6b1c-6f4e40f00252.htm",
 57722|       "dll_signature_verified": true,
 57723|       "dll_relationship_scope": "declared",
 57724|       "dll_semantic_verified": null,
 57725|       "dll_verified_status": "signature_verified_declared",
 57726|       "revitlookup_referenced": null,
 57727|       "revitlookup_requires_document_context": null
 57728|     },
 57729|     {
 57730|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
```
