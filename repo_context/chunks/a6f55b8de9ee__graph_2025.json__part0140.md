# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 140 of 216
- Original line range: 54211-54610
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 54211|       "member_kind": "method",
 54212|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 54213|       "confidence": "direct_return_type",
 54214|       "confidence_tier": "unverified_reference",
 54215|       "target_resolution": "exact",
 54216|       "evidence": [
 54217|         "return type 'PhaseArrayIterator' directly names a Revit DB object type"
 54218|       ],
 54219|       "source_url": "https://www.revitapidocs.com/2025/432aeda4-3638-52fa-69d5-aae94f3d61ac.htm",
 54220|       "dll_signature_verified": true,
 54221|       "dll_relationship_scope": "declared",
 54222|       "dll_semantic_verified": null,
 54223|       "dll_verified_status": "signature_verified_declared",
 54224|       "revitlookup_referenced": null,
 54225|       "revitlookup_requires_document_context": null
 54226|     },
 54227|     {
 54228|       "source": "Autodesk.Revit.DB.PhaseArray",
 54229|       "target": "Autodesk.Revit.DB.PhaseArrayIterator",
 54230|       "member_name": "ReverseIterator",
 54231|       "member_kind": "method",
 54232|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 54233|       "confidence": "direct_return_type",
 54234|       "confidence_tier": "unverified_reference",
 54235|       "target_resolution": "exact",
 54236|       "evidence": [
 54237|         "return type 'PhaseArrayIterator' directly names a Revit DB object type"
 54238|       ],
 54239|       "source_url": "https://www.revitapidocs.com/2025/a7e6cbb1-b223-5d5d-02ef-17a93b6d86b0.htm",
 54240|       "dll_signature_verified": true,
 54241|       "dll_relationship_scope": "declared",
 54242|       "dll_semantic_verified": null,
 54243|       "dll_verified_status": "signature_verified_declared",
 54244|       "revitlookup_referenced": null,
 54245|       "revitlookup_requires_document_context": null
 54246|     },
 54247|     {
 54248|       "source": "Autodesk.Revit.DB.PhaseFilter",
 54249|       "target": "Autodesk.Revit.DB.Phase",
 54250|       "member_name": "GetPhaseStatusPresentation",
 54251|       "member_kind": "method",
 54252|       "edge_type": "ASSIGNED_TO_PHASE",
 54253|       "confidence": "name_only_candidate",
 54254|       "confidence_tier": "likely",
 54255|       "target_resolution": "exact",
 54256|       "evidence": [
 54257|         "member name 'GetPhaseStatusPresentation' matches keyword pattern /Phase/ but return type 'PhaseStatusPresentation' gives no type-level confirmation"
 54258|       ],
 54259|       "source_url": "https://www.revitapidocs.com/2025/6f6bf5a7-eea0-faa5-c35d-8c421388eeea.htm",
 54260|       "dll_signature_verified": true,
 54261|       "dll_relationship_scope": "declared",
 54262|       "dll_semantic_verified": null,
 54263|       "dll_verified_status": "signature_verified_declared",
 54264|       "revitlookup_referenced": null,
 54265|       "revitlookup_requires_document_context": null
 54266|     },
 54267|     {
 54268|       "source": "Autodesk.Revit.DB.PhaseFilter",
 54269|       "target": "Autodesk.Revit.DB.Phase",
 54270|       "member_name": "SetPhaseStatusPresentation",
 54271|       "member_kind": "method",
 54272|       "edge_type": "ASSIGNED_TO_PHASE",
 54273|       "confidence": "name_only_candidate",
 54274|       "confidence_tier": "likely",
 54275|       "target_resolution": "exact",
 54276|       "evidence": [
 54277|         "member name 'SetPhaseStatusPresentation' matches keyword pattern /Phase/ but return type 'void' gives no type-level confirmation"
 54278|       ],
 54279|       "source_url": "https://www.revitapidocs.com/2025/a0554313-bda4-9036-0320-2d9294c2bde6.htm",
 54280|       "dll_signature_verified": true,
 54281|       "dll_relationship_scope": "declared",
 54282|       "dll_semantic_verified": null,
 54283|       "dll_verified_status": "signature_verified_declared",
 54284|       "revitlookup_referenced": null,
 54285|       "revitlookup_requires_document_context": null
 54286|     },
 54287|     {
 54288|       "source": "Autodesk.Revit.DB.PlanCircuit",
 54289|       "target": "Autodesk.Revit.DB.Architecture.Room",
 54290|       "member_name": "IsRoomLocated",
 54291|       "member_kind": "property",
 54292|       "edge_type": "REFERENCES",
 54293|       "confidence": "name_only_candidate",
 54294|       "confidence_tier": "likely",
 54295|       "target_resolution": "exact",
 54296|       "evidence": [
 54297|         "member name 'IsRoomLocated' matches keyword pattern /Room/ but return type 'bool' gives no type-level confirmation"
 54298|       ],
 54299|       "source_url": "https://www.revitapidocs.com/2025/c8f370bd-6395-06c0-6221-7df52adeda81.htm",
 54300|       "dll_signature_verified": true,
 54301|       "dll_relationship_scope": "declared",
 54302|       "dll_semantic_verified": null,
 54303|       "dll_verified_status": "signature_verified_declared",
 54304|       "revitlookup_referenced": null,
 54305|       "revitlookup_requires_document_context": null
 54306|     },
 54307|     {
 54308|       "source": "Autodesk.Revit.DB.PlanCircuitSet",
 54309|       "target": "Autodesk.Revit.DB.PlanCircuitSetIterator",
 54310|       "member_name": "ForwardIterator",
 54311|       "member_kind": "method",
 54312|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 54313|       "confidence": "direct_return_type",
 54314|       "confidence_tier": "unverified_reference",
 54315|       "target_resolution": "exact",
 54316|       "evidence": [
 54317|         "return type 'PlanCircuitSetIterator' directly names a Revit DB object type"
 54318|       ],
 54319|       "source_url": "https://www.revitapidocs.com/2025/541ced25-7d64-2741-7aee-e9f70b07989d.htm",
 54320|       "dll_signature_verified": true,
 54321|       "dll_relationship_scope": "declared",
 54322|       "dll_semantic_verified": null,
 54323|       "dll_verified_status": "signature_verified_declared",
 54324|       "revitlookup_referenced": null,
 54325|       "revitlookup_requires_document_context": null
 54326|     },
 54327|     {
 54328|       "source": "Autodesk.Revit.DB.PlanCircuitSet",
 54329|       "target": "Autodesk.Revit.DB.PlanCircuitSetIterator",
 54330|       "member_name": "ReverseIterator",
 54331|       "member_kind": "method",
 54332|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 54333|       "confidence": "direct_return_type",
 54334|       "confidence_tier": "unverified_reference",
 54335|       "target_resolution": "exact",
 54336|       "evidence": [
 54337|         "return type 'PlanCircuitSetIterator' directly names a Revit DB object type"
 54338|       ],
 54339|       "source_url": "https://www.revitapidocs.com/2025/6fcdecf0-916a-fc5b-170c-c7fa49ec978a.htm",
 54340|       "dll_signature_verified": true,
 54341|       "dll_relationship_scope": "declared",
 54342|       "dll_semantic_verified": null,
 54343|       "dll_verified_status": "signature_verified_declared",
 54344|       "revitlookup_referenced": null,
 54345|       "revitlookup_requires_document_context": null
 54346|     },
 54347|     {
 54348|       "source": "Autodesk.Revit.DB.PlanTopology",
 54349|       "target": "Autodesk.Revit.DB.PlanCircuitSet",
 54350|       "member_name": "Circuits",
 54351|       "member_kind": "property",
 54352|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 54353|       "confidence": "direct_return_type",
 54354|       "confidence_tier": "unverified_reference",
 54355|       "target_resolution": "exact",
 54356|       "evidence": [
 54357|         "return type 'PlanCircuitSet' directly names a Revit DB object type"
 54358|       ],
 54359|       "source_url": "https://www.revitapidocs.com/2025/42b88afe-69fe-4e8a-4bc2-63f84c63ea64.htm",
 54360|       "dll_signature_verified": true,
 54361|       "dll_relationship_scope": "declared",
 54362|       "dll_semantic_verified": null,
 54363|       "dll_verified_status": "signature_verified_declared",
 54364|       "revitlookup_referenced": null,
 54365|       "revitlookup_requires_document_context": null
 54366|     },
 54367|     {
 54368|       "source": "Autodesk.Revit.DB.PlanTopology",
 54369|       "target": "Autodesk.Revit.DB.Level",
 54370|       "member_name": "Level",
 54371|       "member_kind": "property",
 54372|       "edge_type": "ASSIGNED_TO_LEVEL",
 54373|       "confidence": "direct_return_type",
 54374|       "confidence_tier": "core",
 54375|       "target_resolution": "exact",
 54376|       "evidence": [
 54377|         "return type 'Level' directly names a Revit DB object type"
 54378|       ],
 54379|       "source_url": "https://www.revitapidocs.com/2025/f51d7ba7-dabd-43de-9d61-50f1e81836fc.htm",
 54380|       "dll_signature_verified": true,
 54381|       "dll_relationship_scope": "declared",
 54382|       "dll_semantic_verified": null,
 54383|       "dll_verified_status": "signature_verified_declared",
 54384|       "revitlookup_referenced": null,
 54385|       "revitlookup_requires_document_context": null
 54386|     },
 54387|     {
 54388|       "source": "Autodesk.Revit.DB.PlanTopology",
 54389|       "target": "Autodesk.Revit.DB.Phase",
 54390|       "member_name": "Phase",
 54391|       "member_kind": "property",
 54392|       "edge_type": "ASSIGNED_TO_PHASE",
 54393|       "confidence": "direct_return_type",
 54394|       "confidence_tier": "core",
 54395|       "target_resolution": "exact",
 54396|       "evidence": [
 54397|         "return type 'Phase' directly names a Revit DB object type"
 54398|       ],
 54399|       "source_url": "https://www.revitapidocs.com/2025/933b55f9-b88f-978f-1901-e7706f705b08.htm",
 54400|       "dll_signature_verified": true,
 54401|       "dll_relationship_scope": "declared",
 54402|       "dll_semantic_verified": null,
 54403|       "dll_verified_status": "signature_verified_declared",
 54404|       "revitlookup_referenced": null,
 54405|       "revitlookup_requires_document_context": null
 54406|     },
 54407|     {
 54408|       "source": "Autodesk.Revit.DB.PlanTopology",
 54409|       "target": "Autodesk.Revit.DB.Architecture.Room",
 54410|       "member_name": "GetRoomIds",
 54411|       "member_kind": "method",
 54412|       "edge_type": "REFERENCES",
 54413|       "confidence": "elementid_collection_with_strong_name",
 54414|       "confidence_tier": "core",
 54415|       "target_resolution": "exact",
 54416|       "evidence": [
 54417|         "member name 'GetRoomIds' matches keyword pattern /Room/"
 54418|       ],
 54419|       "source_url": "https://www.revitapidocs.com/2025/b94486f2-f0ae-a2fe-d23a-bab761d60835.htm",
 54420|       "dll_signature_verified": true,
 54421|       "dll_relationship_scope": "declared",
 54422|       "dll_semantic_verified": null,
 54423|       "dll_verified_status": "signature_verified_declared",
 54424|       "revitlookup_referenced": null,
 54425|       "revitlookup_requires_document_context": null
 54426|     },
 54427|     {
 54428|       "source": "Autodesk.Revit.DB.PlanTopologySet",
 54429|       "target": "Autodesk.Revit.DB.PlanTopologySetIterator",
 54430|       "member_name": "ForwardIterator",
 54431|       "member_kind": "method",
 54432|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 54433|       "confidence": "direct_return_type",
 54434|       "confidence_tier": "unverified_reference",
 54435|       "target_resolution": "exact",
 54436|       "evidence": [
 54437|         "return type 'PlanTopologySetIterator' directly names a Revit DB object type"
 54438|       ],
 54439|       "source_url": "https://www.revitapidocs.com/2025/57cc301a-de8e-2c20-cdbc-e09e776df0b5.htm",
 54440|       "dll_signature_verified": true,
 54441|       "dll_relationship_scope": "declared",
 54442|       "dll_semantic_verified": null,
 54443|       "dll_verified_status": "signature_verified_declared",
 54444|       "revitlookup_referenced": null,
 54445|       "revitlookup_requires_document_context": null
 54446|     },
 54447|     {
 54448|       "source": "Autodesk.Revit.DB.PlanTopologySet",
 54449|       "target": "Autodesk.Revit.DB.PlanTopologySetIterator",
 54450|       "member_name": "ReverseIterator",
 54451|       "member_kind": "method",
 54452|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 54453|       "confidence": "direct_return_type",
 54454|       "confidence_tier": "unverified_reference",
 54455|       "target_resolution": "exact",
 54456|       "evidence": [
 54457|         "return type 'PlanTopologySetIterator' directly names a Revit DB object type"
 54458|       ],
 54459|       "source_url": "https://www.revitapidocs.com/2025/fbda471e-2e84-0263-89c3-ff21d6b96115.htm",
 54460|       "dll_signature_verified": true,
 54461|       "dll_relationship_scope": "declared",
 54462|       "dll_semantic_verified": null,
 54463|       "dll_verified_status": "signature_verified_declared",
 54464|       "revitlookup_referenced": null,
 54465|       "revitlookup_requires_document_context": null
 54466|     },
 54467|     {
 54468|       "source": "Autodesk.Revit.DB.PlanViewRange",
 54469|       "target": null,
 54470|       "member_name": "Current",
 54471|       "member_kind": "property",
 54472|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 54473|       "confidence": "unknown_reference",
 54474|       "confidence_tier": "unverified_reference",
 54475|       "target_resolution": "none",
 54476|       "evidence": [
 54477|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 54478|       ],
 54479|       "source_url": "https://www.revitapidocs.com/2025/4ced7a98-7576-a63b-f37e-97f70fd212c9.htm",
 54480|       "dll_signature_verified": true,
 54481|       "dll_relationship_scope": "declared",
 54482|       "dll_semantic_verified": null,
 54483|       "dll_verified_status": "signature_verified_declared",
 54484|       "revitlookup_referenced": null,
 54485|       "revitlookup_requires_document_context": null
 54486|     },
 54487|     {
 54488|       "source": "Autodesk.Revit.DB.PlanViewRange",
 54489|       "target": "Autodesk.Revit.DB.Level",
 54490|       "member_name": "LevelAbove",
 54491|       "member_kind": "property",
 54492|       "edge_type": "ASSIGNED_TO_LEVEL",
 54493|       "confidence": "elementid_with_strong_name",
 54494|       "confidence_tier": "core",
 54495|       "target_resolution": "exact",
 54496|       "evidence": [
 54497|         "member name 'LevelAbove' matches keyword pattern /Level/"
 54498|       ],
 54499|       "source_url": "https://www.revitapidocs.com/2025/9c2c47f9-1fc8-addf-f6bd-dcf767efe3b8.htm",
 54500|       "dll_signature_verified": true,
 54501|       "dll_relationship_scope": "declared",
 54502|       "dll_semantic_verified": null,
 54503|       "dll_verified_status": "signature_verified_declared",
 54504|       "revitlookup_referenced": null,
 54505|       "revitlookup_requires_document_context": null
 54506|     },
 54507|     {
 54508|       "source": "Autodesk.Revit.DB.PlanViewRange",
 54509|       "target": "Autodesk.Revit.DB.Level",
 54510|       "member_name": "LevelBelow",
 54511|       "member_kind": "property",
 54512|       "edge_type": "ASSIGNED_TO_LEVEL",
 54513|       "confidence": "elementid_with_strong_name",
 54514|       "confidence_tier": "core",
 54515|       "target_resolution": "exact",
 54516|       "evidence": [
 54517|         "member name 'LevelBelow' matches keyword pattern /Level/"
 54518|       ],
 54519|       "source_url": "https://www.revitapidocs.com/2025/b474e148-6212-feeb-9d1b-351937ad238c.htm",
 54520|       "dll_signature_verified": true,
 54521|       "dll_relationship_scope": "declared",
 54522|       "dll_semantic_verified": null,
 54523|       "dll_verified_status": "signature_verified_declared",
 54524|       "revitlookup_referenced": null,
 54525|       "revitlookup_requires_document_context": null
 54526|     },
 54527|     {
 54528|       "source": "Autodesk.Revit.DB.PlanViewRange",
 54529|       "target": null,
 54530|       "member_name": "Unlimited",
 54531|       "member_kind": "property",
 54532|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 54533|       "confidence": "unknown_reference",
 54534|       "confidence_tier": "unverified_reference",
 54535|       "target_resolution": "none",
 54536|       "evidence": [
 54537|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 54538|       ],
 54539|       "source_url": "https://www.revitapidocs.com/2025/5b21cada-9846-35fa-0a1e-e661d3d916c0.htm",
 54540|       "dll_signature_verified": true,
 54541|       "dll_relationship_scope": "declared",
 54542|       "dll_semantic_verified": null,
 54543|       "dll_verified_status": "signature_verified_declared",
 54544|       "revitlookup_referenced": null,
 54545|       "revitlookup_requires_document_context": null
 54546|     },
 54547|     {
 54548|       "source": "Autodesk.Revit.DB.PlanViewRange",
 54549|       "target": "Autodesk.Revit.DB.Level",
 54550|       "member_name": "GetLevelId",
 54551|       "member_kind": "method",
 54552|       "edge_type": "ASSIGNED_TO_LEVEL",
 54553|       "confidence": "elementid_with_strong_name",
 54554|       "confidence_tier": "core",
 54555|       "target_resolution": "exact",
 54556|       "evidence": [
 54557|         "member name 'GetLevelId' matches keyword pattern /Level/"
 54558|       ],
 54559|       "source_url": "https://www.revitapidocs.com/2025/9c56cd0b-bd1b-47f6-b669-5870b2966c1f.htm",
 54560|       "dll_signature_verified": true,
 54561|       "dll_relationship_scope": "declared",
 54562|       "dll_semantic_verified": null,
 54563|       "dll_verified_status": "signature_verified_declared",
 54564|       "revitlookup_referenced": true,
 54565|       "revitlookup_requires_document_context": false
 54566|     },
 54567|     {
 54568|       "source": "Autodesk.Revit.DB.PlanViewRange",
 54569|       "target": "Autodesk.Revit.DB.Level",
 54570|       "member_name": "SetLevelId",
 54571|       "member_kind": "method",
 54572|       "edge_type": "ASSIGNED_TO_LEVEL",
 54573|       "confidence": "name_only_candidate",
 54574|       "confidence_tier": "likely",
 54575|       "target_resolution": "exact",
 54576|       "evidence": [
 54577|         "member name 'SetLevelId' matches keyword pattern /Level/ but return type 'void' gives no type-level confirmation"
 54578|       ],
 54579|       "source_url": "https://www.revitapidocs.com/2025/ef2d4027-3b09-f62e-5507-2d39615b8b4a.htm",
 54580|       "dll_signature_verified": true,
 54581|       "dll_relationship_scope": "declared",
 54582|       "dll_semantic_verified": null,
 54583|       "dll_verified_status": "signature_verified_declared",
 54584|       "revitlookup_referenced": null,
 54585|       "revitlookup_requires_document_context": null
 54586|     },
 54587|     {
 54588|       "source": "Autodesk.Revit.DB.Point",
 54589|       "target": "Autodesk.Revit.DB.Reference",
 54590|       "member_name": "Reference",
 54591|       "member_kind": "property",
 54592|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 54593|       "confidence": "direct_return_type",
 54594|       "confidence_tier": "unverified_reference",
 54595|       "target_resolution": "exact",
 54596|       "evidence": [
 54597|         "return type 'Reference' directly names a Revit DB object type"
 54598|       ],
 54599|       "source_url": "https://www.revitapidocs.com/2025/683d22a4-b86f-3180-2e0a-2bb4685f6aa5.htm",
 54600|       "dll_signature_verified": true,
 54601|       "dll_relationship_scope": "declared",
 54602|       "dll_semantic_verified": null,
 54603|       "dll_verified_status": "signature_verified_declared",
 54604|       "revitlookup_referenced": null,
 54605|       "revitlookup_requires_document_context": null
 54606|     },
 54607|     {
 54608|       "source": "Autodesk.Revit.DB.PointCloudInstance",
 54609|       "target": "Autodesk.Revit.DB.PointClouds.PointCollection",
 54610|       "member_name": "GetPoints",
```
