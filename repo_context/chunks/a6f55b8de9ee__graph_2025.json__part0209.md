# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 209 of 216
- Original line range: 81121-81520
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 81121|       "dll_relationship_scope": "declared",
 81122|       "dll_semantic_verified": null,
 81123|       "dll_verified_status": "signature_verified_declared",
 81124|       "revitlookup_referenced": null,
 81125|       "revitlookup_requires_document_context": null
 81126|     },
 81127|     {
 81128|       "source": "Autodesk.Revit.DB.Structure.RebarFreeFormAccessor",
 81129|       "target": null,
 81130|       "member_name": "GetEndTreatmentTypeIdAtIndex",
 81131|       "member_kind": "method",
 81132|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 81133|       "confidence": "unknown_reference",
 81134|       "confidence_tier": "unverified_reference",
 81135|       "target_resolution": "none",
 81136|       "evidence": [
 81137|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 81138|       ],
 81139|       "source_url": "https://www.revitapidocs.com/2025/12bf4634-9e2c-08c3-5aa2-54f796651c70.htm",
 81140|       "dll_signature_verified": true,
 81141|       "dll_relationship_scope": "declared",
 81142|       "dll_semantic_verified": null,
 81143|       "dll_verified_status": "signature_verified_declared",
 81144|       "revitlookup_referenced": null,
 81145|       "revitlookup_requires_document_context": null
 81146|     },
 81147|     {
 81148|       "source": "Autodesk.Revit.DB.Structure.RebarFreeFormAccessor",
 81149|       "target": null,
 81150|       "member_name": "GetHookTypeIdAtIndex",
 81151|       "member_kind": "method",
 81152|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 81153|       "confidence": "unknown_reference",
 81154|       "confidence_tier": "unverified_reference",
 81155|       "target_resolution": "none",
 81156|       "evidence": [
 81157|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 81158|       ],
 81159|       "source_url": "https://www.revitapidocs.com/2025/27a70740-3367-6509-aeae-e58fb578e763.htm",
 81160|       "dll_signature_verified": true,
 81161|       "dll_relationship_scope": "declared",
 81162|       "dll_semantic_verified": null,
 81163|       "dll_verified_status": "signature_verified_declared",
 81164|       "revitlookup_referenced": null,
 81165|       "revitlookup_requires_document_context": null
 81166|     },
 81167|     {
 81168|       "source": "Autodesk.Revit.DB.Structure.RebarFreeFormAccessor",
 81169|       "target": null,
 81170|       "member_name": "GetShapeIdAtIndex",
 81171|       "member_kind": "method",
 81172|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 81173|       "confidence": "unknown_reference",
 81174|       "confidence_tier": "unverified_reference",
 81175|       "target_resolution": "none",
 81176|       "evidence": [
 81177|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 81178|       ],
 81179|       "source_url": "https://www.revitapidocs.com/2025/79172a28-c9c1-3659-681f-f365ba834f03.htm",
 81180|       "dll_signature_verified": true,
 81181|       "dll_relationship_scope": "declared",
 81182|       "dll_semantic_verified": null,
 81183|       "dll_verified_status": "signature_verified_declared",
 81184|       "revitlookup_referenced": null,
 81185|       "revitlookup_requires_document_context": null
 81186|     },
 81187|     {
 81188|       "source": "Autodesk.Revit.DB.Structure.RebarFreeFormAccessor",
 81189|       "target": null,
 81190|       "member_name": "GetUpdatingSharedParameters",
 81191|       "member_kind": "method",
 81192|       "edge_type": "HAS_PARAMETER",
 81193|       "confidence": "elementid_collection_with_strong_name",
 81194|       "confidence_tier": "core",
 81195|       "target_resolution": "none",
 81196|       "evidence": [
 81197|         "member name 'GetUpdatingSharedParameters' matches keyword pattern /Parameter/"
 81198|       ],
 81199|       "source_url": "https://www.revitapidocs.com/2025/7a5ecdb0-a5cd-e64b-1640-c4c03cd16a25.htm",
 81200|       "dll_signature_verified": true,
 81201|       "dll_relationship_scope": "declared",
 81202|       "dll_semantic_verified": null,
 81203|       "dll_verified_status": "signature_verified_declared",
 81204|       "revitlookup_referenced": null,
 81205|       "revitlookup_requires_document_context": null
 81206|     },
 81207|     {
 81208|       "source": "Autodesk.Revit.DB.Structure.RebarFreeFormAccessor",
 81209|       "target": null,
 81210|       "member_name": "RemoveUpdatingSharedParameter",
 81211|       "member_kind": "method",
 81212|       "edge_type": "HAS_PARAMETER",
 81213|       "confidence": "name_only_candidate",
 81214|       "confidence_tier": "likely",
 81215|       "target_resolution": "none",
 81216|       "evidence": [
 81217|         "member name 'RemoveUpdatingSharedParameter' matches keyword pattern /Parameter/ but return type 'void' gives no type-level confirmation"
 81218|       ],
 81219|       "source_url": "https://www.revitapidocs.com/2025/17cac627-4846-e71d-b181-6ea6ef7d5257.htm",
 81220|       "dll_signature_verified": true,
 81221|       "dll_relationship_scope": "declared",
 81222|       "dll_semantic_verified": null,
 81223|       "dll_verified_status": "signature_verified_declared",
 81224|       "revitlookup_referenced": null,
 81225|       "revitlookup_requires_document_context": null
 81226|     },
 81227|     {
 81228|       "source": "Autodesk.Revit.DB.Structure.RebarHandleNameData",
 81229|       "target": null,
 81230|       "member_name": "GetCustomHandleTag",
 81231|       "member_kind": "method",
 81232|       "edge_type": "TAGS_ELEMENT",
 81233|       "confidence": "name_only_candidate",
 81234|       "confidence_tier": "likely",
 81235|       "target_resolution": "none",
 81236|       "evidence": [
 81237|         "member name 'GetCustomHandleTag' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'int' gives no type-level confirmation"
 81238|       ],
 81239|       "source_url": "https://www.revitapidocs.com/2025/028dbe8c-b0f2-c1da-d5c1-93de6ad840c7.htm",
 81240|       "dll_signature_verified": true,
 81241|       "dll_relationship_scope": "declared",
 81242|       "dll_semantic_verified": null,
 81243|       "dll_verified_status": "signature_verified_declared",
 81244|       "revitlookup_referenced": null,
 81245|       "revitlookup_requires_document_context": null
 81246|     },
 81247|     {
 81248|       "source": "Autodesk.Revit.DB.Structure.RebarHostData",
 81249|       "target": "Autodesk.Revit.DB.Structure.AreaReinforcement",
 81250|       "member_name": "GetAreaReinforcementsInHost",
 81251|       "member_kind": "method",
 81252|       "edge_type": "HOSTED_BY",
 81253|       "confidence": "needs_runtime_validation",
 81254|       "confidence_tier": "needs_validation",
 81255|       "target_resolution": "short_name_fallback",
 81256|       "evidence": [
 81257|         "return type 'IList < AreaReinforcement >' is a generic collection whose element type cannot be statically confirmed as reference-bearing",
 81258|         "docs text contains relationship phrase: 'hosted by'"
 81259|       ],
 81260|       "source_url": "https://www.revitapidocs.com/2025/28ecf84e-491a-4407-b6c8-37ab4cbc0257.htm",
 81261|       "dll_signature_verified": true,
 81262|       "dll_relationship_scope": "declared",
 81263|       "dll_semantic_verified": null,
 81264|       "dll_verified_status": "signature_verified_declared",
 81265|       "revitlookup_referenced": null,
 81266|       "revitlookup_requires_document_context": null
 81267|     },
 81268|     {
 81269|       "source": "Autodesk.Revit.DB.Structure.RebarHostData",
 81270|       "target": "Autodesk.Revit.DB.Structure.RebarCoverType",
 81271|       "member_name": "GetCommonCoverType",
 81272|       "member_kind": "method",
 81273|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 81274|       "confidence": "direct_return_type",
 81275|       "confidence_tier": "unverified_reference",
 81276|       "target_resolution": "short_name_fallback",
 81277|       "evidence": [
 81278|         "return type 'RebarCoverType' directly names a Revit DB object type"
 81279|       ],
 81280|       "source_url": "https://www.revitapidocs.com/2025/eb1839ca-5de6-b651-6009-db078cb8fd91.htm",
 81281|       "dll_signature_verified": true,
 81282|       "dll_relationship_scope": "declared",
 81283|       "dll_semantic_verified": null,
 81284|       "dll_verified_status": "signature_verified_declared",
 81285|       "revitlookup_referenced": null,
 81286|       "revitlookup_requires_document_context": null
 81287|     },
 81288|     {
 81289|       "source": "Autodesk.Revit.DB.Structure.RebarHostData",
 81290|       "target": "Autodesk.Revit.DB.Structure.RebarCoverType",
 81291|       "member_name": "GetCoverType",
 81292|       "member_kind": "method",
 81293|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 81294|       "confidence": "direct_return_type",
 81295|       "confidence_tier": "unverified_reference",
 81296|       "target_resolution": "short_name_fallback",
 81297|       "evidence": [
 81298|         "return type 'RebarCoverType' directly names a Revit DB object type"
 81299|       ],
 81300|       "source_url": "https://www.revitapidocs.com/2025/4d952f72-42b5-88f1-0788-7e64ff6589bb.htm",
 81301|       "dll_signature_verified": true,
 81302|       "dll_relationship_scope": "declared",
 81303|       "dll_semantic_verified": null,
 81304|       "dll_verified_status": "signature_verified_declared",
 81305|       "revitlookup_referenced": null,
 81306|       "revitlookup_requires_document_context": null
 81307|     },
 81308|     {
 81309|       "source": "Autodesk.Revit.DB.Structure.RebarHostData",
 81310|       "target": "Autodesk.Revit.DB.Reference",
 81311|       "member_name": "GetExposedFaces",
 81312|       "member_kind": "method",
 81313|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 81314|       "confidence": "needs_runtime_validation",
 81315|       "confidence_tier": "needs_validation",
 81316|       "target_resolution": "exact",
 81317|       "evidence": [
 81318|         "return type 'IList < Reference >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 81319|       ],
 81320|       "source_url": "https://www.revitapidocs.com/2025/2afebbf0-931a-353a-69c1-9aba76c33cc1.htm",
 81321|       "dll_signature_verified": true,
 81322|       "dll_relationship_scope": "declared",
 81323|       "dll_semantic_verified": null,
 81324|       "dll_verified_status": "signature_verified_declared",
 81325|       "revitlookup_referenced": null,
 81326|       "revitlookup_requires_document_context": null
 81327|     },
 81328|     {
 81329|       "source": "Autodesk.Revit.DB.Structure.RebarHostData",
 81330|       "target": "Autodesk.Revit.DB.Structure.FabricArea",
 81331|       "member_name": "GetFabricAreasInHost",
 81332|       "member_kind": "method",
 81333|       "edge_type": "HOSTED_BY",
 81334|       "confidence": "needs_runtime_validation",
 81335|       "confidence_tier": "needs_validation",
 81336|       "target_resolution": "short_name_fallback",
 81337|       "evidence": [
 81338|         "return type 'IList < FabricArea >' is a generic collection whose element type cannot be statically confirmed as reference-bearing",
 81339|         "docs text contains relationship phrase: 'hosted by'"
 81340|       ],
 81341|       "source_url": "https://www.revitapidocs.com/2025/9cde5767-f653-44a5-2182-eccfa4e8fc55.htm",
 81342|       "dll_signature_verified": true,
 81343|       "dll_relationship_scope": "declared",
 81344|       "dll_semantic_verified": null,
 81345|       "dll_verified_status": "signature_verified_declared",
 81346|       "revitlookup_referenced": null,
 81347|       "revitlookup_requires_document_context": null
 81348|     },
 81349|     {
 81350|       "source": "Autodesk.Revit.DB.Structure.RebarHostData",
 81351|       "target": "Autodesk.Revit.DB.Structure.FabricSheet",
 81352|       "member_name": "GetFabricSheetsInHost",
 81353|       "member_kind": "method",
 81354|       "edge_type": "PLACED_ON_SHEET",
 81355|       "confidence": "needs_runtime_validation",
 81356|       "confidence_tier": "needs_validation",
 81357|       "target_resolution": "short_name_fallback",
 81358|       "evidence": [
 81359|         "return type 'IList < FabricSheet >' is a generic collection whose element type cannot be statically confirmed as reference-bearing",
 81360|         "docs text contains relationship phrase: 'hosted by'"
 81361|       ],
 81362|       "source_url": "https://www.revitapidocs.com/2025/ccdf094f-6198-3720-14c3-1fe3f131e6fe.htm",
 81363|       "dll_signature_verified": true,
 81364|       "dll_relationship_scope": "declared",
 81365|       "dll_semantic_verified": null,
 81366|       "dll_verified_status": "signature_verified_declared",
 81367|       "revitlookup_referenced": null,
 81368|       "revitlookup_requires_document_context": null
 81369|     },
 81370|     {
 81371|       "source": "Autodesk.Revit.DB.Structure.RebarHostData",
 81372|       "target": "Autodesk.Revit.DB.Structure.PathReinforcement",
 81373|       "member_name": "GetPathReinforcementsInHost",
 81374|       "member_kind": "method",
 81375|       "edge_type": "HOSTED_BY",
 81376|       "confidence": "needs_runtime_validation",
 81377|       "confidence_tier": "needs_validation",
 81378|       "target_resolution": "short_name_fallback",
 81379|       "evidence": [
 81380|         "return type 'IList < PathReinforcement >' is a generic collection whose element type cannot be statically confirmed as reference-bearing",
 81381|         "docs text contains relationship phrase: 'hosted by'"
 81382|       ],
 81383|       "source_url": "https://www.revitapidocs.com/2025/a508c6b3-0fce-0572-e17c-bcc06be368c6.htm",
 81384|       "dll_signature_verified": true,
 81385|       "dll_relationship_scope": "declared",
 81386|       "dll_semantic_verified": null,
 81387|       "dll_verified_status": "signature_verified_declared",
 81388|       "revitlookup_referenced": null,
 81389|       "revitlookup_requires_document_context": null
 81390|     },
 81391|     {
 81392|       "source": "Autodesk.Revit.DB.Structure.RebarHostData",
 81393|       "target": "Autodesk.Revit.DB.Structure.RebarContainer",
 81394|       "member_name": "GetRebarContainersInHost",
 81395|       "member_kind": "method",
 81396|       "edge_type": "HOSTED_BY",
 81397|       "confidence": "needs_runtime_validation",
 81398|       "confidence_tier": "needs_validation",
 81399|       "target_resolution": "short_name_fallback",
 81400|       "evidence": [
 81401|         "return type 'IList < RebarContainer >' is a generic collection whose element type cannot be statically confirmed as reference-bearing",
 81402|         "docs text contains relationship phrase: 'hosted by'"
 81403|       ],
 81404|       "source_url": "https://www.revitapidocs.com/2025/56ac8351-cf96-d1cb-cd44-551917ab3540.htm",
 81405|       "dll_signature_verified": true,
 81406|       "dll_relationship_scope": "declared",
 81407|       "dll_semantic_verified": null,
 81408|       "dll_verified_status": "signature_verified_declared",
 81409|       "revitlookup_referenced": null,
 81410|       "revitlookup_requires_document_context": null
 81411|     },
 81412|     {
 81413|       "source": "Autodesk.Revit.DB.Structure.RebarHostData",
 81414|       "target": null,
 81415|       "member_name": "GetRebarHostDirectNeighbors",
 81416|       "member_kind": "method",
 81417|       "edge_type": "HOSTED_BY",
 81418|       "confidence": "elementid_collection_with_strong_name",
 81419|       "confidence_tier": "core",
 81420|       "target_resolution": "none",
 81421|       "evidence": [
 81422|         "member name 'GetRebarHostDirectNeighbors' matches keyword pattern /^GetHosted|Host/"
 81423|       ],
 81424|       "source_url": "https://www.revitapidocs.com/2025/fc625f1c-4711-886b-c193-4087e4eafcde.htm",
 81425|       "dll_signature_verified": true,
 81426|       "dll_relationship_scope": "declared",
 81427|       "dll_semantic_verified": null,
 81428|       "dll_verified_status": "signature_verified_declared",
 81429|       "revitlookup_referenced": null,
 81430|       "revitlookup_requires_document_context": null
 81431|     },
 81432|     {
 81433|       "source": "Autodesk.Revit.DB.Structure.RebarHostData",
 81434|       "target": "Autodesk.Revit.DB.Structure.Rebar",
 81435|       "member_name": "GetRebarsInHost",
 81436|       "member_kind": "method",
 81437|       "edge_type": "HOSTED_BY",
 81438|       "confidence": "needs_runtime_validation",
 81439|       "confidence_tier": "needs_validation",
 81440|       "target_resolution": "short_name_fallback",
 81441|       "evidence": [
 81442|         "return type 'IList < Rebar >' is a generic collection whose element type cannot be statically confirmed as reference-bearing",
 81443|         "docs text contains relationship phrase: 'hosted by'"
 81444|       ],
 81445|       "source_url": "https://www.revitapidocs.com/2025/be275d81-a411-f199-79bc-0cd21af8c645.htm",
 81446|       "dll_signature_verified": true,
 81447|       "dll_relationship_scope": "declared",
 81448|       "dll_semantic_verified": null,
 81449|       "dll_verified_status": "signature_verified_declared",
 81450|       "revitlookup_referenced": null,
 81451|       "revitlookup_requires_document_context": null
 81452|     },
 81453|     {
 81454|       "source": "Autodesk.Revit.DB.Structure.RebarHostData",
 81455|       "target": null,
 81456|       "member_name": "IsReferenceContainedByAValidHost",
 81457|       "member_kind": "method",
 81458|       "edge_type": "HOSTED_BY",
 81459|       "confidence": "name_only_candidate",
 81460|       "confidence_tier": "likely",
 81461|       "target_resolution": "none",
 81462|       "evidence": [
 81463|         "member name 'IsReferenceContainedByAValidHost' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 81464|       ],
 81465|       "source_url": "https://www.revitapidocs.com/2025/962297a1-ccdf-80f7-6190-3c208b9d4a7c.htm",
 81466|       "dll_signature_verified": true,
 81467|       "dll_relationship_scope": "declared",
 81468|       "dll_semantic_verified": null,
 81469|       "dll_verified_status": "signature_verified_declared",
 81470|       "revitlookup_referenced": null,
 81471|       "revitlookup_requires_document_context": null
 81472|     },
 81473|     {
 81474|       "source": "Autodesk.Revit.DB.Structure.RebarHostData",
 81475|       "target": null,
 81476|       "member_name": "IsValidHost",
 81477|       "member_kind": "method",
 81478|       "edge_type": "HOSTED_BY",
 81479|       "confidence": "name_only_candidate",
 81480|       "confidence_tier": "likely",
 81481|       "target_resolution": "none",
 81482|       "evidence": [
 81483|         "member name 'IsValidHost' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 81484|       ],
 81485|       "source_url": "https://www.revitapidocs.com/2025/c1c4d8ff-4636-67b6-75d2-7c37a17a7276.htm",
 81486|       "dll_signature_verified": true,
 81487|       "dll_relationship_scope": "declared",
 81488|       "dll_semantic_verified": null,
 81489|       "dll_verified_status": "signature_verified_declared",
 81490|       "revitlookup_referenced": null,
 81491|       "revitlookup_requires_document_context": null
 81492|     },
 81493|     {
 81494|       "source": "Autodesk.Revit.DB.Structure.RebarHostData",
 81495|       "target": null,
 81496|       "member_name": "IsValidHost",
 81497|       "member_kind": "method",
 81498|       "edge_type": "HOSTED_BY",
 81499|       "confidence": "name_only_candidate",
 81500|       "confidence_tier": "likely",
 81501|       "target_resolution": "none",
 81502|       "evidence": [
 81503|         "member name 'IsValidHost' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 81504|       ],
 81505|       "source_url": "https://www.revitapidocs.com/2025/0d6cf4c6-6f5c-9f21-a6ee-0c15f4cbaabf.htm",
 81506|       "dll_signature_verified": true,
 81507|       "dll_relationship_scope": "declared",
 81508|       "dll_semantic_verified": null,
 81509|       "dll_verified_status": "signature_verified_declared",
 81510|       "revitlookup_referenced": null,
 81511|       "revitlookup_requires_document_context": null
 81512|     },
 81513|     {
 81514|       "source": "Autodesk.Revit.DB.Structure.RebarInSystem",
 81515|       "target": null,
 81516|       "member_name": "RebarShapeId",
 81517|       "member_kind": "property",
 81518|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 81519|       "confidence": "unknown_reference",
 81520|       "confidence_tier": "unverified_reference",
```
