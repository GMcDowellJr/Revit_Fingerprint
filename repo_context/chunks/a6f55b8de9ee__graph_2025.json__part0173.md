# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 173 of 216
- Original line range: 67081-67480
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 67081|       "confidence": "needs_runtime_validation",
 67082|       "confidence_tier": "needs_validation",
 67083|       "target_resolution": "short_name_fallback",
 67084|       "evidence": [
 67085|         "return type 'IList < MEPNetworkSegmentId >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 67086|       ],
 67087|       "source_url": "https://www.revitapidocs.com/2025/7bc0931e-957d-5176-c843-b7d60ed81d9e.htm",
 67088|       "dll_signature_verified": true,
 67089|       "dll_relationship_scope": "declared",
 67090|       "dll_semantic_verified": null,
 67091|       "dll_verified_status": "signature_verified_declared",
 67092|       "revitlookup_referenced": null,
 67093|       "revitlookup_requires_document_context": null
 67094|     },
 67095|     {
 67096|       "source": "Autodesk.Revit.DB.Analysis.MEPNetworkSegmentId",
 67097|       "target": null,
 67098|       "member_name": "ElementId",
 67099|       "member_kind": "property",
 67100|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 67101|       "confidence": "unknown_reference",
 67102|       "confidence_tier": "unverified_reference",
 67103|       "target_resolution": "none",
 67104|       "evidence": [
 67105|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 67106|       ],
 67107|       "source_url": "https://www.revitapidocs.com/2025/f79cdfec-43ce-18ca-91bd-bfbb9fbbeec7.htm",
 67108|       "dll_signature_verified": true,
 67109|       "dll_relationship_scope": "declared",
 67110|       "dll_semantic_verified": null,
 67111|       "dll_verified_status": "signature_verified_declared",
 67112|       "revitlookup_referenced": null,
 67113|       "revitlookup_requires_document_context": null
 67114|     },
 67115|     {
 67116|       "source": "Autodesk.Revit.DB.Analysis.PathOfTravel",
 67117|       "target": null,
 67118|       "member_name": "LineStyle",
 67119|       "member_kind": "property",
 67120|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 67121|       "confidence": "unknown_reference",
 67122|       "confidence_tier": "unverified_reference",
 67123|       "target_resolution": "none",
 67124|       "evidence": [
 67125|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 67126|       ],
 67127|       "source_url": "https://www.revitapidocs.com/2025/012d6c63-fd01-a578-5b42-9a9ca095c843.htm",
 67128|       "dll_signature_verified": true,
 67129|       "dll_relationship_scope": "declared",
 67130|       "dll_semantic_verified": null,
 67131|       "dll_verified_status": "signature_verified_declared",
 67132|       "revitlookup_referenced": null,
 67133|       "revitlookup_requires_document_context": null
 67134|     },
 67135|     {
 67136|       "source": "Autodesk.Revit.DB.Analysis.RouteAnalysisSettings",
 67137|       "target": "Autodesk.Revit.DB.Category",
 67138|       "member_name": "EnableIgnoredCategoryIds",
 67139|       "member_kind": "property",
 67140|       "edge_type": "HAS_CATEGORY",
 67141|       "confidence": "name_only_candidate",
 67142|       "confidence_tier": "likely",
 67143|       "target_resolution": "exact",
 67144|       "evidence": [
 67145|         "member name 'EnableIgnoredCategoryIds' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 67146|       ],
 67147|       "source_url": "https://www.revitapidocs.com/2025/c146653a-ed81-ee52-7102-a6c2ad8a4d7c.htm",
 67148|       "dll_signature_verified": true,
 67149|       "dll_relationship_scope": "declared",
 67150|       "dll_semantic_verified": null,
 67151|       "dll_verified_status": "signature_verified_declared",
 67152|       "revitlookup_referenced": null,
 67153|       "revitlookup_requires_document_context": null
 67154|     },
 67155|     {
 67156|       "source": "Autodesk.Revit.DB.Analysis.RouteAnalysisSettings",
 67157|       "target": "Autodesk.Revit.DB.Category",
 67158|       "member_name": "GetExcludedCategoryIds",
 67159|       "member_kind": "method",
 67160|       "edge_type": "HAS_CATEGORY",
 67161|       "confidence": "elementid_collection_with_strong_name",
 67162|       "confidence_tier": "core",
 67163|       "target_resolution": "exact",
 67164|       "evidence": [
 67165|         "member name 'GetExcludedCategoryIds' matches keyword pattern /Category/"
 67166|       ],
 67167|       "source_url": "https://www.revitapidocs.com/2025/d1cf8070-f971-feb0-df35-649982e68bc6.htm",
 67168|       "dll_signature_verified": true,
 67169|       "dll_relationship_scope": "declared",
 67170|       "dll_semantic_verified": null,
 67171|       "dll_verified_status": "signature_verified_declared",
 67172|       "revitlookup_referenced": null,
 67173|       "revitlookup_requires_document_context": null
 67174|     },
 67175|     {
 67176|       "source": "Autodesk.Revit.DB.Analysis.RouteAnalysisSettings",
 67177|       "target": "Autodesk.Revit.DB.Category",
 67178|       "member_name": "GetIgnoredCategoryIds",
 67179|       "member_kind": "method",
 67180|       "edge_type": "HAS_CATEGORY",
 67181|       "confidence": "elementid_collection_with_strong_name",
 67182|       "confidence_tier": "core",
 67183|       "target_resolution": "exact",
 67184|       "evidence": [
 67185|         "member name 'GetIgnoredCategoryIds' matches keyword pattern /Category/"
 67186|       ],
 67187|       "source_url": "https://www.revitapidocs.com/2025/e4115866-1e15-9b46-d783-7fc5786e88c5.htm",
 67188|       "dll_signature_verified": true,
 67189|       "dll_relationship_scope": "declared",
 67190|       "dll_semantic_verified": null,
 67191|       "dll_verified_status": "signature_verified_declared",
 67192|       "revitlookup_referenced": null,
 67193|       "revitlookup_requires_document_context": null
 67194|     },
 67195|     {
 67196|       "source": "Autodesk.Revit.DB.Analysis.RouteAnalysisSettings",
 67197|       "target": "Autodesk.Revit.DB.Category",
 67198|       "member_name": "SetIgnoredCategoryIds",
 67199|       "member_kind": "method",
 67200|       "edge_type": "HAS_CATEGORY",
 67201|       "confidence": "name_only_candidate",
 67202|       "confidence_tier": "likely",
 67203|       "target_resolution": "exact",
 67204|       "evidence": [
 67205|         "member name 'SetIgnoredCategoryIds' matches keyword pattern /Category/ but return type 'void' gives no type-level confirmation"
 67206|       ],
 67207|       "source_url": "https://www.revitapidocs.com/2025/e7f826a1-97f9-5181-2ddf-aa7a71d8bab7.htm",
 67208|       "dll_signature_verified": true,
 67209|       "dll_relationship_scope": "declared",
 67210|       "dll_semantic_verified": null,
 67211|       "dll_verified_status": "signature_verified_declared",
 67212|       "revitlookup_referenced": null,
 67213|       "revitlookup_requires_document_context": null
 67214|     },
 67215|     {
 67216|       "source": "Autodesk.Revit.DB.Analysis.SpatialFieldManager",
 67217|       "target": null,
 67218|       "member_name": "LegendTextTypeId",
 67219|       "member_kind": "property",
 67220|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 67221|       "confidence": "unknown_reference",
 67222|       "confidence_tier": "unverified_reference",
 67223|       "target_resolution": "none",
 67224|       "evidence": [
 67225|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 67226|       ],
 67227|       "source_url": "https://www.revitapidocs.com/2025/fce9ad33-53db-47a2-b505-993eff7fd532.htm",
 67228|       "dll_signature_verified": true,
 67229|       "dll_relationship_scope": "declared",
 67230|       "dll_semantic_verified": null,
 67231|       "dll_verified_status": "signature_verified_declared",
 67232|       "revitlookup_referenced": null,
 67233|       "revitlookup_requires_document_context": null
 67234|     },
 67235|     {
 67236|       "source": "Autodesk.Revit.DB.Analysis.SpatialFieldManager",
 67237|       "target": "Autodesk.Revit.DB.Analysis.AnalysisDisplayLegend",
 67238|       "member_name": "GetLegend",
 67239|       "member_kind": "method",
 67240|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67241|       "confidence": "direct_return_type",
 67242|       "confidence_tier": "unverified_reference",
 67243|       "target_resolution": "short_name_fallback",
 67244|       "evidence": [
 67245|         "return type 'AnalysisDisplayLegend' directly names a Revit DB object type"
 67246|       ],
 67247|       "source_url": "https://www.revitapidocs.com/2025/c679eb34-4440-8d0c-d0c7-ffeb6e3d5eee.htm",
 67248|       "dll_signature_verified": true,
 67249|       "dll_relationship_scope": "declared",
 67250|       "dll_semantic_verified": null,
 67251|       "dll_verified_status": "signature_verified_declared",
 67252|       "revitlookup_referenced": null,
 67253|       "revitlookup_requires_document_context": null
 67254|     },
 67255|     {
 67256|       "source": "Autodesk.Revit.DB.Analysis.SpatialFieldManager",
 67257|       "target": "Autodesk.Revit.DB.Analysis.AnalysisResultSchema",
 67258|       "member_name": "GetResultSchema",
 67259|       "member_kind": "method",
 67260|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67261|       "confidence": "direct_return_type",
 67262|       "confidence_tier": "unverified_reference",
 67263|       "target_resolution": "short_name_fallback",
 67264|       "evidence": [
 67265|         "return type 'AnalysisResultSchema' directly names a Revit DB object type"
 67266|       ],
 67267|       "source_url": "https://www.revitapidocs.com/2025/4e236ada-5098-9e8c-e23e-2efb06916565.htm",
 67268|       "dll_signature_verified": true,
 67269|       "dll_relationship_scope": "declared",
 67270|       "dll_semantic_verified": null,
 67271|       "dll_verified_status": "signature_verified_declared",
 67272|       "revitlookup_referenced": null,
 67273|       "revitlookup_requires_document_context": null
 67274|     },
 67275|     {
 67276|       "source": "Autodesk.Revit.DB.Analysis.ViewSystemsAnalysisReport",
 67277|       "target": null,
 67278|       "member_name": "GetLatestSystemsAnalysisReport",
 67279|       "member_kind": "method",
 67280|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 67281|       "confidence": "unknown_reference",
 67282|       "confidence_tier": "unverified_reference",
 67283|       "target_resolution": "none",
 67284|       "evidence": [
 67285|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 67286|       ],
 67287|       "source_url": "https://www.revitapidocs.com/2025/e5aabba3-038a-3017-081b-9b1b82cf7872.htm",
 67288|       "dll_signature_verified": true,
 67289|       "dll_relationship_scope": "declared",
 67290|       "dll_semantic_verified": null,
 67291|       "dll_verified_status": "signature_verified_declared",
 67292|       "revitlookup_referenced": null,
 67293|       "revitlookup_requires_document_context": null
 67294|     },
 67295|     {
 67296|       "source": "Autodesk.Revit.DB.Architecture.BalusterInfo",
 67297|       "target": null,
 67298|       "member_name": "BalusterFamilyId",
 67299|       "member_kind": "property",
 67300|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 67301|       "confidence": "unknown_reference",
 67302|       "confidence_tier": "unverified_reference",
 67303|       "target_resolution": "none",
 67304|       "evidence": [
 67305|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 67306|       ],
 67307|       "source_url": "https://www.revitapidocs.com/2025/ca4c5f7c-da65-9a71-51ab-3ba05f6b15ba.htm",
 67308|       "dll_signature_verified": true,
 67309|       "dll_relationship_scope": "declared",
 67310|       "dll_semantic_verified": null,
 67311|       "dll_verified_status": "signature_verified_declared",
 67312|       "revitlookup_referenced": null,
 67313|       "revitlookup_requires_document_context": null
 67314|     },
 67315|     {
 67316|       "source": "Autodesk.Revit.DB.Architecture.BalusterInfo",
 67317|       "target": null,
 67318|       "member_name": "GetReferenceNameForHost",
 67319|       "member_kind": "method",
 67320|       "edge_type": "HOSTED_BY",
 67321|       "confidence": "name_only_candidate",
 67322|       "confidence_tier": "likely",
 67323|       "target_resolution": "none",
 67324|       "evidence": [
 67325|         "member name 'GetReferenceNameForHost' matches keyword pattern /^GetHosted|Host/ but return type 'string' gives no type-level confirmation"
 67326|       ],
 67327|       "source_url": "https://www.revitapidocs.com/2025/9519e69c-82c0-6444-6568-0d29cf9db0b5.htm",
 67328|       "dll_signature_verified": true,
 67329|       "dll_relationship_scope": "declared",
 67330|       "dll_semantic_verified": null,
 67331|       "dll_verified_status": "signature_verified_declared",
 67332|       "revitlookup_referenced": null,
 67333|       "revitlookup_requires_document_context": null
 67334|     },
 67335|     {
 67336|       "source": "Autodesk.Revit.DB.Architecture.BalusterPattern",
 67337|       "target": null,
 67338|       "member_name": "ExcessLengthFillBalusterId",
 67339|       "member_kind": "property",
 67340|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 67341|       "confidence": "unknown_reference",
 67342|       "confidence_tier": "unverified_reference",
 67343|       "target_resolution": "none",
 67344|       "evidence": [
 67345|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 67346|       ],
 67347|       "source_url": "https://www.revitapidocs.com/2025/f65fecb5-e3c1-6a68-dd67-c4e951efdc74.htm",
 67348|       "dll_signature_verified": true,
 67349|       "dll_relationship_scope": "declared",
 67350|       "dll_semantic_verified": null,
 67351|       "dll_verified_status": "signature_verified_declared",
 67352|       "revitlookup_referenced": null,
 67353|       "revitlookup_requires_document_context": null
 67354|     },
 67355|     {
 67356|       "source": "Autodesk.Revit.DB.Architecture.BalusterPattern",
 67357|       "target": "Autodesk.Revit.DB.Architecture.BalusterInfo",
 67358|       "member_name": "DuplicateBaluster",
 67359|       "member_kind": "method",
 67360|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67361|       "confidence": "direct_return_type",
 67362|       "confidence_tier": "unverified_reference",
 67363|       "target_resolution": "short_name_fallback",
 67364|       "evidence": [
 67365|         "return type 'BalusterInfo' directly names a Revit DB object type"
 67366|       ],
 67367|       "source_url": "https://www.revitapidocs.com/2025/10f4c489-9d7e-5520-f8a0-b50a53d87dc5.htm",
 67368|       "dll_signature_verified": true,
 67369|       "dll_relationship_scope": "declared",
 67370|       "dll_semantic_verified": null,
 67371|       "dll_verified_status": "signature_verified_declared",
 67372|       "revitlookup_referenced": null,
 67373|       "revitlookup_requires_document_context": null
 67374|     },
 67375|     {
 67376|       "source": "Autodesk.Revit.DB.Architecture.BalusterPattern",
 67377|       "target": "Autodesk.Revit.DB.Architecture.BalusterInfo",
 67378|       "member_name": "GetBaluster",
 67379|       "member_kind": "method",
 67380|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67381|       "confidence": "direct_return_type",
 67382|       "confidence_tier": "unverified_reference",
 67383|       "target_resolution": "short_name_fallback",
 67384|       "evidence": [
 67385|         "return type 'BalusterInfo' directly names a Revit DB object type"
 67386|       ],
 67387|       "source_url": "https://www.revitapidocs.com/2025/abe74f0f-7ef2-c977-5efb-5a940e9b8df9.htm",
 67388|       "dll_signature_verified": true,
 67389|       "dll_relationship_scope": "declared",
 67390|       "dll_semantic_verified": null,
 67391|       "dll_verified_status": "signature_verified_declared",
 67392|       "revitlookup_referenced": null,
 67393|       "revitlookup_requires_document_context": null
 67394|     },
 67395|     {
 67396|       "source": "Autodesk.Revit.DB.Architecture.BalusterPlacement",
 67397|       "target": "Autodesk.Revit.DB.Architecture.BalusterPattern",
 67398|       "member_name": "BalusterPattern",
 67399|       "member_kind": "property",
 67400|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67401|       "confidence": "direct_return_type",
 67402|       "confidence_tier": "unverified_reference",
 67403|       "target_resolution": "short_name_fallback",
 67404|       "evidence": [
 67405|         "return type 'BalusterPattern' directly names a Revit DB object type"
 67406|       ],
 67407|       "source_url": "https://www.revitapidocs.com/2025/1d8b109e-c916-80fd-f41b-04c9deeccddd.htm",
 67408|       "dll_signature_verified": true,
 67409|       "dll_relationship_scope": "declared",
 67410|       "dll_semantic_verified": null,
 67411|       "dll_verified_status": "signature_verified_declared",
 67412|       "revitlookup_referenced": null,
 67413|       "revitlookup_requires_document_context": null
 67414|     },
 67415|     {
 67416|       "source": "Autodesk.Revit.DB.Architecture.BalusterPlacement",
 67417|       "target": null,
 67418|       "member_name": "BalusterPerTreadFamilyId",
 67419|       "member_kind": "property",
 67420|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 67421|       "confidence": "unknown_reference",
 67422|       "confidence_tier": "unverified_reference",
 67423|       "target_resolution": "none",
 67424|       "evidence": [
 67425|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 67426|       ],
 67427|       "source_url": "https://www.revitapidocs.com/2025/eeb75390-9f5e-1ff8-6259-d454ec3e99e2.htm",
 67428|       "dll_signature_verified": true,
 67429|       "dll_relationship_scope": "declared",
 67430|       "dll_semantic_verified": null,
 67431|       "dll_verified_status": "signature_verified_declared",
 67432|       "revitlookup_referenced": null,
 67433|       "revitlookup_requires_document_context": null
 67434|     },
 67435|     {
 67436|       "source": "Autodesk.Revit.DB.Architecture.BalusterPlacement",
 67437|       "target": "Autodesk.Revit.DB.Architecture.PostPattern",
 67438|       "member_name": "PostPattern",
 67439|       "member_kind": "property",
 67440|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67441|       "confidence": "direct_return_type",
 67442|       "confidence_tier": "unverified_reference",
 67443|       "target_resolution": "short_name_fallback",
 67444|       "evidence": [
 67445|         "return type 'PostPattern' directly names a Revit DB object type"
 67446|       ],
 67447|       "source_url": "https://www.revitapidocs.com/2025/d95f08c8-4d68-4af3-7b4d-c8929848be95.htm",
 67448|       "dll_signature_verified": true,
 67449|       "dll_relationship_scope": "declared",
 67450|       "dll_semantic_verified": null,
 67451|       "dll_verified_status": "signature_verified_declared",
 67452|       "revitlookup_referenced": null,
 67453|       "revitlookup_requires_document_context": null
 67454|     },
 67455|     {
 67456|       "source": "Autodesk.Revit.DB.Architecture.BuildingPad",
 67457|       "target": null,
 67458|       "member_name": "AssociatedTopographySurfaceId",
 67459|       "member_kind": "property",
 67460|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 67461|       "confidence": "unknown_reference",
 67462|       "confidence_tier": "unverified_reference",
 67463|       "target_resolution": "none",
 67464|       "evidence": [
 67465|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 67466|       ],
 67467|       "source_url": "https://www.revitapidocs.com/2025/7c972f93-c6c8-dd93-3bb0-6bf5aae67d30.htm",
 67468|       "dll_signature_verified": true,
 67469|       "dll_relationship_scope": "declared",
 67470|       "dll_semantic_verified": null,
 67471|       "dll_verified_status": "signature_verified_declared",
 67472|       "revitlookup_referenced": null,
 67473|       "revitlookup_requires_document_context": null
 67474|     },
 67475|     {
 67476|       "source": "Autodesk.Revit.DB.Architecture.BuildingPad",
 67477|       "target": null,
 67478|       "member_name": "HostId",
 67479|       "member_kind": "property",
 67480|       "edge_type": "HOSTED_BY",
```
