# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 158 of 216
- Original line range: 61231-61630
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 61231|       "source": "Autodesk.Revit.DB.TransactWithCentralOptions",
 61232|       "target": "Autodesk.Revit.DB.ICentralLockedCallback",
 61233|       "member_name": "GetLockCallback",
 61234|       "member_kind": "method",
 61235|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61236|       "confidence": "direct_return_type",
 61237|       "confidence_tier": "unverified_reference",
 61238|       "target_resolution": "exact",
 61239|       "evidence": [
 61240|         "return type 'ICentralLockedCallback' directly names a Revit DB object type"
 61241|       ],
 61242|       "source_url": "https://www.revitapidocs.com/2025/07798372-8670-5cc0-f8eb-3df6a64d9c75.htm",
 61243|       "dll_signature_verified": true,
 61244|       "dll_relationship_scope": "declared",
 61245|       "dll_semantic_verified": null,
 61246|       "dll_verified_status": "signature_verified_declared",
 61247|       "revitlookup_referenced": null,
 61248|       "revitlookup_requires_document_context": null
 61249|     },
 61250|     {
 61251|       "source": "Autodesk.Revit.DB.Transform1D",
 61252|       "target": null,
 61253|       "member_name": "TransformParameterDomain",
 61254|       "member_kind": "method",
 61255|       "edge_type": "HAS_PARAMETER",
 61256|       "confidence": "name_only_candidate",
 61257|       "confidence_tier": "likely",
 61258|       "target_resolution": "none",
 61259|       "evidence": [
 61260|         "member name 'TransformParameterDomain' matches keyword pattern /Parameter/ but return type 'IList < double >' gives no type-level confirmation"
 61261|       ],
 61262|       "source_url": "https://www.revitapidocs.com/2025/fe97e6d2-eea0-26e5-0d32-16281ea95d19.htm",
 61263|       "dll_signature_verified": true,
 61264|       "dll_relationship_scope": "declared",
 61265|       "dll_semantic_verified": null,
 61266|       "dll_verified_status": "signature_verified_declared",
 61267|       "revitlookup_referenced": null,
 61268|       "revitlookup_requires_document_context": null
 61269|     },
 61270|     {
 61271|       "source": "Autodesk.Revit.DB.Transform2D",
 61272|       "target": "Autodesk.Revit.DB.BoundingBoxUV",
 61273|       "member_name": "TransformUVDomainIfPossible",
 61274|       "member_kind": "method",
 61275|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61276|       "confidence": "direct_return_type",
 61277|       "confidence_tier": "unverified_reference",
 61278|       "target_resolution": "exact",
 61279|       "evidence": [
 61280|         "return type 'BoundingBoxUV' directly names a Revit DB object type"
 61281|       ],
 61282|       "source_url": "https://www.revitapidocs.com/2025/977e71c5-7a76-a4ee-5232-f826a00f7471.htm",
 61283|       "dll_signature_verified": true,
 61284|       "dll_relationship_scope": "declared",
 61285|       "dll_semantic_verified": null,
 61286|       "dll_verified_status": "signature_verified_declared",
 61287|       "revitlookup_referenced": null,
 61288|       "revitlookup_requires_document_context": null
 61289|     },
 61290|     {
 61291|       "source": "Autodesk.Revit.DB.TransmissionData",
 61292|       "target": null,
 61293|       "member_name": "GetAllExternalFileReferenceIds",
 61294|       "member_kind": "method",
 61295|       "edge_type": "RETURNS_ELEMENT_IDS",
 61296|       "confidence": "elementid_collection_with_strong_name",
 61297|       "confidence_tier": "core",
 61298|       "target_resolution": "none",
 61299|       "evidence": [
 61300|         "member name 'GetAllExternalFileReferenceIds' matches keyword pattern /^GetAll/"
 61301|       ],
 61302|       "source_url": "https://www.revitapidocs.com/2025/7df7afd5-8c73-30d9-0f01-0690a3861d1f.htm",
 61303|       "dll_signature_verified": true,
 61304|       "dll_relationship_scope": "declared",
 61305|       "dll_semantic_verified": null,
 61306|       "dll_verified_status": "signature_verified_declared",
 61307|       "revitlookup_referenced": null,
 61308|       "revitlookup_requires_document_context": null
 61309|     },
 61310|     {
 61311|       "source": "Autodesk.Revit.DB.TransmissionData",
 61312|       "target": "Autodesk.Revit.DB.ExternalFileReference",
 61313|       "member_name": "GetDesiredReferenceData",
 61314|       "member_kind": "method",
 61315|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61316|       "confidence": "direct_return_type",
 61317|       "confidence_tier": "unverified_reference",
 61318|       "target_resolution": "exact",
 61319|       "evidence": [
 61320|         "return type 'ExternalFileReference' directly names a Revit DB object type"
 61321|       ],
 61322|       "source_url": "https://www.revitapidocs.com/2025/c27ef733-3960-9710-64e9-aa42b01657dc.htm",
 61323|       "dll_signature_verified": true,
 61324|       "dll_relationship_scope": "declared",
 61325|       "dll_semantic_verified": null,
 61326|       "dll_verified_status": "signature_verified_declared",
 61327|       "revitlookup_referenced": null,
 61328|       "revitlookup_requires_document_context": null
 61329|     },
 61330|     {
 61331|       "source": "Autodesk.Revit.DB.TransmissionData",
 61332|       "target": "Autodesk.Revit.DB.ExternalFileReference",
 61333|       "member_name": "GetLastSavedReferenceData",
 61334|       "member_kind": "method",
 61335|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61336|       "confidence": "direct_return_type",
 61337|       "confidence_tier": "unverified_reference",
 61338|       "target_resolution": "exact",
 61339|       "evidence": [
 61340|         "return type 'ExternalFileReference' directly names a Revit DB object type"
 61341|       ],
 61342|       "source_url": "https://www.revitapidocs.com/2025/d5e70e0b-69f2-fcb4-0d91-4b184930b68d.htm",
 61343|       "dll_signature_verified": true,
 61344|       "dll_relationship_scope": "declared",
 61345|       "dll_semantic_verified": null,
 61346|       "dll_verified_status": "signature_verified_declared",
 61347|       "revitlookup_referenced": null,
 61348|       "revitlookup_requires_document_context": null
 61349|     },
 61350|     {
 61351|       "source": "Autodesk.Revit.DB.TriangulatedShellComponent",
 61352|       "target": "Autodesk.Revit.DB.TriangleInShellComponent",
 61353|       "member_name": "GetTriangle",
 61354|       "member_kind": "method",
 61355|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61356|       "confidence": "direct_return_type",
 61357|       "confidence_tier": "unverified_reference",
 61358|       "target_resolution": "exact",
 61359|       "evidence": [
 61360|         "return type 'TriangleInShellComponent' directly names a Revit DB object type"
 61361|       ],
 61362|       "source_url": "https://www.revitapidocs.com/2025/dfc40de3-d27b-45b8-1539-0ca682316592.htm",
 61363|       "dll_signature_verified": true,
 61364|       "dll_relationship_scope": "declared",
 61365|       "dll_semantic_verified": null,
 61366|       "dll_verified_status": "signature_verified_declared",
 61367|       "revitlookup_referenced": null,
 61368|       "revitlookup_requires_document_context": null
 61369|     },
 61370|     {
 61371|       "source": "Autodesk.Revit.DB.TriangulatedSolidOrShell",
 61372|       "target": "Autodesk.Revit.DB.TriangulatedShellComponent",
 61373|       "member_name": "GetShellComponent",
 61374|       "member_kind": "method",
 61375|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61376|       "confidence": "direct_return_type",
 61377|       "confidence_tier": "unverified_reference",
 61378|       "target_resolution": "exact",
 61379|       "evidence": [
 61380|         "return type 'TriangulatedShellComponent' directly names a Revit DB object type"
 61381|       ],
 61382|       "source_url": "https://www.revitapidocs.com/2025/af104f01-d601-4e61-ee48-9cd75a3b1b06.htm",
 61383|       "dll_signature_verified": true,
 61384|       "dll_relationship_scope": "declared",
 61385|       "dll_semantic_verified": null,
 61386|       "dll_verified_status": "signature_verified_declared",
 61387|       "revitlookup_referenced": null,
 61388|       "revitlookup_requires_document_context": null
 61389|     },
 61390|     {
 61391|       "source": "Autodesk.Revit.DB.Units",
 61392|       "target": null,
 61393|       "member_name": "DigitGroupingAmount",
 61394|       "member_kind": "property",
 61395|       "edge_type": "MEMBER_OF_GROUP",
 61396|       "confidence": "name_only_candidate",
 61397|       "confidence_tier": "likely",
 61398|       "target_resolution": "none",
 61399|       "evidence": [
 61400|         "member name 'DigitGroupingAmount' matches keyword pattern /^GetMember|Group/ but return type 'DigitGroupingAmount' gives no type-level confirmation"
 61401|       ],
 61402|       "source_url": "https://www.revitapidocs.com/2025/03c48a42-726d-2ca7-64ae-79b851820fd1.htm",
 61403|       "dll_signature_verified": true,
 61404|       "dll_relationship_scope": "declared",
 61405|       "dll_semantic_verified": null,
 61406|       "dll_verified_status": "signature_verified_declared",
 61407|       "revitlookup_referenced": null,
 61408|       "revitlookup_requires_document_context": null
 61409|     },
 61410|     {
 61411|       "source": "Autodesk.Revit.DB.Units",
 61412|       "target": null,
 61413|       "member_name": "DigitGroupingSymbol",
 61414|       "member_kind": "property",
 61415|       "edge_type": "MEMBER_OF_GROUP",
 61416|       "confidence": "name_only_candidate",
 61417|       "confidence_tier": "likely",
 61418|       "target_resolution": "none",
 61419|       "evidence": [
 61420|         "member name 'DigitGroupingSymbol' matches keyword pattern /^GetMember|Group/ but return type 'DigitGroupingSymbol' gives no type-level confirmation"
 61421|       ],
 61422|       "source_url": "https://www.revitapidocs.com/2025/09e0547f-f950-b2aa-1f0c-52c4b62f1ced.htm",
 61423|       "dll_signature_verified": true,
 61424|       "dll_relationship_scope": "declared",
 61425|       "dll_semantic_verified": null,
 61426|       "dll_verified_status": "signature_verified_declared",
 61427|       "revitlookup_referenced": null,
 61428|       "revitlookup_requires_document_context": null
 61429|     },
 61430|     {
 61431|       "source": "Autodesk.Revit.DB.UpdaterData",
 61432|       "target": null,
 61433|       "member_name": "GetAddedElementIds",
 61434|       "member_kind": "method",
 61435|       "edge_type": "RETURNS_ELEMENT_IDS",
 61436|       "confidence": "unknown_reference",
 61437|       "confidence_tier": "unverified_reference",
 61438|       "target_resolution": "none",
 61439|       "evidence": [
 61440|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 61441|       ],
 61442|       "source_url": "https://www.revitapidocs.com/2025/b9676f82-ebc4-79f8-160e-4d3c4c1823a2.htm",
 61443|       "dll_signature_verified": true,
 61444|       "dll_relationship_scope": "declared",
 61445|       "dll_semantic_verified": null,
 61446|       "dll_verified_status": "signature_verified_declared",
 61447|       "revitlookup_referenced": null,
 61448|       "revitlookup_requires_document_context": null
 61449|     },
 61450|     {
 61451|       "source": "Autodesk.Revit.DB.UpdaterData",
 61452|       "target": null,
 61453|       "member_name": "GetDeletedElementIds",
 61454|       "member_kind": "method",
 61455|       "edge_type": "RETURNS_ELEMENT_IDS",
 61456|       "confidence": "unknown_reference",
 61457|       "confidence_tier": "unverified_reference",
 61458|       "target_resolution": "none",
 61459|       "evidence": [
 61460|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 61461|       ],
 61462|       "source_url": "https://www.revitapidocs.com/2025/d19575f3-a6cb-c532-78a2-2b513378af4a.htm",
 61463|       "dll_signature_verified": true,
 61464|       "dll_relationship_scope": "declared",
 61465|       "dll_semantic_verified": null,
 61466|       "dll_verified_status": "signature_verified_declared",
 61467|       "revitlookup_referenced": null,
 61468|       "revitlookup_requires_document_context": null
 61469|     },
 61470|     {
 61471|       "source": "Autodesk.Revit.DB.UpdaterData",
 61472|       "target": "Autodesk.Revit.DB.Document",
 61473|       "member_name": "GetDocument",
 61474|       "member_kind": "method",
 61475|       "edge_type": "REFERENCES",
 61476|       "confidence": "direct_return_type",
 61477|       "confidence_tier": "core",
 61478|       "target_resolution": "exact",
 61479|       "evidence": [
 61480|         "return type 'Document' directly names a Revit DB object type"
 61481|       ],
 61482|       "source_url": "https://www.revitapidocs.com/2025/cb58fbb1-e923-b2f3-8b74-9aac45ad2d0f.htm",
 61483|       "dll_signature_verified": true,
 61484|       "dll_relationship_scope": "declared",
 61485|       "dll_semantic_verified": null,
 61486|       "dll_verified_status": "signature_verified_declared",
 61487|       "revitlookup_referenced": null,
 61488|       "revitlookup_requires_document_context": null
 61489|     },
 61490|     {
 61491|       "source": "Autodesk.Revit.DB.UpdaterData",
 61492|       "target": null,
 61493|       "member_name": "GetModifiedElementIds",
 61494|       "member_kind": "method",
 61495|       "edge_type": "RETURNS_ELEMENT_IDS",
 61496|       "confidence": "unknown_reference",
 61497|       "confidence_tier": "unverified_reference",
 61498|       "target_resolution": "none",
 61499|       "evidence": [
 61500|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 61501|       ],
 61502|       "source_url": "https://www.revitapidocs.com/2025/f06a0804-5756-47e7-3dc3-bcc828e5adaf.htm",
 61503|       "dll_signature_verified": true,
 61504|       "dll_relationship_scope": "declared",
 61505|       "dll_semantic_verified": null,
 61506|       "dll_verified_status": "signature_verified_declared",
 61507|       "revitlookup_referenced": null,
 61508|       "revitlookup_requires_document_context": null
 61509|     },
 61510|     {
 61511|       "source": "Autodesk.Revit.DB.UpdaterId",
 61512|       "target": "Autodesk.Revit.DB.AddInId",
 61513|       "member_name": "GetAddInId",
 61514|       "member_kind": "method",
 61515|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61516|       "confidence": "direct_return_type",
 61517|       "confidence_tier": "unverified_reference",
 61518|       "target_resolution": "exact",
 61519|       "evidence": [
 61520|         "return type 'AddInId' directly names a Revit DB object type"
 61521|       ],
 61522|       "source_url": "https://www.revitapidocs.com/2025/7ee77980-7354-632d-76f3-b9d496704bc4.htm",
 61523|       "dll_signature_verified": true,
 61524|       "dll_relationship_scope": "declared",
 61525|       "dll_semantic_verified": null,
 61526|       "dll_verified_status": "signature_verified_declared",
 61527|       "revitlookup_referenced": null,
 61528|       "revitlookup_requires_document_context": null
 61529|     },
 61530|     {
 61531|       "source": "Autodesk.Revit.DB.UpdaterRegistry",
 61532|       "target": "Autodesk.Revit.DB.UpdaterInfo",
 61533|       "member_name": "GetRegisteredUpdaterInfos",
 61534|       "member_kind": "method",
 61535|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61536|       "confidence": "needs_runtime_validation",
 61537|       "confidence_tier": "needs_validation",
 61538|       "target_resolution": "exact",
 61539|       "evidence": [
 61540|         "return type 'IList < UpdaterInfo >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 61541|       ],
 61542|       "source_url": "https://www.revitapidocs.com/2025/1cf828fd-f6f4-56cd-c428-b957fcf912ff.htm",
 61543|       "dll_signature_verified": true,
 61544|       "dll_relationship_scope": "declared",
 61545|       "dll_semantic_verified": null,
 61546|       "dll_verified_status": "signature_verified_declared",
 61547|       "revitlookup_referenced": null,
 61548|       "revitlookup_requires_document_context": null
 61549|     },
 61550|     {
 61551|       "source": "Autodesk.Revit.DB.UpdaterRegistry",
 61552|       "target": "Autodesk.Revit.DB.UpdaterInfo",
 61553|       "member_name": "GetRegisteredUpdaterInfos",
 61554|       "member_kind": "method",
 61555|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61556|       "confidence": "needs_runtime_validation",
 61557|       "confidence_tier": "needs_validation",
 61558|       "target_resolution": "exact",
 61559|       "evidence": [
 61560|         "return type 'IList < UpdaterInfo >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 61561|       ],
 61562|       "source_url": "https://www.revitapidocs.com/2025/cfbf287a-a972-238e-def6-9c8cc6640db9.htm",
 61563|       "dll_signature_verified": true,
 61564|       "dll_relationship_scope": "declared",
 61565|       "dll_semantic_verified": null,
 61566|       "dll_verified_status": "signature_verified_declared",
 61567|       "revitlookup_referenced": null,
 61568|       "revitlookup_requires_document_context": null
 61569|     },
 61570|     {
 61571|       "source": "Autodesk.Revit.DB.VertexIndexPairArray",
 61572|       "target": "Autodesk.Revit.DB.VertexIndexPairArrayIterator",
 61573|       "member_name": "ForwardIterator",
 61574|       "member_kind": "method",
 61575|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61576|       "confidence": "direct_return_type",
 61577|       "confidence_tier": "unverified_reference",
 61578|       "target_resolution": "exact",
 61579|       "evidence": [
 61580|         "return type 'VertexIndexPairArrayIterator' directly names a Revit DB object type"
 61581|       ],
 61582|       "source_url": "https://www.revitapidocs.com/2025/593d1eca-d0c9-ed33-2028-42971302435a.htm",
 61583|       "dll_signature_verified": true,
 61584|       "dll_relationship_scope": "declared",
 61585|       "dll_semantic_verified": null,
 61586|       "dll_verified_status": "signature_verified_declared",
 61587|       "revitlookup_referenced": null,
 61588|       "revitlookup_requires_document_context": null
 61589|     },
 61590|     {
 61591|       "source": "Autodesk.Revit.DB.VertexIndexPairArray",
 61592|       "target": "Autodesk.Revit.DB.VertexIndexPairArrayIterator",
 61593|       "member_name": "ReverseIterator",
 61594|       "member_kind": "method",
 61595|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61596|       "confidence": "direct_return_type",
 61597|       "confidence_tier": "unverified_reference",
 61598|       "target_resolution": "exact",
 61599|       "evidence": [
 61600|         "return type 'VertexIndexPairArrayIterator' directly names a Revit DB object type"
 61601|       ],
 61602|       "source_url": "https://www.revitapidocs.com/2025/da665fb0-51d9-3089-b739-697a70d2d4fb.htm",
 61603|       "dll_signature_verified": true,
 61604|       "dll_relationship_scope": "declared",
 61605|       "dll_semantic_verified": null,
 61606|       "dll_verified_status": "signature_verified_declared",
 61607|       "revitlookup_referenced": null,
 61608|       "revitlookup_requires_document_context": null
 61609|     },
 61610|     {
 61611|       "source": "Autodesk.Revit.DB.View",
 61612|       "target": null,
 61613|       "member_name": "AnalysisDisplayStyleId",
 61614|       "member_kind": "property",
 61615|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 61616|       "confidence": "unknown_reference",
 61617|       "confidence_tier": "unverified_reference",
 61618|       "target_resolution": "none",
 61619|       "evidence": [
 61620|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 61621|       ],
 61622|       "source_url": "https://www.revitapidocs.com/2025/7becc6f0-0510-745b-b4ba-24bbf875919d.htm",
 61623|       "dll_signature_verified": true,
 61624|       "dll_relationship_scope": "declared",
 61625|       "dll_semantic_verified": null,
 61626|       "dll_verified_status": "signature_verified_declared",
 61627|       "revitlookup_referenced": null,
 61628|       "revitlookup_requires_document_context": null
 61629|     },
 61630|     {
```
