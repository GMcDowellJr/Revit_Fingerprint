# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 109 of 216
- Original line range: 42121-42520
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 42121|       "confidence_tier": "core",
 42122|       "target_resolution": "exact",
 42123|       "evidence": [
 42124|         "return type 'DefinitionGroup' directly names a Revit DB object type"
 42125|       ],
 42126|       "source_url": "https://www.revitapidocs.com/2025/4ec35eb2-c09f-deee-7b2d-9f87c24e5f2c.htm",
 42127|       "dll_signature_verified": true,
 42128|       "dll_relationship_scope": "declared",
 42129|       "dll_semantic_verified": null,
 42130|       "dll_verified_status": "signature_verified_declared",
 42131|       "revitlookup_referenced": null,
 42132|       "revitlookup_requires_document_context": null
 42133|     },
 42134|     {
 42135|       "source": "Autodesk.Revit.DB.ExternalFileReference",
 42136|       "target": null,
 42137|       "member_name": "GetReferencingId",
 42138|       "member_kind": "method",
 42139|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 42140|       "confidence": "unknown_reference",
 42141|       "confidence_tier": "unverified_reference",
 42142|       "target_resolution": "none",
 42143|       "evidence": [
 42144|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 42145|       ],
 42146|       "source_url": "https://www.revitapidocs.com/2025/1f0e36ac-506b-4b7e-5869-e13f982736ab.htm",
 42147|       "dll_signature_verified": true,
 42148|       "dll_relationship_scope": "declared",
 42149|       "dll_semantic_verified": null,
 42150|       "dll_verified_status": "signature_verified_declared",
 42151|       "revitlookup_referenced": null,
 42152|       "revitlookup_requires_document_context": null
 42153|     },
 42154|     {
 42155|       "source": "Autodesk.Revit.DB.ExternalFileUtils",
 42156|       "target": null,
 42157|       "member_name": "GetAllExternalFileReferences",
 42158|       "member_kind": "method",
 42159|       "edge_type": "RETURNS_ELEMENT_IDS",
 42160|       "confidence": "elementid_collection_with_strong_name",
 42161|       "confidence_tier": "core",
 42162|       "target_resolution": "none",
 42163|       "evidence": [
 42164|         "member name 'GetAllExternalFileReferences' matches keyword pattern /^GetAll/"
 42165|       ],
 42166|       "source_url": "https://www.revitapidocs.com/2025/be61b425-020c-61c6-9199-05feb39a0ebf.htm",
 42167|       "dll_signature_verified": true,
 42168|       "dll_relationship_scope": "declared",
 42169|       "dll_semantic_verified": null,
 42170|       "dll_verified_status": "signature_verified_declared",
 42171|       "revitlookup_referenced": null,
 42172|       "revitlookup_requires_document_context": null
 42173|     },
 42174|     {
 42175|       "source": "Autodesk.Revit.DB.ExternalFileUtils",
 42176|       "target": "Autodesk.Revit.DB.ExternalFileReference",
 42177|       "member_name": "GetExternalFileReference",
 42178|       "member_kind": "method",
 42179|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 42180|       "confidence": "direct_return_type",
 42181|       "confidence_tier": "unverified_reference",
 42182|       "target_resolution": "exact",
 42183|       "evidence": [
 42184|         "return type 'ExternalFileReference' directly names a Revit DB object type"
 42185|       ],
 42186|       "source_url": "https://www.revitapidocs.com/2025/edede302-83dc-c285-17ea-5d0a168a94dd.htm",
 42187|       "dll_signature_verified": true,
 42188|       "dll_relationship_scope": "declared",
 42189|       "dll_semantic_verified": null,
 42190|       "dll_verified_status": "signature_verified_declared",
 42191|       "revitlookup_referenced": null,
 42192|       "revitlookup_requires_document_context": null
 42193|     },
 42194|     {
 42195|       "source": "Autodesk.Revit.DB.ExternallyTaggedGeometryObject",
 42196|       "target": "Autodesk.Revit.DB.ExternalGeometryId",
 42197|       "member_name": "ExternalId",
 42198|       "member_kind": "property",
 42199|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 42200|       "confidence": "direct_return_type",
 42201|       "confidence_tier": "unverified_reference",
 42202|       "target_resolution": "exact",
 42203|       "evidence": [
 42204|         "return type 'ExternalGeometryId' directly names a Revit DB object type"
 42205|       ],
 42206|       "source_url": "https://www.revitapidocs.com/2025/54c4728b-b8ce-47b3-a67a-5687fb8b7b1b.htm",
 42207|       "dll_signature_verified": true,
 42208|       "dll_relationship_scope": "declared",
 42209|       "dll_semantic_verified": null,
 42210|       "dll_verified_status": "signature_verified_declared",
 42211|       "revitlookup_referenced": null,
 42212|       "revitlookup_requires_document_context": null
 42213|     },
 42214|     {
 42215|       "source": "Autodesk.Revit.DB.ExternallyTaggedNonBReps",
 42216|       "target": null,
 42217|       "member_name": "CanAddExternallyTaggedNonBRep",
 42218|       "member_kind": "method",
 42219|       "edge_type": "TAGS_ELEMENT",
 42220|       "confidence": "name_only_candidate",
 42221|       "confidence_tier": "likely",
 42222|       "target_resolution": "none",
 42223|       "evidence": [
 42224|         "member name 'CanAddExternallyTaggedNonBRep' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'bool' gives no type-level confirmation"
 42225|       ],
 42226|       "source_url": "https://www.revitapidocs.com/2025/47c39c8d-5edc-2b78-1dfb-23af7fe4e6f8.htm",
 42227|       "dll_signature_verified": true,
 42228|       "dll_relationship_scope": "declared",
 42229|       "dll_semantic_verified": null,
 42230|       "dll_verified_status": "signature_verified_declared",
 42231|       "revitlookup_referenced": null,
 42232|       "revitlookup_requires_document_context": null
 42233|     },
 42234|     {
 42235|       "source": "Autodesk.Revit.DB.ExternalResourceBrowserData",
 42236|       "target": "Autodesk.Revit.DB.ExternalResourceMatchOptions",
 42237|       "member_name": "GetMatchOptions",
 42238|       "member_kind": "method",
 42239|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 42240|       "confidence": "direct_return_type",
 42241|       "confidence_tier": "unverified_reference",
 42242|       "target_resolution": "exact",
 42243|       "evidence": [
 42244|         "return type 'ExternalResourceMatchOptions' directly names a Revit DB object type"
 42245|       ],
 42246|       "source_url": "https://www.revitapidocs.com/2025/18e6e337-9e0e-3c4f-b021-59003c5b4883.htm",
 42247|       "dll_signature_verified": true,
 42248|       "dll_relationship_scope": "declared",
 42249|       "dll_semantic_verified": null,
 42250|       "dll_verified_status": "signature_verified_declared",
 42251|       "revitlookup_referenced": null,
 42252|       "revitlookup_requires_document_context": null
 42253|     },
 42254|     {
 42255|       "source": "Autodesk.Revit.DB.ExternalResourceBrowserData",
 42256|       "target": "Autodesk.Revit.DB.ExternalResourceReference",
 42257|       "member_name": "GetResources",
 42258|       "member_kind": "method",
 42259|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 42260|       "confidence": "needs_runtime_validation",
 42261|       "confidence_tier": "needs_validation",
 42262|       "target_resolution": "exact",
 42263|       "evidence": [
 42264|         "return type 'IList < ExternalResourceReference >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 42265|       ],
 42266|       "source_url": "https://www.revitapidocs.com/2025/616cff02-a764-70ad-251b-c0b494145c74.htm",
 42267|       "dll_signature_verified": true,
 42268|       "dll_relationship_scope": "declared",
 42269|       "dll_semantic_verified": null,
 42270|       "dll_verified_status": "signature_verified_declared",
 42271|       "revitlookup_referenced": null,
 42272|       "revitlookup_requires_document_context": null
 42273|     },
 42274|     {
 42275|       "source": "Autodesk.Revit.DB.ExternalResourceBrowserData",
 42276|       "target": "Autodesk.Revit.DB.ExternalResourceSubFolder",
 42277|       "member_name": "GetSubFoldersData",
 42278|       "member_kind": "method",
 42279|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 42280|       "confidence": "needs_runtime_validation",
 42281|       "confidence_tier": "needs_validation",
 42282|       "target_resolution": "exact",
 42283|       "evidence": [
 42284|         "return type 'IList < ExternalResourceSubFolder >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 42285|       ],
 42286|       "source_url": "https://www.revitapidocs.com/2025/68ac11a5-1134-4944-3d57-e002cd376bec.htm",
 42287|       "dll_signature_verified": true,
 42288|       "dll_relationship_scope": "declared",
 42289|       "dll_semantic_verified": null,
 42290|       "dll_verified_status": "signature_verified_declared",
 42291|       "revitlookup_referenced": null,
 42292|       "revitlookup_requires_document_context": null
 42293|     },
 42294|     {
 42295|       "source": "Autodesk.Revit.DB.ExternalResourceLoadContext",
 42296|       "target": "Autodesk.Revit.DB.ExternalResourceReference",
 42297|       "member_name": "GetCurrentlyLoadedReference",
 42298|       "member_kind": "method",
 42299|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 42300|       "confidence": "direct_return_type",
 42301|       "confidence_tier": "unverified_reference",
 42302|       "target_resolution": "exact",
 42303|       "evidence": [
 42304|         "return type 'ExternalResourceReference' directly names a Revit DB object type"
 42305|       ],
 42306|       "source_url": "https://www.revitapidocs.com/2025/8f5826f7-3f2e-69e0-23f3-6e6c2cbdf6c6.htm",
 42307|       "dll_signature_verified": true,
 42308|       "dll_relationship_scope": "declared",
 42309|       "dll_semantic_verified": null,
 42310|       "dll_verified_status": "signature_verified_declared",
 42311|       "revitlookup_referenced": null,
 42312|       "revitlookup_requires_document_context": null
 42313|     },
 42314|     {
 42315|       "source": "Autodesk.Revit.DB.ExternalResourceLoadData",
 42316|       "target": "Autodesk.Revit.DB.ExternalResourceReference",
 42317|       "member_name": "GetExternalResourceReference",
 42318|       "member_kind": "method",
 42319|       "edge_type": "REFERENCES",
 42320|       "confidence": "direct_return_type",
 42321|       "confidence_tier": "core",
 42322|       "target_resolution": "exact",
 42323|       "evidence": [
 42324|         "return type 'ExternalResourceReference' directly names a Revit DB object type"
 42325|       ],
 42326|       "source_url": "https://www.revitapidocs.com/2025/9d7e42c8-561c-374b-e8d9-d16c1f46dfa9.htm",
 42327|       "dll_signature_verified": true,
 42328|       "dll_relationship_scope": "declared",
 42329|       "dll_semantic_verified": null,
 42330|       "dll_verified_status": "signature_verified_declared",
 42331|       "revitlookup_referenced": null,
 42332|       "revitlookup_requires_document_context": null
 42333|     },
 42334|     {
 42335|       "source": "Autodesk.Revit.DB.ExternalResourceLoadData",
 42336|       "target": "Autodesk.Revit.DB.ExternalResourceLoadContent",
 42337|       "member_name": "GetLoadContent",
 42338|       "member_kind": "method",
 42339|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 42340|       "confidence": "direct_return_type",
 42341|       "confidence_tier": "unverified_reference",
 42342|       "target_resolution": "exact",
 42343|       "evidence": [
 42344|         "return type 'ExternalResourceLoadContent' directly names a Revit DB object type"
 42345|       ],
 42346|       "source_url": "https://www.revitapidocs.com/2025/17491a3b-1bc7-161c-56e0-836c58537585.htm",
 42347|       "dll_signature_verified": true,
 42348|       "dll_relationship_scope": "declared",
 42349|       "dll_semantic_verified": null,
 42350|       "dll_verified_status": "signature_verified_declared",
 42351|       "revitlookup_referenced": null,
 42352|       "revitlookup_requires_document_context": null
 42353|     },
 42354|     {
 42355|       "source": "Autodesk.Revit.DB.ExternalResourceLoadData",
 42356|       "target": "Autodesk.Revit.DB.ExternalResourceLoadContext",
 42357|       "member_name": "GetLoadContext",
 42358|       "member_kind": "method",
 42359|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 42360|       "confidence": "direct_return_type",
 42361|       "confidence_tier": "unverified_reference",
 42362|       "target_resolution": "exact",
 42363|       "evidence": [
 42364|         "return type 'ExternalResourceLoadContext' directly names a Revit DB object type"
 42365|       ],
 42366|       "source_url": "https://www.revitapidocs.com/2025/a22e400a-700b-1dc3-fd62-8f1af22ffdcf.htm",
 42367|       "dll_signature_verified": true,
 42368|       "dll_relationship_scope": "declared",
 42369|       "dll_semantic_verified": null,
 42370|       "dll_verified_status": "signature_verified_declared",
 42371|       "revitlookup_referenced": null,
 42372|       "revitlookup_requires_document_context": null
 42373|     },
 42374|     {
 42375|       "source": "Autodesk.Revit.DB.ExternalResourceServerExtensions",
 42376|       "target": "Autodesk.Revit.DB.CADLinkOperations",
 42377|       "member_name": "GetCADLinkOperations",
 42378|       "member_kind": "method",
 42379|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 42380|       "confidence": "direct_return_type",
 42381|       "confidence_tier": "unverified_reference",
 42382|       "target_resolution": "exact",
 42383|       "evidence": [
 42384|         "return type 'CADLinkOperations' directly names a Revit DB object type"
 42385|       ],
 42386|       "source_url": "https://www.revitapidocs.com/2025/b4ed124d-5577-411e-946e-04bd0a3dc522.htm",
 42387|       "dll_signature_verified": true,
 42388|       "dll_relationship_scope": "declared",
 42389|       "dll_semantic_verified": null,
 42390|       "dll_verified_status": "signature_verified_declared",
 42391|       "revitlookup_referenced": null,
 42392|       "revitlookup_requires_document_context": null
 42393|     },
 42394|     {
 42395|       "source": "Autodesk.Revit.DB.ExternalResourceServerExtensions",
 42396|       "target": "Autodesk.Revit.DB.RevitLinkOperations",
 42397|       "member_name": "GetRevitLinkOperations",
 42398|       "member_kind": "method",
 42399|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 42400|       "confidence": "direct_return_type",
 42401|       "confidence_tier": "unverified_reference",
 42402|       "target_resolution": "exact",
 42403|       "evidence": [
 42404|         "return type 'RevitLinkOperations' directly names a Revit DB object type"
 42405|       ],
 42406|       "source_url": "https://www.revitapidocs.com/2025/b0f35e96-beaf-cc07-d3d6-52788f63d16f.htm",
 42407|       "dll_signature_verified": true,
 42408|       "dll_relationship_scope": "declared",
 42409|       "dll_semantic_verified": null,
 42410|       "dll_verified_status": "signature_verified_declared",
 42411|       "revitlookup_referenced": null,
 42412|       "revitlookup_requires_document_context": null
 42413|     },
 42414|     {
 42415|       "source": "Autodesk.Revit.DB.ExternalResourceServerUtils",
 42416|       "target": null,
 42417|       "member_name": "ServerSupportsAssemblyCodeData",
 42418|       "member_kind": "method",
 42419|       "edge_type": "MEMBER_OF_ASSEMBLY",
 42420|       "confidence": "name_only_candidate",
 42421|       "confidence_tier": "likely",
 42422|       "target_resolution": "none",
 42423|       "evidence": [
 42424|         "member name 'ServerSupportsAssemblyCodeData' matches keyword pattern /Assembly/ but return type 'bool' gives no type-level confirmation"
 42425|       ],
 42426|       "source_url": "https://www.revitapidocs.com/2025/7db6277b-f48f-a7e9-6bd4-2798999cb9df.htm",
 42427|       "dll_signature_verified": true,
 42428|       "dll_relationship_scope": "declared",
 42429|       "dll_semantic_verified": null,
 42430|       "dll_verified_status": "signature_verified_declared",
 42431|       "revitlookup_referenced": null,
 42432|       "revitlookup_requires_document_context": null
 42433|     },
 42434|     {
 42435|       "source": "Autodesk.Revit.DB.ExternalResourceServiceUtils",
 42436|       "target": "Autodesk.Revit.DB.IExternalResourceServer",
 42437|       "member_name": "GetServersByType",
 42438|       "member_kind": "method",
 42439|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 42440|       "confidence": "needs_runtime_validation",
 42441|       "confidence_tier": "needs_validation",
 42442|       "target_resolution": "exact",
 42443|       "evidence": [
 42444|         "return type 'IList < IExternalResourceServer >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 42445|       ],
 42446|       "source_url": "https://www.revitapidocs.com/2025/0d22fb7a-13f6-7e8f-ae5c-41a646366de3.htm",
 42447|       "dll_signature_verified": true,
 42448|       "dll_relationship_scope": "declared",
 42449|       "dll_semantic_verified": null,
 42450|       "dll_verified_status": "signature_verified_declared",
 42451|       "revitlookup_referenced": null,
 42452|       "revitlookup_requires_document_context": null
 42453|     },
 42454|     {
 42455|       "source": "Autodesk.Revit.DB.ExternalResourceUtils",
 42456|       "target": null,
 42457|       "member_name": "GetAllExternalResourceReferences",
 42458|       "member_kind": "method",
 42459|       "edge_type": "RETURNS_ELEMENT_IDS",
 42460|       "confidence": "elementid_collection_with_strong_name",
 42461|       "confidence_tier": "core",
 42462|       "target_resolution": "none",
 42463|       "evidence": [
 42464|         "member name 'GetAllExternalResourceReferences' matches keyword pattern /^GetAll/"
 42465|       ],
 42466|       "source_url": "https://www.revitapidocs.com/2025/0bfac259-4cc2-6a40-1a5f-7dd26f6ec3a5.htm",
 42467|       "dll_signature_verified": true,
 42468|       "dll_relationship_scope": "declared",
 42469|       "dll_semantic_verified": null,
 42470|       "dll_verified_status": "signature_verified_declared",
 42471|       "revitlookup_referenced": null,
 42472|       "revitlookup_requires_document_context": null
 42473|     },
 42474|     {
 42475|       "source": "Autodesk.Revit.DB.ExternalResourceUtils",
 42476|       "target": null,
 42477|       "member_name": "GetAllExternalResourceReferences",
 42478|       "member_kind": "method",
 42479|       "edge_type": "RETURNS_ELEMENT_IDS",
 42480|       "confidence": "elementid_collection_with_strong_name",
 42481|       "confidence_tier": "core",
 42482|       "target_resolution": "none",
 42483|       "evidence": [
 42484|         "member name 'GetAllExternalResourceReferences' matches keyword pattern /^GetAll/"
 42485|       ],
 42486|       "source_url": "https://www.revitapidocs.com/2025/fe34e7b3-4147-6dc2-8d4f-c2368f42f210.htm",
 42487|       "dll_signature_verified": true,
 42488|       "dll_relationship_scope": "declared",
 42489|       "dll_semantic_verified": null,
 42490|       "dll_verified_status": "signature_verified_declared",
 42491|       "revitlookup_referenced": null,
 42492|       "revitlookup_requires_document_context": null
 42493|     },
 42494|     {
 42495|       "source": "Autodesk.Revit.DB.Extrusion",
 42496|       "target": "Autodesk.Revit.DB.Sketch",
 42497|       "member_name": "Sketch",
 42498|       "member_kind": "property",
 42499|       "edge_type": "DEPENDS_ON",
 42500|       "confidence": "direct_return_type",
 42501|       "confidence_tier": "core",
 42502|       "target_resolution": "exact",
 42503|       "evidence": [
 42504|         "return type 'Sketch' directly names a Revit DB object type"
 42505|       ],
 42506|       "source_url": "https://www.revitapidocs.com/2025/53ea6889-85a4-9c99-caf5-3c3e6d507f0e.htm",
 42507|       "dll_signature_verified": true,
 42508|       "dll_relationship_scope": "declared",
 42509|       "dll_semantic_verified": null,
 42510|       "dll_verified_status": "signature_verified_declared",
 42511|       "revitlookup_referenced": null,
 42512|       "revitlookup_requires_document_context": null
 42513|     },
 42514|     {
 42515|       "source": "Autodesk.Revit.DB.ExtrusionAnalyzer",
 42516|       "target": null,
 42517|       "member_name": "EndParameter",
 42518|       "member_kind": "property",
 42519|       "edge_type": "HAS_PARAMETER",
 42520|       "confidence": "name_only_candidate",
```
