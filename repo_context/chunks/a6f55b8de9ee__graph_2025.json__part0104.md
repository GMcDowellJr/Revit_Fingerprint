# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 104 of 216
- Original line range: 40171-40570
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 40171|       "revitlookup_referenced": null,
 40172|       "revitlookup_requires_document_context": null
 40173|     },
 40174|     {
 40175|       "source": "Autodesk.Revit.DB.Element",
 40176|       "target": "Autodesk.Revit.DB.ExternalFileReference",
 40177|       "member_name": "GetExternalFileReference",
 40178|       "member_kind": "method",
 40179|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 40180|       "confidence": "direct_return_type",
 40181|       "confidence_tier": "unverified_reference",
 40182|       "target_resolution": "exact",
 40183|       "evidence": [
 40184|         "return type 'ExternalFileReference' directly names a Revit DB object type"
 40185|       ],
 40186|       "source_url": "https://www.revitapidocs.com/2025/e784fb6e-94f4-09bd-1f9c-17e6968e18a5.htm",
 40187|       "dll_signature_verified": true,
 40188|       "dll_relationship_scope": "declared",
 40189|       "dll_semantic_verified": null,
 40190|       "dll_verified_status": "signature_verified_declared",
 40191|       "revitlookup_referenced": null,
 40192|       "revitlookup_requires_document_context": null
 40193|     },
 40194|     {
 40195|       "source": "Autodesk.Revit.DB.Element",
 40196|       "target": "Autodesk.Revit.DB.ExternalResourceReference",
 40197|       "member_name": "GetExternalResourceReference",
 40198|       "member_kind": "method",
 40199|       "edge_type": "REFERENCES",
 40200|       "confidence": "direct_return_type",
 40201|       "confidence_tier": "core",
 40202|       "target_resolution": "exact",
 40203|       "evidence": [
 40204|         "return type 'ExternalResourceReference' directly names a Revit DB object type"
 40205|       ],
 40206|       "source_url": "https://www.revitapidocs.com/2025/fb4b9493-1d7b-5387-c171-2078225183ca.htm",
 40207|       "dll_signature_verified": true,
 40208|       "dll_relationship_scope": "declared",
 40209|       "dll_semantic_verified": null,
 40210|       "dll_verified_status": "signature_verified_declared",
 40211|       "revitlookup_referenced": null,
 40212|       "revitlookup_requires_document_context": null
 40213|     },
 40214|     {
 40215|       "source": "Autodesk.Revit.DB.Element",
 40216|       "target": "Autodesk.Revit.DB.ExternalResourceReference",
 40217|       "member_name": "GetExternalResourceReferenceExpanded",
 40218|       "member_kind": "method",
 40219|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 40220|       "confidence": "needs_runtime_validation",
 40221|       "confidence_tier": "needs_validation",
 40222|       "target_resolution": "exact",
 40223|       "evidence": [
 40224|         "return type 'IList < ExternalResourceReference >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 40225|       ],
 40226|       "source_url": "https://www.revitapidocs.com/2025/1a28171e-8460-d849-4e7d-9a306a22cd6e.htm",
 40227|       "dll_signature_verified": true,
 40228|       "dll_relationship_scope": "declared",
 40229|       "dll_semantic_verified": null,
 40230|       "dll_verified_status": "signature_verified_declared",
 40231|       "revitlookup_referenced": null,
 40232|       "revitlookup_requires_document_context": null
 40233|     },
 40234|     {
 40235|       "source": "Autodesk.Revit.DB.Element",
 40236|       "target": null,
 40237|       "member_name": "GetGeneratingElementIds",
 40238|       "member_kind": "method",
 40239|       "edge_type": "DEPENDS_ON",
 40240|       "confidence": "elementid_collection_with_strong_name",
 40241|       "confidence_tier": "core",
 40242|       "target_resolution": "none",
 40243|       "evidence": [
 40244|         "member name 'GetGeneratingElementIds' matches keyword pattern /^GetGenerating/"
 40245|       ],
 40246|       "source_url": "https://www.revitapidocs.com/2025/112590d2-de20-dd1f-ae05-df7dfb3b410f.htm",
 40247|       "dll_signature_verified": true,
 40248|       "dll_relationship_scope": "declared",
 40249|       "dll_semantic_verified": null,
 40250|       "dll_verified_status": "signature_verified_declared",
 40251|       "revitlookup_referenced": null,
 40252|       "revitlookup_requires_document_context": null
 40253|     },
 40254|     {
 40255|       "source": "Autodesk.Revit.DB.Element",
 40256|       "target": "Autodesk.Revit.DB.Material",
 40257|       "member_name": "GetMaterialArea",
 40258|       "member_kind": "method",
 40259|       "edge_type": "USES_MATERIAL",
 40260|       "confidence": "name_only_candidate",
 40261|       "confidence_tier": "likely",
 40262|       "target_resolution": "exact",
 40263|       "evidence": [
 40264|         "member name 'GetMaterialArea' matches keyword pattern /Material/ but return type 'double' gives no type-level confirmation"
 40265|       ],
 40266|       "source_url": "https://www.revitapidocs.com/2025/02417c40-bcc4-f04c-9897-cf47737e8739.htm",
 40267|       "dll_signature_verified": true,
 40268|       "dll_relationship_scope": "declared",
 40269|       "dll_semantic_verified": null,
 40270|       "dll_verified_status": "signature_verified_declared",
 40271|       "revitlookup_referenced": true,
 40272|       "revitlookup_requires_document_context": false
 40273|     },
 40274|     {
 40275|       "source": "Autodesk.Revit.DB.Element",
 40276|       "target": "Autodesk.Revit.DB.Material",
 40277|       "member_name": "GetMaterialIds",
 40278|       "member_kind": "method",
 40279|       "edge_type": "USES_MATERIAL",
 40280|       "confidence": "elementid_collection_with_strong_name",
 40281|       "confidence_tier": "core",
 40282|       "target_resolution": "exact",
 40283|       "evidence": [
 40284|         "member name 'GetMaterialIds' matches keyword pattern /Material/"
 40285|       ],
 40286|       "source_url": "https://www.revitapidocs.com/2025/6011352e-151b-b8ac-14cc-45970f2fe5ad.htm",
 40287|       "dll_signature_verified": true,
 40288|       "dll_relationship_scope": "declared",
 40289|       "dll_semantic_verified": null,
 40290|       "dll_verified_status": "signature_verified_declared",
 40291|       "revitlookup_referenced": true,
 40292|       "revitlookup_requires_document_context": false
 40293|     },
 40294|     {
 40295|       "source": "Autodesk.Revit.DB.Element",
 40296|       "target": "Autodesk.Revit.DB.Material",
 40297|       "member_name": "GetMaterialVolume",
 40298|       "member_kind": "method",
 40299|       "edge_type": "USES_MATERIAL",
 40300|       "confidence": "name_only_candidate",
 40301|       "confidence_tier": "likely",
 40302|       "target_resolution": "exact",
 40303|       "evidence": [
 40304|         "member name 'GetMaterialVolume' matches keyword pattern /Material/ but return type 'double' gives no type-level confirmation"
 40305|       ],
 40306|       "source_url": "https://www.revitapidocs.com/2025/99b50d87-bfa6-ca67-e205-47b22cad6587.htm",
 40307|       "dll_signature_verified": true,
 40308|       "dll_relationship_scope": "declared",
 40309|       "dll_semantic_verified": null,
 40310|       "dll_verified_status": "signature_verified_declared",
 40311|       "revitlookup_referenced": true,
 40312|       "revitlookup_requires_document_context": false
 40313|     },
 40314|     {
 40315|       "source": "Autodesk.Revit.DB.Element",
 40316|       "target": null,
 40317|       "member_name": "GetMonitoredLinkElementIds",
 40318|       "member_kind": "method",
 40319|       "edge_type": "RETURNS_ELEMENT_IDS",
 40320|       "confidence": "unknown_reference",
 40321|       "confidence_tier": "unverified_reference",
 40322|       "target_resolution": "none",
 40323|       "evidence": [
 40324|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 40325|       ],
 40326|       "source_url": "https://www.revitapidocs.com/2025/42b25291-f1b9-d240-c876-1b53f24f60e0.htm",
 40327|       "dll_signature_verified": true,
 40328|       "dll_relationship_scope": "declared",
 40329|       "dll_semantic_verified": null,
 40330|       "dll_verified_status": "signature_verified_declared",
 40331|       "revitlookup_referenced": null,
 40332|       "revitlookup_requires_document_context": null
 40333|     },
 40334|     {
 40335|       "source": "Autodesk.Revit.DB.Element",
 40336|       "target": null,
 40337|       "member_name": "GetMonitoredLocalElementIds",
 40338|       "member_kind": "method",
 40339|       "edge_type": "RETURNS_ELEMENT_IDS",
 40340|       "confidence": "unknown_reference",
 40341|       "confidence_tier": "unverified_reference",
 40342|       "target_resolution": "none",
 40343|       "evidence": [
 40344|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 40345|       ],
 40346|       "source_url": "https://www.revitapidocs.com/2025/47ca1e8c-f79d-a18b-505b-73a4358d2264.htm",
 40347|       "dll_signature_verified": true,
 40348|       "dll_relationship_scope": "declared",
 40349|       "dll_semantic_verified": null,
 40350|       "dll_verified_status": "signature_verified_declared",
 40351|       "revitlookup_referenced": null,
 40352|       "revitlookup_requires_document_context": null
 40353|     },
 40354|     {
 40355|       "source": "Autodesk.Revit.DB.Element",
 40356|       "target": "Autodesk.Revit.DB.Parameter",
 40357|       "member_name": "GetOrderedParameters",
 40358|       "member_kind": "method",
 40359|       "edge_type": "HAS_PARAMETER",
 40360|       "confidence": "needs_runtime_validation",
 40361|       "confidence_tier": "needs_validation",
 40362|       "target_resolution": "exact",
 40363|       "evidence": [
 40364|         "return type 'IList < Parameter >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 40365|       ],
 40366|       "source_url": "https://www.revitapidocs.com/2025/4bf4c0da-f841-0943-f9e0-246a666c1775.htm",
 40367|       "dll_signature_verified": true,
 40368|       "dll_relationship_scope": "declared",
 40369|       "dll_semantic_verified": null,
 40370|       "dll_verified_status": "signature_verified_declared",
 40371|       "revitlookup_referenced": null,
 40372|       "revitlookup_requires_document_context": null
 40373|     },
 40374|     {
 40375|       "source": "Autodesk.Revit.DB.Element",
 40376|       "target": "Autodesk.Revit.DB.Parameter",
 40377|       "member_name": "GetParameter",
 40378|       "member_kind": "method",
 40379|       "edge_type": "HAS_PARAMETER",
 40380|       "confidence": "direct_return_type",
 40381|       "confidence_tier": "core",
 40382|       "target_resolution": "exact",
 40383|       "evidence": [
 40384|         "return type 'Parameter' directly names a Revit DB object type"
 40385|       ],
 40386|       "source_url": "https://www.revitapidocs.com/2025/fc4e5245-d2e5-e31d-a6e3-177106e75e10.htm",
 40387|       "dll_signature_verified": true,
 40388|       "dll_relationship_scope": "declared",
 40389|       "dll_semantic_verified": null,
 40390|       "dll_verified_status": "signature_verified_declared",
 40391|       "revitlookup_referenced": null,
 40392|       "revitlookup_requires_document_context": null
 40393|     },
 40394|     {
 40395|       "source": "Autodesk.Revit.DB.Element",
 40396|       "target": "Autodesk.Revit.DB.Parameter",
 40397|       "member_name": "GetParameters",
 40398|       "member_kind": "method",
 40399|       "edge_type": "HAS_PARAMETER",
 40400|       "confidence": "needs_runtime_validation",
 40401|       "confidence_tier": "needs_validation",
 40402|       "target_resolution": "exact",
 40403|       "evidence": [
 40404|         "return type 'IList < Parameter >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 40405|       ],
 40406|       "source_url": "https://www.revitapidocs.com/2025/0cf342ef-c64f-b0b7-cbec-da8f3428a7dc.htm",
 40407|       "dll_signature_verified": true,
 40408|       "dll_relationship_scope": "declared",
 40409|       "dll_semantic_verified": null,
 40410|       "dll_verified_status": "signature_verified_declared",
 40411|       "revitlookup_referenced": null,
 40412|       "revitlookup_requires_document_context": null
 40413|     },
 40414|     {
 40415|       "source": "Autodesk.Revit.DB.Element",
 40416|       "target": "Autodesk.Revit.DB.Phase",
 40417|       "member_name": "GetPhaseStatus",
 40418|       "member_kind": "method",
 40419|       "edge_type": "ASSIGNED_TO_PHASE",
 40420|       "confidence": "name_only_candidate",
 40421|       "confidence_tier": "likely",
 40422|       "target_resolution": "exact",
 40423|       "evidence": [
 40424|         "member name 'GetPhaseStatus' matches keyword pattern /Phase/ but return type 'ElementOnPhaseStatus' gives no type-level confirmation"
 40425|       ],
 40426|       "source_url": "https://www.revitapidocs.com/2025/eedf5981-b5e2-dda7-cb5e-01a4d4fc7f6c.htm",
 40427|       "dll_signature_verified": true,
 40428|       "dll_relationship_scope": "declared",
 40429|       "dll_semantic_verified": null,
 40430|       "dll_verified_status": "signature_verified_declared",
 40431|       "revitlookup_referenced": true,
 40432|       "revitlookup_requires_document_context": true
 40433|     },
 40434|     {
 40435|       "source": "Autodesk.Revit.DB.Element",
 40436|       "target": "Autodesk.Revit.DB.Subelement",
 40437|       "member_name": "GetSubelements",
 40438|       "member_kind": "method",
 40439|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 40440|       "confidence": "needs_runtime_validation",
 40441|       "confidence_tier": "needs_validation",
 40442|       "target_resolution": "exact",
 40443|       "evidence": [
 40444|         "return type 'IList < Subelement >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 40445|       ],
 40446|       "source_url": "https://www.revitapidocs.com/2025/feabfd59-bd0f-ab61-34a1-d0d22f58c881.htm",
 40447|       "dll_signature_verified": true,
 40448|       "dll_relationship_scope": "declared",
 40449|       "dll_semantic_verified": null,
 40450|       "dll_verified_status": "signature_verified_declared",
 40451|       "revitlookup_referenced": null,
 40452|       "revitlookup_requires_document_context": null
 40453|     },
 40454|     {
 40455|       "source": "Autodesk.Revit.DB.Element",
 40456|       "target": null,
 40457|       "member_name": "GetTypeId",
 40458|       "member_kind": "method",
 40459|       "edge_type": "TYPE_OF",
 40460|       "confidence": "elementid_with_strong_name",
 40461|       "confidence_tier": "core",
 40462|       "target_resolution": "none",
 40463|       "evidence": [
 40464|         "member name 'GetTypeId' matches keyword pattern /^(Type|TypeId|GetTypeId)$/"
 40465|       ],
 40466|       "source_url": "https://www.revitapidocs.com/2025/cc66ca8e-302e-f072-edca-d847bcf14c86.htm",
 40467|       "dll_signature_verified": true,
 40468|       "dll_relationship_scope": "declared",
 40469|       "dll_semantic_verified": null,
 40470|       "dll_verified_status": "signature_verified_declared",
 40471|       "revitlookup_referenced": null,
 40472|       "revitlookup_requires_document_context": null
 40473|     },
 40474|     {
 40475|       "source": "Autodesk.Revit.DB.Element",
 40476|       "target": null,
 40477|       "member_name": "GetValidTypes",
 40478|       "member_kind": "method",
 40479|       "edge_type": "RETURNS_ELEMENT_IDS",
 40480|       "confidence": "unknown_reference",
 40481|       "confidence_tier": "unverified_reference",
 40482|       "target_resolution": "none",
 40483|       "evidence": [
 40484|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 40485|       ],
 40486|       "source_url": "https://www.revitapidocs.com/2025/086554ba-3c70-9c0f-8a09-55a4da4ef905.htm",
 40487|       "dll_signature_verified": true,
 40488|       "dll_relationship_scope": "declared",
 40489|       "dll_semantic_verified": null,
 40490|       "dll_verified_status": "signature_verified_declared",
 40491|       "revitlookup_referenced": null,
 40492|       "revitlookup_requires_document_context": null
 40493|     },
 40494|     {
 40495|       "source": "Autodesk.Revit.DB.Element",
 40496|       "target": null,
 40497|       "member_name": "GetValidTypes",
 40498|       "member_kind": "method",
 40499|       "edge_type": "RETURNS_ELEMENT_IDS",
 40500|       "confidence": "unknown_reference",
 40501|       "confidence_tier": "unverified_reference",
 40502|       "target_resolution": "none",
 40503|       "evidence": [
 40504|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 40505|       ],
 40506|       "source_url": "https://www.revitapidocs.com/2025/43dc2992-0b0d-ca73-d63c-2ac829bf1a89.htm",
 40507|       "dll_signature_verified": true,
 40508|       "dll_relationship_scope": "declared",
 40509|       "dll_semantic_verified": null,
 40510|       "dll_verified_status": "signature_verified_declared",
 40511|       "revitlookup_referenced": null,
 40512|       "revitlookup_requires_document_context": null
 40513|     },
 40514|     {
 40515|       "source": "Autodesk.Revit.DB.Element",
 40516|       "target": "Autodesk.Revit.DB.Phase",
 40517|       "member_name": "HasPhases",
 40518|       "member_kind": "method",
 40519|       "edge_type": "ASSIGNED_TO_PHASE",
 40520|       "confidence": "name_only_candidate",
 40521|       "confidence_tier": "likely",
 40522|       "target_resolution": "exact",
 40523|       "evidence": [
 40524|         "member name 'HasPhases' matches keyword pattern /Phase/ but return type 'bool' gives no type-level confirmation"
 40525|       ],
 40526|       "source_url": "https://www.revitapidocs.com/2025/5d850f8a-4a50-406b-6c59-b85d49dcbb2e.htm",
 40527|       "dll_signature_verified": true,
 40528|       "dll_relationship_scope": "declared",
 40529|       "dll_semantic_verified": null,
 40530|       "dll_verified_status": "signature_verified_declared",
 40531|       "revitlookup_referenced": null,
 40532|       "revitlookup_requires_document_context": null
 40533|     },
 40534|     {
 40535|       "source": "Autodesk.Revit.DB.Element",
 40536|       "target": "Autodesk.Revit.DB.Phase",
 40537|       "member_name": "IsCreatedPhaseOrderValid",
 40538|       "member_kind": "method",
 40539|       "edge_type": "ASSIGNED_TO_PHASE",
 40540|       "confidence": "name_only_candidate",
 40541|       "confidence_tier": "likely",
 40542|       "target_resolution": "exact",
 40543|       "evidence": [
 40544|         "member name 'IsCreatedPhaseOrderValid' matches keyword pattern /Phase/ but return type 'bool' gives no type-level confirmation"
 40545|       ],
 40546|       "source_url": "https://www.revitapidocs.com/2025/b2bcaf7f-c453-d6e2-fd85-083783e935f3.htm",
 40547|       "dll_signature_verified": true,
 40548|       "dll_relationship_scope": "declared",
 40549|       "dll_semantic_verified": null,
 40550|       "dll_verified_status": "signature_verified_declared",
 40551|       "revitlookup_referenced": true,
 40552|       "revitlookup_requires_document_context": true
 40553|     },
 40554|     {
 40555|       "source": "Autodesk.Revit.DB.Element",
 40556|       "target": "Autodesk.Revit.DB.Phase",
 40557|       "member_name": "IsDemolishedPhaseOrderValid",
 40558|       "member_kind": "method",
 40559|       "edge_type": "ASSIGNED_TO_PHASE",
 40560|       "confidence": "name_only_candidate",
 40561|       "confidence_tier": "likely",
 40562|       "target_resolution": "exact",
 40563|       "evidence": [
 40564|         "member name 'IsDemolishedPhaseOrderValid' matches keyword pattern /Phase/ but return type 'bool' gives no type-level confirmation"
 40565|       ],
 40566|       "source_url": "https://www.revitapidocs.com/2025/46ec60b6-b1c5-25aa-c544-34379298c7b8.htm",
 40567|       "dll_signature_verified": true,
 40568|       "dll_relationship_scope": "declared",
 40569|       "dll_semantic_verified": null,
 40570|       "dll_verified_status": "signature_verified_declared",
```
