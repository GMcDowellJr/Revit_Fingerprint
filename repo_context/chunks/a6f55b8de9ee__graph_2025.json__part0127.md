# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 127 of 216
- Original line range: 49141-49540
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 49141|       "revitlookup_requires_document_context": null
 49142|     },
 49143|     {
 49144|       "source": "Autodesk.Revit.DB.InsulationLiningBase",
 49145|       "target": null,
 49146|       "member_name": "GetInsulationIds",
 49147|       "member_kind": "method",
 49148|       "edge_type": "RETURNS_ELEMENT_IDS",
 49149|       "confidence": "unknown_reference",
 49150|       "confidence_tier": "unverified_reference",
 49151|       "target_resolution": "none",
 49152|       "evidence": [
 49153|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 49154|       ],
 49155|       "source_url": "https://www.revitapidocs.com/2025/3d2a12de-0c85-da2d-006c-e5b3714ebdf4.htm",
 49156|       "dll_signature_verified": true,
 49157|       "dll_relationship_scope": "declared",
 49158|       "dll_semantic_verified": null,
 49159|       "dll_verified_status": "signature_verified_declared",
 49160|       "revitlookup_referenced": null,
 49161|       "revitlookup_requires_document_context": null
 49162|     },
 49163|     {
 49164|       "source": "Autodesk.Revit.DB.InsulationLiningBase",
 49165|       "target": null,
 49166|       "member_name": "GetLiningIds",
 49167|       "member_kind": "method",
 49168|       "edge_type": "RETURNS_ELEMENT_IDS",
 49169|       "confidence": "unknown_reference",
 49170|       "confidence_tier": "unverified_reference",
 49171|       "target_resolution": "none",
 49172|       "evidence": [
 49173|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 49174|       ],
 49175|       "source_url": "https://www.revitapidocs.com/2025/b13b6336-657f-1c89-47d6-e9c6dc3f7998.htm",
 49176|       "dll_signature_verified": true,
 49177|       "dll_relationship_scope": "declared",
 49178|       "dll_semantic_verified": null,
 49179|       "dll_verified_status": "signature_verified_declared",
 49180|       "revitlookup_referenced": null,
 49181|       "revitlookup_requires_document_context": null
 49182|     },
 49183|     {
 49184|       "source": "Autodesk.Revit.DB.InternalDefinition",
 49185|       "target": null,
 49186|       "member_name": "BuiltInParameter",
 49187|       "member_kind": "property",
 49188|       "edge_type": "HAS_PARAMETER",
 49189|       "confidence": "name_only_candidate",
 49190|       "confidence_tier": "likely",
 49191|       "target_resolution": "none",
 49192|       "evidence": [
 49193|         "member name 'BuiltInParameter' matches keyword pattern /Parameter/ but return type 'BuiltInParameter' gives no type-level confirmation"
 49194|       ],
 49195|       "source_url": "https://www.revitapidocs.com/2025/31c4b24f-c65a-8541-3fa8-c513563321cf.htm",
 49196|       "dll_signature_verified": true,
 49197|       "dll_relationship_scope": "declared",
 49198|       "dll_semantic_verified": null,
 49199|       "dll_verified_status": "signature_verified_declared",
 49200|       "revitlookup_referenced": null,
 49201|       "revitlookup_requires_document_context": null
 49202|     },
 49203|     {
 49204|       "source": "Autodesk.Revit.DB.InternalDefinition",
 49205|       "target": null,
 49206|       "member_name": "Id",
 49207|       "member_kind": "property",
 49208|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 49209|       "confidence": "unknown_reference",
 49210|       "confidence_tier": "unverified_reference",
 49211|       "target_resolution": "none",
 49212|       "evidence": [
 49213|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 49214|       ],
 49215|       "source_url": "https://www.revitapidocs.com/2025/6b71158a-443a-7220-8934-5e86271984ee.htm",
 49216|       "dll_signature_verified": true,
 49217|       "dll_relationship_scope": "declared",
 49218|       "dll_semantic_verified": null,
 49219|       "dll_verified_status": "signature_verified_declared",
 49220|       "revitlookup_referenced": null,
 49221|       "revitlookup_requires_document_context": null
 49222|     },
 49223|     {
 49224|       "source": "Autodesk.Revit.DB.InternalDefinition",
 49225|       "target": null,
 49226|       "member_name": "VariesAcrossGroups",
 49227|       "member_kind": "property",
 49228|       "edge_type": "MEMBER_OF_GROUP",
 49229|       "confidence": "name_only_candidate",
 49230|       "confidence_tier": "likely",
 49231|       "target_resolution": "none",
 49232|       "evidence": [
 49233|         "member name 'VariesAcrossGroups' matches keyword pattern /^GetMember|Group/ but return type 'bool' gives no type-level confirmation"
 49234|       ],
 49235|       "source_url": "https://www.revitapidocs.com/2025/089c01dd-030f-2621-cf30-0e60cb7c5868.htm",
 49236|       "dll_signature_verified": true,
 49237|       "dll_relationship_scope": "declared",
 49238|       "dll_semantic_verified": null,
 49239|       "dll_verified_status": "signature_verified_declared",
 49240|       "revitlookup_referenced": null,
 49241|       "revitlookup_requires_document_context": null
 49242|     },
 49243|     {
 49244|       "source": "Autodesk.Revit.DB.InternalDefinition",
 49245|       "target": null,
 49246|       "member_name": "SetAllowVaryBetweenGroups",
 49247|       "member_kind": "method",
 49248|       "edge_type": "MEMBER_OF_GROUP",
 49249|       "confidence": "elementid_collection_with_strong_name",
 49250|       "confidence_tier": "core",
 49251|       "target_resolution": "none",
 49252|       "evidence": [
 49253|         "member name 'SetAllowVaryBetweenGroups' matches keyword pattern /^GetMember|Group/"
 49254|       ],
 49255|       "source_url": "https://www.revitapidocs.com/2025/6f5af0cc-2ab3-153a-e07d-78fbc12aefc1.htm",
 49256|       "dll_signature_verified": true,
 49257|       "dll_relationship_scope": "declared",
 49258|       "dll_semantic_verified": null,
 49259|       "dll_verified_status": "signature_verified_declared",
 49260|       "revitlookup_referenced": null,
 49261|       "revitlookup_requires_document_context": null
 49262|     },
 49263|     {
 49264|       "source": "Autodesk.Revit.DB.InternalDefinition",
 49265|       "target": null,
 49266|       "member_name": "SetGroupTypeId",
 49267|       "member_kind": "method",
 49268|       "edge_type": "MEMBER_OF_GROUP",
 49269|       "confidence": "name_only_candidate",
 49270|       "confidence_tier": "likely",
 49271|       "target_resolution": "none",
 49272|       "evidence": [
 49273|         "member name 'SetGroupTypeId' matches keyword pattern /^GetMember|Group/ but return type 'void' gives no type-level confirmation"
 49274|       ],
 49275|       "source_url": "https://www.revitapidocs.com/2025/62a8a155-a7a6-e019-8cd8-9a7c9b4cd80a.htm",
 49276|       "dll_signature_verified": true,
 49277|       "dll_relationship_scope": "declared",
 49278|       "dll_semantic_verified": null,
 49279|       "dll_verified_status": "signature_verified_declared",
 49280|       "revitlookup_referenced": null,
 49281|       "revitlookup_requires_document_context": null
 49282|     },
 49283|     {
 49284|       "source": "Autodesk.Revit.DB.IntersectingElementData",
 49285|       "target": null,
 49286|       "member_name": "IntersectedElementId",
 49287|       "member_kind": "property",
 49288|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 49289|       "confidence": "unknown_reference",
 49290|       "confidence_tier": "unverified_reference",
 49291|       "target_resolution": "none",
 49292|       "evidence": [
 49293|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 49294|       ],
 49295|       "source_url": "https://www.revitapidocs.com/2025/f7a3a07f-8ec1-f2a2-ecf7-48310b90bb99.htm",
 49296|       "dll_signature_verified": true,
 49297|       "dll_relationship_scope": "declared",
 49298|       "dll_semantic_verified": null,
 49299|       "dll_verified_status": "signature_verified_declared",
 49300|       "revitlookup_referenced": null,
 49301|       "revitlookup_requires_document_context": null
 49302|     },
 49303|     {
 49304|       "source": "Autodesk.Revit.DB.IntersectingElementData",
 49305|       "target": null,
 49306|       "member_name": "IntersectingElementId",
 49307|       "member_kind": "property",
 49308|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 49309|       "confidence": "unknown_reference",
 49310|       "confidence_tier": "unverified_reference",
 49311|       "target_resolution": "none",
 49312|       "evidence": [
 49313|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 49314|       ],
 49315|       "source_url": "https://www.revitapidocs.com/2025/659fae1c-a161-d1f3-cd78-58cc304f8ec8.htm",
 49316|       "dll_signature_verified": true,
 49317|       "dll_relationship_scope": "declared",
 49318|       "dll_semantic_verified": null,
 49319|       "dll_verified_status": "signature_verified_declared",
 49320|       "revitlookup_referenced": null,
 49321|       "revitlookup_requires_document_context": null
 49322|     },
 49323|     {
 49324|       "source": "Autodesk.Revit.DB.IntersectionResult",
 49325|       "target": "Autodesk.Revit.DB.Edge",
 49326|       "member_name": "EdgeObject",
 49327|       "member_kind": "property",
 49328|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49329|       "confidence": "direct_return_type",
 49330|       "confidence_tier": "unverified_reference",
 49331|       "target_resolution": "exact",
 49332|       "evidence": [
 49333|         "return type 'Edge' directly names a Revit DB object type"
 49334|       ],
 49335|       "source_url": "https://www.revitapidocs.com/2025/4e4cc05e-b264-7a9e-5d49-8af876165d9b.htm",
 49336|       "dll_signature_verified": true,
 49337|       "dll_relationship_scope": "declared",
 49338|       "dll_semantic_verified": null,
 49339|       "dll_verified_status": "signature_verified_declared",
 49340|       "revitlookup_referenced": null,
 49341|       "revitlookup_requires_document_context": null
 49342|     },
 49343|     {
 49344|       "source": "Autodesk.Revit.DB.IntersectionResult",
 49345|       "target": null,
 49346|       "member_name": "EdgeParameter",
 49347|       "member_kind": "property",
 49348|       "edge_type": "HAS_PARAMETER",
 49349|       "confidence": "name_only_candidate",
 49350|       "confidence_tier": "likely",
 49351|       "target_resolution": "none",
 49352|       "evidence": [
 49353|         "member name 'EdgeParameter' matches keyword pattern /Parameter/ but return type 'double' gives no type-level confirmation"
 49354|       ],
 49355|       "source_url": "https://www.revitapidocs.com/2025/0388b3c1-6007-176a-40bf-fb0fd946a70e.htm",
 49356|       "dll_signature_verified": true,
 49357|       "dll_relationship_scope": "declared",
 49358|       "dll_semantic_verified": null,
 49359|       "dll_verified_status": "signature_verified_declared",
 49360|       "revitlookup_referenced": null,
 49361|       "revitlookup_requires_document_context": null
 49362|     },
 49363|     {
 49364|       "source": "Autodesk.Revit.DB.IntersectionResult",
 49365|       "target": null,
 49366|       "member_name": "Parameter",
 49367|       "member_kind": "property",
 49368|       "edge_type": "HAS_PARAMETER",
 49369|       "confidence": "name_only_candidate",
 49370|       "confidence_tier": "likely",
 49371|       "target_resolution": "none",
 49372|       "evidence": [
 49373|         "member name 'Parameter' matches keyword pattern /Parameter/ but return type 'double' gives no type-level confirmation"
 49374|       ],
 49375|       "source_url": "https://www.revitapidocs.com/2025/5ca02b0e-289a-f1ef-7ce2-8b3f175fe402.htm",
 49376|       "dll_signature_verified": true,
 49377|       "dll_relationship_scope": "declared",
 49378|       "dll_semantic_verified": null,
 49379|       "dll_verified_status": "signature_verified_declared",
 49380|       "revitlookup_referenced": null,
 49381|       "revitlookup_requires_document_context": null
 49382|     },
 49383|     {
 49384|       "source": "Autodesk.Revit.DB.IntersectionResultArray",
 49385|       "target": "Autodesk.Revit.DB.IntersectionResultArrayIterator",
 49386|       "member_name": "ForwardIterator",
 49387|       "member_kind": "method",
 49388|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49389|       "confidence": "direct_return_type",
 49390|       "confidence_tier": "unverified_reference",
 49391|       "target_resolution": "exact",
 49392|       "evidence": [
 49393|         "return type 'IntersectionResultArrayIterator' directly names a Revit DB object type"
 49394|       ],
 49395|       "source_url": "https://www.revitapidocs.com/2025/17d7c835-69f1-8fa2-9eed-1b521cbd0ebc.htm",
 49396|       "dll_signature_verified": true,
 49397|       "dll_relationship_scope": "declared",
 49398|       "dll_semantic_verified": null,
 49399|       "dll_verified_status": "signature_verified_declared",
 49400|       "revitlookup_referenced": null,
 49401|       "revitlookup_requires_document_context": null
 49402|     },
 49403|     {
 49404|       "source": "Autodesk.Revit.DB.IntersectionResultArray",
 49405|       "target": "Autodesk.Revit.DB.IntersectionResultArrayIterator",
 49406|       "member_name": "ReverseIterator",
 49407|       "member_kind": "method",
 49408|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49409|       "confidence": "direct_return_type",
 49410|       "confidence_tier": "unverified_reference",
 49411|       "target_resolution": "exact",
 49412|       "evidence": [
 49413|         "return type 'IntersectionResultArrayIterator' directly names a Revit DB object type"
 49414|       ],
 49415|       "source_url": "https://www.revitapidocs.com/2025/9df89750-b335-dc6e-9c4d-49c51423fbdf.htm",
 49416|       "dll_signature_verified": true,
 49417|       "dll_relationship_scope": "declared",
 49418|       "dll_semantic_verified": null,
 49419|       "dll_verified_status": "signature_verified_declared",
 49420|       "revitlookup_referenced": null,
 49421|       "revitlookup_requires_document_context": null
 49422|     },
 49423|     {
 49424|       "source": "Autodesk.Revit.DB.IPerformanceAdviserRule",
 49425|       "target": "Autodesk.Revit.DB.ElementFilter",
 49426|       "member_name": "GetElementFilter",
 49427|       "member_kind": "method",
 49428|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49429|       "confidence": "direct_return_type",
 49430|       "confidence_tier": "unverified_reference",
 49431|       "target_resolution": "exact",
 49432|       "evidence": [
 49433|         "return type 'ElementFilter' directly names a Revit DB object type"
 49434|       ],
 49435|       "source_url": "https://www.revitapidocs.com/2025/748d52b4-e49e-0820-a8dc-6d3b48bf37fc.htm",
 49436|       "dll_signature_verified": true,
 49437|       "dll_relationship_scope": "declared",
 49438|       "dll_semantic_verified": null,
 49439|       "dll_verified_status": "signature_verified_declared",
 49440|       "revitlookup_referenced": null,
 49441|       "revitlookup_requires_document_context": null
 49442|     },
 49443|     {
 49444|       "source": "Autodesk.Revit.DB.IPrintSetting",
 49445|       "target": "Autodesk.Revit.DB.PrintParameters",
 49446|       "member_name": "PrintParameters",
 49447|       "member_kind": "property",
 49448|       "edge_type": "HAS_PARAMETER",
 49449|       "confidence": "direct_return_type",
 49450|       "confidence_tier": "core",
 49451|       "target_resolution": "exact",
 49452|       "evidence": [
 49453|         "return type 'PrintParameters' directly names a Revit DB object type"
 49454|       ],
 49455|       "source_url": "https://www.revitapidocs.com/2025/66605827-b48a-ccc7-b2ad-8397b8810ac6.htm",
 49456|       "dll_signature_verified": true,
 49457|       "dll_relationship_scope": "declared",
 49458|       "dll_semantic_verified": null,
 49459|       "dll_verified_status": "signature_verified_declared",
 49460|       "revitlookup_referenced": null,
 49461|       "revitlookup_requires_document_context": null
 49462|     },
 49463|     {
 49464|       "source": "Autodesk.Revit.DB.IUpdater",
 49465|       "target": "Autodesk.Revit.DB.UpdaterId",
 49466|       "member_name": "GetUpdaterId",
 49467|       "member_kind": "method",
 49468|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49469|       "confidence": "direct_return_type",
 49470|       "confidence_tier": "unverified_reference",
 49471|       "target_resolution": "exact",
 49472|       "evidence": [
 49473|         "return type 'UpdaterId' directly names a Revit DB object type"
 49474|       ],
 49475|       "source_url": "https://www.revitapidocs.com/2025/d00e60eb-6123-3ce3-c158-4a2b4ff0b387.htm",
 49476|       "dll_signature_verified": true,
 49477|       "dll_relationship_scope": "declared",
 49478|       "dll_semantic_verified": null,
 49479|       "dll_verified_status": "signature_verified_declared",
 49480|       "revitlookup_referenced": null,
 49481|       "revitlookup_requires_document_context": null
 49482|     },
 49483|     {
 49484|       "source": "Autodesk.Revit.DB.IViewSheetSet",
 49485|       "target": "Autodesk.Revit.DB.ViewSheet",
 49486|       "member_name": "SheetOrganizationId",
 49487|       "member_kind": "property",
 49488|       "edge_type": "PLACED_ON_SHEET",
 49489|       "confidence": "elementid_with_strong_name",
 49490|       "confidence_tier": "core",
 49491|       "target_resolution": "exact",
 49492|       "evidence": [
 49493|         "member name 'SheetOrganizationId' matches keyword pattern /Sheet/"
 49494|       ],
 49495|       "source_url": "https://www.revitapidocs.com/2025/69873730-6de8-19d4-eef7-ae05d8990856.htm",
 49496|       "dll_signature_verified": true,
 49497|       "dll_relationship_scope": "declared",
 49498|       "dll_semantic_verified": null,
 49499|       "dll_verified_status": "signature_verified_declared",
 49500|       "revitlookup_referenced": null,
 49501|       "revitlookup_requires_document_context": null
 49502|     },
 49503|     {
 49504|       "source": "Autodesk.Revit.DB.IViewSheetSet",
 49505|       "target": null,
 49506|       "member_name": "ViewOrganizationId",
 49507|       "member_kind": "property",
 49508|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 49509|       "confidence": "unknown_reference",
 49510|       "confidence_tier": "unverified_reference",
 49511|       "target_resolution": "none",
 49512|       "evidence": [
 49513|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 49514|       ],
 49515|       "source_url": "https://www.revitapidocs.com/2025/cab190d0-c36b-4bdb-1259-93241c94fd58.htm",
 49516|       "dll_signature_verified": true,
 49517|       "dll_relationship_scope": "declared",
 49518|       "dll_semantic_verified": null,
 49519|       "dll_verified_status": "signature_verified_declared",
 49520|       "revitlookup_referenced": null,
 49521|       "revitlookup_requires_document_context": null
 49522|     },
 49523|     {
 49524|       "source": "Autodesk.Revit.DB.IViewSheetSet",
 49525|       "target": "Autodesk.Revit.DB.ViewSet",
 49526|       "member_name": "Views",
 49527|       "member_kind": "property",
 49528|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49529|       "confidence": "direct_return_type",
 49530|       "confidence_tier": "unverified_reference",
 49531|       "target_resolution": "exact",
 49532|       "evidence": [
 49533|         "return type 'ViewSet' directly names a Revit DB object type"
 49534|       ],
 49535|       "source_url": "https://www.revitapidocs.com/2025/48d5707a-ef8b-3609-a573-c393026bc812.htm",
 49536|       "dll_signature_verified": true,
 49537|       "dll_relationship_scope": "declared",
 49538|       "dll_semantic_verified": null,
 49539|       "dll_verified_status": "signature_verified_declared",
 49540|       "revitlookup_referenced": null,
```
