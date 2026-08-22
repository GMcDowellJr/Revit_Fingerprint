# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 94 of 216
- Original line range: 36271-36670
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 36271|     },
 36272|     {
 36273|       "source": "Autodesk.Revit.DB.CurveByPoints",
 36274|       "target": "Autodesk.Revit.DB.ReferencePointArray",
 36275|       "member_name": "GetPoints",
 36276|       "member_kind": "method",
 36277|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36278|       "confidence": "direct_return_type",
 36279|       "confidence_tier": "unverified_reference",
 36280|       "target_resolution": "exact",
 36281|       "evidence": [
 36282|         "return type 'ReferencePointArray' directly names a Revit DB object type"
 36283|       ],
 36284|       "source_url": "https://www.revitapidocs.com/2025/b359ed6c-0944-0ff0-5c15-b39289cb563b.htm",
 36285|       "dll_signature_verified": true,
 36286|       "dll_relationship_scope": "declared",
 36287|       "dll_semantic_verified": null,
 36288|       "dll_verified_status": "signature_verified_declared",
 36289|       "revitlookup_referenced": null,
 36290|       "revitlookup_requires_document_context": null
 36291|     },
 36292|     {
 36293|       "source": "Autodesk.Revit.DB.CurveByPoints",
 36294|       "target": "Autodesk.Revit.DB.FamilyElementVisibility",
 36295|       "member_name": "GetVisibility",
 36296|       "member_kind": "method",
 36297|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36298|       "confidence": "direct_return_type",
 36299|       "confidence_tier": "unverified_reference",
 36300|       "target_resolution": "exact",
 36301|       "evidence": [
 36302|         "return type 'FamilyElementVisibility' directly names a Revit DB object type"
 36303|       ],
 36304|       "source_url": "https://www.revitapidocs.com/2025/f688bb4d-b4f6-0e61-ba0c-2216b200e05f.htm",
 36305|       "dll_signature_verified": true,
 36306|       "dll_relationship_scope": "declared",
 36307|       "dll_semantic_verified": null,
 36308|       "dll_verified_status": "signature_verified_declared",
 36309|       "revitlookup_referenced": null,
 36310|       "revitlookup_requires_document_context": null
 36311|     },
 36312|     {
 36313|       "source": "Autodesk.Revit.DB.CurveByPointsArray",
 36314|       "target": "Autodesk.Revit.DB.CurveByPointsArrayIterator",
 36315|       "member_name": "ForwardIterator",
 36316|       "member_kind": "method",
 36317|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36318|       "confidence": "direct_return_type",
 36319|       "confidence_tier": "unverified_reference",
 36320|       "target_resolution": "exact",
 36321|       "evidence": [
 36322|         "return type 'CurveByPointsArrayIterator' directly names a Revit DB object type"
 36323|       ],
 36324|       "source_url": "https://www.revitapidocs.com/2025/e5a802ca-df92-7d4b-a403-624e7816d1ad.htm",
 36325|       "dll_signature_verified": true,
 36326|       "dll_relationship_scope": "declared",
 36327|       "dll_semantic_verified": null,
 36328|       "dll_verified_status": "signature_verified_declared",
 36329|       "revitlookup_referenced": null,
 36330|       "revitlookup_requires_document_context": null
 36331|     },
 36332|     {
 36333|       "source": "Autodesk.Revit.DB.CurveByPointsArray",
 36334|       "target": "Autodesk.Revit.DB.CurveByPointsArrayIterator",
 36335|       "member_name": "ReverseIterator",
 36336|       "member_kind": "method",
 36337|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36338|       "confidence": "direct_return_type",
 36339|       "confidence_tier": "unverified_reference",
 36340|       "target_resolution": "exact",
 36341|       "evidence": [
 36342|         "return type 'CurveByPointsArrayIterator' directly names a Revit DB object type"
 36343|       ],
 36344|       "source_url": "https://www.revitapidocs.com/2025/24aaa9ad-5d68-daed-191e-f5757f03ca52.htm",
 36345|       "dll_signature_verified": true,
 36346|       "dll_relationship_scope": "declared",
 36347|       "dll_semantic_verified": null,
 36348|       "dll_verified_status": "signature_verified_declared",
 36349|       "revitlookup_referenced": null,
 36350|       "revitlookup_requires_document_context": null
 36351|     },
 36352|     {
 36353|       "source": "Autodesk.Revit.DB.CurveByPointsUtils",
 36354|       "target": "Autodesk.Revit.DB.Reference",
 36355|       "member_name": "GetFaceRegions",
 36356|       "member_kind": "method",
 36357|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36358|       "confidence": "needs_runtime_validation",
 36359|       "confidence_tier": "needs_validation",
 36360|       "target_resolution": "exact",
 36361|       "evidence": [
 36362|         "return type 'IList < Reference >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 36363|       ],
 36364|       "source_url": "https://www.revitapidocs.com/2025/4dd110d1-ee73-928b-2b97-3ddd51d0591c.htm",
 36365|       "dll_signature_verified": true,
 36366|       "dll_relationship_scope": "declared",
 36367|       "dll_semantic_verified": null,
 36368|       "dll_verified_status": "signature_verified_declared",
 36369|       "revitlookup_referenced": null,
 36370|       "revitlookup_requires_document_context": null
 36371|     },
 36372|     {
 36373|       "source": "Autodesk.Revit.DB.CurveByPointsUtils",
 36374|       "target": "Autodesk.Revit.DB.Reference",
 36375|       "member_name": "GetHostFace",
 36376|       "member_kind": "method",
 36377|       "edge_type": "HOSTED_BY",
 36378|       "confidence": "direct_return_type",
 36379|       "confidence_tier": "core",
 36380|       "target_resolution": "exact",
 36381|       "evidence": [
 36382|         "return type 'Reference' directly names a Revit DB object type"
 36383|       ],
 36384|       "source_url": "https://www.revitapidocs.com/2025/19436661-0781-47be-8880-43f0eb451baf.htm",
 36385|       "dll_signature_verified": true,
 36386|       "dll_relationship_scope": "declared",
 36387|       "dll_semantic_verified": null,
 36388|       "dll_verified_status": "signature_verified_declared",
 36389|       "revitlookup_referenced": null,
 36390|       "revitlookup_requires_document_context": null
 36391|     },
 36392|     {
 36393|       "source": "Autodesk.Revit.DB.CurveElement",
 36394|       "target": "Autodesk.Revit.DB.Reference",
 36395|       "member_name": "CenterPointReference",
 36396|       "member_kind": "property",
 36397|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36398|       "confidence": "direct_return_type",
 36399|       "confidence_tier": "unverified_reference",
 36400|       "target_resolution": "exact",
 36401|       "evidence": [
 36402|         "return type 'Reference' directly names a Revit DB object type"
 36403|       ],
 36404|       "source_url": "https://www.revitapidocs.com/2025/8e17164e-f2d0-828d-8fe2-9720fec91303.htm",
 36405|       "dll_signature_verified": true,
 36406|       "dll_relationship_scope": "declared",
 36407|       "dll_semantic_verified": null,
 36408|       "dll_verified_status": "signature_verified_declared",
 36409|       "revitlookup_referenced": null,
 36410|       "revitlookup_requires_document_context": null
 36411|     },
 36412|     {
 36413|       "source": "Autodesk.Revit.DB.CurveElement",
 36414|       "target": "Autodesk.Revit.DB.Element",
 36415|       "member_name": "LineStyle",
 36416|       "member_kind": "property",
 36417|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36418|       "confidence": "direct_return_type",
 36419|       "confidence_tier": "unverified_reference",
 36420|       "target_resolution": "exact",
 36421|       "evidence": [
 36422|         "return type 'Element' directly names a Revit DB object type"
 36423|       ],
 36424|       "source_url": "https://www.revitapidocs.com/2025/691e64a2-e5ea-b619-4362-1a2c17e23b2f.htm",
 36425|       "dll_signature_verified": true,
 36426|       "dll_relationship_scope": "declared",
 36427|       "dll_semantic_verified": null,
 36428|       "dll_verified_status": "signature_verified_declared",
 36429|       "revitlookup_referenced": null,
 36430|       "revitlookup_requires_document_context": null
 36431|     },
 36432|     {
 36433|       "source": "Autodesk.Revit.DB.CurveElement",
 36434|       "target": "Autodesk.Revit.DB.SketchPlane",
 36435|       "member_name": "SketchPlane",
 36436|       "member_kind": "property",
 36437|       "edge_type": "REFERENCES",
 36438|       "confidence": "direct_return_type",
 36439|       "confidence_tier": "core",
 36440|       "target_resolution": "exact",
 36441|       "evidence": [
 36442|         "return type 'SketchPlane' directly names a Revit DB object type"
 36443|       ],
 36444|       "source_url": "https://www.revitapidocs.com/2025/e8c6a9e9-e048-d750-2951-6f45ac7f350d.htm",
 36445|       "dll_signature_verified": true,
 36446|       "dll_relationship_scope": "declared",
 36447|       "dll_semantic_verified": null,
 36448|       "dll_verified_status": "signature_verified_declared",
 36449|       "revitlookup_referenced": null,
 36450|       "revitlookup_requires_document_context": null
 36451|     },
 36452|     {
 36453|       "source": "Autodesk.Revit.DB.CurveElement",
 36454|       "target": null,
 36455|       "member_name": "GetAdjoinedCurveElements",
 36456|       "member_kind": "method",
 36457|       "edge_type": "RETURNS_ELEMENT_IDS",
 36458|       "confidence": "unknown_reference",
 36459|       "confidence_tier": "unverified_reference",
 36460|       "target_resolution": "none",
 36461|       "evidence": [
 36462|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 36463|       ],
 36464|       "source_url": "https://www.revitapidocs.com/2025/5f3eaf8b-047e-d287-49a9-8777af3a67da.htm",
 36465|       "dll_signature_verified": true,
 36466|       "dll_relationship_scope": "declared",
 36467|       "dll_semantic_verified": null,
 36468|       "dll_verified_status": "signature_verified_declared",
 36469|       "revitlookup_referenced": true,
 36470|       "revitlookup_requires_document_context": false
 36471|     },
 36472|     {
 36473|       "source": "Autodesk.Revit.DB.CurveElement",
 36474|       "target": "Autodesk.Revit.DB.Electrical.AreaBasedLoadBoundaryLineData",
 36475|       "member_name": "GetAreaBasedLoadBoundaryLineData",
 36476|       "member_kind": "method",
 36477|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36478|       "confidence": "direct_return_type",
 36479|       "confidence_tier": "unverified_reference",
 36480|       "target_resolution": "short_name_fallback",
 36481|       "evidence": [
 36482|         "return type 'AreaBasedLoadBoundaryLineData' directly names a Revit DB object type"
 36483|       ],
 36484|       "source_url": "https://www.revitapidocs.com/2025/a99e772c-9f79-c81f-14ec-71703e19676e.htm",
 36485|       "dll_signature_verified": true,
 36486|       "dll_relationship_scope": "declared",
 36487|       "dll_semantic_verified": null,
 36488|       "dll_verified_status": "signature_verified_declared",
 36489|       "revitlookup_referenced": null,
 36490|       "revitlookup_requires_document_context": null
 36491|     },
 36492|     {
 36493|       "source": "Autodesk.Revit.DB.CurveElement",
 36494|       "target": null,
 36495|       "member_name": "GetLineStyleIds",
 36496|       "member_kind": "method",
 36497|       "edge_type": "RETURNS_ELEMENT_IDS",
 36498|       "confidence": "unknown_reference",
 36499|       "confidence_tier": "unverified_reference",
 36500|       "target_resolution": "none",
 36501|       "evidence": [
 36502|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 36503|       ],
 36504|       "source_url": "https://www.revitapidocs.com/2025/3e30d0b4-6c74-18bf-043f-2430ff9ac17b.htm",
 36505|       "dll_signature_verified": true,
 36506|       "dll_relationship_scope": "declared",
 36507|       "dll_semantic_verified": null,
 36508|       "dll_verified_status": "signature_verified_declared",
 36509|       "revitlookup_referenced": null,
 36510|       "revitlookup_requires_document_context": null
 36511|     },
 36512|     {
 36513|       "source": "Autodesk.Revit.DB.CurveExtents",
 36514|       "target": null,
 36515|       "member_name": "EndParameter",
 36516|       "member_kind": "property",
 36517|       "edge_type": "HAS_PARAMETER",
 36518|       "confidence": "name_only_candidate",
 36519|       "confidence_tier": "likely",
 36520|       "target_resolution": "none",
 36521|       "evidence": [
 36522|         "member name 'EndParameter' matches keyword pattern /Parameter/ but return type 'double' gives no type-level confirmation"
 36523|       ],
 36524|       "source_url": "https://www.revitapidocs.com/2025/e3195cac-ca48-33c5-88bc-64263ee4cb89.htm",
 36525|       "dll_signature_verified": true,
 36526|       "dll_relationship_scope": "declared",
 36527|       "dll_semantic_verified": null,
 36528|       "dll_verified_status": "signature_verified_declared",
 36529|       "revitlookup_referenced": null,
 36530|       "revitlookup_requires_document_context": null
 36531|     },
 36532|     {
 36533|       "source": "Autodesk.Revit.DB.CurveExtents",
 36534|       "target": null,
 36535|       "member_name": "StartParameter",
 36536|       "member_kind": "property",
 36537|       "edge_type": "HAS_PARAMETER",
 36538|       "confidence": "name_only_candidate",
 36539|       "confidence_tier": "likely",
 36540|       "target_resolution": "none",
 36541|       "evidence": [
 36542|         "member name 'StartParameter' matches keyword pattern /Parameter/ but return type 'double' gives no type-level confirmation"
 36543|       ],
 36544|       "source_url": "https://www.revitapidocs.com/2025/b64e4caf-c1e1-8adc-d95a-09e9acc36eda.htm",
 36545|       "dll_signature_verified": true,
 36546|       "dll_relationship_scope": "declared",
 36547|       "dll_semantic_verified": null,
 36548|       "dll_verified_status": "signature_verified_declared",
 36549|       "revitlookup_referenced": null,
 36550|       "revitlookup_requires_document_context": null
 36551|     },
 36552|     {
 36553|       "source": "Autodesk.Revit.DB.CurveLoop",
 36554|       "target": "Autodesk.Revit.DB.CurveLoopIterator",
 36555|       "member_name": "GetCurveLoopIterator",
 36556|       "member_kind": "method",
 36557|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36558|       "confidence": "direct_return_type",
 36559|       "confidence_tier": "unverified_reference",
 36560|       "target_resolution": "exact",
 36561|       "evidence": [
 36562|         "return type 'CurveLoopIterator' directly names a Revit DB object type"
 36563|       ],
 36564|       "source_url": "https://www.revitapidocs.com/2025/56bdb38b-2385-7e32-13db-6cfd6dbe3e65.htm",
 36565|       "dll_signature_verified": true,
 36566|       "dll_relationship_scope": "declared",
 36567|       "dll_semantic_verified": null,
 36568|       "dll_verified_status": "signature_verified_declared",
 36569|       "revitlookup_referenced": null,
 36570|       "revitlookup_requires_document_context": null
 36571|     },
 36572|     {
 36573|       "source": "Autodesk.Revit.DB.CurveUV",
 36574|       "target": null,
 36575|       "member_name": "GetEndParameter",
 36576|       "member_kind": "method",
 36577|       "edge_type": "HAS_PARAMETER",
 36578|       "confidence": "name_only_candidate",
 36579|       "confidence_tier": "likely",
 36580|       "target_resolution": "none",
 36581|       "evidence": [
 36582|         "member name 'GetEndParameter' matches keyword pattern /Parameter/ but return type 'double' gives no type-level confirmation"
 36583|       ],
 36584|       "source_url": "https://www.revitapidocs.com/2025/daa1ae74-36c8-fcfb-48d9-d9040df6d54f.htm",
 36585|       "dll_signature_verified": true,
 36586|       "dll_relationship_scope": "declared",
 36587|       "dll_semantic_verified": null,
 36588|       "dll_verified_status": "signature_verified_declared",
 36589|       "revitlookup_referenced": null,
 36590|       "revitlookup_requires_document_context": null
 36591|     },
 36592|     {
 36593|       "source": "Autodesk.Revit.DB.CustomFieldData",
 36594|       "target": "Autodesk.Revit.DB.ViewSheet",
 36595|       "member_name": "DefaultRowHeightOnSheet",
 36596|       "member_kind": "property",
 36597|       "edge_type": "PLACED_ON_SHEET",
 36598|       "confidence": "name_only_candidate",
 36599|       "confidence_tier": "likely",
 36600|       "target_resolution": "exact",
 36601|       "evidence": [
 36602|         "member name 'DefaultRowHeightOnSheet' matches keyword pattern /Sheet/ but return type 'double' gives no type-level confirmation"
 36603|       ],
 36604|       "source_url": "https://www.revitapidocs.com/2025/76d96a8d-c4e1-2851-637a-a95623c391dd.htm",
 36605|       "dll_signature_verified": true,
 36606|       "dll_relationship_scope": "declared",
 36607|       "dll_semantic_verified": null,
 36608|       "dll_verified_status": "signature_verified_declared",
 36609|       "revitlookup_referenced": null,
 36610|       "revitlookup_requires_document_context": null
 36611|     },
 36612|     {
 36613|       "source": "Autodesk.Revit.DB.CustomFieldData",
 36614|       "target": "Autodesk.Revit.DB.ICustomFieldProperties",
 36615|       "member_name": "GetCustomFieldProperties",
 36616|       "member_kind": "method",
 36617|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36618|       "confidence": "direct_return_type",
 36619|       "confidence_tier": "unverified_reference",
 36620|       "target_resolution": "exact",
 36621|       "evidence": [
 36622|         "return type 'ICustomFieldProperties' directly names a Revit DB object type"
 36623|       ],
 36624|       "source_url": "https://www.revitapidocs.com/2025/9de9b290-c9f0-167c-ed37-952704c046c6.htm",
 36625|       "dll_signature_verified": true,
 36626|       "dll_relationship_scope": "declared",
 36627|       "dll_semantic_verified": null,
 36628|       "dll_verified_status": "signature_verified_declared",
 36629|       "revitlookup_referenced": null,
 36630|       "revitlookup_requires_document_context": null
 36631|     },
 36632|     {
 36633|       "source": "Autodesk.Revit.DB.CylindricalSurface",
 36634|       "target": "Autodesk.Revit.DB.Frame",
 36635|       "member_name": "GetFrameOfReference",
 36636|       "member_kind": "method",
 36637|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36638|       "confidence": "direct_return_type",
 36639|       "confidence_tier": "unverified_reference",
 36640|       "target_resolution": "exact",
 36641|       "evidence": [
 36642|         "return type 'Frame' directly names a Revit DB object type"
 36643|       ],
 36644|       "source_url": "https://www.revitapidocs.com/2025/547f05b7-1e57-bc40-8df8-681fb80e61e9.htm",
 36645|       "dll_signature_verified": true,
 36646|       "dll_relationship_scope": "declared",
 36647|       "dll_semantic_verified": null,
 36648|       "dll_verified_status": "signature_verified_declared",
 36649|       "revitlookup_referenced": null,
 36650|       "revitlookup_requires_document_context": null
 36651|     },
 36652|     {
 36653|       "source": "Autodesk.Revit.DB.DatumPlane",
 36654|       "target": "Autodesk.Revit.DB.Leader",
 36655|       "member_name": "AddLeader",
 36656|       "member_kind": "method",
 36657|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36658|       "confidence": "direct_return_type",
 36659|       "confidence_tier": "unverified_reference",
 36660|       "target_resolution": "exact",
 36661|       "evidence": [
 36662|         "return type 'Leader' directly names a Revit DB object type"
 36663|       ],
 36664|       "source_url": "https://www.revitapidocs.com/2025/0373eec6-5963-b036-e816-de4d93f2f5f1.htm",
 36665|       "dll_signature_verified": true,
 36666|       "dll_relationship_scope": "declared",
 36667|       "dll_semantic_verified": null,
 36668|       "dll_verified_status": "signature_verified_declared",
 36669|       "revitlookup_referenced": null,
 36670|       "revitlookup_requires_document_context": null
```
