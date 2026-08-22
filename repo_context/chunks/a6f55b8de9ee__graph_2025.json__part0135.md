# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 135 of 216
- Original line range: 52261-52660
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 52261|       "dll_semantic_verified": null,
 52262|       "dll_verified_status": "signature_verified_declared",
 52263|       "revitlookup_referenced": null,
 52264|       "revitlookup_requires_document_context": null
 52265|     },
 52266|     {
 52267|       "source": "Autodesk.Revit.DB.NestedFamilyTypeReference",
 52268|       "target": "Autodesk.Revit.DB.Category",
 52269|       "member_name": "CategoryId",
 52270|       "member_kind": "property",
 52271|       "edge_type": "HAS_CATEGORY",
 52272|       "confidence": "elementid_with_strong_name",
 52273|       "confidence_tier": "core",
 52274|       "target_resolution": "exact",
 52275|       "evidence": [
 52276|         "member name 'CategoryId' matches keyword pattern /Category/"
 52277|       ],
 52278|       "source_url": "https://www.revitapidocs.com/2025/18d2498d-d503-58c1-6c22-ac25cedf32e0.htm",
 52279|       "dll_signature_verified": true,
 52280|       "dll_relationship_scope": "declared",
 52281|       "dll_semantic_verified": null,
 52282|       "dll_verified_status": "signature_verified_declared",
 52283|       "revitlookup_referenced": null,
 52284|       "revitlookup_requires_document_context": null
 52285|     },
 52286|     {
 52287|       "source": "Autodesk.Revit.DB.NumberingSchema",
 52288|       "target": null,
 52289|       "member_name": "NumberingParameterId",
 52290|       "member_kind": "property",
 52291|       "edge_type": "HAS_PARAMETER",
 52292|       "confidence": "elementid_with_strong_name",
 52293|       "confidence_tier": "core",
 52294|       "target_resolution": "none",
 52295|       "evidence": [
 52296|         "member name 'NumberingParameterId' matches keyword pattern /Parameter/"
 52297|       ],
 52298|       "source_url": "https://www.revitapidocs.com/2025/94659f29-c7f2-9643-f443-9451a3177cc2.htm",
 52299|       "dll_signature_verified": true,
 52300|       "dll_relationship_scope": "declared",
 52301|       "dll_semantic_verified": null,
 52302|       "dll_verified_status": "signature_verified_declared",
 52303|       "revitlookup_referenced": null,
 52304|       "revitlookup_requires_document_context": null
 52305|     },
 52306|     {
 52307|       "source": "Autodesk.Revit.DB.NumberingSchema",
 52308|       "target": "Autodesk.Revit.DB.NumberingSchemaType",
 52309|       "member_name": "SchemaType",
 52310|       "member_kind": "property",
 52311|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 52312|       "confidence": "direct_return_type",
 52313|       "confidence_tier": "unverified_reference",
 52314|       "target_resolution": "exact",
 52315|       "evidence": [
 52316|         "return type 'NumberingSchemaType' directly names a Revit DB object type"
 52317|       ],
 52318|       "source_url": "https://www.revitapidocs.com/2025/6c9d8ec5-3622-95ca-e3dc-c663f0a3b155.htm",
 52319|       "dll_signature_verified": true,
 52320|       "dll_relationship_scope": "declared",
 52321|       "dll_semantic_verified": null,
 52322|       "dll_verified_status": "signature_verified_declared",
 52323|       "revitlookup_referenced": null,
 52324|       "revitlookup_requires_document_context": null
 52325|     },
 52326|     {
 52327|       "source": "Autodesk.Revit.DB.NumberingSchema",
 52328|       "target": null,
 52329|       "member_name": "ChangeNumber",
 52330|       "member_kind": "method",
 52331|       "edge_type": "RETURNS_ELEMENT_IDS",
 52332|       "confidence": "unknown_reference",
 52333|       "confidence_tier": "unverified_reference",
 52334|       "target_resolution": "none",
 52335|       "evidence": [
 52336|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 52337|       ],
 52338|       "source_url": "https://www.revitapidocs.com/2025/dc93cd7f-dc11-45da-3ed6-c459d1c55c97.htm",
 52339|       "dll_signature_verified": true,
 52340|       "dll_relationship_scope": "declared",
 52341|       "dll_semantic_verified": null,
 52342|       "dll_verified_status": "signature_verified_declared",
 52343|       "revitlookup_referenced": null,
 52344|       "revitlookup_requires_document_context": null
 52345|     },
 52346|     {
 52347|       "source": "Autodesk.Revit.DB.NumberingSchema",
 52348|       "target": "Autodesk.Revit.DB.IntegerRange",
 52349|       "member_name": "GetNumbers",
 52350|       "member_kind": "method",
 52351|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 52352|       "confidence": "needs_runtime_validation",
 52353|       "confidence_tier": "needs_validation",
 52354|       "target_resolution": "exact",
 52355|       "evidence": [
 52356|         "return type 'IList < IntegerRange >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 52357|       ],
 52358|       "source_url": "https://www.revitapidocs.com/2025/3153106d-9f77-eb5d-20af-cd651e91b640.htm",
 52359|       "dll_signature_verified": true,
 52360|       "dll_relationship_scope": "declared",
 52361|       "dll_semantic_verified": null,
 52362|       "dll_verified_status": "signature_verified_declared",
 52363|       "revitlookup_referenced": null,
 52364|       "revitlookup_requires_document_context": null
 52365|     },
 52366|     {
 52367|       "source": "Autodesk.Revit.DB.NumberingSchema",
 52368|       "target": null,
 52369|       "member_name": "GetSchemasInDocument",
 52370|       "member_kind": "method",
 52371|       "edge_type": "RETURNS_ELEMENT_IDS",
 52372|       "confidence": "unknown_reference",
 52373|       "confidence_tier": "unverified_reference",
 52374|       "target_resolution": "none",
 52375|       "evidence": [
 52376|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 52377|       ],
 52378|       "source_url": "https://www.revitapidocs.com/2025/9bd7a9f5-49af-1d41-9f55-932723b7023e.htm",
 52379|       "dll_signature_verified": true,
 52380|       "dll_relationship_scope": "declared",
 52381|       "dll_semantic_verified": null,
 52382|       "dll_verified_status": "signature_verified_declared",
 52383|       "revitlookup_referenced": null,
 52384|       "revitlookup_requires_document_context": null
 52385|     },
 52386|     {
 52387|       "source": "Autodesk.Revit.DB.NumberingSchemaTypes.StructuralNumberingSchemas",
 52388|       "target": "Autodesk.Revit.DB.NumberingSchemaType",
 52389|       "member_name": "Rebar",
 52390|       "member_kind": "property",
 52391|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 52392|       "confidence": "direct_return_type",
 52393|       "confidence_tier": "unverified_reference",
 52394|       "target_resolution": "exact",
 52395|       "evidence": [
 52396|         "return type 'NumberingSchemaType' directly names a Revit DB object type"
 52397|       ],
 52398|       "source_url": "https://www.revitapidocs.com/2025/fc841a45-730b-c9df-27c6-8d1bacd9527b.htm",
 52399|       "dll_signature_verified": true,
 52400|       "dll_relationship_scope": "declared",
 52401|       "dll_semantic_verified": null,
 52402|       "dll_verified_status": "signature_verified_declared",
 52403|       "revitlookup_referenced": null,
 52404|       "revitlookup_requires_document_context": null
 52405|     },
 52406|     {
 52407|       "source": "Autodesk.Revit.DB.NumberingSchemaTypes.StructuralNumberingSchemas",
 52408|       "target": "Autodesk.Revit.DB.NumberingSchemaType",
 52409|       "member_name": "RebarCoupler",
 52410|       "member_kind": "property",
 52411|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 52412|       "confidence": "direct_return_type",
 52413|       "confidence_tier": "unverified_reference",
 52414|       "target_resolution": "exact",
 52415|       "evidence": [
 52416|         "return type 'NumberingSchemaType' directly names a Revit DB object type"
 52417|       ],
 52418|       "source_url": "https://www.revitapidocs.com/2025/1488da84-f9c7-c4fe-6241-6a6e20d27164.htm",
 52419|       "dll_signature_verified": true,
 52420|       "dll_relationship_scope": "declared",
 52421|       "dll_semantic_verified": null,
 52422|       "dll_verified_status": "signature_verified_declared",
 52423|       "revitlookup_referenced": null,
 52424|       "revitlookup_requires_document_context": null
 52425|     },
 52426|     {
 52427|       "source": "Autodesk.Revit.DB.NumberingSchemaTypes.StructuralNumberingSchemas",
 52428|       "target": "Autodesk.Revit.DB.NumberingSchemaType",
 52429|       "member_name": "ReinforcementFabric",
 52430|       "member_kind": "property",
 52431|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 52432|       "confidence": "direct_return_type",
 52433|       "confidence_tier": "unverified_reference",
 52434|       "target_resolution": "exact",
 52435|       "evidence": [
 52436|         "return type 'NumberingSchemaType' directly names a Revit DB object type"
 52437|       ],
 52438|       "source_url": "https://www.revitapidocs.com/2025/2d61508d-d0fc-baa1-1aee-5fc5b4d5ba4f.htm",
 52439|       "dll_signature_verified": true,
 52440|       "dll_relationship_scope": "declared",
 52441|       "dll_semantic_verified": null,
 52442|       "dll_verified_status": "signature_verified_declared",
 52443|       "revitlookup_referenced": null,
 52444|       "revitlookup_requires_document_context": null
 52445|     },
 52446|     {
 52447|       "source": "Autodesk.Revit.DB.NumberSystem",
 52448|       "target": null,
 52449|       "member_name": "NumberedElementId",
 52450|       "member_kind": "property",
 52451|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 52452|       "confidence": "unknown_reference",
 52453|       "confidence_tier": "unverified_reference",
 52454|       "target_resolution": "none",
 52455|       "evidence": [
 52456|         "return type is 'LinkElementId', an ID wrapper, but member name gives no strong hint of the target type"
 52457|       ],
 52458|       "source_url": "https://www.revitapidocs.com/2025/2baae2d1-c174-87d2-3949-32dffd42fdda.htm",
 52459|       "dll_signature_verified": true,
 52460|       "dll_relationship_scope": "declared",
 52461|       "dll_semantic_verified": null,
 52462|       "dll_verified_status": "signature_verified_declared",
 52463|       "revitlookup_referenced": null,
 52464|       "revitlookup_requires_document_context": null
 52465|     },
 52466|     {
 52467|       "source": "Autodesk.Revit.DB.NumberSystem",
 52468|       "target": "Autodesk.Revit.DB.Level",
 52469|       "member_name": "PlacementLevelId",
 52470|       "member_kind": "property",
 52471|       "edge_type": "ASSIGNED_TO_LEVEL",
 52472|       "confidence": "elementid_with_strong_name",
 52473|       "confidence_tier": "core",
 52474|       "target_resolution": "exact",
 52475|       "evidence": [
 52476|         "member name 'PlacementLevelId' matches keyword pattern /Level/"
 52477|       ],
 52478|       "source_url": "https://www.revitapidocs.com/2025/fe0c22de-11c5-3436-e981-6703e45610c1.htm",
 52479|       "dll_signature_verified": true,
 52480|       "dll_relationship_scope": "declared",
 52481|       "dll_semantic_verified": null,
 52482|       "dll_verified_status": "signature_verified_declared",
 52483|       "revitlookup_referenced": null,
 52484|       "revitlookup_requires_document_context": null
 52485|     },
 52486|     {
 52487|       "source": "Autodesk.Revit.DB.NumberSystem",
 52488|       "target": "Autodesk.Revit.DB.Reference",
 52489|       "member_name": "GetReferencePick",
 52490|       "member_kind": "method",
 52491|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 52492|       "confidence": "direct_return_type",
 52493|       "confidence_tier": "unverified_reference",
 52494|       "target_resolution": "exact",
 52495|       "evidence": [
 52496|         "return type 'Reference' directly names a Revit DB object type"
 52497|       ],
 52498|       "source_url": "https://www.revitapidocs.com/2025/8c6e215d-1783-4f3f-6b58-9efcb5e43cc8.htm",
 52499|       "dll_signature_verified": true,
 52500|       "dll_relationship_scope": "declared",
 52501|       "dll_semantic_verified": null,
 52502|       "dll_verified_status": "signature_verified_declared",
 52503|       "revitlookup_referenced": null,
 52504|       "revitlookup_requires_document_context": null
 52505|     },
 52506|     {
 52507|       "source": "Autodesk.Revit.DB.NurbSpline",
 52508|       "target": "Autodesk.Revit.DB.DoubleArray",
 52509|       "member_name": "Knots",
 52510|       "member_kind": "property",
 52511|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 52512|       "confidence": "direct_return_type",
 52513|       "confidence_tier": "unverified_reference",
 52514|       "target_resolution": "exact",
 52515|       "evidence": [
 52516|         "return type 'DoubleArray' directly names a Revit DB object type"
 52517|       ],
 52518|       "source_url": "https://www.revitapidocs.com/2025/bcb5d28e-12ab-5ea3-397d-ed5ffa8f872c.htm",
 52519|       "dll_signature_verified": true,
 52520|       "dll_relationship_scope": "declared",
 52521|       "dll_semantic_verified": null,
 52522|       "dll_verified_status": "signature_verified_declared",
 52523|       "revitlookup_referenced": null,
 52524|       "revitlookup_requires_document_context": null
 52525|     },
 52526|     {
 52527|       "source": "Autodesk.Revit.DB.NurbSpline",
 52528|       "target": "Autodesk.Revit.DB.DoubleArray",
 52529|       "member_name": "Weights",
 52530|       "member_kind": "property",
 52531|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 52532|       "confidence": "direct_return_type",
 52533|       "confidence_tier": "unverified_reference",
 52534|       "target_resolution": "exact",
 52535|       "evidence": [
 52536|         "return type 'DoubleArray' directly names a Revit DB object type"
 52537|       ],
 52538|       "source_url": "https://www.revitapidocs.com/2025/3cbb6ef9-6234-672a-3705-205b43e1da17.htm",
 52539|       "dll_signature_verified": true,
 52540|       "dll_relationship_scope": "declared",
 52541|       "dll_semantic_verified": null,
 52542|       "dll_verified_status": "signature_verified_declared",
 52543|       "revitlookup_referenced": null,
 52544|       "revitlookup_requires_document_context": null
 52545|     },
 52546|     {
 52547|       "source": "Autodesk.Revit.DB.OffsetSurface",
 52548|       "target": "Autodesk.Revit.DB.Surface",
 52549|       "member_name": "GetBasisSurface",
 52550|       "member_kind": "method",
 52551|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 52552|       "confidence": "direct_return_type",
 52553|       "confidence_tier": "unverified_reference",
 52554|       "target_resolution": "exact",
 52555|       "evidence": [
 52556|         "return type 'Surface' directly names a Revit DB object type"
 52557|       ],
 52558|       "source_url": "https://www.revitapidocs.com/2025/5fb55047-9bf3-aa08-a0f6-695916946c30.htm",
 52559|       "dll_signature_verified": true,
 52560|       "dll_relationship_scope": "declared",
 52561|       "dll_semantic_verified": null,
 52562|       "dll_verified_status": "signature_verified_declared",
 52563|       "revitlookup_referenced": null,
 52564|       "revitlookup_requires_document_context": null
 52565|     },
 52566|     {
 52567|       "source": "Autodesk.Revit.DB.Opening",
 52568|       "target": "Autodesk.Revit.DB.Element",
 52569|       "member_name": "Host",
 52570|       "member_kind": "property",
 52571|       "edge_type": "HOSTED_BY",
 52572|       "confidence": "direct_return_type",
 52573|       "confidence_tier": "core",
 52574|       "target_resolution": "exact",
 52575|       "evidence": [
 52576|         "return type 'Element' directly names a Revit DB object type"
 52577|       ],
 52578|       "source_url": "https://www.revitapidocs.com/2025/6f603c8c-7644-4d71-a6d2-e3bca22b6c27.htm",
 52579|       "dll_signature_verified": true,
 52580|       "dll_relationship_scope": "declared",
 52581|       "dll_semantic_verified": null,
 52582|       "dll_verified_status": "signature_verified_declared",
 52583|       "revitlookup_referenced": null,
 52584|       "revitlookup_requires_document_context": null
 52585|     },
 52586|     {
 52587|       "source": "Autodesk.Revit.DB.Opening",
 52588|       "target": "Autodesk.Revit.DB.Sketch",
 52589|       "member_name": "SketchId",
 52590|       "member_kind": "property",
 52591|       "edge_type": "DEPENDS_ON",
 52592|       "confidence": "elementid_with_strong_name",
 52593|       "confidence_tier": "core",
 52594|       "target_resolution": "exact",
 52595|       "evidence": [
 52596|         "member name 'SketchId' matches keyword pattern /Sketch(Id)?$/"
 52597|       ],
 52598|       "source_url": "https://www.revitapidocs.com/2025/91ed6571-13b3-fa4c-d1c3-624e1cbd73f9.htm",
 52599|       "dll_signature_verified": true,
 52600|       "dll_relationship_scope": "declared",
 52601|       "dll_semantic_verified": null,
 52602|       "dll_verified_status": "signature_verified_declared",
 52603|       "revitlookup_referenced": null,
 52604|       "revitlookup_requires_document_context": null
 52605|     },
 52606|     {
 52607|       "source": "Autodesk.Revit.DB.OpenOptions",
 52608|       "target": "Autodesk.Revit.DB.WorksetConfiguration",
 52609|       "member_name": "GetOpenWorksetsConfiguration",
 52610|       "member_kind": "method",
 52611|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 52612|       "confidence": "direct_return_type",
 52613|       "confidence_tier": "unverified_reference",
 52614|       "target_resolution": "exact",
 52615|       "evidence": [
 52616|         "member name 'GetOpenWorksetsConfiguration' matches keyword pattern /Workset/ implying target 'Workset', but the actual return type 'WorksetConfiguration' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 52617|         "return type 'WorksetConfiguration' directly names a Revit DB object type"
 52618|       ],
 52619|       "source_url": "https://www.revitapidocs.com/2025/f06b3060-cd2d-31db-f627-fe0d96236e3c.htm",
 52620|       "dll_signature_verified": true,
 52621|       "dll_relationship_scope": "declared",
 52622|       "dll_semantic_verified": null,
 52623|       "dll_verified_status": "signature_verified_declared",
 52624|       "revitlookup_referenced": null,
 52625|       "revitlookup_requires_document_context": null
 52626|     },
 52627|     {
 52628|       "source": "Autodesk.Revit.DB.OpenOptions",
 52629|       "target": "Autodesk.Revit.DB.Workset",
 52630|       "member_name": "SetOpenWorksetsConfiguration",
 52631|       "member_kind": "method",
 52632|       "edge_type": "OWNED_BY_WORKSET",
 52633|       "confidence": "name_only_candidate",
 52634|       "confidence_tier": "likely",
 52635|       "target_resolution": "exact",
 52636|       "evidence": [
 52637|         "member name 'SetOpenWorksetsConfiguration' matches keyword pattern /Workset/ but return type 'void' gives no type-level confirmation"
 52638|       ],
 52639|       "source_url": "https://www.revitapidocs.com/2025/88de72a4-cf23-c2e7-7b38-acadc45591e7.htm",
 52640|       "dll_signature_verified": true,
 52641|       "dll_relationship_scope": "declared",
 52642|       "dll_semantic_verified": null,
 52643|       "dll_verified_status": "signature_verified_declared",
 52644|       "revitlookup_referenced": null,
 52645|       "revitlookup_requires_document_context": null
 52646|     },
 52647|     {
 52648|       "source": "Autodesk.Revit.DB.Options",
 52649|       "target": "Autodesk.Revit.DB.Level",
 52650|       "member_name": "DetailLevel",
 52651|       "member_kind": "property",
 52652|       "edge_type": "ASSIGNED_TO_LEVEL",
 52653|       "confidence": "name_only_candidate",
 52654|       "confidence_tier": "likely",
 52655|       "target_resolution": "exact",
 52656|       "evidence": [
 52657|         "member name 'DetailLevel' matches keyword pattern /Level/ but return type 'ViewDetailLevel' gives no type-level confirmation"
 52658|       ],
 52659|       "source_url": "https://www.revitapidocs.com/2025/887c4c25-fe14-2633-b84c-09d2f1279c9e.htm",
 52660|       "dll_signature_verified": true,
```
