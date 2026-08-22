# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 181 of 216
- Original line range: 70201-70600
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 70201|       "source": "Autodesk.Revit.DB.Electrical.CableTraySizes",
 70202|       "target": "Autodesk.Revit.DB.Electrical.CableTraySizeIterator",
 70203|       "member_name": "GetCableTraySizesIterator",
 70204|       "member_kind": "method",
 70205|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70206|       "confidence": "direct_return_type",
 70207|       "confidence_tier": "unverified_reference",
 70208|       "target_resolution": "short_name_fallback",
 70209|       "evidence": [
 70210|         "return type 'CableTraySizeIterator' directly names a Revit DB object type"
 70211|       ],
 70212|       "source_url": "https://www.revitapidocs.com/2025/a0aabc61-8f85-c9eb-c3ce-0026f4e5758d.htm",
 70213|       "dll_signature_verified": true,
 70214|       "dll_relationship_scope": "declared",
 70215|       "dll_semantic_verified": null,
 70216|       "dll_verified_status": "signature_verified_declared",
 70217|       "revitlookup_referenced": null,
 70218|       "revitlookup_requires_document_context": null
 70219|     },
 70220|     {
 70221|       "source": "Autodesk.Revit.DB.Electrical.CircuitNamingScheme",
 70222|       "target": "Autodesk.Revit.DB.TableCellCombinedParameterData",
 70223|       "member_name": "GetCombinedParameters",
 70224|       "member_kind": "method",
 70225|       "edge_type": "HAS_PARAMETER",
 70226|       "confidence": "needs_runtime_validation",
 70227|       "confidence_tier": "needs_validation",
 70228|       "target_resolution": "exact",
 70229|       "evidence": [
 70230|         "return type 'IList < TableCellCombinedParameterData >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 70231|       ],
 70232|       "source_url": "https://www.revitapidocs.com/2025/d54cc44d-d057-9f13-6d35-837bba8e0f32.htm",
 70233|       "dll_signature_verified": true,
 70234|       "dll_relationship_scope": "declared",
 70235|       "dll_semantic_verified": null,
 70236|       "dll_verified_status": "signature_verified_declared",
 70237|       "revitlookup_referenced": null,
 70238|       "revitlookup_requires_document_context": null
 70239|     },
 70240|     {
 70241|       "source": "Autodesk.Revit.DB.Electrical.CircuitNamingScheme",
 70242|       "target": null,
 70243|       "member_name": "IsValidCombinedParameters",
 70244|       "member_kind": "method",
 70245|       "edge_type": "HAS_PARAMETER",
 70246|       "confidence": "name_only_candidate",
 70247|       "confidence_tier": "likely",
 70248|       "target_resolution": "none",
 70249|       "evidence": [
 70250|         "member name 'IsValidCombinedParameters' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 70251|       ],
 70252|       "source_url": "https://www.revitapidocs.com/2025/67351dc2-02d0-4c5a-d72b-23fc3a4c0caf.htm",
 70253|       "dll_signature_verified": true,
 70254|       "dll_relationship_scope": "declared",
 70255|       "dll_semantic_verified": null,
 70256|       "dll_verified_status": "signature_verified_declared",
 70257|       "revitlookup_referenced": null,
 70258|       "revitlookup_requires_document_context": null
 70259|     },
 70260|     {
 70261|       "source": "Autodesk.Revit.DB.Electrical.CircuitNamingScheme",
 70262|       "target": null,
 70263|       "member_name": "SetCombinedParameters",
 70264|       "member_kind": "method",
 70265|       "edge_type": "HAS_PARAMETER",
 70266|       "confidence": "name_only_candidate",
 70267|       "confidence_tier": "likely",
 70268|       "target_resolution": "none",
 70269|       "evidence": [
 70270|         "member name 'SetCombinedParameters' matches keyword pattern /Parameter/ but return type 'void' gives no type-level confirmation"
 70271|       ],
 70272|       "source_url": "https://www.revitapidocs.com/2025/edd9f04a-2a63-3426-b9d8-4b72a0c74791.htm",
 70273|       "dll_signature_verified": true,
 70274|       "dll_relationship_scope": "declared",
 70275|       "dll_semantic_verified": null,
 70276|       "dll_verified_status": "signature_verified_declared",
 70277|       "revitlookup_referenced": null,
 70278|       "revitlookup_requires_document_context": null
 70279|     },
 70280|     {
 70281|       "source": "Autodesk.Revit.DB.Electrical.CircuitNamingSchemeSettings",
 70282|       "target": null,
 70283|       "member_name": "CircuitNamingSchemeId",
 70284|       "member_kind": "property",
 70285|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 70286|       "confidence": "unknown_reference",
 70287|       "confidence_tier": "unverified_reference",
 70288|       "target_resolution": "none",
 70289|       "evidence": [
 70290|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 70291|       ],
 70292|       "source_url": "https://www.revitapidocs.com/2025/2cc3d296-fb16-e1b4-b2f3-b344702d11a2.htm",
 70293|       "dll_signature_verified": true,
 70294|       "dll_relationship_scope": "declared",
 70295|       "dll_semantic_verified": null,
 70296|       "dll_verified_status": "signature_verified_declared",
 70297|       "revitlookup_referenced": null,
 70298|       "revitlookup_requires_document_context": null
 70299|     },
 70300|     {
 70301|       "source": "Autodesk.Revit.DB.Electrical.ConduitSizeIterator",
 70302|       "target": "Autodesk.Revit.DB.Electrical.ConduitSize",
 70303|       "member_name": "Current",
 70304|       "member_kind": "property",
 70305|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70306|       "confidence": "direct_return_type",
 70307|       "confidence_tier": "unverified_reference",
 70308|       "target_resolution": "short_name_fallback",
 70309|       "evidence": [
 70310|         "return type 'ConduitSize' directly names a Revit DB object type"
 70311|       ],
 70312|       "source_url": "https://www.revitapidocs.com/2025/e69bfad3-dc3e-1bd2-1508-ba8c406699fd.htm",
 70313|       "dll_signature_verified": true,
 70314|       "dll_relationship_scope": "declared",
 70315|       "dll_semantic_verified": null,
 70316|       "dll_verified_status": "signature_verified_declared",
 70317|       "revitlookup_referenced": null,
 70318|       "revitlookup_requires_document_context": null
 70319|     },
 70320|     {
 70321|       "source": "Autodesk.Revit.DB.Electrical.ConduitSizeIterator",
 70322|       "target": "Autodesk.Revit.DB.Electrical.ConduitSize",
 70323|       "member_name": "GetCurrent",
 70324|       "member_kind": "method",
 70325|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70326|       "confidence": "direct_return_type",
 70327|       "confidence_tier": "unverified_reference",
 70328|       "target_resolution": "short_name_fallback",
 70329|       "evidence": [
 70330|         "return type 'ConduitSize' directly names a Revit DB object type"
 70331|       ],
 70332|       "source_url": "https://www.revitapidocs.com/2025/59cd53c3-b8d9-8e29-7c6b-c39cd36d4095.htm",
 70333|       "dll_signature_verified": true,
 70334|       "dll_relationship_scope": "declared",
 70335|       "dll_semantic_verified": null,
 70336|       "dll_verified_status": "signature_verified_declared",
 70337|       "revitlookup_referenced": null,
 70338|       "revitlookup_requires_document_context": null
 70339|     },
 70340|     {
 70341|       "source": "Autodesk.Revit.DB.Electrical.ConduitSizes",
 70342|       "target": "Autodesk.Revit.DB.Electrical.ConduitSizeIterator",
 70343|       "member_name": "GetConduitSizesIterator",
 70344|       "member_kind": "method",
 70345|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70346|       "confidence": "direct_return_type",
 70347|       "confidence_tier": "unverified_reference",
 70348|       "target_resolution": "short_name_fallback",
 70349|       "evidence": [
 70350|         "return type 'ConduitSizeIterator' directly names a Revit DB object type"
 70351|       ],
 70352|       "source_url": "https://www.revitapidocs.com/2025/7a01fb6a-00dd-5c39-3539-fd0cc1cef15b.htm",
 70353|       "dll_signature_verified": true,
 70354|       "dll_relationship_scope": "declared",
 70355|       "dll_semantic_verified": null,
 70356|       "dll_verified_status": "signature_verified_declared",
 70357|       "revitlookup_referenced": null,
 70358|       "revitlookup_requires_document_context": null
 70359|     },
 70360|     {
 70361|       "source": "Autodesk.Revit.DB.Electrical.ConduitSizeSettings",
 70362|       "target": "Autodesk.Revit.DB.Electrical.ConduitSizeSettingIterator",
 70363|       "member_name": "GetConduitSizeSettingsIterator",
 70364|       "member_kind": "method",
 70365|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70366|       "confidence": "direct_return_type",
 70367|       "confidence_tier": "unverified_reference",
 70368|       "target_resolution": "short_name_fallback",
 70369|       "evidence": [
 70370|         "return type 'ConduitSizeSettingIterator' directly names a Revit DB object type"
 70371|       ],
 70372|       "source_url": "https://www.revitapidocs.com/2025/8d9283aa-e31a-b7bc-66e3-08b9ed143d54.htm",
 70373|       "dll_signature_verified": true,
 70374|       "dll_relationship_scope": "declared",
 70375|       "dll_semantic_verified": null,
 70376|       "dll_verified_status": "signature_verified_declared",
 70377|       "revitlookup_referenced": null,
 70378|       "revitlookup_requires_document_context": null
 70379|     },
 70380|     {
 70381|       "source": "Autodesk.Revit.DB.Electrical.CorrectionFactorSet",
 70382|       "target": "Autodesk.Revit.DB.Electrical.CorrectionFactorSetIterator",
 70383|       "member_name": "ForwardIterator",
 70384|       "member_kind": "method",
 70385|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70386|       "confidence": "direct_return_type",
 70387|       "confidence_tier": "unverified_reference",
 70388|       "target_resolution": "short_name_fallback",
 70389|       "evidence": [
 70390|         "return type 'CorrectionFactorSetIterator' directly names a Revit DB object type"
 70391|       ],
 70392|       "source_url": "https://www.revitapidocs.com/2025/cda4b2a7-c49c-a1a8-bda2-e89bb780fe6a.htm",
 70393|       "dll_signature_verified": true,
 70394|       "dll_relationship_scope": "declared",
 70395|       "dll_semantic_verified": null,
 70396|       "dll_verified_status": "signature_verified_declared",
 70397|       "revitlookup_referenced": null,
 70398|       "revitlookup_requires_document_context": null
 70399|     },
 70400|     {
 70401|       "source": "Autodesk.Revit.DB.Electrical.CorrectionFactorSet",
 70402|       "target": "Autodesk.Revit.DB.Electrical.CorrectionFactorSetIterator",
 70403|       "member_name": "ReverseIterator",
 70404|       "member_kind": "method",
 70405|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70406|       "confidence": "direct_return_type",
 70407|       "confidence_tier": "unverified_reference",
 70408|       "target_resolution": "short_name_fallback",
 70409|       "evidence": [
 70410|         "return type 'CorrectionFactorSetIterator' directly names a Revit DB object type"
 70411|       ],
 70412|       "source_url": "https://www.revitapidocs.com/2025/012415ba-04a3-3c94-e48b-9b6684d94f5d.htm",
 70413|       "dll_signature_verified": true,
 70414|       "dll_relationship_scope": "declared",
 70415|       "dll_semantic_verified": null,
 70416|       "dll_verified_status": "signature_verified_declared",
 70417|       "revitlookup_referenced": null,
 70418|       "revitlookup_requires_document_context": null
 70419|     },
 70420|     {
 70421|       "source": "Autodesk.Revit.DB.Electrical.DistributionSysType",
 70422|       "target": "Autodesk.Revit.DB.Phase",
 70423|       "member_name": "ElectricalPhase",
 70424|       "member_kind": "property",
 70425|       "edge_type": "ASSIGNED_TO_PHASE",
 70426|       "confidence": "name_only_candidate",
 70427|       "confidence_tier": "likely",
 70428|       "target_resolution": "exact",
 70429|       "evidence": [
 70430|         "member name 'ElectricalPhase' matches keyword pattern /Phase/ but return type 'ElectricalPhase' gives no type-level confirmation"
 70431|       ],
 70432|       "source_url": "https://www.revitapidocs.com/2025/dc3fb2a6-d39e-3b80-3506-550de98945a9.htm",
 70433|       "dll_signature_verified": true,
 70434|       "dll_relationship_scope": "declared",
 70435|       "dll_semantic_verified": null,
 70436|       "dll_verified_status": "signature_verified_declared",
 70437|       "revitlookup_referenced": null,
 70438|       "revitlookup_requires_document_context": null
 70439|     },
 70440|     {
 70441|       "source": "Autodesk.Revit.DB.Electrical.DistributionSysType",
 70442|       "target": "Autodesk.Revit.DB.Phase",
 70443|       "member_name": "ElectricalPhaseConfiguration",
 70444|       "member_kind": "property",
 70445|       "edge_type": "ASSIGNED_TO_PHASE",
 70446|       "confidence": "name_only_candidate",
 70447|       "confidence_tier": "likely",
 70448|       "target_resolution": "exact",
 70449|       "evidence": [
 70450|         "member name 'ElectricalPhaseConfiguration' matches keyword pattern /Phase/ but return type 'ElectricalPhaseConfiguration' gives no type-level confirmation"
 70451|       ],
 70452|       "source_url": "https://www.revitapidocs.com/2025/933cb07d-9ca3-0030-c07e-571690365cce.htm",
 70453|       "dll_signature_verified": true,
 70454|       "dll_relationship_scope": "declared",
 70455|       "dll_semantic_verified": null,
 70456|       "dll_verified_status": "signature_verified_declared",
 70457|       "revitlookup_referenced": null,
 70458|       "revitlookup_requires_document_context": null
 70459|     },
 70460|     {
 70461|       "source": "Autodesk.Revit.DB.Electrical.DistributionSysType",
 70462|       "target": "Autodesk.Revit.DB.Phase",
 70463|       "member_name": "HighLegPhase",
 70464|       "member_kind": "property",
 70465|       "edge_type": "ASSIGNED_TO_PHASE",
 70466|       "confidence": "name_only_candidate",
 70467|       "confidence_tier": "likely",
 70468|       "target_resolution": "exact",
 70469|       "evidence": [
 70470|         "member name 'HighLegPhase' matches keyword pattern /Phase/ but return type 'ElectricalPhaseLine' gives no type-level confirmation"
 70471|       ],
 70472|       "source_url": "https://www.revitapidocs.com/2025/f24d8c45-b2c0-b1c0-20e9-5b097fe485d2.htm",
 70473|       "dll_signature_verified": true,
 70474|       "dll_relationship_scope": "declared",
 70475|       "dll_semantic_verified": null,
 70476|       "dll_verified_status": "signature_verified_declared",
 70477|       "revitlookup_referenced": null,
 70478|       "revitlookup_requires_document_context": null
 70479|     },
 70480|     {
 70481|       "source": "Autodesk.Revit.DB.Electrical.DistributionSysType",
 70482|       "target": "Autodesk.Revit.DB.Electrical.VoltageType",
 70483|       "member_name": "VoltageLineToGround",
 70484|       "member_kind": "property",
 70485|       "edge_type": "TAGS_ELEMENT",
 70486|       "confidence": "direct_return_type",
 70487|       "confidence_tier": "core",
 70488|       "target_resolution": "short_name_fallback",
 70489|       "evidence": [
 70490|         "return type 'VoltageType' directly names a Revit DB object type"
 70491|       ],
 70492|       "source_url": "https://www.revitapidocs.com/2025/ec5e6690-0d63-45d1-5414-f8115bcf6965.htm",
 70493|       "dll_signature_verified": true,
 70494|       "dll_relationship_scope": "declared",
 70495|       "dll_semantic_verified": null,
 70496|       "dll_verified_status": "signature_verified_declared",
 70497|       "revitlookup_referenced": null,
 70498|       "revitlookup_requires_document_context": null
 70499|     },
 70500|     {
 70501|       "source": "Autodesk.Revit.DB.Electrical.DistributionSysType",
 70502|       "target": "Autodesk.Revit.DB.Electrical.VoltageType",
 70503|       "member_name": "VoltageLineToLine",
 70504|       "member_kind": "property",
 70505|       "edge_type": "TAGS_ELEMENT",
 70506|       "confidence": "direct_return_type",
 70507|       "confidence_tier": "core",
 70508|       "target_resolution": "short_name_fallback",
 70509|       "evidence": [
 70510|         "return type 'VoltageType' directly names a Revit DB object type"
 70511|       ],
 70512|       "source_url": "https://www.revitapidocs.com/2025/3b0d256f-2611-57f5-05b7-065d60259e7a.htm",
 70513|       "dll_signature_verified": true,
 70514|       "dll_relationship_scope": "declared",
 70515|       "dll_semantic_verified": null,
 70516|       "dll_verified_status": "signature_verified_declared",
 70517|       "revitlookup_referenced": null,
 70518|       "revitlookup_requires_document_context": null
 70519|     },
 70520|     {
 70521|       "source": "Autodesk.Revit.DB.Electrical.DistributionSysTypeSet",
 70522|       "target": "Autodesk.Revit.DB.Electrical.DistributionSysTypeSetIterator",
 70523|       "member_name": "ForwardIterator",
 70524|       "member_kind": "method",
 70525|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70526|       "confidence": "direct_return_type",
 70527|       "confidence_tier": "unverified_reference",
 70528|       "target_resolution": "short_name_fallback",
 70529|       "evidence": [
 70530|         "return type 'DistributionSysTypeSetIterator' directly names a Revit DB object type"
 70531|       ],
 70532|       "source_url": "https://www.revitapidocs.com/2025/9d19a1b3-fcc8-53f5-4fd6-da094ee893fc.htm",
 70533|       "dll_signature_verified": true,
 70534|       "dll_relationship_scope": "declared",
 70535|       "dll_semantic_verified": null,
 70536|       "dll_verified_status": "signature_verified_declared",
 70537|       "revitlookup_referenced": null,
 70538|       "revitlookup_requires_document_context": null
 70539|     },
 70540|     {
 70541|       "source": "Autodesk.Revit.DB.Electrical.DistributionSysTypeSet",
 70542|       "target": "Autodesk.Revit.DB.Electrical.DistributionSysTypeSetIterator",
 70543|       "member_name": "ReverseIterator",
 70544|       "member_kind": "method",
 70545|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70546|       "confidence": "direct_return_type",
 70547|       "confidence_tier": "unverified_reference",
 70548|       "target_resolution": "short_name_fallback",
 70549|       "evidence": [
 70550|         "return type 'DistributionSysTypeSetIterator' directly names a Revit DB object type"
 70551|       ],
 70552|       "source_url": "https://www.revitapidocs.com/2025/6571a6ee-a994-96c8-9e37-94864d1061ee.htm",
 70553|       "dll_signature_verified": true,
 70554|       "dll_relationship_scope": "declared",
 70555|       "dll_semantic_verified": null,
 70556|       "dll_verified_status": "signature_verified_declared",
 70557|       "revitlookup_referenced": null,
 70558|       "revitlookup_requires_document_context": null
 70559|     },
 70560|     {
 70561|       "source": "Autodesk.Revit.DB.Electrical.ElectricalAnalyticalLoadSet",
 70562|       "target": null,
 70563|       "member_name": "GetLoadIds",
 70564|       "member_kind": "method",
 70565|       "edge_type": "RETURNS_ELEMENT_IDS",
 70566|       "confidence": "unknown_reference",
 70567|       "confidence_tier": "unverified_reference",
 70568|       "target_resolution": "none",
 70569|       "evidence": [
 70570|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 70571|       ],
 70572|       "source_url": "https://www.revitapidocs.com/2025/2b5171dd-45be-b19d-6980-2a0bc5a90398.htm",
 70573|       "dll_signature_verified": true,
 70574|       "dll_relationship_scope": "declared",
 70575|       "dll_semantic_verified": null,
 70576|       "dll_verified_status": "signature_verified_declared",
 70577|       "revitlookup_referenced": null,
 70578|       "revitlookup_requires_document_context": null
 70579|     },
 70580|     {
 70581|       "source": "Autodesk.Revit.DB.Electrical.ElectricalAnalyticalNode",
 70582|       "target": null,
 70583|       "member_name": "GetAllDownstreamLoadIds",
 70584|       "member_kind": "method",
 70585|       "edge_type": "RETURNS_ELEMENT_IDS",
 70586|       "confidence": "elementid_collection_with_strong_name",
 70587|       "confidence_tier": "core",
 70588|       "target_resolution": "none",
 70589|       "evidence": [
 70590|         "member name 'GetAllDownstreamLoadIds' matches keyword pattern /^GetAll/"
 70591|       ],
 70592|       "source_url": "https://www.revitapidocs.com/2025/6d99429d-8994-86c8-c99e-6095096d8454.htm",
 70593|       "dll_signature_verified": true,
 70594|       "dll_relationship_scope": "declared",
 70595|       "dll_semantic_verified": null,
 70596|       "dll_verified_status": "signature_verified_declared",
 70597|       "revitlookup_referenced": null,
 70598|       "revitlookup_requires_document_context": null
 70599|     },
 70600|     {
```
