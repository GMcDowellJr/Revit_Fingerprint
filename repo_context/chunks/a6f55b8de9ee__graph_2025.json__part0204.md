# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 204 of 216
- Original line range: 79171-79570
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 79171|       "member_kind": "property",
 79172|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 79173|       "confidence": "direct_return_type",
 79174|       "confidence_tier": "unverified_reference",
 79175|       "target_resolution": "short_name_fallback",
 79176|       "evidence": [
 79177|         "return type 'PathReinforcementType' directly names a Revit DB object type"
 79178|       ],
 79179|       "source_url": "https://www.revitapidocs.com/2025/01592a23-de13-fd3e-7508-55e1d83994d1.htm",
 79180|       "dll_signature_verified": true,
 79181|       "dll_relationship_scope": "declared",
 79182|       "dll_semantic_verified": null,
 79183|       "dll_verified_status": "signature_verified_declared",
 79184|       "revitlookup_referenced": null,
 79185|       "revitlookup_requires_document_context": null
 79186|     },
 79187|     {
 79188|       "source": "Autodesk.Revit.DB.Structure.PathReinforcement",
 79189|       "target": null,
 79190|       "member_name": "PrimaryBarShapeId",
 79191|       "member_kind": "property",
 79192|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 79193|       "confidence": "unknown_reference",
 79194|       "confidence_tier": "unverified_reference",
 79195|       "target_resolution": "none",
 79196|       "evidence": [
 79197|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 79198|       ],
 79199|       "source_url": "https://www.revitapidocs.com/2025/06e0653f-9e58-c180-67b3-cbdd74f2d345.htm",
 79200|       "dll_signature_verified": true,
 79201|       "dll_relationship_scope": "declared",
 79202|       "dll_semantic_verified": null,
 79203|       "dll_verified_status": "signature_verified_declared",
 79204|       "revitlookup_referenced": null,
 79205|       "revitlookup_requires_document_context": null
 79206|     },
 79207|     {
 79208|       "source": "Autodesk.Revit.DB.Structure.PathReinforcement",
 79209|       "target": null,
 79210|       "member_name": "ConvertRebarInSystemToRebars",
 79211|       "member_kind": "method",
 79212|       "edge_type": "RETURNS_ELEMENT_IDS",
 79213|       "confidence": "unknown_reference",
 79214|       "confidence_tier": "unverified_reference",
 79215|       "target_resolution": "none",
 79216|       "evidence": [
 79217|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 79218|       ],
 79219|       "source_url": "https://www.revitapidocs.com/2025/860c0773-13e2-00be-eeb8-25afa57fe2be.htm",
 79220|       "dll_signature_verified": true,
 79221|       "dll_relationship_scope": "declared",
 79222|       "dll_semantic_verified": null,
 79223|       "dll_verified_status": "signature_verified_declared",
 79224|       "revitlookup_referenced": null,
 79225|       "revitlookup_requires_document_context": null
 79226|     },
 79227|     {
 79228|       "source": "Autodesk.Revit.DB.Structure.PathReinforcement",
 79229|       "target": null,
 79230|       "member_name": "GetCurveElementIds",
 79231|       "member_kind": "method",
 79232|       "edge_type": "RETURNS_ELEMENT_IDS",
 79233|       "confidence": "unknown_reference",
 79234|       "confidence_tier": "unverified_reference",
 79235|       "target_resolution": "none",
 79236|       "evidence": [
 79237|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 79238|       ],
 79239|       "source_url": "https://www.revitapidocs.com/2025/6a17f4fa-5d3a-2e3b-ce1c-fefa0dd941dd.htm",
 79240|       "dll_signature_verified": true,
 79241|       "dll_relationship_scope": "declared",
 79242|       "dll_semantic_verified": null,
 79243|       "dll_verified_status": "signature_verified_declared",
 79244|       "revitlookup_referenced": null,
 79245|       "revitlookup_requires_document_context": null
 79246|     },
 79247|     {
 79248|       "source": "Autodesk.Revit.DB.Structure.PathReinforcement",
 79249|       "target": null,
 79250|       "member_name": "GetHostId",
 79251|       "member_kind": "method",
 79252|       "edge_type": "HOSTED_BY",
 79253|       "confidence": "elementid_with_strong_name",
 79254|       "confidence_tier": "core",
 79255|       "target_resolution": "none",
 79256|       "evidence": [
 79257|         "member name 'GetHostId' matches keyword pattern /^GetHosted|Host/"
 79258|       ],
 79259|       "source_url": "https://www.revitapidocs.com/2025/61909903-e670-132a-cd41-07f85ff6302b.htm",
 79260|       "dll_signature_verified": true,
 79261|       "dll_relationship_scope": "declared",
 79262|       "dll_semantic_verified": null,
 79263|       "dll_verified_status": "signature_verified_declared",
 79264|       "revitlookup_referenced": null,
 79265|       "revitlookup_requires_document_context": null
 79266|     },
 79267|     {
 79268|       "source": "Autodesk.Revit.DB.Structure.PathReinforcement",
 79269|       "target": null,
 79270|       "member_name": "GetOrCreateDefaultRebarShape",
 79271|       "member_kind": "method",
 79272|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 79273|       "confidence": "unknown_reference",
 79274|       "confidence_tier": "unverified_reference",
 79275|       "target_resolution": "none",
 79276|       "evidence": [
 79277|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 79278|       ],
 79279|       "source_url": "https://www.revitapidocs.com/2025/4db777aa-4687-e1c1-3104-0fa1c8d9e576.htm",
 79280|       "dll_signature_verified": true,
 79281|       "dll_relationship_scope": "declared",
 79282|       "dll_semantic_verified": null,
 79283|       "dll_verified_status": "signature_verified_declared",
 79284|       "revitlookup_referenced": null,
 79285|       "revitlookup_requires_document_context": null
 79286|     },
 79287|     {
 79288|       "source": "Autodesk.Revit.DB.Structure.PathReinforcement",
 79289|       "target": null,
 79290|       "member_name": "GetRebarInSystemIds",
 79291|       "member_kind": "method",
 79292|       "edge_type": "RETURNS_ELEMENT_IDS",
 79293|       "confidence": "unknown_reference",
 79294|       "confidence_tier": "unverified_reference",
 79295|       "target_resolution": "none",
 79296|       "evidence": [
 79297|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 79298|       ],
 79299|       "source_url": "https://www.revitapidocs.com/2025/66d7ac46-c022-c65a-0c18-4fdedb77c5f6.htm",
 79300|       "dll_signature_verified": true,
 79301|       "dll_relationship_scope": "declared",
 79302|       "dll_semantic_verified": null,
 79303|       "dll_verified_status": "signature_verified_declared",
 79304|       "revitlookup_referenced": null,
 79305|       "revitlookup_requires_document_context": null
 79306|     },
 79307|     {
 79308|       "source": "Autodesk.Revit.DB.Structure.PathReinforcement",
 79309|       "target": null,
 79310|       "member_name": "RemovePathReinforcementSystem",
 79311|       "member_kind": "method",
 79312|       "edge_type": "RETURNS_ELEMENT_IDS",
 79313|       "confidence": "unknown_reference",
 79314|       "confidence_tier": "unverified_reference",
 79315|       "target_resolution": "none",
 79316|       "evidence": [
 79317|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 79318|       ],
 79319|       "source_url": "https://www.revitapidocs.com/2025/f363e5b7-056b-4fa2-7b0f-388614effd85.htm",
 79320|       "dll_signature_verified": true,
 79321|       "dll_relationship_scope": "declared",
 79322|       "dll_semantic_verified": null,
 79323|       "dll_verified_status": "signature_verified_declared",
 79324|       "revitlookup_referenced": null,
 79325|       "revitlookup_requires_document_context": null
 79326|     },
 79327|     {
 79328|       "source": "Autodesk.Revit.DB.Structure.PointLoad",
 79329|       "target": null,
 79330|       "member_name": "IsPointInsideHostBoundaries",
 79331|       "member_kind": "method",
 79332|       "edge_type": "HOSTED_BY",
 79333|       "confidence": "name_only_candidate",
 79334|       "confidence_tier": "likely",
 79335|       "target_resolution": "none",
 79336|       "evidence": [
 79337|         "member name 'IsPointInsideHostBoundaries' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 79338|       ],
 79339|       "source_url": "https://www.revitapidocs.com/2025/994add06-c832-952e-38b0-1f9d30e51047.htm",
 79340|       "dll_signature_verified": true,
 79341|       "dll_relationship_scope": "declared",
 79342|       "dll_semantic_verified": null,
 79343|       "dll_verified_status": "signature_verified_declared",
 79344|       "revitlookup_referenced": null,
 79345|       "revitlookup_requires_document_context": null
 79346|     },
 79347|     {
 79348|       "source": "Autodesk.Revit.DB.Structure.PointLoad",
 79349|       "target": null,
 79350|       "member_name": "IsValidHostId",
 79351|       "member_kind": "method",
 79352|       "edge_type": "HOSTED_BY",
 79353|       "confidence": "name_only_candidate",
 79354|       "confidence_tier": "likely",
 79355|       "target_resolution": "none",
 79356|       "evidence": [
 79357|         "member name 'IsValidHostId' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 79358|       ],
 79359|       "source_url": "https://www.revitapidocs.com/2025/c5304968-914f-7e7e-ea26-ffd6e1dee6d5.htm",
 79360|       "dll_signature_verified": true,
 79361|       "dll_relationship_scope": "declared",
 79362|       "dll_semantic_verified": null,
 79363|       "dll_verified_status": "signature_verified_declared",
 79364|       "revitlookup_referenced": null,
 79365|       "revitlookup_requires_document_context": null
 79366|     },
 79367|     {
 79368|       "source": "Autodesk.Revit.DB.Structure.Rebar",
 79369|       "target": null,
 79370|       "member_name": "ReadOnlyParameters",
 79371|       "member_kind": "property",
 79372|       "edge_type": "HAS_PARAMETER",
 79373|       "confidence": "name_only_candidate",
 79374|       "confidence_tier": "likely",
 79375|       "target_resolution": "none",
 79376|       "evidence": [
 79377|         "member name 'ReadOnlyParameters' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 79378|       ],
 79379|       "source_url": "https://www.revitapidocs.com/2025/6e992635-8245-ac60-3514-ca02f6b8e85d.htm",
 79380|       "dll_signature_verified": true,
 79381|       "dll_relationship_scope": "declared",
 79382|       "dll_semantic_verified": null,
 79383|       "dll_verified_status": "signature_verified_declared",
 79384|       "revitlookup_referenced": null,
 79385|       "revitlookup_requires_document_context": null
 79386|     },
 79387|     {
 79388|       "source": "Autodesk.Revit.DB.Structure.Rebar",
 79389|       "target": null,
 79390|       "member_name": "GetAllRebarShapeIds",
 79391|       "member_kind": "method",
 79392|       "edge_type": "RETURNS_ELEMENT_IDS",
 79393|       "confidence": "elementid_collection_with_strong_name",
 79394|       "confidence_tier": "core",
 79395|       "target_resolution": "none",
 79396|       "evidence": [
 79397|         "member name 'GetAllRebarShapeIds' matches keyword pattern /^GetAll/"
 79398|       ],
 79399|       "source_url": "https://www.revitapidocs.com/2025/a4226864-acec-b0b9-ddb2-fae12b48f378.htm",
 79400|       "dll_signature_verified": true,
 79401|       "dll_relationship_scope": "declared",
 79402|       "dll_semantic_verified": null,
 79403|       "dll_verified_status": "signature_verified_declared",
 79404|       "revitlookup_referenced": null,
 79405|       "revitlookup_requires_document_context": null
 79406|     },
 79407|     {
 79408|       "source": "Autodesk.Revit.DB.Structure.Rebar",
 79409|       "target": "Autodesk.Revit.DB.Structure.RebarBendData",
 79410|       "member_name": "GetBendData",
 79411|       "member_kind": "method",
 79412|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 79413|       "confidence": "direct_return_type",
 79414|       "confidence_tier": "unverified_reference",
 79415|       "target_resolution": "short_name_fallback",
 79416|       "evidence": [
 79417|         "return type 'RebarBendData' directly names a Revit DB object type"
 79418|       ],
 79419|       "source_url": "https://www.revitapidocs.com/2025/e20b8d30-f5b8-bf5e-4df5-dbb7498e2ccc.htm",
 79420|       "dll_signature_verified": true,
 79421|       "dll_relationship_scope": "declared",
 79422|       "dll_semantic_verified": null,
 79423|       "dll_verified_status": "signature_verified_declared",
 79424|       "revitlookup_referenced": null,
 79425|       "revitlookup_requires_document_context": null
 79426|     },
 79427|     {
 79428|       "source": "Autodesk.Revit.DB.Structure.Rebar",
 79429|       "target": null,
 79430|       "member_name": "GetCouplerId",
 79431|       "member_kind": "method",
 79432|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 79433|       "confidence": "unknown_reference",
 79434|       "confidence_tier": "unverified_reference",
 79435|       "target_resolution": "none",
 79436|       "evidence": [
 79437|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 79438|       ],
 79439|       "source_url": "https://www.revitapidocs.com/2025/72b65231-27d4-79b2-1193-136bab814951.htm",
 79440|       "dll_signature_verified": true,
 79441|       "dll_relationship_scope": "declared",
 79442|       "dll_semantic_verified": null,
 79443|       "dll_verified_status": "signature_verified_declared",
 79444|       "revitlookup_referenced": null,
 79445|       "revitlookup_requires_document_context": null
 79446|     },
 79447|     {
 79448|       "source": "Autodesk.Revit.DB.Structure.Rebar",
 79449|       "target": null,
 79450|       "member_name": "GetEndTreatmentTypeId",
 79451|       "member_kind": "method",
 79452|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 79453|       "confidence": "unknown_reference",
 79454|       "confidence_tier": "unverified_reference",
 79455|       "target_resolution": "none",
 79456|       "evidence": [
 79457|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 79458|       ],
 79459|       "source_url": "https://www.revitapidocs.com/2025/3521d0c8-5746-6dde-4839-3e9a14dbd93e.htm",
 79460|       "dll_signature_verified": true,
 79461|       "dll_relationship_scope": "declared",
 79462|       "dll_semantic_verified": null,
 79463|       "dll_verified_status": "signature_verified_declared",
 79464|       "revitlookup_referenced": null,
 79465|       "revitlookup_requires_document_context": null
 79466|     },
 79467|     {
 79468|       "source": "Autodesk.Revit.DB.Structure.Rebar",
 79469|       "target": "Autodesk.Revit.DB.Structure.RebarFreeFormAccessor",
 79470|       "member_name": "GetFreeFormAccessor",
 79471|       "member_kind": "method",
 79472|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 79473|       "confidence": "direct_return_type",
 79474|       "confidence_tier": "unverified_reference",
 79475|       "target_resolution": "short_name_fallback",
 79476|       "evidence": [
 79477|         "return type 'RebarFreeFormAccessor' directly names a Revit DB object type"
 79478|       ],
 79479|       "source_url": "https://www.revitapidocs.com/2025/67be446c-e2e1-9dfe-315f-f5d6adc779b9.htm",
 79480|       "dll_signature_verified": true,
 79481|       "dll_relationship_scope": "declared",
 79482|       "dll_semantic_verified": null,
 79483|       "dll_verified_status": "signature_verified_declared",
 79484|       "revitlookup_referenced": null,
 79485|       "revitlookup_requires_document_context": null
 79486|     },
 79487|     {
 79488|       "source": "Autodesk.Revit.DB.Structure.Rebar",
 79489|       "target": null,
 79490|       "member_name": "GetHookTypeId",
 79491|       "member_kind": "method",
 79492|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 79493|       "confidence": "unknown_reference",
 79494|       "confidence_tier": "unverified_reference",
 79495|       "target_resolution": "none",
 79496|       "evidence": [
 79497|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 79498|       ],
 79499|       "source_url": "https://www.revitapidocs.com/2025/016d53d9-0ef5-99d1-b12f-089f04df3952.htm",
 79500|       "dll_signature_verified": true,
 79501|       "dll_relationship_scope": "declared",
 79502|       "dll_semantic_verified": null,
 79503|       "dll_verified_status": "signature_verified_declared",
 79504|       "revitlookup_referenced": null,
 79505|       "revitlookup_requires_document_context": null
 79506|     },
 79507|     {
 79508|       "source": "Autodesk.Revit.DB.Structure.Rebar",
 79509|       "target": null,
 79510|       "member_name": "GetHostId",
 79511|       "member_kind": "method",
 79512|       "edge_type": "HOSTED_BY",
 79513|       "confidence": "elementid_with_strong_name",
 79514|       "confidence_tier": "core",
 79515|       "target_resolution": "none",
 79516|       "evidence": [
 79517|         "member name 'GetHostId' matches keyword pattern /^GetHosted|Host/"
 79518|       ],
 79519|       "source_url": "https://www.revitapidocs.com/2025/aa67c490-8875-2756-c621-49484423d026.htm",
 79520|       "dll_signature_verified": true,
 79521|       "dll_relationship_scope": "declared",
 79522|       "dll_semantic_verified": null,
 79523|       "dll_verified_status": "signature_verified_declared",
 79524|       "revitlookup_referenced": null,
 79525|       "revitlookup_requires_document_context": null
 79526|     },
 79527|     {
 79528|       "source": "Autodesk.Revit.DB.Structure.Rebar",
 79529|       "target": null,
 79530|       "member_name": "GetOverridableHookParameters",
 79531|       "member_kind": "method",
 79532|       "edge_type": "HAS_PARAMETER",
 79533|       "confidence": "name_only_candidate",
 79534|       "confidence_tier": "likely",
 79535|       "target_resolution": "none",
 79536|       "evidence": [
 79537|         "member name 'GetOverridableHookParameters' matches keyword pattern /Parameter/ but return type 'void' gives no type-level confirmation"
 79538|       ],
 79539|       "source_url": "https://www.revitapidocs.com/2025/40de7723-ff71-2507-7369-56983b8d2842.htm",
 79540|       "dll_signature_verified": true,
 79541|       "dll_relationship_scope": "declared",
 79542|       "dll_semantic_verified": null,
 79543|       "dll_verified_status": "signature_verified_declared",
 79544|       "revitlookup_referenced": null,
 79545|       "revitlookup_requires_document_context": null
 79546|     },
 79547|     {
 79548|       "source": "Autodesk.Revit.DB.Structure.Rebar",
 79549|       "target": "Autodesk.Revit.DB.ParameterValue",
 79550|       "member_name": "GetParameterValueAtIndex",
 79551|       "member_kind": "method",
 79552|       "edge_type": "HAS_PARAMETER",
 79553|       "confidence": "direct_return_type",
 79554|       "confidence_tier": "core",
 79555|       "target_resolution": "exact",
 79556|       "evidence": [
 79557|         "return type 'ParameterValue' directly names a Revit DB object type"
 79558|       ],
 79559|       "source_url": "https://www.revitapidocs.com/2025/d4d5a126-4e14-8fda-bbb9-2178b7162486.htm",
 79560|       "dll_signature_verified": true,
 79561|       "dll_relationship_scope": "declared",
 79562|       "dll_semantic_verified": null,
 79563|       "dll_verified_status": "signature_verified_declared",
 79564|       "revitlookup_referenced": null,
 79565|       "revitlookup_requires_document_context": null
 79566|     },
 79567|     {
 79568|       "source": "Autodesk.Revit.DB.Structure.Rebar",
 79569|       "target": "Autodesk.Revit.DB.Structure.RebarConstraintsManager",
 79570|       "member_name": "GetRebarConstraintsManager",
```
