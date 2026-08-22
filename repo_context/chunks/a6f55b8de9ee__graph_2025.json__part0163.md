# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 163 of 216
- Original line range: 63181-63580
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 63181|       "target_resolution": "exact",
 63182|       "evidence": [
 63183|         "member name 'GetUnderlayTopLevel' matches keyword pattern /Level/"
 63184|       ],
 63185|       "source_url": "https://www.revitapidocs.com/2025/5d401ec0-ead1-39bd-459b-2dca075b2797.htm",
 63186|       "dll_signature_verified": true,
 63187|       "dll_relationship_scope": "declared",
 63188|       "dll_semantic_verified": null,
 63189|       "dll_verified_status": "signature_verified_declared",
 63190|       "revitlookup_referenced": null,
 63191|       "revitlookup_requires_document_context": null
 63192|     },
 63193|     {
 63194|       "source": "Autodesk.Revit.DB.ViewPlan",
 63195|       "target": "Autodesk.Revit.DB.PlanViewRange",
 63196|       "member_name": "GetViewRange",
 63197|       "member_kind": "method",
 63198|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 63199|       "confidence": "direct_return_type",
 63200|       "confidence_tier": "unverified_reference",
 63201|       "target_resolution": "exact",
 63202|       "evidence": [
 63203|         "return type 'PlanViewRange' directly names a Revit DB object type"
 63204|       ],
 63205|       "source_url": "https://www.revitapidocs.com/2025/0a8b7c58-406d-b801-5921-8b23568806be.htm",
 63206|       "dll_signature_verified": true,
 63207|       "dll_relationship_scope": "declared",
 63208|       "dll_semantic_verified": null,
 63209|       "dll_verified_status": "signature_verified_declared",
 63210|       "revitlookup_referenced": null,
 63211|       "revitlookup_requires_document_context": null
 63212|     },
 63213|     {
 63214|       "source": "Autodesk.Revit.DB.ViewPlan",
 63215|       "target": "Autodesk.Revit.DB.Level",
 63216|       "member_name": "SetUnderlayBaseLevel",
 63217|       "member_kind": "method",
 63218|       "edge_type": "ASSIGNED_TO_LEVEL",
 63219|       "confidence": "name_only_candidate",
 63220|       "confidence_tier": "likely",
 63221|       "target_resolution": "exact",
 63222|       "evidence": [
 63223|         "member name 'SetUnderlayBaseLevel' matches keyword pattern /Level/ but return type 'void' gives no type-level confirmation"
 63224|       ],
 63225|       "source_url": "https://www.revitapidocs.com/2025/68a95ba8-b6ff-6275-eb2b-2b54fe6d9e62.htm",
 63226|       "dll_signature_verified": true,
 63227|       "dll_relationship_scope": "declared",
 63228|       "dll_semantic_verified": null,
 63229|       "dll_verified_status": "signature_verified_declared",
 63230|       "revitlookup_referenced": null,
 63231|       "revitlookup_requires_document_context": null
 63232|     },
 63233|     {
 63234|       "source": "Autodesk.Revit.DB.Viewport",
 63235|       "target": "Autodesk.Revit.DB.ViewSheet",
 63236|       "member_name": "SheetId",
 63237|       "member_kind": "property",
 63238|       "edge_type": "PLACED_ON_SHEET",
 63239|       "confidence": "elementid_with_strong_name",
 63240|       "confidence_tier": "core",
 63241|       "target_resolution": "exact",
 63242|       "evidence": [
 63243|         "member name 'SheetId' matches keyword pattern /Sheet/"
 63244|       ],
 63245|       "source_url": "https://www.revitapidocs.com/2025/187ece0f-f732-ec3c-1f20-ad688b8c1499.htm",
 63246|       "dll_signature_verified": true,
 63247|       "dll_relationship_scope": "declared",
 63248|       "dll_semantic_verified": null,
 63249|       "dll_verified_status": "signature_verified_declared",
 63250|       "revitlookup_referenced": null,
 63251|       "revitlookup_requires_document_context": null
 63252|     },
 63253|     {
 63254|       "source": "Autodesk.Revit.DB.Viewport",
 63255|       "target": "Autodesk.Revit.DB.View",
 63256|       "member_name": "ViewId",
 63257|       "member_kind": "property",
 63258|       "edge_type": "REFERENCES",
 63259|       "confidence": "elementid_with_strong_name",
 63260|       "confidence_tier": "core",
 63261|       "target_resolution": "exact",
 63262|       "evidence": [
 63263|         "member name 'ViewId' matches keyword pattern /^(Get)?ViewId$/"
 63264|       ],
 63265|       "source_url": "https://www.revitapidocs.com/2025/f96f5255-17b5-d4fc-c3a3-049fae1f6eb9.htm",
 63266|       "dll_signature_verified": true,
 63267|       "dll_relationship_scope": "declared",
 63268|       "dll_semantic_verified": null,
 63269|       "dll_verified_status": "signature_verified_declared",
 63270|       "revitlookup_referenced": null,
 63271|       "revitlookup_requires_document_context": null
 63272|     },
 63273|     {
 63274|       "source": "Autodesk.Revit.DB.Viewport",
 63275|       "target": "Autodesk.Revit.DB.ViewSheet",
 63276|       "member_name": "CanAddViewToSheet",
 63277|       "member_kind": "method",
 63278|       "edge_type": "PLACED_ON_SHEET",
 63279|       "confidence": "name_only_candidate",
 63280|       "confidence_tier": "likely",
 63281|       "target_resolution": "exact",
 63282|       "evidence": [
 63283|         "member name 'CanAddViewToSheet' matches keyword pattern /Sheet/ but return type 'bool' gives no type-level confirmation"
 63284|       ],
 63285|       "source_url": "https://www.revitapidocs.com/2025/1da33e38-255f-a27a-0626-9891d897b29a.htm",
 63286|       "dll_signature_verified": true,
 63287|       "dll_relationship_scope": "declared",
 63288|       "dll_semantic_verified": null,
 63289|       "dll_verified_status": "signature_verified_declared",
 63290|       "revitlookup_referenced": null,
 63291|       "revitlookup_requires_document_context": null
 63292|     },
 63293|     {
 63294|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63295|       "target": null,
 63296|       "member_name": "BodyTextTypeId",
 63297|       "member_kind": "property",
 63298|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 63299|       "confidence": "unknown_reference",
 63300|       "confidence_tier": "unverified_reference",
 63301|       "target_resolution": "none",
 63302|       "evidence": [
 63303|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 63304|       ],
 63305|       "source_url": "https://www.revitapidocs.com/2025/980d0623-f826-bfee-7f68-78d1db70d663.htm",
 63306|       "dll_signature_verified": true,
 63307|       "dll_relationship_scope": "declared",
 63308|       "dll_semantic_verified": null,
 63309|       "dll_verified_status": "signature_verified_declared",
 63310|       "revitlookup_referenced": null,
 63311|       "revitlookup_requires_document_context": null
 63312|     },
 63313|     {
 63314|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63315|       "target": "Autodesk.Revit.DB.ScheduleDefinition",
 63316|       "member_name": "Definition",
 63317|       "member_kind": "property",
 63318|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 63319|       "confidence": "direct_return_type",
 63320|       "confidence_tier": "unverified_reference",
 63321|       "target_resolution": "exact",
 63322|       "evidence": [
 63323|         "return type 'ScheduleDefinition' directly names a Revit DB object type"
 63324|       ],
 63325|       "source_url": "https://www.revitapidocs.com/2025/fe88f8dc-4926-4ff4-4490-7aee58d5a3f4.htm",
 63326|       "dll_signature_verified": true,
 63327|       "dll_relationship_scope": "declared",
 63328|       "dll_semantic_verified": null,
 63329|       "dll_verified_status": "signature_verified_declared",
 63330|       "revitlookup_referenced": null,
 63331|       "revitlookup_requires_document_context": null
 63332|     },
 63333|     {
 63334|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63335|       "target": "Autodesk.Revit.DB.ScheduleDefinition",
 63336|       "member_name": "EmbeddedDefinition",
 63337|       "member_kind": "property",
 63338|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 63339|       "confidence": "direct_return_type",
 63340|       "confidence_tier": "unverified_reference",
 63341|       "target_resolution": "exact",
 63342|       "evidence": [
 63343|         "return type 'ScheduleDefinition' directly names a Revit DB object type"
 63344|       ],
 63345|       "source_url": "https://www.revitapidocs.com/2025/d844369e-1bf1-28af-8d93-80a7e3f19cf8.htm",
 63346|       "dll_signature_verified": true,
 63347|       "dll_relationship_scope": "declared",
 63348|       "dll_semantic_verified": null,
 63349|       "dll_verified_status": "signature_verified_declared",
 63350|       "revitlookup_referenced": null,
 63351|       "revitlookup_requires_document_context": null
 63352|     },
 63353|     {
 63354|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63355|       "target": null,
 63356|       "member_name": "HeaderTextTypeId",
 63357|       "member_kind": "property",
 63358|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 63359|       "confidence": "unknown_reference",
 63360|       "confidence_tier": "unverified_reference",
 63361|       "target_resolution": "none",
 63362|       "evidence": [
 63363|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 63364|       ],
 63365|       "source_url": "https://www.revitapidocs.com/2025/615568b8-7f0e-7680-7560-e5e475220396.htm",
 63366|       "dll_signature_verified": true,
 63367|       "dll_relationship_scope": "declared",
 63368|       "dll_semantic_verified": null,
 63369|       "dll_verified_status": "signature_verified_declared",
 63370|       "revitlookup_referenced": null,
 63371|       "revitlookup_requires_document_context": null
 63372|     },
 63373|     {
 63374|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63375|       "target": null,
 63376|       "member_name": "KeyScheduleParameterName",
 63377|       "member_kind": "property",
 63378|       "edge_type": "HAS_PARAMETER",
 63379|       "confidence": "name_only_candidate",
 63380|       "confidence_tier": "likely",
 63381|       "target_resolution": "none",
 63382|       "evidence": [
 63383|         "member name 'KeyScheduleParameterName' matches keyword pattern /Parameter/ but return type 'string' gives no type-level confirmation"
 63384|       ],
 63385|       "source_url": "https://www.revitapidocs.com/2025/c6fce00e-ec7c-dccb-7dcc-7297ab6368a9.htm",
 63386|       "dll_signature_verified": true,
 63387|       "dll_relationship_scope": "declared",
 63388|       "dll_semantic_verified": null,
 63389|       "dll_verified_status": "signature_verified_declared",
 63390|       "revitlookup_referenced": null,
 63391|       "revitlookup_requires_document_context": null
 63392|     },
 63393|     {
 63394|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63395|       "target": null,
 63396|       "member_name": "TitleTextTypeId",
 63397|       "member_kind": "property",
 63398|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 63399|       "confidence": "unknown_reference",
 63400|       "confidence_tier": "unverified_reference",
 63401|       "target_resolution": "none",
 63402|       "evidence": [
 63403|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 63404|       ],
 63405|       "source_url": "https://www.revitapidocs.com/2025/5dc9d891-f903-edac-33fa-30dda043b711.htm",
 63406|       "dll_signature_verified": true,
 63407|       "dll_relationship_scope": "declared",
 63408|       "dll_semantic_verified": null,
 63409|       "dll_verified_status": "signature_verified_declared",
 63410|       "revitlookup_referenced": null,
 63411|       "revitlookup_requires_document_context": null
 63412|     },
 63413|     {
 63414|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63415|       "target": "Autodesk.Revit.DB.ViewSheet",
 63416|       "member_name": "UseStripedRowsOnSheets",
 63417|       "member_kind": "property",
 63418|       "edge_type": "PLACED_ON_SHEET",
 63419|       "confidence": "name_only_candidate",
 63420|       "confidence_tier": "likely",
 63421|       "target_resolution": "exact",
 63422|       "evidence": [
 63423|         "member name 'UseStripedRowsOnSheets' matches keyword pattern /Sheet/ but return type 'bool' gives no type-level confirmation"
 63424|       ],
 63425|       "source_url": "https://www.revitapidocs.com/2025/3ffca97c-3963-1d68-0775-fbab3644856e.htm",
 63426|       "dll_signature_verified": true,
 63427|       "dll_relationship_scope": "declared",
 63428|       "dll_semantic_verified": null,
 63429|       "dll_verified_status": "signature_verified_declared",
 63430|       "revitlookup_referenced": null,
 63431|       "revitlookup_requires_document_context": null
 63432|     },
 63433|     {
 63434|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63435|       "target": null,
 63436|       "member_name": "CanGroupHeaders",
 63437|       "member_kind": "method",
 63438|       "edge_type": "MEMBER_OF_GROUP",
 63439|       "confidence": "name_only_candidate",
 63440|       "confidence_tier": "likely",
 63441|       "target_resolution": "none",
 63442|       "evidence": [
 63443|         "member name 'CanGroupHeaders' matches keyword pattern /^GetMember|Group/ but return type 'bool' gives no type-level confirmation"
 63444|       ],
 63445|       "source_url": "https://www.revitapidocs.com/2025/509911b4-ea7b-535d-f5f6-3b377c292e4c.htm",
 63446|       "dll_signature_verified": true,
 63447|       "dll_relationship_scope": "declared",
 63448|       "dll_semantic_verified": null,
 63449|       "dll_verified_status": "signature_verified_declared",
 63450|       "revitlookup_referenced": null,
 63451|       "revitlookup_requires_document_context": null
 63452|     },
 63453|     {
 63454|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63455|       "target": null,
 63456|       "member_name": "CanUngroupHeaders",
 63457|       "member_kind": "method",
 63458|       "edge_type": "MEMBER_OF_GROUP",
 63459|       "confidence": "name_only_candidate",
 63460|       "confidence_tier": "likely",
 63461|       "target_resolution": "none",
 63462|       "evidence": [
 63463|         "member name 'CanUngroupHeaders' matches keyword pattern /^GetMember|Group/ but return type 'bool' gives no type-level confirmation"
 63464|       ],
 63465|       "source_url": "https://www.revitapidocs.com/2025/2c5cbf50-8d29-a5d2-103d-06621e08da0b.htm",
 63466|       "dll_signature_verified": true,
 63467|       "dll_relationship_scope": "declared",
 63468|       "dll_semantic_verified": null,
 63469|       "dll_verified_status": "signature_verified_declared",
 63470|       "revitlookup_referenced": null,
 63471|       "revitlookup_requires_document_context": null
 63472|     },
 63473|     {
 63474|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63475|       "target": "Autodesk.Revit.DB.Material",
 63476|       "member_name": "GetDefaultNameForMaterialTakeoff",
 63477|       "member_kind": "method",
 63478|       "edge_type": "USES_MATERIAL",
 63479|       "confidence": "name_only_candidate",
 63480|       "confidence_tier": "likely",
 63481|       "target_resolution": "exact",
 63482|       "evidence": [
 63483|         "member name 'GetDefaultNameForMaterialTakeoff' matches keyword pattern /Material/ but return type 'string' gives no type-level confirmation"
 63484|       ],
 63485|       "source_url": "https://www.revitapidocs.com/2025/9c91593a-2e48-f9af-0ee4-da8c7699b9ba.htm",
 63486|       "dll_signature_verified": true,
 63487|       "dll_relationship_scope": "declared",
 63488|       "dll_semantic_verified": null,
 63489|       "dll_verified_status": "signature_verified_declared",
 63490|       "revitlookup_referenced": true,
 63491|       "revitlookup_requires_document_context": false
 63492|     },
 63493|     {
 63494|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63495|       "target": "Autodesk.Revit.DB.ViewSheet",
 63496|       "member_name": "GetDefaultNameForSheetList",
 63497|       "member_kind": "method",
 63498|       "edge_type": "PLACED_ON_SHEET",
 63499|       "confidence": "name_only_candidate",
 63500|       "confidence_tier": "likely",
 63501|       "target_resolution": "exact",
 63502|       "evidence": [
 63503|         "member name 'GetDefaultNameForSheetList' matches keyword pattern /Sheet/ but return type 'string' gives no type-level confirmation"
 63504|       ],
 63505|       "source_url": "https://www.revitapidocs.com/2025/523987e1-4fcc-5a11-9cf8-2c2bbcaa62a0.htm",
 63506|       "dll_signature_verified": true,
 63507|       "dll_relationship_scope": "declared",
 63508|       "dll_semantic_verified": null,
 63509|       "dll_verified_status": "signature_verified_declared",
 63510|       "revitlookup_referenced": true,
 63511|       "revitlookup_requires_document_context": false
 63512|     },
 63513|     {
 63514|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63515|       "target": null,
 63516|       "member_name": "GetDefaultParameterNameForKeySchedule",
 63517|       "member_kind": "method",
 63518|       "edge_type": "HAS_PARAMETER",
 63519|       "confidence": "name_only_candidate",
 63520|       "confidence_tier": "likely",
 63521|       "target_resolution": "none",
 63522|       "evidence": [
 63523|         "member name 'GetDefaultParameterNameForKeySchedule' matches keyword pattern /Parameter/ but return type 'string' gives no type-level confirmation"
 63524|       ],
 63525|       "source_url": "https://www.revitapidocs.com/2025/70f24220-ca7b-6202-5167-1f8ca618b20b.htm",
 63526|       "dll_signature_verified": true,
 63527|       "dll_relationship_scope": "declared",
 63528|       "dll_semantic_verified": null,
 63529|       "dll_verified_status": "signature_verified_declared",
 63530|       "revitlookup_referenced": true,
 63531|       "revitlookup_requires_document_context": false
 63532|     },
 63533|     {
 63534|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63535|       "target": "Autodesk.Revit.DB.ScheduleHeightsOnSheet",
 63536|       "member_name": "GetScheduleHeightsOnSheet",
 63537|       "member_kind": "method",
 63538|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 63539|       "confidence": "direct_return_type",
 63540|       "confidence_tier": "unverified_reference",
 63541|       "target_resolution": "exact",
 63542|       "evidence": [
 63543|         "member name 'GetScheduleHeightsOnSheet' matches keyword pattern /Sheet/ implying target 'ViewSheet', but the actual return type 'ScheduleHeightsOnSheet' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 63544|         "return type 'ScheduleHeightsOnSheet' directly names a Revit DB object type"
 63545|       ],
 63546|       "source_url": "https://www.revitapidocs.com/2025/6e9d5114-0578-8ccd-9550-2264c6ae6737.htm",
 63547|       "dll_signature_verified": true,
 63548|       "dll_relationship_scope": "declared",
 63549|       "dll_semantic_verified": null,
 63550|       "dll_verified_status": "signature_verified_declared",
 63551|       "revitlookup_referenced": null,
 63552|       "revitlookup_requires_document_context": null
 63553|     },
 63554|     {
 63555|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63556|       "target": null,
 63557|       "member_name": "GetScheduleInstances",
 63558|       "member_kind": "method",
 63559|       "edge_type": "RETURNS_ELEMENT_IDS",
 63560|       "confidence": "unknown_reference",
 63561|       "confidence_tier": "unverified_reference",
 63562|       "target_resolution": "none",
 63563|       "evidence": [
 63564|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 63565|       ],
 63566|       "source_url": "https://www.revitapidocs.com/2025/58b62eb9-893a-56d3-f689-c7837a50ab02.htm",
 63567|       "dll_signature_verified": true,
 63568|       "dll_relationship_scope": "declared",
 63569|       "dll_semantic_verified": null,
 63570|       "dll_verified_status": "signature_verified_declared",
 63571|       "revitlookup_referenced": true,
 63572|       "revitlookup_requires_document_context": false
 63573|     },
 63574|     {
 63575|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63576|       "target": "Autodesk.Revit.DB.TableData",
 63577|       "member_name": "GetTableData",
 63578|       "member_kind": "method",
 63579|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 63580|       "confidence": "direct_return_type",
```
