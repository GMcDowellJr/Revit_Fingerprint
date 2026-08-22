# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 184 of 216
- Original line range: 71371-71770
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 71371|       "evidence": [
 71372|         "member name 'TrueLoadPhaseC' matches keyword pattern /Phase/ but return type 'double' gives no type-level confirmation"
 71373|       ],
 71374|       "source_url": "https://www.revitapidocs.com/2025/4ce9af8d-7da1-cc2d-b56c-526fb60c8e4c.htm",
 71375|       "dll_signature_verified": true,
 71376|       "dll_relationship_scope": "declared",
 71377|       "dll_semantic_verified": null,
 71378|       "dll_verified_status": "signature_verified_declared",
 71379|       "revitlookup_referenced": null,
 71380|       "revitlookup_requires_document_context": null
 71381|     },
 71382|     {
 71383|       "source": "Autodesk.Revit.DB.Electrical.ElectricalSystem",
 71384|       "target": null,
 71385|       "member_name": "Voltage",
 71386|       "member_kind": "property",
 71387|       "edge_type": "TAGS_ELEMENT",
 71388|       "confidence": "name_only_candidate",
 71389|       "confidence_tier": "likely",
 71390|       "target_resolution": "none",
 71391|       "evidence": [
 71392|         "member name 'Voltage' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'double' gives no type-level confirmation"
 71393|       ],
 71394|       "source_url": "https://www.revitapidocs.com/2025/f0c134b8-7a0f-ebf0-48d1-e6d1ca03aad4.htm",
 71395|       "dll_signature_verified": true,
 71396|       "dll_relationship_scope": "declared",
 71397|       "dll_semantic_verified": null,
 71398|       "dll_verified_status": "signature_verified_declared",
 71399|       "revitlookup_referenced": null,
 71400|       "revitlookup_requires_document_context": null
 71401|     },
 71402|     {
 71403|       "source": "Autodesk.Revit.DB.Electrical.ElectricalSystem",
 71404|       "target": null,
 71405|       "member_name": "VoltageDrop",
 71406|       "member_kind": "property",
 71407|       "edge_type": "TAGS_ELEMENT",
 71408|       "confidence": "name_only_candidate",
 71409|       "confidence_tier": "likely",
 71410|       "target_resolution": "none",
 71411|       "evidence": [
 71412|         "member name 'VoltageDrop' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'double' gives no type-level confirmation"
 71413|       ],
 71414|       "source_url": "https://www.revitapidocs.com/2025/2f7d179a-a6c3-375b-d184-cc796e918f5d.htm",
 71415|       "dll_signature_verified": true,
 71416|       "dll_relationship_scope": "declared",
 71417|       "dll_semantic_verified": null,
 71418|       "dll_verified_status": "signature_verified_declared",
 71419|       "revitlookup_referenced": null,
 71420|       "revitlookup_requires_document_context": null
 71421|     },
 71422|     {
 71423|       "source": "Autodesk.Revit.DB.Electrical.ElectricalSystem",
 71424|       "target": "Autodesk.Revit.DB.Electrical.WireType",
 71425|       "member_name": "WireType",
 71426|       "member_kind": "property",
 71427|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 71428|       "confidence": "direct_return_type",
 71429|       "confidence_tier": "unverified_reference",
 71430|       "target_resolution": "short_name_fallback",
 71431|       "evidence": [
 71432|         "return type 'WireType' directly names a Revit DB object type"
 71433|       ],
 71434|       "source_url": "https://www.revitapidocs.com/2025/d605965d-bbed-c3c9-14fa-2d040ec76dca.htm",
 71435|       "dll_signature_verified": true,
 71436|       "dll_relationship_scope": "declared",
 71437|       "dll_semantic_verified": null,
 71438|       "dll_verified_status": "signature_verified_declared",
 71439|       "revitlookup_referenced": null,
 71440|       "revitlookup_requires_document_context": null
 71441|     },
 71442|     {
 71443|       "source": "Autodesk.Revit.DB.Electrical.ElectricalSystem",
 71444|       "target": "Autodesk.Revit.DB.Electrical.WireSet",
 71445|       "member_name": "NewWires",
 71446|       "member_kind": "method",
 71447|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 71448|       "confidence": "direct_return_type",
 71449|       "confidence_tier": "unverified_reference",
 71450|       "target_resolution": "short_name_fallback",
 71451|       "evidence": [
 71452|         "return type 'WireSet' directly names a Revit DB object type"
 71453|       ],
 71454|       "source_url": "https://www.revitapidocs.com/2025/e4aeb633-5e67-955f-dde6-6c5f36cd0edc.htm",
 71455|       "dll_signature_verified": true,
 71456|       "dll_relationship_scope": "declared",
 71457|       "dll_semantic_verified": null,
 71458|       "dll_verified_status": "signature_verified_declared",
 71459|       "revitlookup_referenced": null,
 71460|       "revitlookup_requires_document_context": null
 71461|     },
 71462|     {
 71463|       "source": "Autodesk.Revit.DB.Electrical.GroundConductorSize",
 71464|       "target": "Autodesk.Revit.DB.Electrical.WireMaterialType",
 71465|       "member_name": "MaterialBelongTo",
 71466|       "member_kind": "property",
 71467|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 71468|       "confidence": "direct_return_type",
 71469|       "confidence_tier": "unverified_reference",
 71470|       "target_resolution": "short_name_fallback",
 71471|       "evidence": [
 71472|         "member name 'MaterialBelongTo' matches keyword pattern /Material/ implying target 'Material', but the actual return type 'WireMaterialType' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 71473|         "return type 'WireMaterialType' directly names a Revit DB object type"
 71474|       ],
 71475|       "source_url": "https://www.revitapidocs.com/2025/1a5ce82b-7509-7045-dca8-558f1ec98cb2.htm",
 71476|       "dll_signature_verified": true,
 71477|       "dll_relationship_scope": "declared",
 71478|       "dll_semantic_verified": null,
 71479|       "dll_verified_status": "signature_verified_declared",
 71480|       "revitlookup_referenced": null,
 71481|       "revitlookup_requires_document_context": null
 71482|     },
 71483|     {
 71484|       "source": "Autodesk.Revit.DB.Electrical.GroundConductorSizeSet",
 71485|       "target": "Autodesk.Revit.DB.Electrical.GroundConductorSizeSetIterator",
 71486|       "member_name": "ForwardIterator",
 71487|       "member_kind": "method",
 71488|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 71489|       "confidence": "direct_return_type",
 71490|       "confidence_tier": "unverified_reference",
 71491|       "target_resolution": "short_name_fallback",
 71492|       "evidence": [
 71493|         "return type 'GroundConductorSizeSetIterator' directly names a Revit DB object type"
 71494|       ],
 71495|       "source_url": "https://www.revitapidocs.com/2025/3a872707-d9e4-646d-51f3-bf769e6a8c5f.htm",
 71496|       "dll_signature_verified": true,
 71497|       "dll_relationship_scope": "declared",
 71498|       "dll_semantic_verified": null,
 71499|       "dll_verified_status": "signature_verified_declared",
 71500|       "revitlookup_referenced": null,
 71501|       "revitlookup_requires_document_context": null
 71502|     },
 71503|     {
 71504|       "source": "Autodesk.Revit.DB.Electrical.GroundConductorSizeSet",
 71505|       "target": "Autodesk.Revit.DB.Electrical.GroundConductorSizeSetIterator",
 71506|       "member_name": "ReverseIterator",
 71507|       "member_kind": "method",
 71508|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 71509|       "confidence": "direct_return_type",
 71510|       "confidence_tier": "unverified_reference",
 71511|       "target_resolution": "short_name_fallback",
 71512|       "evidence": [
 71513|         "return type 'GroundConductorSizeSetIterator' directly names a Revit DB object type"
 71514|       ],
 71515|       "source_url": "https://www.revitapidocs.com/2025/68024a1c-b37d-5e71-7ba9-e470fd9ea48f.htm",
 71516|       "dll_signature_verified": true,
 71517|       "dll_relationship_scope": "declared",
 71518|       "dll_semantic_verified": null,
 71519|       "dll_verified_status": "signature_verified_declared",
 71520|       "revitlookup_referenced": null,
 71521|       "revitlookup_requires_document_context": null
 71522|     },
 71523|     {
 71524|       "source": "Autodesk.Revit.DB.Electrical.InsulationTypeSet",
 71525|       "target": "Autodesk.Revit.DB.Electrical.InsulationTypeSetIterator",
 71526|       "member_name": "ForwardIterator",
 71527|       "member_kind": "method",
 71528|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 71529|       "confidence": "direct_return_type",
 71530|       "confidence_tier": "unverified_reference",
 71531|       "target_resolution": "short_name_fallback",
 71532|       "evidence": [
 71533|         "return type 'InsulationTypeSetIterator' directly names a Revit DB object type"
 71534|       ],
 71535|       "source_url": "https://www.revitapidocs.com/2025/0587e2c2-e4d9-4e30-26bf-efd92c1073a2.htm",
 71536|       "dll_signature_verified": true,
 71537|       "dll_relationship_scope": "declared",
 71538|       "dll_semantic_verified": null,
 71539|       "dll_verified_status": "signature_verified_declared",
 71540|       "revitlookup_referenced": null,
 71541|       "revitlookup_requires_document_context": null
 71542|     },
 71543|     {
 71544|       "source": "Autodesk.Revit.DB.Electrical.InsulationTypeSet",
 71545|       "target": "Autodesk.Revit.DB.Electrical.InsulationTypeSetIterator",
 71546|       "member_name": "ReverseIterator",
 71547|       "member_kind": "method",
 71548|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 71549|       "confidence": "direct_return_type",
 71550|       "confidence_tier": "unverified_reference",
 71551|       "target_resolution": "short_name_fallback",
 71552|       "evidence": [
 71553|         "return type 'InsulationTypeSetIterator' directly names a Revit DB object type"
 71554|       ],
 71555|       "source_url": "https://www.revitapidocs.com/2025/9d263b9b-8aef-b9fb-9331-5b94713b07db.htm",
 71556|       "dll_signature_verified": true,
 71557|       "dll_relationship_scope": "declared",
 71558|       "dll_semantic_verified": null,
 71559|       "dll_verified_status": "signature_verified_declared",
 71560|       "revitlookup_referenced": null,
 71561|       "revitlookup_requires_document_context": null
 71562|     },
 71563|     {
 71564|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleData",
 71565|       "target": null,
 71566|       "member_name": "BorderAroundSchedule",
 71567|       "member_kind": "property",
 71568|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 71569|       "confidence": "unknown_reference",
 71570|       "confidence_tier": "unverified_reference",
 71571|       "target_resolution": "none",
 71572|       "evidence": [
 71573|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 71574|       ],
 71575|       "source_url": "https://www.revitapidocs.com/2025/4e87fd46-6762-6ec0-5bf9-896825205572.htm",
 71576|       "dll_signature_verified": true,
 71577|       "dll_relationship_scope": "declared",
 71578|       "dll_semantic_verified": null,
 71579|       "dll_verified_status": "signature_verified_declared",
 71580|       "revitlookup_referenced": null,
 71581|       "revitlookup_requires_document_context": null
 71582|     },
 71583|     {
 71584|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleData",
 71585|       "target": null,
 71586|       "member_name": "BorderAroundSections",
 71587|       "member_kind": "property",
 71588|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 71589|       "confidence": "unknown_reference",
 71590|       "confidence_tier": "unverified_reference",
 71591|       "target_resolution": "none",
 71592|       "evidence": [
 71593|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 71594|       ],
 71595|       "source_url": "https://www.revitapidocs.com/2025/ee887897-cd12-99ae-f6f3-a0cd96fe4818.htm",
 71596|       "dll_signature_verified": true,
 71597|       "dll_relationship_scope": "declared",
 71598|       "dll_semantic_verified": null,
 71599|       "dll_verified_status": "signature_verified_declared",
 71600|       "revitlookup_referenced": null,
 71601|       "revitlookup_requires_document_context": null
 71602|     },
 71603|     {
 71604|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleData",
 71605|       "target": "Autodesk.Revit.DB.Phase",
 71606|       "member_name": "IsPanelSinglePhase",
 71607|       "member_kind": "property",
 71608|       "edge_type": "ASSIGNED_TO_PHASE",
 71609|       "confidence": "name_only_candidate",
 71610|       "confidence_tier": "likely",
 71611|       "target_resolution": "exact",
 71612|       "evidence": [
 71613|         "member name 'IsPanelSinglePhase' matches keyword pattern /Phase/ but return type 'bool' gives no type-level confirmation"
 71614|       ],
 71615|       "source_url": "https://www.revitapidocs.com/2025/57c53807-579e-d23c-2e3c-bb08b5ca9440.htm",
 71616|       "dll_signature_verified": true,
 71617|       "dll_relationship_scope": "declared",
 71618|       "dll_semantic_verified": null,
 71619|       "dll_verified_status": "signature_verified_declared",
 71620|       "revitlookup_referenced": null,
 71621|       "revitlookup_requires_document_context": null
 71622|     },
 71623|     {
 71624|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleData",
 71625|       "target": "Autodesk.Revit.DB.Phase",
 71626|       "member_name": "IsUnusedPhaseHidden",
 71627|       "member_kind": "property",
 71628|       "edge_type": "ASSIGNED_TO_PHASE",
 71629|       "confidence": "name_only_candidate",
 71630|       "confidence_tier": "likely",
 71631|       "target_resolution": "exact",
 71632|       "evidence": [
 71633|         "member name 'IsUnusedPhaseHidden' matches keyword pattern /Phase/ but return type 'bool' gives no type-level confirmation"
 71634|       ],
 71635|       "source_url": "https://www.revitapidocs.com/2025/522a3c15-d46e-19dc-2a01-7482ba87d935.htm",
 71636|       "dll_signature_verified": true,
 71637|       "dll_relationship_scope": "declared",
 71638|       "dll_semantic_verified": null,
 71639|       "dll_verified_status": "signature_verified_declared",
 71640|       "revitlookup_referenced": null,
 71641|       "revitlookup_requires_document_context": null
 71642|     },
 71643|     {
 71644|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleData",
 71645|       "target": "Autodesk.Revit.DB.Phase",
 71646|       "member_name": "PhaseLoadType",
 71647|       "member_kind": "property",
 71648|       "edge_type": "ASSIGNED_TO_PHASE",
 71649|       "confidence": "name_only_candidate",
 71650|       "confidence_tier": "likely",
 71651|       "target_resolution": "exact",
 71652|       "evidence": [
 71653|         "member name 'PhaseLoadType' matches keyword pattern /Phase/ but return type 'PanelSchedulePhaseLoadType' gives no type-level confirmation"
 71654|       ],
 71655|       "source_url": "https://www.revitapidocs.com/2025/ccdab100-5dfb-10ff-1e7c-1910c049d0a3.htm",
 71656|       "dll_signature_verified": true,
 71657|       "dll_relationship_scope": "declared",
 71658|       "dll_semantic_verified": null,
 71659|       "dll_verified_status": "signature_verified_declared",
 71660|       "revitlookup_referenced": null,
 71661|       "revitlookup_requires_document_context": null
 71662|     },
 71663|     {
 71664|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleData",
 71665|       "target": "Autodesk.Revit.DB.Phase",
 71666|       "member_name": "PhasesAsCurrents",
 71667|       "member_kind": "property",
 71668|       "edge_type": "ASSIGNED_TO_PHASE",
 71669|       "confidence": "name_only_candidate",
 71670|       "confidence_tier": "likely",
 71671|       "target_resolution": "exact",
 71672|       "evidence": [
 71673|         "member name 'PhasesAsCurrents' matches keyword pattern /Phase/ but return type 'bool' gives no type-level confirmation"
 71674|       ],
 71675|       "source_url": "https://www.revitapidocs.com/2025/50bd4c92-f81a-7f5c-01cc-92d49e7fd56f.htm",
 71676|       "dll_signature_verified": true,
 71677|       "dll_relationship_scope": "declared",
 71678|       "dll_semantic_verified": null,
 71679|       "dll_verified_status": "signature_verified_declared",
 71680|       "revitlookup_referenced": null,
 71681|       "revitlookup_requires_document_context": null
 71682|     },
 71683|     {
 71684|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleData",
 71685|       "target": "Autodesk.Revit.DB.Phase",
 71686|       "member_name": "ShowCircuitNumberOnOneRowForMultiphaseCircuits",
 71687|       "member_kind": "property",
 71688|       "edge_type": "ASSIGNED_TO_PHASE",
 71689|       "confidence": "name_only_candidate",
 71690|       "confidence_tier": "likely",
 71691|       "target_resolution": "exact",
 71692|       "evidence": [
 71693|         "member name 'ShowCircuitNumberOnOneRowForMultiphaseCircuits' matches keyword pattern /Phase/ but return type 'bool' gives no type-level confirmation"
 71694|       ],
 71695|       "source_url": "https://www.revitapidocs.com/2025/2c400aa9-7d56-e1fb-8762-2ec3b603ddfe.htm",
 71696|       "dll_signature_verified": true,
 71697|       "dll_relationship_scope": "declared",
 71698|       "dll_semantic_verified": null,
 71699|       "dll_verified_status": "signature_verified_declared",
 71700|       "revitlookup_referenced": null,
 71701|       "revitlookup_requires_document_context": null
 71702|     },
 71703|     {
 71704|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleData",
 71705|       "target": "Autodesk.Revit.DB.Phase",
 71706|       "member_name": "ShowMultipleRowsForMultiphaseCircuits",
 71707|       "member_kind": "property",
 71708|       "edge_type": "ASSIGNED_TO_PHASE",
 71709|       "confidence": "name_only_candidate",
 71710|       "confidence_tier": "likely",
 71711|       "target_resolution": "exact",
 71712|       "evidence": [
 71713|         "member name 'ShowMultipleRowsForMultiphaseCircuits' matches keyword pattern /Phase/ but return type 'bool' gives no type-level confirmation"
 71714|       ],
 71715|       "source_url": "https://www.revitapidocs.com/2025/4ccaca7c-243a-4264-5703-868836f13d6f.htm",
 71716|       "dll_signature_verified": true,
 71717|       "dll_relationship_scope": "declared",
 71718|       "dll_semantic_verified": null,
 71719|       "dll_verified_status": "signature_verified_declared",
 71720|       "revitlookup_referenced": null,
 71721|       "revitlookup_requires_document_context": null
 71722|     },
 71723|     {
 71724|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleData",
 71725|       "target": "Autodesk.Revit.DB.View",
 71726|       "member_name": "ShowSlotFromDeviceInsteadOfTemplate",
 71727|       "member_kind": "property",
 71728|       "edge_type": "CONTROLLED_BY_TEMPLATE",
 71729|       "confidence": "name_only_candidate",
 71730|       "confidence_tier": "likely",
 71731|       "target_resolution": "exact",
 71732|       "evidence": [
 71733|         "member name 'ShowSlotFromDeviceInsteadOfTemplate' matches keyword pattern /Template/ but return type 'bool' gives no type-level confirmation"
 71734|       ],
 71735|       "source_url": "https://www.revitapidocs.com/2025/c5ea28f4-3388-5e21-54a7-750f6a433aa0.htm",
 71736|       "dll_signature_verified": true,
 71737|       "dll_relationship_scope": "declared",
 71738|       "dll_semantic_verified": null,
 71739|       "dll_verified_status": "signature_verified_declared",
 71740|       "revitlookup_referenced": null,
 71741|       "revitlookup_requires_document_context": null
 71742|     },
 71743|     {
 71744|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleData",
 71745|       "target": null,
 71746|       "member_name": "SummaryShowsGroups",
 71747|       "member_kind": "property",
 71748|       "edge_type": "MEMBER_OF_GROUP",
 71749|       "confidence": "name_only_candidate",
 71750|       "confidence_tier": "likely",
 71751|       "target_resolution": "none",
 71752|       "evidence": [
 71753|         "member name 'SummaryShowsGroups' matches keyword pattern /^GetMember|Group/ but return type 'bool' gives no type-level confirmation"
 71754|       ],
 71755|       "source_url": "https://www.revitapidocs.com/2025/9244a87e-c35f-b6df-9e13-8b1246a27f25.htm",
 71756|       "dll_signature_verified": true,
 71757|       "dll_relationship_scope": "declared",
 71758|       "dll_semantic_verified": null,
 71759|       "dll_verified_status": "signature_verified_declared",
 71760|       "revitlookup_referenced": null,
 71761|       "revitlookup_requires_document_context": null
 71762|     },
 71763|     {
 71764|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleData",
 71765|       "target": null,
 71766|       "member_name": "GetLoadClassifications",
 71767|       "member_kind": "method",
 71768|       "edge_type": "RETURNS_ELEMENT_IDS",
 71769|       "confidence": "unknown_reference",
 71770|       "confidence_tier": "unverified_reference",
```
