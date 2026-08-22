# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 174 of 216
- Original line range: 67471-67870
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 67471|       "dll_verified_status": "signature_verified_declared",
 67472|       "revitlookup_referenced": null,
 67473|       "revitlookup_requires_document_context": null
 67474|     },
 67475|     {
 67476|       "source": "Autodesk.Revit.DB.Architecture.BuildingPad",
 67477|       "target": null,
 67478|       "member_name": "HostId",
 67479|       "member_kind": "property",
 67480|       "edge_type": "HOSTED_BY",
 67481|       "confidence": "elementid_with_strong_name",
 67482|       "confidence_tier": "core",
 67483|       "target_resolution": "none",
 67484|       "evidence": [
 67485|         "member name 'HostId' matches keyword pattern /^GetHosted|Host/"
 67486|       ],
 67487|       "source_url": "https://www.revitapidocs.com/2025/de889336-b51c-0fdb-a939-5ae74f01c3e9.htm",
 67488|       "dll_signature_verified": true,
 67489|       "dll_relationship_scope": "declared",
 67490|       "dll_semantic_verified": null,
 67491|       "dll_verified_status": "signature_verified_declared",
 67492|       "revitlookup_referenced": null,
 67493|       "revitlookup_requires_document_context": null
 67494|     },
 67495|     {
 67496|       "source": "Autodesk.Revit.DB.Architecture.ContinuousRail",
 67497|       "target": null,
 67498|       "member_name": "HostRailingId",
 67499|       "member_kind": "property",
 67500|       "edge_type": "HOSTED_BY",
 67501|       "confidence": "elementid_with_strong_name",
 67502|       "confidence_tier": "core",
 67503|       "target_resolution": "none",
 67504|       "evidence": [
 67505|         "member name 'HostRailingId' matches keyword pattern /^GetHosted|Host/"
 67506|       ],
 67507|       "source_url": "https://www.revitapidocs.com/2025/39f8e3c5-543d-f490-b9a4-1407c21292b6.htm",
 67508|       "dll_signature_verified": true,
 67509|       "dll_relationship_scope": "declared",
 67510|       "dll_semantic_verified": null,
 67511|       "dll_verified_status": "signature_verified_declared",
 67512|       "revitlookup_referenced": null,
 67513|       "revitlookup_requires_document_context": null
 67514|     },
 67515|     {
 67516|       "source": "Autodesk.Revit.DB.Architecture.ContinuousRail",
 67517|       "target": null,
 67518|       "member_name": "GetSupports",
 67519|       "member_kind": "method",
 67520|       "edge_type": "RETURNS_ELEMENT_IDS",
 67521|       "confidence": "unknown_reference",
 67522|       "confidence_tier": "unverified_reference",
 67523|       "target_resolution": "none",
 67524|       "evidence": [
 67525|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 67526|       ],
 67527|       "source_url": "https://www.revitapidocs.com/2025/e966884e-1c38-cb8c-2723-29d8746e5c00.htm",
 67528|       "dll_signature_verified": true,
 67529|       "dll_relationship_scope": "declared",
 67530|       "dll_semantic_verified": null,
 67531|       "dll_verified_status": "signature_verified_declared",
 67532|       "revitlookup_referenced": null,
 67533|       "revitlookup_requires_document_context": null
 67534|     },
 67535|     {
 67536|       "source": "Autodesk.Revit.DB.Architecture.ContinuousRailType",
 67537|       "target": null,
 67538|       "member_name": "EndOrTopTermination",
 67539|       "member_kind": "property",
 67540|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 67541|       "confidence": "unknown_reference",
 67542|       "confidence_tier": "unverified_reference",
 67543|       "target_resolution": "none",
 67544|       "evidence": [
 67545|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 67546|       ],
 67547|       "source_url": "https://www.revitapidocs.com/2025/751e2f28-e0b6-30a6-2ac4-8a3abf82d39c.htm",
 67548|       "dll_signature_verified": true,
 67549|       "dll_relationship_scope": "declared",
 67550|       "dll_semantic_verified": null,
 67551|       "dll_verified_status": "signature_verified_declared",
 67552|       "revitlookup_referenced": null,
 67553|       "revitlookup_requires_document_context": null
 67554|     },
 67555|     {
 67556|       "source": "Autodesk.Revit.DB.Architecture.ContinuousRailType",
 67557|       "target": null,
 67558|       "member_name": "ProfileId",
 67559|       "member_kind": "property",
 67560|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 67561|       "confidence": "unknown_reference",
 67562|       "confidence_tier": "unverified_reference",
 67563|       "target_resolution": "none",
 67564|       "evidence": [
 67565|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 67566|       ],
 67567|       "source_url": "https://www.revitapidocs.com/2025/79f73965-9ca2-42d9-8473-b799a01bb961.htm",
 67568|       "dll_signature_verified": true,
 67569|       "dll_relationship_scope": "declared",
 67570|       "dll_semantic_verified": null,
 67571|       "dll_verified_status": "signature_verified_declared",
 67572|       "revitlookup_referenced": null,
 67573|       "revitlookup_requires_document_context": null
 67574|     },
 67575|     {
 67576|       "source": "Autodesk.Revit.DB.Architecture.ContinuousRailType",
 67577|       "target": null,
 67578|       "member_name": "StartOrBottomTermination",
 67579|       "member_kind": "property",
 67580|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 67581|       "confidence": "unknown_reference",
 67582|       "confidence_tier": "unverified_reference",
 67583|       "target_resolution": "none",
 67584|       "evidence": [
 67585|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 67586|       ],
 67587|       "source_url": "https://www.revitapidocs.com/2025/45b39093-e7bd-b422-a70f-ea63fa6e7634.htm",
 67588|       "dll_signature_verified": true,
 67589|       "dll_relationship_scope": "declared",
 67590|       "dll_semantic_verified": null,
 67591|       "dll_verified_status": "signature_verified_declared",
 67592|       "revitlookup_referenced": null,
 67593|       "revitlookup_requires_document_context": null
 67594|     },
 67595|     {
 67596|       "source": "Autodesk.Revit.DB.Architecture.Fascia",
 67597|       "target": "Autodesk.Revit.DB.Architecture.FasciaType",
 67598|       "member_name": "FasciaType",
 67599|       "member_kind": "property",
 67600|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67601|       "confidence": "direct_return_type",
 67602|       "confidence_tier": "unverified_reference",
 67603|       "target_resolution": "short_name_fallback",
 67604|       "evidence": [
 67605|         "return type 'FasciaType' directly names a Revit DB object type"
 67606|       ],
 67607|       "source_url": "https://www.revitapidocs.com/2025/cb6d7473-3192-49bf-a2ef-4ca4618a8219.htm",
 67608|       "dll_signature_verified": true,
 67609|       "dll_relationship_scope": "declared",
 67610|       "dll_semantic_verified": null,
 67611|       "dll_verified_status": "signature_verified_declared",
 67612|       "revitlookup_referenced": null,
 67613|       "revitlookup_requires_document_context": null
 67614|     },
 67615|     {
 67616|       "source": "Autodesk.Revit.DB.Architecture.Gutter",
 67617|       "target": "Autodesk.Revit.DB.Architecture.GutterType",
 67618|       "member_name": "GutterType",
 67619|       "member_kind": "property",
 67620|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67621|       "confidence": "direct_return_type",
 67622|       "confidence_tier": "unverified_reference",
 67623|       "target_resolution": "short_name_fallback",
 67624|       "evidence": [
 67625|         "return type 'GutterType' directly names a Revit DB object type"
 67626|       ],
 67627|       "source_url": "https://www.revitapidocs.com/2025/75fe8039-cddc-a565-ee2a-9833c24114a9.htm",
 67628|       "dll_signature_verified": true,
 67629|       "dll_relationship_scope": "declared",
 67630|       "dll_semantic_verified": null,
 67631|       "dll_verified_status": "signature_verified_declared",
 67632|       "revitlookup_referenced": null,
 67633|       "revitlookup_requires_document_context": null
 67634|     },
 67635|     {
 67636|       "source": "Autodesk.Revit.DB.Architecture.HandRailType",
 67637|       "target": null,
 67638|       "member_name": "SupportTypeId",
 67639|       "member_kind": "property",
 67640|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 67641|       "confidence": "unknown_reference",
 67642|       "confidence_tier": "unverified_reference",
 67643|       "target_resolution": "none",
 67644|       "evidence": [
 67645|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 67646|       ],
 67647|       "source_url": "https://www.revitapidocs.com/2025/2ac66afd-c996-bb2a-1633-91820a758642.htm",
 67648|       "dll_signature_verified": true,
 67649|       "dll_relationship_scope": "declared",
 67650|       "dll_semantic_verified": null,
 67651|       "dll_verified_status": "signature_verified_declared",
 67652|       "revitlookup_referenced": null,
 67653|       "revitlookup_requires_document_context": null
 67654|     },
 67655|     {
 67656|       "source": "Autodesk.Revit.DB.Architecture.MultistoryStairs",
 67657|       "target": null,
 67658|       "member_name": "StandardStairsId",
 67659|       "member_kind": "property",
 67660|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 67661|       "confidence": "unknown_reference",
 67662|       "confidence_tier": "unverified_reference",
 67663|       "target_resolution": "none",
 67664|       "evidence": [
 67665|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 67666|       ],
 67667|       "source_url": "https://www.revitapidocs.com/2025/2430a0ec-b9b4-28ad-de89-3d7a1340d239.htm",
 67668|       "dll_signature_verified": true,
 67669|       "dll_relationship_scope": "declared",
 67670|       "dll_semantic_verified": null,
 67671|       "dll_verified_status": "signature_verified_declared",
 67672|       "revitlookup_referenced": null,
 67673|       "revitlookup_requires_document_context": null
 67674|     },
 67675|     {
 67676|       "source": "Autodesk.Revit.DB.Architecture.MultistoryStairs",
 67677|       "target": "Autodesk.Revit.DB.Level",
 67678|       "member_name": "CanConnectLevel",
 67679|       "member_kind": "method",
 67680|       "edge_type": "ASSIGNED_TO_LEVEL",
 67681|       "confidence": "name_only_candidate",
 67682|       "confidence_tier": "likely",
 67683|       "target_resolution": "exact",
 67684|       "evidence": [
 67685|         "member name 'CanConnectLevel' matches keyword pattern /Level/ but return type 'bool' gives no type-level confirmation"
 67686|       ],
 67687|       "source_url": "https://www.revitapidocs.com/2025/01069be7-0661-3d91-48d3-9a6dea6c9711.htm",
 67688|       "dll_signature_verified": true,
 67689|       "dll_relationship_scope": "declared",
 67690|       "dll_semantic_verified": null,
 67691|       "dll_verified_status": "signature_verified_declared",
 67692|       "revitlookup_referenced": null,
 67693|       "revitlookup_requires_document_context": null
 67694|     },
 67695|     {
 67696|       "source": "Autodesk.Revit.DB.Architecture.MultistoryStairs",
 67697|       "target": "Autodesk.Revit.DB.Level",
 67698|       "member_name": "CanDisconnectLevel",
 67699|       "member_kind": "method",
 67700|       "edge_type": "ASSIGNED_TO_LEVEL",
 67701|       "confidence": "name_only_candidate",
 67702|       "confidence_tier": "likely",
 67703|       "target_resolution": "exact",
 67704|       "evidence": [
 67705|         "member name 'CanDisconnectLevel' matches keyword pattern /Level/ but return type 'bool' gives no type-level confirmation"
 67706|       ],
 67707|       "source_url": "https://www.revitapidocs.com/2025/471db1d0-7f14-6a45-a31e-b31a85f1f124.htm",
 67708|       "dll_signature_verified": true,
 67709|       "dll_relationship_scope": "declared",
 67710|       "dll_semantic_verified": null,
 67711|       "dll_verified_status": "signature_verified_declared",
 67712|       "revitlookup_referenced": null,
 67713|       "revitlookup_requires_document_context": null
 67714|     },
 67715|     {
 67716|       "source": "Autodesk.Revit.DB.Architecture.MultistoryStairs",
 67717|       "target": "Autodesk.Revit.DB.Level",
 67718|       "member_name": "ConnectLevels",
 67719|       "member_kind": "method",
 67720|       "edge_type": "ASSIGNED_TO_LEVEL",
 67721|       "confidence": "name_only_candidate",
 67722|       "confidence_tier": "likely",
 67723|       "target_resolution": "exact",
 67724|       "evidence": [
 67725|         "member name 'ConnectLevels' matches keyword pattern /Level/ but return type 'void' gives no type-level confirmation"
 67726|       ],
 67727|       "source_url": "https://www.revitapidocs.com/2025/e6734f94-80de-fedd-e972-488899569085.htm",
 67728|       "dll_signature_verified": true,
 67729|       "dll_relationship_scope": "declared",
 67730|       "dll_semantic_verified": null,
 67731|       "dll_verified_status": "signature_verified_declared",
 67732|       "revitlookup_referenced": null,
 67733|       "revitlookup_requires_document_context": null
 67734|     },
 67735|     {
 67736|       "source": "Autodesk.Revit.DB.Architecture.MultistoryStairs",
 67737|       "target": "Autodesk.Revit.DB.Level",
 67738|       "member_name": "DisconnectLevels",
 67739|       "member_kind": "method",
 67740|       "edge_type": "ASSIGNED_TO_LEVEL",
 67741|       "confidence": "name_only_candidate",
 67742|       "confidence_tier": "likely",
 67743|       "target_resolution": "exact",
 67744|       "evidence": [
 67745|         "member name 'DisconnectLevels' matches keyword pattern /Level/ but return type 'void' gives no type-level confirmation"
 67746|       ],
 67747|       "source_url": "https://www.revitapidocs.com/2025/482be079-8bd4-e275-8231-7d3ae487e73c.htm",
 67748|       "dll_signature_verified": true,
 67749|       "dll_relationship_scope": "declared",
 67750|       "dll_semantic_verified": null,
 67751|       "dll_verified_status": "signature_verified_declared",
 67752|       "revitlookup_referenced": null,
 67753|       "revitlookup_requires_document_context": null
 67754|     },
 67755|     {
 67756|       "source": "Autodesk.Revit.DB.Architecture.MultistoryStairs",
 67757|       "target": "Autodesk.Revit.DB.Level",
 67758|       "member_name": "GetAllConnectedLevels",
 67759|       "member_kind": "method",
 67760|       "edge_type": "ASSIGNED_TO_LEVEL",
 67761|       "confidence": "elementid_collection_with_strong_name",
 67762|       "confidence_tier": "core",
 67763|       "target_resolution": "exact",
 67764|       "evidence": [
 67765|         "member name 'GetAllConnectedLevels' matches keyword pattern /Level/"
 67766|       ],
 67767|       "source_url": "https://www.revitapidocs.com/2025/c6c09472-e44a-9bf5-6847-e4a0c2212d00.htm",
 67768|       "dll_signature_verified": true,
 67769|       "dll_relationship_scope": "declared",
 67770|       "dll_semantic_verified": null,
 67771|       "dll_verified_status": "signature_verified_declared",
 67772|       "revitlookup_referenced": null,
 67773|       "revitlookup_requires_document_context": null
 67774|     },
 67775|     {
 67776|       "source": "Autodesk.Revit.DB.Architecture.MultistoryStairs",
 67777|       "target": null,
 67778|       "member_name": "GetAllStairsIds",
 67779|       "member_kind": "method",
 67780|       "edge_type": "RETURNS_ELEMENT_IDS",
 67781|       "confidence": "elementid_collection_with_strong_name",
 67782|       "confidence_tier": "core",
 67783|       "target_resolution": "none",
 67784|       "evidence": [
 67785|         "member name 'GetAllStairsIds' matches keyword pattern /^GetAll/"
 67786|       ],
 67787|       "source_url": "https://www.revitapidocs.com/2025/a66792ff-4d73-afc7-8df6-ae8733cf69de.htm",
 67788|       "dll_signature_verified": true,
 67789|       "dll_relationship_scope": "declared",
 67790|       "dll_semantic_verified": null,
 67791|       "dll_verified_status": "signature_verified_declared",
 67792|       "revitlookup_referenced": null,
 67793|       "revitlookup_requires_document_context": null
 67794|     },
 67795|     {
 67796|       "source": "Autodesk.Revit.DB.Architecture.MultistoryStairs",
 67797|       "target": "Autodesk.Revit.DB.Architecture.Stairs",
 67798|       "member_name": "GetStairsOnLevel",
 67799|       "member_kind": "method",
 67800|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67801|       "confidence": "direct_return_type",
 67802|       "confidence_tier": "unverified_reference",
 67803|       "target_resolution": "short_name_fallback",
 67804|       "evidence": [
 67805|         "member name 'GetStairsOnLevel' matches keyword pattern /Level/ implying target 'Level', but the actual return type 'Stairs' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 67806|         "return type 'Stairs' directly names a Revit DB object type"
 67807|       ],
 67808|       "source_url": "https://www.revitapidocs.com/2025/7a591c9d-6dd9-398e-9eb5-280eca78d396.htm",
 67809|       "dll_signature_verified": true,
 67810|       "dll_relationship_scope": "declared",
 67811|       "dll_semantic_verified": null,
 67812|       "dll_verified_status": "signature_verified_declared",
 67813|       "revitlookup_referenced": null,
 67814|       "revitlookup_requires_document_context": null
 67815|     },
 67816|     {
 67817|       "source": "Autodesk.Revit.DB.Architecture.MultistoryStairs",
 67818|       "target": "Autodesk.Revit.DB.Level",
 67819|       "member_name": "GetStairsPlacementLevels",
 67820|       "member_kind": "method",
 67821|       "edge_type": "ASSIGNED_TO_LEVEL",
 67822|       "confidence": "elementid_collection_with_strong_name",
 67823|       "confidence_tier": "core",
 67824|       "target_resolution": "exact",
 67825|       "evidence": [
 67826|         "member name 'GetStairsPlacementLevels' matches keyword pattern /Level/"
 67827|       ],
 67828|       "source_url": "https://www.revitapidocs.com/2025/1d81b0f3-4065-8ec7-e7f8-0fbb1d120617.htm",
 67829|       "dll_signature_verified": true,
 67830|       "dll_relationship_scope": "declared",
 67831|       "dll_semantic_verified": null,
 67832|       "dll_verified_status": "signature_verified_declared",
 67833|       "revitlookup_referenced": null,
 67834|       "revitlookup_requires_document_context": null
 67835|     },
 67836|     {
 67837|       "source": "Autodesk.Revit.DB.Architecture.MultistoryStairs",
 67838|       "target": "Autodesk.Revit.DB.Architecture.Stairs",
 67839|       "member_name": "Pin",
 67840|       "member_kind": "method",
 67841|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67842|       "confidence": "direct_return_type",
 67843|       "confidence_tier": "unverified_reference",
 67844|       "target_resolution": "short_name_fallback",
 67845|       "evidence": [
 67846|         "return type 'Stairs' directly names a Revit DB object type"
 67847|       ],
 67848|       "source_url": "https://www.revitapidocs.com/2025/41a11436-7ef4-f8c8-f247-04d529f8c466.htm",
 67849|       "dll_signature_verified": true,
 67850|       "dll_relationship_scope": "declared",
 67851|       "dll_semantic_verified": null,
 67852|       "dll_verified_status": "signature_verified_declared",
 67853|       "revitlookup_referenced": null,
 67854|       "revitlookup_requires_document_context": null
 67855|     },
 67856|     {
 67857|       "source": "Autodesk.Revit.DB.Architecture.MultistoryStairs",
 67858|       "target": "Autodesk.Revit.DB.Architecture.Stairs",
 67859|       "member_name": "Unpin",
 67860|       "member_kind": "method",
 67861|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67862|       "confidence": "direct_return_type",
 67863|       "confidence_tier": "unverified_reference",
 67864|       "target_resolution": "short_name_fallback",
 67865|       "evidence": [
 67866|         "return type 'Stairs' directly names a Revit DB object type"
 67867|       ],
 67868|       "source_url": "https://www.revitapidocs.com/2025/f1eb4c84-2b7e-1b6a-dc8b-dfc6c0c994c9.htm",
 67869|       "dll_signature_verified": true,
 67870|       "dll_relationship_scope": "declared",
```
