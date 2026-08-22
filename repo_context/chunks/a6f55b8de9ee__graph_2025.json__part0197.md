# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 197 of 216
- Original line range: 76441-76840
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 76441|       "dll_relationship_scope": "declared",
 76442|       "dll_semantic_verified": null,
 76443|       "dll_verified_status": "signature_verified_declared",
 76444|       "revitlookup_referenced": null,
 76445|       "revitlookup_requires_document_context": null
 76446|     },
 76447|     {
 76448|       "source": "Autodesk.Revit.DB.Mechanical.SpaceTag",
 76449|       "target": "Autodesk.Revit.DB.Mechanical.Space",
 76450|       "member_name": "Space",
 76451|       "member_kind": "property",
 76452|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76453|       "confidence": "direct_return_type",
 76454|       "confidence_tier": "unverified_reference",
 76455|       "target_resolution": "short_name_fallback",
 76456|       "evidence": [
 76457|         "return type 'Space' directly names a Revit DB object type"
 76458|       ],
 76459|       "source_url": "https://www.revitapidocs.com/2025/7fa4b1d8-2459-a46b-f68f-0a3a78a8383d.htm",
 76460|       "dll_signature_verified": true,
 76461|       "dll_relationship_scope": "declared",
 76462|       "dll_semantic_verified": null,
 76463|       "dll_verified_status": "signature_verified_declared",
 76464|       "revitlookup_referenced": null,
 76465|       "revitlookup_requires_document_context": null
 76466|     },
 76467|     {
 76468|       "source": "Autodesk.Revit.DB.Mechanical.SpaceTag",
 76469|       "target": "Autodesk.Revit.DB.Mechanical.SpaceTagType",
 76470|       "member_name": "SpaceTagType",
 76471|       "member_kind": "property",
 76472|       "edge_type": "TAGS_ELEMENT",
 76473|       "confidence": "direct_return_type",
 76474|       "confidence_tier": "core",
 76475|       "target_resolution": "short_name_fallback",
 76476|       "evidence": [
 76477|         "return type 'SpaceTagType' directly names a Revit DB object type"
 76478|       ],
 76479|       "source_url": "https://www.revitapidocs.com/2025/fe2ef2e7-a378-40f1-388c-0acc0f03b1ab.htm",
 76480|       "dll_signature_verified": true,
 76481|       "dll_relationship_scope": "declared",
 76482|       "dll_semantic_verified": null,
 76483|       "dll_verified_status": "signature_verified_declared",
 76484|       "revitlookup_referenced": null,
 76485|       "revitlookup_requires_document_context": null
 76486|     },
 76487|     {
 76488|       "source": "Autodesk.Revit.DB.Mechanical.SystemZoneData",
 76489|       "target": null,
 76490|       "member_name": "ZoneEquipmentId",
 76491|       "member_kind": "property",
 76492|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 76493|       "confidence": "unknown_reference",
 76494|       "confidence_tier": "unverified_reference",
 76495|       "target_resolution": "none",
 76496|       "evidence": [
 76497|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 76498|       ],
 76499|       "source_url": "https://www.revitapidocs.com/2025/b5cbf792-2a36-2932-b554-04e7a2edcf8f.htm",
 76500|       "dll_signature_verified": true,
 76501|       "dll_relationship_scope": "declared",
 76502|       "dll_semantic_verified": null,
 76503|       "dll_verified_status": "signature_verified_declared",
 76504|       "revitlookup_referenced": null,
 76505|       "revitlookup_requires_document_context": null
 76506|     },
 76507|     {
 76508|       "source": "Autodesk.Revit.DB.Mechanical.WaterLoopData",
 76509|       "target": null,
 76510|       "member_name": "CondenserWaterLoopId",
 76511|       "member_kind": "property",
 76512|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 76513|       "confidence": "unknown_reference",
 76514|       "confidence_tier": "unverified_reference",
 76515|       "target_resolution": "none",
 76516|       "evidence": [
 76517|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 76518|       ],
 76519|       "source_url": "https://www.revitapidocs.com/2025/b9ca47f2-86c4-b68b-ac68-2e1a34954c6b.htm",
 76520|       "dll_signature_verified": true,
 76521|       "dll_relationship_scope": "declared",
 76522|       "dll_semantic_verified": null,
 76523|       "dll_verified_status": "signature_verified_declared",
 76524|       "revitlookup_referenced": null,
 76525|       "revitlookup_requires_document_context": null
 76526|     },
 76527|     {
 76528|       "source": "Autodesk.Revit.DB.Mechanical.Zone",
 76529|       "target": "Autodesk.Revit.DB.Phase",
 76530|       "member_name": "Phase",
 76531|       "member_kind": "property",
 76532|       "edge_type": "ASSIGNED_TO_PHASE",
 76533|       "confidence": "direct_return_type",
 76534|       "confidence_tier": "core",
 76535|       "target_resolution": "exact",
 76536|       "evidence": [
 76537|         "return type 'Phase' directly names a Revit DB object type"
 76538|       ],
 76539|       "source_url": "https://www.revitapidocs.com/2025/968ffbac-2fdd-994e-3f70-2070cab24c41.htm",
 76540|       "dll_signature_verified": true,
 76541|       "dll_relationship_scope": "declared",
 76542|       "dll_semantic_verified": null,
 76543|       "dll_verified_status": "signature_verified_declared",
 76544|       "revitlookup_referenced": null,
 76545|       "revitlookup_requires_document_context": null
 76546|     },
 76547|     {
 76548|       "source": "Autodesk.Revit.DB.Mechanical.Zone",
 76549|       "target": "Autodesk.Revit.DB.Mechanical.SpaceSet",
 76550|       "member_name": "Spaces",
 76551|       "member_kind": "property",
 76552|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76553|       "confidence": "direct_return_type",
 76554|       "confidence_tier": "unverified_reference",
 76555|       "target_resolution": "short_name_fallback",
 76556|       "evidence": [
 76557|         "return type 'SpaceSet' directly names a Revit DB object type"
 76558|       ],
 76559|       "source_url": "https://www.revitapidocs.com/2025/c3e6c1c6-f009-c08c-ac71-4182c5000981.htm",
 76560|       "dll_signature_verified": true,
 76561|       "dll_relationship_scope": "declared",
 76562|       "dll_semantic_verified": null,
 76563|       "dll_verified_status": "signature_verified_declared",
 76564|       "revitlookup_referenced": null,
 76565|       "revitlookup_requires_document_context": null
 76566|     },
 76567|     {
 76568|       "source": "Autodesk.Revit.DB.Mechanical.Zone",
 76569|       "target": "Autodesk.Revit.DB.Mechanical.ZoneElementDomainData",
 76570|       "member_name": "GetDomainData",
 76571|       "member_kind": "method",
 76572|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76573|       "confidence": "direct_return_type",
 76574|       "confidence_tier": "unverified_reference",
 76575|       "target_resolution": "short_name_fallback",
 76576|       "evidence": [
 76577|         "return type 'ZoneElementDomainData' directly names a Revit DB object type"
 76578|       ],
 76579|       "source_url": "https://www.revitapidocs.com/2025/7edb0818-7c6a-2bfc-6269-bab9c8baea35.htm",
 76580|       "dll_signature_verified": true,
 76581|       "dll_relationship_scope": "declared",
 76582|       "dll_semantic_verified": null,
 76583|       "dll_verified_status": "signature_verified_declared",
 76584|       "revitlookup_referenced": null,
 76585|       "revitlookup_requires_document_context": null
 76586|     },
 76587|     {
 76588|       "source": "Autodesk.Revit.DB.Mechanical.ZoneEquipment",
 76589|       "target": null,
 76590|       "member_name": "GetAssociatedZoneEquipment",
 76591|       "member_kind": "method",
 76592|       "edge_type": "RETURNS_ELEMENT_IDS",
 76593|       "confidence": "unknown_reference",
 76594|       "confidence_tier": "unverified_reference",
 76595|       "target_resolution": "none",
 76596|       "evidence": [
 76597|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 76598|       ],
 76599|       "source_url": "https://www.revitapidocs.com/2025/3668167f-4abc-763c-45b2-24cf02545dea.htm",
 76600|       "dll_signature_verified": true,
 76601|       "dll_relationship_scope": "declared",
 76602|       "dll_semantic_verified": null,
 76603|       "dll_verified_status": "signature_verified_declared",
 76604|       "revitlookup_referenced": null,
 76605|       "revitlookup_requires_document_context": null
 76606|     },
 76607|     {
 76608|       "source": "Autodesk.Revit.DB.Mechanical.ZoneEquipment",
 76609|       "target": null,
 76610|       "member_name": "GetAssociatedZoneEquipment",
 76611|       "member_kind": "method",
 76612|       "edge_type": "RETURNS_ELEMENT_IDS",
 76613|       "confidence": "unknown_reference",
 76614|       "confidence_tier": "unverified_reference",
 76615|       "target_resolution": "none",
 76616|       "evidence": [
 76617|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 76618|       ],
 76619|       "source_url": "https://www.revitapidocs.com/2025/68d30140-93f9-9956-2d31-84b3c7b45af8.htm",
 76620|       "dll_signature_verified": true,
 76621|       "dll_relationship_scope": "declared",
 76622|       "dll_semantic_verified": null,
 76623|       "dll_verified_status": "signature_verified_declared",
 76624|       "revitlookup_referenced": null,
 76625|       "revitlookup_requires_document_context": null
 76626|     },
 76627|     {
 76628|       "source": "Autodesk.Revit.DB.Mechanical.ZoneEquipment",
 76629|       "target": "Autodesk.Revit.DB.Mechanical.ZoneEquipmentData",
 76630|       "member_name": "GetZoneEquipmentData",
 76631|       "member_kind": "method",
 76632|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76633|       "confidence": "direct_return_type",
 76634|       "confidence_tier": "unverified_reference",
 76635|       "target_resolution": "short_name_fallback",
 76636|       "evidence": [
 76637|         "return type 'ZoneEquipmentData' directly names a Revit DB object type"
 76638|       ],
 76639|       "source_url": "https://www.revitapidocs.com/2025/4f7eb984-8878-b009-ecef-bcb5587ca673.htm",
 76640|       "dll_signature_verified": true,
 76641|       "dll_relationship_scope": "declared",
 76642|       "dll_semantic_verified": null,
 76643|       "dll_verified_status": "signature_verified_declared",
 76644|       "revitlookup_referenced": null,
 76645|       "revitlookup_requires_document_context": null
 76646|     },
 76647|     {
 76648|       "source": "Autodesk.Revit.DB.Mechanical.ZoneEquipmentData",
 76649|       "target": null,
 76650|       "member_name": "AirSystemId",
 76651|       "member_kind": "property",
 76652|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 76653|       "confidence": "unknown_reference",
 76654|       "confidence_tier": "unverified_reference",
 76655|       "target_resolution": "none",
 76656|       "evidence": [
 76657|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 76658|       ],
 76659|       "source_url": "https://www.revitapidocs.com/2025/0ce0067f-c37e-0623-b82b-3acdee65177f.htm",
 76660|       "dll_signature_verified": true,
 76661|       "dll_relationship_scope": "declared",
 76662|       "dll_semantic_verified": null,
 76663|       "dll_verified_status": "signature_verified_declared",
 76664|       "revitlookup_referenced": null,
 76665|       "revitlookup_requires_document_context": null
 76666|     },
 76667|     {
 76668|       "source": "Autodesk.Revit.DB.Mechanical.ZoneEquipmentData",
 76669|       "target": null,
 76670|       "member_name": "ChilledWaterLoopId",
 76671|       "member_kind": "property",
 76672|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 76673|       "confidence": "unknown_reference",
 76674|       "confidence_tier": "unverified_reference",
 76675|       "target_resolution": "none",
 76676|       "evidence": [
 76677|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 76678|       ],
 76679|       "source_url": "https://www.revitapidocs.com/2025/6dab9886-6d57-c8d5-1632-db3cf1336981.htm",
 76680|       "dll_signature_verified": true,
 76681|       "dll_relationship_scope": "declared",
 76682|       "dll_semantic_verified": null,
 76683|       "dll_verified_status": "signature_verified_declared",
 76684|       "revitlookup_referenced": null,
 76685|       "revitlookup_requires_document_context": null
 76686|     },
 76687|     {
 76688|       "source": "Autodesk.Revit.DB.Mechanical.ZoneEquipmentData",
 76689|       "target": null,
 76690|       "member_name": "CondenserWaterLoopId",
 76691|       "member_kind": "property",
 76692|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 76693|       "confidence": "unknown_reference",
 76694|       "confidence_tier": "unverified_reference",
 76695|       "target_resolution": "none",
 76696|       "evidence": [
 76697|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 76698|       ],
 76699|       "source_url": "https://www.revitapidocs.com/2025/47f543fc-aab9-aa58-c350-05cf3d896426.htm",
 76700|       "dll_signature_verified": true,
 76701|       "dll_relationship_scope": "declared",
 76702|       "dll_semantic_verified": null,
 76703|       "dll_verified_status": "signature_verified_declared",
 76704|       "revitlookup_referenced": null,
 76705|       "revitlookup_requires_document_context": null
 76706|     },
 76707|     {
 76708|       "source": "Autodesk.Revit.DB.Mechanical.ZoneEquipmentData",
 76709|       "target": null,
 76710|       "member_name": "HotWaterLoopId",
 76711|       "member_kind": "property",
 76712|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 76713|       "confidence": "unknown_reference",
 76714|       "confidence_tier": "unverified_reference",
 76715|       "target_resolution": "none",
 76716|       "evidence": [
 76717|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 76718|       ],
 76719|       "source_url": "https://www.revitapidocs.com/2025/0cb8dad3-1241-af85-78e6-9ec277a30944.htm",
 76720|       "dll_signature_verified": true,
 76721|       "dll_relationship_scope": "declared",
 76722|       "dll_semantic_verified": null,
 76723|       "dll_verified_status": "signature_verified_declared",
 76724|       "revitlookup_referenced": null,
 76725|       "revitlookup_requires_document_context": null
 76726|     },
 76727|     {
 76728|       "source": "Autodesk.Revit.DB.Mechanical.ZoneEquipmentData",
 76729|       "target": null,
 76730|       "member_name": "VRFLoopId",
 76731|       "member_kind": "property",
 76732|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 76733|       "confidence": "unknown_reference",
 76734|       "confidence_tier": "unverified_reference",
 76735|       "target_resolution": "none",
 76736|       "evidence": [
 76737|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 76738|       ],
 76739|       "source_url": "https://www.revitapidocs.com/2025/a24286dd-bf72-9135-2b32-386bd90e1d8b.htm",
 76740|       "dll_signature_verified": true,
 76741|       "dll_relationship_scope": "declared",
 76742|       "dll_semantic_verified": null,
 76743|       "dll_verified_status": "signature_verified_declared",
 76744|       "revitlookup_referenced": null,
 76745|       "revitlookup_requires_document_context": null
 76746|     },
 76747|     {
 76748|       "source": "Autodesk.Revit.DB.Plumbing.FlexPipe",
 76749|       "target": "Autodesk.Revit.DB.Plumbing.FlexPipeType",
 76750|       "member_name": "FlexPipeType",
 76751|       "member_kind": "property",
 76752|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76753|       "confidence": "direct_return_type",
 76754|       "confidence_tier": "unverified_reference",
 76755|       "target_resolution": "short_name_fallback",
 76756|       "evidence": [
 76757|         "return type 'FlexPipeType' directly names a Revit DB object type"
 76758|       ],
 76759|       "source_url": "https://www.revitapidocs.com/2025/5b7ab8d0-bd8d-f834-5d2e-a2504d3a91ce.htm",
 76760|       "dll_signature_verified": true,
 76761|       "dll_relationship_scope": "declared",
 76762|       "dll_semantic_verified": null,
 76763|       "dll_verified_status": "signature_verified_declared",
 76764|       "revitlookup_referenced": null,
 76765|       "revitlookup_requires_document_context": null
 76766|     },
 76767|     {
 76768|       "source": "Autodesk.Revit.DB.Plumbing.FluidTemperatureSetIterator",
 76769|       "target": "Autodesk.Revit.DB.Plumbing.FluidTemperature",
 76770|       "member_name": "Current",
 76771|       "member_kind": "property",
 76772|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76773|       "confidence": "direct_return_type",
 76774|       "confidence_tier": "unverified_reference",
 76775|       "target_resolution": "short_name_fallback",
 76776|       "evidence": [
 76777|         "return type 'FluidTemperature' directly names a Revit DB object type"
 76778|       ],
 76779|       "source_url": "https://www.revitapidocs.com/2025/30e48d5b-e280-0aaa-e615-fca90d069055.htm",
 76780|       "dll_signature_verified": true,
 76781|       "dll_relationship_scope": "declared",
 76782|       "dll_semantic_verified": null,
 76783|       "dll_verified_status": "signature_verified_declared",
 76784|       "revitlookup_referenced": null,
 76785|       "revitlookup_requires_document_context": null
 76786|     },
 76787|     {
 76788|       "source": "Autodesk.Revit.DB.Plumbing.FluidTemperatureSetIterator",
 76789|       "target": "Autodesk.Revit.DB.Plumbing.FluidTemperature",
 76790|       "member_name": "GetCurrent",
 76791|       "member_kind": "method",
 76792|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76793|       "confidence": "direct_return_type",
 76794|       "confidence_tier": "unverified_reference",
 76795|       "target_resolution": "short_name_fallback",
 76796|       "evidence": [
 76797|         "return type 'FluidTemperature' directly names a Revit DB object type"
 76798|       ],
 76799|       "source_url": "https://www.revitapidocs.com/2025/b9035a98-c5b6-5b8d-6a47-2f575435c8e5.htm",
 76800|       "dll_signature_verified": true,
 76801|       "dll_relationship_scope": "declared",
 76802|       "dll_semantic_verified": null,
 76803|       "dll_verified_status": "signature_verified_declared",
 76804|       "revitlookup_referenced": null,
 76805|       "revitlookup_requires_document_context": null
 76806|     },
 76807|     {
 76808|       "source": "Autodesk.Revit.DB.Plumbing.FluidType",
 76809|       "target": "Autodesk.Revit.DB.Plumbing.FluidTemperatureSetIterator",
 76810|       "member_name": "GetFluidTemperatureSetIterator",
 76811|       "member_kind": "method",
 76812|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76813|       "confidence": "direct_return_type",
 76814|       "confidence_tier": "unverified_reference",
 76815|       "target_resolution": "short_name_fallback",
 76816|       "evidence": [
 76817|         "return type 'FluidTemperatureSetIterator' directly names a Revit DB object type"
 76818|       ],
 76819|       "source_url": "https://www.revitapidocs.com/2025/890a61fc-60fe-b8bf-6f64-ebf94618493a.htm",
 76820|       "dll_signature_verified": true,
 76821|       "dll_relationship_scope": "declared",
 76822|       "dll_semantic_verified": null,
 76823|       "dll_verified_status": "signature_verified_declared",
 76824|       "revitlookup_referenced": null,
 76825|       "revitlookup_requires_document_context": null
 76826|     },
 76827|     {
 76828|       "source": "Autodesk.Revit.DB.Plumbing.FluidType",
 76829|       "target": "Autodesk.Revit.DB.Plumbing.FluidTemperature",
 76830|       "member_name": "GetTemperature",
 76831|       "member_kind": "method",
 76832|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76833|       "confidence": "direct_return_type",
 76834|       "confidence_tier": "unverified_reference",
 76835|       "target_resolution": "short_name_fallback",
 76836|       "evidence": [
 76837|         "return type 'FluidTemperature' directly names a Revit DB object type"
 76838|       ],
 76839|       "source_url": "https://www.revitapidocs.com/2025/c9aa98b4-430f-d035-4e32-a0c55392c6f6.htm",
 76840|       "dll_signature_verified": true,
```
