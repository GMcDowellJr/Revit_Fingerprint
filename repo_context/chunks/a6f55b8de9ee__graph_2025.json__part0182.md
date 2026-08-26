# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 182 of 216
- Original line range: 70591-70990
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
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
 70601|       "source": "Autodesk.Revit.DB.Electrical.ElectricalAnalyticalNode",
 70602|       "target": "Autodesk.Revit.DB.Electrical.AnalyticalDistributionNodePropertyData",
 70603|       "member_name": "GetAnalyticalPropertyData",
 70604|       "member_kind": "method",
 70605|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70606|       "confidence": "direct_return_type",
 70607|       "confidence_tier": "unverified_reference",
 70608|       "target_resolution": "short_name_fallback",
 70609|       "evidence": [
 70610|         "return type 'AnalyticalDistributionNodePropertyData' directly names a Revit DB object type"
 70611|       ],
 70612|       "source_url": "https://www.revitapidocs.com/2025/1b6aa6c7-35a8-46cf-8e05-efa5665f8f5d.htm",
 70613|       "dll_signature_verified": true,
 70614|       "dll_relationship_scope": "declared",
 70615|       "dll_semantic_verified": null,
 70616|       "dll_verified_status": "signature_verified_declared",
 70617|       "revitlookup_referenced": null,
 70618|       "revitlookup_requires_document_context": null
 70619|     },
 70620|     {
 70621|       "source": "Autodesk.Revit.DB.Electrical.ElectricalAnalyticalNode",
 70622|       "target": null,
 70623|       "member_name": "GetDownstreamNodeIds",
 70624|       "member_kind": "method",
 70625|       "edge_type": "RETURNS_ELEMENT_IDS",
 70626|       "confidence": "unknown_reference",
 70627|       "confidence_tier": "unverified_reference",
 70628|       "target_resolution": "none",
 70629|       "evidence": [
 70630|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 70631|       ],
 70632|       "source_url": "https://www.revitapidocs.com/2025/5ff642a5-6bb9-fc6f-6b1a-52f337a9be5d.htm",
 70633|       "dll_signature_verified": true,
 70634|       "dll_relationship_scope": "declared",
 70635|       "dll_semantic_verified": null,
 70636|       "dll_verified_status": "signature_verified_declared",
 70637|       "revitlookup_referenced": null,
 70638|       "revitlookup_requires_document_context": null
 70639|     },
 70640|     {
 70641|       "source": "Autodesk.Revit.DB.Electrical.ElectricalAnalyticalNode",
 70642|       "target": null,
 70643|       "member_name": "GetUpstreamNodeIds",
 70644|       "member_kind": "method",
 70645|       "edge_type": "RETURNS_ELEMENT_IDS",
 70646|       "confidence": "unknown_reference",
 70647|       "confidence_tier": "unverified_reference",
 70648|       "target_resolution": "none",
 70649|       "evidence": [
 70650|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 70651|       ],
 70652|       "source_url": "https://www.revitapidocs.com/2025/8f3adbf1-ccaa-dc1f-8440-a8cdf9ba747c.htm",
 70653|       "dll_signature_verified": true,
 70654|       "dll_relationship_scope": "declared",
 70655|       "dll_semantic_verified": null,
 70656|       "dll_verified_status": "signature_verified_declared",
 70657|       "revitlookup_referenced": null,
 70658|       "revitlookup_requires_document_context": null
 70659|     },
 70660|     {
 70661|       "source": "Autodesk.Revit.DB.Electrical.ElectricalDemandFactorDefinition",
 70662|       "target": "Autodesk.Revit.DB.Electrical.ElectricalDemandFactorValue",
 70663|       "member_name": "GetValues",
 70664|       "member_kind": "method",
 70665|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70666|       "confidence": "needs_runtime_validation",
 70667|       "confidence_tier": "needs_validation",
 70668|       "target_resolution": "short_name_fallback",
 70669|       "evidence": [
 70670|         "return type 'ICollection < ElectricalDemandFactorValue >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 70671|       ],
 70672|       "source_url": "https://www.revitapidocs.com/2025/6f45b084-2c52-bfae-ae08-1945f716f55d.htm",
 70673|       "dll_signature_verified": true,
 70674|       "dll_relationship_scope": "declared",
 70675|       "dll_semantic_verified": null,
 70676|       "dll_verified_status": "signature_verified_declared",
 70677|       "revitlookup_referenced": null,
 70678|       "revitlookup_requires_document_context": null
 70679|     },
 70680|     {
 70681|       "source": "Autodesk.Revit.DB.Electrical.ElectricalEquipment",
 70682|       "target": null,
 70683|       "member_name": "CircuitNamingSchemeId",
 70684|       "member_kind": "property",
 70685|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 70686|       "confidence": "unknown_reference",
 70687|       "confidence_tier": "unverified_reference",
 70688|       "target_resolution": "none",
 70689|       "evidence": [
 70690|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 70691|       ],
 70692|       "source_url": "https://www.revitapidocs.com/2025/dc4fafa0-17d5-2283-91e1-ef70c7b13aa5.htm",
 70693|       "dll_signature_verified": true,
 70694|       "dll_relationship_scope": "declared",
 70695|       "dll_semantic_verified": null,
 70696|       "dll_verified_status": "signature_verified_declared",
 70697|       "revitlookup_referenced": null,
 70698|       "revitlookup_requires_document_context": null
 70699|     },
 70700|     {
 70701|       "source": "Autodesk.Revit.DB.Electrical.ElectricalEquipment",
 70702|       "target": "Autodesk.Revit.DB.Electrical.DistributionSysType",
 70703|       "member_name": "DistributionSystem",
 70704|       "member_kind": "property",
 70705|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70706|       "confidence": "direct_return_type",
 70707|       "confidence_tier": "unverified_reference",
 70708|       "target_resolution": "short_name_fallback",
 70709|       "evidence": [
 70710|         "return type 'DistributionSysType' directly names a Revit DB object type"
 70711|       ],
 70712|       "source_url": "https://www.revitapidocs.com/2025/007c2efd-757f-dda3-c875-50622e546406.htm",
 70713|       "dll_signature_verified": true,
 70714|       "dll_relationship_scope": "declared",
 70715|       "dll_semantic_verified": null,
 70716|       "dll_verified_status": "signature_verified_declared",
 70717|       "revitlookup_referenced": null,
 70718|       "revitlookup_requires_document_context": null
 70719|     },
 70720|     {
 70721|       "source": "Autodesk.Revit.DB.Electrical.ElectricalLoadAreaData",
 70722|       "target": null,
 70723|       "member_name": "GetAreaBasedLoadIds",
 70724|       "member_kind": "method",
 70725|       "edge_type": "RETURNS_ELEMENT_IDS",
 70726|       "confidence": "unknown_reference",
 70727|       "confidence_tier": "unverified_reference",
 70728|       "target_resolution": "none",
 70729|       "evidence": [
 70730|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 70731|       ],
 70732|       "source_url": "https://www.revitapidocs.com/2025/c27d0636-3568-7c77-2f23-350489260904.htm",
 70733|       "dll_signature_verified": true,
 70734|       "dll_relationship_scope": "declared",
 70735|       "dll_semantic_verified": null,
 70736|       "dll_verified_status": "signature_verified_declared",
 70737|       "revitlookup_referenced": null,
 70738|       "revitlookup_requires_document_context": null
 70739|     },
 70740|     {
 70741|       "source": "Autodesk.Revit.DB.Electrical.ElectricalLoadClassification",
 70742|       "target": null,
 70743|       "member_name": "DemandFactorId",
 70744|       "member_kind": "property",
 70745|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 70746|       "confidence": "unknown_reference",
 70747|       "confidence_tier": "unverified_reference",
 70748|       "target_resolution": "none",
 70749|       "evidence": [
 70750|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 70751|       ],
 70752|       "source_url": "https://www.revitapidocs.com/2025/ae578380-e6de-dea0-dfe3-27b2bb5a6bc3.htm",
 70753|       "dll_signature_verified": true,
 70754|       "dll_relationship_scope": "declared",
 70755|       "dll_semantic_verified": null,
 70756|       "dll_verified_status": "signature_verified_declared",
 70757|       "revitlookup_referenced": null,
 70758|       "revitlookup_requires_document_context": null
 70759|     },
 70760|     {
 70761|       "source": "Autodesk.Revit.DB.Electrical.ElectricalPerPhaseData",
 70762|       "target": "Autodesk.Revit.DB.Phase",
 70763|       "member_name": "CurrentPhaseA",
 70764|       "member_kind": "property",
 70765|       "edge_type": "ASSIGNED_TO_PHASE",
 70766|       "confidence": "name_only_candidate",
 70767|       "confidence_tier": "likely",
 70768|       "target_resolution": "exact",
 70769|       "evidence": [
 70770|         "member name 'CurrentPhaseA' matches keyword pattern /Phase/ but return type 'double' gives no type-level confirmation"
 70771|       ],
 70772|       "source_url": "https://www.revitapidocs.com/2025/5442d2ed-2005-f8a7-1515-27172a594f82.htm",
 70773|       "dll_signature_verified": true,
 70774|       "dll_relationship_scope": "declared",
 70775|       "dll_semantic_verified": null,
 70776|       "dll_verified_status": "signature_verified_declared",
 70777|       "revitlookup_referenced": null,
 70778|       "revitlookup_requires_document_context": null
 70779|     },
 70780|     {
 70781|       "source": "Autodesk.Revit.DB.Electrical.ElectricalPerPhaseData",
 70782|       "target": "Autodesk.Revit.DB.Phase",
 70783|       "member_name": "CurrentPhaseB",
 70784|       "member_kind": "property",
 70785|       "edge_type": "ASSIGNED_TO_PHASE",
 70786|       "confidence": "name_only_candidate",
 70787|       "confidence_tier": "likely",
 70788|       "target_resolution": "exact",
 70789|       "evidence": [
 70790|         "member name 'CurrentPhaseB' matches keyword pattern /Phase/ but return type 'double' gives no type-level confirmation"
 70791|       ],
 70792|       "source_url": "https://www.revitapidocs.com/2025/05ceb2e5-6fae-e77f-70d1-72dfec57ec93.htm",
 70793|       "dll_signature_verified": true,
 70794|       "dll_relationship_scope": "declared",
 70795|       "dll_semantic_verified": null,
 70796|       "dll_verified_status": "signature_verified_declared",
 70797|       "revitlookup_referenced": null,
 70798|       "revitlookup_requires_document_context": null
 70799|     },
 70800|     {
 70801|       "source": "Autodesk.Revit.DB.Electrical.ElectricalPerPhaseData",
 70802|       "target": "Autodesk.Revit.DB.Phase",
 70803|       "member_name": "CurrentPhaseC",
 70804|       "member_kind": "property",
 70805|       "edge_type": "ASSIGNED_TO_PHASE",
 70806|       "confidence": "name_only_candidate",
 70807|       "confidence_tier": "likely",
 70808|       "target_resolution": "exact",
 70809|       "evidence": [
 70810|         "member name 'CurrentPhaseC' matches keyword pattern /Phase/ but return type 'double' gives no type-level confirmation"
 70811|       ],
 70812|       "source_url": "https://www.revitapidocs.com/2025/3fbed83c-8c43-7222-55de-9eac4da6796b.htm",
 70813|       "dll_signature_verified": true,
 70814|       "dll_relationship_scope": "declared",
 70815|       "dll_semantic_verified": null,
 70816|       "dll_verified_status": "signature_verified_declared",
 70817|       "revitlookup_referenced": null,
 70818|       "revitlookup_requires_document_context": null
 70819|     },
 70820|     {
 70821|       "source": "Autodesk.Revit.DB.Electrical.ElectricalSetting",
 70822|       "target": "Autodesk.Revit.DB.Phase",
 70823|       "member_name": "CircuitNamePhaseA",
 70824|       "member_kind": "property",
 70825|       "edge_type": "ASSIGNED_TO_PHASE",
 70826|       "confidence": "name_only_candidate",
 70827|       "confidence_tier": "likely",
 70828|       "target_resolution": "exact",
 70829|       "evidence": [
 70830|         "member name 'CircuitNamePhaseA' matches keyword pattern /Phase/ but return type 'string' gives no type-level confirmation"
 70831|       ],
 70832|       "source_url": "https://www.revitapidocs.com/2025/f6cce876-b1cd-7884-cba4-06c57f790f8e.htm",
 70833|       "dll_signature_verified": true,
 70834|       "dll_relationship_scope": "declared",
 70835|       "dll_semantic_verified": null,
 70836|       "dll_verified_status": "signature_verified_declared",
 70837|       "revitlookup_referenced": null,
 70838|       "revitlookup_requires_document_context": null
 70839|     },
 70840|     {
 70841|       "source": "Autodesk.Revit.DB.Electrical.ElectricalSetting",
 70842|       "target": "Autodesk.Revit.DB.Phase",
 70843|       "member_name": "CircuitNamePhaseB",
 70844|       "member_kind": "property",
 70845|       "edge_type": "ASSIGNED_TO_PHASE",
 70846|       "confidence": "name_only_candidate",
 70847|       "confidence_tier": "likely",
 70848|       "target_resolution": "exact",
 70849|       "evidence": [
 70850|         "member name 'CircuitNamePhaseB' matches keyword pattern /Phase/ but return type 'string' gives no type-level confirmation"
 70851|       ],
 70852|       "source_url": "https://www.revitapidocs.com/2025/3036634e-8530-fac4-8f76-d08e662ad858.htm",
 70853|       "dll_signature_verified": true,
 70854|       "dll_relationship_scope": "declared",
 70855|       "dll_semantic_verified": null,
 70856|       "dll_verified_status": "signature_verified_declared",
 70857|       "revitlookup_referenced": null,
 70858|       "revitlookup_requires_document_context": null
 70859|     },
 70860|     {
 70861|       "source": "Autodesk.Revit.DB.Electrical.ElectricalSetting",
 70862|       "target": "Autodesk.Revit.DB.Phase",
 70863|       "member_name": "CircuitNamePhaseC",
 70864|       "member_kind": "property",
 70865|       "edge_type": "ASSIGNED_TO_PHASE",
 70866|       "confidence": "name_only_candidate",
 70867|       "confidence_tier": "likely",
 70868|       "target_resolution": "exact",
 70869|       "evidence": [
 70870|         "member name 'CircuitNamePhaseC' matches keyword pattern /Phase/ but return type 'string' gives no type-level confirmation"
 70871|       ],
 70872|       "source_url": "https://www.revitapidocs.com/2025/95cfc9a6-b8d8-1290-ce7a-b43c61cbee96.htm",
 70873|       "dll_signature_verified": true,
 70874|       "dll_relationship_scope": "declared",
 70875|       "dll_semantic_verified": null,
 70876|       "dll_verified_status": "signature_verified_declared",
 70877|       "revitlookup_referenced": null,
 70878|       "revitlookup_requires_document_context": null
 70879|     },
 70880|     {
 70881|       "source": "Autodesk.Revit.DB.Electrical.ElectricalSetting",
 70882|       "target": "Autodesk.Revit.DB.Electrical.DistributionSysTypeSet",
 70883|       "member_name": "DistributionSysTypes",
 70884|       "member_kind": "property",
 70885|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70886|       "confidence": "direct_return_type",
 70887|       "confidence_tier": "unverified_reference",
 70888|       "target_resolution": "short_name_fallback",
 70889|       "evidence": [
 70890|         "return type 'DistributionSysTypeSet' directly names a Revit DB object type"
 70891|       ],
 70892|       "source_url": "https://www.revitapidocs.com/2025/64780ae2-610f-35a1-18be-88b7be2e4abc.htm",
 70893|       "dll_signature_verified": true,
 70894|       "dll_relationship_scope": "declared",
 70895|       "dll_semantic_verified": null,
 70896|       "dll_verified_status": "signature_verified_declared",
 70897|       "revitlookup_referenced": null,
 70898|       "revitlookup_requires_document_context": null
 70899|     },
 70900|     {
 70901|       "source": "Autodesk.Revit.DB.Electrical.ElectricalSetting",
 70902|       "target": "Autodesk.Revit.DB.Electrical.VoltageTypeSet",
 70903|       "member_name": "VoltageTypes",
 70904|       "member_kind": "property",
 70905|       "edge_type": "TAGS_ELEMENT",
 70906|       "confidence": "direct_return_type",
 70907|       "confidence_tier": "core",
 70908|       "target_resolution": "short_name_fallback",
 70909|       "evidence": [
 70910|         "return type 'VoltageTypeSet' directly names a Revit DB object type"
 70911|       ],
 70912|       "source_url": "https://www.revitapidocs.com/2025/537601fb-42b8-f7b0-bd1a-58b4e4adf437.htm",
 70913|       "dll_signature_verified": true,
 70914|       "dll_relationship_scope": "declared",
 70915|       "dll_semantic_verified": null,
 70916|       "dll_verified_status": "signature_verified_declared",
 70917|       "revitlookup_referenced": null,
 70918|       "revitlookup_requires_document_context": null
 70919|     },
 70920|     {
 70921|       "source": "Autodesk.Revit.DB.Electrical.ElectricalSetting",
 70922|       "target": "Autodesk.Revit.DB.Electrical.WireConduitTypeSet",
 70923|       "member_name": "WireConduitTypes",
 70924|       "member_kind": "property",
 70925|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70926|       "confidence": "direct_return_type",
 70927|       "confidence_tier": "unverified_reference",
 70928|       "target_resolution": "short_name_fallback",
 70929|       "evidence": [
 70930|         "return type 'WireConduitTypeSet' directly names a Revit DB object type"
 70931|       ],
 70932|       "source_url": "https://www.revitapidocs.com/2025/62dab13d-2046-6d3d-59af-2a8cf4b7fd74.htm",
 70933|       "dll_signature_verified": true,
 70934|       "dll_relationship_scope": "declared",
 70935|       "dll_semantic_verified": null,
 70936|       "dll_verified_status": "signature_verified_declared",
 70937|       "revitlookup_referenced": null,
 70938|       "revitlookup_requires_document_context": null
 70939|     },
 70940|     {
 70941|       "source": "Autodesk.Revit.DB.Electrical.ElectricalSetting",
 70942|       "target": "Autodesk.Revit.DB.Electrical.WireMaterialTypeSet",
 70943|       "member_name": "WireMaterialTypes",
 70944|       "member_kind": "property",
 70945|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70946|       "confidence": "direct_return_type",
 70947|       "confidence_tier": "unverified_reference",
 70948|       "target_resolution": "short_name_fallback",
 70949|       "evidence": [
 70950|         "member name 'WireMaterialTypes' matches keyword pattern /Material/ implying target 'Material', but the actual return type 'WireMaterialTypeSet' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 70951|         "return type 'WireMaterialTypeSet' directly names a Revit DB object type"
 70952|       ],
 70953|       "source_url": "https://www.revitapidocs.com/2025/dd5bc515-19eb-79b3-a5e3-2c7586b11599.htm",
 70954|       "dll_signature_verified": true,
 70955|       "dll_relationship_scope": "declared",
 70956|       "dll_semantic_verified": null,
 70957|       "dll_verified_status": "signature_verified_declared",
 70958|       "revitlookup_referenced": null,
 70959|       "revitlookup_requires_document_context": null
 70960|     },
 70961|     {
 70962|       "source": "Autodesk.Revit.DB.Electrical.ElectricalSetting",
 70963|       "target": "Autodesk.Revit.DB.Electrical.WireTypeSet",
 70964|       "member_name": "WireTypes",
 70965|       "member_kind": "property",
 70966|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70967|       "confidence": "direct_return_type",
 70968|       "confidence_tier": "unverified_reference",
 70969|       "target_resolution": "short_name_fallback",
 70970|       "evidence": [
 70971|         "return type 'WireTypeSet' directly names a Revit DB object type"
 70972|       ],
 70973|       "source_url": "https://www.revitapidocs.com/2025/2cf6315a-b891-db9d-677a-89a8d73dd7cf.htm",
 70974|       "dll_signature_verified": true,
 70975|       "dll_relationship_scope": "declared",
 70976|       "dll_semantic_verified": null,
 70977|       "dll_verified_status": "signature_verified_declared",
 70978|       "revitlookup_referenced": null,
 70979|       "revitlookup_requires_document_context": null
 70980|     },
 70981|     {
 70982|       "source": "Autodesk.Revit.DB.Electrical.ElectricalSetting",
 70983|       "target": "Autodesk.Revit.DB.Electrical.DistributionSysType",
 70984|       "member_name": "AddDistributionSysType",
 70985|       "member_kind": "method",
 70986|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 70987|       "confidence": "direct_return_type",
 70988|       "confidence_tier": "unverified_reference",
 70989|       "target_resolution": "short_name_fallback",
 70990|       "evidence": [
```
