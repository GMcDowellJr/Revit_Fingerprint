# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 112 of 216
- Original line range: 43291-43690
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 43291|       "revitlookup_referenced": null,
 43292|       "revitlookup_requires_document_context": null
 43293|     },
 43294|     {
 43295|       "source": "Autodesk.Revit.DB.FabricationHostedInfo",
 43296|       "target": null,
 43297|       "member_name": "HostId",
 43298|       "member_kind": "property",
 43299|       "edge_type": "HOSTED_BY",
 43300|       "confidence": "elementid_with_strong_name",
 43301|       "confidence_tier": "core",
 43302|       "target_resolution": "none",
 43303|       "evidence": [
 43304|         "member name 'HostId' matches keyword pattern /^GetHosted|Host/"
 43305|       ],
 43306|       "source_url": "https://www.revitapidocs.com/2025/d48cf763-b47f-1aea-2999-b86e03ff1caf.htm",
 43307|       "dll_signature_verified": true,
 43308|       "dll_relationship_scope": "declared",
 43309|       "dll_semantic_verified": null,
 43310|       "dll_verified_status": "signature_verified_declared",
 43311|       "revitlookup_referenced": null,
 43312|       "revitlookup_requires_document_context": null
 43313|     },
 43314|     {
 43315|       "source": "Autodesk.Revit.DB.FabricationHostedInfo",
 43316|       "target": null,
 43317|       "member_name": "DisconnectFromHost",
 43318|       "member_kind": "method",
 43319|       "edge_type": "HOSTED_BY",
 43320|       "confidence": "name_only_candidate",
 43321|       "confidence_tier": "likely",
 43322|       "target_resolution": "none",
 43323|       "evidence": [
 43324|         "member name 'DisconnectFromHost' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 43325|       ],
 43326|       "source_url": "https://www.revitapidocs.com/2025/22be6751-af02-cda5-11ca-3433835f3e94.htm",
 43327|       "dll_signature_verified": true,
 43328|       "dll_relationship_scope": "declared",
 43329|       "dll_semantic_verified": null,
 43330|       "dll_verified_status": "signature_verified_declared",
 43331|       "revitlookup_referenced": null,
 43332|       "revitlookup_requires_document_context": null
 43333|     },
 43334|     {
 43335|       "source": "Autodesk.Revit.DB.FabricationHostedInfo",
 43336|       "target": "Autodesk.Revit.DB.Line",
 43337|       "member_name": "GetBearerCenterline",
 43338|       "member_kind": "method",
 43339|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 43340|       "confidence": "direct_return_type",
 43341|       "confidence_tier": "unverified_reference",
 43342|       "target_resolution": "exact",
 43343|       "evidence": [
 43344|         "return type 'Line' directly names a Revit DB object type"
 43345|       ],
 43346|       "source_url": "https://www.revitapidocs.com/2025/9fd81153-3d66-f18f-eb43-41bc84284b13.htm",
 43347|       "dll_signature_verified": true,
 43348|       "dll_relationship_scope": "declared",
 43349|       "dll_semantic_verified": null,
 43350|       "dll_verified_status": "signature_verified_declared",
 43351|       "revitlookup_referenced": null,
 43352|       "revitlookup_requires_document_context": null
 43353|     },
 43354|     {
 43355|       "source": "Autodesk.Revit.DB.FabricationHostedInfo",
 43356|       "target": null,
 43357|       "member_name": "PlaceOnHost",
 43358|       "member_kind": "method",
 43359|       "edge_type": "HOSTED_BY",
 43360|       "confidence": "name_only_candidate",
 43361|       "confidence_tier": "likely",
 43362|       "target_resolution": "none",
 43363|       "evidence": [
 43364|         "member name 'PlaceOnHost' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 43365|       ],
 43366|       "source_url": "https://www.revitapidocs.com/2025/66f2c679-d136-7252-d417-5cb122c9840d.htm",
 43367|       "dll_signature_verified": true,
 43368|       "dll_relationship_scope": "declared",
 43369|       "dll_semantic_verified": null,
 43370|       "dll_verified_status": "signature_verified_declared",
 43371|       "revitlookup_referenced": null,
 43372|       "revitlookup_requires_document_context": null
 43373|     },
 43374|     {
 43375|       "source": "Autodesk.Revit.DB.FabricationHostedInfo",
 43376|       "target": null,
 43377|       "member_name": "PlaceOnHost",
 43378|       "member_kind": "method",
 43379|       "edge_type": "HOSTED_BY",
 43380|       "confidence": "name_only_candidate",
 43381|       "confidence_tier": "likely",
 43382|       "target_resolution": "none",
 43383|       "evidence": [
 43384|         "member name 'PlaceOnHost' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 43385|       ],
 43386|       "source_url": "https://www.revitapidocs.com/2025/aba22d62-a05c-0d34-91c8-e2a08041994f.htm",
 43387|       "dll_signature_verified": true,
 43388|       "dll_relationship_scope": "declared",
 43389|       "dll_semantic_verified": null,
 43390|       "dll_verified_status": "signature_verified_declared",
 43391|       "revitlookup_referenced": null,
 43392|       "revitlookup_requires_document_context": null
 43393|     },
 43394|     {
 43395|       "source": "Autodesk.Revit.DB.FabricationItemFolder",
 43396|       "target": "Autodesk.Revit.DB.FabricationItemFile",
 43397|       "member_name": "GetItemFiles",
 43398|       "member_kind": "method",
 43399|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 43400|       "confidence": "needs_runtime_validation",
 43401|       "confidence_tier": "needs_validation",
 43402|       "target_resolution": "exact",
 43403|       "evidence": [
 43404|         "return type 'IList < FabricationItemFile >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 43405|       ],
 43406|       "source_url": "https://www.revitapidocs.com/2025/bc33c7bf-045d-6059-1f33-655c7484ce47.htm",
 43407|       "dll_signature_verified": true,
 43408|       "dll_relationship_scope": "declared",
 43409|       "dll_semantic_verified": null,
 43410|       "dll_verified_status": "signature_verified_declared",
 43411|       "revitlookup_referenced": null,
 43412|       "revitlookup_requires_document_context": null
 43413|     },
 43414|     {
 43415|       "source": "Autodesk.Revit.DB.FabricationItemFolder",
 43416|       "target": "Autodesk.Revit.DB.FabricationItemFolder",
 43417|       "member_name": "GetSubFolders",
 43418|       "member_kind": "method",
 43419|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 43420|       "confidence": "needs_runtime_validation",
 43421|       "confidence_tier": "needs_validation",
 43422|       "target_resolution": "exact",
 43423|       "evidence": [
 43424|         "return type 'IList < FabricationItemFolder >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 43425|       ],
 43426|       "source_url": "https://www.revitapidocs.com/2025/997c0284-416a-68ca-4ff0-cc1afebf05f7.htm",
 43427|       "dll_signature_verified": true,
 43428|       "dll_relationship_scope": "declared",
 43429|       "dll_semantic_verified": null,
 43430|       "dll_verified_status": "signature_verified_declared",
 43431|       "revitlookup_referenced": null,
 43432|       "revitlookup_requires_document_context": null
 43433|     },
 43434|     {
 43435|       "source": "Autodesk.Revit.DB.FabricationPart",
 43436|       "target": "Autodesk.Revit.DB.ConnectorManager",
 43437|       "member_name": "ConnectorManager",
 43438|       "member_kind": "property",
 43439|       "edge_type": "REFERENCES",
 43440|       "confidence": "direct_return_type",
 43441|       "confidence_tier": "core",
 43442|       "target_resolution": "exact",
 43443|       "evidence": [
 43444|         "return type 'ConnectorManager' directly names a Revit DB object type"
 43445|       ],
 43446|       "source_url": "https://www.revitapidocs.com/2025/797ddd82-8847-e4ae-cc7e-879e0708d757.htm",
 43447|       "dll_signature_verified": true,
 43448|       "dll_relationship_scope": "declared",
 43449|       "dll_semantic_verified": null,
 43450|       "dll_verified_status": "signature_verified_declared",
 43451|       "revitlookup_referenced": null,
 43452|       "revitlookup_requires_document_context": null
 43453|     },
 43454|     {
 43455|       "source": "Autodesk.Revit.DB.FabricationPart",
 43456|       "target": "Autodesk.Revit.DB.Material",
 43457|       "member_name": "DoubleWallMaterial",
 43458|       "member_kind": "property",
 43459|       "edge_type": "USES_MATERIAL",
 43460|       "confidence": "name_only_candidate",
 43461|       "confidence_tier": "likely",
 43462|       "target_resolution": "exact",
 43463|       "evidence": [
 43464|         "member name 'DoubleWallMaterial' matches keyword pattern /Material/ but return type 'int' gives no type-level confirmation"
 43465|       ],
 43466|       "source_url": "https://www.revitapidocs.com/2025/c25b92b2-93d0-2aca-b7b2-1b0bdf193e2e.htm",
 43467|       "dll_signature_verified": true,
 43468|       "dll_relationship_scope": "declared",
 43469|       "dll_semantic_verified": null,
 43470|       "dll_verified_status": "signature_verified_declared",
 43471|       "revitlookup_referenced": null,
 43472|       "revitlookup_requires_document_context": null
 43473|     },
 43474|     {
 43475|       "source": "Autodesk.Revit.DB.FabricationPart",
 43476|       "target": "Autodesk.Revit.DB.Material",
 43477|       "member_name": "DoubleWallMaterialArea",
 43478|       "member_kind": "property",
 43479|       "edge_type": "USES_MATERIAL",
 43480|       "confidence": "name_only_candidate",
 43481|       "confidence_tier": "likely",
 43482|       "target_resolution": "exact",
 43483|       "evidence": [
 43484|         "member name 'DoubleWallMaterialArea' matches keyword pattern /Material/ but return type 'double' gives no type-level confirmation"
 43485|       ],
 43486|       "source_url": "https://www.revitapidocs.com/2025/f71c703e-3efb-80a8-9275-c8dd49738af1.htm",
 43487|       "dll_signature_verified": true,
 43488|       "dll_relationship_scope": "declared",
 43489|       "dll_semantic_verified": null,
 43490|       "dll_verified_status": "signature_verified_declared",
 43491|       "revitlookup_referenced": null,
 43492|       "revitlookup_requires_document_context": null
 43493|     },
 43494|     {
 43495|       "source": "Autodesk.Revit.DB.FabricationPart",
 43496|       "target": "Autodesk.Revit.DB.Material",
 43497|       "member_name": "DoubleWallMaterialThickness",
 43498|       "member_kind": "property",
 43499|       "edge_type": "USES_MATERIAL",
 43500|       "confidence": "name_only_candidate",
 43501|       "confidence_tier": "likely",
 43502|       "target_resolution": "exact",
 43503|       "evidence": [
 43504|         "member name 'DoubleWallMaterialThickness' matches keyword pattern /Material/ but return type 'double' gives no type-level confirmation"
 43505|       ],
 43506|       "source_url": "https://www.revitapidocs.com/2025/a9971533-bb60-157c-97fc-66b04b8c27d7.htm",
 43507|       "dll_signature_verified": true,
 43508|       "dll_relationship_scope": "declared",
 43509|       "dll_semantic_verified": null,
 43510|       "dll_verified_status": "signature_verified_declared",
 43511|       "revitlookup_referenced": null,
 43512|       "revitlookup_requires_document_context": null
 43513|     },
 43514|     {
 43515|       "source": "Autodesk.Revit.DB.FabricationPart",
 43516|       "target": "Autodesk.Revit.DB.Visual.AssetPropertyUInt64",
 43517|       "member_name": "GeometryChecksum",
 43518|       "member_kind": "property",
 43519|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 43520|       "confidence": "direct_return_type",
 43521|       "confidence_tier": "unverified_reference",
 43522|       "target_resolution": "short_name_fallback",
 43523|       "evidence": [
 43524|         "return type 'AssetPropertyUInt64' directly names a Revit DB object type"
 43525|       ],
 43526|       "source_url": "https://www.revitapidocs.com/2025/1f839e5c-1c4a-f4de-94d5-4662f38c18f3.htm",
 43527|       "dll_signature_verified": true,
 43528|       "dll_relationship_scope": "declared",
 43529|       "dll_semantic_verified": null,
 43530|       "dll_verified_status": "signature_verified_declared",
 43531|       "revitlookup_referenced": null,
 43532|       "revitlookup_requires_document_context": null
 43533|     },
 43534|     {
 43535|       "source": "Autodesk.Revit.DB.FabricationPart",
 43536|       "target": "Autodesk.Revit.DB.Level",
 43537|       "member_name": "LevelOffset",
 43538|       "member_kind": "property",
 43539|       "edge_type": "ASSIGNED_TO_LEVEL",
 43540|       "confidence": "name_only_candidate",
 43541|       "confidence_tier": "likely",
 43542|       "target_resolution": "exact",
 43543|       "evidence": [
 43544|         "member name 'LevelOffset' matches keyword pattern /Level/ but return type 'double' gives no type-level confirmation"
 43545|       ],
 43546|       "source_url": "https://www.revitapidocs.com/2025/865f446a-2f1b-9136-6cec-1a4d30cfb864.htm",
 43547|       "dll_signature_verified": true,
 43548|       "dll_relationship_scope": "declared",
 43549|       "dll_semantic_verified": null,
 43550|       "dll_verified_status": "signature_verified_declared",
 43551|       "revitlookup_referenced": null,
 43552|       "revitlookup_requires_document_context": null
 43553|     },
 43554|     {
 43555|       "source": "Autodesk.Revit.DB.FabricationPart",
 43556|       "target": "Autodesk.Revit.DB.Material",
 43557|       "member_name": "Material",
 43558|       "member_kind": "property",
 43559|       "edge_type": "USES_MATERIAL",
 43560|       "confidence": "name_only_candidate",
 43561|       "confidence_tier": "likely",
 43562|       "target_resolution": "exact",
 43563|       "evidence": [
 43564|         "member name 'Material' matches keyword pattern /Material/ but return type 'int' gives no type-level confirmation"
 43565|       ],
 43566|       "source_url": "https://www.revitapidocs.com/2025/d86b29d3-b030-5f11-4bf3-c5ef677918b4.htm",
 43567|       "dll_signature_verified": true,
 43568|       "dll_relationship_scope": "declared",
 43569|       "dll_semantic_verified": null,
 43570|       "dll_verified_status": "signature_verified_declared",
 43571|       "revitlookup_referenced": null,
 43572|       "revitlookup_requires_document_context": null
 43573|     },
 43574|     {
 43575|       "source": "Autodesk.Revit.DB.FabricationPart",
 43576|       "target": "Autodesk.Revit.DB.Material",
 43577|       "member_name": "MaterialGauge",
 43578|       "member_kind": "property",
 43579|       "edge_type": "USES_MATERIAL",
 43580|       "confidence": "name_only_candidate",
 43581|       "confidence_tier": "likely",
 43582|       "target_resolution": "exact",
 43583|       "evidence": [
 43584|         "member name 'MaterialGauge' matches keyword pattern /Material/ but return type 'int' gives no type-level confirmation"
 43585|       ],
 43586|       "source_url": "https://www.revitapidocs.com/2025/0392e62d-e827-522a-9c4f-39efd42b0b16.htm",
 43587|       "dll_signature_verified": true,
 43588|       "dll_relationship_scope": "declared",
 43589|       "dll_semantic_verified": null,
 43590|       "dll_verified_status": "signature_verified_declared",
 43591|       "revitlookup_referenced": null,
 43592|       "revitlookup_requires_document_context": null
 43593|     },
 43594|     {
 43595|       "source": "Autodesk.Revit.DB.FabricationPart",
 43596|       "target": "Autodesk.Revit.DB.Material",
 43597|       "member_name": "MaterialThickness",
 43598|       "member_kind": "property",
 43599|       "edge_type": "USES_MATERIAL",
 43600|       "confidence": "name_only_candidate",
 43601|       "confidence_tier": "likely",
 43602|       "target_resolution": "exact",
 43603|       "evidence": [
 43604|         "member name 'MaterialThickness' matches keyword pattern /Material/ but return type 'double' gives no type-level confirmation"
 43605|       ],
 43606|       "source_url": "https://www.revitapidocs.com/2025/8e98353f-a0db-1074-690c-94196f03419e.htm",
 43607|       "dll_signature_verified": true,
 43608|       "dll_relationship_scope": "declared",
 43609|       "dll_semantic_verified": null,
 43610|       "dll_verified_status": "signature_verified_declared",
 43611|       "revitlookup_referenced": null,
 43612|       "revitlookup_requires_document_context": null
 43613|     },
 43614|     {
 43615|       "source": "Autodesk.Revit.DB.FabricationPart",
 43616|       "target": "Autodesk.Revit.DB.Material",
 43617|       "member_name": "ProductMaterialDescription",
 43618|       "member_kind": "property",
 43619|       "edge_type": "USES_MATERIAL",
 43620|       "confidence": "name_only_candidate",
 43621|       "confidence_tier": "likely",
 43622|       "target_resolution": "exact",
 43623|       "evidence": [
 43624|         "member name 'ProductMaterialDescription' matches keyword pattern /Material/ but return type 'string' gives no type-level confirmation"
 43625|       ],
 43626|       "source_url": "https://www.revitapidocs.com/2025/956f1a31-50a6-33f8-a135-86d8c6e7f6b2.htm",
 43627|       "dll_signature_verified": true,
 43628|       "dll_relationship_scope": "declared",
 43629|       "dll_semantic_verified": null,
 43630|       "dll_verified_status": "signature_verified_declared",
 43631|       "revitlookup_referenced": null,
 43632|       "revitlookup_requires_document_context": null
 43633|     },
 43634|     {
 43635|       "source": "Autodesk.Revit.DB.FabricationPart",
 43636|       "target": "Autodesk.Revit.DB.ViewSheet",
 43637|       "member_name": "SheetMetalArea",
 43638|       "member_kind": "property",
 43639|       "edge_type": "PLACED_ON_SHEET",
 43640|       "confidence": "name_only_candidate",
 43641|       "confidence_tier": "likely",
 43642|       "target_resolution": "exact",
 43643|       "evidence": [
 43644|         "member name 'SheetMetalArea' matches keyword pattern /Sheet/ but return type 'double' gives no type-level confirmation"
 43645|       ],
 43646|       "source_url": "https://www.revitapidocs.com/2025/f66b0b5c-531e-cc49-d902-d19cea6e34ac.htm",
 43647|       "dll_signature_verified": true,
 43648|       "dll_relationship_scope": "declared",
 43649|       "dll_semantic_verified": null,
 43650|       "dll_verified_status": "signature_verified_declared",
 43651|       "revitlookup_referenced": null,
 43652|       "revitlookup_requires_document_context": null
 43653|     },
 43654|     {
 43655|       "source": "Autodesk.Revit.DB.FabricationPart",
 43656|       "target": "Autodesk.Revit.DB.FabricationDimensionDefinition",
 43657|       "member_name": "GetDimensions",
 43658|       "member_kind": "method",
 43659|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 43660|       "confidence": "needs_runtime_validation",
 43661|       "confidence_tier": "needs_validation",
 43662|       "target_resolution": "exact",
 43663|       "evidence": [
 43664|         "return type 'IList < FabricationDimensionDefinition >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 43665|       ],
 43666|       "source_url": "https://www.revitapidocs.com/2025/e7cb5e57-cfb5-e1d9-8d98-6c99373de04e.htm",
 43667|       "dll_signature_verified": true,
 43668|       "dll_relationship_scope": "declared",
 43669|       "dll_semantic_verified": null,
 43670|       "dll_verified_status": "signature_verified_declared",
 43671|       "revitlookup_referenced": null,
 43672|       "revitlookup_requires_document_context": null
 43673|     },
 43674|     {
 43675|       "source": "Autodesk.Revit.DB.FabricationPart",
 43676|       "target": "Autodesk.Revit.DB.FabricationHostedInfo",
 43677|       "member_name": "GetHostedInfo",
 43678|       "member_kind": "method",
 43679|       "edge_type": "HOSTED_BY",
 43680|       "confidence": "direct_return_type",
 43681|       "confidence_tier": "core",
 43682|       "target_resolution": "exact",
 43683|       "evidence": [
 43684|         "return type 'FabricationHostedInfo' directly names a Revit DB object type"
 43685|       ],
 43686|       "source_url": "https://www.revitapidocs.com/2025/e11c4774-dc2e-0b85-5511-503d8aabf764.htm",
 43687|       "dll_signature_verified": true,
 43688|       "dll_relationship_scope": "declared",
 43689|       "dll_semantic_verified": null,
 43690|       "dll_verified_status": "signature_verified_declared",
```
