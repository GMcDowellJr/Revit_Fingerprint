# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 194 of 216
- Original line range: 75271-75670
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 75271|       "member_kind": "method",
 75272|       "edge_type": "MEMBER_OF_GROUP",
 75273|       "confidence": "name_only_candidate",
 75274|       "confidence_tier": "likely",
 75275|       "target_resolution": "none",
 75276|       "evidence": [
 75277|         "member name 'IsLightGroupOn' matches keyword pattern /^GetMember|Group/ but return type 'bool' gives no type-level confirmation"
 75278|       ],
 75279|       "source_url": "https://www.revitapidocs.com/2025/3214ec82-7ec9-ecab-e687-4e282ffe57b5.htm",
 75280|       "dll_signature_verified": true,
 75281|       "dll_relationship_scope": "declared",
 75282|       "dll_semantic_verified": null,
 75283|       "dll_verified_status": "signature_verified_declared",
 75284|       "revitlookup_referenced": null,
 75285|       "revitlookup_requires_document_context": null
 75286|     },
 75287|     {
 75288|       "source": "Autodesk.Revit.DB.Lighting.LightGroupManager",
 75289|       "target": null,
 75290|       "member_name": "SetLightGroupOn",
 75291|       "member_kind": "method",
 75292|       "edge_type": "MEMBER_OF_GROUP",
 75293|       "confidence": "name_only_candidate",
 75294|       "confidence_tier": "likely",
 75295|       "target_resolution": "none",
 75296|       "evidence": [
 75297|         "member name 'SetLightGroupOn' matches keyword pattern /^GetMember|Group/ but return type 'void' gives no type-level confirmation"
 75298|       ],
 75299|       "source_url": "https://www.revitapidocs.com/2025/33b097c4-15ee-ffec-e9a9-0efdec482e12.htm",
 75300|       "dll_signature_verified": true,
 75301|       "dll_relationship_scope": "declared",
 75302|       "dll_semantic_verified": null,
 75303|       "dll_verified_status": "signature_verified_declared",
 75304|       "revitlookup_referenced": null,
 75305|       "revitlookup_requires_document_context": null
 75306|     },
 75307|     {
 75308|       "source": "Autodesk.Revit.DB.Lighting.LightType",
 75309|       "target": "Autodesk.Revit.DB.Lighting.InitialColor",
 75310|       "member_name": "GetInitialColor",
 75311|       "member_kind": "method",
 75312|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75313|       "confidence": "direct_return_type",
 75314|       "confidence_tier": "unverified_reference",
 75315|       "target_resolution": "short_name_fallback",
 75316|       "evidence": [
 75317|         "return type 'InitialColor' directly names a Revit DB object type"
 75318|       ],
 75319|       "source_url": "https://www.revitapidocs.com/2025/e0fdfc8c-c842-1291-3993-e66efa501953.htm",
 75320|       "dll_signature_verified": true,
 75321|       "dll_relationship_scope": "declared",
 75322|       "dll_semantic_verified": null,
 75323|       "dll_verified_status": "signature_verified_declared",
 75324|       "revitlookup_referenced": null,
 75325|       "revitlookup_requires_document_context": null
 75326|     },
 75327|     {
 75328|       "source": "Autodesk.Revit.DB.Lighting.LightType",
 75329|       "target": "Autodesk.Revit.DB.Lighting.InitialIntensity",
 75330|       "member_name": "GetInitialIntensity",
 75331|       "member_kind": "method",
 75332|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75333|       "confidence": "direct_return_type",
 75334|       "confidence_tier": "unverified_reference",
 75335|       "target_resolution": "short_name_fallback",
 75336|       "evidence": [
 75337|         "return type 'InitialIntensity' directly names a Revit DB object type"
 75338|       ],
 75339|       "source_url": "https://www.revitapidocs.com/2025/3ac41b1a-a2a8-c15f-6bba-eb41e48006c6.htm",
 75340|       "dll_signature_verified": true,
 75341|       "dll_relationship_scope": "declared",
 75342|       "dll_semantic_verified": null,
 75343|       "dll_verified_status": "signature_verified_declared",
 75344|       "revitlookup_referenced": null,
 75345|       "revitlookup_requires_document_context": null
 75346|     },
 75347|     {
 75348|       "source": "Autodesk.Revit.DB.Lighting.LightType",
 75349|       "target": "Autodesk.Revit.DB.Lighting.LightDistribution",
 75350|       "member_name": "GetLightDistribution",
 75351|       "member_kind": "method",
 75352|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75353|       "confidence": "direct_return_type",
 75354|       "confidence_tier": "unverified_reference",
 75355|       "target_resolution": "short_name_fallback",
 75356|       "evidence": [
 75357|         "return type 'LightDistribution' directly names a Revit DB object type"
 75358|       ],
 75359|       "source_url": "https://www.revitapidocs.com/2025/8c915d67-4a0c-3a92-36f3-64cdba5f59a5.htm",
 75360|       "dll_signature_verified": true,
 75361|       "dll_relationship_scope": "declared",
 75362|       "dll_semantic_verified": null,
 75363|       "dll_verified_status": "signature_verified_declared",
 75364|       "revitlookup_referenced": null,
 75365|       "revitlookup_requires_document_context": null
 75366|     },
 75367|     {
 75368|       "source": "Autodesk.Revit.DB.Lighting.LightType",
 75369|       "target": "Autodesk.Revit.DB.Lighting.LightShape",
 75370|       "member_name": "GetLightShape",
 75371|       "member_kind": "method",
 75372|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75373|       "confidence": "direct_return_type",
 75374|       "confidence_tier": "unverified_reference",
 75375|       "target_resolution": "short_name_fallback",
 75376|       "evidence": [
 75377|         "return type 'LightShape' directly names a Revit DB object type"
 75378|       ],
 75379|       "source_url": "https://www.revitapidocs.com/2025/0686aa9f-7b29-3d3c-b17f-926c96750cde.htm",
 75380|       "dll_signature_verified": true,
 75381|       "dll_relationship_scope": "declared",
 75382|       "dll_semantic_verified": null,
 75383|       "dll_verified_status": "signature_verified_declared",
 75384|       "revitlookup_referenced": null,
 75385|       "revitlookup_requires_document_context": null
 75386|     },
 75387|     {
 75388|       "source": "Autodesk.Revit.DB.Lighting.LightType",
 75389|       "target": "Autodesk.Revit.DB.Lighting.LossFactor",
 75390|       "member_name": "GetLossFactor",
 75391|       "member_kind": "method",
 75392|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75393|       "confidence": "direct_return_type",
 75394|       "confidence_tier": "unverified_reference",
 75395|       "target_resolution": "short_name_fallback",
 75396|       "evidence": [
 75397|         "return type 'LossFactor' directly names a Revit DB object type"
 75398|       ],
 75399|       "source_url": "https://www.revitapidocs.com/2025/70ea1fae-a218-8367-25ca-a9fa13237b70.htm",
 75400|       "dll_signature_verified": true,
 75401|       "dll_relationship_scope": "declared",
 75402|       "dll_semantic_verified": null,
 75403|       "dll_verified_status": "signature_verified_declared",
 75404|       "revitlookup_referenced": null,
 75405|       "revitlookup_requires_document_context": null
 75406|     },
 75407|     {
 75408|       "source": "Autodesk.Revit.DB.Macros.Macro",
 75409|       "target": "Autodesk.Revit.DB.Macros.MacroModule",
 75410|       "member_name": "MacroModule",
 75411|       "member_kind": "property",
 75412|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75413|       "confidence": "direct_return_type",
 75414|       "confidence_tier": "unverified_reference",
 75415|       "target_resolution": "short_name_fallback",
 75416|       "evidence": [
 75417|         "return type 'MacroModule' directly names a Revit DB object type"
 75418|       ],
 75419|       "source_url": "https://www.revitapidocs.com/2025/8693fc15-381a-4f24-51fe-f539c0c3ec9e.htm",
 75420|       "dll_signature_verified": true,
 75421|       "dll_relationship_scope": "declared",
 75422|       "dll_semantic_verified": null,
 75423|       "dll_verified_status": "signature_verified_declared",
 75424|       "revitlookup_referenced": null,
 75425|       "revitlookup_requires_document_context": null
 75426|     },
 75427|     {
 75428|       "source": "Autodesk.Revit.DB.Macros.MacroManager",
 75429|       "target": "Autodesk.Revit.DB.Macros.MacroModule",
 75430|       "member_name": "AddModule",
 75431|       "member_kind": "method",
 75432|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75433|       "confidence": "direct_return_type",
 75434|       "confidence_tier": "unverified_reference",
 75435|       "target_resolution": "short_name_fallback",
 75436|       "evidence": [
 75437|         "return type 'MacroModule' directly names a Revit DB object type"
 75438|       ],
 75439|       "source_url": "https://www.revitapidocs.com/2025/009d1b16-eb01-913b-51cf-b77ac907833d.htm",
 75440|       "dll_signature_verified": true,
 75441|       "dll_relationship_scope": "declared",
 75442|       "dll_semantic_verified": null,
 75443|       "dll_verified_status": "signature_verified_declared",
 75444|       "revitlookup_referenced": null,
 75445|       "revitlookup_requires_document_context": null
 75446|     },
 75447|     {
 75448|       "source": "Autodesk.Revit.DB.Macros.MacroManager",
 75449|       "target": "Autodesk.Revit.DB.Macros.MacroManagerIterator",
 75450|       "member_name": "GetMacroManagerIterator",
 75451|       "member_kind": "method",
 75452|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75453|       "confidence": "direct_return_type",
 75454|       "confidence_tier": "unverified_reference",
 75455|       "target_resolution": "short_name_fallback",
 75456|       "evidence": [
 75457|         "return type 'MacroManagerIterator' directly names a Revit DB object type"
 75458|       ],
 75459|       "source_url": "https://www.revitapidocs.com/2025/00849391-fb3b-9381-4b9a-af908024c311.htm",
 75460|       "dll_signature_verified": true,
 75461|       "dll_relationship_scope": "declared",
 75462|       "dll_semantic_verified": null,
 75463|       "dll_verified_status": "signature_verified_declared",
 75464|       "revitlookup_referenced": null,
 75465|       "revitlookup_requires_document_context": null
 75466|     },
 75467|     {
 75468|       "source": "Autodesk.Revit.DB.Macros.MacroManagerIterator",
 75469|       "target": "Autodesk.Revit.DB.Macros.MacroModule",
 75470|       "member_name": "Current",
 75471|       "member_kind": "property",
 75472|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75473|       "confidence": "direct_return_type",
 75474|       "confidence_tier": "unverified_reference",
 75475|       "target_resolution": "short_name_fallback",
 75476|       "evidence": [
 75477|         "return type 'MacroModule' directly names a Revit DB object type"
 75478|       ],
 75479|       "source_url": "https://www.revitapidocs.com/2025/1d5dc2e5-fd8a-c30e-0d57-488d747f5d41.htm",
 75480|       "dll_signature_verified": true,
 75481|       "dll_relationship_scope": "declared",
 75482|       "dll_semantic_verified": null,
 75483|       "dll_verified_status": "signature_verified_declared",
 75484|       "revitlookup_referenced": null,
 75485|       "revitlookup_requires_document_context": null
 75486|     },
 75487|     {
 75488|       "source": "Autodesk.Revit.DB.Macros.MacroManagerIterator",
 75489|       "target": "Autodesk.Revit.DB.Macros.MacroModule",
 75490|       "member_name": "GetCurrent",
 75491|       "member_kind": "method",
 75492|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75493|       "confidence": "direct_return_type",
 75494|       "confidence_tier": "unverified_reference",
 75495|       "target_resolution": "short_name_fallback",
 75496|       "evidence": [
 75497|         "return type 'MacroModule' directly names a Revit DB object type"
 75498|       ],
 75499|       "source_url": "https://www.revitapidocs.com/2025/37c4ffd8-0c6b-b861-8c07-921e7463a834.htm",
 75500|       "dll_signature_verified": true,
 75501|       "dll_relationship_scope": "declared",
 75502|       "dll_semantic_verified": null,
 75503|       "dll_verified_status": "signature_verified_declared",
 75504|       "revitlookup_referenced": null,
 75505|       "revitlookup_requires_document_context": null
 75506|     },
 75507|     {
 75508|       "source": "Autodesk.Revit.DB.Macros.MacroModule",
 75509|       "target": "Autodesk.Revit.DB.Macros.MacroManager",
 75510|       "member_name": "MacroManager",
 75511|       "member_kind": "property",
 75512|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75513|       "confidence": "direct_return_type",
 75514|       "confidence_tier": "unverified_reference",
 75515|       "target_resolution": "short_name_fallback",
 75516|       "evidence": [
 75517|         "return type 'MacroManager' directly names a Revit DB object type"
 75518|       ],
 75519|       "source_url": "https://www.revitapidocs.com/2025/27324968-a822-dda4-c8ea-31ce7593fd8a.htm",
 75520|       "dll_signature_verified": true,
 75521|       "dll_relationship_scope": "declared",
 75522|       "dll_semantic_verified": null,
 75523|       "dll_verified_status": "signature_verified_declared",
 75524|       "revitlookup_referenced": null,
 75525|       "revitlookup_requires_document_context": null
 75526|     },
 75527|     {
 75528|       "source": "Autodesk.Revit.DB.Macros.MacroModule",
 75529|       "target": "Autodesk.Revit.DB.Macros.Macro",
 75530|       "member_name": "GetMacro",
 75531|       "member_kind": "method",
 75532|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75533|       "confidence": "direct_return_type",
 75534|       "confidence_tier": "unverified_reference",
 75535|       "target_resolution": "short_name_fallback",
 75536|       "evidence": [
 75537|         "return type 'Macro' directly names a Revit DB object type"
 75538|       ],
 75539|       "source_url": "https://www.revitapidocs.com/2025/c19af6ed-efb5-3d99-859f-5b82b73c3548.htm",
 75540|       "dll_signature_verified": true,
 75541|       "dll_relationship_scope": "declared",
 75542|       "dll_semantic_verified": null,
 75543|       "dll_verified_status": "signature_verified_declared",
 75544|       "revitlookup_referenced": null,
 75545|       "revitlookup_requires_document_context": null
 75546|     },
 75547|     {
 75548|       "source": "Autodesk.Revit.DB.Macros.MacroModule",
 75549|       "target": "Autodesk.Revit.DB.Macros.MacroModuleIterator",
 75550|       "member_name": "GetMacroModuleIterator",
 75551|       "member_kind": "method",
 75552|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75553|       "confidence": "direct_return_type",
 75554|       "confidence_tier": "unverified_reference",
 75555|       "target_resolution": "short_name_fallback",
 75556|       "evidence": [
 75557|         "return type 'MacroModuleIterator' directly names a Revit DB object type"
 75558|       ],
 75559|       "source_url": "https://www.revitapidocs.com/2025/852610f9-f354-fe96-5bfd-a0821ada7fa0.htm",
 75560|       "dll_signature_verified": true,
 75561|       "dll_relationship_scope": "declared",
 75562|       "dll_semantic_verified": null,
 75563|       "dll_verified_status": "signature_verified_declared",
 75564|       "revitlookup_referenced": null,
 75565|       "revitlookup_requires_document_context": null
 75566|     },
 75567|     {
 75568|       "source": "Autodesk.Revit.DB.Macros.MacroModuleIterator",
 75569|       "target": "Autodesk.Revit.DB.Macros.Macro",
 75570|       "member_name": "Current",
 75571|       "member_kind": "property",
 75572|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75573|       "confidence": "direct_return_type",
 75574|       "confidence_tier": "unverified_reference",
 75575|       "target_resolution": "short_name_fallback",
 75576|       "evidence": [
 75577|         "return type 'Macro' directly names a Revit DB object type"
 75578|       ],
 75579|       "source_url": "https://www.revitapidocs.com/2025/423853c6-d711-2a8d-7cce-9df6b3f3e2be.htm",
 75580|       "dll_signature_verified": true,
 75581|       "dll_relationship_scope": "declared",
 75582|       "dll_semantic_verified": null,
 75583|       "dll_verified_status": "signature_verified_declared",
 75584|       "revitlookup_referenced": null,
 75585|       "revitlookup_requires_document_context": null
 75586|     },
 75587|     {
 75588|       "source": "Autodesk.Revit.DB.Macros.MacroModuleIterator",
 75589|       "target": "Autodesk.Revit.DB.Macros.Macro",
 75590|       "member_name": "GetCurrent",
 75591|       "member_kind": "method",
 75592|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75593|       "confidence": "direct_return_type",
 75594|       "confidence_tier": "unverified_reference",
 75595|       "target_resolution": "short_name_fallback",
 75596|       "evidence": [
 75597|         "return type 'Macro' directly names a Revit DB object type"
 75598|       ],
 75599|       "source_url": "https://www.revitapidocs.com/2025/7a448a5d-472f-6573-ef92-4cad6e86dfdf.htm",
 75600|       "dll_signature_verified": true,
 75601|       "dll_relationship_scope": "declared",
 75602|       "dll_semantic_verified": null,
 75603|       "dll_verified_status": "signature_verified_declared",
 75604|       "revitlookup_referenced": null,
 75605|       "revitlookup_requires_document_context": null
 75606|     },
 75607|     {
 75608|       "source": "Autodesk.Revit.DB.Mechanical.AirSystemData",
 75609|       "target": null,
 75610|       "member_name": "ChilledWaterLoopId",
 75611|       "member_kind": "property",
 75612|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 75613|       "confidence": "unknown_reference",
 75614|       "confidence_tier": "unverified_reference",
 75615|       "target_resolution": "none",
 75616|       "evidence": [
 75617|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 75618|       ],
 75619|       "source_url": "https://www.revitapidocs.com/2025/d26b6b0b-a01e-5da8-f462-0617cfd7ee28.htm",
 75620|       "dll_signature_verified": true,
 75621|       "dll_relationship_scope": "declared",
 75622|       "dll_semantic_verified": null,
 75623|       "dll_verified_status": "signature_verified_declared",
 75624|       "revitlookup_referenced": null,
 75625|       "revitlookup_requires_document_context": null
 75626|     },
 75627|     {
 75628|       "source": "Autodesk.Revit.DB.Mechanical.AirSystemData",
 75629|       "target": null,
 75630|       "member_name": "HeatingHotWaterLoopId",
 75631|       "member_kind": "property",
 75632|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 75633|       "confidence": "unknown_reference",
 75634|       "confidence_tier": "unverified_reference",
 75635|       "target_resolution": "none",
 75636|       "evidence": [
 75637|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 75638|       ],
 75639|       "source_url": "https://www.revitapidocs.com/2025/5271439e-4ff2-11ba-d5b8-9ec0e0621963.htm",
 75640|       "dll_signature_verified": true,
 75641|       "dll_relationship_scope": "declared",
 75642|       "dll_semantic_verified": null,
 75643|       "dll_verified_status": "signature_verified_declared",
 75644|       "revitlookup_referenced": null,
 75645|       "revitlookup_requires_document_context": null
 75646|     },
 75647|     {
 75648|       "source": "Autodesk.Revit.DB.Mechanical.AirSystemData",
 75649|       "target": null,
 75650|       "member_name": "PreheatHotWaterLoopId",
 75651|       "member_kind": "property",
 75652|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 75653|       "confidence": "unknown_reference",
 75654|       "confidence_tier": "unverified_reference",
 75655|       "target_resolution": "none",
 75656|       "evidence": [
 75657|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 75658|       ],
 75659|       "source_url": "https://www.revitapidocs.com/2025/402223f4-7f05-ce66-bed6-94821f07b7e5.htm",
 75660|       "dll_signature_verified": true,
 75661|       "dll_relationship_scope": "declared",
 75662|       "dll_semantic_verified": null,
 75663|       "dll_verified_status": "signature_verified_declared",
 75664|       "revitlookup_referenced": null,
 75665|       "revitlookup_requires_document_context": null
 75666|     },
 75667|     {
 75668|       "source": "Autodesk.Revit.DB.Mechanical.Duct",
 75669|       "target": "Autodesk.Revit.DB.Mechanical.DuctType",
 75670|       "member_name": "DuctType",
```
