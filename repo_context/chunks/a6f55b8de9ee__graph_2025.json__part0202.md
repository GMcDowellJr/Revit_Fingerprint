# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 202 of 216
- Original line range: 78391-78790
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 78391|       "member_kind": "property",
 78392|       "edge_type": "HOSTED_BY",
 78393|       "confidence": "elementid_with_strong_name",
 78394|       "confidence_tier": "core",
 78395|       "target_resolution": "none",
 78396|       "evidence": [
 78397|         "member name 'HostId' matches keyword pattern /^GetHosted|Host/"
 78398|       ],
 78399|       "source_url": "https://www.revitapidocs.com/2025/f1c5db9c-4dfa-6f9e-3248-056b1460442a.htm",
 78400|       "dll_signature_verified": true,
 78401|       "dll_relationship_scope": "declared",
 78402|       "dll_semantic_verified": null,
 78403|       "dll_verified_status": "signature_verified_declared",
 78404|       "revitlookup_referenced": null,
 78405|       "revitlookup_requires_document_context": null
 78406|     },
 78407|     {
 78408|       "source": "Autodesk.Revit.DB.Structure.FabricSheet",
 78409|       "target": "Autodesk.Revit.DB.Sketch",
 78410|       "member_name": "SketchId",
 78411|       "member_kind": "property",
 78412|       "edge_type": "DEPENDS_ON",
 78413|       "confidence": "elementid_with_strong_name",
 78414|       "confidence_tier": "core",
 78415|       "target_resolution": "exact",
 78416|       "evidence": [
 78417|         "member name 'SketchId' matches keyword pattern /Sketch(Id)?$/"
 78418|       ],
 78419|       "source_url": "https://www.revitapidocs.com/2025/9ad2ae97-2292-14f2-4d66-085ca70ec371.htm",
 78420|       "dll_signature_verified": true,
 78421|       "dll_relationship_scope": "declared",
 78422|       "dll_semantic_verified": null,
 78423|       "dll_verified_status": "signature_verified_declared",
 78424|       "revitlookup_referenced": null,
 78425|       "revitlookup_requires_document_context": null
 78426|     },
 78427|     {
 78428|       "source": "Autodesk.Revit.DB.Structure.FabricSheet",
 78429|       "target": "Autodesk.Revit.DB.Structure.FabricRoundingManager",
 78430|       "member_name": "GetReinforcementRoundingManager",
 78431|       "member_kind": "method",
 78432|       "edge_type": "REFERENCES",
 78433|       "confidence": "direct_return_type",
 78434|       "confidence_tier": "core",
 78435|       "target_resolution": "short_name_fallback",
 78436|       "evidence": [
 78437|         "return type 'FabricRoundingManager' directly names a Revit DB object type"
 78438|       ],
 78439|       "source_url": "https://www.revitapidocs.com/2025/6dc752d4-a675-2f4f-03fd-42a58620dae3.htm",
 78440|       "dll_signature_verified": true,
 78441|       "dll_relationship_scope": "declared",
 78442|       "dll_semantic_verified": null,
 78443|       "dll_verified_status": "signature_verified_declared",
 78444|       "revitlookup_referenced": null,
 78445|       "revitlookup_requires_document_context": null
 78446|     },
 78447|     {
 78448|       "source": "Autodesk.Revit.DB.Structure.FabricSheet",
 78449|       "target": null,
 78450|       "member_name": "GetSegmentParameterIdsAndLengths",
 78451|       "member_kind": "method",
 78452|       "edge_type": "HAS_PARAMETER",
 78453|       "confidence": "name_only_candidate",
 78454|       "confidence_tier": "likely",
 78455|       "target_resolution": "none",
 78456|       "evidence": [
 78457|         "member name 'GetSegmentParameterIdsAndLengths' matches keyword pattern /Parameter/ but return type 'IDictionary < ElementId , double >' gives no type-level confirmation"
 78458|       ],
 78459|       "source_url": "https://www.revitapidocs.com/2025/1542c812-735a-0483-e5b5-7f02bc16b2f9.htm",
 78460|       "dll_signature_verified": true,
 78461|       "dll_relationship_scope": "declared",
 78462|       "dll_semantic_verified": null,
 78463|       "dll_verified_status": "signature_verified_declared",
 78464|       "revitlookup_referenced": null,
 78465|       "revitlookup_requires_document_context": null
 78466|     },
 78467|     {
 78468|       "source": "Autodesk.Revit.DB.Structure.FabricSheet",
 78469|       "target": "Autodesk.Revit.DB.ViewSheet",
 78470|       "member_name": "IsSingleFabricSheetWithinHost",
 78471|       "member_kind": "method",
 78472|       "edge_type": "PLACED_ON_SHEET",
 78473|       "confidence": "name_only_candidate",
 78474|       "confidence_tier": "likely",
 78475|       "target_resolution": "exact",
 78476|       "evidence": [
 78477|         "member name 'IsSingleFabricSheetWithinHost' matches keyword pattern /Sheet/ but return type 'bool' gives no type-level confirmation"
 78478|       ],
 78479|       "source_url": "https://www.revitapidocs.com/2025/d0e85e9c-31f1-6ee4-c98b-8c124cc33d9c.htm",
 78480|       "dll_signature_verified": true,
 78481|       "dll_relationship_scope": "declared",
 78482|       "dll_semantic_verified": null,
 78483|       "dll_verified_status": "signature_verified_declared",
 78484|       "revitlookup_referenced": null,
 78485|       "revitlookup_requires_document_context": null
 78486|     },
 78487|     {
 78488|       "source": "Autodesk.Revit.DB.Structure.FabricSheet",
 78489|       "target": null,
 78490|       "member_name": "IsValidHost",
 78491|       "member_kind": "method",
 78492|       "edge_type": "HOSTED_BY",
 78493|       "confidence": "name_only_candidate",
 78494|       "confidence_tier": "likely",
 78495|       "target_resolution": "none",
 78496|       "evidence": [
 78497|         "member name 'IsValidHost' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 78498|       ],
 78499|       "source_url": "https://www.revitapidocs.com/2025/dcb690eb-7514-0371-ddb1-f070aba43a08.htm",
 78500|       "dll_signature_verified": true,
 78501|       "dll_relationship_scope": "declared",
 78502|       "dll_semantic_verified": null,
 78503|       "dll_verified_status": "signature_verified_declared",
 78504|       "revitlookup_referenced": null,
 78505|       "revitlookup_requires_document_context": null
 78506|     },
 78507|     {
 78508|       "source": "Autodesk.Revit.DB.Structure.FabricSheet",
 78509|       "target": null,
 78510|       "member_name": "IsValidHost",
 78511|       "member_kind": "method",
 78512|       "edge_type": "HOSTED_BY",
 78513|       "confidence": "name_only_candidate",
 78514|       "confidence_tier": "likely",
 78515|       "target_resolution": "none",
 78516|       "evidence": [
 78517|         "member name 'IsValidHost' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 78518|       ],
 78519|       "source_url": "https://www.revitapidocs.com/2025/d8f29732-ec68-27c7-b105-084c216be625.htm",
 78520|       "dll_signature_verified": true,
 78521|       "dll_relationship_scope": "declared",
 78522|       "dll_semantic_verified": null,
 78523|       "dll_verified_status": "signature_verified_declared",
 78524|       "revitlookup_referenced": null,
 78525|       "revitlookup_requires_document_context": null
 78526|     },
 78527|     {
 78528|       "source": "Autodesk.Revit.DB.Structure.FabricSheet",
 78529|       "target": null,
 78530|       "member_name": "PlaceInHost",
 78531|       "member_kind": "method",
 78532|       "edge_type": "HOSTED_BY",
 78533|       "confidence": "name_only_candidate",
 78534|       "confidence_tier": "likely",
 78535|       "target_resolution": "none",
 78536|       "evidence": [
 78537|         "member name 'PlaceInHost' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 78538|       ],
 78539|       "source_url": "https://www.revitapidocs.com/2025/f1eb07fb-91a6-6ca9-f763-44f29e34014d.htm",
 78540|       "dll_signature_verified": true,
 78541|       "dll_relationship_scope": "declared",
 78542|       "dll_semantic_verified": null,
 78543|       "dll_verified_status": "signature_verified_declared",
 78544|       "revitlookup_referenced": null,
 78545|       "revitlookup_requires_document_context": null
 78546|     },
 78547|     {
 78548|       "source": "Autodesk.Revit.DB.Structure.FabricSheetType",
 78549|       "target": null,
 78550|       "member_name": "MajorDirectionWireType",
 78551|       "member_kind": "property",
 78552|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 78553|       "confidence": "unknown_reference",
 78554|       "confidence_tier": "unverified_reference",
 78555|       "target_resolution": "none",
 78556|       "evidence": [
 78557|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 78558|       ],
 78559|       "source_url": "https://www.revitapidocs.com/2025/bdf9641d-5810-e238-1dd4-42c127a9477e.htm",
 78560|       "dll_signature_verified": true,
 78561|       "dll_relationship_scope": "declared",
 78562|       "dll_semantic_verified": null,
 78563|       "dll_verified_status": "signature_verified_declared",
 78564|       "revitlookup_referenced": null,
 78565|       "revitlookup_requires_document_context": null
 78566|     },
 78567|     {
 78568|       "source": "Autodesk.Revit.DB.Structure.FabricSheetType",
 78569|       "target": "Autodesk.Revit.DB.Material",
 78570|       "member_name": "Material",
 78571|       "member_kind": "property",
 78572|       "edge_type": "USES_MATERIAL",
 78573|       "confidence": "elementid_with_strong_name",
 78574|       "confidence_tier": "core",
 78575|       "target_resolution": "exact",
 78576|       "evidence": [
 78577|         "member name 'Material' matches keyword pattern /Material/"
 78578|       ],
 78579|       "source_url": "https://www.revitapidocs.com/2025/7d91df23-bddb-9052-f05d-782a3bb4e618.htm",
 78580|       "dll_signature_verified": true,
 78581|       "dll_relationship_scope": "declared",
 78582|       "dll_semantic_verified": null,
 78583|       "dll_verified_status": "signature_verified_declared",
 78584|       "revitlookup_referenced": null,
 78585|       "revitlookup_requires_document_context": null
 78586|     },
 78587|     {
 78588|       "source": "Autodesk.Revit.DB.Structure.FabricSheetType",
 78589|       "target": null,
 78590|       "member_name": "MinorDirectionWireType",
 78591|       "member_kind": "property",
 78592|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 78593|       "confidence": "unknown_reference",
 78594|       "confidence_tier": "unverified_reference",
 78595|       "target_resolution": "none",
 78596|       "evidence": [
 78597|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 78598|       ],
 78599|       "source_url": "https://www.revitapidocs.com/2025/963f96c5-6e30-3dd7-1fa6-a31a6cee9856.htm",
 78600|       "dll_signature_verified": true,
 78601|       "dll_relationship_scope": "declared",
 78602|       "dll_semantic_verified": null,
 78603|       "dll_verified_status": "signature_verified_declared",
 78604|       "revitlookup_referenced": null,
 78605|       "revitlookup_requires_document_context": null
 78606|     },
 78607|     {
 78608|       "source": "Autodesk.Revit.DB.Structure.FabricSheetType",
 78609|       "target": "Autodesk.Revit.DB.ViewSheet",
 78610|       "member_name": "SheetMass",
 78611|       "member_kind": "property",
 78612|       "edge_type": "PLACED_ON_SHEET",
 78613|       "confidence": "name_only_candidate",
 78614|       "confidence_tier": "likely",
 78615|       "target_resolution": "exact",
 78616|       "evidence": [
 78617|         "member name 'SheetMass' matches keyword pattern /Sheet/ but return type 'double' gives no type-level confirmation"
 78618|       ],
 78619|       "source_url": "https://www.revitapidocs.com/2025/d300cae8-6398-d321-4fb9-dcac1973e038.htm",
 78620|       "dll_signature_verified": true,
 78621|       "dll_relationship_scope": "declared",
 78622|       "dll_semantic_verified": null,
 78623|       "dll_verified_status": "signature_verified_declared",
 78624|       "revitlookup_referenced": null,
 78625|       "revitlookup_requires_document_context": null
 78626|     },
 78627|     {
 78628|       "source": "Autodesk.Revit.DB.Structure.FabricSheetType",
 78629|       "target": "Autodesk.Revit.DB.ViewSheet",
 78630|       "member_name": "SheetMassUnit",
 78631|       "member_kind": "property",
 78632|       "edge_type": "PLACED_ON_SHEET",
 78633|       "confidence": "name_only_candidate",
 78634|       "confidence_tier": "likely",
 78635|       "target_resolution": "exact",
 78636|       "evidence": [
 78637|         "member name 'SheetMassUnit' matches keyword pattern /Sheet/ but return type 'double' gives no type-level confirmation"
 78638|       ],
 78639|       "source_url": "https://www.revitapidocs.com/2025/b9a2806b-22ce-2def-00f9-78727ddc4ad0.htm",
 78640|       "dll_signature_verified": true,
 78641|       "dll_relationship_scope": "declared",
 78642|       "dll_semantic_verified": null,
 78643|       "dll_verified_status": "signature_verified_declared",
 78644|       "revitlookup_referenced": null,
 78645|       "revitlookup_requires_document_context": null
 78646|     },
 78647|     {
 78648|       "source": "Autodesk.Revit.DB.Structure.FabricSheetType",
 78649|       "target": "Autodesk.Revit.DB.Structure.FabricRoundingManager",
 78650|       "member_name": "GetReinforcementRoundingManager",
 78651|       "member_kind": "method",
 78652|       "edge_type": "REFERENCES",
 78653|       "confidence": "direct_return_type",
 78654|       "confidence_tier": "core",
 78655|       "target_resolution": "short_name_fallback",
 78656|       "evidence": [
 78657|         "return type 'FabricRoundingManager' directly names a Revit DB object type"
 78658|       ],
 78659|       "source_url": "https://www.revitapidocs.com/2025/bcb66de9-4e74-6ddf-5a01-d2a009d25224.htm",
 78660|       "dll_signature_verified": true,
 78661|       "dll_relationship_scope": "declared",
 78662|       "dll_semantic_verified": null,
 78663|       "dll_verified_status": "signature_verified_declared",
 78664|       "revitlookup_referenced": null,
 78665|       "revitlookup_requires_document_context": null
 78666|     },
 78667|     {
 78668|       "source": "Autodesk.Revit.DB.Structure.FabricSheetType",
 78669|       "target": "Autodesk.Revit.DB.Structure.FabricWireItem",
 78670|       "member_name": "GetWireItem",
 78671|       "member_kind": "method",
 78672|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 78673|       "confidence": "direct_return_type",
 78674|       "confidence_tier": "unverified_reference",
 78675|       "target_resolution": "short_name_fallback",
 78676|       "evidence": [
 78677|         "return type 'FabricWireItem' directly names a Revit DB object type"
 78678|       ],
 78679|       "source_url": "https://www.revitapidocs.com/2025/2cceb1d7-9370-8e64-c2eb-80d8e4083e4b.htm",
 78680|       "dll_signature_verified": true,
 78681|       "dll_relationship_scope": "declared",
 78682|       "dll_semantic_verified": null,
 78683|       "dll_verified_status": "signature_verified_declared",
 78684|       "revitlookup_referenced": null,
 78685|       "revitlookup_requires_document_context": null
 78686|     },
 78687|     {
 78688|       "source": "Autodesk.Revit.DB.Structure.FabricWireItem",
 78689|       "target": null,
 78690|       "member_name": "WireType",
 78691|       "member_kind": "property",
 78692|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 78693|       "confidence": "unknown_reference",
 78694|       "confidence_tier": "unverified_reference",
 78695|       "target_resolution": "none",
 78696|       "evidence": [
 78697|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 78698|       ],
 78699|       "source_url": "https://www.revitapidocs.com/2025/bf83d6ed-a392-4908-45ed-e955860d15a2.htm",
 78700|       "dll_signature_verified": true,
 78701|       "dll_relationship_scope": "declared",
 78702|       "dll_semantic_verified": null,
 78703|       "dll_verified_status": "signature_verified_declared",
 78704|       "revitlookup_referenced": null,
 78705|       "revitlookup_requires_document_context": null
 78706|     },
 78707|     {
 78708|       "source": "Autodesk.Revit.DB.Structure.FamilyStructuralMaterialTypeFilter",
 78709|       "target": "Autodesk.Revit.DB.Material",
 78710|       "member_name": "StructuralMaterialType",
 78711|       "member_kind": "property",
 78712|       "edge_type": "USES_MATERIAL",
 78713|       "confidence": "name_only_candidate",
 78714|       "confidence_tier": "likely",
 78715|       "target_resolution": "exact",
 78716|       "evidence": [
 78717|         "member name 'StructuralMaterialType' matches keyword pattern /Material/ but return type 'StructuralMaterialType' gives no type-level confirmation"
 78718|       ],
 78719|       "source_url": "https://www.revitapidocs.com/2025/443d21b4-4cd1-0516-3516-e1ac07ce1e9a.htm",
 78720|       "dll_signature_verified": true,
 78721|       "dll_relationship_scope": "declared",
 78722|       "dll_semantic_verified": null,
 78723|       "dll_verified_status": "signature_verified_declared",
 78724|       "revitlookup_referenced": null,
 78725|       "revitlookup_requires_document_context": null
 78726|     },
 78727|     {
 78728|       "source": "Autodesk.Revit.DB.Structure.Hub",
 78729|       "target": "Autodesk.Revit.DB.ConnectorManager",
 78730|       "member_name": "GetHubConnectorManager",
 78731|       "member_kind": "method",
 78732|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 78733|       "confidence": "direct_return_type",
 78734|       "confidence_tier": "unverified_reference",
 78735|       "target_resolution": "exact",
 78736|       "evidence": [
 78737|         "return type 'ConnectorManager' directly names a Revit DB object type"
 78738|       ],
 78739|       "source_url": "https://www.revitapidocs.com/2025/801d16cd-b376-423a-e159-160f9afa18a9.htm",
 78740|       "dll_signature_verified": true,
 78741|       "dll_relationship_scope": "declared",
 78742|       "dll_semantic_verified": null,
 78743|       "dll_verified_status": "signature_verified_declared",
 78744|       "revitlookup_referenced": null,
 78745|       "revitlookup_requires_document_context": null
 78746|     },
 78747|     {
 78748|       "source": "Autodesk.Revit.DB.Structure.LineLoad",
 78749|       "target": null,
 78750|       "member_name": "IsCurveInsideHostBoundaries",
 78751|       "member_kind": "method",
 78752|       "edge_type": "HOSTED_BY",
 78753|       "confidence": "name_only_candidate",
 78754|       "confidence_tier": "likely",
 78755|       "target_resolution": "none",
 78756|       "evidence": [
 78757|         "member name 'IsCurveInsideHostBoundaries' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 78758|       ],
 78759|       "source_url": "https://www.revitapidocs.com/2025/4bdb0447-3312-6ce7-08a6-f9a8de84ab3e.htm",
 78760|       "dll_signature_verified": true,
 78761|       "dll_relationship_scope": "declared",
 78762|       "dll_semantic_verified": null,
 78763|       "dll_verified_status": "signature_verified_declared",
 78764|       "revitlookup_referenced": null,
 78765|       "revitlookup_requires_document_context": null
 78766|     },
 78767|     {
 78768|       "source": "Autodesk.Revit.DB.Structure.LineLoad",
 78769|       "target": null,
 78770|       "member_name": "IsValidHostId",
 78771|       "member_kind": "method",
 78772|       "edge_type": "HOSTED_BY",
 78773|       "confidence": "name_only_candidate",
 78774|       "confidence_tier": "likely",
 78775|       "target_resolution": "none",
 78776|       "evidence": [
 78777|         "member name 'IsValidHostId' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 78778|       ],
 78779|       "source_url": "https://www.revitapidocs.com/2025/f294365b-5eee-57c0-ad6a-cbdd7bd7637f.htm",
 78780|       "dll_signature_verified": true,
 78781|       "dll_relationship_scope": "declared",
 78782|       "dll_semantic_verified": null,
 78783|       "dll_verified_status": "signature_verified_declared",
 78784|       "revitlookup_referenced": null,
 78785|       "revitlookup_requires_document_context": null
 78786|     },
 78787|     {
 78788|       "source": "Autodesk.Revit.DB.Structure.LoadBase",
 78789|       "target": null,
 78790|       "member_name": "HostElementId",
```
