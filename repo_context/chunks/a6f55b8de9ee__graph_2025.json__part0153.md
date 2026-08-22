# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 153 of 216
- Original line range: 59281-59680
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 59281|       "source_url": "https://www.revitapidocs.com/2025/ac42ff31-d480-8b0e-4735-b5eb6ee1d53e.htm",
 59282|       "dll_signature_verified": true,
 59283|       "dll_relationship_scope": "declared",
 59284|       "dll_semantic_verified": null,
 59285|       "dll_verified_status": "signature_verified_declared",
 59286|       "revitlookup_referenced": null,
 59287|       "revitlookup_requires_document_context": null
 59288|     },
 59289|     {
 59290|       "source": "Autodesk.Revit.DB.SpatialElementTag",
 59291|       "target": null,
 59292|       "member_name": "TagOrientation",
 59293|       "member_kind": "property",
 59294|       "edge_type": "TAGS_ELEMENT",
 59295|       "confidence": "name_only_candidate",
 59296|       "confidence_tier": "likely",
 59297|       "target_resolution": "none",
 59298|       "evidence": [
 59299|         "member name 'TagOrientation' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'SpatialElementTagOrientation' gives no type-level confirmation"
 59300|       ],
 59301|       "source_url": "https://www.revitapidocs.com/2025/710d99f6-e7eb-7254-d81c-392b18e9ddc6.htm",
 59302|       "dll_signature_verified": true,
 59303|       "dll_relationship_scope": "declared",
 59304|       "dll_semantic_verified": null,
 59305|       "dll_verified_status": "signature_verified_declared",
 59306|       "revitlookup_referenced": null,
 59307|       "revitlookup_requires_document_context": null
 59308|     },
 59309|     {
 59310|       "source": "Autodesk.Revit.DB.SpatialElementTag",
 59311|       "target": null,
 59312|       "member_name": "TagText",
 59313|       "member_kind": "property",
 59314|       "edge_type": "TAGS_ELEMENT",
 59315|       "confidence": "name_only_candidate",
 59316|       "confidence_tier": "likely",
 59317|       "target_resolution": "none",
 59318|       "evidence": [
 59319|         "member name 'TagText' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'string' gives no type-level confirmation"
 59320|       ],
 59321|       "source_url": "https://www.revitapidocs.com/2025/0d546fcf-75eb-85e2-d603-171a629ed9fc.htm",
 59322|       "dll_signature_verified": true,
 59323|       "dll_relationship_scope": "declared",
 59324|       "dll_semantic_verified": null,
 59325|       "dll_verified_status": "signature_verified_declared",
 59326|       "revitlookup_referenced": null,
 59327|       "revitlookup_requires_document_context": null
 59328|     },
 59329|     {
 59330|       "source": "Autodesk.Revit.DB.SpatialElementTag",
 59331|       "target": "Autodesk.Revit.DB.View",
 59332|       "member_name": "View",
 59333|       "member_kind": "property",
 59334|       "edge_type": "REFERENCES",
 59335|       "confidence": "direct_return_type",
 59336|       "confidence_tier": "core",
 59337|       "target_resolution": "exact",
 59338|       "evidence": [
 59339|         "return type 'View' directly names a Revit DB object type"
 59340|       ],
 59341|       "source_url": "https://www.revitapidocs.com/2025/abc98018-65b1-0908-373d-1d9359346167.htm",
 59342|       "dll_signature_verified": true,
 59343|       "dll_relationship_scope": "declared",
 59344|       "dll_semantic_verified": null,
 59345|       "dll_verified_status": "signature_verified_declared",
 59346|       "revitlookup_referenced": null,
 59347|       "revitlookup_requires_document_context": null
 59348|     },
 59349|     {
 59350|       "source": "Autodesk.Revit.DB.SpotDimension",
 59351|       "target": "Autodesk.Revit.DB.SpotDimensionType",
 59352|       "member_name": "SpotDimensionType",
 59353|       "member_kind": "property",
 59354|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 59355|       "confidence": "direct_return_type",
 59356|       "confidence_tier": "unverified_reference",
 59357|       "target_resolution": "exact",
 59358|       "evidence": [
 59359|         "return type 'SpotDimensionType' directly names a Revit DB object type"
 59360|       ],
 59361|       "source_url": "https://www.revitapidocs.com/2025/e653cdb6-319d-6c7a-c5b0-16992144e6f6.htm",
 59362|       "dll_signature_verified": true,
 59363|       "dll_relationship_scope": "declared",
 59364|       "dll_semantic_verified": null,
 59365|       "dll_verified_status": "signature_verified_declared",
 59366|       "revitlookup_referenced": null,
 59367|       "revitlookup_requires_document_context": null
 59368|     },
 59369|     {
 59370|       "source": "Autodesk.Revit.DB.StairsEditScope",
 59371|       "target": null,
 59372|       "member_name": "Start",
 59373|       "member_kind": "method",
 59374|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 59375|       "confidence": "unknown_reference",
 59376|       "confidence_tier": "unverified_reference",
 59377|       "target_resolution": "none",
 59378|       "evidence": [
 59379|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 59380|       ],
 59381|       "source_url": "https://www.revitapidocs.com/2025/2293899c-c7f5-d62f-61ff-1dc28f2ee76a.htm",
 59382|       "dll_signature_verified": true,
 59383|       "dll_relationship_scope": "declared",
 59384|       "dll_semantic_verified": null,
 59385|       "dll_verified_status": "signature_verified_declared",
 59386|       "revitlookup_referenced": null,
 59387|       "revitlookup_requires_document_context": null
 59388|     },
 59389|     {
 59390|       "source": "Autodesk.Revit.DB.StairsEditScope",
 59391|       "target": null,
 59392|       "member_name": "Start",
 59393|       "member_kind": "method",
 59394|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 59395|       "confidence": "unknown_reference",
 59396|       "confidence_tier": "unverified_reference",
 59397|       "target_resolution": "none",
 59398|       "evidence": [
 59399|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 59400|       ],
 59401|       "source_url": "https://www.revitapidocs.com/2025/3cc137bd-4b9c-e0c8-f93a-14536a11bd18.htm",
 59402|       "dll_signature_verified": true,
 59403|       "dll_relationship_scope": "declared",
 59404|       "dll_semantic_verified": null,
 59405|       "dll_verified_status": "signature_verified_declared",
 59406|       "revitlookup_referenced": null,
 59407|       "revitlookup_requires_document_context": null
 59408|     },
 59409|     {
 59410|       "source": "Autodesk.Revit.DB.StartingViewSettings",
 59411|       "target": "Autodesk.Revit.DB.View",
 59412|       "member_name": "ViewId",
 59413|       "member_kind": "property",
 59414|       "edge_type": "REFERENCES",
 59415|       "confidence": "elementid_with_strong_name",
 59416|       "confidence_tier": "core",
 59417|       "target_resolution": "exact",
 59418|       "evidence": [
 59419|         "member name 'ViewId' matches keyword pattern /^(Get)?ViewId$/"
 59420|       ],
 59421|       "source_url": "https://www.revitapidocs.com/2025/dd5cd741-e211-9bb3-666c-ee056520c2d9.htm",
 59422|       "dll_signature_verified": true,
 59423|       "dll_relationship_scope": "declared",
 59424|       "dll_semantic_verified": null,
 59425|       "dll_verified_status": "signature_verified_declared",
 59426|       "revitlookup_referenced": null,
 59427|       "revitlookup_requires_document_context": null
 59428|     },
 59429|     {
 59430|       "source": "Autodesk.Revit.DB.Subelement",
 59431|       "target": "Autodesk.Revit.DB.Category",
 59432|       "member_name": "Category",
 59433|       "member_kind": "property",
 59434|       "edge_type": "HAS_CATEGORY",
 59435|       "confidence": "direct_return_type",
 59436|       "confidence_tier": "core",
 59437|       "target_resolution": "exact",
 59438|       "evidence": [
 59439|         "return type 'Category' directly names a Revit DB object type"
 59440|       ],
 59441|       "source_url": "https://www.revitapidocs.com/2025/0b0c9dba-f5ce-b20d-f883-5ef39bb4a6a5.htm",
 59442|       "dll_signature_verified": true,
 59443|       "dll_relationship_scope": "declared",
 59444|       "dll_semantic_verified": null,
 59445|       "dll_verified_status": "signature_verified_declared",
 59446|       "revitlookup_referenced": null,
 59447|       "revitlookup_requires_document_context": null
 59448|     },
 59449|     {
 59450|       "source": "Autodesk.Revit.DB.Subelement",
 59451|       "target": "Autodesk.Revit.DB.Document",
 59452|       "member_name": "Document",
 59453|       "member_kind": "property",
 59454|       "edge_type": "REFERENCES",
 59455|       "confidence": "direct_return_type",
 59456|       "confidence_tier": "core",
 59457|       "target_resolution": "exact",
 59458|       "evidence": [
 59459|         "return type 'Document' directly names a Revit DB object type"
 59460|       ],
 59461|       "source_url": "https://www.revitapidocs.com/2025/5606267f-30e4-bd6c-8e06-43ad1f495585.htm",
 59462|       "dll_signature_verified": true,
 59463|       "dll_relationship_scope": "declared",
 59464|       "dll_semantic_verified": null,
 59465|       "dll_verified_status": "signature_verified_declared",
 59466|       "revitlookup_referenced": null,
 59467|       "revitlookup_requires_document_context": null
 59468|     },
 59469|     {
 59470|       "source": "Autodesk.Revit.DB.Subelement",
 59471|       "target": "Autodesk.Revit.DB.Element",
 59472|       "member_name": "Element",
 59473|       "member_kind": "property",
 59474|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 59475|       "confidence": "direct_return_type",
 59476|       "confidence_tier": "unverified_reference",
 59477|       "target_resolution": "exact",
 59478|       "evidence": [
 59479|         "return type 'Element' directly names a Revit DB object type"
 59480|       ],
 59481|       "source_url": "https://www.revitapidocs.com/2025/507946d2-87d1-ccc4-d174-7f5e789ceadd.htm",
 59482|       "dll_signature_verified": true,
 59483|       "dll_relationship_scope": "declared",
 59484|       "dll_semantic_verified": null,
 59485|       "dll_verified_status": "signature_verified_declared",
 59486|       "revitlookup_referenced": null,
 59487|       "revitlookup_requires_document_context": null
 59488|     },
 59489|     {
 59490|       "source": "Autodesk.Revit.DB.Subelement",
 59491|       "target": null,
 59492|       "member_name": "TypeId",
 59493|       "member_kind": "property",
 59494|       "edge_type": "TYPE_OF",
 59495|       "confidence": "elementid_with_strong_name",
 59496|       "confidence_tier": "core",
 59497|       "target_resolution": "none",
 59498|       "evidence": [
 59499|         "member name 'TypeId' matches keyword pattern /^(Type|TypeId|GetTypeId)$/"
 59500|       ],
 59501|       "source_url": "https://www.revitapidocs.com/2025/3480a4eb-8b80-c694-6a9b-9c5559cac920.htm",
 59502|       "dll_signature_verified": true,
 59503|       "dll_relationship_scope": "declared",
 59504|       "dll_semantic_verified": null,
 59505|       "dll_verified_status": "signature_verified_declared",
 59506|       "revitlookup_referenced": null,
 59507|       "revitlookup_requires_document_context": null
 59508|     },
 59509|     {
 59510|       "source": "Autodesk.Revit.DB.Subelement",
 59511|       "target": null,
 59512|       "member_name": "GetAllParameters",
 59513|       "member_kind": "method",
 59514|       "edge_type": "HAS_PARAMETER",
 59515|       "confidence": "elementid_collection_with_strong_name",
 59516|       "confidence_tier": "core",
 59517|       "target_resolution": "none",
 59518|       "evidence": [
 59519|         "member name 'GetAllParameters' matches keyword pattern /Parameter/"
 59520|       ],
 59521|       "source_url": "https://www.revitapidocs.com/2025/f7ee81cc-3a1c-08c8-b495-c562968010cd.htm",
 59522|       "dll_signature_verified": true,
 59523|       "dll_relationship_scope": "declared",
 59524|       "dll_semantic_verified": null,
 59525|       "dll_verified_status": "signature_verified_declared",
 59526|       "revitlookup_referenced": null,
 59527|       "revitlookup_requires_document_context": null
 59528|     },
 59529|     {
 59530|       "source": "Autodesk.Revit.DB.Subelement",
 59531|       "target": "Autodesk.Revit.DB.ParameterValue",
 59532|       "member_name": "GetParameterValue",
 59533|       "member_kind": "method",
 59534|       "edge_type": "HAS_PARAMETER",
 59535|       "confidence": "direct_return_type",
 59536|       "confidence_tier": "core",
 59537|       "target_resolution": "exact",
 59538|       "evidence": [
 59539|         "return type 'ParameterValue' directly names a Revit DB object type"
 59540|       ],
 59541|       "source_url": "https://www.revitapidocs.com/2025/c1af0433-3e94-6e40-429b-ad77aaeaff73.htm",
 59542|       "dll_signature_verified": true,
 59543|       "dll_relationship_scope": "declared",
 59544|       "dll_semantic_verified": null,
 59545|       "dll_verified_status": "signature_verified_declared",
 59546|       "revitlookup_referenced": null,
 59547|       "revitlookup_requires_document_context": null
 59548|     },
 59549|     {
 59550|       "source": "Autodesk.Revit.DB.Subelement",
 59551|       "target": "Autodesk.Revit.DB.Reference",
 59552|       "member_name": "GetReference",
 59553|       "member_kind": "method",
 59554|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 59555|       "confidence": "direct_return_type",
 59556|       "confidence_tier": "unverified_reference",
 59557|       "target_resolution": "exact",
 59558|       "evidence": [
 59559|         "return type 'Reference' directly names a Revit DB object type"
 59560|       ],
 59561|       "source_url": "https://www.revitapidocs.com/2025/62e0779b-25c1-b83a-0a13-ed2bf9cececc.htm",
 59562|       "dll_signature_verified": true,
 59563|       "dll_relationship_scope": "declared",
 59564|       "dll_semantic_verified": null,
 59565|       "dll_verified_status": "signature_verified_declared",
 59566|       "revitlookup_referenced": null,
 59567|       "revitlookup_requires_document_context": null
 59568|     },
 59569|     {
 59570|       "source": "Autodesk.Revit.DB.Subelement",
 59571|       "target": null,
 59572|       "member_name": "GetValidTypes",
 59573|       "member_kind": "method",
 59574|       "edge_type": "RETURNS_ELEMENT_IDS",
 59575|       "confidence": "unknown_reference",
 59576|       "confidence_tier": "unverified_reference",
 59577|       "target_resolution": "none",
 59578|       "evidence": [
 59579|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 59580|       ],
 59581|       "source_url": "https://www.revitapidocs.com/2025/e39919d5-4bca-bdf4-4e24-c73e03cf147a.htm",
 59582|       "dll_signature_verified": true,
 59583|       "dll_relationship_scope": "declared",
 59584|       "dll_semantic_verified": null,
 59585|       "dll_verified_status": "signature_verified_declared",
 59586|       "revitlookup_referenced": null,
 59587|       "revitlookup_requires_document_context": null
 59588|     },
 59589|     {
 59590|       "source": "Autodesk.Revit.DB.Subelement",
 59591|       "target": null,
 59592|       "member_name": "HasParameter",
 59593|       "member_kind": "method",
 59594|       "edge_type": "HAS_PARAMETER",
 59595|       "confidence": "name_only_candidate",
 59596|       "confidence_tier": "likely",
 59597|       "target_resolution": "none",
 59598|       "evidence": [
 59599|         "member name 'HasParameter' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 59600|       ],
 59601|       "source_url": "https://www.revitapidocs.com/2025/5725cdbe-5482-b403-f72f-936443e50e83.htm",
 59602|       "dll_signature_verified": true,
 59603|       "dll_relationship_scope": "declared",
 59604|       "dll_semantic_verified": null,
 59605|       "dll_verified_status": "signature_verified_declared",
 59606|       "revitlookup_referenced": null,
 59607|       "revitlookup_requires_document_context": null
 59608|     },
 59609|     {
 59610|       "source": "Autodesk.Revit.DB.Subelement",
 59611|       "target": null,
 59612|       "member_name": "IsParameterModifiable",
 59613|       "member_kind": "method",
 59614|       "edge_type": "HAS_PARAMETER",
 59615|       "confidence": "name_only_candidate",
 59616|       "confidence_tier": "likely",
 59617|       "target_resolution": "none",
 59618|       "evidence": [
 59619|         "member name 'IsParameterModifiable' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 59620|       ],
 59621|       "source_url": "https://www.revitapidocs.com/2025/82d6f753-6e14-3bd1-1fb2-caa284bf4686.htm",
 59622|       "dll_signature_verified": true,
 59623|       "dll_relationship_scope": "declared",
 59624|       "dll_semantic_verified": null,
 59625|       "dll_verified_status": "signature_verified_declared",
 59626|       "revitlookup_referenced": null,
 59627|       "revitlookup_requires_document_context": null
 59628|     },
 59629|     {
 59630|       "source": "Autodesk.Revit.DB.Subelement",
 59631|       "target": null,
 59632|       "member_name": "SetParameterValue",
 59633|       "member_kind": "method",
 59634|       "edge_type": "HAS_PARAMETER",
 59635|       "confidence": "name_only_candidate",
 59636|       "confidence_tier": "likely",
 59637|       "target_resolution": "none",
 59638|       "evidence": [
 59639|         "member name 'SetParameterValue' matches keyword pattern /Parameter/ but return type 'void' gives no type-level confirmation"
 59640|       ],
 59641|       "source_url": "https://www.revitapidocs.com/2025/b391bde2-d940-c022-8ab0-a86c7a083b64.htm",
 59642|       "dll_signature_verified": true,
 59643|       "dll_relationship_scope": "declared",
 59644|       "dll_semantic_verified": null,
 59645|       "dll_verified_status": "signature_verified_declared",
 59646|       "revitlookup_referenced": null,
 59647|       "revitlookup_requires_document_context": null
 59648|     },
 59649|     {
 59650|       "source": "Autodesk.Revit.DB.SunAndShadowSettings",
 59651|       "target": "Autodesk.Revit.DB.Level",
 59652|       "member_name": "GroundPlaneLevelId",
 59653|       "member_kind": "property",
 59654|       "edge_type": "ASSIGNED_TO_LEVEL",
 59655|       "confidence": "elementid_with_strong_name",
 59656|       "confidence_tier": "core",
 59657|       "target_resolution": "exact",
 59658|       "evidence": [
 59659|         "member name 'GroundPlaneLevelId' matches keyword pattern /Level/"
 59660|       ],
 59661|       "source_url": "https://www.revitapidocs.com/2025/cb9cf987-a64c-e1cb-7d85-f4374eb953e9.htm",
 59662|       "dll_signature_verified": true,
 59663|       "dll_relationship_scope": "declared",
 59664|       "dll_semantic_verified": null,
 59665|       "dll_verified_status": "signature_verified_declared",
 59666|       "revitlookup_referenced": null,
 59667|       "revitlookup_requires_document_context": null
 59668|     },
 59669|     {
 59670|       "source": "Autodesk.Revit.DB.SunAndShadowSettings",
 59671|       "target": null,
 59672|       "member_name": "ProjectLocationId",
 59673|       "member_kind": "property",
 59674|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 59675|       "confidence": "unknown_reference",
 59676|       "confidence_tier": "unverified_reference",
 59677|       "target_resolution": "none",
 59678|       "evidence": [
 59679|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 59680|       ],
```
