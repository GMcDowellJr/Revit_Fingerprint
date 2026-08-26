# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 89 of 216
- Original line range: 34321-34720
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 34321|       "evidence": [
 34322|         "member name 'SketchId' matches keyword pattern /Sketch(Id)?$/"
 34323|       ],
 34324|       "source_url": "https://www.revitapidocs.com/2025/6ddc8f5d-8090-9418-82fe-67f55649ebac.htm",
 34325|       "dll_signature_verified": true,
 34326|       "dll_relationship_scope": "declared",
 34327|       "dll_semantic_verified": null,
 34328|       "dll_verified_status": "signature_verified_declared",
 34329|       "revitlookup_referenced": null,
 34330|       "revitlookup_requires_document_context": null
 34331|     },
 34332|     {
 34333|       "source": "Autodesk.Revit.DB.CeilingType",
 34334|       "target": "Autodesk.Revit.DB.ThermalProperties",
 34335|       "member_name": "ThermalProperties",
 34336|       "member_kind": "property",
 34337|       "edge_type": "REFERENCES",
 34338|       "confidence": "direct_return_type",
 34339|       "confidence_tier": "core",
 34340|       "target_resolution": "exact",
 34341|       "evidence": [
 34342|         "return type 'ThermalProperties' directly names a Revit DB object type"
 34343|       ],
 34344|       "source_url": "https://www.revitapidocs.com/2025/a5ef2aa8-db6b-61a1-2c16-f4cf3fc40782.htm",
 34345|       "dll_signature_verified": true,
 34346|       "dll_relationship_scope": "declared",
 34347|       "dll_semantic_verified": null,
 34348|       "dll_verified_status": "signature_verified_declared",
 34349|       "revitlookup_referenced": null,
 34350|       "revitlookup_requires_document_context": null
 34351|     },
 34352|     {
 34353|       "source": "Autodesk.Revit.DB.CitySet",
 34354|       "target": "Autodesk.Revit.DB.CitySetIterator",
 34355|       "member_name": "ForwardIterator",
 34356|       "member_kind": "method",
 34357|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 34358|       "confidence": "direct_return_type",
 34359|       "confidence_tier": "unverified_reference",
 34360|       "target_resolution": "exact",
 34361|       "evidence": [
 34362|         "return type 'CitySetIterator' directly names a Revit DB object type"
 34363|       ],
 34364|       "source_url": "https://www.revitapidocs.com/2025/76d21b21-dd48-3a31-643e-3869decd8865.htm",
 34365|       "dll_signature_verified": true,
 34366|       "dll_relationship_scope": "declared",
 34367|       "dll_semantic_verified": null,
 34368|       "dll_verified_status": "signature_verified_declared",
 34369|       "revitlookup_referenced": null,
 34370|       "revitlookup_requires_document_context": null
 34371|     },
 34372|     {
 34373|       "source": "Autodesk.Revit.DB.CitySet",
 34374|       "target": "Autodesk.Revit.DB.CitySetIterator",
 34375|       "member_name": "ReverseIterator",
 34376|       "member_kind": "method",
 34377|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 34378|       "confidence": "direct_return_type",
 34379|       "confidence_tier": "unverified_reference",
 34380|       "target_resolution": "exact",
 34381|       "evidence": [
 34382|         "return type 'CitySetIterator' directly names a Revit DB object type"
 34383|       ],
 34384|       "source_url": "https://www.revitapidocs.com/2025/88896eaa-61b8-bc3e-b6a7-857cea72ad69.htm",
 34385|       "dll_signature_verified": true,
 34386|       "dll_relationship_scope": "declared",
 34387|       "dll_semantic_verified": null,
 34388|       "dll_verified_status": "signature_verified_declared",
 34389|       "revitlookup_referenced": null,
 34390|       "revitlookup_requires_document_context": null
 34391|     },
 34392|     {
 34393|       "source": "Autodesk.Revit.DB.ClassificationEntry",
 34394|       "target": "Autodesk.Revit.DB.Category",
 34395|       "member_name": "CategoryId",
 34396|       "member_kind": "property",
 34397|       "edge_type": "HAS_CATEGORY",
 34398|       "confidence": "elementid_with_strong_name",
 34399|       "confidence_tier": "core",
 34400|       "target_resolution": "exact",
 34401|       "evidence": [
 34402|         "member name 'CategoryId' matches keyword pattern /Category/"
 34403|       ],
 34404|       "source_url": "https://www.revitapidocs.com/2025/0df469ce-96b5-5f05-3a04-b0a4f191c131.htm",
 34405|       "dll_signature_verified": true,
 34406|       "dll_relationship_scope": "declared",
 34407|       "dll_semantic_verified": null,
 34408|       "dll_verified_status": "signature_verified_declared",
 34409|       "revitlookup_referenced": null,
 34410|       "revitlookup_requires_document_context": null
 34411|     },
 34412|     {
 34413|       "source": "Autodesk.Revit.DB.ClassificationEntry",
 34414|       "target": "Autodesk.Revit.DB.Level",
 34415|       "member_name": "Level",
 34416|       "member_kind": "property",
 34417|       "edge_type": "ASSIGNED_TO_LEVEL",
 34418|       "confidence": "name_only_candidate",
 34419|       "confidence_tier": "likely",
 34420|       "target_resolution": "exact",
 34421|       "evidence": [
 34422|         "member name 'Level' matches keyword pattern /Level/ but return type 'int' gives no type-level confirmation"
 34423|       ],
 34424|       "source_url": "https://www.revitapidocs.com/2025/7a781ba4-66bd-1433-75ec-5babb73f3a6a.htm",
 34425|       "dll_signature_verified": true,
 34426|       "dll_relationship_scope": "declared",
 34427|       "dll_semantic_verified": null,
 34428|       "dll_verified_status": "signature_verified_declared",
 34429|       "revitlookup_referenced": null,
 34430|       "revitlookup_requires_document_context": null
 34431|     },
 34432|     {
 34433|       "source": "Autodesk.Revit.DB.ClassificationEntry",
 34434|       "target": "Autodesk.Revit.DB.Category",
 34435|       "member_name": "HasBadCategoryId",
 34436|       "member_kind": "method",
 34437|       "edge_type": "HAS_CATEGORY",
 34438|       "confidence": "name_only_candidate",
 34439|       "confidence_tier": "likely",
 34440|       "target_resolution": "exact",
 34441|       "evidence": [
 34442|         "member name 'HasBadCategoryId' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 34443|       ],
 34444|       "source_url": "https://www.revitapidocs.com/2025/995f6795-6527-4ac1-5bef-a24d4972fab7.htm",
 34445|       "dll_signature_verified": true,
 34446|       "dll_relationship_scope": "declared",
 34447|       "dll_semantic_verified": null,
 34448|       "dll_verified_status": "signature_verified_declared",
 34449|       "revitlookup_referenced": null,
 34450|       "revitlookup_requires_document_context": null
 34451|     },
 34452|     {
 34453|       "source": "Autodesk.Revit.DB.ClassificationEntry",
 34454|       "target": "Autodesk.Revit.DB.Level",
 34455|       "member_name": "HasBadLevel",
 34456|       "member_kind": "method",
 34457|       "edge_type": "ASSIGNED_TO_LEVEL",
 34458|       "confidence": "name_only_candidate",
 34459|       "confidence_tier": "likely",
 34460|       "target_resolution": "exact",
 34461|       "evidence": [
 34462|         "member name 'HasBadLevel' matches keyword pattern /Level/ but return type 'bool' gives no type-level confirmation"
 34463|       ],
 34464|       "source_url": "https://www.revitapidocs.com/2025/69b9940f-9890-8aa7-c935-67c5c80b2f24.htm",
 34465|       "dll_signature_verified": true,
 34466|       "dll_relationship_scope": "declared",
 34467|       "dll_semantic_verified": null,
 34468|       "dll_verified_status": "signature_verified_declared",
 34469|       "revitlookup_referenced": null,
 34470|       "revitlookup_requires_document_context": null
 34471|     },
 34472|     {
 34473|       "source": "Autodesk.Revit.DB.ClosestPointsPairBetweenTwoCurves",
 34474|       "target": null,
 34475|       "member_name": "ParameterOnFirstCurve",
 34476|       "member_kind": "property",
 34477|       "edge_type": "HAS_PARAMETER",
 34478|       "confidence": "name_only_candidate",
 34479|       "confidence_tier": "likely",
 34480|       "target_resolution": "none",
 34481|       "evidence": [
 34482|         "member name 'ParameterOnFirstCurve' matches keyword pattern /Parameter/ but return type 'double' gives no type-level confirmation"
 34483|       ],
 34484|       "source_url": "https://www.revitapidocs.com/2025/224787ac-9ec0-d2f0-e4ca-e58e30027f1a.htm",
 34485|       "dll_signature_verified": true,
 34486|       "dll_relationship_scope": "declared",
 34487|       "dll_semantic_verified": null,
 34488|       "dll_verified_status": "signature_verified_declared",
 34489|       "revitlookup_referenced": null,
 34490|       "revitlookup_requires_document_context": null
 34491|     },
 34492|     {
 34493|       "source": "Autodesk.Revit.DB.ClosestPointsPairBetweenTwoCurves",
 34494|       "target": null,
 34495|       "member_name": "ParameterOnSecondCurve",
 34496|       "member_kind": "property",
 34497|       "edge_type": "HAS_PARAMETER",
 34498|       "confidence": "name_only_candidate",
 34499|       "confidence_tier": "likely",
 34500|       "target_resolution": "none",
 34501|       "evidence": [
 34502|         "member name 'ParameterOnSecondCurve' matches keyword pattern /Parameter/ but return type 'double' gives no type-level confirmation"
 34503|       ],
 34504|       "source_url": "https://www.revitapidocs.com/2025/cd32ec9c-5e44-28f1-0034-619a615f9c34.htm",
 34505|       "dll_signature_verified": true,
 34506|       "dll_relationship_scope": "declared",
 34507|       "dll_semantic_verified": null,
 34508|       "dll_verified_status": "signature_verified_declared",
 34509|       "revitlookup_referenced": null,
 34510|       "revitlookup_requires_document_context": null
 34511|     },
 34512|     {
 34513|       "source": "Autodesk.Revit.DB.ColorFillLegend",
 34514|       "target": "Autodesk.Revit.DB.Category",
 34515|       "member_name": "ColorFillCategoryId",
 34516|       "member_kind": "property",
 34517|       "edge_type": "HAS_CATEGORY",
 34518|       "confidence": "elementid_with_strong_name",
 34519|       "confidence_tier": "core",
 34520|       "target_resolution": "exact",
 34521|       "evidence": [
 34522|         "member name 'ColorFillCategoryId' matches keyword pattern /Category/"
 34523|       ],
 34524|       "source_url": "https://www.revitapidocs.com/2025/7fe9c873-16d6-d661-e6dc-94fd1c7d2d69.htm",
 34525|       "dll_signature_verified": true,
 34526|       "dll_relationship_scope": "declared",
 34527|       "dll_semantic_verified": null,
 34528|       "dll_verified_status": "signature_verified_declared",
 34529|       "revitlookup_referenced": null,
 34530|       "revitlookup_requires_document_context": null
 34531|     },
 34532|     {
 34533|       "source": "Autodesk.Revit.DB.ColorFillScheme",
 34534|       "target": null,
 34535|       "member_name": "AreaSchemeId",
 34536|       "member_kind": "property",
 34537|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 34538|       "confidence": "unknown_reference",
 34539|       "confidence_tier": "unverified_reference",
 34540|       "target_resolution": "none",
 34541|       "evidence": [
 34542|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 34543|       ],
 34544|       "source_url": "https://www.revitapidocs.com/2025/e24d76dd-38fb-c951-7ae4-d10101b4981b.htm",
 34545|       "dll_signature_verified": true,
 34546|       "dll_relationship_scope": "declared",
 34547|       "dll_semantic_verified": null,
 34548|       "dll_verified_status": "signature_verified_declared",
 34549|       "revitlookup_referenced": null,
 34550|       "revitlookup_requires_document_context": null
 34551|     },
 34552|     {
 34553|       "source": "Autodesk.Revit.DB.ColorFillScheme",
 34554|       "target": "Autodesk.Revit.DB.Category",
 34555|       "member_name": "CategoryId",
 34556|       "member_kind": "property",
 34557|       "edge_type": "HAS_CATEGORY",
 34558|       "confidence": "elementid_with_strong_name",
 34559|       "confidence_tier": "core",
 34560|       "target_resolution": "exact",
 34561|       "evidence": [
 34562|         "member name 'CategoryId' matches keyword pattern /Category/"
 34563|       ],
 34564|       "source_url": "https://www.revitapidocs.com/2025/7f1d0a3c-4194-f165-0203-5aba9431a1b8.htm",
 34565|       "dll_signature_verified": true,
 34566|       "dll_relationship_scope": "declared",
 34567|       "dll_semantic_verified": null,
 34568|       "dll_verified_status": "signature_verified_declared",
 34569|       "revitlookup_referenced": null,
 34570|       "revitlookup_requires_document_context": null
 34571|     },
 34572|     {
 34573|       "source": "Autodesk.Revit.DB.ColorFillScheme",
 34574|       "target": null,
 34575|       "member_name": "ParameterDefinition",
 34576|       "member_kind": "property",
 34577|       "edge_type": "HAS_PARAMETER",
 34578|       "confidence": "elementid_with_strong_name",
 34579|       "confidence_tier": "core",
 34580|       "target_resolution": "none",
 34581|       "evidence": [
 34582|         "member name 'ParameterDefinition' matches keyword pattern /Parameter/"
 34583|       ],
 34584|       "source_url": "https://www.revitapidocs.com/2025/554f7720-d040-ebcf-5986-9910b8038b87.htm",
 34585|       "dll_signature_verified": true,
 34586|       "dll_relationship_scope": "declared",
 34587|       "dll_semantic_verified": null,
 34588|       "dll_verified_status": "signature_verified_declared",
 34589|       "revitlookup_referenced": null,
 34590|       "revitlookup_requires_document_context": null
 34591|     },
 34592|     {
 34593|       "source": "Autodesk.Revit.DB.ColorFillScheme",
 34594|       "target": null,
 34595|       "member_name": "Duplicate",
 34596|       "member_kind": "method",
 34597|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 34598|       "confidence": "unknown_reference",
 34599|       "confidence_tier": "unverified_reference",
 34600|       "target_resolution": "none",
 34601|       "evidence": [
 34602|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 34603|       ],
 34604|       "source_url": "https://www.revitapidocs.com/2025/095596ae-d215-bf22-ccfa-fae85109d1a0.htm",
 34605|       "dll_signature_verified": true,
 34606|       "dll_relationship_scope": "declared",
 34607|       "dll_semantic_verified": null,
 34608|       "dll_verified_status": "signature_verified_declared",
 34609|       "revitlookup_referenced": null,
 34610|       "revitlookup_requires_document_context": null
 34611|     },
 34612|     {
 34613|       "source": "Autodesk.Revit.DB.ColorFillScheme",
 34614|       "target": "Autodesk.Revit.DB.ColorFillSchemeEntry",
 34615|       "member_name": "GetEntries",
 34616|       "member_kind": "method",
 34617|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 34618|       "confidence": "needs_runtime_validation",
 34619|       "confidence_tier": "needs_validation",
 34620|       "target_resolution": "exact",
 34621|       "evidence": [
 34622|         "return type 'IList < ColorFillSchemeEntry >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 34623|       ],
 34624|       "source_url": "https://www.revitapidocs.com/2025/bb3b650c-2718-28b7-c4bb-be3f80fb3e32.htm",
 34625|       "dll_signature_verified": true,
 34626|       "dll_relationship_scope": "declared",
 34627|       "dll_semantic_verified": null,
 34628|       "dll_verified_status": "signature_verified_declared",
 34629|       "revitlookup_referenced": null,
 34630|       "revitlookup_requires_document_context": null
 34631|     },
 34632|     {
 34633|       "source": "Autodesk.Revit.DB.ColorFillScheme",
 34634|       "target": null,
 34635|       "member_name": "GetSupportedParameterIds",
 34636|       "member_kind": "method",
 34637|       "edge_type": "HAS_PARAMETER",
 34638|       "confidence": "elementid_collection_with_strong_name",
 34639|       "confidence_tier": "core",
 34640|       "target_resolution": "none",
 34641|       "evidence": [
 34642|         "member name 'GetSupportedParameterIds' matches keyword pattern /Parameter/"
 34643|       ],
 34644|       "source_url": "https://www.revitapidocs.com/2025/d19ac539-c56d-1c3c-1b7b-8801e9713c06.htm",
 34645|       "dll_signature_verified": true,
 34646|       "dll_relationship_scope": "declared",
 34647|       "dll_semantic_verified": null,
 34648|       "dll_verified_status": "signature_verified_declared",
 34649|       "revitlookup_referenced": null,
 34650|       "revitlookup_requires_document_context": null
 34651|     },
 34652|     {
 34653|       "source": "Autodesk.Revit.DB.ColorFillScheme",
 34654|       "target": null,
 34655|       "member_name": "IsValidParameterDefinitionId",
 34656|       "member_kind": "method",
 34657|       "edge_type": "HAS_PARAMETER",
 34658|       "confidence": "name_only_candidate",
 34659|       "confidence_tier": "likely",
 34660|       "target_resolution": "none",
 34661|       "evidence": [
 34662|         "member name 'IsValidParameterDefinitionId' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 34663|       ],
 34664|       "source_url": "https://www.revitapidocs.com/2025/df43fe3e-0b93-ef67-c877-eedbaf6a7f49.htm",
 34665|       "dll_signature_verified": true,
 34666|       "dll_relationship_scope": "declared",
 34667|       "dll_semantic_verified": null,
 34668|       "dll_verified_status": "signature_verified_declared",
 34669|       "revitlookup_referenced": null,
 34670|       "revitlookup_requires_document_context": null
 34671|     },
 34672|     {
 34673|       "source": "Autodesk.Revit.DB.ColorFillSchemeEntry",
 34674|       "target": "Autodesk.Revit.DB.FillPatternElement",
 34675|       "member_name": "FillPatternId",
 34676|       "member_kind": "property",
 34677|       "edge_type": "USES_FILL_PATTERN",
 34678|       "confidence": "elementid_with_strong_name",
 34679|       "confidence_tier": "core",
 34680|       "target_resolution": "exact",
 34681|       "evidence": [
 34682|         "member name 'FillPatternId' matches keyword pattern /FillPattern/"
 34683|       ],
 34684|       "source_url": "https://www.revitapidocs.com/2025/60eb8528-0a4b-e71a-a420-41ec39d020ae.htm",
 34685|       "dll_signature_verified": true,
 34686|       "dll_relationship_scope": "declared",
 34687|       "dll_semantic_verified": null,
 34688|       "dll_verified_status": "signature_verified_declared",
 34689|       "revitlookup_referenced": null,
 34690|       "revitlookup_requires_document_context": null
 34691|     },
 34692|     {
 34693|       "source": "Autodesk.Revit.DB.ColorFillSchemeEntry",
 34694|       "target": null,
 34695|       "member_name": "GetElementIdValue",
 34696|       "member_kind": "method",
 34697|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 34698|       "confidence": "unknown_reference",
 34699|       "confidence_tier": "unverified_reference",
 34700|       "target_resolution": "none",
 34701|       "evidence": [
 34702|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 34703|       ],
 34704|       "source_url": "https://www.revitapidocs.com/2025/1483c739-c936-5e88-8fd9-f82baf472a45.htm",
 34705|       "dll_signature_verified": true,
 34706|       "dll_relationship_scope": "declared",
 34707|       "dll_semantic_verified": null,
 34708|       "dll_verified_status": "signature_verified_declared",
 34709|       "revitlookup_referenced": null,
 34710|       "revitlookup_requires_document_context": null
 34711|     },
 34712|     {
 34713|       "source": "Autodesk.Revit.DB.ColumnAttachment",
 34714|       "target": null,
 34715|       "member_name": "TargetId",
 34716|       "member_kind": "property",
 34717|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 34718|       "confidence": "unknown_reference",
 34719|       "confidence_tier": "unverified_reference",
 34720|       "target_resolution": "none",
```
