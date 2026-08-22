# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 102 of 216
- Original line range: 39391-39790
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 39391|       "revitlookup_referenced": null,
 39392|       "revitlookup_requires_document_context": null
 39393|     },
 39394|     {
 39395|       "source": "Autodesk.Revit.DB.DWFExportOptions",
 39396|       "target": null,
 39397|       "member_name": "ExportOnlyViewId",
 39398|       "member_kind": "property",
 39399|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 39400|       "confidence": "unknown_reference",
 39401|       "confidence_tier": "unverified_reference",
 39402|       "target_resolution": "none",
 39403|       "evidence": [
 39404|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 39405|       ],
 39406|       "source_url": "https://www.revitapidocs.com/2025/a682bcfd-47a8-be6c-482f-ec7e5334172a.htm",
 39407|       "dll_signature_verified": true,
 39408|       "dll_relationship_scope": "declared",
 39409|       "dll_semantic_verified": null,
 39410|       "dll_verified_status": "signature_verified_declared",
 39411|       "revitlookup_referenced": null,
 39412|       "revitlookup_requires_document_context": null
 39413|     },
 39414|     {
 39415|       "source": "Autodesk.Revit.DB.DWFImportOptions",
 39416|       "target": "Autodesk.Revit.DB.ViewSheet",
 39417|       "member_name": "GetSheetViews",
 39418|       "member_kind": "method",
 39419|       "edge_type": "PLACED_ON_SHEET",
 39420|       "confidence": "elementid_collection_with_strong_name",
 39421|       "confidence_tier": "core",
 39422|       "target_resolution": "exact",
 39423|       "evidence": [
 39424|         "member name 'GetSheetViews' matches keyword pattern /Sheet/"
 39425|       ],
 39426|       "source_url": "https://www.revitapidocs.com/2025/532b4d54-d4f9-1cd1-09d6-41f3b0169780.htm",
 39427|       "dll_signature_verified": true,
 39428|       "dll_relationship_scope": "declared",
 39429|       "dll_semantic_verified": null,
 39430|       "dll_verified_status": "signature_verified_declared",
 39431|       "revitlookup_referenced": null,
 39432|       "revitlookup_requires_document_context": null
 39433|     },
 39434|     {
 39435|       "source": "Autodesk.Revit.DB.DWFImportOptions",
 39436|       "target": "Autodesk.Revit.DB.ViewSheet",
 39437|       "member_name": "SetSheetViews",
 39438|       "member_kind": "method",
 39439|       "edge_type": "PLACED_ON_SHEET",
 39440|       "confidence": "name_only_candidate",
 39441|       "confidence_tier": "likely",
 39442|       "target_resolution": "exact",
 39443|       "evidence": [
 39444|         "member name 'SetSheetViews' matches keyword pattern /Sheet/ but return type 'void' gives no type-level confirmation"
 39445|       ],
 39446|       "source_url": "https://www.revitapidocs.com/2025/94fa19b9-1e01-2066-923a-f4af50ce270d.htm",
 39447|       "dll_signature_verified": true,
 39448|       "dll_relationship_scope": "declared",
 39449|       "dll_semantic_verified": null,
 39450|       "dll_verified_status": "signature_verified_declared",
 39451|       "revitlookup_referenced": null,
 39452|       "revitlookup_requires_document_context": null
 39453|     },
 39454|     {
 39455|       "source": "Autodesk.Revit.DB.Edge",
 39456|       "target": "Autodesk.Revit.DB.Reference",
 39457|       "member_name": "Reference",
 39458|       "member_kind": "property",
 39459|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 39460|       "confidence": "direct_return_type",
 39461|       "confidence_tier": "unverified_reference",
 39462|       "target_resolution": "exact",
 39463|       "evidence": [
 39464|         "return type 'Reference' directly names a Revit DB object type"
 39465|       ],
 39466|       "source_url": "https://www.revitapidocs.com/2025/7ed9676a-c2bf-7580-41e0-5b1a29e27fb2.htm",
 39467|       "dll_signature_verified": true,
 39468|       "dll_relationship_scope": "declared",
 39469|       "dll_semantic_verified": null,
 39470|       "dll_verified_status": "signature_verified_declared",
 39471|       "revitlookup_referenced": null,
 39472|       "revitlookup_requires_document_context": null
 39473|     },
 39474|     {
 39475|       "source": "Autodesk.Revit.DB.Edge",
 39476|       "target": "Autodesk.Revit.DB.CurveUV",
 39477|       "member_name": "GetCurveUV",
 39478|       "member_kind": "method",
 39479|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 39480|       "confidence": "direct_return_type",
 39481|       "confidence_tier": "unverified_reference",
 39482|       "target_resolution": "exact",
 39483|       "evidence": [
 39484|         "return type 'CurveUV' directly names a Revit DB object type"
 39485|       ],
 39486|       "source_url": "https://www.revitapidocs.com/2025/e06c6807-14e7-3b6f-976f-8b78eab42081.htm",
 39487|       "dll_signature_verified": true,
 39488|       "dll_relationship_scope": "declared",
 39489|       "dll_semantic_verified": null,
 39490|       "dll_verified_status": "signature_verified_declared",
 39491|       "revitlookup_referenced": null,
 39492|       "revitlookup_requires_document_context": null
 39493|     },
 39494|     {
 39495|       "source": "Autodesk.Revit.DB.Edge",
 39496|       "target": "Autodesk.Revit.DB.CurveUV",
 39497|       "member_name": "GetCurveUV",
 39498|       "member_kind": "method",
 39499|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 39500|       "confidence": "direct_return_type",
 39501|       "confidence_tier": "unverified_reference",
 39502|       "target_resolution": "exact",
 39503|       "evidence": [
 39504|         "return type 'CurveUV' directly names a Revit DB object type"
 39505|       ],
 39506|       "source_url": "https://www.revitapidocs.com/2025/ce3b5773-846f-9ad4-6316-ebbfeadde0bb.htm",
 39507|       "dll_signature_verified": true,
 39508|       "dll_relationship_scope": "declared",
 39509|       "dll_semantic_verified": null,
 39510|       "dll_verified_status": "signature_verified_declared",
 39511|       "revitlookup_referenced": null,
 39512|       "revitlookup_requires_document_context": null
 39513|     },
 39514|     {
 39515|       "source": "Autodesk.Revit.DB.Edge",
 39516|       "target": "Autodesk.Revit.DB.Reference",
 39517|       "member_name": "GetEndPointReference",
 39518|       "member_kind": "method",
 39519|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 39520|       "confidence": "direct_return_type",
 39521|       "confidence_tier": "unverified_reference",
 39522|       "target_resolution": "exact",
 39523|       "evidence": [
 39524|         "return type 'Reference' directly names a Revit DB object type"
 39525|       ],
 39526|       "source_url": "https://www.revitapidocs.com/2025/c6471321-61c7-22b6-698a-be803c77ff70.htm",
 39527|       "dll_signature_verified": true,
 39528|       "dll_relationship_scope": "declared",
 39529|       "dll_semantic_verified": null,
 39530|       "dll_verified_status": "signature_verified_declared",
 39531|       "revitlookup_referenced": null,
 39532|       "revitlookup_requires_document_context": null
 39533|     },
 39534|     {
 39535|       "source": "Autodesk.Revit.DB.EdgeArray",
 39536|       "target": "Autodesk.Revit.DB.EdgeArrayIterator",
 39537|       "member_name": "ForwardIterator",
 39538|       "member_kind": "method",
 39539|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 39540|       "confidence": "direct_return_type",
 39541|       "confidence_tier": "unverified_reference",
 39542|       "target_resolution": "exact",
 39543|       "evidence": [
 39544|         "return type 'EdgeArrayIterator' directly names a Revit DB object type"
 39545|       ],
 39546|       "source_url": "https://www.revitapidocs.com/2025/154a2a44-36d5-2958-cc88-884bfa9a2800.htm",
 39547|       "dll_signature_verified": true,
 39548|       "dll_relationship_scope": "declared",
 39549|       "dll_semantic_verified": null,
 39550|       "dll_verified_status": "signature_verified_declared",
 39551|       "revitlookup_referenced": null,
 39552|       "revitlookup_requires_document_context": null
 39553|     },
 39554|     {
 39555|       "source": "Autodesk.Revit.DB.EdgeArray",
 39556|       "target": "Autodesk.Revit.DB.EdgeArrayIterator",
 39557|       "member_name": "ReverseIterator",
 39558|       "member_kind": "method",
 39559|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 39560|       "confidence": "direct_return_type",
 39561|       "confidence_tier": "unverified_reference",
 39562|       "target_resolution": "exact",
 39563|       "evidence": [
 39564|         "return type 'EdgeArrayIterator' directly names a Revit DB object type"
 39565|       ],
 39566|       "source_url": "https://www.revitapidocs.com/2025/9d238f70-a27d-02ab-a553-ec01d7a33c39.htm",
 39567|       "dll_signature_verified": true,
 39568|       "dll_relationship_scope": "declared",
 39569|       "dll_semantic_verified": null,
 39570|       "dll_verified_status": "signature_verified_declared",
 39571|       "revitlookup_referenced": null,
 39572|       "revitlookup_requires_document_context": null
 39573|     },
 39574|     {
 39575|       "source": "Autodesk.Revit.DB.EdgeArrayArray",
 39576|       "target": "Autodesk.Revit.DB.EdgeArrayArrayIterator",
 39577|       "member_name": "ForwardIterator",
 39578|       "member_kind": "method",
 39579|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 39580|       "confidence": "direct_return_type",
 39581|       "confidence_tier": "unverified_reference",
 39582|       "target_resolution": "exact",
 39583|       "evidence": [
 39584|         "return type 'EdgeArrayArrayIterator' directly names a Revit DB object type"
 39585|       ],
 39586|       "source_url": "https://www.revitapidocs.com/2025/1b3aab39-84c9-7592-7be2-060c68e1d276.htm",
 39587|       "dll_signature_verified": true,
 39588|       "dll_relationship_scope": "declared",
 39589|       "dll_semantic_verified": null,
 39590|       "dll_verified_status": "signature_verified_declared",
 39591|       "revitlookup_referenced": null,
 39592|       "revitlookup_requires_document_context": null
 39593|     },
 39594|     {
 39595|       "source": "Autodesk.Revit.DB.EdgeArrayArray",
 39596|       "target": "Autodesk.Revit.DB.EdgeArrayArrayIterator",
 39597|       "member_name": "ReverseIterator",
 39598|       "member_kind": "method",
 39599|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 39600|       "confidence": "direct_return_type",
 39601|       "confidence_tier": "unverified_reference",
 39602|       "target_resolution": "exact",
 39603|       "evidence": [
 39604|         "return type 'EdgeArrayArrayIterator' directly names a Revit DB object type"
 39605|       ],
 39606|       "source_url": "https://www.revitapidocs.com/2025/aacf7bc9-5505-6e46-2ea8-b62c5202d991.htm",
 39607|       "dll_signature_verified": true,
 39608|       "dll_relationship_scope": "declared",
 39609|       "dll_semantic_verified": null,
 39610|       "dll_verified_status": "signature_verified_declared",
 39611|       "revitlookup_referenced": null,
 39612|       "revitlookup_requires_document_context": null
 39613|     },
 39614|     {
 39615|       "source": "Autodesk.Revit.DB.EdgeEndPoint",
 39616|       "target": "Autodesk.Revit.DB.Edge",
 39617|       "member_name": "Edge",
 39618|       "member_kind": "property",
 39619|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 39620|       "confidence": "direct_return_type",
 39621|       "confidence_tier": "unverified_reference",
 39622|       "target_resolution": "exact",
 39623|       "evidence": [
 39624|         "return type 'Edge' directly names a Revit DB object type"
 39625|       ],
 39626|       "source_url": "https://www.revitapidocs.com/2025/c4f5ff6e-da5b-5775-50a7-f918f5224fc7.htm",
 39627|       "dll_signature_verified": true,
 39628|       "dll_relationship_scope": "declared",
 39629|       "dll_semantic_verified": null,
 39630|       "dll_verified_status": "signature_verified_declared",
 39631|       "revitlookup_referenced": null,
 39632|       "revitlookup_requires_document_context": null
 39633|     },
 39634|     {
 39635|       "source": "Autodesk.Revit.DB.Element",
 39636|       "target": null,
 39637|       "member_name": "AssemblyInstanceId",
 39638|       "member_kind": "property",
 39639|       "edge_type": "MEMBER_OF_ASSEMBLY",
 39640|       "confidence": "elementid_with_strong_name",
 39641|       "confidence_tier": "core",
 39642|       "target_resolution": "none",
 39643|       "evidence": [
 39644|         "member name 'AssemblyInstanceId' matches keyword pattern /Assembly/"
 39645|       ],
 39646|       "source_url": "https://www.revitapidocs.com/2025/83989f69-1aca-1a49-9647-e57bc2d58b21.htm",
 39647|       "dll_signature_verified": true,
 39648|       "dll_relationship_scope": "declared",
 39649|       "dll_semantic_verified": null,
 39650|       "dll_verified_status": "signature_verified_declared",
 39651|       "revitlookup_referenced": null,
 39652|       "revitlookup_requires_document_context": null
 39653|     },
 39654|     {
 39655|       "source": "Autodesk.Revit.DB.Element",
 39656|       "target": "Autodesk.Revit.DB.Category",
 39657|       "member_name": "Category",
 39658|       "member_kind": "property",
 39659|       "edge_type": "HAS_CATEGORY",
 39660|       "confidence": "direct_return_type",
 39661|       "confidence_tier": "core",
 39662|       "target_resolution": "exact",
 39663|       "evidence": [
 39664|         "return type 'Category' directly names a Revit DB object type"
 39665|       ],
 39666|       "source_url": "https://www.revitapidocs.com/2025/8990bd36-af08-fc99-496b-f94fcb056b21.htm",
 39667|       "dll_signature_verified": true,
 39668|       "dll_relationship_scope": "declared",
 39669|       "dll_semantic_verified": null,
 39670|       "dll_verified_status": "signature_verified_declared",
 39671|       "revitlookup_referenced": null,
 39672|       "revitlookup_requires_document_context": null
 39673|     },
 39674|     {
 39675|       "source": "Autodesk.Revit.DB.Element",
 39676|       "target": "Autodesk.Revit.DB.Phase",
 39677|       "member_name": "CreatedPhaseId",
 39678|       "member_kind": "property",
 39679|       "edge_type": "ASSIGNED_TO_PHASE",
 39680|       "confidence": "elementid_with_strong_name",
 39681|       "confidence_tier": "core",
 39682|       "target_resolution": "exact",
 39683|       "evidence": [
 39684|         "member name 'CreatedPhaseId' matches keyword pattern /Phase/"
 39685|       ],
 39686|       "source_url": "https://www.revitapidocs.com/2025/c6032e01-f7cb-b2ea-3312-697d14216a31.htm",
 39687|       "dll_signature_verified": true,
 39688|       "dll_relationship_scope": "declared",
 39689|       "dll_semantic_verified": null,
 39690|       "dll_verified_status": "signature_verified_declared",
 39691|       "revitlookup_referenced": null,
 39692|       "revitlookup_requires_document_context": null
 39693|     },
 39694|     {
 39695|       "source": "Autodesk.Revit.DB.Element",
 39696|       "target": "Autodesk.Revit.DB.Phase",
 39697|       "member_name": "DemolishedPhaseId",
 39698|       "member_kind": "property",
 39699|       "edge_type": "ASSIGNED_TO_PHASE",
 39700|       "confidence": "elementid_with_strong_name",
 39701|       "confidence_tier": "core",
 39702|       "target_resolution": "exact",
 39703|       "evidence": [
 39704|         "member name 'DemolishedPhaseId' matches keyword pattern /Phase/"
 39705|       ],
 39706|       "source_url": "https://www.revitapidocs.com/2025/7949a983-c5dc-62a3-594a-d685365449d5.htm",
 39707|       "dll_signature_verified": true,
 39708|       "dll_relationship_scope": "declared",
 39709|       "dll_semantic_verified": null,
 39710|       "dll_verified_status": "signature_verified_declared",
 39711|       "revitlookup_referenced": null,
 39712|       "revitlookup_requires_document_context": null
 39713|     },
 39714|     {
 39715|       "source": "Autodesk.Revit.DB.Element",
 39716|       "target": "Autodesk.Revit.DB.DesignOption",
 39717|       "member_name": "DesignOption",
 39718|       "member_kind": "property",
 39719|       "edge_type": "ASSIGNED_TO_DESIGN_OPTION",
 39720|       "confidence": "direct_return_type",
 39721|       "confidence_tier": "core",
 39722|       "target_resolution": "exact",
 39723|       "evidence": [
 39724|         "return type 'DesignOption' directly names a Revit DB object type"
 39725|       ],
 39726|       "source_url": "https://www.revitapidocs.com/2025/5c20fe58-e301-6ddb-3438-666db5c586ee.htm",
 39727|       "dll_signature_verified": true,
 39728|       "dll_relationship_scope": "declared",
 39729|       "dll_semantic_verified": null,
 39730|       "dll_verified_status": "signature_verified_declared",
 39731|       "revitlookup_referenced": null,
 39732|       "revitlookup_requires_document_context": null
 39733|     },
 39734|     {
 39735|       "source": "Autodesk.Revit.DB.Element",
 39736|       "target": "Autodesk.Revit.DB.Document",
 39737|       "member_name": "Document",
 39738|       "member_kind": "property",
 39739|       "edge_type": "REFERENCES",
 39740|       "confidence": "direct_return_type",
 39741|       "confidence_tier": "core",
 39742|       "target_resolution": "exact",
 39743|       "evidence": [
 39744|         "return type 'Document' directly names a Revit DB object type"
 39745|       ],
 39746|       "source_url": "https://www.revitapidocs.com/2025/9e530d25-61ca-3899-a531-cbcfd994358d.htm",
 39747|       "dll_signature_verified": true,
 39748|       "dll_relationship_scope": "declared",
 39749|       "dll_semantic_verified": null,
 39750|       "dll_verified_status": "signature_verified_declared",
 39751|       "revitlookup_referenced": null,
 39752|       "revitlookup_requires_document_context": null
 39753|     },
 39754|     {
 39755|       "source": "Autodesk.Revit.DB.Element",
 39756|       "target": null,
 39757|       "member_name": "GroupId",
 39758|       "member_kind": "property",
 39759|       "edge_type": "MEMBER_OF_GROUP",
 39760|       "confidence": "elementid_with_strong_name",
 39761|       "confidence_tier": "core",
 39762|       "target_resolution": "none",
 39763|       "evidence": [
 39764|         "member name 'GroupId' matches keyword pattern /^GetMember|Group/"
 39765|       ],
 39766|       "source_url": "https://www.revitapidocs.com/2025/9508a6c5-9681-bbef-07c5-1351583b0e1e.htm",
 39767|       "dll_signature_verified": true,
 39768|       "dll_relationship_scope": "declared",
 39769|       "dll_semantic_verified": null,
 39770|       "dll_verified_status": "signature_verified_declared",
 39771|       "revitlookup_referenced": null,
 39772|       "revitlookup_requires_document_context": null
 39773|     },
 39774|     {
 39775|       "source": "Autodesk.Revit.DB.Element",
 39776|       "target": null,
 39777|       "member_name": "Id",
 39778|       "member_kind": "property",
 39779|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 39780|       "confidence": "unknown_reference",
 39781|       "confidence_tier": "unverified_reference",
 39782|       "target_resolution": "none",
 39783|       "evidence": [
 39784|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 39785|       ],
 39786|       "source_url": "https://www.revitapidocs.com/2025/9235095b-b7ae-b6e5-6cc2-2b8d397644de.htm",
 39787|       "dll_signature_verified": true,
 39788|       "dll_relationship_scope": "declared",
 39789|       "dll_semantic_verified": null,
 39790|       "dll_verified_status": "signature_verified_declared",
```
