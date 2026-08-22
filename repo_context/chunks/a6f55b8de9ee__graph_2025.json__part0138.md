# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 138 of 216
- Original line range: 53431-53830
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 53431|       "member_kind": "method",
 53432|       "edge_type": "RETURNS_ELEMENT_IDS",
 53433|       "confidence": "elementid_collection_with_strong_name",
 53434|       "confidence_tier": "core",
 53435|       "target_resolution": "none",
 53436|       "evidence": [
 53437|         "member name 'GetAllFilterableCategories' matches keyword pattern /^GetAll/"
 53438|       ],
 53439|       "source_url": "https://www.revitapidocs.com/2025/5dc40235-09fe-d2e4-5ca3-399519fe0255.htm",
 53440|       "dll_signature_verified": true,
 53441|       "dll_relationship_scope": "declared",
 53442|       "dll_semantic_verified": null,
 53443|       "dll_verified_status": "signature_verified_declared",
 53444|       "revitlookup_referenced": null,
 53445|       "revitlookup_requires_document_context": null
 53446|     },
 53447|     {
 53448|       "source": "Autodesk.Revit.DB.ParameterFilterUtilities",
 53449|       "target": null,
 53450|       "member_name": "GetFilterableParametersInCommon",
 53451|       "member_kind": "method",
 53452|       "edge_type": "HAS_PARAMETER",
 53453|       "confidence": "elementid_collection_with_strong_name",
 53454|       "confidence_tier": "core",
 53455|       "target_resolution": "none",
 53456|       "evidence": [
 53457|         "member name 'GetFilterableParametersInCommon' matches keyword pattern /Parameter/"
 53458|       ],
 53459|       "source_url": "https://www.revitapidocs.com/2025/7ea624c7-2c0d-c9bb-3b2c-1ac798cf6606.htm",
 53460|       "dll_signature_verified": true,
 53461|       "dll_relationship_scope": "declared",
 53462|       "dll_semantic_verified": null,
 53463|       "dll_verified_status": "signature_verified_declared",
 53464|       "revitlookup_referenced": null,
 53465|       "revitlookup_requires_document_context": null
 53466|     },
 53467|     {
 53468|       "source": "Autodesk.Revit.DB.ParameterFilterUtilities",
 53469|       "target": null,
 53470|       "member_name": "GetInapplicableParameters",
 53471|       "member_kind": "method",
 53472|       "edge_type": "HAS_PARAMETER",
 53473|       "confidence": "elementid_collection_with_strong_name",
 53474|       "confidence_tier": "core",
 53475|       "target_resolution": "none",
 53476|       "evidence": [
 53477|         "member name 'GetInapplicableParameters' matches keyword pattern /Parameter/"
 53478|       ],
 53479|       "source_url": "https://www.revitapidocs.com/2025/5b7a1f72-6095-4137-9838-a7b6564624f4.htm",
 53480|       "dll_signature_verified": true,
 53481|       "dll_relationship_scope": "declared",
 53482|       "dll_semantic_verified": null,
 53483|       "dll_verified_status": "signature_verified_declared",
 53484|       "revitlookup_referenced": null,
 53485|       "revitlookup_requires_document_context": null
 53486|     },
 53487|     {
 53488|       "source": "Autodesk.Revit.DB.ParameterFilterUtilities",
 53489|       "target": null,
 53490|       "member_name": "IsParameterApplicable",
 53491|       "member_kind": "method",
 53492|       "edge_type": "HAS_PARAMETER",
 53493|       "confidence": "name_only_candidate",
 53494|       "confidence_tier": "likely",
 53495|       "target_resolution": "none",
 53496|       "evidence": [
 53497|         "member name 'IsParameterApplicable' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 53498|       ],
 53499|       "source_url": "https://www.revitapidocs.com/2025/b8d82e63-1ecd-75c8-d28e-e03d9cc0675c.htm",
 53500|       "dll_signature_verified": true,
 53501|       "dll_relationship_scope": "declared",
 53502|       "dll_semantic_verified": null,
 53503|       "dll_verified_status": "signature_verified_declared",
 53504|       "revitlookup_referenced": null,
 53505|       "revitlookup_requires_document_context": null
 53506|     },
 53507|     {
 53508|       "source": "Autodesk.Revit.DB.ParameterFilterUtilities",
 53509|       "target": null,
 53510|       "member_name": "RemoveUnfilterableCategories",
 53511|       "member_kind": "method",
 53512|       "edge_type": "RETURNS_ELEMENT_IDS",
 53513|       "confidence": "unknown_reference",
 53514|       "confidence_tier": "unverified_reference",
 53515|       "target_resolution": "none",
 53516|       "evidence": [
 53517|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 53518|       ],
 53519|       "source_url": "https://www.revitapidocs.com/2025/21cd2cd7-3054-d114-1f32-efbbfd069ef0.htm",
 53520|       "dll_signature_verified": true,
 53521|       "dll_relationship_scope": "declared",
 53522|       "dll_semantic_verified": null,
 53523|       "dll_verified_status": "signature_verified_declared",
 53524|       "revitlookup_referenced": null,
 53525|       "revitlookup_requires_document_context": null
 53526|     },
 53527|     {
 53528|       "source": "Autodesk.Revit.DB.ParameterMap",
 53529|       "target": "Autodesk.Revit.DB.ParameterMapIterator",
 53530|       "member_name": "ForwardIterator",
 53531|       "member_kind": "method",
 53532|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 53533|       "confidence": "direct_return_type",
 53534|       "confidence_tier": "unverified_reference",
 53535|       "target_resolution": "exact",
 53536|       "evidence": [
 53537|         "return type 'ParameterMapIterator' directly names a Revit DB object type"
 53538|       ],
 53539|       "source_url": "https://www.revitapidocs.com/2025/8ad1533b-b495-679d-7a7f-1407d692eee0.htm",
 53540|       "dll_signature_verified": true,
 53541|       "dll_relationship_scope": "declared",
 53542|       "dll_semantic_verified": null,
 53543|       "dll_verified_status": "signature_verified_declared",
 53544|       "revitlookup_referenced": null,
 53545|       "revitlookup_requires_document_context": null
 53546|     },
 53547|     {
 53548|       "source": "Autodesk.Revit.DB.ParameterMap",
 53549|       "target": "Autodesk.Revit.DB.ParameterMapIterator",
 53550|       "member_name": "ReverseIterator",
 53551|       "member_kind": "method",
 53552|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 53553|       "confidence": "direct_return_type",
 53554|       "confidence_tier": "unverified_reference",
 53555|       "target_resolution": "exact",
 53556|       "evidence": [
 53557|         "return type 'ParameterMapIterator' directly names a Revit DB object type"
 53558|       ],
 53559|       "source_url": "https://www.revitapidocs.com/2025/b9688ea8-f87a-c401-39b2-462ead4ecd16.htm",
 53560|       "dll_signature_verified": true,
 53561|       "dll_relationship_scope": "declared",
 53562|       "dll_semantic_verified": null,
 53563|       "dll_verified_status": "signature_verified_declared",
 53564|       "revitlookup_referenced": null,
 53565|       "revitlookup_requires_document_context": null
 53566|     },
 53567|     {
 53568|       "source": "Autodesk.Revit.DB.ParameterSet",
 53569|       "target": "Autodesk.Revit.DB.ParameterSetIterator",
 53570|       "member_name": "ForwardIterator",
 53571|       "member_kind": "method",
 53572|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 53573|       "confidence": "direct_return_type",
 53574|       "confidence_tier": "unverified_reference",
 53575|       "target_resolution": "exact",
 53576|       "evidence": [
 53577|         "return type 'ParameterSetIterator' directly names a Revit DB object type"
 53578|       ],
 53579|       "source_url": "https://www.revitapidocs.com/2025/e0f70e1e-f3b8-ad47-4715-ca71c47b28b2.htm",
 53580|       "dll_signature_verified": true,
 53581|       "dll_relationship_scope": "declared",
 53582|       "dll_semantic_verified": null,
 53583|       "dll_verified_status": "signature_verified_declared",
 53584|       "revitlookup_referenced": null,
 53585|       "revitlookup_requires_document_context": null
 53586|     },
 53587|     {
 53588|       "source": "Autodesk.Revit.DB.ParameterSet",
 53589|       "target": "Autodesk.Revit.DB.ParameterSetIterator",
 53590|       "member_name": "ReverseIterator",
 53591|       "member_kind": "method",
 53592|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 53593|       "confidence": "direct_return_type",
 53594|       "confidence_tier": "unverified_reference",
 53595|       "target_resolution": "exact",
 53596|       "evidence": [
 53597|         "return type 'ParameterSetIterator' directly names a Revit DB object type"
 53598|       ],
 53599|       "source_url": "https://www.revitapidocs.com/2025/8ccc9805-a4a4-f9dc-203a-fd992faab4a2.htm",
 53600|       "dll_signature_verified": true,
 53601|       "dll_relationship_scope": "declared",
 53602|       "dll_semantic_verified": null,
 53603|       "dll_verified_status": "signature_verified_declared",
 53604|       "revitlookup_referenced": null,
 53605|       "revitlookup_requires_document_context": null
 53606|     },
 53607|     {
 53608|       "source": "Autodesk.Revit.DB.ParameterUtils",
 53609|       "target": "Autodesk.Revit.DB.SharedParameterElement",
 53610|       "member_name": "DownloadParameter",
 53611|       "member_kind": "method",
 53612|       "edge_type": "HAS_PARAMETER",
 53613|       "confidence": "direct_return_type",
 53614|       "confidence_tier": "core",
 53615|       "target_resolution": "exact",
 53616|       "evidence": [
 53617|         "return type 'SharedParameterElement' directly names a Revit DB object type"
 53618|       ],
 53619|       "source_url": "https://www.revitapidocs.com/2025/6449c1fe-90af-e6d4-e852-91f6eeae5c97.htm",
 53620|       "dll_signature_verified": true,
 53621|       "dll_relationship_scope": "declared",
 53622|       "dll_semantic_verified": null,
 53623|       "dll_verified_status": "signature_verified_declared",
 53624|       "revitlookup_referenced": null,
 53625|       "revitlookup_requires_document_context": null
 53626|     },
 53627|     {
 53628|       "source": "Autodesk.Revit.DB.ParameterUtils",
 53629|       "target": "Autodesk.Revit.DB.ParameterDownloadOptions",
 53630|       "member_name": "DownloadParameterOptions",
 53631|       "member_kind": "method",
 53632|       "edge_type": "HAS_PARAMETER",
 53633|       "confidence": "direct_return_type",
 53634|       "confidence_tier": "core",
 53635|       "target_resolution": "exact",
 53636|       "evidence": [
 53637|         "return type 'ParameterDownloadOptions' directly names a Revit DB object type"
 53638|       ],
 53639|       "source_url": "https://www.revitapidocs.com/2025/fd6683df-c93e-eabe-3f6c-dffe61b5cef9.htm",
 53640|       "dll_signature_verified": true,
 53641|       "dll_relationship_scope": "declared",
 53642|       "dll_semantic_verified": null,
 53643|       "dll_verified_status": "signature_verified_declared",
 53644|       "revitlookup_referenced": null,
 53645|       "revitlookup_requires_document_context": null
 53646|     },
 53647|     {
 53648|       "source": "Autodesk.Revit.DB.ParameterUtils",
 53649|       "target": null,
 53650|       "member_name": "GetBuiltInParameter",
 53651|       "member_kind": "method",
 53652|       "edge_type": "HAS_PARAMETER",
 53653|       "confidence": "name_only_candidate",
 53654|       "confidence_tier": "likely",
 53655|       "target_resolution": "none",
 53656|       "evidence": [
 53657|         "member name 'GetBuiltInParameter' matches keyword pattern /Parameter/ but return type 'BuiltInParameter' gives no type-level confirmation"
 53658|       ],
 53659|       "source_url": "https://www.revitapidocs.com/2025/9b2b9b94-5220-0e9f-d259-c05faaf86625.htm",
 53660|       "dll_signature_verified": true,
 53661|       "dll_relationship_scope": "declared",
 53662|       "dll_semantic_verified": null,
 53663|       "dll_verified_status": "signature_verified_declared",
 53664|       "revitlookup_referenced": null,
 53665|       "revitlookup_requires_document_context": null
 53666|     },
 53667|     {
 53668|       "source": "Autodesk.Revit.DB.ParameterUtils",
 53669|       "target": null,
 53670|       "member_name": "IsBuiltInGroup",
 53671|       "member_kind": "method",
 53672|       "edge_type": "MEMBER_OF_GROUP",
 53673|       "confidence": "name_only_candidate",
 53674|       "confidence_tier": "likely",
 53675|       "target_resolution": "none",
 53676|       "evidence": [
 53677|         "member name 'IsBuiltInGroup' matches keyword pattern /^GetMember|Group/ but return type 'bool' gives no type-level confirmation"
 53678|       ],
 53679|       "source_url": "https://www.revitapidocs.com/2025/50a42579-6e5e-7f9d-30ff-fdf41036c8e7.htm",
 53680|       "dll_signature_verified": true,
 53681|       "dll_relationship_scope": "declared",
 53682|       "dll_semantic_verified": null,
 53683|       "dll_verified_status": "signature_verified_declared",
 53684|       "revitlookup_referenced": null,
 53685|       "revitlookup_requires_document_context": null
 53686|     },
 53687|     {
 53688|       "source": "Autodesk.Revit.DB.ParameterUtils",
 53689|       "target": null,
 53690|       "member_name": "IsBuiltInParameter",
 53691|       "member_kind": "method",
 53692|       "edge_type": "HAS_PARAMETER",
 53693|       "confidence": "name_only_candidate",
 53694|       "confidence_tier": "likely",
 53695|       "target_resolution": "none",
 53696|       "evidence": [
 53697|         "member name 'IsBuiltInParameter' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 53698|       ],
 53699|       "source_url": "https://www.revitapidocs.com/2025/7df6bd75-52ac-3657-aef1-6d594809c6f9.htm",
 53700|       "dll_signature_verified": true,
 53701|       "dll_relationship_scope": "declared",
 53702|       "dll_semantic_verified": null,
 53703|       "dll_verified_status": "signature_verified_declared",
 53704|       "revitlookup_referenced": null,
 53705|       "revitlookup_requires_document_context": null
 53706|     },
 53707|     {
 53708|       "source": "Autodesk.Revit.DB.ParameterUtils",
 53709|       "target": null,
 53710|       "member_name": "IsBuiltInParameter",
 53711|       "member_kind": "method",
 53712|       "edge_type": "HAS_PARAMETER",
 53713|       "confidence": "name_only_candidate",
 53714|       "confidence_tier": "likely",
 53715|       "target_resolution": "none",
 53716|       "evidence": [
 53717|         "member name 'IsBuiltInParameter' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 53718|       ],
 53719|       "source_url": "https://www.revitapidocs.com/2025/dd94c332-1755-910b-d3db-65ad9d396ce1.htm",
 53720|       "dll_signature_verified": true,
 53721|       "dll_relationship_scope": "declared",
 53722|       "dll_semantic_verified": null,
 53723|       "dll_verified_status": "signature_verified_declared",
 53724|       "revitlookup_referenced": null,
 53725|       "revitlookup_requires_document_context": null
 53726|     },
 53727|     {
 53728|       "source": "Autodesk.Revit.DB.ParameterValuePresenceRule",
 53729|       "target": null,
 53730|       "member_name": "Parameter",
 53731|       "member_kind": "property",
 53732|       "edge_type": "HAS_PARAMETER",
 53733|       "confidence": "elementid_with_strong_name",
 53734|       "confidence_tier": "core",
 53735|       "target_resolution": "none",
 53736|       "evidence": [
 53737|         "member name 'Parameter' matches keyword pattern /Parameter/"
 53738|       ],
 53739|       "source_url": "https://www.revitapidocs.com/2025/a412718c-49f2-bb49-841b-673d5e1ca657.htm",
 53740|       "dll_signature_verified": true,
 53741|       "dll_relationship_scope": "declared",
 53742|       "dll_semantic_verified": null,
 53743|       "dll_verified_status": "signature_verified_declared",
 53744|       "revitlookup_referenced": null,
 53745|       "revitlookup_requires_document_context": null
 53746|     },
 53747|     {
 53748|       "source": "Autodesk.Revit.DB.ParameterValueProvider",
 53749|       "target": null,
 53750|       "member_name": "Parameter",
 53751|       "member_kind": "property",
 53752|       "edge_type": "HAS_PARAMETER",
 53753|       "confidence": "elementid_with_strong_name",
 53754|       "confidence_tier": "core",
 53755|       "target_resolution": "none",
 53756|       "evidence": [
 53757|         "member name 'Parameter' matches keyword pattern /Parameter/"
 53758|       ],
 53759|       "source_url": "https://www.revitapidocs.com/2025/3f40b842-1ea9-534a-7e77-b47dc589bb3a.htm",
 53760|       "dll_signature_verified": true,
 53761|       "dll_relationship_scope": "declared",
 53762|       "dll_semantic_verified": null,
 53763|       "dll_verified_status": "signature_verified_declared",
 53764|       "revitlookup_referenced": null,
 53765|       "revitlookup_requires_document_context": null
 53766|     },
 53767|     {
 53768|       "source": "Autodesk.Revit.DB.Part",
 53769|       "target": "Autodesk.Revit.DB.Category",
 53770|       "member_name": "OriginalCategoryId",
 53771|       "member_kind": "property",
 53772|       "edge_type": "HAS_CATEGORY",
 53773|       "confidence": "elementid_with_strong_name",
 53774|       "confidence_tier": "core",
 53775|       "target_resolution": "exact",
 53776|       "evidence": [
 53777|         "member name 'OriginalCategoryId' matches keyword pattern /Category/"
 53778|       ],
 53779|       "source_url": "https://www.revitapidocs.com/2025/a06ea771-6aea-6a39-9036-4d3f1389b7ed.htm",
 53780|       "dll_signature_verified": true,
 53781|       "dll_relationship_scope": "declared",
 53782|       "dll_semantic_verified": null,
 53783|       "dll_verified_status": "signature_verified_declared",
 53784|       "revitlookup_referenced": null,
 53785|       "revitlookup_requires_document_context": null
 53786|     },
 53787|     {
 53788|       "source": "Autodesk.Revit.DB.Part",
 53789|       "target": "Autodesk.Revit.DB.PartMaker",
 53790|       "member_name": "PartMaker",
 53791|       "member_kind": "property",
 53792|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 53793|       "confidence": "direct_return_type",
 53794|       "confidence_tier": "unverified_reference",
 53795|       "target_resolution": "exact",
 53796|       "evidence": [
 53797|         "return type 'PartMaker' directly names a Revit DB object type"
 53798|       ],
 53799|       "source_url": "https://www.revitapidocs.com/2025/67743418-5f84-a6b6-cfcf-14d8072391b1.htm",
 53800|       "dll_signature_verified": true,
 53801|       "dll_relationship_scope": "declared",
 53802|       "dll_semantic_verified": null,
 53803|       "dll_verified_status": "signature_verified_declared",
 53804|       "revitlookup_referenced": null,
 53805|       "revitlookup_requires_document_context": null
 53806|     },
 53807|     {
 53808|       "source": "Autodesk.Revit.DB.Part",
 53809|       "target": null,
 53810|       "member_name": "GetSourceElementIds",
 53811|       "member_kind": "method",
 53812|       "edge_type": "RETURNS_ELEMENT_IDS",
 53813|       "confidence": "unknown_reference",
 53814|       "confidence_tier": "unverified_reference",
 53815|       "target_resolution": "none",
 53816|       "evidence": [
 53817|         "return type 'ICollection < LinkElementId >' is a collection of ID wrappers with no strong name hint"
 53818|       ],
 53819|       "source_url": "https://www.revitapidocs.com/2025/35943e3b-b8d2-2bd6-6258-27599d012ee8.htm",
 53820|       "dll_signature_verified": true,
 53821|       "dll_relationship_scope": "declared",
 53822|       "dll_semantic_verified": null,
 53823|       "dll_verified_status": "signature_verified_declared",
 53824|       "revitlookup_referenced": null,
 53825|       "revitlookup_requires_document_context": null
 53826|     },
 53827|     {
 53828|       "source": "Autodesk.Revit.DB.Part",
 53829|       "target": "Autodesk.Revit.DB.Category",
 53830|       "member_name": "GetSourceElementOriginalCategoryIds",
```
