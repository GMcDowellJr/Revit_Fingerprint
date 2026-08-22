# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 172 of 216
- Original line range: 66691-67090
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 66691|       "dll_verified_status": "signature_verified_declared",
 66692|       "revitlookup_referenced": null,
 66693|       "revitlookup_requires_document_context": null
 66694|     },
 66695|     {
 66696|       "source": "Autodesk.Revit.DB.Analysis.MassSurfaceData",
 66697|       "target": "Autodesk.Revit.DB.Material",
 66698|       "member_name": "MaterialType",
 66699|       "member_kind": "property",
 66700|       "edge_type": "USES_MATERIAL",
 66701|       "confidence": "name_only_candidate",
 66702|       "confidence_tier": "likely",
 66703|       "target_resolution": "exact",
 66704|       "evidence": [
 66705|         "member name 'MaterialType' matches keyword pattern /Material/ but return type 'MassSurfaceDataMaterialType' gives no type-level confirmation"
 66706|       ],
 66707|       "source_url": "https://www.revitapidocs.com/2025/9218731b-4b3b-012d-3db5-a474d01e513d.htm",
 66708|       "dll_signature_verified": true,
 66709|       "dll_relationship_scope": "declared",
 66710|       "dll_semantic_verified": null,
 66711|       "dll_verified_status": "signature_verified_declared",
 66712|       "revitlookup_referenced": null,
 66713|       "revitlookup_requires_document_context": null
 66714|     },
 66715|     {
 66716|       "source": "Autodesk.Revit.DB.Analysis.MassSurfaceData",
 66717|       "target": null,
 66718|       "member_name": "PercentageGlazing",
 66719|       "member_kind": "property",
 66720|       "edge_type": "TAGS_ELEMENT",
 66721|       "confidence": "name_only_candidate",
 66722|       "confidence_tier": "likely",
 66723|       "target_resolution": "none",
 66724|       "evidence": [
 66725|         "member name 'PercentageGlazing' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'double' gives no type-level confirmation"
 66726|       ],
 66727|       "source_url": "https://www.revitapidocs.com/2025/c6d03198-1d30-f0d8-e2b9-d4b94128b984.htm",
 66728|       "dll_signature_verified": true,
 66729|       "dll_relationship_scope": "declared",
 66730|       "dll_semantic_verified": null,
 66731|       "dll_verified_status": "signature_verified_declared",
 66732|       "revitlookup_referenced": null,
 66733|       "revitlookup_requires_document_context": null
 66734|     },
 66735|     {
 66736|       "source": "Autodesk.Revit.DB.Analysis.MassSurfaceData",
 66737|       "target": null,
 66738|       "member_name": "PercentageSkylights",
 66739|       "member_kind": "property",
 66740|       "edge_type": "TAGS_ELEMENT",
 66741|       "confidence": "name_only_candidate",
 66742|       "confidence_tier": "likely",
 66743|       "target_resolution": "none",
 66744|       "evidence": [
 66745|         "member name 'PercentageSkylights' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'double' gives no type-level confirmation"
 66746|       ],
 66747|       "source_url": "https://www.revitapidocs.com/2025/789c7354-a904-227b-2135-95fcce25c476.htm",
 66748|       "dll_signature_verified": true,
 66749|       "dll_relationship_scope": "declared",
 66750|       "dll_semantic_verified": null,
 66751|       "dll_verified_status": "signature_verified_declared",
 66752|       "revitlookup_referenced": null,
 66753|       "revitlookup_requires_document_context": null
 66754|     },
 66755|     {
 66756|       "source": "Autodesk.Revit.DB.Analysis.MassSurfaceData",
 66757|       "target": null,
 66758|       "member_name": "ReferenceElementId",
 66759|       "member_kind": "property",
 66760|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 66761|       "confidence": "unknown_reference",
 66762|       "confidence_tier": "unverified_reference",
 66763|       "target_resolution": "none",
 66764|       "evidence": [
 66765|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 66766|       ],
 66767|       "source_url": "https://www.revitapidocs.com/2025/771ae7d7-0977-660c-1881-6fba10dd6a1f.htm",
 66768|       "dll_signature_verified": true,
 66769|       "dll_relationship_scope": "declared",
 66770|       "dll_semantic_verified": null,
 66771|       "dll_verified_status": "signature_verified_declared",
 66772|       "revitlookup_referenced": null,
 66773|       "revitlookup_requires_document_context": null
 66774|     },
 66775|     {
 66776|       "source": "Autodesk.Revit.DB.Analysis.MassSurfaceData",
 66777|       "target": "Autodesk.Revit.DB.Reference",
 66778|       "member_name": "GetFaceReferences",
 66779|       "member_kind": "method",
 66780|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 66781|       "confidence": "needs_runtime_validation",
 66782|       "confidence_tier": "needs_validation",
 66783|       "target_resolution": "exact",
 66784|       "evidence": [
 66785|         "return type 'IList < Reference >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 66786|       ],
 66787|       "source_url": "https://www.revitapidocs.com/2025/a7afead8-f134-96b4-daf6-e2ceee81f3e5.htm",
 66788|       "dll_signature_verified": true,
 66789|       "dll_relationship_scope": "declared",
 66790|       "dll_semantic_verified": null,
 66791|       "dll_verified_status": "signature_verified_declared",
 66792|       "revitlookup_referenced": null,
 66793|       "revitlookup_requires_document_context": null
 66794|     },
 66795|     {
 66796|       "source": "Autodesk.Revit.DB.Analysis.MEPAnalyticalModelData",
 66797|       "target": "Autodesk.Revit.DB.Analysis.MEPAnalyticalNode",
 66798|       "member_name": "GetNodeById",
 66799|       "member_kind": "method",
 66800|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 66801|       "confidence": "direct_return_type",
 66802|       "confidence_tier": "unverified_reference",
 66803|       "target_resolution": "short_name_fallback",
 66804|       "evidence": [
 66805|         "return type 'MEPAnalyticalNode' directly names a Revit DB object type"
 66806|       ],
 66807|       "source_url": "https://www.revitapidocs.com/2025/4cd9a535-f3da-c375-06cd-dfda231933a6.htm",
 66808|       "dll_signature_verified": true,
 66809|       "dll_relationship_scope": "declared",
 66810|       "dll_semantic_verified": null,
 66811|       "dll_verified_status": "signature_verified_declared",
 66812|       "revitlookup_referenced": null,
 66813|       "revitlookup_requires_document_context": null
 66814|     },
 66815|     {
 66816|       "source": "Autodesk.Revit.DB.Analysis.MEPAnalyticalModelData",
 66817|       "target": "Autodesk.Revit.DB.Analysis.MEPAnalyticalNode",
 66818|       "member_name": "GetNodeByIndex",
 66819|       "member_kind": "method",
 66820|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 66821|       "confidence": "direct_return_type",
 66822|       "confidence_tier": "unverified_reference",
 66823|       "target_resolution": "short_name_fallback",
 66824|       "evidence": [
 66825|         "return type 'MEPAnalyticalNode' directly names a Revit DB object type"
 66826|       ],
 66827|       "source_url": "https://www.revitapidocs.com/2025/9ebcaff8-b11a-e81a-3e95-eb30eff36c52.htm",
 66828|       "dll_signature_verified": true,
 66829|       "dll_relationship_scope": "declared",
 66830|       "dll_semantic_verified": null,
 66831|       "dll_verified_status": "signature_verified_declared",
 66832|       "revitlookup_referenced": null,
 66833|       "revitlookup_requires_document_context": null
 66834|     },
 66835|     {
 66836|       "source": "Autodesk.Revit.DB.Analysis.MEPAnalyticalModelData",
 66837|       "target": "Autodesk.Revit.DB.Analysis.MEPAnalyticalSegment",
 66838|       "member_name": "GetSegmentById",
 66839|       "member_kind": "method",
 66840|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 66841|       "confidence": "direct_return_type",
 66842|       "confidence_tier": "unverified_reference",
 66843|       "target_resolution": "short_name_fallback",
 66844|       "evidence": [
 66845|         "return type 'MEPAnalyticalSegment' directly names a Revit DB object type"
 66846|       ],
 66847|       "source_url": "https://www.revitapidocs.com/2025/d251400b-5c09-ff21-6340-df144707123e.htm",
 66848|       "dll_signature_verified": true,
 66849|       "dll_relationship_scope": "declared",
 66850|       "dll_semantic_verified": null,
 66851|       "dll_verified_status": "signature_verified_declared",
 66852|       "revitlookup_referenced": null,
 66853|       "revitlookup_requires_document_context": null
 66854|     },
 66855|     {
 66856|       "source": "Autodesk.Revit.DB.Analysis.MEPAnalyticalModelData",
 66857|       "target": "Autodesk.Revit.DB.Analysis.MEPAnalyticalSegment",
 66858|       "member_name": "GetSegmentByIndex",
 66859|       "member_kind": "method",
 66860|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 66861|       "confidence": "direct_return_type",
 66862|       "confidence_tier": "unverified_reference",
 66863|       "target_resolution": "short_name_fallback",
 66864|       "evidence": [
 66865|         "return type 'MEPAnalyticalSegment' directly names a Revit DB object type"
 66866|       ],
 66867|       "source_url": "https://www.revitapidocs.com/2025/459a0ee4-5aa7-5a7a-dc1d-405a9cb5dec8.htm",
 66868|       "dll_signature_verified": true,
 66869|       "dll_relationship_scope": "declared",
 66870|       "dll_semantic_verified": null,
 66871|       "dll_verified_status": "signature_verified_declared",
 66872|       "revitlookup_referenced": null,
 66873|       "revitlookup_requires_document_context": null
 66874|     },
 66875|     {
 66876|       "source": "Autodesk.Revit.DB.Analysis.MEPAnalyticalModelData",
 66877|       "target": "Autodesk.Revit.DB.Analysis.MEPNetworkSegmentData",
 66878|       "member_name": "GetSegmentData",
 66879|       "member_kind": "method",
 66880|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 66881|       "confidence": "direct_return_type",
 66882|       "confidence_tier": "unverified_reference",
 66883|       "target_resolution": "short_name_fallback",
 66884|       "evidence": [
 66885|         "return type 'MEPNetworkSegmentData' directly names a Revit DB object type"
 66886|       ],
 66887|       "source_url": "https://www.revitapidocs.com/2025/9f0dcd5d-569a-4e50-3c9a-39491227840d.htm",
 66888|       "dll_signature_verified": true,
 66889|       "dll_relationship_scope": "declared",
 66890|       "dll_semantic_verified": null,
 66891|       "dll_verified_status": "signature_verified_declared",
 66892|       "revitlookup_referenced": null,
 66893|       "revitlookup_requires_document_context": null
 66894|     },
 66895|     {
 66896|       "source": "Autodesk.Revit.DB.Analysis.MEPAnalyticalSegment",
 66897|       "target": null,
 66898|       "member_name": "RevitElementId",
 66899|       "member_kind": "property",
 66900|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 66901|       "confidence": "unknown_reference",
 66902|       "confidence_tier": "unverified_reference",
 66903|       "target_resolution": "none",
 66904|       "evidence": [
 66905|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 66906|       ],
 66907|       "source_url": "https://www.revitapidocs.com/2025/ea394f2e-6929-ca71-5ebd-6c29566a39bf.htm",
 66908|       "dll_signature_verified": true,
 66909|       "dll_relationship_scope": "declared",
 66910|       "dll_semantic_verified": null,
 66911|       "dll_verified_status": "signature_verified_declared",
 66912|       "revitlookup_referenced": null,
 66913|       "revitlookup_requires_document_context": null
 66914|     },
 66915|     {
 66916|       "source": "Autodesk.Revit.DB.Analysis.MEPAnalyticalSegment",
 66917|       "target": "Autodesk.Revit.DB.Analysis.MEPNetworkSegmentId",
 66918|       "member_name": "GetNetworkSegmentId",
 66919|       "member_kind": "method",
 66920|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 66921|       "confidence": "direct_return_type",
 66922|       "confidence_tier": "unverified_reference",
 66923|       "target_resolution": "short_name_fallback",
 66924|       "evidence": [
 66925|         "return type 'MEPNetworkSegmentId' directly names a Revit DB object type"
 66926|       ],
 66927|       "source_url": "https://www.revitapidocs.com/2025/f9baea4a-46d9-52e8-982e-a56dbf151faf.htm",
 66928|       "dll_signature_verified": true,
 66929|       "dll_relationship_scope": "declared",
 66930|       "dll_semantic_verified": null,
 66931|       "dll_verified_status": "signature_verified_declared",
 66932|       "revitlookup_referenced": null,
 66933|       "revitlookup_requires_document_context": null
 66934|     },
 66935|     {
 66936|       "source": "Autodesk.Revit.DB.Analysis.MEPNetworkIterator",
 66937|       "target": null,
 66938|       "member_name": "CurrentElementId",
 66939|       "member_kind": "property",
 66940|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 66941|       "confidence": "unknown_reference",
 66942|       "confidence_tier": "unverified_reference",
 66943|       "target_resolution": "none",
 66944|       "evidence": [
 66945|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 66946|       ],
 66947|       "source_url": "https://www.revitapidocs.com/2025/995fbf58-5361-67e4-34d7-c4e47b5b56df.htm",
 66948|       "dll_signature_verified": true,
 66949|       "dll_relationship_scope": "declared",
 66950|       "dll_semantic_verified": null,
 66951|       "dll_verified_status": "signature_verified_declared",
 66952|       "revitlookup_referenced": null,
 66953|       "revitlookup_requires_document_context": null
 66954|     },
 66955|     {
 66956|       "source": "Autodesk.Revit.DB.Analysis.MEPNetworkIterator",
 66957|       "target": null,
 66958|       "member_name": "SystemId",
 66959|       "member_kind": "property",
 66960|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 66961|       "confidence": "unknown_reference",
 66962|       "confidence_tier": "unverified_reference",
 66963|       "target_resolution": "none",
 66964|       "evidence": [
 66965|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 66966|       ],
 66967|       "source_url": "https://www.revitapidocs.com/2025/a0c47cb7-40de-abb3-421f-d122281328cb.htm",
 66968|       "dll_signature_verified": true,
 66969|       "dll_relationship_scope": "declared",
 66970|       "dll_semantic_verified": null,
 66971|       "dll_verified_status": "signature_verified_declared",
 66972|       "revitlookup_referenced": null,
 66973|       "revitlookup_requires_document_context": null
 66974|     },
 66975|     {
 66976|       "source": "Autodesk.Revit.DB.Analysis.MEPNetworkIterator",
 66977|       "target": "Autodesk.Revit.DB.Analysis.MEPAnalyticalModelData",
 66978|       "member_name": "GetAnalyticalModelData",
 66979|       "member_kind": "method",
 66980|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 66981|       "confidence": "direct_return_type",
 66982|       "confidence_tier": "unverified_reference",
 66983|       "target_resolution": "short_name_fallback",
 66984|       "evidence": [
 66985|         "return type 'MEPAnalyticalModelData' directly names a Revit DB object type"
 66986|       ],
 66987|       "source_url": "https://www.revitapidocs.com/2025/4c492289-beea-37b3-77c6-d27a5b389c6c.htm",
 66988|       "dll_signature_verified": true,
 66989|       "dll_relationship_scope": "declared",
 66990|       "dll_semantic_verified": null,
 66991|       "dll_verified_status": "signature_verified_declared",
 66992|       "revitlookup_referenced": null,
 66993|       "revitlookup_requires_document_context": null
 66994|     },
 66995|     {
 66996|       "source": "Autodesk.Revit.DB.Analysis.MEPNetworkIterator",
 66997|       "target": "Autodesk.Revit.DB.Analysis.MEPAnalyticalNode",
 66998|       "member_name": "GetAnalyticalNode",
 66999|       "member_kind": "method",
 67000|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67001|       "confidence": "direct_return_type",
 67002|       "confidence_tier": "unverified_reference",
 67003|       "target_resolution": "short_name_fallback",
 67004|       "evidence": [
 67005|         "return type 'MEPAnalyticalNode' directly names a Revit DB object type"
 67006|       ],
 67007|       "source_url": "https://www.revitapidocs.com/2025/17b6fe15-2ebd-9f43-bc97-7b5948f9f4a3.htm",
 67008|       "dll_signature_verified": true,
 67009|       "dll_relationship_scope": "declared",
 67010|       "dll_semantic_verified": null,
 67011|       "dll_verified_status": "signature_verified_declared",
 67012|       "revitlookup_referenced": null,
 67013|       "revitlookup_requires_document_context": null
 67014|     },
 67015|     {
 67016|       "source": "Autodesk.Revit.DB.Analysis.MEPNetworkIterator",
 67017|       "target": "Autodesk.Revit.DB.Analysis.MEPAnalyticalSegment",
 67018|       "member_name": "GetAnalyticalSegment",
 67019|       "member_kind": "method",
 67020|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67021|       "confidence": "direct_return_type",
 67022|       "confidence_tier": "unverified_reference",
 67023|       "target_resolution": "short_name_fallback",
 67024|       "evidence": [
 67025|         "return type 'MEPAnalyticalSegment' directly names a Revit DB object type"
 67026|       ],
 67027|       "source_url": "https://www.revitapidocs.com/2025/a36b0175-dd17-c1b0-99ea-542c476e832f.htm",
 67028|       "dll_signature_verified": true,
 67029|       "dll_relationship_scope": "declared",
 67030|       "dll_semantic_verified": null,
 67031|       "dll_verified_status": "signature_verified_declared",
 67032|       "revitlookup_referenced": null,
 67033|       "revitlookup_requires_document_context": null
 67034|     },
 67035|     {
 67036|       "source": "Autodesk.Revit.DB.Analysis.MEPNetworkIterator",
 67037|       "target": "Autodesk.Revit.DB.Analysis.MEPAnalyticalNode",
 67038|       "member_name": "GetOtherAnalyticalNode",
 67039|       "member_kind": "method",
 67040|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67041|       "confidence": "direct_return_type",
 67042|       "confidence_tier": "unverified_reference",
 67043|       "target_resolution": "short_name_fallback",
 67044|       "evidence": [
 67045|         "return type 'MEPAnalyticalNode' directly names a Revit DB object type"
 67046|       ],
 67047|       "source_url": "https://www.revitapidocs.com/2025/7124c822-5a6b-eb5d-3db0-20ff041f39e5.htm",
 67048|       "dll_signature_verified": true,
 67049|       "dll_relationship_scope": "declared",
 67050|       "dll_semantic_verified": null,
 67051|       "dll_verified_status": "signature_verified_declared",
 67052|       "revitlookup_referenced": null,
 67053|       "revitlookup_requires_document_context": null
 67054|     },
 67055|     {
 67056|       "source": "Autodesk.Revit.DB.Analysis.MEPNetworkSegmentData",
 67057|       "target": "Autodesk.Revit.DB.Analysis.MEPNetworkSegmentId",
 67058|       "member_name": "GetDownstreamSegments",
 67059|       "member_kind": "method",
 67060|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67061|       "confidence": "needs_runtime_validation",
 67062|       "confidence_tier": "needs_validation",
 67063|       "target_resolution": "short_name_fallback",
 67064|       "evidence": [
 67065|         "return type 'IList < MEPNetworkSegmentId >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 67066|       ],
 67067|       "source_url": "https://www.revitapidocs.com/2025/4c3a42e2-2aa9-b984-e70f-4f05ad2c8453.htm",
 67068|       "dll_signature_verified": true,
 67069|       "dll_relationship_scope": "declared",
 67070|       "dll_semantic_verified": null,
 67071|       "dll_verified_status": "signature_verified_declared",
 67072|       "revitlookup_referenced": null,
 67073|       "revitlookup_requires_document_context": null
 67074|     },
 67075|     {
 67076|       "source": "Autodesk.Revit.DB.Analysis.MEPNetworkSegmentData",
 67077|       "target": "Autodesk.Revit.DB.Analysis.MEPNetworkSegmentId",
 67078|       "member_name": "GetUpstreamSegments",
 67079|       "member_kind": "method",
 67080|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67081|       "confidence": "needs_runtime_validation",
 67082|       "confidence_tier": "needs_validation",
 67083|       "target_resolution": "short_name_fallback",
 67084|       "evidence": [
 67085|         "return type 'IList < MEPNetworkSegmentId >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 67086|       ],
 67087|       "source_url": "https://www.revitapidocs.com/2025/7bc0931e-957d-5176-c843-b7d60ed81d9e.htm",
 67088|       "dll_signature_verified": true,
 67089|       "dll_relationship_scope": "declared",
 67090|       "dll_semantic_verified": null,
```
