# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 149 of 216
- Original line range: 57721-58120
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 57721|       "source_url": "https://www.revitapidocs.com/2025/57376e72-79c9-da97-6b1c-6f4e40f00252.htm",
 57722|       "dll_signature_verified": true,
 57723|       "dll_relationship_scope": "declared",
 57724|       "dll_semantic_verified": null,
 57725|       "dll_verified_status": "signature_verified_declared",
 57726|       "revitlookup_referenced": null,
 57727|       "revitlookup_requires_document_context": null
 57728|     },
 57729|     {
 57730|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57731|       "target": "Autodesk.Revit.DB.ScheduleField",
 57732|       "member_name": "InsertField",
 57733|       "member_kind": "method",
 57734|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57735|       "confidence": "direct_return_type",
 57736|       "confidence_tier": "unverified_reference",
 57737|       "target_resolution": "exact",
 57738|       "evidence": [
 57739|         "return type 'ScheduleField' directly names a Revit DB object type"
 57740|       ],
 57741|       "source_url": "https://www.revitapidocs.com/2025/914f1282-2b24-5479-0784-1ab5329dba1c.htm",
 57742|       "dll_signature_verified": true,
 57743|       "dll_relationship_scope": "declared",
 57744|       "dll_semantic_verified": null,
 57745|       "dll_verified_status": "signature_verified_declared",
 57746|       "revitlookup_referenced": null,
 57747|       "revitlookup_requires_document_context": null
 57748|     },
 57749|     {
 57750|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57751|       "target": "Autodesk.Revit.DB.ScheduleField",
 57752|       "member_name": "InsertField",
 57753|       "member_kind": "method",
 57754|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57755|       "confidence": "direct_return_type",
 57756|       "confidence_tier": "unverified_reference",
 57757|       "target_resolution": "exact",
 57758|       "evidence": [
 57759|         "return type 'ScheduleField' directly names a Revit DB object type"
 57760|       ],
 57761|       "source_url": "https://www.revitapidocs.com/2025/443f3eed-9d4c-a729-a0d4-20e52a9bdd14.htm",
 57762|       "dll_signature_verified": true,
 57763|       "dll_relationship_scope": "declared",
 57764|       "dll_semantic_verified": null,
 57765|       "dll_verified_status": "signature_verified_declared",
 57766|       "revitlookup_referenced": null,
 57767|       "revitlookup_requires_document_context": null
 57768|     },
 57769|     {
 57770|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57771|       "target": null,
 57772|       "member_name": "InsertSortGroupField",
 57773|       "member_kind": "method",
 57774|       "edge_type": "MEMBER_OF_GROUP",
 57775|       "confidence": "name_only_candidate",
 57776|       "confidence_tier": "likely",
 57777|       "target_resolution": "none",
 57778|       "evidence": [
 57779|         "member name 'InsertSortGroupField' matches keyword pattern /^GetMember|Group/ but return type 'void' gives no type-level confirmation"
 57780|       ],
 57781|       "source_url": "https://www.revitapidocs.com/2025/8e9b2895-7627-7430-6db4-0ed0e25ffa60.htm",
 57782|       "dll_signature_verified": true,
 57783|       "dll_relationship_scope": "declared",
 57784|       "dll_semantic_verified": null,
 57785|       "dll_verified_status": "signature_verified_declared",
 57786|       "revitlookup_referenced": null,
 57787|       "revitlookup_requires_document_context": null
 57788|     },
 57789|     {
 57790|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57791|       "target": "Autodesk.Revit.DB.Category",
 57792|       "member_name": "IsValidCategoryForEmbeddedSchedule",
 57793|       "member_kind": "method",
 57794|       "edge_type": "HAS_CATEGORY",
 57795|       "confidence": "name_only_candidate",
 57796|       "confidence_tier": "likely",
 57797|       "target_resolution": "exact",
 57798|       "evidence": [
 57799|         "member name 'IsValidCategoryForEmbeddedSchedule' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 57800|       ],
 57801|       "source_url": "https://www.revitapidocs.com/2025/b0996b95-7ec3-82fe-91dd-224058266e30.htm",
 57802|       "dll_signature_verified": true,
 57803|       "dll_relationship_scope": "declared",
 57804|       "dll_semantic_verified": null,
 57805|       "dll_verified_status": "signature_verified_declared",
 57806|       "revitlookup_referenced": true,
 57807|       "revitlookup_requires_document_context": true
 57808|     },
 57809|     {
 57810|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57811|       "target": "Autodesk.Revit.DB.ViewSheet",
 57812|       "member_name": "IsValidCategoryForFilterBySheet",
 57813|       "member_kind": "method",
 57814|       "edge_type": "PLACED_ON_SHEET",
 57815|       "confidence": "name_only_candidate",
 57816|       "confidence_tier": "likely",
 57817|       "target_resolution": "exact",
 57818|       "evidence": [
 57819|         "member name 'IsValidCategoryForFilterBySheet' matches keyword pattern /Sheet/ but return type 'bool' gives no type-level confirmation"
 57820|       ],
 57821|       "source_url": "https://www.revitapidocs.com/2025/150856e4-938c-b7a6-9c27-5c46d60898b2.htm",
 57822|       "dll_signature_verified": true,
 57823|       "dll_relationship_scope": "declared",
 57824|       "dll_semantic_verified": null,
 57825|       "dll_verified_status": "signature_verified_declared",
 57826|       "revitlookup_referenced": null,
 57827|       "revitlookup_requires_document_context": null
 57828|     },
 57829|     {
 57830|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57831|       "target": null,
 57832|       "member_name": "IsValidCombinedParameters",
 57833|       "member_kind": "method",
 57834|       "edge_type": "HAS_PARAMETER",
 57835|       "confidence": "name_only_candidate",
 57836|       "confidence_tier": "likely",
 57837|       "target_resolution": "none",
 57838|       "evidence": [
 57839|         "member name 'IsValidCombinedParameters' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 57840|       ],
 57841|       "source_url": "https://www.revitapidocs.com/2025/38e0bbe3-61fe-5565-bf74-96089108e2ee.htm",
 57842|       "dll_signature_verified": true,
 57843|       "dll_relationship_scope": "declared",
 57844|       "dll_semantic_verified": null,
 57845|       "dll_verified_status": "signature_verified_declared",
 57846|       "revitlookup_referenced": null,
 57847|       "revitlookup_requires_document_context": null
 57848|     },
 57849|     {
 57850|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57851|       "target": null,
 57852|       "member_name": "RemoveSortGroupField",
 57853|       "member_kind": "method",
 57854|       "edge_type": "MEMBER_OF_GROUP",
 57855|       "confidence": "name_only_candidate",
 57856|       "confidence_tier": "likely",
 57857|       "target_resolution": "none",
 57858|       "evidence": [
 57859|         "member name 'RemoveSortGroupField' matches keyword pattern /^GetMember|Group/ but return type 'void' gives no type-level confirmation"
 57860|       ],
 57861|       "source_url": "https://www.revitapidocs.com/2025/6e47776f-9a70-f422-0f14-35fd72b81b36.htm",
 57862|       "dll_signature_verified": true,
 57863|       "dll_relationship_scope": "declared",
 57864|       "dll_semantic_verified": null,
 57865|       "dll_verified_status": "signature_verified_declared",
 57866|       "revitlookup_referenced": null,
 57867|       "revitlookup_requires_document_context": null
 57868|     },
 57869|     {
 57870|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57871|       "target": null,
 57872|       "member_name": "SetSortGroupField",
 57873|       "member_kind": "method",
 57874|       "edge_type": "MEMBER_OF_GROUP",
 57875|       "confidence": "name_only_candidate",
 57876|       "confidence_tier": "likely",
 57877|       "target_resolution": "none",
 57878|       "evidence": [
 57879|         "member name 'SetSortGroupField' matches keyword pattern /^GetMember|Group/ but return type 'void' gives no type-level confirmation"
 57880|       ],
 57881|       "source_url": "https://www.revitapidocs.com/2025/37b78a55-2926-62c9-7c53-5beabc3852da.htm",
 57882|       "dll_signature_verified": true,
 57883|       "dll_relationship_scope": "declared",
 57884|       "dll_semantic_verified": null,
 57885|       "dll_verified_status": "signature_verified_declared",
 57886|       "revitlookup_referenced": null,
 57887|       "revitlookup_requires_document_context": null
 57888|     },
 57889|     {
 57890|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57891|       "target": null,
 57892|       "member_name": "SetSortGroupFields",
 57893|       "member_kind": "method",
 57894|       "edge_type": "MEMBER_OF_GROUP",
 57895|       "confidence": "name_only_candidate",
 57896|       "confidence_tier": "likely",
 57897|       "target_resolution": "none",
 57898|       "evidence": [
 57899|         "member name 'SetSortGroupFields' matches keyword pattern /^GetMember|Group/ but return type 'void' gives no type-level confirmation"
 57900|       ],
 57901|       "source_url": "https://www.revitapidocs.com/2025/e45815f2-fc16-9686-9bf8-f1d3b5976cfa.htm",
 57902|       "dll_signature_verified": true,
 57903|       "dll_relationship_scope": "declared",
 57904|       "dll_semantic_verified": null,
 57905|       "dll_verified_status": "signature_verified_declared",
 57906|       "revitlookup_referenced": null,
 57907|       "revitlookup_requires_document_context": null
 57908|     },
 57909|     {
 57910|       "source": "Autodesk.Revit.DB.ScheduleField",
 57911|       "target": "Autodesk.Revit.DB.ScheduleDefinition",
 57912|       "member_name": "Definition",
 57913|       "member_kind": "property",
 57914|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57915|       "confidence": "direct_return_type",
 57916|       "confidence_tier": "unverified_reference",
 57917|       "target_resolution": "exact",
 57918|       "evidence": [
 57919|         "return type 'ScheduleDefinition' directly names a Revit DB object type"
 57920|       ],
 57921|       "source_url": "https://www.revitapidocs.com/2025/0fa34479-59f2-7f67-4d16-48238dc4d2af.htm",
 57922|       "dll_signature_verified": true,
 57923|       "dll_relationship_scope": "declared",
 57924|       "dll_semantic_verified": null,
 57925|       "dll_verified_status": "signature_verified_declared",
 57926|       "revitlookup_referenced": null,
 57927|       "revitlookup_requires_document_context": null
 57928|     },
 57929|     {
 57930|       "source": "Autodesk.Revit.DB.ScheduleField",
 57931|       "target": "Autodesk.Revit.DB.ScheduleFieldId",
 57932|       "member_name": "FieldId",
 57933|       "member_kind": "property",
 57934|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57935|       "confidence": "direct_return_type",
 57936|       "confidence_tier": "unverified_reference",
 57937|       "target_resolution": "exact",
 57938|       "evidence": [
 57939|         "return type 'ScheduleFieldId' directly names a Revit DB object type"
 57940|       ],
 57941|       "source_url": "https://www.revitapidocs.com/2025/e7b1a3c3-1ab5-9e65-a59e-fed8a7d27d42.htm",
 57942|       "dll_signature_verified": true,
 57943|       "dll_relationship_scope": "declared",
 57944|       "dll_semantic_verified": null,
 57945|       "dll_verified_status": "signature_verified_declared",
 57946|       "revitlookup_referenced": null,
 57947|       "revitlookup_requires_document_context": null
 57948|     },
 57949|     {
 57950|       "source": "Autodesk.Revit.DB.ScheduleField",
 57951|       "target": null,
 57952|       "member_name": "IsCombinedParameterField",
 57953|       "member_kind": "property",
 57954|       "edge_type": "HAS_PARAMETER",
 57955|       "confidence": "name_only_candidate",
 57956|       "confidence_tier": "likely",
 57957|       "target_resolution": "none",
 57958|       "evidence": [
 57959|         "member name 'IsCombinedParameterField' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 57960|       ],
 57961|       "source_url": "https://www.revitapidocs.com/2025/52da022b-4dcd-09dd-3137-d32f47ccbfee.htm",
 57962|       "dll_signature_verified": true,
 57963|       "dll_relationship_scope": "declared",
 57964|       "dll_semantic_verified": null,
 57965|       "dll_verified_status": "signature_verified_declared",
 57966|       "revitlookup_referenced": null,
 57967|       "revitlookup_requires_document_context": null
 57968|     },
 57969|     {
 57970|       "source": "Autodesk.Revit.DB.ScheduleField",
 57971|       "target": null,
 57972|       "member_name": "ParameterId",
 57973|       "member_kind": "property",
 57974|       "edge_type": "HAS_PARAMETER",
 57975|       "confidence": "elementid_with_strong_name",
 57976|       "confidence_tier": "core",
 57977|       "target_resolution": "none",
 57978|       "evidence": [
 57979|         "member name 'ParameterId' matches keyword pattern /Parameter/"
 57980|       ],
 57981|       "source_url": "https://www.revitapidocs.com/2025/ecad009d-a968-2adc-9891-128e9ee8074a.htm",
 57982|       "dll_signature_verified": true,
 57983|       "dll_relationship_scope": "declared",
 57984|       "dll_semantic_verified": null,
 57985|       "dll_verified_status": "signature_verified_declared",
 57986|       "revitlookup_referenced": null,
 57987|       "revitlookup_requires_document_context": null
 57988|     },
 57989|     {
 57990|       "source": "Autodesk.Revit.DB.ScheduleField",
 57991|       "target": "Autodesk.Revit.DB.ScheduleFieldId",
 57992|       "member_name": "PercentageBy",
 57993|       "member_kind": "property",
 57994|       "edge_type": "TAGS_ELEMENT",
 57995|       "confidence": "direct_return_type",
 57996|       "confidence_tier": "core",
 57997|       "target_resolution": "exact",
 57998|       "evidence": [
 57999|         "return type 'ScheduleFieldId' directly names a Revit DB object type"
 58000|       ],
 58001|       "source_url": "https://www.revitapidocs.com/2025/7c606b36-212f-0392-6eb5-799ab748a330.htm",
 58002|       "dll_signature_verified": true,
 58003|       "dll_relationship_scope": "declared",
 58004|       "dll_semantic_verified": null,
 58005|       "dll_verified_status": "signature_verified_declared",
 58006|       "revitlookup_referenced": null,
 58007|       "revitlookup_requires_document_context": null
 58008|     },
 58009|     {
 58010|       "source": "Autodesk.Revit.DB.ScheduleField",
 58011|       "target": "Autodesk.Revit.DB.ScheduleFieldId",
 58012|       "member_name": "PercentageOf",
 58013|       "member_kind": "property",
 58014|       "edge_type": "TAGS_ELEMENT",
 58015|       "confidence": "direct_return_type",
 58016|       "confidence_tier": "core",
 58017|       "target_resolution": "exact",
 58018|       "evidence": [
 58019|         "return type 'ScheduleFieldId' directly names a Revit DB object type"
 58020|       ],
 58021|       "source_url": "https://www.revitapidocs.com/2025/12f76318-e8fa-d5b8-d52e-434a07f159f9.htm",
 58022|       "dll_signature_verified": true,
 58023|       "dll_relationship_scope": "declared",
 58024|       "dll_semantic_verified": null,
 58025|       "dll_verified_status": "signature_verified_declared",
 58026|       "revitlookup_referenced": null,
 58027|       "revitlookup_requires_document_context": null
 58028|     },
 58029|     {
 58030|       "source": "Autodesk.Revit.DB.ScheduleField",
 58031|       "target": "Autodesk.Revit.DB.ViewSchedule",
 58032|       "member_name": "Schedule",
 58033|       "member_kind": "property",
 58034|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58035|       "confidence": "direct_return_type",
 58036|       "confidence_tier": "unverified_reference",
 58037|       "target_resolution": "exact",
 58038|       "evidence": [
 58039|         "return type 'ViewSchedule' directly names a Revit DB object type"
 58040|       ],
 58041|       "source_url": "https://www.revitapidocs.com/2025/1b5f2a55-5ea2-e468-b887-7f3c98aa6e85.htm",
 58042|       "dll_signature_verified": true,
 58043|       "dll_relationship_scope": "declared",
 58044|       "dll_semantic_verified": null,
 58045|       "dll_verified_status": "signature_verified_declared",
 58046|       "revitlookup_referenced": null,
 58047|       "revitlookup_requires_document_context": null
 58048|     },
 58049|     {
 58050|       "source": "Autodesk.Revit.DB.ScheduleField",
 58051|       "target": "Autodesk.Revit.DB.ViewSheet",
 58052|       "member_name": "SheetColumnWidth",
 58053|       "member_kind": "property",
 58054|       "edge_type": "PLACED_ON_SHEET",
 58055|       "confidence": "name_only_candidate",
 58056|       "confidence_tier": "likely",
 58057|       "target_resolution": "exact",
 58058|       "evidence": [
 58059|         "member name 'SheetColumnWidth' matches keyword pattern /Sheet/ but return type 'double' gives no type-level confirmation"
 58060|       ],
 58061|       "source_url": "https://www.revitapidocs.com/2025/999e9e46-2259-19f4-cfc1-9c52509a2385.htm",
 58062|       "dll_signature_verified": true,
 58063|       "dll_relationship_scope": "declared",
 58064|       "dll_semantic_verified": null,
 58065|       "dll_verified_status": "signature_verified_declared",
 58066|       "revitlookup_referenced": null,
 58067|       "revitlookup_requires_document_context": null
 58068|     },
 58069|     {
 58070|       "source": "Autodesk.Revit.DB.ScheduleField",
 58071|       "target": null,
 58072|       "member_name": "TotalByAssemblyType",
 58073|       "member_kind": "property",
 58074|       "edge_type": "MEMBER_OF_ASSEMBLY",
 58075|       "confidence": "name_only_candidate",
 58076|       "confidence_tier": "likely",
 58077|       "target_resolution": "none",
 58078|       "evidence": [
 58079|         "member name 'TotalByAssemblyType' matches keyword pattern /Assembly/ but return type 'bool' gives no type-level confirmation"
 58080|       ],
 58081|       "source_url": "https://www.revitapidocs.com/2025/672a1283-cdb4-f7fb-b697-f67238c8755c.htm",
 58082|       "dll_signature_verified": true,
 58083|       "dll_relationship_scope": "declared",
 58084|       "dll_semantic_verified": null,
 58085|       "dll_verified_status": "signature_verified_declared",
 58086|       "revitlookup_referenced": null,
 58087|       "revitlookup_requires_document_context": null
 58088|     },
 58089|     {
 58090|       "source": "Autodesk.Revit.DB.ScheduleField",
 58091|       "target": null,
 58092|       "member_name": "CanTotalByAssemblyType",
 58093|       "member_kind": "method",
 58094|       "edge_type": "MEMBER_OF_ASSEMBLY",
 58095|       "confidence": "name_only_candidate",
 58096|       "confidence_tier": "likely",
 58097|       "target_resolution": "none",
 58098|       "evidence": [
 58099|         "member name 'CanTotalByAssemblyType' matches keyword pattern /Assembly/ but return type 'bool' gives no type-level confirmation"
 58100|       ],
 58101|       "source_url": "https://www.revitapidocs.com/2025/bb92f36f-f3ef-9aa5-eb1e-50f830726f51.htm",
 58102|       "dll_signature_verified": true,
 58103|       "dll_relationship_scope": "declared",
 58104|       "dll_semantic_verified": null,
 58105|       "dll_verified_status": "signature_verified_declared",
 58106|       "revitlookup_referenced": null,
 58107|       "revitlookup_requires_document_context": null
 58108|     },
 58109|     {
 58110|       "source": "Autodesk.Revit.DB.ScheduleField",
 58111|       "target": "Autodesk.Revit.DB.TableCellCombinedParameterData",
 58112|       "member_name": "GetCombinedParameters",
 58113|       "member_kind": "method",
 58114|       "edge_type": "HAS_PARAMETER",
 58115|       "confidence": "needs_runtime_validation",
 58116|       "confidence_tier": "needs_validation",
 58117|       "target_resolution": "exact",
 58118|       "evidence": [
 58119|         "return type 'IList < TableCellCombinedParameterData >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 58120|       ],
```
