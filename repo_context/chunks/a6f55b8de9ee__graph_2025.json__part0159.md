# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 159 of 216
- Original line range: 61621-62020
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 61621|       ],
 61622|       "source_url": "https://www.revitapidocs.com/2025/7becc6f0-0510-745b-b4ba-24bbf875919d.htm",
 61623|       "dll_signature_verified": true,
 61624|       "dll_relationship_scope": "declared",
 61625|       "dll_semantic_verified": null,
 61626|       "dll_verified_status": "signature_verified_declared",
 61627|       "revitlookup_referenced": null,
 61628|       "revitlookup_requires_document_context": null
 61629|     },
 61630|     {
 61631|       "source": "Autodesk.Revit.DB.View",
 61632|       "target": null,
 61633|       "member_name": "AssociatedAssemblyInstanceId",
 61634|       "member_kind": "property",
 61635|       "edge_type": "MEMBER_OF_ASSEMBLY",
 61636|       "confidence": "elementid_with_strong_name",
 61637|       "confidence_tier": "core",
 61638|       "target_resolution": "none",
 61639|       "evidence": [
 61640|         "member name 'AssociatedAssemblyInstanceId' matches keyword pattern /Assembly/"
 61641|       ],
 61642|       "source_url": "https://www.revitapidocs.com/2025/57f30f8b-8155-8916-4bef-b904e22b583e.htm",
 61643|       "dll_signature_verified": true,
 61644|       "dll_relationship_scope": "declared",
 61645|       "dll_semantic_verified": null,
 61646|       "dll_verified_status": "signature_verified_declared",
 61647|       "revitlookup_referenced": null,
 61648|       "revitlookup_requires_document_context": null
 61649|     },
 61650|     {
 61651|       "source": "Autodesk.Revit.DB.View",
 61652|       "target": "Autodesk.Revit.DB.Level",
 61653|       "member_name": "DetailLevel",
 61654|       "member_kind": "property",
 61655|       "edge_type": "ASSIGNED_TO_LEVEL",
 61656|       "confidence": "name_only_candidate",
 61657|       "confidence_tier": "likely",
 61658|       "target_resolution": "exact",
 61659|       "evidence": [
 61660|         "member name 'DetailLevel' matches keyword pattern /Level/ but return type 'ViewDetailLevel' gives no type-level confirmation"
 61661|       ],
 61662|       "source_url": "https://www.revitapidocs.com/2025/a4e9896a-606e-8b97-c8a7-a8397419145e.htm",
 61663|       "dll_signature_verified": true,
 61664|       "dll_relationship_scope": "declared",
 61665|       "dll_semantic_verified": null,
 61666|       "dll_verified_status": "signature_verified_declared",
 61667|       "revitlookup_referenced": null,
 61668|       "revitlookup_requires_document_context": null
 61669|     },
 61670|     {
 61671|       "source": "Autodesk.Revit.DB.View",
 61672|       "target": "Autodesk.Revit.DB.Level",
 61673|       "member_name": "GenLevel",
 61674|       "member_kind": "property",
 61675|       "edge_type": "ASSIGNED_TO_LEVEL",
 61676|       "confidence": "direct_return_type",
 61677|       "confidence_tier": "core",
 61678|       "target_resolution": "exact",
 61679|       "evidence": [
 61680|         "return type 'Level' directly names a Revit DB object type"
 61681|       ],
 61682|       "source_url": "https://www.revitapidocs.com/2025/95f430ac-65e5-87b1-aea9-937412ef2eb8.htm",
 61683|       "dll_signature_verified": true,
 61684|       "dll_relationship_scope": "declared",
 61685|       "dll_semantic_verified": null,
 61686|       "dll_verified_status": "signature_verified_declared",
 61687|       "revitlookup_referenced": null,
 61688|       "revitlookup_requires_document_context": null
 61689|     },
 61690|     {
 61691|       "source": "Autodesk.Revit.DB.View",
 61692|       "target": null,
 61693|       "member_name": "IsAssemblyView",
 61694|       "member_kind": "property",
 61695|       "edge_type": "MEMBER_OF_ASSEMBLY",
 61696|       "confidence": "name_only_candidate",
 61697|       "confidence_tier": "likely",
 61698|       "target_resolution": "none",
 61699|       "evidence": [
 61700|         "member name 'IsAssemblyView' matches keyword pattern /Assembly/ but return type 'bool' gives no type-level confirmation"
 61701|       ],
 61702|       "source_url": "https://www.revitapidocs.com/2025/259a699e-b118-58d9-debe-7975d04fd2bb.htm",
 61703|       "dll_signature_verified": true,
 61704|       "dll_relationship_scope": "declared",
 61705|       "dll_semantic_verified": null,
 61706|       "dll_verified_status": "signature_verified_declared",
 61707|       "revitlookup_referenced": null,
 61708|       "revitlookup_requires_document_context": null
 61709|     },
 61710|     {
 61711|       "source": "Autodesk.Revit.DB.View",
 61712|       "target": "Autodesk.Revit.DB.View",
 61713|       "member_name": "IsTemplate",
 61714|       "member_kind": "property",
 61715|       "edge_type": "CONTROLLED_BY_TEMPLATE",
 61716|       "confidence": "name_only_candidate",
 61717|       "confidence_tier": "likely",
 61718|       "target_resolution": "exact",
 61719|       "evidence": [
 61720|         "member name 'IsTemplate' matches keyword pattern /Template/ but return type 'bool' gives no type-level confirmation"
 61721|       ],
 61722|       "source_url": "https://www.revitapidocs.com/2025/bd7d3469-dc89-93b7-1cb8-848fd4f38d65.htm",
 61723|       "dll_signature_verified": true,
 61724|       "dll_relationship_scope": "declared",
 61725|       "dll_semantic_verified": null,
 61726|       "dll_verified_status": "signature_verified_declared",
 61727|       "revitlookup_referenced": null,
 61728|       "revitlookup_requires_document_context": null
 61729|     },
 61730|     {
 61731|       "source": "Autodesk.Revit.DB.View",
 61732|       "target": "Autodesk.Revit.DB.BoundingBoxUV",
 61733|       "member_name": "Outline",
 61734|       "member_kind": "property",
 61735|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61736|       "confidence": "direct_return_type",
 61737|       "confidence_tier": "unverified_reference",
 61738|       "target_resolution": "exact",
 61739|       "evidence": [
 61740|         "return type 'BoundingBoxUV' directly names a Revit DB object type"
 61741|       ],
 61742|       "source_url": "https://www.revitapidocs.com/2025/43a99f7c-6eb1-efb9-6aff-9540c434f8f2.htm",
 61743|       "dll_signature_verified": true,
 61744|       "dll_relationship_scope": "declared",
 61745|       "dll_semantic_verified": null,
 61746|       "dll_verified_status": "signature_verified_declared",
 61747|       "revitlookup_referenced": null,
 61748|       "revitlookup_requires_document_context": null
 61749|     },
 61750|     {
 61751|       "source": "Autodesk.Revit.DB.View",
 61752|       "target": "Autodesk.Revit.DB.SketchPlane",
 61753|       "member_name": "SketchPlane",
 61754|       "member_kind": "property",
 61755|       "edge_type": "REFERENCES",
 61756|       "confidence": "direct_return_type",
 61757|       "confidence_tier": "core",
 61758|       "target_resolution": "exact",
 61759|       "evidence": [
 61760|         "return type 'SketchPlane' directly names a Revit DB object type"
 61761|       ],
 61762|       "source_url": "https://www.revitapidocs.com/2025/2531634f-f0d4-3cb3-7f8a-fe809d33a61f.htm",
 61763|       "dll_signature_verified": true,
 61764|       "dll_relationship_scope": "declared",
 61765|       "dll_semantic_verified": null,
 61766|       "dll_verified_status": "signature_verified_declared",
 61767|       "revitlookup_referenced": null,
 61768|       "revitlookup_requires_document_context": null
 61769|     },
 61770|     {
 61771|       "source": "Autodesk.Revit.DB.View",
 61772|       "target": "Autodesk.Revit.DB.SunAndShadowSettings",
 61773|       "member_name": "SunAndShadowSettings",
 61774|       "member_kind": "property",
 61775|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61776|       "confidence": "direct_return_type",
 61777|       "confidence_tier": "unverified_reference",
 61778|       "target_resolution": "exact",
 61779|       "evidence": [
 61780|         "return type 'SunAndShadowSettings' directly names a Revit DB object type"
 61781|       ],
 61782|       "source_url": "https://www.revitapidocs.com/2025/899dbf41-8961-7f31-2810-bffb87b32dd2.htm",
 61783|       "dll_signature_verified": true,
 61784|       "dll_relationship_scope": "declared",
 61785|       "dll_semantic_verified": null,
 61786|       "dll_verified_status": "signature_verified_declared",
 61787|       "revitlookup_referenced": null,
 61788|       "revitlookup_requires_document_context": null
 61789|     },
 61790|     {
 61791|       "source": "Autodesk.Revit.DB.View",
 61792|       "target": "Autodesk.Revit.DB.TemporaryViewModes",
 61793|       "member_name": "TemporaryViewModes",
 61794|       "member_kind": "property",
 61795|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61796|       "confidence": "direct_return_type",
 61797|       "confidence_tier": "unverified_reference",
 61798|       "target_resolution": "exact",
 61799|       "evidence": [
 61800|         "return type 'TemporaryViewModes' directly names a Revit DB object type"
 61801|       ],
 61802|       "source_url": "https://www.revitapidocs.com/2025/4828f9cb-4759-2fdb-e842-a592533f6b8c.htm",
 61803|       "dll_signature_verified": true,
 61804|       "dll_relationship_scope": "declared",
 61805|       "dll_semantic_verified": null,
 61806|       "dll_verified_status": "signature_verified_declared",
 61807|       "revitlookup_referenced": null,
 61808|       "revitlookup_requires_document_context": null
 61809|     },
 61810|     {
 61811|       "source": "Autodesk.Revit.DB.View",
 61812|       "target": "Autodesk.Revit.DB.View",
 61813|       "member_name": "ViewTemplateId",
 61814|       "member_kind": "property",
 61815|       "edge_type": "CONTROLLED_BY_TEMPLATE",
 61816|       "confidence": "elementid_with_strong_name",
 61817|       "confidence_tier": "core",
 61818|       "target_resolution": "exact",
 61819|       "evidence": [
 61820|         "member name 'ViewTemplateId' matches keyword pattern /Template/"
 61821|       ],
 61822|       "source_url": "https://www.revitapidocs.com/2025/2559f20b-87d4-e879-3139-7f555b251b71.htm",
 61823|       "dll_signature_verified": true,
 61824|       "dll_relationship_scope": "declared",
 61825|       "dll_semantic_verified": null,
 61826|       "dll_verified_status": "signature_verified_declared",
 61827|       "revitlookup_referenced": null,
 61828|       "revitlookup_requires_document_context": null
 61829|     },
 61830|     {
 61831|       "source": "Autodesk.Revit.DB.View",
 61832|       "target": "Autodesk.Revit.DB.View",
 61833|       "member_name": "ApplyViewTemplateParameters",
 61834|       "member_kind": "method",
 61835|       "edge_type": "CONTROLLED_BY_TEMPLATE",
 61836|       "confidence": "name_only_candidate",
 61837|       "confidence_tier": "likely",
 61838|       "target_resolution": "exact",
 61839|       "evidence": [
 61840|         "member name 'ApplyViewTemplateParameters' matches keyword pattern /Template/ but return type 'void' gives no type-level confirmation"
 61841|       ],
 61842|       "source_url": "https://www.revitapidocs.com/2025/2e27f324-a743-85d3-e232-13df5dbcf58e.htm",
 61843|       "dll_signature_verified": true,
 61844|       "dll_relationship_scope": "declared",
 61845|       "dll_semantic_verified": null,
 61846|       "dll_verified_status": "signature_verified_declared",
 61847|       "revitlookup_referenced": null,
 61848|       "revitlookup_requires_document_context": null
 61849|     },
 61850|     {
 61851|       "source": "Autodesk.Revit.DB.View",
 61852|       "target": "Autodesk.Revit.DB.Category",
 61853|       "member_name": "CanCategoryBeHidden",
 61854|       "member_kind": "method",
 61855|       "edge_type": "HAS_CATEGORY",
 61856|       "confidence": "name_only_candidate",
 61857|       "confidence_tier": "likely",
 61858|       "target_resolution": "exact",
 61859|       "evidence": [
 61860|         "member name 'CanCategoryBeHidden' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 61861|       ],
 61862|       "source_url": "https://www.revitapidocs.com/2025/238a1789-90e1-2527-66e3-867db66a9b3b.htm",
 61863|       "dll_signature_verified": true,
 61864|       "dll_relationship_scope": "declared",
 61865|       "dll_semantic_verified": null,
 61866|       "dll_verified_status": "signature_verified_declared",
 61867|       "revitlookup_referenced": true,
 61868|       "revitlookup_requires_document_context": true
 61869|     },
 61870|     {
 61871|       "source": "Autodesk.Revit.DB.View",
 61872|       "target": "Autodesk.Revit.DB.Category",
 61873|       "member_name": "CanCategoryBeHiddenTemporary",
 61874|       "member_kind": "method",
 61875|       "edge_type": "HAS_CATEGORY",
 61876|       "confidence": "name_only_candidate",
 61877|       "confidence_tier": "likely",
 61878|       "target_resolution": "exact",
 61879|       "evidence": [
 61880|         "member name 'CanCategoryBeHiddenTemporary' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 61881|       ],
 61882|       "source_url": "https://www.revitapidocs.com/2025/d6d5309a-4dfc-ead8-fc27-97b459111fb8.htm",
 61883|       "dll_signature_verified": true,
 61884|       "dll_relationship_scope": "declared",
 61885|       "dll_semantic_verified": null,
 61886|       "dll_verified_status": "signature_verified_declared",
 61887|       "revitlookup_referenced": true,
 61888|       "revitlookup_requires_document_context": true
 61889|     },
 61890|     {
 61891|       "source": "Autodesk.Revit.DB.View",
 61892|       "target": "Autodesk.Revit.DB.Level",
 61893|       "member_name": "CanModifyDetailLevel",
 61894|       "member_kind": "method",
 61895|       "edge_type": "ASSIGNED_TO_LEVEL",
 61896|       "confidence": "name_only_candidate",
 61897|       "confidence_tier": "likely",
 61898|       "target_resolution": "exact",
 61899|       "evidence": [
 61900|         "member name 'CanModifyDetailLevel' matches keyword pattern /Level/ but return type 'bool' gives no type-level confirmation"
 61901|       ],
 61902|       "source_url": "https://www.revitapidocs.com/2025/78d27a7c-e5eb-6029-2fd4-4287cc84f09d.htm",
 61903|       "dll_signature_verified": true,
 61904|       "dll_relationship_scope": "declared",
 61905|       "dll_semantic_verified": null,
 61906|       "dll_verified_status": "signature_verified_declared",
 61907|       "revitlookup_referenced": null,
 61908|       "revitlookup_requires_document_context": null
 61909|     },
 61910|     {
 61911|       "source": "Autodesk.Revit.DB.View",
 61912|       "target": null,
 61913|       "member_name": "ConvertToIndependent",
 61914|       "member_kind": "method",
 61915|       "edge_type": "DEPENDS_ON",
 61916|       "confidence": "name_only_candidate",
 61917|       "confidence_tier": "likely",
 61918|       "target_resolution": "none",
 61919|       "evidence": [
 61920|         "member name 'ConvertToIndependent' matches keyword pattern /^GetDependent|Dependent/ but return type 'void' gives no type-level confirmation"
 61921|       ],
 61922|       "source_url": "https://www.revitapidocs.com/2025/fff3da7f-b885-f543-e12d-3f75e9b5bd94.htm",
 61923|       "dll_signature_verified": true,
 61924|       "dll_relationship_scope": "declared",
 61925|       "dll_semantic_verified": null,
 61926|       "dll_verified_status": "signature_verified_declared",
 61927|       "revitlookup_referenced": null,
 61928|       "revitlookup_requires_document_context": null
 61929|     },
 61930|     {
 61931|       "source": "Autodesk.Revit.DB.View",
 61932|       "target": null,
 61933|       "member_name": "Duplicate",
 61934|       "member_kind": "method",
 61935|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 61936|       "confidence": "unknown_reference",
 61937|       "confidence_tier": "unverified_reference",
 61938|       "target_resolution": "none",
 61939|       "evidence": [
 61940|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 61941|       ],
 61942|       "source_url": "https://www.revitapidocs.com/2025/0cb1793f-1df0-d5b4-3b72-8d468b80199e.htm",
 61943|       "dll_signature_verified": true,
 61944|       "dll_relationship_scope": "declared",
 61945|       "dll_semantic_verified": null,
 61946|       "dll_verified_status": "signature_verified_declared",
 61947|       "revitlookup_referenced": null,
 61948|       "revitlookup_requires_document_context": null
 61949|     },
 61950|     {
 61951|       "source": "Autodesk.Revit.DB.View",
 61952|       "target": "Autodesk.Revit.DB.ViewDisplayBackground",
 61953|       "member_name": "GetBackground",
 61954|       "member_kind": "method",
 61955|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 61956|       "confidence": "direct_return_type",
 61957|       "confidence_tier": "unverified_reference",
 61958|       "target_resolution": "exact",
 61959|       "evidence": [
 61960|         "return type 'ViewDisplayBackground' directly names a Revit DB object type"
 61961|       ],
 61962|       "source_url": "https://www.revitapidocs.com/2025/0f62287e-e8b8-0f6a-ab07-b246b0f42c7f.htm",
 61963|       "dll_signature_verified": true,
 61964|       "dll_relationship_scope": "declared",
 61965|       "dll_semantic_verified": null,
 61966|       "dll_verified_status": "signature_verified_declared",
 61967|       "revitlookup_referenced": null,
 61968|       "revitlookup_requires_document_context": null
 61969|     },
 61970|     {
 61971|       "source": "Autodesk.Revit.DB.View",
 61972|       "target": null,
 61973|       "member_name": "GetCalloutParentId",
 61974|       "member_kind": "method",
 61975|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 61976|       "confidence": "unknown_reference",
 61977|       "confidence_tier": "unverified_reference",
 61978|       "target_resolution": "none",
 61979|       "evidence": [
 61980|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 61981|       ],
 61982|       "source_url": "https://www.revitapidocs.com/2025/aa470950-40bc-6ec8-2c91-f6f423b8cc2e.htm",
 61983|       "dll_signature_verified": true,
 61984|       "dll_relationship_scope": "declared",
 61985|       "dll_semantic_verified": null,
 61986|       "dll_verified_status": "signature_verified_declared",
 61987|       "revitlookup_referenced": null,
 61988|       "revitlookup_requires_document_context": null
 61989|     },
 61990|     {
 61991|       "source": "Autodesk.Revit.DB.View",
 61992|       "target": "Autodesk.Revit.DB.Category",
 61993|       "member_name": "GetCategoryHidden",
 61994|       "member_kind": "method",
 61995|       "edge_type": "HAS_CATEGORY",
 61996|       "confidence": "name_only_candidate",
 61997|       "confidence_tier": "likely",
 61998|       "target_resolution": "exact",
 61999|       "evidence": [
 62000|         "member name 'GetCategoryHidden' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 62001|       ],
 62002|       "source_url": "https://www.revitapidocs.com/2025/52ce4cea-6f27-9e85-f82a-115e308eebfc.htm",
 62003|       "dll_signature_verified": true,
 62004|       "dll_relationship_scope": "declared",
 62005|       "dll_semantic_verified": null,
 62006|       "dll_verified_status": "signature_verified_declared",
 62007|       "revitlookup_referenced": true,
 62008|       "revitlookup_requires_document_context": true
 62009|     },
 62010|     {
 62011|       "source": "Autodesk.Revit.DB.View",
 62012|       "target": "Autodesk.Revit.DB.OverrideGraphicSettings",
 62013|       "member_name": "GetCategoryOverrides",
 62014|       "member_kind": "method",
 62015|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 62016|       "confidence": "direct_return_type",
 62017|       "confidence_tier": "unverified_reference",
 62018|       "target_resolution": "exact",
 62019|       "evidence": [
 62020|         "member name 'GetCategoryOverrides' matches keyword pattern /Category/ implying target 'Category', but the actual return type 'OverrideGraphicSettings' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
```
