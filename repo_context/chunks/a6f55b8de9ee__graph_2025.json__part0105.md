# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 105 of 216
- Original line range: 40561-40960
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 40561|       "confidence_tier": "likely",
 40562|       "target_resolution": "exact",
 40563|       "evidence": [
 40564|         "member name 'IsDemolishedPhaseOrderValid' matches keyword pattern /Phase/ but return type 'bool' gives no type-level confirmation"
 40565|       ],
 40566|       "source_url": "https://www.revitapidocs.com/2025/46ec60b6-b1c5-25aa-c544-34379298c7b8.htm",
 40567|       "dll_signature_verified": true,
 40568|       "dll_relationship_scope": "declared",
 40569|       "dll_semantic_verified": null,
 40570|       "dll_verified_status": "signature_verified_declared",
 40571|       "revitlookup_referenced": true,
 40572|       "revitlookup_requires_document_context": true
 40573|     },
 40574|     {
 40575|       "source": "Autodesk.Revit.DB.Element",
 40576|       "target": "Autodesk.Revit.DB.Phase",
 40577|       "member_name": "IsPhaseCreatedValid",
 40578|       "member_kind": "method",
 40579|       "edge_type": "ASSIGNED_TO_PHASE",
 40580|       "confidence": "name_only_candidate",
 40581|       "confidence_tier": "likely",
 40582|       "target_resolution": "exact",
 40583|       "evidence": [
 40584|         "member name 'IsPhaseCreatedValid' matches keyword pattern /Phase/ but return type 'bool' gives no type-level confirmation"
 40585|       ],
 40586|       "source_url": "https://www.revitapidocs.com/2025/ae48b10d-4a66-ee2c-85bf-f426435d0dbe.htm",
 40587|       "dll_signature_verified": true,
 40588|       "dll_relationship_scope": "declared",
 40589|       "dll_semantic_verified": null,
 40590|       "dll_verified_status": "signature_verified_declared",
 40591|       "revitlookup_referenced": true,
 40592|       "revitlookup_requires_document_context": true
 40593|     },
 40594|     {
 40595|       "source": "Autodesk.Revit.DB.Element",
 40596|       "target": "Autodesk.Revit.DB.Phase",
 40597|       "member_name": "IsPhaseDemolishedValid",
 40598|       "member_kind": "method",
 40599|       "edge_type": "ASSIGNED_TO_PHASE",
 40600|       "confidence": "name_only_candidate",
 40601|       "confidence_tier": "likely",
 40602|       "target_resolution": "exact",
 40603|       "evidence": [
 40604|         "member name 'IsPhaseDemolishedValid' matches keyword pattern /Phase/ but return type 'bool' gives no type-level confirmation"
 40605|       ],
 40606|       "source_url": "https://www.revitapidocs.com/2025/f97c9af7-fcbe-f617-d7ff-cfd4fb5af37f.htm",
 40607|       "dll_signature_verified": true,
 40608|       "dll_relationship_scope": "declared",
 40609|       "dll_semantic_verified": null,
 40610|       "dll_verified_status": "signature_verified_declared",
 40611|       "revitlookup_referenced": true,
 40612|       "revitlookup_requires_document_context": true
 40613|     },
 40614|     {
 40615|       "source": "Autodesk.Revit.DB.Element",
 40616|       "target": "Autodesk.Revit.DB.Parameter",
 40617|       "member_name": "LookupParameter",
 40618|       "member_kind": "method",
 40619|       "edge_type": "HAS_PARAMETER",
 40620|       "confidence": "direct_return_type",
 40621|       "confidence_tier": "core",
 40622|       "target_resolution": "exact",
 40623|       "evidence": [
 40624|         "return type 'Parameter' directly names a Revit DB object type"
 40625|       ],
 40626|       "source_url": "https://www.revitapidocs.com/2025/4400b9f8-3787-0947-5113-2522ff5e5de2.htm",
 40627|       "dll_signature_verified": true,
 40628|       "dll_relationship_scope": "declared",
 40629|       "dll_semantic_verified": null,
 40630|       "dll_verified_status": "signature_verified_declared",
 40631|       "revitlookup_referenced": null,
 40632|       "revitlookup_requires_document_context": null
 40633|     },
 40634|     {
 40635|       "source": "Autodesk.Revit.DB.ElementArray",
 40636|       "target": "Autodesk.Revit.DB.ElementArrayIterator",
 40637|       "member_name": "ForwardIterator",
 40638|       "member_kind": "method",
 40639|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 40640|       "confidence": "direct_return_type",
 40641|       "confidence_tier": "unverified_reference",
 40642|       "target_resolution": "exact",
 40643|       "evidence": [
 40644|         "return type 'ElementArrayIterator' directly names a Revit DB object type"
 40645|       ],
 40646|       "source_url": "https://www.revitapidocs.com/2025/62b1fae6-7763-674b-b012-85de805bf8d2.htm",
 40647|       "dll_signature_verified": true,
 40648|       "dll_relationship_scope": "declared",
 40649|       "dll_semantic_verified": null,
 40650|       "dll_verified_status": "signature_verified_declared",
 40651|       "revitlookup_referenced": null,
 40652|       "revitlookup_requires_document_context": null
 40653|     },
 40654|     {
 40655|       "source": "Autodesk.Revit.DB.ElementArray",
 40656|       "target": "Autodesk.Revit.DB.ElementArrayIterator",
 40657|       "member_name": "ReverseIterator",
 40658|       "member_kind": "method",
 40659|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 40660|       "confidence": "direct_return_type",
 40661|       "confidence_tier": "unverified_reference",
 40662|       "target_resolution": "exact",
 40663|       "evidence": [
 40664|         "return type 'ElementArrayIterator' directly names a Revit DB object type"
 40665|       ],
 40666|       "source_url": "https://www.revitapidocs.com/2025/f307398e-c7c1-21eb-e155-c5d535e205f2.htm",
 40667|       "dll_signature_verified": true,
 40668|       "dll_relationship_scope": "declared",
 40669|       "dll_semantic_verified": null,
 40670|       "dll_verified_status": "signature_verified_declared",
 40671|       "revitlookup_referenced": null,
 40672|       "revitlookup_requires_document_context": null
 40673|     },
 40674|     {
 40675|       "source": "Autodesk.Revit.DB.ElementBinding",
 40676|       "target": "Autodesk.Revit.DB.CategorySet",
 40677|       "member_name": "Categories",
 40678|       "member_kind": "property",
 40679|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 40680|       "confidence": "direct_return_type",
 40681|       "confidence_tier": "unverified_reference",
 40682|       "target_resolution": "exact",
 40683|       "evidence": [
 40684|         "return type 'CategorySet' directly names a Revit DB object type"
 40685|       ],
 40686|       "source_url": "https://www.revitapidocs.com/2025/ee40289c-c274-2c5d-def8-9ff211d06279.htm",
 40687|       "dll_signature_verified": true,
 40688|       "dll_relationship_scope": "declared",
 40689|       "dll_semantic_verified": null,
 40690|       "dll_verified_status": "signature_verified_declared",
 40691|       "revitlookup_referenced": null,
 40692|       "revitlookup_requires_document_context": null
 40693|     },
 40694|     {
 40695|       "source": "Autodesk.Revit.DB.ElementCategoryFilter",
 40696|       "target": "Autodesk.Revit.DB.Category",
 40697|       "member_name": "CategoryId",
 40698|       "member_kind": "property",
 40699|       "edge_type": "HAS_CATEGORY",
 40700|       "confidence": "elementid_with_strong_name",
 40701|       "confidence_tier": "core",
 40702|       "target_resolution": "exact",
 40703|       "evidence": [
 40704|         "member name 'CategoryId' matches keyword pattern /Category/"
 40705|       ],
 40706|       "source_url": "https://www.revitapidocs.com/2025/d68724ed-ae1e-3c6e-89d0-3e4280f745e5.htm",
 40707|       "dll_signature_verified": true,
 40708|       "dll_relationship_scope": "declared",
 40709|       "dll_semantic_verified": null,
 40710|       "dll_verified_status": "signature_verified_declared",
 40711|       "revitlookup_referenced": null,
 40712|       "revitlookup_requires_document_context": null
 40713|     },
 40714|     {
 40715|       "source": "Autodesk.Revit.DB.ElementDesignOptionFilter",
 40716|       "target": "Autodesk.Revit.DB.DesignOption",
 40717|       "member_name": "DesignOptionId",
 40718|       "member_kind": "property",
 40719|       "edge_type": "ASSIGNED_TO_DESIGN_OPTION",
 40720|       "confidence": "elementid_with_strong_name",
 40721|       "confidence_tier": "core",
 40722|       "target_resolution": "exact",
 40723|       "evidence": [
 40724|         "member name 'DesignOptionId' matches keyword pattern /DesignOption/"
 40725|       ],
 40726|       "source_url": "https://www.revitapidocs.com/2025/9730dc27-f547-6a93-a5d6-7c5d65e9b8c9.htm",
 40727|       "dll_signature_verified": true,
 40728|       "dll_relationship_scope": "declared",
 40729|       "dll_semantic_verified": null,
 40730|       "dll_verified_status": "signature_verified_declared",
 40731|       "revitlookup_referenced": null,
 40732|       "revitlookup_requires_document_context": null
 40733|     },
 40734|     {
 40735|       "source": "Autodesk.Revit.DB.ElementId",
 40736|       "target": null,
 40737|       "member_name": "InvalidElementId",
 40738|       "member_kind": "property",
 40739|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 40740|       "confidence": "unknown_reference",
 40741|       "confidence_tier": "unverified_reference",
 40742|       "target_resolution": "none",
 40743|       "evidence": [
 40744|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 40745|       ],
 40746|       "source_url": "https://www.revitapidocs.com/2025/08ae8886-6ab3-3ef5-d2e0-0da2ffa7bd2c.htm",
 40747|       "dll_signature_verified": true,
 40748|       "dll_relationship_scope": "declared",
 40749|       "dll_semantic_verified": null,
 40750|       "dll_verified_status": "signature_verified_declared",
 40751|       "revitlookup_referenced": null,
 40752|       "revitlookup_requires_document_context": null
 40753|     },
 40754|     {
 40755|       "source": "Autodesk.Revit.DB.ElementIdParameterValue",
 40756|       "target": null,
 40757|       "member_name": "Value",
 40758|       "member_kind": "property",
 40759|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 40760|       "confidence": "unknown_reference",
 40761|       "confidence_tier": "unverified_reference",
 40762|       "target_resolution": "none",
 40763|       "evidence": [
 40764|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 40765|       ],
 40766|       "source_url": "https://www.revitapidocs.com/2025/2f706aa4-5138-e192-b928-9a3388ab7b60.htm",
 40767|       "dll_signature_verified": true,
 40768|       "dll_relationship_scope": "declared",
 40769|       "dll_semantic_verified": null,
 40770|       "dll_verified_status": "signature_verified_declared",
 40771|       "revitlookup_referenced": null,
 40772|       "revitlookup_requires_document_context": null
 40773|     },
 40774|     {
 40775|       "source": "Autodesk.Revit.DB.ElementIdSetFilter",
 40776|       "target": null,
 40777|       "member_name": "GetIdsToInclude",
 40778|       "member_kind": "method",
 40779|       "edge_type": "RETURNS_ELEMENT_IDS",
 40780|       "confidence": "unknown_reference",
 40781|       "confidence_tier": "unverified_reference",
 40782|       "target_resolution": "none",
 40783|       "evidence": [
 40784|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 40785|       ],
 40786|       "source_url": "https://www.revitapidocs.com/2025/cadf037a-cfb1-354c-9d7a-701edeb70a6d.htm",
 40787|       "dll_signature_verified": true,
 40788|       "dll_relationship_scope": "declared",
 40789|       "dll_semantic_verified": null,
 40790|       "dll_verified_status": "signature_verified_declared",
 40791|       "revitlookup_referenced": null,
 40792|       "revitlookup_requires_document_context": null
 40793|     },
 40794|     {
 40795|       "source": "Autodesk.Revit.DB.ElementIntersectsElementFilter",
 40796|       "target": "Autodesk.Revit.DB.Element",
 40797|       "member_name": "GetElement",
 40798|       "member_kind": "method",
 40799|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 40800|       "confidence": "direct_return_type",
 40801|       "confidence_tier": "unverified_reference",
 40802|       "target_resolution": "exact",
 40803|       "evidence": [
 40804|         "return type 'Element' directly names a Revit DB object type"
 40805|       ],
 40806|       "source_url": "https://www.revitapidocs.com/2025/90610610-97aa-f61f-8c85-4eec449d0035.htm",
 40807|       "dll_signature_verified": true,
 40808|       "dll_relationship_scope": "declared",
 40809|       "dll_semantic_verified": null,
 40810|       "dll_verified_status": "signature_verified_declared",
 40811|       "revitlookup_referenced": null,
 40812|       "revitlookup_requires_document_context": null
 40813|     },
 40814|     {
 40815|       "source": "Autodesk.Revit.DB.ElementIntersectsFilter",
 40816|       "target": "Autodesk.Revit.DB.Category",
 40817|       "member_name": "IsCategorySupported",
 40818|       "member_kind": "method",
 40819|       "edge_type": "HAS_CATEGORY",
 40820|       "confidence": "name_only_candidate",
 40821|       "confidence_tier": "likely",
 40822|       "target_resolution": "exact",
 40823|       "evidence": [
 40824|         "member name 'IsCategorySupported' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 40825|       ],
 40826|       "source_url": "https://www.revitapidocs.com/2025/a85b752e-895f-1041-279e-bbab04ba6d1c.htm",
 40827|       "dll_signature_verified": true,
 40828|       "dll_relationship_scope": "declared",
 40829|       "dll_semantic_verified": null,
 40830|       "dll_verified_status": "signature_verified_declared",
 40831|       "revitlookup_referenced": null,
 40832|       "revitlookup_requires_document_context": null
 40833|     },
 40834|     {
 40835|       "source": "Autodesk.Revit.DB.ElementLevelFilter",
 40836|       "target": "Autodesk.Revit.DB.Level",
 40837|       "member_name": "LevelId",
 40838|       "member_kind": "property",
 40839|       "edge_type": "ASSIGNED_TO_LEVEL",
 40840|       "confidence": "elementid_with_strong_name",
 40841|       "confidence_tier": "core",
 40842|       "target_resolution": "exact",
 40843|       "evidence": [
 40844|         "member name 'LevelId' matches keyword pattern /Level/"
 40845|       ],
 40846|       "source_url": "https://www.revitapidocs.com/2025/edaa0ec7-c6fd-037a-3ee5-624953e4648c.htm",
 40847|       "dll_signature_verified": true,
 40848|       "dll_relationship_scope": "declared",
 40849|       "dll_semantic_verified": null,
 40850|       "dll_verified_status": "signature_verified_declared",
 40851|       "revitlookup_referenced": null,
 40852|       "revitlookup_requires_document_context": null
 40853|     },
 40854|     {
 40855|       "source": "Autodesk.Revit.DB.ElementLogicalFilter",
 40856|       "target": "Autodesk.Revit.DB.ElementFilter",
 40857|       "member_name": "GetFilters",
 40858|       "member_kind": "method",
 40859|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 40860|       "confidence": "needs_runtime_validation",
 40861|       "confidence_tier": "needs_validation",
 40862|       "target_resolution": "exact",
 40863|       "evidence": [
 40864|         "return type 'IList < ElementFilter >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 40865|       ],
 40866|       "source_url": "https://www.revitapidocs.com/2025/e7369155-14ee-f702-132f-b19c3a300c80.htm",
 40867|       "dll_signature_verified": true,
 40868|       "dll_relationship_scope": "declared",
 40869|       "dll_semantic_verified": null,
 40870|       "dll_verified_status": "signature_verified_declared",
 40871|       "revitlookup_referenced": null,
 40872|       "revitlookup_requires_document_context": null
 40873|     },
 40874|     {
 40875|       "source": "Autodesk.Revit.DB.ElementMulticategoryFilter",
 40876|       "target": "Autodesk.Revit.DB.Category",
 40877|       "member_name": "GetCategoryIds",
 40878|       "member_kind": "method",
 40879|       "edge_type": "HAS_CATEGORY",
 40880|       "confidence": "elementid_collection_with_strong_name",
 40881|       "confidence_tier": "core",
 40882|       "target_resolution": "exact",
 40883|       "evidence": [
 40884|         "member name 'GetCategoryIds' matches keyword pattern /Category/"
 40885|       ],
 40886|       "source_url": "https://www.revitapidocs.com/2025/9cac5860-9125-cad2-4390-7599dfd4581a.htm",
 40887|       "dll_signature_verified": true,
 40888|       "dll_relationship_scope": "declared",
 40889|       "dll_semantic_verified": null,
 40890|       "dll_verified_status": "signature_verified_declared",
 40891|       "revitlookup_referenced": null,
 40892|       "revitlookup_requires_document_context": null
 40893|     },
 40894|     {
 40895|       "source": "Autodesk.Revit.DB.ElementNode",
 40896|       "target": "Autodesk.Revit.DB.Document",
 40897|       "member_name": "Document",
 40898|       "member_kind": "property",
 40899|       "edge_type": "REFERENCES",
 40900|       "confidence": "direct_return_type",
 40901|       "confidence_tier": "core",
 40902|       "target_resolution": "exact",
 40903|       "evidence": [
 40904|         "return type 'Document' directly names a Revit DB object type"
 40905|       ],
 40906|       "source_url": "https://www.revitapidocs.com/2025/140d8019-121f-927e-6388-c3db5d767956.htm",
 40907|       "dll_signature_verified": true,
 40908|       "dll_relationship_scope": "declared",
 40909|       "dll_semantic_verified": null,
 40910|       "dll_verified_status": "signature_verified_declared",
 40911|       "revitlookup_referenced": null,
 40912|       "revitlookup_requires_document_context": null
 40913|     },
 40914|     {
 40915|       "source": "Autodesk.Revit.DB.ElementNode",
 40916|       "target": null,
 40917|       "member_name": "ElementId",
 40918|       "member_kind": "property",
 40919|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 40920|       "confidence": "unknown_reference",
 40921|       "confidence_tier": "unverified_reference",
 40922|       "target_resolution": "none",
 40923|       "evidence": [
 40924|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 40925|       ],
 40926|       "source_url": "https://www.revitapidocs.com/2025/d030f0f5-76f5-2f33-d3fd-14e383de6b7b.htm",
 40927|       "dll_signature_verified": true,
 40928|       "dll_relationship_scope": "declared",
 40929|       "dll_semantic_verified": null,
 40930|       "dll_verified_status": "signature_verified_declared",
 40931|       "revitlookup_referenced": null,
 40932|       "revitlookup_requires_document_context": null
 40933|     },
 40934|     {
 40935|       "source": "Autodesk.Revit.DB.ElementNode",
 40936|       "target": null,
 40937|       "member_name": "LinkInstanceId",
 40938|       "member_kind": "property",
 40939|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 40940|       "confidence": "unknown_reference",
 40941|       "confidence_tier": "unverified_reference",
 40942|       "target_resolution": "none",
 40943|       "evidence": [
 40944|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 40945|       ],
 40946|       "source_url": "https://www.revitapidocs.com/2025/c3b0fdbf-8fd9-0e84-0b29-3868a55c1d81.htm",
 40947|       "dll_signature_verified": true,
 40948|       "dll_relationship_scope": "declared",
 40949|       "dll_semantic_verified": null,
 40950|       "dll_verified_status": "signature_verified_declared",
 40951|       "revitlookup_referenced": null,
 40952|       "revitlookup_requires_document_context": null
 40953|     },
 40954|     {
 40955|       "source": "Autodesk.Revit.DB.ElementOwnerViewFilter",
 40956|       "target": "Autodesk.Revit.DB.View",
 40957|       "member_name": "ViewId",
 40958|       "member_kind": "property",
 40959|       "edge_type": "REFERENCES",
 40960|       "confidence": "elementid_with_strong_name",
```
