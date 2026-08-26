# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 164 of 216
- Original line range: 63571-63970
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 63571|       "revitlookup_referenced": true,
 63572|       "revitlookup_requires_document_context": false
 63573|     },
 63574|     {
 63575|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63576|       "target": "Autodesk.Revit.DB.TableData",
 63577|       "member_name": "GetTableData",
 63578|       "member_kind": "method",
 63579|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 63580|       "confidence": "direct_return_type",
 63581|       "confidence_tier": "unverified_reference",
 63582|       "target_resolution": "exact",
 63583|       "evidence": [
 63584|         "return type 'TableData' directly names a Revit DB object type"
 63585|       ],
 63586|       "source_url": "https://www.revitapidocs.com/2025/e067f2c7-f809-89d4-5b61-a93004e3710d.htm",
 63587|       "dll_signature_verified": true,
 63588|       "dll_relationship_scope": "declared",
 63589|       "dll_semantic_verified": null,
 63590|       "dll_verified_status": "signature_verified_declared",
 63591|       "revitlookup_referenced": null,
 63592|       "revitlookup_requires_document_context": null
 63593|     },
 63594|     {
 63595|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63596|       "target": null,
 63597|       "member_name": "GetValidCategoriesForKeySchedule",
 63598|       "member_kind": "method",
 63599|       "edge_type": "RETURNS_ELEMENT_IDS",
 63600|       "confidence": "unknown_reference",
 63601|       "confidence_tier": "unverified_reference",
 63602|       "target_resolution": "none",
 63603|       "evidence": [
 63604|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 63605|       ],
 63606|       "source_url": "https://www.revitapidocs.com/2025/84bd09c5-00dc-2238-1bd4-562e22dbbea5.htm",
 63607|       "dll_signature_verified": true,
 63608|       "dll_relationship_scope": "declared",
 63609|       "dll_semantic_verified": null,
 63610|       "dll_verified_status": "signature_verified_declared",
 63611|       "revitlookup_referenced": null,
 63612|       "revitlookup_requires_document_context": null
 63613|     },
 63614|     {
 63615|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63616|       "target": "Autodesk.Revit.DB.Material",
 63617|       "member_name": "GetValidCategoriesForMaterialTakeoff",
 63618|       "member_kind": "method",
 63619|       "edge_type": "USES_MATERIAL",
 63620|       "confidence": "elementid_collection_with_strong_name",
 63621|       "confidence_tier": "core",
 63622|       "target_resolution": "exact",
 63623|       "evidence": [
 63624|         "member name 'GetValidCategoriesForMaterialTakeoff' matches keyword pattern /Material/"
 63625|       ],
 63626|       "source_url": "https://www.revitapidocs.com/2025/87a6e8ff-bf0e-94f3-354f-bb498c335f23.htm",
 63627|       "dll_signature_verified": true,
 63628|       "dll_relationship_scope": "declared",
 63629|       "dll_semantic_verified": null,
 63630|       "dll_verified_status": "signature_verified_declared",
 63631|       "revitlookup_referenced": null,
 63632|       "revitlookup_requires_document_context": null
 63633|     },
 63634|     {
 63635|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63636|       "target": null,
 63637|       "member_name": "GetValidCategoriesForSchedule",
 63638|       "member_kind": "method",
 63639|       "edge_type": "RETURNS_ELEMENT_IDS",
 63640|       "confidence": "unknown_reference",
 63641|       "confidence_tier": "unverified_reference",
 63642|       "target_resolution": "none",
 63643|       "evidence": [
 63644|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 63645|       ],
 63646|       "source_url": "https://www.revitapidocs.com/2025/c84966d1-5dc5-86be-dd22-852aa32f1249.htm",
 63647|       "dll_signature_verified": true,
 63648|       "dll_relationship_scope": "declared",
 63649|       "dll_semantic_verified": null,
 63650|       "dll_verified_status": "signature_verified_declared",
 63651|       "revitlookup_referenced": null,
 63652|       "revitlookup_requires_document_context": null
 63653|     },
 63654|     {
 63655|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63656|       "target": null,
 63657|       "member_name": "GetValidFamiliesForNoteBlock",
 63658|       "member_kind": "method",
 63659|       "edge_type": "RETURNS_ELEMENT_IDS",
 63660|       "confidence": "unknown_reference",
 63661|       "confidence_tier": "unverified_reference",
 63662|       "target_resolution": "none",
 63663|       "evidence": [
 63664|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 63665|       ],
 63666|       "source_url": "https://www.revitapidocs.com/2025/80c6d4cf-d375-5b55-e04a-1a6bd8e43cf5.htm",
 63667|       "dll_signature_verified": true,
 63668|       "dll_relationship_scope": "declared",
 63669|       "dll_semantic_verified": null,
 63670|       "dll_verified_status": "signature_verified_declared",
 63671|       "revitlookup_referenced": true,
 63672|       "revitlookup_requires_document_context": false
 63673|     },
 63674|     {
 63675|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63676|       "target": null,
 63677|       "member_name": "GroupHeaders",
 63678|       "member_kind": "method",
 63679|       "edge_type": "MEMBER_OF_GROUP",
 63680|       "confidence": "name_only_candidate",
 63681|       "confidence_tier": "likely",
 63682|       "target_resolution": "none",
 63683|       "evidence": [
 63684|         "member name 'GroupHeaders' matches keyword pattern /^GetMember|Group/ but return type 'void' gives no type-level confirmation"
 63685|       ],
 63686|       "source_url": "https://www.revitapidocs.com/2025/62a4bc0e-5226-eaed-2881-0bc0a8f259d6.htm",
 63687|       "dll_signature_verified": true,
 63688|       "dll_relationship_scope": "declared",
 63689|       "dll_semantic_verified": null,
 63690|       "dll_verified_status": "signature_verified_declared",
 63691|       "revitlookup_referenced": null,
 63692|       "revitlookup_requires_document_context": null
 63693|     },
 63694|     {
 63695|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63696|       "target": "Autodesk.Revit.DB.Category",
 63697|       "member_name": "IsValidCategoryForKeySchedule",
 63698|       "member_kind": "method",
 63699|       "edge_type": "HAS_CATEGORY",
 63700|       "confidence": "name_only_candidate",
 63701|       "confidence_tier": "likely",
 63702|       "target_resolution": "exact",
 63703|       "evidence": [
 63704|         "member name 'IsValidCategoryForKeySchedule' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 63705|       ],
 63706|       "source_url": "https://www.revitapidocs.com/2025/c74a52c2-0daf-2c3c-0097-f71a9f802dd0.htm",
 63707|       "dll_signature_verified": true,
 63708|       "dll_relationship_scope": "declared",
 63709|       "dll_semantic_verified": null,
 63710|       "dll_verified_status": "signature_verified_declared",
 63711|       "revitlookup_referenced": true,
 63712|       "revitlookup_requires_document_context": true
 63713|     },
 63714|     {
 63715|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63716|       "target": "Autodesk.Revit.DB.Material",
 63717|       "member_name": "IsValidCategoryForMaterialTakeoff",
 63718|       "member_kind": "method",
 63719|       "edge_type": "USES_MATERIAL",
 63720|       "confidence": "name_only_candidate",
 63721|       "confidence_tier": "likely",
 63722|       "target_resolution": "exact",
 63723|       "evidence": [
 63724|         "member name 'IsValidCategoryForMaterialTakeoff' matches keyword pattern /Material/ but return type 'bool' gives no type-level confirmation"
 63725|       ],
 63726|       "source_url": "https://www.revitapidocs.com/2025/e2bb583d-f2e1-30af-4713-9a0edcc3cece.htm",
 63727|       "dll_signature_verified": true,
 63728|       "dll_relationship_scope": "declared",
 63729|       "dll_semantic_verified": null,
 63730|       "dll_verified_status": "signature_verified_declared",
 63731|       "revitlookup_referenced": true,
 63732|       "revitlookup_requires_document_context": true
 63733|     },
 63734|     {
 63735|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63736|       "target": "Autodesk.Revit.DB.Category",
 63737|       "member_name": "IsValidCategoryForSchedule",
 63738|       "member_kind": "method",
 63739|       "edge_type": "HAS_CATEGORY",
 63740|       "confidence": "name_only_candidate",
 63741|       "confidence_tier": "likely",
 63742|       "target_resolution": "exact",
 63743|       "evidence": [
 63744|         "member name 'IsValidCategoryForSchedule' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 63745|       ],
 63746|       "source_url": "https://www.revitapidocs.com/2025/6998c7b9-a44d-9b8e-839e-fb1d23ae26d1.htm",
 63747|       "dll_signature_verified": true,
 63748|       "dll_relationship_scope": "declared",
 63749|       "dll_semantic_verified": null,
 63750|       "dll_verified_status": "signature_verified_declared",
 63751|       "revitlookup_referenced": true,
 63752|       "revitlookup_requires_document_context": true
 63753|     },
 63754|     {
 63755|       "source": "Autodesk.Revit.DB.ViewSchedule",
 63756|       "target": null,
 63757|       "member_name": "UngroupHeaders",
 63758|       "member_kind": "method",
 63759|       "edge_type": "MEMBER_OF_GROUP",
 63760|       "confidence": "name_only_candidate",
 63761|       "confidence_tier": "likely",
 63762|       "target_resolution": "none",
 63763|       "evidence": [
 63764|         "member name 'UngroupHeaders' matches keyword pattern /^GetMember|Group/ but return type 'void' gives no type-level confirmation"
 63765|       ],
 63766|       "source_url": "https://www.revitapidocs.com/2025/682bd641-f652-96b0-9644-210282a56047.htm",
 63767|       "dll_signature_verified": true,
 63768|       "dll_relationship_scope": "declared",
 63769|       "dll_semantic_verified": null,
 63770|       "dll_verified_status": "signature_verified_declared",
 63771|       "revitlookup_referenced": null,
 63772|       "revitlookup_requires_document_context": null
 63773|     },
 63774|     {
 63775|       "source": "Autodesk.Revit.DB.ViewSet",
 63776|       "target": "Autodesk.Revit.DB.ViewSetIterator",
 63777|       "member_name": "ForwardIterator",
 63778|       "member_kind": "method",
 63779|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 63780|       "confidence": "direct_return_type",
 63781|       "confidence_tier": "unverified_reference",
 63782|       "target_resolution": "exact",
 63783|       "evidence": [
 63784|         "return type 'ViewSetIterator' directly names a Revit DB object type"
 63785|       ],
 63786|       "source_url": "https://www.revitapidocs.com/2025/c87bc114-edf4-56f6-5160-48d4f1d001e4.htm",
 63787|       "dll_signature_verified": true,
 63788|       "dll_relationship_scope": "declared",
 63789|       "dll_semantic_verified": null,
 63790|       "dll_verified_status": "signature_verified_declared",
 63791|       "revitlookup_referenced": null,
 63792|       "revitlookup_requires_document_context": null
 63793|     },
 63794|     {
 63795|       "source": "Autodesk.Revit.DB.ViewSet",
 63796|       "target": "Autodesk.Revit.DB.ViewSetIterator",
 63797|       "member_name": "ReverseIterator",
 63798|       "member_kind": "method",
 63799|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 63800|       "confidence": "direct_return_type",
 63801|       "confidence_tier": "unverified_reference",
 63802|       "target_resolution": "exact",
 63803|       "evidence": [
 63804|         "return type 'ViewSetIterator' directly names a Revit DB object type"
 63805|       ],
 63806|       "source_url": "https://www.revitapidocs.com/2025/12a69c96-7ae7-8112-3eb0-f81b68aca3d4.htm",
 63807|       "dll_signature_verified": true,
 63808|       "dll_relationship_scope": "declared",
 63809|       "dll_semantic_verified": null,
 63810|       "dll_verified_status": "signature_verified_declared",
 63811|       "revitlookup_referenced": null,
 63812|       "revitlookup_requires_document_context": null
 63813|     },
 63814|     {
 63815|       "source": "Autodesk.Revit.DB.ViewSheet",
 63816|       "target": "Autodesk.Revit.DB.ViewSheet",
 63817|       "member_name": "SheetCollectionId",
 63818|       "member_kind": "property",
 63819|       "edge_type": "PLACED_ON_SHEET",
 63820|       "confidence": "elementid_with_strong_name",
 63821|       "confidence_tier": "core",
 63822|       "target_resolution": "exact",
 63823|       "evidence": [
 63824|         "member name 'SheetCollectionId' matches keyword pattern /Sheet/"
 63825|       ],
 63826|       "source_url": "https://www.revitapidocs.com/2025/81b57b4c-6059-969b-fbe1-201861d1f750.htm",
 63827|       "dll_signature_verified": true,
 63828|       "dll_relationship_scope": "declared",
 63829|       "dll_semantic_verified": null,
 63830|       "dll_verified_status": "signature_verified_declared",
 63831|       "revitlookup_referenced": null,
 63832|       "revitlookup_requires_document_context": null
 63833|     },
 63834|     {
 63835|       "source": "Autodesk.Revit.DB.ViewSheet",
 63836|       "target": "Autodesk.Revit.DB.ViewSheet",
 63837|       "member_name": "SheetNumber",
 63838|       "member_kind": "property",
 63839|       "edge_type": "PLACED_ON_SHEET",
 63840|       "confidence": "name_only_candidate",
 63841|       "confidence_tier": "likely",
 63842|       "target_resolution": "exact",
 63843|       "evidence": [
 63844|         "member name 'SheetNumber' matches keyword pattern /Sheet/ but return type 'string' gives no type-level confirmation"
 63845|       ],
 63846|       "source_url": "https://www.revitapidocs.com/2025/5f9129ba-b323-c55d-e27e-46d88bec503b.htm",
 63847|       "dll_signature_verified": true,
 63848|       "dll_relationship_scope": "declared",
 63849|       "dll_semantic_verified": null,
 63850|       "dll_verified_status": "signature_verified_declared",
 63851|       "revitlookup_referenced": null,
 63852|       "revitlookup_requires_document_context": null
 63853|     },
 63854|     {
 63855|       "source": "Autodesk.Revit.DB.ViewSheet",
 63856|       "target": "Autodesk.Revit.DB.ViewSheet",
 63857|       "member_name": "ConvertToRealSheet",
 63858|       "member_kind": "method",
 63859|       "edge_type": "PLACED_ON_SHEET",
 63860|       "confidence": "name_only_candidate",
 63861|       "confidence_tier": "likely",
 63862|       "target_resolution": "exact",
 63863|       "evidence": [
 63864|         "member name 'ConvertToRealSheet' matches keyword pattern /Sheet/ but return type 'void' gives no type-level confirmation"
 63865|       ],
 63866|       "source_url": "https://www.revitapidocs.com/2025/cfd7f789-41cf-1e86-a757-fe4ff5d8ba89.htm",
 63867|       "dll_signature_verified": true,
 63868|       "dll_relationship_scope": "declared",
 63869|       "dll_semantic_verified": null,
 63870|       "dll_verified_status": "signature_verified_declared",
 63871|       "revitlookup_referenced": null,
 63872|       "revitlookup_requires_document_context": null
 63873|     },
 63874|     {
 63875|       "source": "Autodesk.Revit.DB.ViewSheet",
 63876|       "target": null,
 63877|       "member_name": "Duplicate",
 63878|       "member_kind": "method",
 63879|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 63880|       "confidence": "unknown_reference",
 63881|       "confidence_tier": "unverified_reference",
 63882|       "target_resolution": "none",
 63883|       "evidence": [
 63884|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 63885|       ],
 63886|       "source_url": "https://www.revitapidocs.com/2025/2466b896-0ce9-2d6e-e136-202d77ed7b79.htm",
 63887|       "dll_signature_verified": true,
 63888|       "dll_relationship_scope": "declared",
 63889|       "dll_semantic_verified": null,
 63890|       "dll_verified_status": "signature_verified_declared",
 63891|       "revitlookup_referenced": null,
 63892|       "revitlookup_requires_document_context": null
 63893|     },
 63894|     {
 63895|       "source": "Autodesk.Revit.DB.ViewSheet",
 63896|       "target": null,
 63897|       "member_name": "GetAdditionalRevisionIds",
 63898|       "member_kind": "method",
 63899|       "edge_type": "RETURNS_ELEMENT_IDS",
 63900|       "confidence": "unknown_reference",
 63901|       "confidence_tier": "unverified_reference",
 63902|       "target_resolution": "none",
 63903|       "evidence": [
 63904|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 63905|       ],
 63906|       "source_url": "https://www.revitapidocs.com/2025/6d852f22-cf1b-3bcb-c255-184998d1334c.htm",
 63907|       "dll_signature_verified": true,
 63908|       "dll_relationship_scope": "declared",
 63909|       "dll_semantic_verified": null,
 63910|       "dll_verified_status": "signature_verified_declared",
 63911|       "revitlookup_referenced": null,
 63912|       "revitlookup_requires_document_context": null
 63913|     },
 63914|     {
 63915|       "source": "Autodesk.Revit.DB.ViewSheet",
 63916|       "target": null,
 63917|       "member_name": "GetAllPlacedViews",
 63918|       "member_kind": "method",
 63919|       "edge_type": "RETURNS_ELEMENT_IDS",
 63920|       "confidence": "elementid_collection_with_strong_name",
 63921|       "confidence_tier": "core",
 63922|       "target_resolution": "none",
 63923|       "evidence": [
 63924|         "member name 'GetAllPlacedViews' matches keyword pattern /^GetAll/"
 63925|       ],
 63926|       "source_url": "https://www.revitapidocs.com/2025/816db942-4e9c-7278-7f59-53048becc46a.htm",
 63927|       "dll_signature_verified": true,
 63928|       "dll_relationship_scope": "declared",
 63929|       "dll_semantic_verified": null,
 63930|       "dll_verified_status": "signature_verified_declared",
 63931|       "revitlookup_referenced": null,
 63932|       "revitlookup_requires_document_context": null
 63933|     },
 63934|     {
 63935|       "source": "Autodesk.Revit.DB.ViewSheet",
 63936|       "target": null,
 63937|       "member_name": "GetAllRevisionCloudIds",
 63938|       "member_kind": "method",
 63939|       "edge_type": "RETURNS_ELEMENT_IDS",
 63940|       "confidence": "elementid_collection_with_strong_name",
 63941|       "confidence_tier": "core",
 63942|       "target_resolution": "none",
 63943|       "evidence": [
 63944|         "member name 'GetAllRevisionCloudIds' matches keyword pattern /^GetAll/"
 63945|       ],
 63946|       "source_url": "https://www.revitapidocs.com/2025/dd1487e6-dffa-5c9c-8fcc-ff8f664b494e.htm",
 63947|       "dll_signature_verified": true,
 63948|       "dll_relationship_scope": "declared",
 63949|       "dll_semantic_verified": null,
 63950|       "dll_verified_status": "signature_verified_declared",
 63951|       "revitlookup_referenced": null,
 63952|       "revitlookup_requires_document_context": null
 63953|     },
 63954|     {
 63955|       "source": "Autodesk.Revit.DB.ViewSheet",
 63956|       "target": null,
 63957|       "member_name": "GetAllRevisionIds",
 63958|       "member_kind": "method",
 63959|       "edge_type": "RETURNS_ELEMENT_IDS",
 63960|       "confidence": "elementid_collection_with_strong_name",
 63961|       "confidence_tier": "core",
 63962|       "target_resolution": "none",
 63963|       "evidence": [
 63964|         "member name 'GetAllRevisionIds' matches keyword pattern /^GetAll/"
 63965|       ],
 63966|       "source_url": "https://www.revitapidocs.com/2025/e6f4e79f-c076-8085-5288-6e0b5a431177.htm",
 63967|       "dll_signature_verified": true,
 63968|       "dll_relationship_scope": "declared",
 63969|       "dll_semantic_verified": null,
 63970|       "dll_verified_status": "signature_verified_declared",
```
