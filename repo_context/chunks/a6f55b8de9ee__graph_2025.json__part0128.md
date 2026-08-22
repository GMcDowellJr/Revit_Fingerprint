# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 128 of 216
- Original line range: 49531-49930
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 49531|       "target_resolution": "exact",
 49532|       "evidence": [
 49533|         "return type 'ViewSet' directly names a Revit DB object type"
 49534|       ],
 49535|       "source_url": "https://www.revitapidocs.com/2025/48d5707a-ef8b-3609-a573-c393026bc812.htm",
 49536|       "dll_signature_verified": true,
 49537|       "dll_relationship_scope": "declared",
 49538|       "dll_semantic_verified": null,
 49539|       "dll_verified_status": "signature_verified_declared",
 49540|       "revitlookup_referenced": null,
 49541|       "revitlookup_requires_document_context": null
 49542|     },
 49543|     {
 49544|       "source": "Autodesk.Revit.DB.JoinGeometryUtils",
 49545|       "target": null,
 49546|       "member_name": "GetJoinedElements",
 49547|       "member_kind": "method",
 49548|       "edge_type": "RETURNS_ELEMENT_IDS",
 49549|       "confidence": "unknown_reference",
 49550|       "confidence_tier": "unverified_reference",
 49551|       "target_resolution": "none",
 49552|       "evidence": [
 49553|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 49554|       ],
 49555|       "source_url": "https://www.revitapidocs.com/2025/3a1b0e1e-e7f2-cb08-9983-c36137cac754.htm",
 49556|       "dll_signature_verified": true,
 49557|       "dll_relationship_scope": "declared",
 49558|       "dll_semantic_verified": null,
 49559|       "dll_verified_status": "signature_verified_declared",
 49560|       "revitlookup_referenced": null,
 49561|       "revitlookup_requires_document_context": null
 49562|     },
 49563|     {
 49564|       "source": "Autodesk.Revit.DB.KeyBasedTreeEntries",
 49565|       "target": "Autodesk.Revit.DB.KeyBasedTreeEntry",
 49566|       "member_name": "FindEntry",
 49567|       "member_kind": "method",
 49568|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49569|       "confidence": "direct_return_type",
 49570|       "confidence_tier": "unverified_reference",
 49571|       "target_resolution": "exact",
 49572|       "evidence": [
 49573|         "return type 'KeyBasedTreeEntry' directly names a Revit DB object type"
 49574|       ],
 49575|       "source_url": "https://www.revitapidocs.com/2025/1fc6cf20-bc62-3c74-f1bf-49676a30f3cd.htm",
 49576|       "dll_signature_verified": true,
 49577|       "dll_relationship_scope": "declared",
 49578|       "dll_semantic_verified": null,
 49579|       "dll_verified_status": "signature_verified_declared",
 49580|       "revitlookup_referenced": null,
 49581|       "revitlookup_requires_document_context": null
 49582|     },
 49583|     {
 49584|       "source": "Autodesk.Revit.DB.KeyBasedTreeEntries",
 49585|       "target": "Autodesk.Revit.DB.KeyBasedTreeEntriesIterator",
 49586|       "member_name": "GetKeyBasedTreeEntriesIterator",
 49587|       "member_kind": "method",
 49588|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49589|       "confidence": "direct_return_type",
 49590|       "confidence_tier": "unverified_reference",
 49591|       "target_resolution": "exact",
 49592|       "evidence": [
 49593|         "return type 'KeyBasedTreeEntriesIterator' directly names a Revit DB object type"
 49594|       ],
 49595|       "source_url": "https://www.revitapidocs.com/2025/29b5c59d-c99f-e4a4-76b8-9d6b00b1b6cd.htm",
 49596|       "dll_signature_verified": true,
 49597|       "dll_relationship_scope": "declared",
 49598|       "dll_semantic_verified": null,
 49599|       "dll_verified_status": "signature_verified_declared",
 49600|       "revitlookup_referenced": null,
 49601|       "revitlookup_requires_document_context": null
 49602|     },
 49603|     {
 49604|       "source": "Autodesk.Revit.DB.KeyBasedTreeEntriesIterator",
 49605|       "target": "Autodesk.Revit.DB.KeyBasedTreeEntry",
 49606|       "member_name": "Current",
 49607|       "member_kind": "property",
 49608|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49609|       "confidence": "direct_return_type",
 49610|       "confidence_tier": "unverified_reference",
 49611|       "target_resolution": "exact",
 49612|       "evidence": [
 49613|         "return type 'KeyBasedTreeEntry' directly names a Revit DB object type"
 49614|       ],
 49615|       "source_url": "https://www.revitapidocs.com/2025/3bad88bc-139c-a0a9-39b6-4c048ef2313a.htm",
 49616|       "dll_signature_verified": true,
 49617|       "dll_relationship_scope": "declared",
 49618|       "dll_semantic_verified": null,
 49619|       "dll_verified_status": "signature_verified_declared",
 49620|       "revitlookup_referenced": null,
 49621|       "revitlookup_requires_document_context": null
 49622|     },
 49623|     {
 49624|       "source": "Autodesk.Revit.DB.KeyBasedTreeEntriesLoadContent",
 49625|       "target": "Autodesk.Revit.DB.KeyBasedTreeEntries",
 49626|       "member_name": "GetEntries",
 49627|       "member_kind": "method",
 49628|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49629|       "confidence": "direct_return_type",
 49630|       "confidence_tier": "unverified_reference",
 49631|       "target_resolution": "exact",
 49632|       "evidence": [
 49633|         "return type 'KeyBasedTreeEntries' directly names a Revit DB object type"
 49634|       ],
 49635|       "source_url": "https://www.revitapidocs.com/2025/1a58fd7b-e423-87bf-5978-c5ca93ae8949.htm",
 49636|       "dll_signature_verified": true,
 49637|       "dll_relationship_scope": "declared",
 49638|       "dll_semantic_verified": null,
 49639|       "dll_verified_status": "signature_verified_declared",
 49640|       "revitlookup_referenced": null,
 49641|       "revitlookup_requires_document_context": null
 49642|     },
 49643|     {
 49644|       "source": "Autodesk.Revit.DB.KeyBasedTreeEntriesLoadContent",
 49645|       "target": "Autodesk.Revit.DB.KeyBasedTreeEntriesLoadResults",
 49646|       "member_name": "GetLoadResults",
 49647|       "member_kind": "method",
 49648|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49649|       "confidence": "direct_return_type",
 49650|       "confidence_tier": "unverified_reference",
 49651|       "target_resolution": "exact",
 49652|       "evidence": [
 49653|         "return type 'KeyBasedTreeEntriesLoadResults' directly names a Revit DB object type"
 49654|       ],
 49655|       "source_url": "https://www.revitapidocs.com/2025/48d39b70-996b-bfd4-4ed0-925cfb19a918.htm",
 49656|       "dll_signature_verified": true,
 49657|       "dll_relationship_scope": "declared",
 49658|       "dll_semantic_verified": null,
 49659|       "dll_verified_status": "signature_verified_declared",
 49660|       "revitlookup_referenced": null,
 49661|       "revitlookup_requires_document_context": null
 49662|     },
 49663|     {
 49664|       "source": "Autodesk.Revit.DB.KeyBasedTreeEntriesLoadResults",
 49665|       "target": "Autodesk.Revit.DB.KeyBasedTreeEntryError",
 49666|       "member_name": "GetKeyBasedTreeEntryErrors",
 49667|       "member_kind": "method",
 49668|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49669|       "confidence": "needs_runtime_validation",
 49670|       "confidence_tier": "needs_validation",
 49671|       "target_resolution": "exact",
 49672|       "evidence": [
 49673|         "return type 'IList < KeyBasedTreeEntryError >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 49674|       ],
 49675|       "source_url": "https://www.revitapidocs.com/2025/315e4d13-615d-24a2-84b7-b5a47f2170d9.htm",
 49676|       "dll_signature_verified": true,
 49677|       "dll_relationship_scope": "declared",
 49678|       "dll_semantic_verified": null,
 49679|       "dll_verified_status": "signature_verified_declared",
 49680|       "revitlookup_referenced": null,
 49681|       "revitlookup_requires_document_context": null
 49682|     },
 49683|     {
 49684|       "source": "Autodesk.Revit.DB.KeyBasedTreeEntriesLoadResults",
 49685|       "target": "Autodesk.Revit.DB.KeyBasedTreeEntryError",
 49686|       "member_name": "GetKeyBasedTreeEntryErrors",
 49687|       "member_kind": "method",
 49688|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49689|       "confidence": "needs_runtime_validation",
 49690|       "confidence_tier": "needs_validation",
 49691|       "target_resolution": "exact",
 49692|       "evidence": [
 49693|         "return type 'IList < KeyBasedTreeEntryError >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 49694|       ],
 49695|       "source_url": "https://www.revitapidocs.com/2025/0c106d01-5ef7-a7a9-41a0-d54327c727d4.htm",
 49696|       "dll_signature_verified": true,
 49697|       "dll_relationship_scope": "declared",
 49698|       "dll_semantic_verified": null,
 49699|       "dll_verified_status": "signature_verified_declared",
 49700|       "revitlookup_referenced": null,
 49701|       "revitlookup_requires_document_context": null
 49702|     },
 49703|     {
 49704|       "source": "Autodesk.Revit.DB.KeyBasedTreeEntryError",
 49705|       "target": "Autodesk.Revit.DB.KeyBasedTreeEntry",
 49706|       "member_name": "GetEntry",
 49707|       "member_kind": "method",
 49708|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49709|       "confidence": "direct_return_type",
 49710|       "confidence_tier": "unverified_reference",
 49711|       "target_resolution": "exact",
 49712|       "evidence": [
 49713|         "return type 'KeyBasedTreeEntry' directly names a Revit DB object type"
 49714|       ],
 49715|       "source_url": "https://www.revitapidocs.com/2025/7f9bd2d3-5af0-1aad-95ce-f754ff98b84a.htm",
 49716|       "dll_signature_verified": true,
 49717|       "dll_relationship_scope": "declared",
 49718|       "dll_semantic_verified": null,
 49719|       "dll_verified_status": "signature_verified_declared",
 49720|       "revitlookup_referenced": null,
 49721|       "revitlookup_requires_document_context": null
 49722|     },
 49723|     {
 49724|       "source": "Autodesk.Revit.DB.KeyBasedTreeEntryTable",
 49725|       "target": "Autodesk.Revit.DB.KeyBasedTreeEntries",
 49726|       "member_name": "GetKeyBasedTreeEntries",
 49727|       "member_kind": "method",
 49728|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49729|       "confidence": "direct_return_type",
 49730|       "confidence_tier": "unverified_reference",
 49731|       "target_resolution": "exact",
 49732|       "evidence": [
 49733|         "return type 'KeyBasedTreeEntries' directly names a Revit DB object type"
 49734|       ],
 49735|       "source_url": "https://www.revitapidocs.com/2025/f485fa08-b03d-f113-0140-077c78408884.htm",
 49736|       "dll_signature_verified": true,
 49737|       "dll_relationship_scope": "declared",
 49738|       "dll_semantic_verified": null,
 49739|       "dll_verified_status": "signature_verified_declared",
 49740|       "revitlookup_referenced": null,
 49741|       "revitlookup_requires_document_context": null
 49742|     },
 49743|     {
 49744|       "source": "Autodesk.Revit.DB.LabelUtils",
 49745|       "target": null,
 49746|       "member_name": "GetLabelForBuiltInParameter",
 49747|       "member_kind": "method",
 49748|       "edge_type": "HAS_PARAMETER",
 49749|       "confidence": "name_only_candidate",
 49750|       "confidence_tier": "likely",
 49751|       "target_resolution": "none",
 49752|       "evidence": [
 49753|         "member name 'GetLabelForBuiltInParameter' matches keyword pattern /Parameter/ but return type 'string' gives no type-level confirmation"
 49754|       ],
 49755|       "source_url": "https://www.revitapidocs.com/2025/482c49db-8994-bcc8-3077-02d8f40ba3db.htm",
 49756|       "dll_signature_verified": true,
 49757|       "dll_relationship_scope": "declared",
 49758|       "dll_semantic_verified": null,
 49759|       "dll_verified_status": "signature_verified_declared",
 49760|       "revitlookup_referenced": null,
 49761|       "revitlookup_requires_document_context": null
 49762|     },
 49763|     {
 49764|       "source": "Autodesk.Revit.DB.LabelUtils",
 49765|       "target": null,
 49766|       "member_name": "GetLabelForBuiltInParameter",
 49767|       "member_kind": "method",
 49768|       "edge_type": "HAS_PARAMETER",
 49769|       "confidence": "name_only_candidate",
 49770|       "confidence_tier": "likely",
 49771|       "target_resolution": "none",
 49772|       "evidence": [
 49773|         "member name 'GetLabelForBuiltInParameter' matches keyword pattern /Parameter/ but return type 'string' gives no type-level confirmation"
 49774|       ],
 49775|       "source_url": "https://www.revitapidocs.com/2025/c823565b-b71f-cc64-597a-eed82de7106f.htm",
 49776|       "dll_signature_verified": true,
 49777|       "dll_relationship_scope": "declared",
 49778|       "dll_semantic_verified": null,
 49779|       "dll_verified_status": "signature_verified_declared",
 49780|       "revitlookup_referenced": null,
 49781|       "revitlookup_requires_document_context": null
 49782|     },
 49783|     {
 49784|       "source": "Autodesk.Revit.DB.LabelUtils",
 49785|       "target": null,
 49786|       "member_name": "GetLabelForGroup",
 49787|       "member_kind": "method",
 49788|       "edge_type": "MEMBER_OF_GROUP",
 49789|       "confidence": "name_only_candidate",
 49790|       "confidence_tier": "likely",
 49791|       "target_resolution": "none",
 49792|       "evidence": [
 49793|         "member name 'GetLabelForGroup' matches keyword pattern /^GetMember|Group/ but return type 'string' gives no type-level confirmation"
 49794|       ],
 49795|       "source_url": "https://www.revitapidocs.com/2025/fad046bf-b6c9-35cd-69f2-1d556ddbbc05.htm",
 49796|       "dll_signature_verified": true,
 49797|       "dll_relationship_scope": "declared",
 49798|       "dll_semantic_verified": null,
 49799|       "dll_verified_status": "signature_verified_declared",
 49800|       "revitlookup_referenced": null,
 49801|       "revitlookup_requires_document_context": null
 49802|     },
 49803|     {
 49804|       "source": "Autodesk.Revit.DB.LeaderArray",
 49805|       "target": "Autodesk.Revit.DB.LeaderArrayIterator",
 49806|       "member_name": "ForwardIterator",
 49807|       "member_kind": "method",
 49808|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49809|       "confidence": "direct_return_type",
 49810|       "confidence_tier": "unverified_reference",
 49811|       "target_resolution": "exact",
 49812|       "evidence": [
 49813|         "return type 'LeaderArrayIterator' directly names a Revit DB object type"
 49814|       ],
 49815|       "source_url": "https://www.revitapidocs.com/2025/63ab6444-2246-3093-e1bb-4d54638141b4.htm",
 49816|       "dll_signature_verified": true,
 49817|       "dll_relationship_scope": "declared",
 49818|       "dll_semantic_verified": null,
 49819|       "dll_verified_status": "signature_verified_declared",
 49820|       "revitlookup_referenced": null,
 49821|       "revitlookup_requires_document_context": null
 49822|     },
 49823|     {
 49824|       "source": "Autodesk.Revit.DB.LeaderArray",
 49825|       "target": "Autodesk.Revit.DB.LeaderArrayIterator",
 49826|       "member_name": "ReverseIterator",
 49827|       "member_kind": "method",
 49828|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49829|       "confidence": "direct_return_type",
 49830|       "confidence_tier": "unverified_reference",
 49831|       "target_resolution": "exact",
 49832|       "evidence": [
 49833|         "return type 'LeaderArrayIterator' directly names a Revit DB object type"
 49834|       ],
 49835|       "source_url": "https://www.revitapidocs.com/2025/fa3afcf9-f8f7-04d8-3631-de9bbb6b3f0a.htm",
 49836|       "dll_signature_verified": true,
 49837|       "dll_relationship_scope": "declared",
 49838|       "dll_semantic_verified": null,
 49839|       "dll_verified_status": "signature_verified_declared",
 49840|       "revitlookup_referenced": null,
 49841|       "revitlookup_requires_document_context": null
 49842|     },
 49843|     {
 49844|       "source": "Autodesk.Revit.DB.Level",
 49845|       "target": null,
 49846|       "member_name": "FindAssociatedPlanViewId",
 49847|       "member_kind": "method",
 49848|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 49849|       "confidence": "unknown_reference",
 49850|       "confidence_tier": "unverified_reference",
 49851|       "target_resolution": "none",
 49852|       "evidence": [
 49853|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 49854|       ],
 49855|       "source_url": "https://www.revitapidocs.com/2025/ff23277a-3bb1-253d-e158-983b23aaf04a.htm",
 49856|       "dll_signature_verified": true,
 49857|       "dll_relationship_scope": "declared",
 49858|       "dll_semantic_verified": null,
 49859|       "dll_verified_status": "signature_verified_declared",
 49860|       "revitlookup_referenced": null,
 49861|       "revitlookup_requires_document_context": null
 49862|     },
 49863|     {
 49864|       "source": "Autodesk.Revit.DB.Level",
 49865|       "target": "Autodesk.Revit.DB.Level",
 49866|       "member_name": "GetNearestLevelId",
 49867|       "member_kind": "method",
 49868|       "edge_type": "ASSIGNED_TO_LEVEL",
 49869|       "confidence": "elementid_with_strong_name",
 49870|       "confidence_tier": "core",
 49871|       "target_resolution": "exact",
 49872|       "evidence": [
 49873|         "member name 'GetNearestLevelId' matches keyword pattern /Level/"
 49874|       ],
 49875|       "source_url": "https://www.revitapidocs.com/2025/1c70cf7c-bcca-7ebf-99e3-8514e159f089.htm",
 49876|       "dll_signature_verified": true,
 49877|       "dll_relationship_scope": "declared",
 49878|       "dll_semantic_verified": null,
 49879|       "dll_verified_status": "signature_verified_declared",
 49880|       "revitlookup_referenced": null,
 49881|       "revitlookup_requires_document_context": null
 49882|     },
 49883|     {
 49884|       "source": "Autodesk.Revit.DB.Level",
 49885|       "target": "Autodesk.Revit.DB.Level",
 49886|       "member_name": "GetNearestLevelId",
 49887|       "member_kind": "method",
 49888|       "edge_type": "ASSIGNED_TO_LEVEL",
 49889|       "confidence": "elementid_with_strong_name",
 49890|       "confidence_tier": "core",
 49891|       "target_resolution": "exact",
 49892|       "evidence": [
 49893|         "member name 'GetNearestLevelId' matches keyword pattern /Level/"
 49894|       ],
 49895|       "source_url": "https://www.revitapidocs.com/2025/55993839-649f-f73e-ce10-1f37128c3b43.htm",
 49896|       "dll_signature_verified": true,
 49897|       "dll_relationship_scope": "declared",
 49898|       "dll_semantic_verified": null,
 49899|       "dll_verified_status": "signature_verified_declared",
 49900|       "revitlookup_referenced": null,
 49901|       "revitlookup_requires_document_context": null
 49902|     },
 49903|     {
 49904|       "source": "Autodesk.Revit.DB.Level",
 49905|       "target": "Autodesk.Revit.DB.Reference",
 49906|       "member_name": "GetPlaneReference",
 49907|       "member_kind": "method",
 49908|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49909|       "confidence": "direct_return_type",
 49910|       "confidence_tier": "unverified_reference",
 49911|       "target_resolution": "exact",
 49912|       "evidence": [
 49913|         "return type 'Reference' directly names a Revit DB object type"
 49914|       ],
 49915|       "source_url": "https://www.revitapidocs.com/2025/4532a1e6-9a6e-4ba3-e708-b916ec154e4b.htm",
 49916|       "dll_signature_verified": true,
 49917|       "dll_relationship_scope": "declared",
 49918|       "dll_semantic_verified": null,
 49919|       "dll_verified_status": "signature_verified_declared",
 49920|       "revitlookup_referenced": null,
 49921|       "revitlookup_requires_document_context": null
 49922|     },
 49923|     {
 49924|       "source": "Autodesk.Revit.DB.LevelAssociationData",
 49925|       "target": "Autodesk.Revit.DB.Level",
 49926|       "member_name": "GetAssociatedLevel",
 49927|       "member_kind": "method",
 49928|       "edge_type": "ASSIGNED_TO_LEVEL",
 49929|       "confidence": "elementid_with_strong_name",
 49930|       "confidence_tier": "core",
```
