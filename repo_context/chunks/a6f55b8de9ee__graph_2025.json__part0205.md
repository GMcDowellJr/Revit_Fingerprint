# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 205 of 216
- Original line range: 79561-79960
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 79561|       "dll_relationship_scope": "declared",
 79562|       "dll_semantic_verified": null,
 79563|       "dll_verified_status": "signature_verified_declared",
 79564|       "revitlookup_referenced": null,
 79565|       "revitlookup_requires_document_context": null
 79566|     },
 79567|     {
 79568|       "source": "Autodesk.Revit.DB.Structure.Rebar",
 79569|       "target": "Autodesk.Revit.DB.Structure.RebarConstraintsManager",
 79570|       "member_name": "GetRebarConstraintsManager",
 79571|       "member_kind": "method",
 79572|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 79573|       "confidence": "direct_return_type",
 79574|       "confidence_tier": "unverified_reference",
 79575|       "target_resolution": "short_name_fallback",
 79576|       "evidence": [
 79577|         "return type 'RebarConstraintsManager' directly names a Revit DB object type"
 79578|       ],
 79579|       "source_url": "https://www.revitapidocs.com/2025/0728a2e5-49db-3a7f-8f83-9db60b92f902.htm",
 79580|       "dll_signature_verified": true,
 79581|       "dll_relationship_scope": "declared",
 79582|       "dll_semantic_verified": null,
 79583|       "dll_verified_status": "signature_verified_declared",
 79584|       "revitlookup_referenced": null,
 79585|       "revitlookup_requires_document_context": null
 79586|     },
 79587|     {
 79588|       "source": "Autodesk.Revit.DB.Structure.Rebar",
 79589|       "target": "Autodesk.Revit.DB.Structure.RebarSplice",
 79590|       "member_name": "GetRebarSplice",
 79591|       "member_kind": "method",
 79592|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 79593|       "confidence": "direct_return_type",
 79594|       "confidence_tier": "unverified_reference",
 79595|       "target_resolution": "short_name_fallback",
 79596|       "evidence": [
 79597|         "return type 'RebarSplice' directly names a Revit DB object type"
 79598|       ],
 79599|       "source_url": "https://www.revitapidocs.com/2025/7ca120f5-8904-29f4-8f25-17ac2489b571.htm",
 79600|       "dll_signature_verified": true,
 79601|       "dll_relationship_scope": "declared",
 79602|       "dll_semantic_verified": null,
 79603|       "dll_verified_status": "signature_verified_declared",
 79604|       "revitlookup_referenced": null,
 79605|       "revitlookup_requires_document_context": null
 79606|     },
 79607|     {
 79608|       "source": "Autodesk.Revit.DB.Structure.Rebar",
 79609|       "target": "Autodesk.Revit.DB.Structure.RebarRoundingManager",
 79610|       "member_name": "GetReinforcementRoundingManager",
 79611|       "member_kind": "method",
 79612|       "edge_type": "REFERENCES",
 79613|       "confidence": "direct_return_type",
 79614|       "confidence_tier": "core",
 79615|       "target_resolution": "short_name_fallback",
 79616|       "evidence": [
 79617|         "return type 'RebarRoundingManager' directly names a Revit DB object type"
 79618|       ],
 79619|       "source_url": "https://www.revitapidocs.com/2025/6d26d7fd-f681-604b-312e-b3395d1e7ca4.htm",
 79620|       "dll_signature_verified": true,
 79621|       "dll_relationship_scope": "declared",
 79622|       "dll_semantic_verified": null,
 79623|       "dll_verified_status": "signature_verified_declared",
 79624|       "revitlookup_referenced": null,
 79625|       "revitlookup_requires_document_context": null
 79626|     },
 79627|     {
 79628|       "source": "Autodesk.Revit.DB.Structure.Rebar",
 79629|       "target": "Autodesk.Revit.DB.Structure.RebarShapeDrivenAccessor",
 79630|       "member_name": "GetShapeDrivenAccessor",
 79631|       "member_kind": "method",
 79632|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 79633|       "confidence": "direct_return_type",
 79634|       "confidence_tier": "unverified_reference",
 79635|       "target_resolution": "short_name_fallback",
 79636|       "evidence": [
 79637|         "return type 'RebarShapeDrivenAccessor' directly names a Revit DB object type"
 79638|       ],
 79639|       "source_url": "https://www.revitapidocs.com/2025/c77085bd-db18-4869-bb2a-1e5c702e273a.htm",
 79640|       "dll_signature_verified": true,
 79641|       "dll_relationship_scope": "declared",
 79642|       "dll_semantic_verified": null,
 79643|       "dll_verified_status": "signature_verified_declared",
 79644|       "revitlookup_referenced": null,
 79645|       "revitlookup_requires_document_context": null
 79646|     },
 79647|     {
 79648|       "source": "Autodesk.Revit.DB.Structure.Rebar",
 79649|       "target": null,
 79650|       "member_name": "GetShapeId",
 79651|       "member_kind": "method",
 79652|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 79653|       "confidence": "unknown_reference",
 79654|       "confidence_tier": "unverified_reference",
 79655|       "target_resolution": "none",
 79656|       "evidence": [
 79657|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 79658|       ],
 79659|       "source_url": "https://www.revitapidocs.com/2025/6edc946f-d8a3-ee78-adbb-7d5359501ed3.htm",
 79660|       "dll_signature_verified": true,
 79661|       "dll_relationship_scope": "declared",
 79662|       "dll_semantic_verified": null,
 79663|       "dll_verified_status": "signature_verified_declared",
 79664|       "revitlookup_referenced": null,
 79665|       "revitlookup_requires_document_context": null
 79666|     },
 79667|     {
 79668|       "source": "Autodesk.Revit.DB.Structure.Rebar",
 79669|       "target": null,
 79670|       "member_name": "GetSpliceStaggerLength",
 79671|       "member_kind": "method",
 79672|       "edge_type": "TAGS_ELEMENT",
 79673|       "confidence": "name_only_candidate",
 79674|       "confidence_tier": "likely",
 79675|       "target_resolution": "none",
 79676|       "evidence": [
 79677|         "member name 'GetSpliceStaggerLength' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'double' gives no type-level confirmation"
 79678|       ],
 79679|       "source_url": "https://www.revitapidocs.com/2025/7b268d5e-1b19-9c4e-ebca-ce1441ea9452.htm",
 79680|       "dll_signature_verified": true,
 79681|       "dll_relationship_scope": "declared",
 79682|       "dll_semantic_verified": null,
 79683|       "dll_verified_status": "signature_verified_declared",
 79684|       "revitlookup_referenced": null,
 79685|       "revitlookup_requires_document_context": null
 79686|     },
 79687|     {
 79688|       "source": "Autodesk.Revit.DB.Structure.Rebar",
 79689|       "target": null,
 79690|       "member_name": "SetHostId",
 79691|       "member_kind": "method",
 79692|       "edge_type": "HOSTED_BY",
 79693|       "confidence": "name_only_candidate",
 79694|       "confidence_tier": "likely",
 79695|       "target_resolution": "none",
 79696|       "evidence": [
 79697|         "member name 'SetHostId' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 79698|       ],
 79699|       "source_url": "https://www.revitapidocs.com/2025/79da5994-50ed-171f-adf2-9a6550c898db.htm",
 79700|       "dll_signature_verified": true,
 79701|       "dll_relationship_scope": "declared",
 79702|       "dll_semantic_verified": null,
 79703|       "dll_verified_status": "signature_verified_declared",
 79704|       "revitlookup_referenced": null,
 79705|       "revitlookup_requires_document_context": null
 79706|     },
 79707|     {
 79708|       "source": "Autodesk.Revit.DB.Structure.RebarBarType",
 79709|       "target": null,
 79710|       "member_name": "GetAutoCalculatedStaggerLength",
 79711|       "member_kind": "method",
 79712|       "edge_type": "TAGS_ELEMENT",
 79713|       "confidence": "name_only_candidate",
 79714|       "confidence_tier": "likely",
 79715|       "target_resolution": "none",
 79716|       "evidence": [
 79717|         "member name 'GetAutoCalculatedStaggerLength' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'bool' gives no type-level confirmation"
 79718|       ],
 79719|       "source_url": "https://www.revitapidocs.com/2025/016b3925-9e16-8268-2a61-01a55af96baf.htm",
 79720|       "dll_signature_verified": true,
 79721|       "dll_relationship_scope": "declared",
 79722|       "dll_semantic_verified": null,
 79723|       "dll_verified_status": "signature_verified_declared",
 79724|       "revitlookup_referenced": null,
 79725|       "revitlookup_requires_document_context": null
 79726|     },
 79727|     {
 79728|       "source": "Autodesk.Revit.DB.Structure.RebarBarType",
 79729|       "target": "Autodesk.Revit.DB.Structure.RebarRoundingManager",
 79730|       "member_name": "GetReinforcementRoundingManager",
 79731|       "member_kind": "method",
 79732|       "edge_type": "REFERENCES",
 79733|       "confidence": "direct_return_type",
 79734|       "confidence_tier": "core",
 79735|       "target_resolution": "short_name_fallback",
 79736|       "evidence": [
 79737|         "return type 'RebarRoundingManager' directly names a Revit DB object type"
 79738|       ],
 79739|       "source_url": "https://www.revitapidocs.com/2025/2f5b170f-bb68-127e-adf7-5d0740e137c1.htm",
 79740|       "dll_signature_verified": true,
 79741|       "dll_relationship_scope": "declared",
 79742|       "dll_semantic_verified": null,
 79743|       "dll_verified_status": "signature_verified_declared",
 79744|       "revitlookup_referenced": null,
 79745|       "revitlookup_requires_document_context": null
 79746|     },
 79747|     {
 79748|       "source": "Autodesk.Revit.DB.Structure.RebarBarType",
 79749|       "target": null,
 79750|       "member_name": "GetStaggerLength",
 79751|       "member_kind": "method",
 79752|       "edge_type": "TAGS_ELEMENT",
 79753|       "confidence": "name_only_candidate",
 79754|       "confidence_tier": "likely",
 79755|       "target_resolution": "none",
 79756|       "evidence": [
 79757|         "member name 'GetStaggerLength' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'double' gives no type-level confirmation"
 79758|       ],
 79759|       "source_url": "https://www.revitapidocs.com/2025/7c69ce6c-2b9b-ed74-6213-9db756a2394d.htm",
 79760|       "dll_signature_verified": true,
 79761|       "dll_relationship_scope": "declared",
 79762|       "dll_semantic_verified": null,
 79763|       "dll_verified_status": "signature_verified_declared",
 79764|       "revitlookup_referenced": null,
 79765|       "revitlookup_requires_document_context": null
 79766|     },
 79767|     {
 79768|       "source": "Autodesk.Revit.DB.Structure.RebarBarType",
 79769|       "target": null,
 79770|       "member_name": "SetAutoCalculatedStaggerLength",
 79771|       "member_kind": "method",
 79772|       "edge_type": "TAGS_ELEMENT",
 79773|       "confidence": "name_only_candidate",
 79774|       "confidence_tier": "likely",
 79775|       "target_resolution": "none",
 79776|       "evidence": [
 79777|         "member name 'SetAutoCalculatedStaggerLength' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'void' gives no type-level confirmation"
 79778|       ],
 79779|       "source_url": "https://www.revitapidocs.com/2025/bb25373e-630f-6637-5820-9b4c4e952d42.htm",
 79780|       "dll_signature_verified": true,
 79781|       "dll_relationship_scope": "declared",
 79782|       "dll_semantic_verified": null,
 79783|       "dll_verified_status": "signature_verified_declared",
 79784|       "revitlookup_referenced": null,
 79785|       "revitlookup_requires_document_context": null
 79786|     },
 79787|     {
 79788|       "source": "Autodesk.Revit.DB.Structure.RebarBarType",
 79789|       "target": null,
 79790|       "member_name": "SetStaggerLength",
 79791|       "member_kind": "method",
 79792|       "edge_type": "TAGS_ELEMENT",
 79793|       "confidence": "name_only_candidate",
 79794|       "confidence_tier": "likely",
 79795|       "target_resolution": "none",
 79796|       "evidence": [
 79797|         "member name 'SetStaggerLength' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'void' gives no type-level confirmation"
 79798|       ],
 79799|       "source_url": "https://www.revitapidocs.com/2025/fdd505c4-fac7-8b03-653f-518eafff70c0.htm",
 79800|       "dll_signature_verified": true,
 79801|       "dll_relationship_scope": "declared",
 79802|       "dll_semantic_verified": null,
 79803|       "dll_verified_status": "signature_verified_declared",
 79804|       "revitlookup_referenced": null,
 79805|       "revitlookup_requires_document_context": null
 79806|     },
 79807|     {
 79808|       "source": "Autodesk.Revit.DB.Structure.RebarBendingDetail",
 79809|       "target": null,
 79810|       "member_name": "AddHosts",
 79811|       "member_kind": "method",
 79812|       "edge_type": "HOSTED_BY",
 79813|       "confidence": "name_only_candidate",
 79814|       "confidence_tier": "likely",
 79815|       "target_resolution": "none",
 79816|       "evidence": [
 79817|         "member name 'AddHosts' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 79818|       ],
 79819|       "source_url": "https://www.revitapidocs.com/2025/4848d235-191d-0962-5245-c296910cb75d.htm",
 79820|       "dll_signature_verified": true,
 79821|       "dll_relationship_scope": "declared",
 79822|       "dll_semantic_verified": null,
 79823|       "dll_verified_status": "signature_verified_declared",
 79824|       "revitlookup_referenced": null,
 79825|       "revitlookup_requires_document_context": null
 79826|     },
 79827|     {
 79828|       "source": "Autodesk.Revit.DB.Structure.RebarBendingDetail",
 79829|       "target": "Autodesk.Revit.DB.Reference",
 79830|       "member_name": "GetHost",
 79831|       "member_kind": "method",
 79832|       "edge_type": "HOSTED_BY",
 79833|       "confidence": "direct_return_type",
 79834|       "confidence_tier": "core",
 79835|       "target_resolution": "exact",
 79836|       "evidence": [
 79837|         "return type 'Reference' directly names a Revit DB object type"
 79838|       ],
 79839|       "source_url": "https://www.revitapidocs.com/2025/cd41fb18-2cb7-83a4-bb6c-9146665425f2.htm",
 79840|       "dll_signature_verified": true,
 79841|       "dll_relationship_scope": "declared",
 79842|       "dll_semantic_verified": null,
 79843|       "dll_verified_status": "signature_verified_declared",
 79844|       "revitlookup_referenced": null,
 79845|       "revitlookup_requires_document_context": null
 79846|     },
 79847|     {
 79848|       "source": "Autodesk.Revit.DB.Structure.RebarBendingDetail",
 79849|       "target": "Autodesk.Revit.DB.Reference",
 79850|       "member_name": "GetHosts",
 79851|       "member_kind": "method",
 79852|       "edge_type": "HOSTED_BY",
 79853|       "confidence": "needs_runtime_validation",
 79854|       "confidence_tier": "needs_validation",
 79855|       "target_resolution": "exact",
 79856|       "evidence": [
 79857|         "return type 'IList < Reference >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 79858|       ],
 79859|       "source_url": "https://www.revitapidocs.com/2025/432a1641-7653-1cff-ad25-a40357c66c2f.htm",
 79860|       "dll_signature_verified": true,
 79861|       "dll_relationship_scope": "declared",
 79862|       "dll_semantic_verified": null,
 79863|       "dll_verified_status": "signature_verified_declared",
 79864|       "revitlookup_referenced": null,
 79865|       "revitlookup_requires_document_context": null
 79866|     },
 79867|     {
 79868|       "source": "Autodesk.Revit.DB.Structure.RebarBendingDetail",
 79869|       "target": null,
 79870|       "member_name": "GetTagRelativeRotation",
 79871|       "member_kind": "method",
 79872|       "edge_type": "TAGS_ELEMENT",
 79873|       "confidence": "name_only_candidate",
 79874|       "confidence_tier": "likely",
 79875|       "target_resolution": "none",
 79876|       "evidence": [
 79877|         "member name 'GetTagRelativeRotation' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'double' gives no type-level confirmation"
 79878|       ],
 79879|       "source_url": "https://www.revitapidocs.com/2025/138ed0ae-c110-000d-6416-2f071d5e3c98.htm",
 79880|       "dll_signature_verified": true,
 79881|       "dll_relationship_scope": "declared",
 79882|       "dll_semantic_verified": null,
 79883|       "dll_verified_status": "signature_verified_declared",
 79884|       "revitlookup_referenced": null,
 79885|       "revitlookup_requires_document_context": null
 79886|     },
 79887|     {
 79888|       "source": "Autodesk.Revit.DB.Structure.RebarBendingDetail",
 79889|       "target": null,
 79890|       "member_name": "RemoveHosts",
 79891|       "member_kind": "method",
 79892|       "edge_type": "HOSTED_BY",
 79893|       "confidence": "name_only_candidate",
 79894|       "confidence_tier": "likely",
 79895|       "target_resolution": "none",
 79896|       "evidence": [
 79897|         "member name 'RemoveHosts' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 79898|       ],
 79899|       "source_url": "https://www.revitapidocs.com/2025/87b27811-b09e-5ad0-3f87-72aad98f3c29.htm",
 79900|       "dll_signature_verified": true,
 79901|       "dll_relationship_scope": "declared",
 79902|       "dll_semantic_verified": null,
 79903|       "dll_verified_status": "signature_verified_declared",
 79904|       "revitlookup_referenced": null,
 79905|       "revitlookup_requires_document_context": null
 79906|     },
 79907|     {
 79908|       "source": "Autodesk.Revit.DB.Structure.RebarBendingDetail",
 79909|       "target": null,
 79910|       "member_name": "ResetTagRelativePosition",
 79911|       "member_kind": "method",
 79912|       "edge_type": "TAGS_ELEMENT",
 79913|       "confidence": "name_only_candidate",
 79914|       "confidence_tier": "likely",
 79915|       "target_resolution": "none",
 79916|       "evidence": [
 79917|         "member name 'ResetTagRelativePosition' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'void' gives no type-level confirmation"
 79918|       ],
 79919|       "source_url": "https://www.revitapidocs.com/2025/7e9b9f7e-2cf1-88f2-dfdc-b8d6bb18dab3.htm",
 79920|       "dll_signature_verified": true,
 79921|       "dll_relationship_scope": "declared",
 79922|       "dll_semantic_verified": null,
 79923|       "dll_verified_status": "signature_verified_declared",
 79924|       "revitlookup_referenced": null,
 79925|       "revitlookup_requires_document_context": null
 79926|     },
 79927|     {
 79928|       "source": "Autodesk.Revit.DB.Structure.RebarBendingDetail",
 79929|       "target": null,
 79930|       "member_name": "SetHost",
 79931|       "member_kind": "method",
 79932|       "edge_type": "HOSTED_BY",
 79933|       "confidence": "name_only_candidate",
 79934|       "confidence_tier": "likely",
 79935|       "target_resolution": "none",
 79936|       "evidence": [
 79937|         "member name 'SetHost' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 79938|       ],
 79939|       "source_url": "https://www.revitapidocs.com/2025/6cad3b9f-43d0-e005-1161-21ba22705d99.htm",
 79940|       "dll_signature_verified": true,
 79941|       "dll_relationship_scope": "declared",
 79942|       "dll_semantic_verified": null,
 79943|       "dll_verified_status": "signature_verified_declared",
 79944|       "revitlookup_referenced": null,
 79945|       "revitlookup_requires_document_context": null
 79946|     },
 79947|     {
 79948|       "source": "Autodesk.Revit.DB.Structure.RebarBendingDetail",
 79949|       "target": null,
 79950|       "member_name": "SetTagRelativePosition",
 79951|       "member_kind": "method",
 79952|       "edge_type": "TAGS_ELEMENT",
 79953|       "confidence": "name_only_candidate",
 79954|       "confidence_tier": "likely",
 79955|       "target_resolution": "none",
 79956|       "evidence": [
 79957|         "member name 'SetTagRelativePosition' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'void' gives no type-level confirmation"
 79958|       ],
 79959|       "source_url": "https://www.revitapidocs.com/2025/519b5d16-6641-bdd8-82b9-cac6df1a4c34.htm",
 79960|       "dll_signature_verified": true,
```
