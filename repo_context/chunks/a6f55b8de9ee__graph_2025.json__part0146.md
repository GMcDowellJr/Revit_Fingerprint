# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 146 of 216
- Original line range: 56551-56950
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 56551|       "member_name": "SetPhaseFilter",
 56552|       "member_kind": "method",
 56553|       "edge_type": "ASSIGNED_TO_PHASE",
 56554|       "confidence": "name_only_candidate",
 56555|       "confidence_tier": "likely",
 56556|       "target_resolution": "exact",
 56557|       "evidence": [
 56558|         "member name 'SetPhaseFilter' matches keyword pattern /Phase/ but return type 'void' gives no type-level confirmation"
 56559|       ],
 56560|       "source_url": "https://www.revitapidocs.com/2025/505099f1-4435-01c1-7ce7-ee59edf50c9a.htm",
 56561|       "dll_signature_verified": true,
 56562|       "dll_relationship_scope": "declared",
 56563|       "dll_semantic_verified": null,
 56564|       "dll_verified_status": "signature_verified_declared",
 56565|       "revitlookup_referenced": null,
 56566|       "revitlookup_requires_document_context": null
 56567|     },
 56568|     {
 56569|       "source": "Autodesk.Revit.DB.RevitLinkGraphicsSettings",
 56570|       "target": "Autodesk.Revit.DB.Level",
 56571|       "member_name": "SetViewDetailLevel",
 56572|       "member_kind": "method",
 56573|       "edge_type": "ASSIGNED_TO_LEVEL",
 56574|       "confidence": "name_only_candidate",
 56575|       "confidence_tier": "likely",
 56576|       "target_resolution": "exact",
 56577|       "evidence": [
 56578|         "member name 'SetViewDetailLevel' matches keyword pattern /Level/ but return type 'void' gives no type-level confirmation"
 56579|       ],
 56580|       "source_url": "https://www.revitapidocs.com/2025/3f74c801-76f0-1c16-0fc2-dcf544aeb76a.htm",
 56581|       "dll_signature_verified": true,
 56582|       "dll_relationship_scope": "declared",
 56583|       "dll_semantic_verified": null,
 56584|       "dll_verified_status": "signature_verified_declared",
 56585|       "revitlookup_referenced": null,
 56586|       "revitlookup_requires_document_context": null
 56587|     },
 56588|     {
 56589|       "source": "Autodesk.Revit.DB.RevitLinkInstance",
 56590|       "target": "Autodesk.Revit.DB.Document",
 56591|       "member_name": "GetLinkDocument",
 56592|       "member_kind": "method",
 56593|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 56594|       "confidence": "direct_return_type",
 56595|       "confidence_tier": "unverified_reference",
 56596|       "target_resolution": "exact",
 56597|       "evidence": [
 56598|         "return type 'Document' directly names a Revit DB object type"
 56599|       ],
 56600|       "source_url": "https://www.revitapidocs.com/2025/6f81c365-15d9-06b8-48ef-df84914fec60.htm",
 56601|       "dll_signature_verified": true,
 56602|       "dll_relationship_scope": "declared",
 56603|       "dll_semantic_verified": null,
 56604|       "dll_verified_status": "signature_verified_declared",
 56605|       "revitlookup_referenced": null,
 56606|       "revitlookup_requires_document_context": null
 56607|     },
 56608|     {
 56609|       "source": "Autodesk.Revit.DB.RevitLinkInstance",
 56610|       "target": null,
 56611|       "member_name": "MoveBasePointToHostBasePoint",
 56612|       "member_kind": "method",
 56613|       "edge_type": "HOSTED_BY",
 56614|       "confidence": "name_only_candidate",
 56615|       "confidence_tier": "likely",
 56616|       "target_resolution": "none",
 56617|       "evidence": [
 56618|         "member name 'MoveBasePointToHostBasePoint' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 56619|       ],
 56620|       "source_url": "https://www.revitapidocs.com/2025/052feb8e-e569-ddcd-30b8-a2373d1466f8.htm",
 56621|       "dll_signature_verified": true,
 56622|       "dll_relationship_scope": "declared",
 56623|       "dll_semantic_verified": null,
 56624|       "dll_verified_status": "signature_verified_declared",
 56625|       "revitlookup_referenced": null,
 56626|       "revitlookup_requires_document_context": null
 56627|     },
 56628|     {
 56629|       "source": "Autodesk.Revit.DB.RevitLinkInstance",
 56630|       "target": null,
 56631|       "member_name": "MoveOriginToHostOrigin",
 56632|       "member_kind": "method",
 56633|       "edge_type": "HOSTED_BY",
 56634|       "confidence": "name_only_candidate",
 56635|       "confidence_tier": "likely",
 56636|       "target_resolution": "none",
 56637|       "evidence": [
 56638|         "member name 'MoveOriginToHostOrigin' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 56639|       ],
 56640|       "source_url": "https://www.revitapidocs.com/2025/2ebcfd4d-c7a4-7694-a06b-372c3675cec9.htm",
 56641|       "dll_signature_verified": true,
 56642|       "dll_relationship_scope": "declared",
 56643|       "dll_semantic_verified": null,
 56644|       "dll_verified_status": "signature_verified_declared",
 56645|       "revitlookup_referenced": null,
 56646|       "revitlookup_requires_document_context": null
 56647|     },
 56648|     {
 56649|       "source": "Autodesk.Revit.DB.RevitLinkOptions",
 56650|       "target": "Autodesk.Revit.DB.WorksetConfiguration",
 56651|       "member_name": "GetWorksetConfiguration",
 56652|       "member_kind": "method",
 56653|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 56654|       "confidence": "direct_return_type",
 56655|       "confidence_tier": "unverified_reference",
 56656|       "target_resolution": "exact",
 56657|       "evidence": [
 56658|         "member name 'GetWorksetConfiguration' matches keyword pattern /Workset/ implying target 'Workset', but the actual return type 'WorksetConfiguration' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 56659|         "return type 'WorksetConfiguration' directly names a Revit DB object type"
 56660|       ],
 56661|       "source_url": "https://www.revitapidocs.com/2025/a091318c-df88-1ebf-c442-e15e869b0ce4.htm",
 56662|       "dll_signature_verified": true,
 56663|       "dll_relationship_scope": "declared",
 56664|       "dll_semantic_verified": null,
 56665|       "dll_verified_status": "signature_verified_declared",
 56666|       "revitlookup_referenced": null,
 56667|       "revitlookup_requires_document_context": null
 56668|     },
 56669|     {
 56670|       "source": "Autodesk.Revit.DB.RevitLinkOptions",
 56671|       "target": "Autodesk.Revit.DB.Workset",
 56672|       "member_name": "SetWorksetConfiguration",
 56673|       "member_kind": "method",
 56674|       "edge_type": "OWNED_BY_WORKSET",
 56675|       "confidence": "name_only_candidate",
 56676|       "confidence_tier": "likely",
 56677|       "target_resolution": "exact",
 56678|       "evidence": [
 56679|         "member name 'SetWorksetConfiguration' matches keyword pattern /Workset/ but return type 'void' gives no type-level confirmation"
 56680|       ],
 56681|       "source_url": "https://www.revitapidocs.com/2025/8e0fe0c5-3dd9-806b-6e0d-d42f8d498be2.htm",
 56682|       "dll_signature_verified": true,
 56683|       "dll_relationship_scope": "declared",
 56684|       "dll_semantic_verified": null,
 56685|       "dll_verified_status": "signature_verified_declared",
 56686|       "revitlookup_referenced": null,
 56687|       "revitlookup_requires_document_context": null
 56688|     },
 56689|     {
 56690|       "source": "Autodesk.Revit.DB.RevitLinkType",
 56691|       "target": null,
 56692|       "member_name": "GetChildIds",
 56693|       "member_kind": "method",
 56694|       "edge_type": "RETURNS_ELEMENT_IDS",
 56695|       "confidence": "unknown_reference",
 56696|       "confidence_tier": "unverified_reference",
 56697|       "target_resolution": "none",
 56698|       "evidence": [
 56699|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 56700|       ],
 56701|       "source_url": "https://www.revitapidocs.com/2025/8336bb72-f686-c742-dbbe-b6245e6b33b4.htm",
 56702|       "dll_signature_verified": true,
 56703|       "dll_relationship_scope": "declared",
 56704|       "dll_semantic_verified": null,
 56705|       "dll_verified_status": "signature_verified_declared",
 56706|       "revitlookup_referenced": null,
 56707|       "revitlookup_requires_document_context": null
 56708|     },
 56709|     {
 56710|       "source": "Autodesk.Revit.DB.RevitLinkType",
 56711|       "target": "Autodesk.Revit.DB.LinkConversionData",
 56712|       "member_name": "GetConversionData",
 56713|       "member_kind": "method",
 56714|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 56715|       "confidence": "direct_return_type",
 56716|       "confidence_tier": "unverified_reference",
 56717|       "target_resolution": "exact",
 56718|       "evidence": [
 56719|         "return type 'LinkConversionData' directly names a Revit DB object type"
 56720|       ],
 56721|       "source_url": "https://www.revitapidocs.com/2025/9809ada4-fced-197c-7e4c-f339e75a2b80.htm",
 56722|       "dll_signature_verified": true,
 56723|       "dll_relationship_scope": "declared",
 56724|       "dll_semantic_verified": null,
 56725|       "dll_verified_status": "signature_verified_declared",
 56726|       "revitlookup_referenced": null,
 56727|       "revitlookup_requires_document_context": null
 56728|     },
 56729|     {
 56730|       "source": "Autodesk.Revit.DB.RevitLinkType",
 56731|       "target": null,
 56732|       "member_name": "GetParentId",
 56733|       "member_kind": "method",
 56734|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 56735|       "confidence": "unknown_reference",
 56736|       "confidence_tier": "unverified_reference",
 56737|       "target_resolution": "none",
 56738|       "evidence": [
 56739|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 56740|       ],
 56741|       "source_url": "https://www.revitapidocs.com/2025/937f7497-a73c-f97c-dc49-1da47d098200.htm",
 56742|       "dll_signature_verified": true,
 56743|       "dll_relationship_scope": "declared",
 56744|       "dll_semantic_verified": null,
 56745|       "dll_verified_status": "signature_verified_declared",
 56746|       "revitlookup_referenced": null,
 56747|       "revitlookup_requires_document_context": null
 56748|     },
 56749|     {
 56750|       "source": "Autodesk.Revit.DB.RevitLinkType",
 56751|       "target": "Autodesk.Revit.DB.Phase",
 56752|       "member_name": "GetPhaseMap",
 56753|       "member_kind": "method",
 56754|       "edge_type": "ASSIGNED_TO_PHASE",
 56755|       "confidence": "name_only_candidate",
 56756|       "confidence_tier": "likely",
 56757|       "target_resolution": "exact",
 56758|       "evidence": [
 56759|         "member name 'GetPhaseMap' matches keyword pattern /Phase/ but return type 'IDictionary < ElementId , ElementId >' gives no type-level confirmation"
 56760|       ],
 56761|       "source_url": "https://www.revitapidocs.com/2025/4d322c11-c2b3-a096-4f10-3b23c9112308.htm",
 56762|       "dll_signature_verified": true,
 56763|       "dll_relationship_scope": "declared",
 56764|       "dll_semantic_verified": null,
 56765|       "dll_verified_status": "signature_verified_declared",
 56766|       "revitlookup_referenced": null,
 56767|       "revitlookup_requires_document_context": null
 56768|     },
 56769|     {
 56770|       "source": "Autodesk.Revit.DB.RevitLinkType",
 56771|       "target": null,
 56772|       "member_name": "GetRootId",
 56773|       "member_kind": "method",
 56774|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 56775|       "confidence": "unknown_reference",
 56776|       "confidence_tier": "unverified_reference",
 56777|       "target_resolution": "none",
 56778|       "evidence": [
 56779|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 56780|       ],
 56781|       "source_url": "https://www.revitapidocs.com/2025/f50ff34a-11db-04ae-8085-636be561031d.htm",
 56782|       "dll_signature_verified": true,
 56783|       "dll_relationship_scope": "declared",
 56784|       "dll_semantic_verified": null,
 56785|       "dll_verified_status": "signature_verified_declared",
 56786|       "revitlookup_referenced": null,
 56787|       "revitlookup_requires_document_context": null
 56788|     },
 56789|     {
 56790|       "source": "Autodesk.Revit.DB.RevitLinkType",
 56791|       "target": "Autodesk.Revit.DB.Level",
 56792|       "member_name": "GetTopLevelLink",
 56793|       "member_kind": "method",
 56794|       "edge_type": "ASSIGNED_TO_LEVEL",
 56795|       "confidence": "elementid_with_strong_name",
 56796|       "confidence_tier": "core",
 56797|       "target_resolution": "exact",
 56798|       "evidence": [
 56799|         "member name 'GetTopLevelLink' matches keyword pattern /Level/"
 56800|       ],
 56801|       "source_url": "https://www.revitapidocs.com/2025/9a32ed88-9ac4-68eb-a129-69cf29aa496d.htm",
 56802|       "dll_signature_verified": true,
 56803|       "dll_relationship_scope": "declared",
 56804|       "dll_semantic_verified": null,
 56805|       "dll_verified_status": "signature_verified_declared",
 56806|       "revitlookup_referenced": null,
 56807|       "revitlookup_requires_document_context": null
 56808|     },
 56809|     {
 56810|       "source": "Autodesk.Revit.DB.RevitLinkType",
 56811|       "target": "Autodesk.Revit.DB.Level",
 56812|       "member_name": "GetTopLevelLink",
 56813|       "member_kind": "method",
 56814|       "edge_type": "ASSIGNED_TO_LEVEL",
 56815|       "confidence": "elementid_with_strong_name",
 56816|       "confidence_tier": "core",
 56817|       "target_resolution": "exact",
 56818|       "evidence": [
 56819|         "member name 'GetTopLevelLink' matches keyword pattern /Level/"
 56820|       ],
 56821|       "source_url": "https://www.revitapidocs.com/2025/7a44c5d9-4cad-1f6b-f78e-b5fef077aa8c.htm",
 56822|       "dll_signature_verified": true,
 56823|       "dll_relationship_scope": "declared",
 56824|       "dll_semantic_verified": null,
 56825|       "dll_verified_status": "signature_verified_declared",
 56826|       "revitlookup_referenced": null,
 56827|       "revitlookup_requires_document_context": null
 56828|     },
 56829|     {
 56830|       "source": "Autodesk.Revit.DB.Revolution",
 56831|       "target": "Autodesk.Revit.DB.ModelLine",
 56832|       "member_name": "Axis",
 56833|       "member_kind": "property",
 56834|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 56835|       "confidence": "direct_return_type",
 56836|       "confidence_tier": "unverified_reference",
 56837|       "target_resolution": "exact",
 56838|       "evidence": [
 56839|         "return type 'ModelLine' directly names a Revit DB object type"
 56840|       ],
 56841|       "source_url": "https://www.revitapidocs.com/2025/030482e1-5974-6cf0-56af-ff43b83c30fd.htm",
 56842|       "dll_signature_verified": true,
 56843|       "dll_relationship_scope": "declared",
 56844|       "dll_semantic_verified": null,
 56845|       "dll_verified_status": "signature_verified_declared",
 56846|       "revitlookup_referenced": null,
 56847|       "revitlookup_requires_document_context": null
 56848|     },
 56849|     {
 56850|       "source": "Autodesk.Revit.DB.Revolution",
 56851|       "target": "Autodesk.Revit.DB.Sketch",
 56852|       "member_name": "Sketch",
 56853|       "member_kind": "property",
 56854|       "edge_type": "DEPENDS_ON",
 56855|       "confidence": "direct_return_type",
 56856|       "confidence_tier": "core",
 56857|       "target_resolution": "exact",
 56858|       "evidence": [
 56859|         "return type 'Sketch' directly names a Revit DB object type"
 56860|       ],
 56861|       "source_url": "https://www.revitapidocs.com/2025/38264451-ef82-f142-436c-6cd3c3df34f6.htm",
 56862|       "dll_signature_verified": true,
 56863|       "dll_relationship_scope": "declared",
 56864|       "dll_semantic_verified": null,
 56865|       "dll_verified_status": "signature_verified_declared",
 56866|       "revitlookup_referenced": null,
 56867|       "revitlookup_requires_document_context": null
 56868|     },
 56869|     {
 56870|       "source": "Autodesk.Revit.DB.RoofBase",
 56871|       "target": "Autodesk.Revit.DB.RoofType",
 56872|       "member_name": "RoofType",
 56873|       "member_kind": "property",
 56874|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 56875|       "confidence": "direct_return_type",
 56876|       "confidence_tier": "unverified_reference",
 56877|       "target_resolution": "exact",
 56878|       "evidence": [
 56879|         "return type 'RoofType' directly names a Revit DB object type"
 56880|       ],
 56881|       "source_url": "https://www.revitapidocs.com/2025/8315630e-d579-70c1-3f64-37b8bbeaf0a0.htm",
 56882|       "dll_signature_verified": true,
 56883|       "dll_relationship_scope": "declared",
 56884|       "dll_semantic_verified": null,
 56885|       "dll_verified_status": "signature_verified_declared",
 56886|       "revitlookup_referenced": null,
 56887|       "revitlookup_requires_document_context": null
 56888|     },
 56889|     {
 56890|       "source": "Autodesk.Revit.DB.RoofBase",
 56891|       "target": "Autodesk.Revit.DB.SlabShapeEditor",
 56892|       "member_name": "GetSlabShapeEditor",
 56893|       "member_kind": "method",
 56894|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 56895|       "confidence": "direct_return_type",
 56896|       "confidence_tier": "unverified_reference",
 56897|       "target_resolution": "exact",
 56898|       "evidence": [
 56899|         "return type 'SlabShapeEditor' directly names a Revit DB object type"
 56900|       ],
 56901|       "source_url": "https://www.revitapidocs.com/2025/54ffcdb1-aadf-4674-16e7-2f6d1ce215a5.htm",
 56902|       "dll_signature_verified": true,
 56903|       "dll_relationship_scope": "declared",
 56904|       "dll_semantic_verified": null,
 56905|       "dll_verified_status": "signature_verified_declared",
 56906|       "revitlookup_referenced": null,
 56907|       "revitlookup_requires_document_context": null
 56908|     },
 56909|     {
 56910|       "source": "Autodesk.Revit.DB.RoofType",
 56911|       "target": "Autodesk.Revit.DB.ThermalProperties",
 56912|       "member_name": "ThermalProperties",
 56913|       "member_kind": "property",
 56914|       "edge_type": "REFERENCES",
 56915|       "confidence": "direct_return_type",
 56916|       "confidence_tier": "core",
 56917|       "target_resolution": "exact",
 56918|       "evidence": [
 56919|         "return type 'ThermalProperties' directly names a Revit DB object type"
 56920|       ],
 56921|       "source_url": "https://www.revitapidocs.com/2025/ab697fdb-c00a-5193-5fd2-a74361363b5f.htm",
 56922|       "dll_signature_verified": true,
 56923|       "dll_relationship_scope": "declared",
 56924|       "dll_semantic_verified": null,
 56925|       "dll_verified_status": "signature_verified_declared",
 56926|       "revitlookup_referenced": null,
 56927|       "revitlookup_requires_document_context": null
 56928|     },
 56929|     {
 56930|       "source": "Autodesk.Revit.DB.RoutingConditions",
 56931|       "target": "Autodesk.Revit.DB.Level",
 56932|       "member_name": "ErrorLevel",
 56933|       "member_kind": "property",
 56934|       "edge_type": "ASSIGNED_TO_LEVEL",
 56935|       "confidence": "name_only_candidate",
 56936|       "confidence_tier": "likely",
 56937|       "target_resolution": "exact",
 56938|       "evidence": [
 56939|         "member name 'ErrorLevel' matches keyword pattern /Level/ but return type 'RoutingPreferenceErrorLevel' gives no type-level confirmation"
 56940|       ],
 56941|       "source_url": "https://www.revitapidocs.com/2025/cc96a880-9f3b-08cf-7a31-e8301a817035.htm",
 56942|       "dll_signature_verified": true,
 56943|       "dll_relationship_scope": "declared",
 56944|       "dll_semantic_verified": null,
 56945|       "dll_verified_status": "signature_verified_declared",
 56946|       "revitlookup_referenced": null,
 56947|       "revitlookup_requires_document_context": null
 56948|     },
 56949|     {
 56950|       "source": "Autodesk.Revit.DB.RoutingConditions",
```
