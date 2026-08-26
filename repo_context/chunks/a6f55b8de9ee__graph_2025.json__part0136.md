# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 136 of 216
- Original line range: 52651-53050
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 52651|       "member_kind": "property",
 52652|       "edge_type": "ASSIGNED_TO_LEVEL",
 52653|       "confidence": "name_only_candidate",
 52654|       "confidence_tier": "likely",
 52655|       "target_resolution": "exact",
 52656|       "evidence": [
 52657|         "member name 'DetailLevel' matches keyword pattern /Level/ but return type 'ViewDetailLevel' gives no type-level confirmation"
 52658|       ],
 52659|       "source_url": "https://www.revitapidocs.com/2025/887c4c25-fe14-2633-b84c-09d2f1279c9e.htm",
 52660|       "dll_signature_verified": true,
 52661|       "dll_relationship_scope": "declared",
 52662|       "dll_semantic_verified": null,
 52663|       "dll_verified_status": "signature_verified_declared",
 52664|       "revitlookup_referenced": null,
 52665|       "revitlookup_requires_document_context": null
 52666|     },
 52667|     {
 52668|       "source": "Autodesk.Revit.DB.Options",
 52669|       "target": "Autodesk.Revit.DB.View",
 52670|       "member_name": "View",
 52671|       "member_kind": "property",
 52672|       "edge_type": "REFERENCES",
 52673|       "confidence": "direct_return_type",
 52674|       "confidence_tier": "core",
 52675|       "target_resolution": "exact",
 52676|       "evidence": [
 52677|         "return type 'View' directly names a Revit DB object type"
 52678|       ],
 52679|       "source_url": "https://www.revitapidocs.com/2025/cea72f89-cfd1-f5cb-f61d-bf047df2681b.htm",
 52680|       "dll_signature_verified": true,
 52681|       "dll_relationship_scope": "declared",
 52682|       "dll_semantic_verified": null,
 52683|       "dll_verified_status": "signature_verified_declared",
 52684|       "revitlookup_referenced": null,
 52685|       "revitlookup_requires_document_context": null
 52686|     },
 52687|     {
 52688|       "source": "Autodesk.Revit.DB.OrdinateDimensionSetting",
 52689|       "target": null,
 52690|       "member_name": "OriginTickMarkId",
 52691|       "member_kind": "property",
 52692|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 52693|       "confidence": "unknown_reference",
 52694|       "confidence_tier": "unverified_reference",
 52695|       "target_resolution": "none",
 52696|       "evidence": [
 52697|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 52698|       ],
 52699|       "source_url": "https://www.revitapidocs.com/2025/f957303b-719e-67c1-6caf-8ec9380da282.htm",
 52700|       "dll_signature_verified": true,
 52701|       "dll_relationship_scope": "declared",
 52702|       "dll_semantic_verified": null,
 52703|       "dll_verified_status": "signature_verified_declared",
 52704|       "revitlookup_referenced": null,
 52705|       "revitlookup_requires_document_context": null
 52706|     },
 52707|     {
 52708|       "source": "Autodesk.Revit.DB.OverrideGraphicSettings",
 52709|       "target": null,
 52710|       "member_name": "CutBackgroundPatternId",
 52711|       "member_kind": "property",
 52712|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 52713|       "confidence": "unknown_reference",
 52714|       "confidence_tier": "unverified_reference",
 52715|       "target_resolution": "none",
 52716|       "evidence": [
 52717|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 52718|       ],
 52719|       "source_url": "https://www.revitapidocs.com/2025/fdd76258-57ff-1e74-8899-ee17bff133f6.htm",
 52720|       "dll_signature_verified": true,
 52721|       "dll_relationship_scope": "declared",
 52722|       "dll_semantic_verified": null,
 52723|       "dll_verified_status": "signature_verified_declared",
 52724|       "revitlookup_referenced": null,
 52725|       "revitlookup_requires_document_context": null
 52726|     },
 52727|     {
 52728|       "source": "Autodesk.Revit.DB.OverrideGraphicSettings",
 52729|       "target": null,
 52730|       "member_name": "CutForegroundPatternId",
 52731|       "member_kind": "property",
 52732|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 52733|       "confidence": "unknown_reference",
 52734|       "confidence_tier": "unverified_reference",
 52735|       "target_resolution": "none",
 52736|       "evidence": [
 52737|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 52738|       ],
 52739|       "source_url": "https://www.revitapidocs.com/2025/e282ebe8-2744-3419-7da0-ef8cda7e3a8c.htm",
 52740|       "dll_signature_verified": true,
 52741|       "dll_relationship_scope": "declared",
 52742|       "dll_semantic_verified": null,
 52743|       "dll_verified_status": "signature_verified_declared",
 52744|       "revitlookup_referenced": null,
 52745|       "revitlookup_requires_document_context": null
 52746|     },
 52747|     {
 52748|       "source": "Autodesk.Revit.DB.OverrideGraphicSettings",
 52749|       "target": "Autodesk.Revit.DB.LinePatternElement",
 52750|       "member_name": "CutLinePatternId",
 52751|       "member_kind": "property",
 52752|       "edge_type": "USES_LINE_PATTERN",
 52753|       "confidence": "elementid_with_strong_name",
 52754|       "confidence_tier": "core",
 52755|       "target_resolution": "exact",
 52756|       "evidence": [
 52757|         "member name 'CutLinePatternId' matches keyword pattern /LinePattern/"
 52758|       ],
 52759|       "source_url": "https://www.revitapidocs.com/2025/b27703e8-6f6e-8def-d5e9-f508f091a068.htm",
 52760|       "dll_signature_verified": true,
 52761|       "dll_relationship_scope": "declared",
 52762|       "dll_semantic_verified": null,
 52763|       "dll_verified_status": "signature_verified_declared",
 52764|       "revitlookup_referenced": null,
 52765|       "revitlookup_requires_document_context": null
 52766|     },
 52767|     {
 52768|       "source": "Autodesk.Revit.DB.OverrideGraphicSettings",
 52769|       "target": "Autodesk.Revit.DB.Level",
 52770|       "member_name": "DetailLevel",
 52771|       "member_kind": "property",
 52772|       "edge_type": "ASSIGNED_TO_LEVEL",
 52773|       "confidence": "name_only_candidate",
 52774|       "confidence_tier": "likely",
 52775|       "target_resolution": "exact",
 52776|       "evidence": [
 52777|         "member name 'DetailLevel' matches keyword pattern /Level/ but return type 'ViewDetailLevel' gives no type-level confirmation"
 52778|       ],
 52779|       "source_url": "https://www.revitapidocs.com/2025/e7042469-8b4d-6e4a-179d-bb0631c4019e.htm",
 52780|       "dll_signature_verified": true,
 52781|       "dll_relationship_scope": "declared",
 52782|       "dll_semantic_verified": null,
 52783|       "dll_verified_status": "signature_verified_declared",
 52784|       "revitlookup_referenced": null,
 52785|       "revitlookup_requires_document_context": null
 52786|     },
 52787|     {
 52788|       "source": "Autodesk.Revit.DB.OverrideGraphicSettings",
 52789|       "target": "Autodesk.Revit.DB.LinePatternElement",
 52790|       "member_name": "ProjectionLinePatternId",
 52791|       "member_kind": "property",
 52792|       "edge_type": "USES_LINE_PATTERN",
 52793|       "confidence": "elementid_with_strong_name",
 52794|       "confidence_tier": "core",
 52795|       "target_resolution": "exact",
 52796|       "evidence": [
 52797|         "member name 'ProjectionLinePatternId' matches keyword pattern /LinePattern/"
 52798|       ],
 52799|       "source_url": "https://www.revitapidocs.com/2025/1aa202a8-bc28-bd00-e00b-3ea339d83ac1.htm",
 52800|       "dll_signature_verified": true,
 52801|       "dll_relationship_scope": "declared",
 52802|       "dll_semantic_verified": null,
 52803|       "dll_verified_status": "signature_verified_declared",
 52804|       "revitlookup_referenced": null,
 52805|       "revitlookup_requires_document_context": null
 52806|     },
 52807|     {
 52808|       "source": "Autodesk.Revit.DB.OverrideGraphicSettings",
 52809|       "target": null,
 52810|       "member_name": "SurfaceBackgroundPatternId",
 52811|       "member_kind": "property",
 52812|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 52813|       "confidence": "unknown_reference",
 52814|       "confidence_tier": "unverified_reference",
 52815|       "target_resolution": "none",
 52816|       "evidence": [
 52817|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 52818|       ],
 52819|       "source_url": "https://www.revitapidocs.com/2025/e3837120-6205-c990-5151-885c0099e2b4.htm",
 52820|       "dll_signature_verified": true,
 52821|       "dll_relationship_scope": "declared",
 52822|       "dll_semantic_verified": null,
 52823|       "dll_verified_status": "signature_verified_declared",
 52824|       "revitlookup_referenced": null,
 52825|       "revitlookup_requires_document_context": null
 52826|     },
 52827|     {
 52828|       "source": "Autodesk.Revit.DB.OverrideGraphicSettings",
 52829|       "target": null,
 52830|       "member_name": "SurfaceForegroundPatternId",
 52831|       "member_kind": "property",
 52832|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 52833|       "confidence": "unknown_reference",
 52834|       "confidence_tier": "unverified_reference",
 52835|       "target_resolution": "none",
 52836|       "evidence": [
 52837|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 52838|       ],
 52839|       "source_url": "https://www.revitapidocs.com/2025/cd64973e-4dd9-67a2-fb29-16d6c913f623.htm",
 52840|       "dll_signature_verified": true,
 52841|       "dll_relationship_scope": "declared",
 52842|       "dll_semantic_verified": null,
 52843|       "dll_verified_status": "signature_verified_declared",
 52844|       "revitlookup_referenced": null,
 52845|       "revitlookup_requires_document_context": null
 52846|     },
 52847|     {
 52848|       "source": "Autodesk.Revit.DB.Panel",
 52849|       "target": "Autodesk.Revit.DB.PanelType",
 52850|       "member_name": "PanelType",
 52851|       "member_kind": "property",
 52852|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 52853|       "confidence": "direct_return_type",
 52854|       "confidence_tier": "unverified_reference",
 52855|       "target_resolution": "exact",
 52856|       "evidence": [
 52857|         "return type 'PanelType' directly names a Revit DB object type"
 52858|       ],
 52859|       "source_url": "https://www.revitapidocs.com/2025/b2a37660-4c7f-2229-04e9-c85c8dddd9cc.htm",
 52860|       "dll_signature_verified": true,
 52861|       "dll_relationship_scope": "declared",
 52862|       "dll_semantic_verified": null,
 52863|       "dll_verified_status": "signature_verified_declared",
 52864|       "revitlookup_referenced": null,
 52865|       "revitlookup_requires_document_context": null
 52866|     },
 52867|     {
 52868|       "source": "Autodesk.Revit.DB.Panel",
 52869|       "target": null,
 52870|       "member_name": "FindHostPanel",
 52871|       "member_kind": "method",
 52872|       "edge_type": "HOSTED_BY",
 52873|       "confidence": "elementid_with_strong_name",
 52874|       "confidence_tier": "core",
 52875|       "target_resolution": "none",
 52876|       "evidence": [
 52877|         "member name 'FindHostPanel' matches keyword pattern /^GetHosted|Host/"
 52878|       ],
 52879|       "source_url": "https://www.revitapidocs.com/2025/412c38f0-a6cc-3d20-4b18-bad93839c986.htm",
 52880|       "dll_signature_verified": true,
 52881|       "dll_relationship_scope": "declared",
 52882|       "dll_semantic_verified": null,
 52883|       "dll_verified_status": "signature_verified_declared",
 52884|       "revitlookup_referenced": null,
 52885|       "revitlookup_requires_document_context": null
 52886|     },
 52887|     {
 52888|       "source": "Autodesk.Revit.DB.PanelTypeSet",
 52889|       "target": "Autodesk.Revit.DB.PanelTypeSetIterator",
 52890|       "member_name": "ForwardIterator",
 52891|       "member_kind": "method",
 52892|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 52893|       "confidence": "direct_return_type",
 52894|       "confidence_tier": "unverified_reference",
 52895|       "target_resolution": "exact",
 52896|       "evidence": [
 52897|         "return type 'PanelTypeSetIterator' directly names a Revit DB object type"
 52898|       ],
 52899|       "source_url": "https://www.revitapidocs.com/2025/fa11671d-4078-a3e6-2fdd-cb38a5eec5f7.htm",
 52900|       "dll_signature_verified": true,
 52901|       "dll_relationship_scope": "declared",
 52902|       "dll_semantic_verified": null,
 52903|       "dll_verified_status": "signature_verified_declared",
 52904|       "revitlookup_referenced": null,
 52905|       "revitlookup_requires_document_context": null
 52906|     },
 52907|     {
 52908|       "source": "Autodesk.Revit.DB.PanelTypeSet",
 52909|       "target": "Autodesk.Revit.DB.PanelTypeSetIterator",
 52910|       "member_name": "ReverseIterator",
 52911|       "member_kind": "method",
 52912|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 52913|       "confidence": "direct_return_type",
 52914|       "confidence_tier": "unverified_reference",
 52915|       "target_resolution": "exact",
 52916|       "evidence": [
 52917|         "return type 'PanelTypeSetIterator' directly names a Revit DB object type"
 52918|       ],
 52919|       "source_url": "https://www.revitapidocs.com/2025/4d3573ed-4e0c-09ae-d29b-59c1dc36d051.htm",
 52920|       "dll_signature_verified": true,
 52921|       "dll_relationship_scope": "declared",
 52922|       "dll_semantic_verified": null,
 52923|       "dll_verified_status": "signature_verified_declared",
 52924|       "revitlookup_referenced": null,
 52925|       "revitlookup_requires_document_context": null
 52926|     },
 52927|     {
 52928|       "source": "Autodesk.Revit.DB.PaperSizeSet",
 52929|       "target": "Autodesk.Revit.DB.PaperSizeSetIterator",
 52930|       "member_name": "ForwardIterator",
 52931|       "member_kind": "method",
 52932|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 52933|       "confidence": "direct_return_type",
 52934|       "confidence_tier": "unverified_reference",
 52935|       "target_resolution": "exact",
 52936|       "evidence": [
 52937|         "return type 'PaperSizeSetIterator' directly names a Revit DB object type"
 52938|       ],
 52939|       "source_url": "https://www.revitapidocs.com/2025/4fea9209-1bdd-4cb6-36ee-1a619671b82e.htm",
 52940|       "dll_signature_verified": true,
 52941|       "dll_relationship_scope": "declared",
 52942|       "dll_semantic_verified": null,
 52943|       "dll_verified_status": "signature_verified_declared",
 52944|       "revitlookup_referenced": null,
 52945|       "revitlookup_requires_document_context": null
 52946|     },
 52947|     {
 52948|       "source": "Autodesk.Revit.DB.PaperSizeSet",
 52949|       "target": "Autodesk.Revit.DB.PaperSizeSetIterator",
 52950|       "member_name": "ReverseIterator",
 52951|       "member_kind": "method",
 52952|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 52953|       "confidence": "direct_return_type",
 52954|       "confidence_tier": "unverified_reference",
 52955|       "target_resolution": "exact",
 52956|       "evidence": [
 52957|         "return type 'PaperSizeSetIterator' directly names a Revit DB object type"
 52958|       ],
 52959|       "source_url": "https://www.revitapidocs.com/2025/2a5121cf-8cbf-f00c-9124-b4d20d36cb7b.htm",
 52960|       "dll_signature_verified": true,
 52961|       "dll_relationship_scope": "declared",
 52962|       "dll_semantic_verified": null,
 52963|       "dll_verified_status": "signature_verified_declared",
 52964|       "revitlookup_referenced": null,
 52965|       "revitlookup_requires_document_context": null
 52966|     },
 52967|     {
 52968|       "source": "Autodesk.Revit.DB.PaperSourceSet",
 52969|       "target": "Autodesk.Revit.DB.PaperSourceSetIterator",
 52970|       "member_name": "ForwardIterator",
 52971|       "member_kind": "method",
 52972|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 52973|       "confidence": "direct_return_type",
 52974|       "confidence_tier": "unverified_reference",
 52975|       "target_resolution": "exact",
 52976|       "evidence": [
 52977|         "return type 'PaperSourceSetIterator' directly names a Revit DB object type"
 52978|       ],
 52979|       "source_url": "https://www.revitapidocs.com/2025/a2fac2d0-19a0-2694-6392-0cc91b029830.htm",
 52980|       "dll_signature_verified": true,
 52981|       "dll_relationship_scope": "declared",
 52982|       "dll_semantic_verified": null,
 52983|       "dll_verified_status": "signature_verified_declared",
 52984|       "revitlookup_referenced": null,
 52985|       "revitlookup_requires_document_context": null
 52986|     },
 52987|     {
 52988|       "source": "Autodesk.Revit.DB.PaperSourceSet",
 52989|       "target": "Autodesk.Revit.DB.PaperSourceSetIterator",
 52990|       "member_name": "ReverseIterator",
 52991|       "member_kind": "method",
 52992|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 52993|       "confidence": "direct_return_type",
 52994|       "confidence_tier": "unverified_reference",
 52995|       "target_resolution": "exact",
 52996|       "evidence": [
 52997|         "return type 'PaperSourceSetIterator' directly names a Revit DB object type"
 52998|       ],
 52999|       "source_url": "https://www.revitapidocs.com/2025/a13727a0-ead9-079e-26ae-45cfaaad3a92.htm",
 53000|       "dll_signature_verified": true,
 53001|       "dll_relationship_scope": "declared",
 53002|       "dll_semantic_verified": null,
 53003|       "dll_verified_status": "signature_verified_declared",
 53004|       "revitlookup_referenced": null,
 53005|       "revitlookup_requires_document_context": null
 53006|     },
 53007|     {
 53008|       "source": "Autodesk.Revit.DB.Parameter",
 53009|       "target": "Autodesk.Revit.DB.Definition",
 53010|       "member_name": "Definition",
 53011|       "member_kind": "property",
 53012|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 53013|       "confidence": "direct_return_type",
 53014|       "confidence_tier": "unverified_reference",
 53015|       "target_resolution": "exact",
 53016|       "evidence": [
 53017|         "return type 'Definition' directly names a Revit DB object type"
 53018|       ],
 53019|       "source_url": "https://www.revitapidocs.com/2025/dc30c65f-cfc4-244e-5a5c-bc333d7cd4c5.htm",
 53020|       "dll_signature_verified": true,
 53021|       "dll_relationship_scope": "declared",
 53022|       "dll_semantic_verified": null,
 53023|       "dll_verified_status": "signature_verified_declared",
 53024|       "revitlookup_referenced": null,
 53025|       "revitlookup_requires_document_context": null
 53026|     },
 53027|     {
 53028|       "source": "Autodesk.Revit.DB.Parameter",
 53029|       "target": "Autodesk.Revit.DB.Element",
 53030|       "member_name": "Element",
 53031|       "member_kind": "property",
 53032|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 53033|       "confidence": "direct_return_type",
 53034|       "confidence_tier": "unverified_reference",
 53035|       "target_resolution": "exact",
 53036|       "evidence": [
 53037|         "return type 'Element' directly names a Revit DB object type"
 53038|       ],
 53039|       "source_url": "https://www.revitapidocs.com/2025/0645cb13-9c25-7f66-b22d-898832dc2ae3.htm",
 53040|       "dll_signature_verified": true,
 53041|       "dll_relationship_scope": "declared",
 53042|       "dll_semantic_verified": null,
 53043|       "dll_verified_status": "signature_verified_declared",
 53044|       "revitlookup_referenced": null,
 53045|       "revitlookup_requires_document_context": null
 53046|     },
 53047|     {
 53048|       "source": "Autodesk.Revit.DB.Parameter",
 53049|       "target": null,
 53050|       "member_name": "Id",
```
