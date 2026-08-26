# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 151 of 216
- Original line range: 58501-58900
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 58501|       "source_url": "https://www.revitapidocs.com/2025/d463203b-c692-7217-09fc-eb4110509145.htm",
 58502|       "dll_signature_verified": true,
 58503|       "dll_relationship_scope": "declared",
 58504|       "dll_semantic_verified": null,
 58505|       "dll_verified_status": "signature_verified_declared",
 58506|       "revitlookup_referenced": null,
 58507|       "revitlookup_requires_document_context": null
 58508|     },
 58509|     {
 58510|       "source": "Autodesk.Revit.DB.Sketch",
 58511|       "target": null,
 58512|       "member_name": "GetAllElements",
 58513|       "member_kind": "method",
 58514|       "edge_type": "RETURNS_ELEMENT_IDS",
 58515|       "confidence": "elementid_collection_with_strong_name",
 58516|       "confidence_tier": "core",
 58517|       "target_resolution": "none",
 58518|       "evidence": [
 58519|         "member name 'GetAllElements' matches keyword pattern /^GetAll/"
 58520|       ],
 58521|       "source_url": "https://www.revitapidocs.com/2025/2c93758e-684c-5f84-9206-f04e5ceb15d5.htm",
 58522|       "dll_signature_verified": true,
 58523|       "dll_relationship_scope": "declared",
 58524|       "dll_semantic_verified": null,
 58525|       "dll_verified_status": "signature_verified_declared",
 58526|       "revitlookup_referenced": null,
 58527|       "revitlookup_requires_document_context": null
 58528|     },
 58529|     {
 58530|       "source": "Autodesk.Revit.DB.SketchEditScope",
 58531|       "target": "Autodesk.Revit.DB.Sketch",
 58532|       "member_name": "IsElementWithoutSketch",
 58533|       "member_kind": "method",
 58534|       "edge_type": "DEPENDS_ON",
 58535|       "confidence": "name_only_candidate",
 58536|       "confidence_tier": "likely",
 58537|       "target_resolution": "exact",
 58538|       "evidence": [
 58539|         "member name 'IsElementWithoutSketch' matches keyword pattern /Sketch(Id)?$/ but return type 'bool' gives no type-level confirmation"
 58540|       ],
 58541|       "source_url": "https://www.revitapidocs.com/2025/ca9debb4-73b6-ce7c-742a-f7a8ab0588da.htm",
 58542|       "dll_signature_verified": true,
 58543|       "dll_relationship_scope": "declared",
 58544|       "dll_semantic_verified": null,
 58545|       "dll_verified_status": "signature_verified_declared",
 58546|       "revitlookup_referenced": null,
 58547|       "revitlookup_requires_document_context": null
 58548|     },
 58549|     {
 58550|       "source": "Autodesk.Revit.DB.SketchEditScope",
 58551|       "target": "Autodesk.Revit.DB.Sketch",
 58552|       "member_name": "StartWithNewSketch",
 58553|       "member_kind": "method",
 58554|       "edge_type": "DEPENDS_ON",
 58555|       "confidence": "name_only_candidate",
 58556|       "confidence_tier": "likely",
 58557|       "target_resolution": "exact",
 58558|       "evidence": [
 58559|         "member name 'StartWithNewSketch' matches keyword pattern /Sketch(Id)?$/ but return type 'void' gives no type-level confirmation"
 58560|       ],
 58561|       "source_url": "https://www.revitapidocs.com/2025/4150d043-a5bf-60ba-b986-11f1dc01eedf.htm",
 58562|       "dll_signature_verified": true,
 58563|       "dll_relationship_scope": "declared",
 58564|       "dll_semantic_verified": null,
 58565|       "dll_verified_status": "signature_verified_declared",
 58566|       "revitlookup_referenced": null,
 58567|       "revitlookup_requires_document_context": null
 58568|     },
 58569|     {
 58570|       "source": "Autodesk.Revit.DB.SketchPlane",
 58571|       "target": "Autodesk.Revit.DB.Reference",
 58572|       "member_name": "GetPlaneReference",
 58573|       "member_kind": "method",
 58574|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58575|       "confidence": "direct_return_type",
 58576|       "confidence_tier": "unverified_reference",
 58577|       "target_resolution": "exact",
 58578|       "evidence": [
 58579|         "return type 'Reference' directly names a Revit DB object type"
 58580|       ],
 58581|       "source_url": "https://www.revitapidocs.com/2025/21ff547e-019a-998f-77c0-5775c0b814ca.htm",
 58582|       "dll_signature_verified": true,
 58583|       "dll_relationship_scope": "declared",
 58584|       "dll_semantic_verified": null,
 58585|       "dll_verified_status": "signature_verified_declared",
 58586|       "revitlookup_referenced": null,
 58587|       "revitlookup_requires_document_context": null
 58588|     },
 58589|     {
 58590|       "source": "Autodesk.Revit.DB.SlabEdge",
 58591|       "target": "Autodesk.Revit.DB.SlabEdgeType",
 58592|       "member_name": "SlabEdgeType",
 58593|       "member_kind": "property",
 58594|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58595|       "confidence": "direct_return_type",
 58596|       "confidence_tier": "unverified_reference",
 58597|       "target_resolution": "exact",
 58598|       "evidence": [
 58599|         "return type 'SlabEdgeType' directly names a Revit DB object type"
 58600|       ],
 58601|       "source_url": "https://www.revitapidocs.com/2025/fd61ed95-0d48-9e46-2dd7-4c8486a4ccce.htm",
 58602|       "dll_signature_verified": true,
 58603|       "dll_relationship_scope": "declared",
 58604|       "dll_semantic_verified": null,
 58605|       "dll_verified_status": "signature_verified_declared",
 58606|       "revitlookup_referenced": null,
 58607|       "revitlookup_requires_document_context": null
 58608|     },
 58609|     {
 58610|       "source": "Autodesk.Revit.DB.SlabShapeCrease",
 58611|       "target": "Autodesk.Revit.DB.SlabShapeVertexArray",
 58612|       "member_name": "EndPoints",
 58613|       "member_kind": "property",
 58614|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58615|       "confidence": "direct_return_type",
 58616|       "confidence_tier": "unverified_reference",
 58617|       "target_resolution": "exact",
 58618|       "evidence": [
 58619|         "return type 'SlabShapeVertexArray' directly names a Revit DB object type"
 58620|       ],
 58621|       "source_url": "https://www.revitapidocs.com/2025/e55954a9-8b9a-6578-c3c0-e35187b2867e.htm",
 58622|       "dll_signature_verified": true,
 58623|       "dll_relationship_scope": "declared",
 58624|       "dll_semantic_verified": null,
 58625|       "dll_verified_status": "signature_verified_declared",
 58626|       "revitlookup_referenced": null,
 58627|       "revitlookup_requires_document_context": null
 58628|     },
 58629|     {
 58630|       "source": "Autodesk.Revit.DB.SlabShapeCreaseArray",
 58631|       "target": "Autodesk.Revit.DB.SlabShapeCreaseArrayIterator",
 58632|       "member_name": "ForwardIterator",
 58633|       "member_kind": "method",
 58634|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58635|       "confidence": "direct_return_type",
 58636|       "confidence_tier": "unverified_reference",
 58637|       "target_resolution": "exact",
 58638|       "evidence": [
 58639|         "return type 'SlabShapeCreaseArrayIterator' directly names a Revit DB object type"
 58640|       ],
 58641|       "source_url": "https://www.revitapidocs.com/2025/0a8bfec9-c0a9-ddcf-2ac6-5af5d7d622c1.htm",
 58642|       "dll_signature_verified": true,
 58643|       "dll_relationship_scope": "declared",
 58644|       "dll_semantic_verified": null,
 58645|       "dll_verified_status": "signature_verified_declared",
 58646|       "revitlookup_referenced": null,
 58647|       "revitlookup_requires_document_context": null
 58648|     },
 58649|     {
 58650|       "source": "Autodesk.Revit.DB.SlabShapeCreaseArray",
 58651|       "target": "Autodesk.Revit.DB.SlabShapeCreaseArrayIterator",
 58652|       "member_name": "ReverseIterator",
 58653|       "member_kind": "method",
 58654|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58655|       "confidence": "direct_return_type",
 58656|       "confidence_tier": "unverified_reference",
 58657|       "target_resolution": "exact",
 58658|       "evidence": [
 58659|         "return type 'SlabShapeCreaseArrayIterator' directly names a Revit DB object type"
 58660|       ],
 58661|       "source_url": "https://www.revitapidocs.com/2025/8b397860-2f53-c81a-260c-6fa6f86e30ab.htm",
 58662|       "dll_signature_verified": true,
 58663|       "dll_relationship_scope": "declared",
 58664|       "dll_semantic_verified": null,
 58665|       "dll_verified_status": "signature_verified_declared",
 58666|       "revitlookup_referenced": null,
 58667|       "revitlookup_requires_document_context": null
 58668|     },
 58669|     {
 58670|       "source": "Autodesk.Revit.DB.SlabShapeEditor",
 58671|       "target": "Autodesk.Revit.DB.SlabShapeCreaseArray",
 58672|       "member_name": "SlabShapeCreases",
 58673|       "member_kind": "property",
 58674|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58675|       "confidence": "direct_return_type",
 58676|       "confidence_tier": "unverified_reference",
 58677|       "target_resolution": "exact",
 58678|       "evidence": [
 58679|         "return type 'SlabShapeCreaseArray' directly names a Revit DB object type"
 58680|       ],
 58681|       "source_url": "https://www.revitapidocs.com/2025/fb345daf-b097-a458-8c69-2d8cbfa1eff3.htm",
 58682|       "dll_signature_verified": true,
 58683|       "dll_relationship_scope": "declared",
 58684|       "dll_semantic_verified": null,
 58685|       "dll_verified_status": "signature_verified_declared",
 58686|       "revitlookup_referenced": null,
 58687|       "revitlookup_requires_document_context": null
 58688|     },
 58689|     {
 58690|       "source": "Autodesk.Revit.DB.SlabShapeEditor",
 58691|       "target": "Autodesk.Revit.DB.SlabShapeVertexArray",
 58692|       "member_name": "SlabShapeVertices",
 58693|       "member_kind": "property",
 58694|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58695|       "confidence": "direct_return_type",
 58696|       "confidence_tier": "unverified_reference",
 58697|       "target_resolution": "exact",
 58698|       "evidence": [
 58699|         "return type 'SlabShapeVertexArray' directly names a Revit DB object type"
 58700|       ],
 58701|       "source_url": "https://www.revitapidocs.com/2025/01fbf5d9-6fa7-6483-6a1c-5cf439f27dc7.htm",
 58702|       "dll_signature_verified": true,
 58703|       "dll_relationship_scope": "declared",
 58704|       "dll_semantic_verified": null,
 58705|       "dll_verified_status": "signature_verified_declared",
 58706|       "revitlookup_referenced": null,
 58707|       "revitlookup_requires_document_context": null
 58708|     },
 58709|     {
 58710|       "source": "Autodesk.Revit.DB.SlabShapeEditor",
 58711|       "target": "Autodesk.Revit.DB.SlabShapeVertex",
 58712|       "member_name": "AddPoint",
 58713|       "member_kind": "method",
 58714|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58715|       "confidence": "direct_return_type",
 58716|       "confidence_tier": "unverified_reference",
 58717|       "target_resolution": "exact",
 58718|       "evidence": [
 58719|         "return type 'SlabShapeVertex' directly names a Revit DB object type"
 58720|       ],
 58721|       "source_url": "https://www.revitapidocs.com/2025/5d875cfd-f401-6f88-cd07-3999543e8f18.htm",
 58722|       "dll_signature_verified": true,
 58723|       "dll_relationship_scope": "declared",
 58724|       "dll_semantic_verified": null,
 58725|       "dll_verified_status": "signature_verified_declared",
 58726|       "revitlookup_referenced": null,
 58727|       "revitlookup_requires_document_context": null
 58728|     },
 58729|     {
 58730|       "source": "Autodesk.Revit.DB.SlabShapeEditor",
 58731|       "target": "Autodesk.Revit.DB.SlabShapeVertex",
 58732|       "member_name": "AddPoints",
 58733|       "member_kind": "method",
 58734|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58735|       "confidence": "needs_runtime_validation",
 58736|       "confidence_tier": "needs_validation",
 58737|       "target_resolution": "exact",
 58738|       "evidence": [
 58739|         "return type 'IList < SlabShapeVertex >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 58740|       ],
 58741|       "source_url": "https://www.revitapidocs.com/2025/52f630ac-2e57-4b33-7776-d499d469630d.htm",
 58742|       "dll_signature_verified": true,
 58743|       "dll_relationship_scope": "declared",
 58744|       "dll_semantic_verified": null,
 58745|       "dll_verified_status": "signature_verified_declared",
 58746|       "revitlookup_referenced": null,
 58747|       "revitlookup_requires_document_context": null
 58748|     },
 58749|     {
 58750|       "source": "Autodesk.Revit.DB.SlabShapeEditor",
 58751|       "target": "Autodesk.Revit.DB.SlabShapeCrease",
 58752|       "member_name": "AddSplitLine",
 58753|       "member_kind": "method",
 58754|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58755|       "confidence": "needs_runtime_validation",
 58756|       "confidence_tier": "needs_validation",
 58757|       "target_resolution": "exact",
 58758|       "evidence": [
 58759|         "return type 'IList < SlabShapeCrease >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 58760|       ],
 58761|       "source_url": "https://www.revitapidocs.com/2025/2a254c45-4bff-9fbc-6ac0-680d79a3c88b.htm",
 58762|       "dll_signature_verified": true,
 58763|       "dll_relationship_scope": "declared",
 58764|       "dll_semantic_verified": null,
 58765|       "dll_verified_status": "signature_verified_declared",
 58766|       "revitlookup_referenced": null,
 58767|       "revitlookup_requires_document_context": null
 58768|     },
 58769|     {
 58770|       "source": "Autodesk.Revit.DB.SlabShapeVertexArray",
 58771|       "target": "Autodesk.Revit.DB.SlabShapeVertexArrayIterator",
 58772|       "member_name": "ForwardIterator",
 58773|       "member_kind": "method",
 58774|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58775|       "confidence": "direct_return_type",
 58776|       "confidence_tier": "unverified_reference",
 58777|       "target_resolution": "exact",
 58778|       "evidence": [
 58779|         "return type 'SlabShapeVertexArrayIterator' directly names a Revit DB object type"
 58780|       ],
 58781|       "source_url": "https://www.revitapidocs.com/2025/7ca07b40-41f7-2ace-1f6d-8ffec0704e82.htm",
 58782|       "dll_signature_verified": true,
 58783|       "dll_relationship_scope": "declared",
 58784|       "dll_semantic_verified": null,
 58785|       "dll_verified_status": "signature_verified_declared",
 58786|       "revitlookup_referenced": null,
 58787|       "revitlookup_requires_document_context": null
 58788|     },
 58789|     {
 58790|       "source": "Autodesk.Revit.DB.SlabShapeVertexArray",
 58791|       "target": "Autodesk.Revit.DB.SlabShapeVertexArrayIterator",
 58792|       "member_name": "ReverseIterator",
 58793|       "member_kind": "method",
 58794|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58795|       "confidence": "direct_return_type",
 58796|       "confidence_tier": "unverified_reference",
 58797|       "target_resolution": "exact",
 58798|       "evidence": [
 58799|         "return type 'SlabShapeVertexArrayIterator' directly names a Revit DB object type"
 58800|       ],
 58801|       "source_url": "https://www.revitapidocs.com/2025/b2df4ea5-65ff-8ddf-8801-969880878710.htm",
 58802|       "dll_signature_verified": true,
 58803|       "dll_relationship_scope": "declared",
 58804|       "dll_semantic_verified": null,
 58805|       "dll_verified_status": "signature_verified_declared",
 58806|       "revitlookup_referenced": null,
 58807|       "revitlookup_requires_document_context": null
 58808|     },
 58809|     {
 58810|       "source": "Autodesk.Revit.DB.Solid",
 58811|       "target": "Autodesk.Revit.DB.EdgeArray",
 58812|       "member_name": "Edges",
 58813|       "member_kind": "property",
 58814|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58815|       "confidence": "direct_return_type",
 58816|       "confidence_tier": "unverified_reference",
 58817|       "target_resolution": "exact",
 58818|       "evidence": [
 58819|         "return type 'EdgeArray' directly names a Revit DB object type"
 58820|       ],
 58821|       "source_url": "https://www.revitapidocs.com/2025/09baca60-e5ab-eef8-2622-2b956f258c8a.htm",
 58822|       "dll_signature_verified": true,
 58823|       "dll_relationship_scope": "declared",
 58824|       "dll_semantic_verified": null,
 58825|       "dll_verified_status": "signature_verified_declared",
 58826|       "revitlookup_referenced": null,
 58827|       "revitlookup_requires_document_context": null
 58828|     },
 58829|     {
 58830|       "source": "Autodesk.Revit.DB.Solid",
 58831|       "target": "Autodesk.Revit.DB.FaceArray",
 58832|       "member_name": "Faces",
 58833|       "member_kind": "property",
 58834|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58835|       "confidence": "direct_return_type",
 58836|       "confidence_tier": "unverified_reference",
 58837|       "target_resolution": "exact",
 58838|       "evidence": [
 58839|         "return type 'FaceArray' directly names a Revit DB object type"
 58840|       ],
 58841|       "source_url": "https://www.revitapidocs.com/2025/b45fa881-3077-409c-0ef1-5d42744e7429.htm",
 58842|       "dll_signature_verified": true,
 58843|       "dll_relationship_scope": "declared",
 58844|       "dll_semantic_verified": null,
 58845|       "dll_verified_status": "signature_verified_declared",
 58846|       "revitlookup_referenced": null,
 58847|       "revitlookup_requires_document_context": null
 58848|     },
 58849|     {
 58850|       "source": "Autodesk.Revit.DB.Solid",
 58851|       "target": "Autodesk.Revit.DB.SolidCurveIntersection",
 58852|       "member_name": "IntersectWithCurve",
 58853|       "member_kind": "method",
 58854|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58855|       "confidence": "direct_return_type",
 58856|       "confidence_tier": "unverified_reference",
 58857|       "target_resolution": "exact",
 58858|       "evidence": [
 58859|         "return type 'SolidCurveIntersection' directly names a Revit DB object type"
 58860|       ],
 58861|       "source_url": "https://www.revitapidocs.com/2025/8e04f956-b262-7f3e-59cb-d2c02c2769d7.htm",
 58862|       "dll_signature_verified": true,
 58863|       "dll_relationship_scope": "declared",
 58864|       "dll_semantic_verified": null,
 58865|       "dll_verified_status": "signature_verified_declared",
 58866|       "revitlookup_referenced": null,
 58867|       "revitlookup_requires_document_context": null
 58868|     },
 58869|     {
 58870|       "source": "Autodesk.Revit.DB.SolidCurveIntersection",
 58871|       "target": "Autodesk.Revit.DB.CurveExtents",
 58872|       "member_name": "GetCurveSegmentExtents",
 58873|       "member_kind": "method",
 58874|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 58875|       "confidence": "direct_return_type",
 58876|       "confidence_tier": "unverified_reference",
 58877|       "target_resolution": "exact",
 58878|       "evidence": [
 58879|         "return type 'CurveExtents' directly names a Revit DB object type"
 58880|       ],
 58881|       "source_url": "https://www.revitapidocs.com/2025/29753329-2fca-2833-3143-5190da1c86a0.htm",
 58882|       "dll_signature_verified": true,
 58883|       "dll_relationship_scope": "declared",
 58884|       "dll_semantic_verified": null,
 58885|       "dll_verified_status": "signature_verified_declared",
 58886|       "revitlookup_referenced": null,
 58887|       "revitlookup_requires_document_context": null
 58888|     },
 58889|     {
 58890|       "source": "Autodesk.Revit.DB.SolidGeometryOptions",
 58891|       "target": null,
 58892|       "member_name": "SolidTag",
 58893|       "member_kind": "property",
 58894|       "edge_type": "TAGS_ELEMENT",
 58895|       "confidence": "name_only_candidate",
 58896|       "confidence_tier": "likely",
 58897|       "target_resolution": "none",
 58898|       "evidence": [
 58899|         "member name 'SolidTag' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'int' gives no type-level confirmation"
 58900|       ],
```
