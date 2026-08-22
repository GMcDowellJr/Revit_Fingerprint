# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 207 of 216
- Original line range: 80341-80740
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 80341|       "dll_relationship_scope": "declared",
 80342|       "dll_semantic_verified": null,
 80343|       "dll_verified_status": "signature_verified_declared",
 80344|       "revitlookup_referenced": null,
 80345|       "revitlookup_requires_document_context": null
 80346|     },
 80347|     {
 80348|       "source": "Autodesk.Revit.DB.Structure.RebarConstraint",
 80349|       "target": "Autodesk.Revit.DB.Reference",
 80350|       "member_name": "GetTargetHostFaceReference",
 80351|       "member_kind": "method",
 80352|       "edge_type": "HOSTED_BY",
 80353|       "confidence": "direct_return_type",
 80354|       "confidence_tier": "core",
 80355|       "target_resolution": "exact",
 80356|       "evidence": [
 80357|         "return type 'Reference' directly names a Revit DB object type"
 80358|       ],
 80359|       "source_url": "https://www.revitapidocs.com/2025/df4e4eca-c29a-faea-9bb8-26fd1ed12586.htm",
 80360|       "dll_signature_verified": true,
 80361|       "dll_relationship_scope": "declared",
 80362|       "dll_semantic_verified": null,
 80363|       "dll_verified_status": "signature_verified_declared",
 80364|       "revitlookup_referenced": null,
 80365|       "revitlookup_requires_document_context": null
 80366|     },
 80367|     {
 80368|       "source": "Autodesk.Revit.DB.Structure.RebarConstraint",
 80369|       "target": "Autodesk.Revit.DB.Reference",
 80370|       "member_name": "GetTargetHostFaceReference",
 80371|       "member_kind": "method",
 80372|       "edge_type": "HOSTED_BY",
 80373|       "confidence": "direct_return_type",
 80374|       "confidence_tier": "core",
 80375|       "target_resolution": "exact",
 80376|       "evidence": [
 80377|         "return type 'Reference' directly names a Revit DB object type"
 80378|       ],
 80379|       "source_url": "https://www.revitapidocs.com/2025/8da5f062-8f8f-3151-e029-6492a3978a50.htm",
 80380|       "dll_signature_verified": true,
 80381|       "dll_relationship_scope": "declared",
 80382|       "dll_semantic_verified": null,
 80383|       "dll_verified_status": "signature_verified_declared",
 80384|       "revitlookup_referenced": null,
 80385|       "revitlookup_requires_document_context": null
 80386|     },
 80387|     {
 80388|       "source": "Autodesk.Revit.DB.Structure.RebarConstraint",
 80389|       "target": null,
 80390|       "member_name": "IsFixedDistanceToHostFace",
 80391|       "member_kind": "method",
 80392|       "edge_type": "HOSTED_BY",
 80393|       "confidence": "name_only_candidate",
 80394|       "confidence_tier": "likely",
 80395|       "target_resolution": "none",
 80396|       "evidence": [
 80397|         "member name 'IsFixedDistanceToHostFace' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 80398|       ],
 80399|       "source_url": "https://www.revitapidocs.com/2025/b81e57f3-802d-3fa9-2e0f-3ec2b26b48cb.htm",
 80400|       "dll_signature_verified": true,
 80401|       "dll_relationship_scope": "declared",
 80402|       "dll_semantic_verified": null,
 80403|       "dll_verified_status": "signature_verified_declared",
 80404|       "revitlookup_referenced": null,
 80405|       "revitlookup_requires_document_context": null
 80406|     },
 80407|     {
 80408|       "source": "Autodesk.Revit.DB.Structure.RebarConstraint",
 80409|       "target": null,
 80410|       "member_name": "IsToHostFaceOrCover",
 80411|       "member_kind": "method",
 80412|       "edge_type": "HOSTED_BY",
 80413|       "confidence": "name_only_candidate",
 80414|       "confidence_tier": "likely",
 80415|       "target_resolution": "none",
 80416|       "evidence": [
 80417|         "member name 'IsToHostFaceOrCover' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 80418|       ],
 80419|       "source_url": "https://www.revitapidocs.com/2025/315e8a24-5a63-0310-758d-56420196880c.htm",
 80420|       "dll_signature_verified": true,
 80421|       "dll_relationship_scope": "declared",
 80422|       "dll_semantic_verified": null,
 80423|       "dll_verified_status": "signature_verified_declared",
 80424|       "revitlookup_referenced": null,
 80425|       "revitlookup_requires_document_context": null
 80426|     },
 80427|     {
 80428|       "source": "Autodesk.Revit.DB.Structure.RebarConstraint",
 80429|       "target": null,
 80430|       "member_name": "SetDistanceToTargetHostFace",
 80431|       "member_kind": "method",
 80432|       "edge_type": "HOSTED_BY",
 80433|       "confidence": "name_only_candidate",
 80434|       "confidence_tier": "likely",
 80435|       "target_resolution": "none",
 80436|       "evidence": [
 80437|         "member name 'SetDistanceToTargetHostFace' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 80438|       ],
 80439|       "source_url": "https://www.revitapidocs.com/2025/87771139-34dd-1135-026b-904e4098bdbc.htm",
 80440|       "dll_signature_verified": true,
 80441|       "dll_relationship_scope": "declared",
 80442|       "dll_semantic_verified": null,
 80443|       "dll_verified_status": "signature_verified_declared",
 80444|       "revitlookup_referenced": null,
 80445|       "revitlookup_requires_document_context": null
 80446|     },
 80447|     {
 80448|       "source": "Autodesk.Revit.DB.Structure.RebarConstraintsManager",
 80449|       "target": "Autodesk.Revit.DB.Structure.RebarConstrainedHandle",
 80450|       "member_name": "GetAllConstrainedHandles",
 80451|       "member_kind": "method",
 80452|       "edge_type": "RETURNS_ELEMENT_IDS",
 80453|       "confidence": "needs_runtime_validation",
 80454|       "confidence_tier": "needs_validation",
 80455|       "target_resolution": "short_name_fallback",
 80456|       "evidence": [
 80457|         "return type 'IList < RebarConstrainedHandle >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 80458|       ],
 80459|       "source_url": "https://www.revitapidocs.com/2025/d87a1741-7965-413d-3c44-666516fd31aa.htm",
 80460|       "dll_signature_verified": true,
 80461|       "dll_relationship_scope": "declared",
 80462|       "dll_semantic_verified": null,
 80463|       "dll_verified_status": "signature_verified_declared",
 80464|       "revitlookup_referenced": null,
 80465|       "revitlookup_requires_document_context": null
 80466|     },
 80467|     {
 80468|       "source": "Autodesk.Revit.DB.Structure.RebarConstraintsManager",
 80469|       "target": "Autodesk.Revit.DB.Structure.RebarConstrainedHandle",
 80470|       "member_name": "GetAllHandles",
 80471|       "member_kind": "method",
 80472|       "edge_type": "RETURNS_ELEMENT_IDS",
 80473|       "confidence": "needs_runtime_validation",
 80474|       "confidence_tier": "needs_validation",
 80475|       "target_resolution": "short_name_fallback",
 80476|       "evidence": [
 80477|         "return type 'IList < RebarConstrainedHandle >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 80478|       ],
 80479|       "source_url": "https://www.revitapidocs.com/2025/1a8dbc43-88f6-8087-1607-7b01d61f4560.htm",
 80480|       "dll_signature_verified": true,
 80481|       "dll_relationship_scope": "declared",
 80482|       "dll_semantic_verified": null,
 80483|       "dll_verified_status": "signature_verified_declared",
 80484|       "revitlookup_referenced": null,
 80485|       "revitlookup_requires_document_context": null
 80486|     },
 80487|     {
 80488|       "source": "Autodesk.Revit.DB.Structure.RebarConstraintsManager",
 80489|       "target": "Autodesk.Revit.DB.Structure.RebarConstraint",
 80490|       "member_name": "GetConstraintCandidatesForHandle",
 80491|       "member_kind": "method",
 80492|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80493|       "confidence": "needs_runtime_validation",
 80494|       "confidence_tier": "needs_validation",
 80495|       "target_resolution": "short_name_fallback",
 80496|       "evidence": [
 80497|         "return type 'IList < RebarConstraint >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 80498|       ],
 80499|       "source_url": "https://www.revitapidocs.com/2025/7fb6f4f8-a01f-b6c5-e553-08197ef55db6.htm",
 80500|       "dll_signature_verified": true,
 80501|       "dll_relationship_scope": "declared",
 80502|       "dll_semantic_verified": null,
 80503|       "dll_verified_status": "signature_verified_declared",
 80504|       "revitlookup_referenced": null,
 80505|       "revitlookup_requires_document_context": null
 80506|     },
 80507|     {
 80508|       "source": "Autodesk.Revit.DB.Structure.RebarConstraintsManager",
 80509|       "target": "Autodesk.Revit.DB.Structure.RebarConstraint",
 80510|       "member_name": "GetConstraintCandidatesForHandle",
 80511|       "member_kind": "method",
 80512|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80513|       "confidence": "needs_runtime_validation",
 80514|       "confidence_tier": "needs_validation",
 80515|       "target_resolution": "short_name_fallback",
 80516|       "evidence": [
 80517|         "return type 'IList < RebarConstraint >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 80518|       ],
 80519|       "source_url": "https://www.revitapidocs.com/2025/0639839a-a7a6-064d-5797-9ed609033b53.htm",
 80520|       "dll_signature_verified": true,
 80521|       "dll_relationship_scope": "declared",
 80522|       "dll_semantic_verified": null,
 80523|       "dll_verified_status": "signature_verified_declared",
 80524|       "revitlookup_referenced": null,
 80525|       "revitlookup_requires_document_context": null
 80526|     },
 80527|     {
 80528|       "source": "Autodesk.Revit.DB.Structure.RebarConstraintsManager",
 80529|       "target": "Autodesk.Revit.DB.Structure.RebarConstraint",
 80530|       "member_name": "GetCurrentConstraintOnHandle",
 80531|       "member_kind": "method",
 80532|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80533|       "confidence": "direct_return_type",
 80534|       "confidence_tier": "unverified_reference",
 80535|       "target_resolution": "short_name_fallback",
 80536|       "evidence": [
 80537|         "return type 'RebarConstraint' directly names a Revit DB object type"
 80538|       ],
 80539|       "source_url": "https://www.revitapidocs.com/2025/6020571a-fa8f-5f21-3874-f808456a8854.htm",
 80540|       "dll_signature_verified": true,
 80541|       "dll_relationship_scope": "declared",
 80542|       "dll_semantic_verified": null,
 80543|       "dll_verified_status": "signature_verified_declared",
 80544|       "revitlookup_referenced": null,
 80545|       "revitlookup_requires_document_context": null
 80546|     },
 80547|     {
 80548|       "source": "Autodesk.Revit.DB.Structure.RebarConstraintsManager",
 80549|       "target": "Autodesk.Revit.DB.Structure.RebarConstraint",
 80550|       "member_name": "GetPreferredConstraintOnHandle",
 80551|       "member_kind": "method",
 80552|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80553|       "confidence": "direct_return_type",
 80554|       "confidence_tier": "unverified_reference",
 80555|       "target_resolution": "short_name_fallback",
 80556|       "evidence": [
 80557|         "return type 'RebarConstraint' directly names a Revit DB object type"
 80558|       ],
 80559|       "source_url": "https://www.revitapidocs.com/2025/4f92a917-683e-52f7-ad29-de2025af0220.htm",
 80560|       "dll_signature_verified": true,
 80561|       "dll_relationship_scope": "declared",
 80562|       "dll_semantic_verified": null,
 80563|       "dll_verified_status": "signature_verified_declared",
 80564|       "revitlookup_referenced": null,
 80565|       "revitlookup_requires_document_context": null
 80566|     },
 80567|     {
 80568|       "source": "Autodesk.Revit.DB.Structure.RebarContainer",
 80569|       "target": "Autodesk.Revit.DB.Structure.RebarContainerItem",
 80570|       "member_name": "AppendItemFromCurves",
 80571|       "member_kind": "method",
 80572|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80573|       "confidence": "direct_return_type",
 80574|       "confidence_tier": "unverified_reference",
 80575|       "target_resolution": "short_name_fallback",
 80576|       "evidence": [
 80577|         "return type 'RebarContainerItem' directly names a Revit DB object type"
 80578|       ],
 80579|       "source_url": "https://www.revitapidocs.com/2025/5e226c33-9073-1211-2ddb-8bd061b6ebf1.htm",
 80580|       "dll_signature_verified": true,
 80581|       "dll_relationship_scope": "declared",
 80582|       "dll_semantic_verified": null,
 80583|       "dll_verified_status": "signature_verified_declared",
 80584|       "revitlookup_referenced": null,
 80585|       "revitlookup_requires_document_context": null
 80586|     },
 80587|     {
 80588|       "source": "Autodesk.Revit.DB.Structure.RebarContainer",
 80589|       "target": "Autodesk.Revit.DB.Structure.RebarContainerItem",
 80590|       "member_name": "AppendItemFromCurvesAndShape",
 80591|       "member_kind": "method",
 80592|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80593|       "confidence": "direct_return_type",
 80594|       "confidence_tier": "unverified_reference",
 80595|       "target_resolution": "short_name_fallback",
 80596|       "evidence": [
 80597|         "return type 'RebarContainerItem' directly names a Revit DB object type"
 80598|       ],
 80599|       "source_url": "https://www.revitapidocs.com/2025/4f027562-d8b9-17b7-74db-3ffde449be5f.htm",
 80600|       "dll_signature_verified": true,
 80601|       "dll_relationship_scope": "declared",
 80602|       "dll_semantic_verified": null,
 80603|       "dll_verified_status": "signature_verified_declared",
 80604|       "revitlookup_referenced": null,
 80605|       "revitlookup_requires_document_context": null
 80606|     },
 80607|     {
 80608|       "source": "Autodesk.Revit.DB.Structure.RebarContainer",
 80609|       "target": "Autodesk.Revit.DB.Structure.RebarContainerItem",
 80610|       "member_name": "AppendItemFromRebar",
 80611|       "member_kind": "method",
 80612|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80613|       "confidence": "direct_return_type",
 80614|       "confidence_tier": "unverified_reference",
 80615|       "target_resolution": "short_name_fallback",
 80616|       "evidence": [
 80617|         "return type 'RebarContainerItem' directly names a Revit DB object type"
 80618|       ],
 80619|       "source_url": "https://www.revitapidocs.com/2025/b7e0adc7-0f2e-af1e-9224-3f1ae4067e3c.htm",
 80620|       "dll_signature_verified": true,
 80621|       "dll_relationship_scope": "declared",
 80622|       "dll_semantic_verified": null,
 80623|       "dll_verified_status": "signature_verified_declared",
 80624|       "revitlookup_referenced": null,
 80625|       "revitlookup_requires_document_context": null
 80626|     },
 80627|     {
 80628|       "source": "Autodesk.Revit.DB.Structure.RebarContainer",
 80629|       "target": "Autodesk.Revit.DB.Structure.RebarContainerItem",
 80630|       "member_name": "AppendItemFromRebarShape",
 80631|       "member_kind": "method",
 80632|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80633|       "confidence": "direct_return_type",
 80634|       "confidence_tier": "unverified_reference",
 80635|       "target_resolution": "short_name_fallback",
 80636|       "evidence": [
 80637|         "return type 'RebarContainerItem' directly names a Revit DB object type"
 80638|       ],
 80639|       "source_url": "https://www.revitapidocs.com/2025/292aade1-0459-1a6a-9bd3-715e8bb634df.htm",
 80640|       "dll_signature_verified": true,
 80641|       "dll_relationship_scope": "declared",
 80642|       "dll_semantic_verified": null,
 80643|       "dll_verified_status": "signature_verified_declared",
 80644|       "revitlookup_referenced": null,
 80645|       "revitlookup_requires_document_context": null
 80646|     },
 80647|     {
 80648|       "source": "Autodesk.Revit.DB.Structure.RebarContainer",
 80649|       "target": null,
 80650|       "member_name": "GetHostId",
 80651|       "member_kind": "method",
 80652|       "edge_type": "HOSTED_BY",
 80653|       "confidence": "elementid_with_strong_name",
 80654|       "confidence_tier": "core",
 80655|       "target_resolution": "none",
 80656|       "evidence": [
 80657|         "member name 'GetHostId' matches keyword pattern /^GetHosted|Host/"
 80658|       ],
 80659|       "source_url": "https://www.revitapidocs.com/2025/da5694c7-b1bb-6155-47f9-93b0b61776ea.htm",
 80660|       "dll_signature_verified": true,
 80661|       "dll_relationship_scope": "declared",
 80662|       "dll_semantic_verified": null,
 80663|       "dll_verified_status": "signature_verified_declared",
 80664|       "revitlookup_referenced": null,
 80665|       "revitlookup_requires_document_context": null
 80666|     },
 80667|     {
 80668|       "source": "Autodesk.Revit.DB.Structure.RebarContainer",
 80669|       "target": "Autodesk.Revit.DB.Structure.RebarContainerItem",
 80670|       "member_name": "GetItem",
 80671|       "member_kind": "method",
 80672|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80673|       "confidence": "direct_return_type",
 80674|       "confidence_tier": "unverified_reference",
 80675|       "target_resolution": "short_name_fallback",
 80676|       "evidence": [
 80677|         "return type 'RebarContainerItem' directly names a Revit DB object type"
 80678|       ],
 80679|       "source_url": "https://www.revitapidocs.com/2025/d591f494-7c8c-a8ef-1a6a-31dc8dbc2ee4.htm",
 80680|       "dll_signature_verified": true,
 80681|       "dll_relationship_scope": "declared",
 80682|       "dll_semantic_verified": null,
 80683|       "dll_verified_status": "signature_verified_declared",
 80684|       "revitlookup_referenced": null,
 80685|       "revitlookup_requires_document_context": null
 80686|     },
 80687|     {
 80688|       "source": "Autodesk.Revit.DB.Structure.RebarContainer",
 80689|       "target": "Autodesk.Revit.DB.Structure.RebarContainerParameterManager",
 80690|       "member_name": "GetParametersManager",
 80691|       "member_kind": "method",
 80692|       "edge_type": "HAS_PARAMETER",
 80693|       "confidence": "direct_return_type",
 80694|       "confidence_tier": "core",
 80695|       "target_resolution": "short_name_fallback",
 80696|       "evidence": [
 80697|         "return type 'RebarContainerParameterManager' directly names a Revit DB object type"
 80698|       ],
 80699|       "source_url": "https://www.revitapidocs.com/2025/276727c8-b3fc-d31c-a7f9-bac1871a20b1.htm",
 80700|       "dll_signature_verified": true,
 80701|       "dll_relationship_scope": "declared",
 80702|       "dll_semantic_verified": null,
 80703|       "dll_verified_status": "signature_verified_declared",
 80704|       "revitlookup_referenced": null,
 80705|       "revitlookup_requires_document_context": null
 80706|     },
 80707|     {
 80708|       "source": "Autodesk.Revit.DB.Structure.RebarContainer",
 80709|       "target": "Autodesk.Revit.DB.Structure.RebarContainerIterator",
 80710|       "member_name": "GetRebarContainerIterator",
 80711|       "member_kind": "method",
 80712|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80713|       "confidence": "direct_return_type",
 80714|       "confidence_tier": "unverified_reference",
 80715|       "target_resolution": "short_name_fallback",
 80716|       "evidence": [
 80717|         "return type 'RebarContainerIterator' directly names a Revit DB object type"
 80718|       ],
 80719|       "source_url": "https://www.revitapidocs.com/2025/e3368385-4e90-a01e-9335-18d0bdc75790.htm",
 80720|       "dll_signature_verified": true,
 80721|       "dll_relationship_scope": "declared",
 80722|       "dll_semantic_verified": null,
 80723|       "dll_verified_status": "signature_verified_declared",
 80724|       "revitlookup_referenced": null,
 80725|       "revitlookup_requires_document_context": null
 80726|     },
 80727|     {
 80728|       "source": "Autodesk.Revit.DB.Structure.RebarContainer",
 80729|       "target": "Autodesk.Revit.DB.Structure.RebarRoundingManager",
 80730|       "member_name": "GetReinforcementRoundingManager",
 80731|       "member_kind": "method",
 80732|       "edge_type": "REFERENCES",
 80733|       "confidence": "direct_return_type",
 80734|       "confidence_tier": "core",
 80735|       "target_resolution": "short_name_fallback",
 80736|       "evidence": [
 80737|         "return type 'RebarRoundingManager' directly names a Revit DB object type"
 80738|       ],
 80739|       "source_url": "https://www.revitapidocs.com/2025/5149cdb5-f393-6b9e-e101-0fc3384d1236.htm",
 80740|       "dll_signature_verified": true,
```
