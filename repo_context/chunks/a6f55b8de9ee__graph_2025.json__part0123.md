# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 123 of 216
- Original line range: 47581-47980
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 47581|     {
 47582|       "source": "Autodesk.Revit.DB.GeometryInstance",
 47583|       "target": "Autodesk.Revit.DB.SymbolGeometryId",
 47584|       "member_name": "GetSymbolGeometryId",
 47585|       "member_kind": "method",
 47586|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 47587|       "confidence": "direct_return_type",
 47588|       "confidence_tier": "unverified_reference",
 47589|       "target_resolution": "exact",
 47590|       "evidence": [
 47591|         "return type 'SymbolGeometryId' directly names a Revit DB object type"
 47592|       ],
 47593|       "source_url": "https://www.revitapidocs.com/2025/28708c47-a358-41b3-d754-dda20f01ac6c.htm",
 47594|       "dll_signature_verified": true,
 47595|       "dll_relationship_scope": "declared",
 47596|       "dll_semantic_verified": null,
 47597|       "dll_verified_status": "signature_verified_declared",
 47598|       "revitlookup_referenced": null,
 47599|       "revitlookup_requires_document_context": null
 47600|     },
 47601|     {
 47602|       "source": "Autodesk.Revit.DB.GeometryObject",
 47603|       "target": "Autodesk.Revit.DB.GraphicsStyle",
 47604|       "member_name": "GraphicsStyleId",
 47605|       "member_kind": "property",
 47606|       "edge_type": "REFERENCES",
 47607|       "confidence": "elementid_with_strong_name",
 47608|       "confidence_tier": "core",
 47609|       "target_resolution": "exact",
 47610|       "evidence": [
 47611|         "member name 'GraphicsStyleId' matches keyword pattern /GraphicsStyle/"
 47612|       ],
 47613|       "source_url": "https://www.revitapidocs.com/2025/4103f148-957e-3f44-9ccd-a5ed6702c689.htm",
 47614|       "dll_signature_verified": true,
 47615|       "dll_relationship_scope": "declared",
 47616|       "dll_semantic_verified": null,
 47617|       "dll_verified_status": "signature_verified_declared",
 47618|       "revitlookup_referenced": null,
 47619|       "revitlookup_requires_document_context": null
 47620|     },
 47621|     {
 47622|       "source": "Autodesk.Revit.DB.GlobalParameter",
 47623|       "target": null,
 47624|       "member_name": "GetAffectedElements",
 47625|       "member_kind": "method",
 47626|       "edge_type": "RETURNS_ELEMENT_IDS",
 47627|       "confidence": "unknown_reference",
 47628|       "confidence_tier": "unverified_reference",
 47629|       "target_resolution": "none",
 47630|       "evidence": [
 47631|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 47632|       ],
 47633|       "source_url": "https://www.revitapidocs.com/2025/c1eb340d-471d-4810-92fe-a2bd6374fc1f.htm",
 47634|       "dll_signature_verified": true,
 47635|       "dll_relationship_scope": "declared",
 47636|       "dll_semantic_verified": null,
 47637|       "dll_verified_status": "signature_verified_declared",
 47638|       "revitlookup_referenced": null,
 47639|       "revitlookup_requires_document_context": null
 47640|     },
 47641|     {
 47642|       "source": "Autodesk.Revit.DB.GlobalParameter",
 47643|       "target": null,
 47644|       "member_name": "GetAffectedGlobalParameters",
 47645|       "member_kind": "method",
 47646|       "edge_type": "HAS_PARAMETER",
 47647|       "confidence": "elementid_collection_with_strong_name",
 47648|       "confidence_tier": "core",
 47649|       "target_resolution": "none",
 47650|       "evidence": [
 47651|         "member name 'GetAffectedGlobalParameters' matches keyword pattern /Parameter/"
 47652|       ],
 47653|       "source_url": "https://www.revitapidocs.com/2025/2028f8a1-2691-e921-8a56-882b1e4080f3.htm",
 47654|       "dll_signature_verified": true,
 47655|       "dll_relationship_scope": "declared",
 47656|       "dll_semantic_verified": null,
 47657|       "dll_verified_status": "signature_verified_declared",
 47658|       "revitlookup_referenced": null,
 47659|       "revitlookup_requires_document_context": null
 47660|     },
 47661|     {
 47662|       "source": "Autodesk.Revit.DB.GlobalParameter",
 47663|       "target": null,
 47664|       "member_name": "GetLabeledDimensions",
 47665|       "member_kind": "method",
 47666|       "edge_type": "RETURNS_ELEMENT_IDS",
 47667|       "confidence": "unknown_reference",
 47668|       "confidence_tier": "unverified_reference",
 47669|       "target_resolution": "none",
 47670|       "evidence": [
 47671|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 47672|       ],
 47673|       "source_url": "https://www.revitapidocs.com/2025/97d29291-74c4-2da5-2ac5-2fa0c0ac9d0c.htm",
 47674|       "dll_signature_verified": true,
 47675|       "dll_relationship_scope": "declared",
 47676|       "dll_semantic_verified": null,
 47677|       "dll_verified_status": "signature_verified_declared",
 47678|       "revitlookup_referenced": null,
 47679|       "revitlookup_requires_document_context": null
 47680|     },
 47681|     {
 47682|       "source": "Autodesk.Revit.DB.GlobalParameter",
 47683|       "target": "Autodesk.Revit.DB.ParameterValue",
 47684|       "member_name": "GetValue",
 47685|       "member_kind": "method",
 47686|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 47687|       "confidence": "direct_return_type",
 47688|       "confidence_tier": "unverified_reference",
 47689|       "target_resolution": "exact",
 47690|       "evidence": [
 47691|         "return type 'ParameterValue' directly names a Revit DB object type"
 47692|       ],
 47693|       "source_url": "https://www.revitapidocs.com/2025/56eb0e54-eac4-9b51-3122-e4fb065b63f0.htm",
 47694|       "dll_signature_verified": true,
 47695|       "dll_relationship_scope": "declared",
 47696|       "dll_semantic_verified": null,
 47697|       "dll_verified_status": "signature_verified_declared",
 47698|       "revitlookup_referenced": null,
 47699|       "revitlookup_requires_document_context": null
 47700|     },
 47701|     {
 47702|       "source": "Autodesk.Revit.DB.GlobalParametersManager",
 47703|       "target": null,
 47704|       "member_name": "AreGlobalParametersAllowed",
 47705|       "member_kind": "method",
 47706|       "edge_type": "HAS_PARAMETER",
 47707|       "confidence": "name_only_candidate",
 47708|       "confidence_tier": "likely",
 47709|       "target_resolution": "none",
 47710|       "evidence": [
 47711|         "member name 'AreGlobalParametersAllowed' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 47712|       ],
 47713|       "source_url": "https://www.revitapidocs.com/2025/0191434b-d8c8-ed25-c81b-2679e8201460.htm",
 47714|       "dll_signature_verified": true,
 47715|       "dll_relationship_scope": "declared",
 47716|       "dll_semantic_verified": null,
 47717|       "dll_verified_status": "signature_verified_declared",
 47718|       "revitlookup_referenced": null,
 47719|       "revitlookup_requires_document_context": null
 47720|     },
 47721|     {
 47722|       "source": "Autodesk.Revit.DB.GlobalParametersManager",
 47723|       "target": null,
 47724|       "member_name": "FindByName",
 47725|       "member_kind": "method",
 47726|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 47727|       "confidence": "unknown_reference",
 47728|       "confidence_tier": "unverified_reference",
 47729|       "target_resolution": "none",
 47730|       "evidence": [
 47731|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 47732|       ],
 47733|       "source_url": "https://www.revitapidocs.com/2025/7c7a7bd3-18e8-d9be-d9a7-66cd9ecdccc7.htm",
 47734|       "dll_signature_verified": true,
 47735|       "dll_relationship_scope": "declared",
 47736|       "dll_semantic_verified": null,
 47737|       "dll_verified_status": "signature_verified_declared",
 47738|       "revitlookup_referenced": null,
 47739|       "revitlookup_requires_document_context": null
 47740|     },
 47741|     {
 47742|       "source": "Autodesk.Revit.DB.GlobalParametersManager",
 47743|       "target": null,
 47744|       "member_name": "GetAllGlobalParameters",
 47745|       "member_kind": "method",
 47746|       "edge_type": "HAS_PARAMETER",
 47747|       "confidence": "elementid_collection_with_strong_name",
 47748|       "confidence_tier": "core",
 47749|       "target_resolution": "none",
 47750|       "evidence": [
 47751|         "member name 'GetAllGlobalParameters' matches keyword pattern /Parameter/"
 47752|       ],
 47753|       "source_url": "https://www.revitapidocs.com/2025/62b46073-1a11-0cc8-1798-8d6d87719888.htm",
 47754|       "dll_signature_verified": true,
 47755|       "dll_relationship_scope": "declared",
 47756|       "dll_semantic_verified": null,
 47757|       "dll_verified_status": "signature_verified_declared",
 47758|       "revitlookup_referenced": null,
 47759|       "revitlookup_requires_document_context": null
 47760|     },
 47761|     {
 47762|       "source": "Autodesk.Revit.DB.GlobalParametersManager",
 47763|       "target": null,
 47764|       "member_name": "GetGlobalParametersOrdered",
 47765|       "member_kind": "method",
 47766|       "edge_type": "HAS_PARAMETER",
 47767|       "confidence": "elementid_collection_with_strong_name",
 47768|       "confidence_tier": "core",
 47769|       "target_resolution": "none",
 47770|       "evidence": [
 47771|         "member name 'GetGlobalParametersOrdered' matches keyword pattern /Parameter/"
 47772|       ],
 47773|       "source_url": "https://www.revitapidocs.com/2025/e899f971-6c97-45e7-ac6d-cdac810e08e8.htm",
 47774|       "dll_signature_verified": true,
 47775|       "dll_relationship_scope": "declared",
 47776|       "dll_semantic_verified": null,
 47777|       "dll_verified_status": "signature_verified_declared",
 47778|       "revitlookup_referenced": null,
 47779|       "revitlookup_requires_document_context": null
 47780|     },
 47781|     {
 47782|       "source": "Autodesk.Revit.DB.GlobalParametersManager",
 47783|       "target": null,
 47784|       "member_name": "IsValidGlobalParameter",
 47785|       "member_kind": "method",
 47786|       "edge_type": "HAS_PARAMETER",
 47787|       "confidence": "name_only_candidate",
 47788|       "confidence_tier": "likely",
 47789|       "target_resolution": "none",
 47790|       "evidence": [
 47791|         "member name 'IsValidGlobalParameter' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 47792|       ],
 47793|       "source_url": "https://www.revitapidocs.com/2025/fe14085f-5643-db65-6cd7-05773be33c3b.htm",
 47794|       "dll_signature_verified": true,
 47795|       "dll_relationship_scope": "declared",
 47796|       "dll_semantic_verified": null,
 47797|       "dll_verified_status": "signature_verified_declared",
 47798|       "revitlookup_referenced": null,
 47799|       "revitlookup_requires_document_context": null
 47800|     },
 47801|     {
 47802|       "source": "Autodesk.Revit.DB.GlobalParametersManager",
 47803|       "target": null,
 47804|       "member_name": "MoveParameterDownOrder",
 47805|       "member_kind": "method",
 47806|       "edge_type": "HAS_PARAMETER",
 47807|       "confidence": "name_only_candidate",
 47808|       "confidence_tier": "likely",
 47809|       "target_resolution": "none",
 47810|       "evidence": [
 47811|         "member name 'MoveParameterDownOrder' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 47812|       ],
 47813|       "source_url": "https://www.revitapidocs.com/2025/ff6d35ee-db72-544c-033c-c8372842ebd0.htm",
 47814|       "dll_signature_verified": true,
 47815|       "dll_relationship_scope": "declared",
 47816|       "dll_semantic_verified": null,
 47817|       "dll_verified_status": "signature_verified_declared",
 47818|       "revitlookup_referenced": null,
 47819|       "revitlookup_requires_document_context": null
 47820|     },
 47821|     {
 47822|       "source": "Autodesk.Revit.DB.GlobalParametersManager",
 47823|       "target": null,
 47824|       "member_name": "MoveParameterUpOrder",
 47825|       "member_kind": "method",
 47826|       "edge_type": "HAS_PARAMETER",
 47827|       "confidence": "name_only_candidate",
 47828|       "confidence_tier": "likely",
 47829|       "target_resolution": "none",
 47830|       "evidence": [
 47831|         "member name 'MoveParameterUpOrder' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 47832|       ],
 47833|       "source_url": "https://www.revitapidocs.com/2025/b347d8cf-9b21-b6d1-8309-d13f6ac7bcea.htm",
 47834|       "dll_signature_verified": true,
 47835|       "dll_relationship_scope": "declared",
 47836|       "dll_semantic_verified": null,
 47837|       "dll_verified_status": "signature_verified_declared",
 47838|       "revitlookup_referenced": null,
 47839|       "revitlookup_requires_document_context": null
 47840|     },
 47841|     {
 47842|       "source": "Autodesk.Revit.DB.GlobalParametersManager",
 47843|       "target": null,
 47844|       "member_name": "SortParameters",
 47845|       "member_kind": "method",
 47846|       "edge_type": "HAS_PARAMETER",
 47847|       "confidence": "name_only_candidate",
 47848|       "confidence_tier": "likely",
 47849|       "target_resolution": "none",
 47850|       "evidence": [
 47851|         "member name 'SortParameters' matches keyword pattern /Parameter/ but return type 'void' gives no type-level confirmation"
 47852|       ],
 47853|       "source_url": "https://www.revitapidocs.com/2025/fe58ca0b-7002-3162-0f7f-ceaa85baea99.htm",
 47854|       "dll_signature_verified": true,
 47855|       "dll_relationship_scope": "declared",
 47856|       "dll_semantic_verified": null,
 47857|       "dll_verified_status": "signature_verified_declared",
 47858|       "revitlookup_referenced": null,
 47859|       "revitlookup_requires_document_context": null
 47860|     },
 47861|     {
 47862|       "source": "Autodesk.Revit.DB.GraphicsStyle",
 47863|       "target": "Autodesk.Revit.DB.Category",
 47864|       "member_name": "GraphicsStyleCategory",
 47865|       "member_kind": "property",
 47866|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 47867|       "confidence": "direct_return_type",
 47868|       "confidence_tier": "unverified_reference",
 47869|       "target_resolution": "exact",
 47870|       "evidence": [
 47871|         "member name 'GraphicsStyleCategory' matches keyword pattern /GraphicsStyle/ implying target 'GraphicsStyle', but the actual return type 'Category' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 47872|         "return type 'Category' directly names a Revit DB object type"
 47873|       ],
 47874|       "source_url": "https://www.revitapidocs.com/2025/a46641a8-932b-7cf0-5b52-52b832aa4dff.htm",
 47875|       "dll_signature_verified": true,
 47876|       "dll_relationship_scope": "declared",
 47877|       "dll_semantic_verified": null,
 47878|       "dll_verified_status": "signature_verified_declared",
 47879|       "revitlookup_referenced": null,
 47880|       "revitlookup_requires_document_context": null
 47881|     },
 47882|     {
 47883|       "source": "Autodesk.Revit.DB.GraphicsStyle",
 47884|       "target": "Autodesk.Revit.DB.GraphicsStyle",
 47885|       "member_name": "GraphicsStyleType",
 47886|       "member_kind": "property",
 47887|       "edge_type": "REFERENCES",
 47888|       "confidence": "name_only_candidate",
 47889|       "confidence_tier": "likely",
 47890|       "target_resolution": "exact",
 47891|       "evidence": [
 47892|         "member name 'GraphicsStyleType' matches keyword pattern /GraphicsStyle/ but return type 'GraphicsStyleType' gives no type-level confirmation"
 47893|       ],
 47894|       "source_url": "https://www.revitapidocs.com/2025/6f969a1c-6691-0d5b-ab9a-c6ab869b7c99.htm",
 47895|       "dll_signature_verified": true,
 47896|       "dll_relationship_scope": "declared",
 47897|       "dll_semantic_verified": null,
 47898|       "dll_verified_status": "signature_verified_declared",
 47899|       "revitlookup_referenced": null,
 47900|       "revitlookup_requires_document_context": null
 47901|     },
 47902|     {
 47903|       "source": "Autodesk.Revit.DB.Group",
 47904|       "target": null,
 47905|       "member_name": "AttachedParentId",
 47906|       "member_kind": "property",
 47907|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 47908|       "confidence": "unknown_reference",
 47909|       "confidence_tier": "unverified_reference",
 47910|       "target_resolution": "none",
 47911|       "evidence": [
 47912|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 47913|       ],
 47914|       "source_url": "https://www.revitapidocs.com/2025/52a0655a-26fc-7f2b-17b1-5d48c75dea70.htm",
 47915|       "dll_signature_verified": true,
 47916|       "dll_relationship_scope": "declared",
 47917|       "dll_semantic_verified": null,
 47918|       "dll_verified_status": "signature_verified_declared",
 47919|       "revitlookup_referenced": null,
 47920|       "revitlookup_requires_document_context": null
 47921|     },
 47922|     {
 47923|       "source": "Autodesk.Revit.DB.Group",
 47924|       "target": "Autodesk.Revit.DB.GroupType",
 47925|       "member_name": "GroupType",
 47926|       "member_kind": "property",
 47927|       "edge_type": "MEMBER_OF_GROUP",
 47928|       "confidence": "direct_return_type",
 47929|       "confidence_tier": "core",
 47930|       "target_resolution": "exact",
 47931|       "evidence": [
 47932|         "return type 'GroupType' directly names a Revit DB object type"
 47933|       ],
 47934|       "source_url": "https://www.revitapidocs.com/2025/d77ac466-dc1f-90cb-0b5f-d23fe88065d6.htm",
 47935|       "dll_signature_verified": true,
 47936|       "dll_relationship_scope": "declared",
 47937|       "dll_semantic_verified": null,
 47938|       "dll_verified_status": "signature_verified_declared",
 47939|       "revitlookup_referenced": null,
 47940|       "revitlookup_requires_document_context": null
 47941|     },
 47942|     {
 47943|       "source": "Autodesk.Revit.DB.Group",
 47944|       "target": "Autodesk.Revit.DB.Location",
 47945|       "member_name": "Location",
 47946|       "member_kind": "property",
 47947|       "edge_type": "REFERENCES",
 47948|       "confidence": "direct_return_type",
 47949|       "confidence_tier": "core",
 47950|       "target_resolution": "exact",
 47951|       "evidence": [
 47952|         "return type 'Location' directly names a Revit DB object type"
 47953|       ],
 47954|       "source_url": "https://www.revitapidocs.com/2025/446dcaea-8792-b703-e0c2-e9e30cbadfd3.htm",
 47955|       "dll_signature_verified": true,
 47956|       "dll_relationship_scope": "declared",
 47957|       "dll_semantic_verified": null,
 47958|       "dll_verified_status": "signature_verified_declared",
 47959|       "revitlookup_referenced": null,
 47960|       "revitlookup_requires_document_context": null
 47961|     },
 47962|     {
 47963|       "source": "Autodesk.Revit.DB.Group",
 47964|       "target": null,
 47965|       "member_name": "GetAvailableAttachedDetailGroupTypeIds",
 47966|       "member_kind": "method",
 47967|       "edge_type": "MEMBER_OF_GROUP",
 47968|       "confidence": "elementid_collection_with_strong_name",
 47969|       "confidence_tier": "core",
 47970|       "target_resolution": "none",
 47971|       "evidence": [
 47972|         "member name 'GetAvailableAttachedDetailGroupTypeIds' matches keyword pattern /^GetMember|Group/"
 47973|       ],
 47974|       "source_url": "https://www.revitapidocs.com/2025/dd127374-e2c5-9c5e-3edd-c1b0ec60e30d.htm",
 47975|       "dll_signature_verified": true,
 47976|       "dll_relationship_scope": "declared",
 47977|       "dll_semantic_verified": null,
 47978|       "dll_verified_status": "signature_verified_declared",
 47979|       "revitlookup_referenced": null,
 47980|       "revitlookup_requires_document_context": null
```
