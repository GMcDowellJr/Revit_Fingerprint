# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 95 of 216
- Original line range: 36661-37060
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 36661|       "evidence": [
 36662|         "return type 'Leader' directly names a Revit DB object type"
 36663|       ],
 36664|       "source_url": "https://www.revitapidocs.com/2025/0373eec6-5963-b036-e816-de4d93f2f5f1.htm",
 36665|       "dll_signature_verified": true,
 36666|       "dll_relationship_scope": "declared",
 36667|       "dll_semantic_verified": null,
 36668|       "dll_verified_status": "signature_verified_declared",
 36669|       "revitlookup_referenced": null,
 36670|       "revitlookup_requires_document_context": null
 36671|     },
 36672|     {
 36673|       "source": "Autodesk.Revit.DB.DatumPlane",
 36674|       "target": "Autodesk.Revit.DB.Leader",
 36675|       "member_name": "GetLeader",
 36676|       "member_kind": "method",
 36677|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36678|       "confidence": "direct_return_type",
 36679|       "confidence_tier": "unverified_reference",
 36680|       "target_resolution": "exact",
 36681|       "evidence": [
 36682|         "return type 'Leader' directly names a Revit DB object type"
 36683|       ],
 36684|       "source_url": "https://www.revitapidocs.com/2025/4f5ebf97-8a85-bd5b-0edd-f2f0f6b18cb2.htm",
 36685|       "dll_signature_verified": true,
 36686|       "dll_relationship_scope": "declared",
 36687|       "dll_semantic_verified": null,
 36688|       "dll_verified_status": "signature_verified_declared",
 36689|       "revitlookup_referenced": true,
 36690|       "revitlookup_requires_document_context": true
 36691|     },
 36692|     {
 36693|       "source": "Autodesk.Revit.DB.DatumPlane",
 36694|       "target": null,
 36695|       "member_name": "GetPropagationViews",
 36696|       "member_kind": "method",
 36697|       "edge_type": "RETURNS_ELEMENT_IDS",
 36698|       "confidence": "unknown_reference",
 36699|       "confidence_tier": "unverified_reference",
 36700|       "target_resolution": "none",
 36701|       "evidence": [
 36702|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 36703|       ],
 36704|       "source_url": "https://www.revitapidocs.com/2025/ecfed956-7434-0c33-6ab0-0bc80bd2a157.htm",
 36705|       "dll_signature_verified": true,
 36706|       "dll_relationship_scope": "declared",
 36707|       "dll_semantic_verified": null,
 36708|       "dll_verified_status": "signature_verified_declared",
 36709|       "revitlookup_referenced": true,
 36710|       "revitlookup_requires_document_context": false
 36711|     },
 36712|     {
 36713|       "source": "Autodesk.Revit.DB.DefinitionBindingMap",
 36714|       "target": "Autodesk.Revit.DB.DefinitionBindingMapIterator",
 36715|       "member_name": "ForwardIterator",
 36716|       "member_kind": "method",
 36717|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36718|       "confidence": "direct_return_type",
 36719|       "confidence_tier": "unverified_reference",
 36720|       "target_resolution": "exact",
 36721|       "evidence": [
 36722|         "return type 'DefinitionBindingMapIterator' directly names a Revit DB object type"
 36723|       ],
 36724|       "source_url": "https://www.revitapidocs.com/2025/6a74cb96-740e-1b30-fbc0-91e45202e797.htm",
 36725|       "dll_signature_verified": true,
 36726|       "dll_relationship_scope": "declared",
 36727|       "dll_semantic_verified": null,
 36728|       "dll_verified_status": "signature_verified_declared",
 36729|       "revitlookup_referenced": null,
 36730|       "revitlookup_requires_document_context": null
 36731|     },
 36732|     {
 36733|       "source": "Autodesk.Revit.DB.DefinitionBindingMap",
 36734|       "target": "Autodesk.Revit.DB.DefinitionBindingMapIterator",
 36735|       "member_name": "ReverseIterator",
 36736|       "member_kind": "method",
 36737|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36738|       "confidence": "direct_return_type",
 36739|       "confidence_tier": "unverified_reference",
 36740|       "target_resolution": "exact",
 36741|       "evidence": [
 36742|         "return type 'DefinitionBindingMapIterator' directly names a Revit DB object type"
 36743|       ],
 36744|       "source_url": "https://www.revitapidocs.com/2025/69ed6e9c-1ece-49b9-5cd2-5b68b3f72aed.htm",
 36745|       "dll_signature_verified": true,
 36746|       "dll_relationship_scope": "declared",
 36747|       "dll_semantic_verified": null,
 36748|       "dll_verified_status": "signature_verified_declared",
 36749|       "revitlookup_referenced": null,
 36750|       "revitlookup_requires_document_context": null
 36751|     },
 36752|     {
 36753|       "source": "Autodesk.Revit.DB.DefinitionBindingMapIterator",
 36754|       "target": "Autodesk.Revit.DB.Definition",
 36755|       "member_name": "Key",
 36756|       "member_kind": "property",
 36757|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36758|       "confidence": "direct_return_type",
 36759|       "confidence_tier": "unverified_reference",
 36760|       "target_resolution": "exact",
 36761|       "evidence": [
 36762|         "return type 'Definition' directly names a Revit DB object type"
 36763|       ],
 36764|       "source_url": "https://www.revitapidocs.com/2025/d94a5479-7f4b-d483-717c-fb9ec1bb2e63.htm",
 36765|       "dll_signature_verified": true,
 36766|       "dll_relationship_scope": "declared",
 36767|       "dll_semantic_verified": null,
 36768|       "dll_verified_status": "signature_verified_declared",
 36769|       "revitlookup_referenced": null,
 36770|       "revitlookup_requires_document_context": null
 36771|     },
 36772|     {
 36773|       "source": "Autodesk.Revit.DB.DefinitionFile",
 36774|       "target": "Autodesk.Revit.DB.DefinitionGroups",
 36775|       "member_name": "Groups",
 36776|       "member_kind": "property",
 36777|       "edge_type": "MEMBER_OF_GROUP",
 36778|       "confidence": "direct_return_type",
 36779|       "confidence_tier": "core",
 36780|       "target_resolution": "exact",
 36781|       "evidence": [
 36782|         "return type 'DefinitionGroups' directly names a Revit DB object type"
 36783|       ],
 36784|       "source_url": "https://www.revitapidocs.com/2025/fa89c916-101a-cf53-0920-5bfde4c17b5f.htm",
 36785|       "dll_signature_verified": true,
 36786|       "dll_relationship_scope": "declared",
 36787|       "dll_semantic_verified": null,
 36788|       "dll_verified_status": "signature_verified_declared",
 36789|       "revitlookup_referenced": null,
 36790|       "revitlookup_requires_document_context": null
 36791|     },
 36792|     {
 36793|       "source": "Autodesk.Revit.DB.DefinitionGroup",
 36794|       "target": "Autodesk.Revit.DB.Definitions",
 36795|       "member_name": "Definitions",
 36796|       "member_kind": "property",
 36797|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36798|       "confidence": "direct_return_type",
 36799|       "confidence_tier": "unverified_reference",
 36800|       "target_resolution": "exact",
 36801|       "evidence": [
 36802|         "return type 'Definitions' directly names a Revit DB object type"
 36803|       ],
 36804|       "source_url": "https://www.revitapidocs.com/2025/d26a47b8-aa3f-8f0e-d2ce-4e66e5be07b2.htm",
 36805|       "dll_signature_verified": true,
 36806|       "dll_relationship_scope": "declared",
 36807|       "dll_semantic_verified": null,
 36808|       "dll_verified_status": "signature_verified_declared",
 36809|       "revitlookup_referenced": null,
 36810|       "revitlookup_requires_document_context": null
 36811|     },
 36812|     {
 36813|       "source": "Autodesk.Revit.DB.DeleteWorksetSettings",
 36814|       "target": "Autodesk.Revit.DB.Workset",
 36815|       "member_name": "DeleteWorksetOption",
 36816|       "member_kind": "property",
 36817|       "edge_type": "OWNED_BY_WORKSET",
 36818|       "confidence": "name_only_candidate",
 36819|       "confidence_tier": "likely",
 36820|       "target_resolution": "exact",
 36821|       "evidence": [
 36822|         "member name 'DeleteWorksetOption' matches keyword pattern /Workset/ but return type 'DeleteWorksetOption' gives no type-level confirmation"
 36823|       ],
 36824|       "source_url": "https://www.revitapidocs.com/2025/087c5fa5-6b5d-19a6-e3bb-6ede5e7bbe22.htm",
 36825|       "dll_signature_verified": true,
 36826|       "dll_relationship_scope": "declared",
 36827|       "dll_semantic_verified": null,
 36828|       "dll_verified_status": "signature_verified_declared",
 36829|       "revitlookup_referenced": null,
 36830|       "revitlookup_requires_document_context": null
 36831|     },
 36832|     {
 36833|       "source": "Autodesk.Revit.DB.DeleteWorksetSettings",
 36834|       "target": "Autodesk.Revit.DB.Workset",
 36835|       "member_name": "WorksetId",
 36836|       "member_kind": "property",
 36837|       "edge_type": "OWNED_BY_WORKSET",
 36838|       "confidence": "direct_return_type",
 36839|       "confidence_tier": "core",
 36840|       "target_resolution": "exact",
 36841|       "evidence": [
 36842|         "return type 'WorksetId' is a typed identifier that unambiguously names its own target ('Workset') through the type system alone"
 36843|       ],
 36844|       "source_url": "https://www.revitapidocs.com/2025/93d2f9b1-d9b0-0ce1-66e0-b9d464be9074.htm",
 36845|       "dll_signature_verified": true,
 36846|       "dll_relationship_scope": "declared",
 36847|       "dll_semantic_verified": null,
 36848|       "dll_verified_status": "signature_verified_declared",
 36849|       "revitlookup_referenced": null,
 36850|       "revitlookup_requires_document_context": null
 36851|     },
 36852|     {
 36853|       "source": "Autodesk.Revit.DB.DesignOption",
 36854|       "target": "Autodesk.Revit.DB.DesignOption",
 36855|       "member_name": "GetActiveDesignOptionId",
 36856|       "member_kind": "method",
 36857|       "edge_type": "ASSIGNED_TO_DESIGN_OPTION",
 36858|       "confidence": "elementid_with_strong_name",
 36859|       "confidence_tier": "core",
 36860|       "target_resolution": "exact",
 36861|       "evidence": [
 36862|         "member name 'GetActiveDesignOptionId' matches keyword pattern /DesignOption/"
 36863|       ],
 36864|       "source_url": "https://www.revitapidocs.com/2025/d0b47e58-a5fc-9424-a94c-2428b4ec4d16.htm",
 36865|       "dll_signature_verified": true,
 36866|       "dll_relationship_scope": "declared",
 36867|       "dll_semantic_verified": null,
 36868|       "dll_verified_status": "signature_verified_declared",
 36869|       "revitlookup_referenced": null,
 36870|       "revitlookup_requires_document_context": null
 36871|     },
 36872|     {
 36873|       "source": "Autodesk.Revit.DB.DetailCurveArray",
 36874|       "target": "Autodesk.Revit.DB.DetailCurveArrayIterator",
 36875|       "member_name": "ForwardIterator",
 36876|       "member_kind": "method",
 36877|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36878|       "confidence": "direct_return_type",
 36879|       "confidence_tier": "unverified_reference",
 36880|       "target_resolution": "exact",
 36881|       "evidence": [
 36882|         "return type 'DetailCurveArrayIterator' directly names a Revit DB object type"
 36883|       ],
 36884|       "source_url": "https://www.revitapidocs.com/2025/93cc0950-151f-1c14-776a-5b8a718e1fd4.htm",
 36885|       "dll_signature_verified": true,
 36886|       "dll_relationship_scope": "declared",
 36887|       "dll_semantic_verified": null,
 36888|       "dll_verified_status": "signature_verified_declared",
 36889|       "revitlookup_referenced": null,
 36890|       "revitlookup_requires_document_context": null
 36891|     },
 36892|     {
 36893|       "source": "Autodesk.Revit.DB.DetailCurveArray",
 36894|       "target": "Autodesk.Revit.DB.DetailCurveArrayIterator",
 36895|       "member_name": "ReverseIterator",
 36896|       "member_kind": "method",
 36897|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36898|       "confidence": "direct_return_type",
 36899|       "confidence_tier": "unverified_reference",
 36900|       "target_resolution": "exact",
 36901|       "evidence": [
 36902|         "return type 'DetailCurveArrayIterator' directly names a Revit DB object type"
 36903|       ],
 36904|       "source_url": "https://www.revitapidocs.com/2025/5327e841-342c-12e5-972c-98b0d6a1bf4b.htm",
 36905|       "dll_signature_verified": true,
 36906|       "dll_relationship_scope": "declared",
 36907|       "dll_semantic_verified": null,
 36908|       "dll_verified_status": "signature_verified_declared",
 36909|       "revitlookup_referenced": null,
 36910|       "revitlookup_requires_document_context": null
 36911|     },
 36912|     {
 36913|       "source": "Autodesk.Revit.DB.DetailElementOrderUtils",
 36914|       "target": null,
 36915|       "member_name": "GetDrawOrderForDetails",
 36916|       "member_kind": "method",
 36917|       "edge_type": "RETURNS_ELEMENT_IDS",
 36918|       "confidence": "unknown_reference",
 36919|       "confidence_tier": "unverified_reference",
 36920|       "target_resolution": "none",
 36921|       "evidence": [
 36922|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 36923|       ],
 36924|       "source_url": "https://www.revitapidocs.com/2025/686020d6-9ca3-c51f-47fc-a54438e3f608.htm",
 36925|       "dll_signature_verified": true,
 36926|       "dll_relationship_scope": "declared",
 36927|       "dll_semantic_verified": null,
 36928|       "dll_verified_status": "signature_verified_declared",
 36929|       "revitlookup_referenced": null,
 36930|       "revitlookup_requires_document_context": null
 36931|     },
 36932|     {
 36933|       "source": "Autodesk.Revit.DB.DGNExportOptions",
 36934|       "target": "Autodesk.Revit.DB.ExportLineweightTable",
 36935|       "member_name": "GetExportLineweightTable",
 36936|       "member_kind": "method",
 36937|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36938|       "confidence": "direct_return_type",
 36939|       "confidence_tier": "unverified_reference",
 36940|       "target_resolution": "exact",
 36941|       "evidence": [
 36942|         "return type 'ExportLineweightTable' directly names a Revit DB object type"
 36943|       ],
 36944|       "source_url": "https://www.revitapidocs.com/2025/4be0abc7-0033-3f99-52c3-2e407cbd8fa0.htm",
 36945|       "dll_signature_verified": true,
 36946|       "dll_relationship_scope": "declared",
 36947|       "dll_semantic_verified": null,
 36948|       "dll_verified_status": "signature_verified_declared",
 36949|       "revitlookup_referenced": null,
 36950|       "revitlookup_requires_document_context": null
 36951|     },
 36952|     {
 36953|       "source": "Autodesk.Revit.DB.Dimension",
 36954|       "target": "Autodesk.Revit.DB.DimensionType",
 36955|       "member_name": "DimensionType",
 36956|       "member_kind": "property",
 36957|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36958|       "confidence": "direct_return_type",
 36959|       "confidence_tier": "unverified_reference",
 36960|       "target_resolution": "exact",
 36961|       "evidence": [
 36962|         "return type 'DimensionType' directly names a Revit DB object type"
 36963|       ],
 36964|       "source_url": "https://www.revitapidocs.com/2025/5f982cf1-cc62-8bb5-5a31-5061b97b6ddc.htm",
 36965|       "dll_signature_verified": true,
 36966|       "dll_relationship_scope": "declared",
 36967|       "dll_semantic_verified": null,
 36968|       "dll_verified_status": "signature_verified_declared",
 36969|       "revitlookup_referenced": null,
 36970|       "revitlookup_requires_document_context": null
 36971|     },
 36972|     {
 36973|       "source": "Autodesk.Revit.DB.Dimension",
 36974|       "target": "Autodesk.Revit.DB.FamilyParameter",
 36975|       "member_name": "FamilyLabel",
 36976|       "member_kind": "property",
 36977|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36978|       "confidence": "direct_return_type",
 36979|       "confidence_tier": "unverified_reference",
 36980|       "target_resolution": "exact",
 36981|       "evidence": [
 36982|         "return type 'FamilyParameter' directly names a Revit DB object type"
 36983|       ],
 36984|       "source_url": "https://www.revitapidocs.com/2025/cd34752d-ff4f-e6f7-05e5-82405af00e52.htm",
 36985|       "dll_signature_verified": true,
 36986|       "dll_relationship_scope": "declared",
 36987|       "dll_semantic_verified": null,
 36988|       "dll_verified_status": "signature_verified_declared",
 36989|       "revitlookup_referenced": null,
 36990|       "revitlookup_requires_document_context": null
 36991|     },
 36992|     {
 36993|       "source": "Autodesk.Revit.DB.Dimension",
 36994|       "target": "Autodesk.Revit.DB.ReferenceArray",
 36995|       "member_name": "References",
 36996|       "member_kind": "property",
 36997|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 36998|       "confidence": "direct_return_type",
 36999|       "confidence_tier": "unverified_reference",
 37000|       "target_resolution": "exact",
 37001|       "evidence": [
 37002|         "return type 'ReferenceArray' directly names a Revit DB object type"
 37003|       ],
 37004|       "source_url": "https://www.revitapidocs.com/2025/fc3bc889-b274-3262-a126-849df2af9019.htm",
 37005|       "dll_signature_verified": true,
 37006|       "dll_relationship_scope": "declared",
 37007|       "dll_semantic_verified": null,
 37008|       "dll_verified_status": "signature_verified_declared",
 37009|       "revitlookup_referenced": null,
 37010|       "revitlookup_requires_document_context": null
 37011|     },
 37012|     {
 37013|       "source": "Autodesk.Revit.DB.Dimension",
 37014|       "target": "Autodesk.Revit.DB.DimensionSegmentArray",
 37015|       "member_name": "Segments",
 37016|       "member_kind": "property",
 37017|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37018|       "confidence": "direct_return_type",
 37019|       "confidence_tier": "unverified_reference",
 37020|       "target_resolution": "exact",
 37021|       "evidence": [
 37022|         "return type 'DimensionSegmentArray' directly names a Revit DB object type"
 37023|       ],
 37024|       "source_url": "https://www.revitapidocs.com/2025/d7fcdab2-ca81-0ed1-4813-f7aa092430d7.htm",
 37025|       "dll_signature_verified": true,
 37026|       "dll_relationship_scope": "declared",
 37027|       "dll_semantic_verified": null,
 37028|       "dll_verified_status": "signature_verified_declared",
 37029|       "revitlookup_referenced": null,
 37030|       "revitlookup_requires_document_context": null
 37031|     },
 37032|     {
 37033|       "source": "Autodesk.Revit.DB.Dimension",
 37034|       "target": "Autodesk.Revit.DB.View",
 37035|       "member_name": "View",
 37036|       "member_kind": "property",
 37037|       "edge_type": "REFERENCES",
 37038|       "confidence": "direct_return_type",
 37039|       "confidence_tier": "core",
 37040|       "target_resolution": "exact",
 37041|       "evidence": [
 37042|         "return type 'View' directly names a Revit DB object type"
 37043|       ],
 37044|       "source_url": "https://www.revitapidocs.com/2025/8520fd39-b766-2f43-7937-1ffb4e0bc59a.htm",
 37045|       "dll_signature_verified": true,
 37046|       "dll_relationship_scope": "declared",
 37047|       "dll_semantic_verified": null,
 37048|       "dll_verified_status": "signature_verified_declared",
 37049|       "revitlookup_referenced": null,
 37050|       "revitlookup_requires_document_context": null
 37051|     },
 37052|     {
 37053|       "source": "Autodesk.Revit.DB.DimensionSegmentArray",
 37054|       "target": "Autodesk.Revit.DB.DimensionSegmentArrayIterator",
 37055|       "member_name": "ForwardIterator",
 37056|       "member_kind": "method",
 37057|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37058|       "confidence": "direct_return_type",
 37059|       "confidence_tier": "unverified_reference",
 37060|       "target_resolution": "exact",
```
