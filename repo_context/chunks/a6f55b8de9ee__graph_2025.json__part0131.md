# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 131 of 216
- Original line range: 50701-51100
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 50701|       "dll_verified_status": "signature_verified_declared",
 50702|       "revitlookup_referenced": null,
 50703|       "revitlookup_requires_document_context": null
 50704|     },
 50705|     {
 50706|       "source": "Autodesk.Revit.DB.Material",
 50707|       "target": "Autodesk.Revit.DB.Material",
 50708|       "member_name": "ClearMaterialAspect",
 50709|       "member_kind": "method",
 50710|       "edge_type": "USES_MATERIAL",
 50711|       "confidence": "name_only_candidate",
 50712|       "confidence_tier": "likely",
 50713|       "target_resolution": "exact",
 50714|       "evidence": [
 50715|         "member name 'ClearMaterialAspect' matches keyword pattern /Material/ but return type 'void' gives no type-level confirmation"
 50716|       ],
 50717|       "source_url": "https://www.revitapidocs.com/2025/f5ff587a-d617-28e1-c277-1f73ccbe7e3a.htm",
 50718|       "dll_signature_verified": true,
 50719|       "dll_relationship_scope": "declared",
 50720|       "dll_semantic_verified": null,
 50721|       "dll_verified_status": "signature_verified_declared",
 50722|       "revitlookup_referenced": null,
 50723|       "revitlookup_requires_document_context": null
 50724|     },
 50725|     {
 50726|       "source": "Autodesk.Revit.DB.Material",
 50727|       "target": "Autodesk.Revit.DB.Material",
 50728|       "member_name": "IsMaterialOrValidDefault",
 50729|       "member_kind": "method",
 50730|       "edge_type": "USES_MATERIAL",
 50731|       "confidence": "name_only_candidate",
 50732|       "confidence_tier": "likely",
 50733|       "target_resolution": "exact",
 50734|       "evidence": [
 50735|         "member name 'IsMaterialOrValidDefault' matches keyword pattern /Material/ but return type 'bool' gives no type-level confirmation"
 50736|       ],
 50737|       "source_url": "https://www.revitapidocs.com/2025/5337f779-e4d7-4f61-0ed5-df974b111543.htm",
 50738|       "dll_signature_verified": true,
 50739|       "dll_relationship_scope": "declared",
 50740|       "dll_semantic_verified": null,
 50741|       "dll_verified_status": "signature_verified_declared",
 50742|       "revitlookup_referenced": null,
 50743|       "revitlookup_requires_document_context": null
 50744|     },
 50745|     {
 50746|       "source": "Autodesk.Revit.DB.Material",
 50747|       "target": "Autodesk.Revit.DB.Material",
 50748|       "member_name": "SetMaterialAspectByPropertySet",
 50749|       "member_kind": "method",
 50750|       "edge_type": "USES_MATERIAL",
 50751|       "confidence": "name_only_candidate",
 50752|       "confidence_tier": "likely",
 50753|       "target_resolution": "exact",
 50754|       "evidence": [
 50755|         "member name 'SetMaterialAspectByPropertySet' matches keyword pattern /Material/ but return type 'void' gives no type-level confirmation"
 50756|       ],
 50757|       "source_url": "https://www.revitapidocs.com/2025/73438593-0643-93d6-5c58-1fb39e2efd47.htm",
 50758|       "dll_signature_verified": true,
 50759|       "dll_relationship_scope": "declared",
 50760|       "dll_semantic_verified": null,
 50761|       "dll_verified_status": "signature_verified_declared",
 50762|       "revitlookup_referenced": null,
 50763|       "revitlookup_requires_document_context": null
 50764|     },
 50765|     {
 50766|       "source": "Autodesk.Revit.DB.MaterialNode",
 50767|       "target": "Autodesk.Revit.DB.Material",
 50768|       "member_name": "MaterialId",
 50769|       "member_kind": "property",
 50770|       "edge_type": "USES_MATERIAL",
 50771|       "confidence": "elementid_with_strong_name",
 50772|       "confidence_tier": "core",
 50773|       "target_resolution": "exact",
 50774|       "evidence": [
 50775|         "member name 'MaterialId' matches keyword pattern /Material/"
 50776|       ],
 50777|       "source_url": "https://www.revitapidocs.com/2025/39cd7c35-da9d-8248-92b8-beae06d63018.htm",
 50778|       "dll_signature_verified": true,
 50779|       "dll_relationship_scope": "declared",
 50780|       "dll_semantic_verified": null,
 50781|       "dll_verified_status": "signature_verified_declared",
 50782|       "revitlookup_referenced": null,
 50783|       "revitlookup_requires_document_context": null
 50784|     },
 50785|     {
 50786|       "source": "Autodesk.Revit.DB.MaterialNode",
 50787|       "target": "Autodesk.Revit.DB.Visual.Asset",
 50788|       "member_name": "GetAppearance",
 50789|       "member_kind": "method",
 50790|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 50791|       "confidence": "direct_return_type",
 50792|       "confidence_tier": "unverified_reference",
 50793|       "target_resolution": "short_name_fallback",
 50794|       "evidence": [
 50795|         "return type 'Asset' directly names a Revit DB object type"
 50796|       ],
 50797|       "source_url": "https://www.revitapidocs.com/2025/59c4b123-e7bb-4108-595f-ad045e151433.htm",
 50798|       "dll_signature_verified": true,
 50799|       "dll_relationship_scope": "declared",
 50800|       "dll_semantic_verified": null,
 50801|       "dll_verified_status": "signature_verified_declared",
 50802|       "revitlookup_referenced": null,
 50803|       "revitlookup_requires_document_context": null
 50804|     },
 50805|     {
 50806|       "source": "Autodesk.Revit.DB.MaterialNode",
 50807|       "target": "Autodesk.Revit.DB.Visual.Asset",
 50808|       "member_name": "GetAppearanceOverride",
 50809|       "member_kind": "method",
 50810|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 50811|       "confidence": "direct_return_type",
 50812|       "confidence_tier": "unverified_reference",
 50813|       "target_resolution": "short_name_fallback",
 50814|       "evidence": [
 50815|         "return type 'Asset' directly names a Revit DB object type"
 50816|       ],
 50817|       "source_url": "https://www.revitapidocs.com/2025/550190e7-9d66-fa2b-62d2-4c4eaa13fa64.htm",
 50818|       "dll_signature_verified": true,
 50819|       "dll_relationship_scope": "declared",
 50820|       "dll_semantic_verified": null,
 50821|       "dll_verified_status": "signature_verified_declared",
 50822|       "revitlookup_referenced": null,
 50823|       "revitlookup_requires_document_context": null
 50824|     },
 50825|     {
 50826|       "source": "Autodesk.Revit.DB.MEPAnalyticalConnectionType",
 50827|       "target": null,
 50828|       "member_name": "FindTypeByName",
 50829|       "member_kind": "method",
 50830|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 50831|       "confidence": "unknown_reference",
 50832|       "confidence_tier": "unverified_reference",
 50833|       "target_resolution": "none",
 50834|       "evidence": [
 50835|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 50836|       ],
 50837|       "source_url": "https://www.revitapidocs.com/2025/3c93daf3-82ed-b686-57ff-6e80f06c618e.htm",
 50838|       "dll_signature_verified": true,
 50839|       "dll_relationship_scope": "declared",
 50840|       "dll_semantic_verified": null,
 50841|       "dll_verified_status": "signature_verified_declared",
 50842|       "revitlookup_referenced": null,
 50843|       "revitlookup_requires_document_context": null
 50844|     },
 50845|     {
 50846|       "source": "Autodesk.Revit.DB.MEPConnectorInfo",
 50847|       "target": "Autodesk.Revit.DB.Connector",
 50848|       "member_name": "LinkedConnector",
 50849|       "member_kind": "property",
 50850|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 50851|       "confidence": "direct_return_type",
 50852|       "confidence_tier": "unverified_reference",
 50853|       "target_resolution": "exact",
 50854|       "evidence": [
 50855|         "return type 'Connector' directly names a Revit DB object type"
 50856|       ],
 50857|       "source_url": "https://www.revitapidocs.com/2025/e7626482-1826-2d22-38e0-507bc97ef243.htm",
 50858|       "dll_signature_verified": true,
 50859|       "dll_relationship_scope": "declared",
 50860|       "dll_semantic_verified": null,
 50861|       "dll_verified_status": "signature_verified_declared",
 50862|       "revitlookup_referenced": null,
 50863|       "revitlookup_requires_document_context": null
 50864|     },
 50865|     {
 50866|       "source": "Autodesk.Revit.DB.MEPCurve",
 50867|       "target": "Autodesk.Revit.DB.ConnectorManager",
 50868|       "member_name": "ConnectorManager",
 50869|       "member_kind": "property",
 50870|       "edge_type": "REFERENCES",
 50871|       "confidence": "direct_return_type",
 50872|       "confidence_tier": "core",
 50873|       "target_resolution": "exact",
 50874|       "evidence": [
 50875|         "return type 'ConnectorManager' directly names a Revit DB object type"
 50876|       ],
 50877|       "source_url": "https://www.revitapidocs.com/2025/5498400a-b28b-786d-170e-383cc0e67875.htm",
 50878|       "dll_signature_verified": true,
 50879|       "dll_relationship_scope": "declared",
 50880|       "dll_semantic_verified": null,
 50881|       "dll_verified_status": "signature_verified_declared",
 50882|       "revitlookup_referenced": null,
 50883|       "revitlookup_requires_document_context": null
 50884|     },
 50885|     {
 50886|       "source": "Autodesk.Revit.DB.MEPCurve",
 50887|       "target": "Autodesk.Revit.DB.Level",
 50888|       "member_name": "LevelOffset",
 50889|       "member_kind": "property",
 50890|       "edge_type": "ASSIGNED_TO_LEVEL",
 50891|       "confidence": "name_only_candidate",
 50892|       "confidence_tier": "likely",
 50893|       "target_resolution": "exact",
 50894|       "evidence": [
 50895|         "member name 'LevelOffset' matches keyword pattern /Level/ but return type 'double' gives no type-level confirmation"
 50896|       ],
 50897|       "source_url": "https://www.revitapidocs.com/2025/de6ae69a-d462-80c9-58e0-ea5bac2a1447.htm",
 50898|       "dll_signature_verified": true,
 50899|       "dll_relationship_scope": "declared",
 50900|       "dll_semantic_verified": null,
 50901|       "dll_verified_status": "signature_verified_declared",
 50902|       "revitlookup_referenced": null,
 50903|       "revitlookup_requires_document_context": null
 50904|     },
 50905|     {
 50906|       "source": "Autodesk.Revit.DB.MEPCurve",
 50907|       "target": "Autodesk.Revit.DB.MEPSystem",
 50908|       "member_name": "MEPSystem",
 50909|       "member_kind": "property",
 50910|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 50911|       "confidence": "direct_return_type",
 50912|       "confidence_tier": "unverified_reference",
 50913|       "target_resolution": "exact",
 50914|       "evidence": [
 50915|         "return type 'MEPSystem' directly names a Revit DB object type"
 50916|       ],
 50917|       "source_url": "https://www.revitapidocs.com/2025/ad663cce-bb05-70bf-33f5-b3d32f9e9b62.htm",
 50918|       "dll_signature_verified": true,
 50919|       "dll_relationship_scope": "declared",
 50920|       "dll_semantic_verified": null,
 50921|       "dll_verified_status": "signature_verified_declared",
 50922|       "revitlookup_referenced": null,
 50923|       "revitlookup_requires_document_context": null
 50924|     },
 50925|     {
 50926|       "source": "Autodesk.Revit.DB.MEPCurve",
 50927|       "target": "Autodesk.Revit.DB.Level",
 50928|       "member_name": "ReferenceLevel",
 50929|       "member_kind": "property",
 50930|       "edge_type": "ASSIGNED_TO_LEVEL",
 50931|       "confidence": "direct_return_type",
 50932|       "confidence_tier": "core",
 50933|       "target_resolution": "exact",
 50934|       "evidence": [
 50935|         "return type 'Level' directly names a Revit DB object type"
 50936|       ],
 50937|       "source_url": "https://www.revitapidocs.com/2025/3d7248c5-558d-60ba-42da-c3b46d35f1cd.htm",
 50938|       "dll_signature_verified": true,
 50939|       "dll_relationship_scope": "declared",
 50940|       "dll_semantic_verified": null,
 50941|       "dll_verified_status": "signature_verified_declared",
 50942|       "revitlookup_referenced": null,
 50943|       "revitlookup_requires_document_context": null
 50944|     },
 50945|     {
 50946|       "source": "Autodesk.Revit.DB.MEPCurveType",
 50947|       "target": "Autodesk.Revit.DB.FamilySymbol",
 50948|       "member_name": "Cross",
 50949|       "member_kind": "property",
 50950|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 50951|       "confidence": "direct_return_type",
 50952|       "confidence_tier": "unverified_reference",
 50953|       "target_resolution": "exact",
 50954|       "evidence": [
 50955|         "return type 'FamilySymbol' directly names a Revit DB object type"
 50956|       ],
 50957|       "source_url": "https://www.revitapidocs.com/2025/33d098c2-e4bc-410d-75d9-83575e83a622.htm",
 50958|       "dll_signature_verified": true,
 50959|       "dll_relationship_scope": "declared",
 50960|       "dll_semantic_verified": null,
 50961|       "dll_verified_status": "signature_verified_declared",
 50962|       "revitlookup_referenced": null,
 50963|       "revitlookup_requires_document_context": null
 50964|     },
 50965|     {
 50966|       "source": "Autodesk.Revit.DB.MEPCurveType",
 50967|       "target": "Autodesk.Revit.DB.FamilySymbol",
 50968|       "member_name": "Elbow",
 50969|       "member_kind": "property",
 50970|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 50971|       "confidence": "direct_return_type",
 50972|       "confidence_tier": "unverified_reference",
 50973|       "target_resolution": "exact",
 50974|       "evidence": [
 50975|         "return type 'FamilySymbol' directly names a Revit DB object type"
 50976|       ],
 50977|       "source_url": "https://www.revitapidocs.com/2025/3eec7500-a2e5-bd1f-2eac-5e2ea6953104.htm",
 50978|       "dll_signature_verified": true,
 50979|       "dll_relationship_scope": "declared",
 50980|       "dll_semantic_verified": null,
 50981|       "dll_verified_status": "signature_verified_declared",
 50982|       "revitlookup_referenced": null,
 50983|       "revitlookup_requires_document_context": null
 50984|     },
 50985|     {
 50986|       "source": "Autodesk.Revit.DB.MEPCurveType",
 50987|       "target": "Autodesk.Revit.DB.FamilySymbol",
 50988|       "member_name": "MultiShapeTransition",
 50989|       "member_kind": "property",
 50990|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 50991|       "confidence": "direct_return_type",
 50992|       "confidence_tier": "unverified_reference",
 50993|       "target_resolution": "exact",
 50994|       "evidence": [
 50995|         "return type 'FamilySymbol' directly names a Revit DB object type"
 50996|       ],
 50997|       "source_url": "https://www.revitapidocs.com/2025/43b3040f-1d0f-a8ee-5fac-bb5380562e45.htm",
 50998|       "dll_signature_verified": true,
 50999|       "dll_relationship_scope": "declared",
 51000|       "dll_semantic_verified": null,
 51001|       "dll_verified_status": "signature_verified_declared",
 51002|       "revitlookup_referenced": null,
 51003|       "revitlookup_requires_document_context": null
 51004|     },
 51005|     {
 51006|       "source": "Autodesk.Revit.DB.MEPCurveType",
 51007|       "target": "Autodesk.Revit.DB.RoutingPreferenceManager",
 51008|       "member_name": "RoutingPreferenceManager",
 51009|       "member_kind": "property",
 51010|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 51011|       "confidence": "direct_return_type",
 51012|       "confidence_tier": "unverified_reference",
 51013|       "target_resolution": "exact",
 51014|       "evidence": [
 51015|         "return type 'RoutingPreferenceManager' directly names a Revit DB object type"
 51016|       ],
 51017|       "source_url": "https://www.revitapidocs.com/2025/9fd171c5-b908-07d0-6fdc-bbcdcfae35a4.htm",
 51018|       "dll_signature_verified": true,
 51019|       "dll_relationship_scope": "declared",
 51020|       "dll_semantic_verified": null,
 51021|       "dll_verified_status": "signature_verified_declared",
 51022|       "revitlookup_referenced": null,
 51023|       "revitlookup_requires_document_context": null
 51024|     },
 51025|     {
 51026|       "source": "Autodesk.Revit.DB.MEPCurveType",
 51027|       "target": "Autodesk.Revit.DB.FamilySymbol",
 51028|       "member_name": "Tap",
 51029|       "member_kind": "property",
 51030|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 51031|       "confidence": "direct_return_type",
 51032|       "confidence_tier": "unverified_reference",
 51033|       "target_resolution": "exact",
 51034|       "evidence": [
 51035|         "return type 'FamilySymbol' directly names a Revit DB object type"
 51036|       ],
 51037|       "source_url": "https://www.revitapidocs.com/2025/2e1d5217-01f2-e1f7-ebab-d3c8d2afa8a6.htm",
 51038|       "dll_signature_verified": true,
 51039|       "dll_relationship_scope": "declared",
 51040|       "dll_semantic_verified": null,
 51041|       "dll_verified_status": "signature_verified_declared",
 51042|       "revitlookup_referenced": null,
 51043|       "revitlookup_requires_document_context": null
 51044|     },
 51045|     {
 51046|       "source": "Autodesk.Revit.DB.MEPCurveType",
 51047|       "target": "Autodesk.Revit.DB.FamilySymbol",
 51048|       "member_name": "Tee",
 51049|       "member_kind": "property",
 51050|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 51051|       "confidence": "direct_return_type",
 51052|       "confidence_tier": "unverified_reference",
 51053|       "target_resolution": "exact",
 51054|       "evidence": [
 51055|         "return type 'FamilySymbol' directly names a Revit DB object type"
 51056|       ],
 51057|       "source_url": "https://www.revitapidocs.com/2025/bd5c961c-3e70-0727-e12c-6fef6a4c1ffa.htm",
 51058|       "dll_signature_verified": true,
 51059|       "dll_relationship_scope": "declared",
 51060|       "dll_semantic_verified": null,
 51061|       "dll_verified_status": "signature_verified_declared",
 51062|       "revitlookup_referenced": null,
 51063|       "revitlookup_requires_document_context": null
 51064|     },
 51065|     {
 51066|       "source": "Autodesk.Revit.DB.MEPCurveType",
 51067|       "target": "Autodesk.Revit.DB.FamilySymbol",
 51068|       "member_name": "Transition",
 51069|       "member_kind": "property",
 51070|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 51071|       "confidence": "direct_return_type",
 51072|       "confidence_tier": "unverified_reference",
 51073|       "target_resolution": "exact",
 51074|       "evidence": [
 51075|         "return type 'FamilySymbol' directly names a Revit DB object type"
 51076|       ],
 51077|       "source_url": "https://www.revitapidocs.com/2025/33842660-06e1-ad7f-096f-01b722ad10c1.htm",
 51078|       "dll_signature_verified": true,
 51079|       "dll_relationship_scope": "declared",
 51080|       "dll_semantic_verified": null,
 51081|       "dll_verified_status": "signature_verified_declared",
 51082|       "revitlookup_referenced": null,
 51083|       "revitlookup_requires_document_context": null
 51084|     },
 51085|     {
 51086|       "source": "Autodesk.Revit.DB.MEPCurveType",
 51087|       "target": "Autodesk.Revit.DB.FamilySymbol",
 51088|       "member_name": "Union",
 51089|       "member_kind": "property",
 51090|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 51091|       "confidence": "direct_return_type",
 51092|       "confidence_tier": "unverified_reference",
 51093|       "target_resolution": "exact",
 51094|       "evidence": [
 51095|         "return type 'FamilySymbol' directly names a Revit DB object type"
 51096|       ],
 51097|       "source_url": "https://www.revitapidocs.com/2025/e74b59ea-3367-c980-030a-831c1733c4e3.htm",
 51098|       "dll_signature_verified": true,
 51099|       "dll_relationship_scope": "declared",
 51100|       "dll_semantic_verified": null,
```
