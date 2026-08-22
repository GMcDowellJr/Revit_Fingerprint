# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 213 of 216
- Original line range: 82681-83080
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 82681|       "target_resolution": "exact",
 82682|       "evidence": [
 82683|         "return type 'Document' directly names a Revit DB object type"
 82684|       ],
 82685|       "source_url": "https://www.revitapidocs.com/2025/ced60288-464b-76e1-8d85-49b691c04a5f.htm",
 82686|       "dll_signature_verified": true,
 82687|       "dll_relationship_scope": "declared",
 82688|       "dll_semantic_verified": null,
 82689|       "dll_verified_status": "signature_verified_declared",
 82690|       "revitlookup_referenced": null,
 82691|       "revitlookup_requires_document_context": null
 82692|     },
 82693|     {
 82694|       "source": "Autodesk.Revit.DB.Structure.RebarUpdateCurvesData",
 82695|       "target": "Autodesk.Revit.DB.Structure.RebarConstraint",
 82696|       "member_name": "GetEndConstraint",
 82697|       "member_kind": "method",
 82698|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 82699|       "confidence": "direct_return_type",
 82700|       "confidence_tier": "unverified_reference",
 82701|       "target_resolution": "short_name_fallback",
 82702|       "evidence": [
 82703|         "return type 'RebarConstraint' directly names a Revit DB object type"
 82704|       ],
 82705|       "source_url": "https://www.revitapidocs.com/2025/bfa1ffbd-d5fa-835b-d628-a9ed97e90017.htm",
 82706|       "dll_signature_verified": true,
 82707|       "dll_relationship_scope": "declared",
 82708|       "dll_semantic_verified": null,
 82709|       "dll_verified_status": "signature_verified_declared",
 82710|       "revitlookup_referenced": null,
 82711|       "revitlookup_requires_document_context": null
 82712|     },
 82713|     {
 82714|       "source": "Autodesk.Revit.DB.Structure.RebarUpdateCurvesData",
 82715|       "target": null,
 82716|       "member_name": "GetHostId",
 82717|       "member_kind": "method",
 82718|       "edge_type": "HOSTED_BY",
 82719|       "confidence": "elementid_with_strong_name",
 82720|       "confidence_tier": "core",
 82721|       "target_resolution": "none",
 82722|       "evidence": [
 82723|         "member name 'GetHostId' matches keyword pattern /^GetHosted|Host/"
 82724|       ],
 82725|       "source_url": "https://www.revitapidocs.com/2025/4f29a5a7-9703-f0d4-9567-4a042d6b927a.htm",
 82726|       "dll_signature_verified": true,
 82727|       "dll_relationship_scope": "declared",
 82728|       "dll_semantic_verified": null,
 82729|       "dll_verified_status": "signature_verified_declared",
 82730|       "revitlookup_referenced": null,
 82731|       "revitlookup_requires_document_context": null
 82732|     },
 82733|     {
 82734|       "source": "Autodesk.Revit.DB.Structure.RebarUpdateCurvesData",
 82735|       "target": null,
 82736|       "member_name": "GetRebarId",
 82737|       "member_kind": "method",
 82738|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 82739|       "confidence": "unknown_reference",
 82740|       "confidence_tier": "unverified_reference",
 82741|       "target_resolution": "none",
 82742|       "evidence": [
 82743|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 82744|       ],
 82745|       "source_url": "https://www.revitapidocs.com/2025/ef027bb1-3944-1abd-78ef-02125ec36a9e.htm",
 82746|       "dll_signature_verified": true,
 82747|       "dll_relationship_scope": "declared",
 82748|       "dll_semantic_verified": null,
 82749|       "dll_verified_status": "signature_verified_declared",
 82750|       "revitlookup_referenced": null,
 82751|       "revitlookup_requires_document_context": null
 82752|     },
 82753|     {
 82754|       "source": "Autodesk.Revit.DB.Structure.RebarUpdateCurvesData",
 82755|       "target": "Autodesk.Revit.DB.Structure.RebarConstraint",
 82756|       "member_name": "GetStartConstraint",
 82757|       "member_kind": "method",
 82758|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 82759|       "confidence": "direct_return_type",
 82760|       "confidence_tier": "unverified_reference",
 82761|       "target_resolution": "short_name_fallback",
 82762|       "evidence": [
 82763|         "return type 'RebarConstraint' directly names a Revit DB object type"
 82764|       ],
 82765|       "source_url": "https://www.revitapidocs.com/2025/6f5516f2-6715-c642-4d73-ccf85be35178.htm",
 82766|       "dll_signature_verified": true,
 82767|       "dll_relationship_scope": "declared",
 82768|       "dll_semantic_verified": null,
 82769|       "dll_verified_status": "signature_verified_declared",
 82770|       "revitlookup_referenced": null,
 82771|       "revitlookup_requires_document_context": null
 82772|     },
 82773|     {
 82774|       "source": "Autodesk.Revit.DB.Structure.ReinforcementAbbreviationTag",
 82775|       "target": null,
 82776|       "member_name": "AbbreviationTag",
 82777|       "member_kind": "property",
 82778|       "edge_type": "TAGS_ELEMENT",
 82779|       "confidence": "name_only_candidate",
 82780|       "confidence_tier": "likely",
 82781|       "target_resolution": "none",
 82782|       "evidence": [
 82783|         "member name 'AbbreviationTag' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'string' gives no type-level confirmation"
 82784|       ],
 82785|       "source_url": "https://www.revitapidocs.com/2025/0ced5f7c-5a01-fbcb-3f18-3c5a4a68430e.htm",
 82786|       "dll_signature_verified": true,
 82787|       "dll_relationship_scope": "declared",
 82788|       "dll_semantic_verified": null,
 82789|       "dll_verified_status": "signature_verified_declared",
 82790|       "revitlookup_referenced": null,
 82791|       "revitlookup_requires_document_context": null
 82792|     },
 82793|     {
 82794|       "source": "Autodesk.Revit.DB.Structure.ReinforcementAbbreviationTag",
 82795|       "target": null,
 82796|       "member_name": "TypeTag",
 82797|       "member_kind": "property",
 82798|       "edge_type": "TAGS_ELEMENT",
 82799|       "confidence": "name_only_candidate",
 82800|       "confidence_tier": "likely",
 82801|       "target_resolution": "none",
 82802|       "evidence": [
 82803|         "member name 'TypeTag' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'ReinforcementAbbreviationTagType' gives no type-level confirmation"
 82804|       ],
 82805|       "source_url": "https://www.revitapidocs.com/2025/f5554812-879c-0255-23f0-d2fabd1a3772.htm",
 82806|       "dll_signature_verified": true,
 82807|       "dll_relationship_scope": "declared",
 82808|       "dll_semantic_verified": null,
 82809|       "dll_verified_status": "signature_verified_declared",
 82810|       "revitlookup_referenced": null,
 82811|       "revitlookup_requires_document_context": null
 82812|     },
 82813|     {
 82814|       "source": "Autodesk.Revit.DB.Structure.ReinforcementRoundingManager",
 82815|       "target": "Autodesk.Revit.DB.Element",
 82816|       "member_name": "Element",
 82817|       "member_kind": "property",
 82818|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 82819|       "confidence": "direct_return_type",
 82820|       "confidence_tier": "unverified_reference",
 82821|       "target_resolution": "exact",
 82822|       "evidence": [
 82823|         "return type 'Element' directly names a Revit DB object type"
 82824|       ],
 82825|       "source_url": "https://www.revitapidocs.com/2025/19607a51-5b44-f2a0-7a6a-9aabde232d6e.htm",
 82826|       "dll_signature_verified": true,
 82827|       "dll_relationship_scope": "declared",
 82828|       "dll_semantic_verified": null,
 82829|       "dll_verified_status": "signature_verified_declared",
 82830|       "revitlookup_referenced": null,
 82831|       "revitlookup_requires_document_context": null
 82832|     },
 82833|     {
 82834|       "source": "Autodesk.Revit.DB.Structure.ReinforcementSettings",
 82835|       "target": null,
 82836|       "member_name": "HostStructuralRebar",
 82837|       "member_kind": "property",
 82838|       "edge_type": "HOSTED_BY",
 82839|       "confidence": "name_only_candidate",
 82840|       "confidence_tier": "likely",
 82841|       "target_resolution": "none",
 82842|       "evidence": [
 82843|         "member name 'HostStructuralRebar' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 82844|       ],
 82845|       "source_url": "https://www.revitapidocs.com/2025/e778d21b-369f-ffc2-ab0f-9191fec3cd8f.htm",
 82846|       "dll_signature_verified": true,
 82847|       "dll_relationship_scope": "declared",
 82848|       "dll_semantic_verified": null,
 82849|       "dll_verified_status": "signature_verified_declared",
 82850|       "revitlookup_referenced": null,
 82851|       "revitlookup_requires_document_context": null
 82852|     },
 82853|     {
 82854|       "source": "Autodesk.Revit.DB.Structure.ReinforcementSettings",
 82855|       "target": "Autodesk.Revit.DB.Structure.FabricRoundingManager",
 82856|       "member_name": "GetFabricRoundingManager",
 82857|       "member_kind": "method",
 82858|       "edge_type": "REFERENCES",
 82859|       "confidence": "direct_return_type",
 82860|       "confidence_tier": "core",
 82861|       "target_resolution": "short_name_fallback",
 82862|       "evidence": [
 82863|         "return type 'FabricRoundingManager' directly names a Revit DB object type"
 82864|       ],
 82865|       "source_url": "https://www.revitapidocs.com/2025/e8fd5c2d-5fa1-1b02-4e4e-4057680a756e.htm",
 82866|       "dll_signature_verified": true,
 82867|       "dll_relationship_scope": "declared",
 82868|       "dll_semantic_verified": null,
 82869|       "dll_verified_status": "signature_verified_declared",
 82870|       "revitlookup_referenced": null,
 82871|       "revitlookup_requires_document_context": null
 82872|     },
 82873|     {
 82874|       "source": "Autodesk.Revit.DB.Structure.ReinforcementSettings",
 82875|       "target": "Autodesk.Revit.DB.Structure.RebarRoundingManager",
 82876|       "member_name": "GetRebarRoundingManager",
 82877|       "member_kind": "method",
 82878|       "edge_type": "REFERENCES",
 82879|       "confidence": "direct_return_type",
 82880|       "confidence_tier": "core",
 82881|       "target_resolution": "short_name_fallback",
 82882|       "evidence": [
 82883|         "return type 'RebarRoundingManager' directly names a Revit DB object type"
 82884|       ],
 82885|       "source_url": "https://www.revitapidocs.com/2025/0471fc13-76ea-a071-b6b6-f26586ba7dd6.htm",
 82886|       "dll_signature_verified": true,
 82887|       "dll_relationship_scope": "declared",
 82888|       "dll_semantic_verified": null,
 82889|       "dll_verified_status": "signature_verified_declared",
 82890|       "revitlookup_referenced": null,
 82891|       "revitlookup_requires_document_context": null
 82892|     },
 82893|     {
 82894|       "source": "Autodesk.Revit.DB.Structure.ReinforcementSettings",
 82895|       "target": null,
 82896|       "member_name": "GetReinforcementAbbreviationTag",
 82897|       "member_kind": "method",
 82898|       "edge_type": "TAGS_ELEMENT",
 82899|       "confidence": "name_only_candidate",
 82900|       "confidence_tier": "likely",
 82901|       "target_resolution": "none",
 82902|       "evidence": [
 82903|         "member name 'GetReinforcementAbbreviationTag' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'string' gives no type-level confirmation"
 82904|       ],
 82905|       "source_url": "https://www.revitapidocs.com/2025/aa2e274b-4a04-2382-958e-ae8c5da76d34.htm",
 82906|       "dll_signature_verified": true,
 82907|       "dll_relationship_scope": "declared",
 82908|       "dll_semantic_verified": null,
 82909|       "dll_verified_status": "signature_verified_declared",
 82910|       "revitlookup_referenced": null,
 82911|       "revitlookup_requires_document_context": null
 82912|     },
 82913|     {
 82914|       "source": "Autodesk.Revit.DB.Structure.ReinforcementSettings",
 82915|       "target": "Autodesk.Revit.DB.Structure.ReinforcementAbbreviationTag",
 82916|       "member_name": "GetReinforcementAbbreviationTags",
 82917|       "member_kind": "method",
 82918|       "edge_type": "TAGS_ELEMENT",
 82919|       "confidence": "needs_runtime_validation",
 82920|       "confidence_tier": "needs_validation",
 82921|       "target_resolution": "short_name_fallback",
 82922|       "evidence": [
 82923|         "return type 'IList < ReinforcementAbbreviationTag >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 82924|       ],
 82925|       "source_url": "https://www.revitapidocs.com/2025/c99a81c9-19ec-3983-beb7-e6080a5f6431.htm",
 82926|       "dll_signature_verified": true,
 82927|       "dll_relationship_scope": "declared",
 82928|       "dll_semantic_verified": null,
 82929|       "dll_verified_status": "signature_verified_declared",
 82930|       "revitlookup_referenced": null,
 82931|       "revitlookup_requires_document_context": null
 82932|     },
 82933|     {
 82934|       "source": "Autodesk.Revit.DB.Structure.ReinforcementSettings",
 82935|       "target": null,
 82936|       "member_name": "SetReinforcementAbbreviationTag",
 82937|       "member_kind": "method",
 82938|       "edge_type": "TAGS_ELEMENT",
 82939|       "confidence": "name_only_candidate",
 82940|       "confidence_tier": "likely",
 82941|       "target_resolution": "none",
 82942|       "evidence": [
 82943|         "member name 'SetReinforcementAbbreviationTag' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'void' gives no type-level confirmation"
 82944|       ],
 82945|       "source_url": "https://www.revitapidocs.com/2025/9907ed20-d7dd-f387-01e8-76f66f1c76f2.htm",
 82946|       "dll_signature_verified": true,
 82947|       "dll_relationship_scope": "declared",
 82948|       "dll_semantic_verified": null,
 82949|       "dll_verified_status": "signature_verified_declared",
 82950|       "revitlookup_referenced": null,
 82951|       "revitlookup_requires_document_context": null
 82952|     },
 82953|     {
 82954|       "source": "Autodesk.Revit.DB.Structure.StructuralConnectionApprovalType",
 82955|       "target": null,
 82956|       "member_name": "GetAllStructuralConnectionApprovalTypes",
 82957|       "member_kind": "method",
 82958|       "edge_type": "RETURNS_ELEMENT_IDS",
 82959|       "confidence": "name_only_candidate",
 82960|       "confidence_tier": "likely",
 82961|       "target_resolution": "none",
 82962|       "evidence": [
 82963|         "member name 'GetAllStructuralConnectionApprovalTypes' matches keyword pattern /^GetAll/ but return type 'void' gives no type-level confirmation"
 82964|       ],
 82965|       "source_url": "https://www.revitapidocs.com/2025/17a6b10f-a08b-0dab-8356-546f546146e7.htm",
 82966|       "dll_signature_verified": true,
 82967|       "dll_relationship_scope": "declared",
 82968|       "dll_semantic_verified": null,
 82969|       "dll_verified_status": "signature_verified_declared",
 82970|       "revitlookup_referenced": null,
 82971|       "revitlookup_requires_document_context": null
 82972|     },
 82973|     {
 82974|       "source": "Autodesk.Revit.DB.Structure.StructuralConnectionHandler",
 82975|       "target": null,
 82976|       "member_name": "ApprovalTypeId",
 82977|       "member_kind": "property",
 82978|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 82979|       "confidence": "unknown_reference",
 82980|       "confidence_tier": "unverified_reference",
 82981|       "target_resolution": "none",
 82982|       "evidence": [
 82983|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 82984|       ],
 82985|       "source_url": "https://www.revitapidocs.com/2025/5257d862-ab65-0c35-bdb8-b6b69b627122.htm",
 82986|       "dll_signature_verified": true,
 82987|       "dll_relationship_scope": "declared",
 82988|       "dll_semantic_verified": null,
 82989|       "dll_verified_status": "signature_verified_declared",
 82990|       "revitlookup_referenced": null,
 82991|       "revitlookup_requires_document_context": null
 82992|     },
 82993|     {
 82994|       "source": "Autodesk.Revit.DB.Structure.StructuralConnectionHandler",
 82995|       "target": null,
 82996|       "member_name": "GetConnectedElementIds",
 82997|       "member_kind": "method",
 82998|       "edge_type": "RETURNS_ELEMENT_IDS",
 82999|       "confidence": "unknown_reference",
 83000|       "confidence_tier": "unverified_reference",
 83001|       "target_resolution": "none",
 83002|       "evidence": [
 83003|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 83004|       ],
 83005|       "source_url": "https://www.revitapidocs.com/2025/0c30ce4f-eef8-fd20-529f-c7b0983998dc.htm",
 83006|       "dll_signature_verified": true,
 83007|       "dll_relationship_scope": "declared",
 83008|       "dll_semantic_verified": null,
 83009|       "dll_verified_status": "signature_verified_declared",
 83010|       "revitlookup_referenced": null,
 83011|       "revitlookup_requires_document_context": null
 83012|     },
 83013|     {
 83014|       "source": "Autodesk.Revit.DB.Structure.StructuralConnectionHandler",
 83015|       "target": "Autodesk.Revit.DB.Structure.ConnectionInputPoint",
 83016|       "member_name": "GetInputPoint",
 83017|       "member_kind": "method",
 83018|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 83019|       "confidence": "direct_return_type",
 83020|       "confidence_tier": "unverified_reference",
 83021|       "target_resolution": "short_name_fallback",
 83022|       "evidence": [
 83023|         "return type 'ConnectionInputPoint' directly names a Revit DB object type"
 83024|       ],
 83025|       "source_url": "https://www.revitapidocs.com/2025/c7a4dc5e-bc26-0987-4995-2c5d2dbcee70.htm",
 83026|       "dll_signature_verified": true,
 83027|       "dll_relationship_scope": "declared",
 83028|       "dll_semantic_verified": null,
 83029|       "dll_verified_status": "signature_verified_declared",
 83030|       "revitlookup_referenced": null,
 83031|       "revitlookup_requires_document_context": null
 83032|     },
 83033|     {
 83034|       "source": "Autodesk.Revit.DB.Structure.StructuralConnectionHandler",
 83035|       "target": "Autodesk.Revit.DB.Structure.ConnectionInputPoint",
 83036|       "member_name": "GetInputPoints",
 83037|       "member_kind": "method",
 83038|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 83039|       "confidence": "needs_runtime_validation",
 83040|       "confidence_tier": "needs_validation",
 83041|       "target_resolution": "short_name_fallback",
 83042|       "evidence": [
 83043|         "return type 'IList < ConnectionInputPoint >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 83044|       ],
 83045|       "source_url": "https://www.revitapidocs.com/2025/1b74c240-55f5-c1ab-1e78-57bba4090a66.htm",
 83046|       "dll_signature_verified": true,
 83047|       "dll_relationship_scope": "declared",
 83048|       "dll_semantic_verified": null,
 83049|       "dll_verified_status": "signature_verified_declared",
 83050|       "revitlookup_referenced": null,
 83051|       "revitlookup_requires_document_context": null
 83052|     },
 83053|     {
 83054|       "source": "Autodesk.Revit.DB.Structure.StructuralConnectionHandler",
 83055|       "target": "Autodesk.Revit.DB.Reference",
 83056|       "member_name": "GetInputReferences",
 83057|       "member_kind": "method",
 83058|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 83059|       "confidence": "needs_runtime_validation",
 83060|       "confidence_tier": "needs_validation",
 83061|       "target_resolution": "exact",
 83062|       "evidence": [
 83063|         "return type 'IList < Reference >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 83064|       ],
 83065|       "source_url": "https://www.revitapidocs.com/2025/64f39922-a906-19ae-bfa0-0642910eefdb.htm",
 83066|       "dll_signature_verified": true,
 83067|       "dll_relationship_scope": "declared",
 83068|       "dll_semantic_verified": null,
 83069|       "dll_verified_status": "signature_verified_declared",
 83070|       "revitlookup_referenced": null,
 83071|       "revitlookup_requires_document_context": null
 83072|     },
 83073|     {
 83074|       "source": "Autodesk.Revit.DB.Structure.StructuralConnectionHandlerType",
 83075|       "target": null,
 83076|       "member_name": "FindGenericConnectionType",
 83077|       "member_kind": "method",
 83078|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 83079|       "confidence": "unknown_reference",
 83080|       "confidence_tier": "unverified_reference",
```
