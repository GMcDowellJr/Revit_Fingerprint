# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 167 of 216
- Original line range: 64741-65140
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 64741|       "confidence": "name_only_candidate",
 64742|       "confidence_tier": "likely",
 64743|       "target_resolution": "exact",
 64744|       "evidence": [
 64745|         "member name 'RenameWorkset' matches keyword pattern /Workset/ but return type 'void' gives no type-level confirmation"
 64746|       ],
 64747|       "source_url": "https://www.revitapidocs.com/2025/aa6f8625-cf32-cad1-bf9a-eec33abab957.htm",
 64748|       "dll_signature_verified": true,
 64749|       "dll_relationship_scope": "declared",
 64750|       "dll_semantic_verified": null,
 64751|       "dll_verified_status": "signature_verified_declared",
 64752|       "revitlookup_referenced": null,
 64753|       "revitlookup_requires_document_context": null
 64754|     },
 64755|     {
 64756|       "source": "Autodesk.Revit.DB.WorksetTable",
 64757|       "target": "Autodesk.Revit.DB.Workset",
 64758|       "member_name": "SetActiveWorksetId",
 64759|       "member_kind": "method",
 64760|       "edge_type": "OWNED_BY_WORKSET",
 64761|       "confidence": "name_only_candidate",
 64762|       "confidence_tier": "likely",
 64763|       "target_resolution": "exact",
 64764|       "evidence": [
 64765|         "member name 'SetActiveWorksetId' matches keyword pattern /Workset/ but return type 'void' gives no type-level confirmation"
 64766|       ],
 64767|       "source_url": "https://www.revitapidocs.com/2025/9f11d796-ca5c-93d9-51e1-67cf8da9baf2.htm",
 64768|       "dll_signature_verified": true,
 64769|       "dll_relationship_scope": "declared",
 64770|       "dll_semantic_verified": null,
 64771|       "dll_verified_status": "signature_verified_declared",
 64772|       "revitlookup_referenced": null,
 64773|       "revitlookup_requires_document_context": null
 64774|     },
 64775|     {
 64776|       "source": "Autodesk.Revit.DB.WorksharingDisplaySettings",
 64777|       "target": null,
 64778|       "member_name": "GetAllUsersWithGraphicOverrides",
 64779|       "member_kind": "method",
 64780|       "edge_type": "RETURNS_ELEMENT_IDS",
 64781|       "confidence": "name_only_candidate",
 64782|       "confidence_tier": "likely",
 64783|       "target_resolution": "none",
 64784|       "evidence": [
 64785|         "member name 'GetAllUsersWithGraphicOverrides' matches keyword pattern /^GetAll/ but return type 'ICollection < string >' gives no type-level confirmation"
 64786|       ],
 64787|       "source_url": "https://www.revitapidocs.com/2025/4df2c37c-fca2-80a3-980d-50a478220c58.htm",
 64788|       "dll_signature_verified": true,
 64789|       "dll_relationship_scope": "declared",
 64790|       "dll_semantic_verified": null,
 64791|       "dll_verified_status": "signature_verified_declared",
 64792|       "revitlookup_referenced": null,
 64793|       "revitlookup_requires_document_context": null
 64794|     },
 64795|     {
 64796|       "source": "Autodesk.Revit.DB.WorksharingDisplaySettings",
 64797|       "target": "Autodesk.Revit.DB.WorksharingDisplayGraphicSettings",
 64798|       "member_name": "GetGraphicOverrides",
 64799|       "member_kind": "method",
 64800|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 64801|       "confidence": "direct_return_type",
 64802|       "confidence_tier": "unverified_reference",
 64803|       "target_resolution": "exact",
 64804|       "evidence": [
 64805|         "return type 'WorksharingDisplayGraphicSettings' directly names a Revit DB object type"
 64806|       ],
 64807|       "source_url": "https://www.revitapidocs.com/2025/34582abf-6edd-d9c5-aa00-5d39268ac5a1.htm",
 64808|       "dll_signature_verified": true,
 64809|       "dll_relationship_scope": "declared",
 64810|       "dll_semantic_verified": null,
 64811|       "dll_verified_status": "signature_verified_declared",
 64812|       "revitlookup_referenced": null,
 64813|       "revitlookup_requires_document_context": null
 64814|     },
 64815|     {
 64816|       "source": "Autodesk.Revit.DB.WorksharingDisplaySettings",
 64817|       "target": "Autodesk.Revit.DB.WorksharingDisplayGraphicSettings",
 64818|       "member_name": "GetGraphicOverrides",
 64819|       "member_kind": "method",
 64820|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 64821|       "confidence": "direct_return_type",
 64822|       "confidence_tier": "unverified_reference",
 64823|       "target_resolution": "exact",
 64824|       "evidence": [
 64825|         "return type 'WorksharingDisplayGraphicSettings' directly names a Revit DB object type"
 64826|       ],
 64827|       "source_url": "https://www.revitapidocs.com/2025/192a99a8-62d4-7330-6552-5f7256de82eb.htm",
 64828|       "dll_signature_verified": true,
 64829|       "dll_relationship_scope": "declared",
 64830|       "dll_semantic_verified": null,
 64831|       "dll_verified_status": "signature_verified_declared",
 64832|       "revitlookup_referenced": null,
 64833|       "revitlookup_requires_document_context": null
 64834|     },
 64835|     {
 64836|       "source": "Autodesk.Revit.DB.WorksharingDisplaySettings",
 64837|       "target": "Autodesk.Revit.DB.WorksharingDisplayGraphicSettings",
 64838|       "member_name": "GetGraphicOverrides",
 64839|       "member_kind": "method",
 64840|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 64841|       "confidence": "direct_return_type",
 64842|       "confidence_tier": "unverified_reference",
 64843|       "target_resolution": "exact",
 64844|       "evidence": [
 64845|         "return type 'WorksharingDisplayGraphicSettings' directly names a Revit DB object type"
 64846|       ],
 64847|       "source_url": "https://www.revitapidocs.com/2025/e642ca72-5c5a-80f4-a2e6-a98b874254e6.htm",
 64848|       "dll_signature_verified": true,
 64849|       "dll_relationship_scope": "declared",
 64850|       "dll_semantic_verified": null,
 64851|       "dll_verified_status": "signature_verified_declared",
 64852|       "revitlookup_referenced": null,
 64853|       "revitlookup_requires_document_context": null
 64854|     },
 64855|     {
 64856|       "source": "Autodesk.Revit.DB.WorksharingDisplaySettings",
 64857|       "target": "Autodesk.Revit.DB.WorksharingDisplayGraphicSettings",
 64858|       "member_name": "GetGraphicOverrides",
 64859|       "member_kind": "method",
 64860|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 64861|       "confidence": "direct_return_type",
 64862|       "confidence_tier": "unverified_reference",
 64863|       "target_resolution": "exact",
 64864|       "evidence": [
 64865|         "return type 'WorksharingDisplayGraphicSettings' directly names a Revit DB object type"
 64866|       ],
 64867|       "source_url": "https://www.revitapidocs.com/2025/6753b405-5170-1636-83ce-902c9a27526f.htm",
 64868|       "dll_signature_verified": true,
 64869|       "dll_relationship_scope": "declared",
 64870|       "dll_semantic_verified": null,
 64871|       "dll_verified_status": "signature_verified_declared",
 64872|       "revitlookup_referenced": null,
 64873|       "revitlookup_requires_document_context": null
 64874|     },
 64875|     {
 64876|       "source": "Autodesk.Revit.DB.WorksharingSaveAsOptions",
 64877|       "target": "Autodesk.Revit.DB.Workset",
 64878|       "member_name": "OpenWorksetsDefault",
 64879|       "member_kind": "property",
 64880|       "edge_type": "OWNED_BY_WORKSET",
 64881|       "confidence": "name_only_candidate",
 64882|       "confidence_tier": "likely",
 64883|       "target_resolution": "exact",
 64884|       "evidence": [
 64885|         "member name 'OpenWorksetsDefault' matches keyword pattern /Workset/ but return type 'SimpleWorksetConfiguration' gives no type-level confirmation"
 64886|       ],
 64887|       "source_url": "https://www.revitapidocs.com/2025/a22a67ee-b8ad-68d4-e055-6dcf69443c0a.htm",
 64888|       "dll_signature_verified": true,
 64889|       "dll_relationship_scope": "declared",
 64890|       "dll_semantic_verified": null,
 64891|       "dll_verified_status": "signature_verified_declared",
 64892|       "revitlookup_referenced": null,
 64893|       "revitlookup_requires_document_context": null
 64894|     },
 64895|     {
 64896|       "source": "Autodesk.Revit.DB.WorksharingUtils",
 64897|       "target": null,
 64898|       "member_name": "CheckoutElements",
 64899|       "member_kind": "method",
 64900|       "edge_type": "RETURNS_ELEMENT_IDS",
 64901|       "confidence": "unknown_reference",
 64902|       "confidence_tier": "unverified_reference",
 64903|       "target_resolution": "none",
 64904|       "evidence": [
 64905|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 64906|       ],
 64907|       "source_url": "https://www.revitapidocs.com/2025/5553298f-e8a3-13f1-3f1b-f3505e82eb5c.htm",
 64908|       "dll_signature_verified": true,
 64909|       "dll_relationship_scope": "declared",
 64910|       "dll_semantic_verified": null,
 64911|       "dll_verified_status": "signature_verified_declared",
 64912|       "revitlookup_referenced": null,
 64913|       "revitlookup_requires_document_context": null
 64914|     },
 64915|     {
 64916|       "source": "Autodesk.Revit.DB.WorksharingUtils",
 64917|       "target": null,
 64918|       "member_name": "CheckoutElements",
 64919|       "member_kind": "method",
 64920|       "edge_type": "RETURNS_ELEMENT_IDS",
 64921|       "confidence": "unknown_reference",
 64922|       "confidence_tier": "unverified_reference",
 64923|       "target_resolution": "none",
 64924|       "evidence": [
 64925|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 64926|       ],
 64927|       "source_url": "https://www.revitapidocs.com/2025/50d213af-17cb-b92a-6251-ce9a79e5ce5b.htm",
 64928|       "dll_signature_verified": true,
 64929|       "dll_relationship_scope": "declared",
 64930|       "dll_semantic_verified": null,
 64931|       "dll_verified_status": "signature_verified_declared",
 64932|       "revitlookup_referenced": null,
 64933|       "revitlookup_requires_document_context": null
 64934|     },
 64935|     {
 64936|       "source": "Autodesk.Revit.DB.WorksharingUtils",
 64937|       "target": "Autodesk.Revit.DB.WorksetId",
 64938|       "member_name": "CheckoutWorksets",
 64939|       "member_kind": "method",
 64940|       "edge_type": "OWNED_BY_WORKSET",
 64941|       "confidence": "needs_runtime_validation",
 64942|       "confidence_tier": "needs_validation",
 64943|       "target_resolution": "exact",
 64944|       "evidence": [
 64945|         "return type 'ICollection < WorksetId >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 64946|       ],
 64947|       "source_url": "https://www.revitapidocs.com/2025/97f0d4eb-ad2a-ca9d-a896-5144bd68c5a5.htm",
 64948|       "dll_signature_verified": true,
 64949|       "dll_relationship_scope": "declared",
 64950|       "dll_semantic_verified": null,
 64951|       "dll_verified_status": "signature_verified_declared",
 64952|       "revitlookup_referenced": null,
 64953|       "revitlookup_requires_document_context": null
 64954|     },
 64955|     {
 64956|       "source": "Autodesk.Revit.DB.WorksharingUtils",
 64957|       "target": "Autodesk.Revit.DB.WorksetId",
 64958|       "member_name": "CheckoutWorksets",
 64959|       "member_kind": "method",
 64960|       "edge_type": "OWNED_BY_WORKSET",
 64961|       "confidence": "needs_runtime_validation",
 64962|       "confidence_tier": "needs_validation",
 64963|       "target_resolution": "exact",
 64964|       "evidence": [
 64965|         "return type 'ISet < WorksetId >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 64966|       ],
 64967|       "source_url": "https://www.revitapidocs.com/2025/39b55560-c85b-bebc-e825-b76b5ba313a7.htm",
 64968|       "dll_signature_verified": true,
 64969|       "dll_relationship_scope": "declared",
 64970|       "dll_semantic_verified": null,
 64971|       "dll_verified_status": "signature_verified_declared",
 64972|       "revitlookup_referenced": null,
 64973|       "revitlookup_requires_document_context": null
 64974|     },
 64975|     {
 64976|       "source": "Autodesk.Revit.DB.WorksharingUtils",
 64977|       "target": "Autodesk.Revit.DB.WorksetPreview",
 64978|       "member_name": "GetUserWorksetInfo",
 64979|       "member_kind": "method",
 64980|       "edge_type": "OWNED_BY_WORKSET",
 64981|       "confidence": "needs_runtime_validation",
 64982|       "confidence_tier": "needs_validation",
 64983|       "target_resolution": "exact",
 64984|       "evidence": [
 64985|         "return type 'IList < WorksetPreview >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 64986|       ],
 64987|       "source_url": "https://www.revitapidocs.com/2025/15ec1e3e-61d5-b6a1-3604-8b866a988270.htm",
 64988|       "dll_signature_verified": true,
 64989|       "dll_relationship_scope": "declared",
 64990|       "dll_semantic_verified": null,
 64991|       "dll_verified_status": "signature_verified_declared",
 64992|       "revitlookup_referenced": null,
 64993|       "revitlookup_requires_document_context": null
 64994|     },
 64995|     {
 64996|       "source": "Autodesk.Revit.DB.WorksharingUtils",
 64997|       "target": "Autodesk.Revit.DB.WorksharingTooltipInfo",
 64998|       "member_name": "GetWorksharingTooltipInfo",
 64999|       "member_kind": "method",
 65000|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 65001|       "confidence": "direct_return_type",
 65002|       "confidence_tier": "unverified_reference",
 65003|       "target_resolution": "exact",
 65004|       "evidence": [
 65005|         "return type 'WorksharingTooltipInfo' directly names a Revit DB object type"
 65006|       ],
 65007|       "source_url": "https://www.revitapidocs.com/2025/1e54c25f-7d7a-7484-be7b-d741084418a9.htm",
 65008|       "dll_signature_verified": true,
 65009|       "dll_relationship_scope": "declared",
 65010|       "dll_semantic_verified": null,
 65011|       "dll_verified_status": "signature_verified_declared",
 65012|       "revitlookup_referenced": null,
 65013|       "revitlookup_requires_document_context": null
 65014|     },
 65015|     {
 65016|       "source": "Autodesk.Revit.DB.WorksharingUtils",
 65017|       "target": "Autodesk.Revit.DB.RelinquishedItems",
 65018|       "member_name": "RelinquishOwnership",
 65019|       "member_kind": "method",
 65020|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 65021|       "confidence": "direct_return_type",
 65022|       "confidence_tier": "unverified_reference",
 65023|       "target_resolution": "exact",
 65024|       "evidence": [
 65025|         "return type 'RelinquishedItems' directly names a Revit DB object type"
 65026|       ],
 65027|       "source_url": "https://www.revitapidocs.com/2025/09f4e163-cb8f-de87-d641-3ba667adf4e0.htm",
 65028|       "dll_signature_verified": true,
 65029|       "dll_relationship_scope": "declared",
 65030|       "dll_semantic_verified": null,
 65031|       "dll_verified_status": "signature_verified_declared",
 65032|       "revitlookup_referenced": null,
 65033|       "revitlookup_requires_document_context": null
 65034|     },
 65035|     {
 65036|       "source": "Autodesk.Revit.DB.Analysis.AnalysisDisplayColorSettings",
 65037|       "target": "Autodesk.Revit.DB.Analysis.AnalysisDisplayColorEntry",
 65038|       "member_name": "GetIntermediateColors",
 65039|       "member_kind": "method",
 65040|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 65041|       "confidence": "needs_runtime_validation",
 65042|       "confidence_tier": "needs_validation",
 65043|       "target_resolution": "short_name_fallback",
 65044|       "evidence": [
 65045|         "return type 'IList < AnalysisDisplayColorEntry >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 65046|       ],
 65047|       "source_url": "https://www.revitapidocs.com/2025/818e8cde-fa1e-72c2-ecc4-b203ce967eb4.htm",
 65048|       "dll_signature_verified": true,
 65049|       "dll_relationship_scope": "declared",
 65050|       "dll_semantic_verified": null,
 65051|       "dll_verified_status": "signature_verified_declared",
 65052|       "revitlookup_referenced": null,
 65053|       "revitlookup_requires_document_context": null
 65054|     },
 65055|     {
 65056|       "source": "Autodesk.Revit.DB.Analysis.AnalysisDisplayDeformedShapeSettings",
 65057|       "target": null,
 65058|       "member_name": "TextTypeId",
 65059|       "member_kind": "property",
 65060|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 65061|       "confidence": "unknown_reference",
 65062|       "confidence_tier": "unverified_reference",
 65063|       "target_resolution": "none",
 65064|       "evidence": [
 65065|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 65066|       ],
 65067|       "source_url": "https://www.revitapidocs.com/2025/a2dd6e73-dfc6-3e6c-d3a2-bfbb1b903fa3.htm",
 65068|       "dll_signature_verified": true,
 65069|       "dll_relationship_scope": "declared",
 65070|       "dll_semantic_verified": null,
 65071|       "dll_verified_status": "signature_verified_declared",
 65072|       "revitlookup_referenced": null,
 65073|       "revitlookup_requires_document_context": null
 65074|     },
 65075|     {
 65076|       "source": "Autodesk.Revit.DB.Analysis.AnalysisDisplayDiagramSettings",
 65077|       "target": null,
 65078|       "member_name": "TextTypeId",
 65079|       "member_kind": "property",
 65080|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 65081|       "confidence": "unknown_reference",
 65082|       "confidence_tier": "unverified_reference",
 65083|       "target_resolution": "none",
 65084|       "evidence": [
 65085|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 65086|       ],
 65087|       "source_url": "https://www.revitapidocs.com/2025/579f8e27-2830-55ea-93d7-14598f234b9f.htm",
 65088|       "dll_signature_verified": true,
 65089|       "dll_relationship_scope": "declared",
 65090|       "dll_semantic_verified": null,
 65091|       "dll_verified_status": "signature_verified_declared",
 65092|       "revitlookup_referenced": null,
 65093|       "revitlookup_requires_document_context": null
 65094|     },
 65095|     {
 65096|       "source": "Autodesk.Revit.DB.Analysis.AnalysisDisplayLegendSettings",
 65097|       "target": null,
 65098|       "member_name": "HeadingTextTypeId",
 65099|       "member_kind": "property",
 65100|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 65101|       "confidence": "unknown_reference",
 65102|       "confidence_tier": "unverified_reference",
 65103|       "target_resolution": "none",
 65104|       "evidence": [
 65105|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 65106|       ],
 65107|       "source_url": "https://www.revitapidocs.com/2025/10e4affb-7622-5431-d0d1-a96575b2810f.htm",
 65108|       "dll_signature_verified": true,
 65109|       "dll_relationship_scope": "declared",
 65110|       "dll_semantic_verified": null,
 65111|       "dll_verified_status": "signature_verified_declared",
 65112|       "revitlookup_referenced": null,
 65113|       "revitlookup_requires_document_context": null
 65114|     },
 65115|     {
 65116|       "source": "Autodesk.Revit.DB.Analysis.AnalysisDisplayLegendSettings",
 65117|       "target": null,
 65118|       "member_name": "TextTypeId",
 65119|       "member_kind": "property",
 65120|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 65121|       "confidence": "unknown_reference",
 65122|       "confidence_tier": "unverified_reference",
 65123|       "target_resolution": "none",
 65124|       "evidence": [
 65125|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 65126|       ],
 65127|       "source_url": "https://www.revitapidocs.com/2025/f1a07a50-278a-7db0-5e19-13a076226b9a.htm",
 65128|       "dll_signature_verified": true,
 65129|       "dll_relationship_scope": "declared",
 65130|       "dll_semantic_verified": null,
 65131|       "dll_verified_status": "signature_verified_declared",
 65132|       "revitlookup_referenced": null,
 65133|       "revitlookup_requires_document_context": null
 65134|     },
 65135|     {
 65136|       "source": "Autodesk.Revit.DB.Analysis.AnalysisDisplayMarkersAndTextSettings",
 65137|       "target": null,
 65138|       "member_name": "TextTypeId",
 65139|       "member_kind": "property",
 65140|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
```
