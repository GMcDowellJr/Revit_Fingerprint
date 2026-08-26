# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 200 of 216
- Original line range: 77611-78010
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 77611|       "member_kind": "method",
 77612|       "edge_type": "RETURNS_ELEMENT_IDS",
 77613|       "confidence": "unknown_reference",
 77614|       "confidence_tier": "unverified_reference",
 77615|       "target_resolution": "none",
 77616|       "evidence": [
 77617|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 77618|       ],
 77619|       "source_url": "https://www.revitapidocs.com/2025/848d940c-3664-0533-576f-7876b5c2bf1d.htm",
 77620|       "dll_signature_verified": true,
 77621|       "dll_relationship_scope": "declared",
 77622|       "dll_semantic_verified": null,
 77623|       "dll_verified_status": "signature_verified_declared",
 77624|       "revitlookup_referenced": null,
 77625|       "revitlookup_requires_document_context": null
 77626|     },
 77627|     {
 77628|       "source": "Autodesk.Revit.DB.Structure.AnalyticalSurfaceBase",
 77629|       "target": "Autodesk.Revit.DB.Sketch",
 77630|       "member_name": "SketchId",
 77631|       "member_kind": "property",
 77632|       "edge_type": "DEPENDS_ON",
 77633|       "confidence": "elementid_with_strong_name",
 77634|       "confidence_tier": "core",
 77635|       "target_resolution": "exact",
 77636|       "evidence": [
 77637|         "member name 'SketchId' matches keyword pattern /Sketch(Id)?$/"
 77638|       ],
 77639|       "source_url": "https://www.revitapidocs.com/2025/41796d62-c246-f1db-dabf-f74f8cb209c7.htm",
 77640|       "dll_signature_verified": true,
 77641|       "dll_relationship_scope": "declared",
 77642|       "dll_semantic_verified": null,
 77643|       "dll_verified_status": "signature_verified_declared",
 77644|       "revitlookup_referenced": null,
 77645|       "revitlookup_requires_document_context": null
 77646|     },
 77647|     {
 77648|       "source": "Autodesk.Revit.DB.Structure.AnalyticalToPhysicalAssociationManager",
 77649|       "target": null,
 77650|       "member_name": "GetAssociatedElementId",
 77651|       "member_kind": "method",
 77652|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 77653|       "confidence": "unknown_reference",
 77654|       "confidence_tier": "unverified_reference",
 77655|       "target_resolution": "none",
 77656|       "evidence": [
 77657|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 77658|       ],
 77659|       "source_url": "https://www.revitapidocs.com/2025/59274411-94e7-633b-2d9e-8989a903cc48.htm",
 77660|       "dll_signature_verified": true,
 77661|       "dll_relationship_scope": "declared",
 77662|       "dll_semantic_verified": null,
 77663|       "dll_verified_status": "signature_verified_declared",
 77664|       "revitlookup_referenced": null,
 77665|       "revitlookup_requires_document_context": null
 77666|     },
 77667|     {
 77668|       "source": "Autodesk.Revit.DB.Structure.AnalyticalToPhysicalAssociationManager",
 77669|       "target": null,
 77670|       "member_name": "GetAssociatedElementIds",
 77671|       "member_kind": "method",
 77672|       "edge_type": "RETURNS_ELEMENT_IDS",
 77673|       "confidence": "unknown_reference",
 77674|       "confidence_tier": "unverified_reference",
 77675|       "target_resolution": "none",
 77676|       "evidence": [
 77677|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 77678|       ],
 77679|       "source_url": "https://www.revitapidocs.com/2025/f3710676-cd32-eb7d-59aa-8a11be872ba9.htm",
 77680|       "dll_signature_verified": true,
 77681|       "dll_relationship_scope": "declared",
 77682|       "dll_semantic_verified": null,
 77683|       "dll_verified_status": "signature_verified_declared",
 77684|       "revitlookup_referenced": null,
 77685|       "revitlookup_requires_document_context": null
 77686|     },
 77687|     {
 77688|       "source": "Autodesk.Revit.DB.Structure.AreaLoad",
 77689|       "target": null,
 77690|       "member_name": "IsCurveLoopsInsideHostBoundaries",
 77691|       "member_kind": "method",
 77692|       "edge_type": "HOSTED_BY",
 77693|       "confidence": "name_only_candidate",
 77694|       "confidence_tier": "likely",
 77695|       "target_resolution": "none",
 77696|       "evidence": [
 77697|         "member name 'IsCurveLoopsInsideHostBoundaries' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 77698|       ],
 77699|       "source_url": "https://www.revitapidocs.com/2025/56f7aaed-c85f-b5ae-07ca-a49eae4efb94.htm",
 77700|       "dll_signature_verified": true,
 77701|       "dll_relationship_scope": "declared",
 77702|       "dll_semantic_verified": null,
 77703|       "dll_verified_status": "signature_verified_declared",
 77704|       "revitlookup_referenced": null,
 77705|       "revitlookup_requires_document_context": null
 77706|     },
 77707|     {
 77708|       "source": "Autodesk.Revit.DB.Structure.AreaLoad",
 77709|       "target": null,
 77710|       "member_name": "IsValidHostId",
 77711|       "member_kind": "method",
 77712|       "edge_type": "HOSTED_BY",
 77713|       "confidence": "name_only_candidate",
 77714|       "confidence_tier": "likely",
 77715|       "target_resolution": "none",
 77716|       "evidence": [
 77717|         "member name 'IsValidHostId' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 77718|       ],
 77719|       "source_url": "https://www.revitapidocs.com/2025/25e62c90-a5b5-09d5-9c49-03058e5e51b0.htm",
 77720|       "dll_signature_verified": true,
 77721|       "dll_relationship_scope": "declared",
 77722|       "dll_semantic_verified": null,
 77723|       "dll_verified_status": "signature_verified_declared",
 77724|       "revitlookup_referenced": null,
 77725|       "revitlookup_requires_document_context": null
 77726|     },
 77727|     {
 77728|       "source": "Autodesk.Revit.DB.Structure.AreaReinforcement",
 77729|       "target": "Autodesk.Revit.DB.Structure.AreaReinforcementType",
 77730|       "member_name": "AreaReinforcementType",
 77731|       "member_kind": "property",
 77732|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 77733|       "confidence": "direct_return_type",
 77734|       "confidence_tier": "unverified_reference",
 77735|       "target_resolution": "short_name_fallback",
 77736|       "evidence": [
 77737|         "return type 'AreaReinforcementType' directly names a Revit DB object type"
 77738|       ],
 77739|       "source_url": "https://www.revitapidocs.com/2025/c653d882-ddde-c6bf-806d-add2df555275.htm",
 77740|       "dll_signature_verified": true,
 77741|       "dll_relationship_scope": "declared",
 77742|       "dll_semantic_verified": null,
 77743|       "dll_verified_status": "signature_verified_declared",
 77744|       "revitlookup_referenced": null,
 77745|       "revitlookup_requires_document_context": null
 77746|     },
 77747|     {
 77748|       "source": "Autodesk.Revit.DB.Structure.AreaReinforcement",
 77749|       "target": null,
 77750|       "member_name": "ConvertRebarInSystemToRebars",
 77751|       "member_kind": "method",
 77752|       "edge_type": "RETURNS_ELEMENT_IDS",
 77753|       "confidence": "unknown_reference",
 77754|       "confidence_tier": "unverified_reference",
 77755|       "target_resolution": "none",
 77756|       "evidence": [
 77757|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 77758|       ],
 77759|       "source_url": "https://www.revitapidocs.com/2025/0b655f89-d5c2-eef1-8900-67601a478b1a.htm",
 77760|       "dll_signature_verified": true,
 77761|       "dll_relationship_scope": "declared",
 77762|       "dll_semantic_verified": null,
 77763|       "dll_verified_status": "signature_verified_declared",
 77764|       "revitlookup_referenced": null,
 77765|       "revitlookup_requires_document_context": null
 77766|     },
 77767|     {
 77768|       "source": "Autodesk.Revit.DB.Structure.AreaReinforcement",
 77769|       "target": null,
 77770|       "member_name": "GetBoundaryCurveIds",
 77771|       "member_kind": "method",
 77772|       "edge_type": "RETURNS_ELEMENT_IDS",
 77773|       "confidence": "unknown_reference",
 77774|       "confidence_tier": "unverified_reference",
 77775|       "target_resolution": "none",
 77776|       "evidence": [
 77777|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 77778|       ],
 77779|       "source_url": "https://www.revitapidocs.com/2025/75225104-99db-98f2-d407-6ee8f48ad9ae.htm",
 77780|       "dll_signature_verified": true,
 77781|       "dll_relationship_scope": "declared",
 77782|       "dll_semantic_verified": null,
 77783|       "dll_verified_status": "signature_verified_declared",
 77784|       "revitlookup_referenced": null,
 77785|       "revitlookup_requires_document_context": null
 77786|     },
 77787|     {
 77788|       "source": "Autodesk.Revit.DB.Structure.AreaReinforcement",
 77789|       "target": null,
 77790|       "member_name": "GetHostId",
 77791|       "member_kind": "method",
 77792|       "edge_type": "HOSTED_BY",
 77793|       "confidence": "elementid_with_strong_name",
 77794|       "confidence_tier": "core",
 77795|       "target_resolution": "none",
 77796|       "evidence": [
 77797|         "member name 'GetHostId' matches keyword pattern /^GetHosted|Host/"
 77798|       ],
 77799|       "source_url": "https://www.revitapidocs.com/2025/f01b201e-507f-0100-6d58-de49f981a449.htm",
 77800|       "dll_signature_verified": true,
 77801|       "dll_relationship_scope": "declared",
 77802|       "dll_semantic_verified": null,
 77803|       "dll_verified_status": "signature_verified_declared",
 77804|       "revitlookup_referenced": null,
 77805|       "revitlookup_requires_document_context": null
 77806|     },
 77807|     {
 77808|       "source": "Autodesk.Revit.DB.Structure.AreaReinforcement",
 77809|       "target": "Autodesk.Revit.DB.Line",
 77810|       "member_name": "GetLineFromLayerAtIndex",
 77811|       "member_kind": "method",
 77812|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 77813|       "confidence": "direct_return_type",
 77814|       "confidence_tier": "unverified_reference",
 77815|       "target_resolution": "exact",
 77816|       "evidence": [
 77817|         "return type 'Line' directly names a Revit DB object type"
 77818|       ],
 77819|       "source_url": "https://www.revitapidocs.com/2025/8fa3c4da-89e0-28a5-d6dc-30f675dbb04b.htm",
 77820|       "dll_signature_verified": true,
 77821|       "dll_relationship_scope": "declared",
 77822|       "dll_semantic_verified": null,
 77823|       "dll_verified_status": "signature_verified_declared",
 77824|       "revitlookup_referenced": null,
 77825|       "revitlookup_requires_document_context": null
 77826|     },
 77827|     {
 77828|       "source": "Autodesk.Revit.DB.Structure.AreaReinforcement",
 77829|       "target": null,
 77830|       "member_name": "GetRebarInSystemIds",
 77831|       "member_kind": "method",
 77832|       "edge_type": "RETURNS_ELEMENT_IDS",
 77833|       "confidence": "unknown_reference",
 77834|       "confidence_tier": "unverified_reference",
 77835|       "target_resolution": "none",
 77836|       "evidence": [
 77837|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 77838|       ],
 77839|       "source_url": "https://www.revitapidocs.com/2025/164ad9af-4034-ca8b-d7f6-2e108becf7e1.htm",
 77840|       "dll_signature_verified": true,
 77841|       "dll_relationship_scope": "declared",
 77842|       "dll_semantic_verified": null,
 77843|       "dll_verified_status": "signature_verified_declared",
 77844|       "revitlookup_referenced": null,
 77845|       "revitlookup_requires_document_context": null
 77846|     },
 77847|     {
 77848|       "source": "Autodesk.Revit.DB.Structure.AreaReinforcement",
 77849|       "target": null,
 77850|       "member_name": "RemoveAreaReinforcementSystem",
 77851|       "member_kind": "method",
 77852|       "edge_type": "RETURNS_ELEMENT_IDS",
 77853|       "confidence": "unknown_reference",
 77854|       "confidence_tier": "unverified_reference",
 77855|       "target_resolution": "none",
 77856|       "evidence": [
 77857|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 77858|       ],
 77859|       "source_url": "https://www.revitapidocs.com/2025/497ef418-b0cc-e0a2-34d1-67ef49274801.htm",
 77860|       "dll_signature_verified": true,
 77861|       "dll_relationship_scope": "declared",
 77862|       "dll_semantic_verified": null,
 77863|       "dll_verified_status": "signature_verified_declared",
 77864|       "revitlookup_referenced": null,
 77865|       "revitlookup_requires_document_context": null
 77866|     },
 77867|     {
 77868|       "source": "Autodesk.Revit.DB.Structure.BendingDetailCustomFieldProperties",
 77869|       "target": null,
 77870|       "member_name": "AngularDimensionTypeId",
 77871|       "member_kind": "property",
 77872|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 77873|       "confidence": "unknown_reference",
 77874|       "confidence_tier": "unverified_reference",
 77875|       "target_resolution": "none",
 77876|       "evidence": [
 77877|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 77878|       ],
 77879|       "source_url": "https://www.revitapidocs.com/2025/9831ce4e-0bef-8e07-5260-32d8ecb80a3e.htm",
 77880|       "dll_signature_verified": true,
 77881|       "dll_relationship_scope": "declared",
 77882|       "dll_semantic_verified": null,
 77883|       "dll_verified_status": "signature_verified_declared",
 77884|       "revitlookup_referenced": null,
 77885|       "revitlookup_requires_document_context": null
 77886|     },
 77887|     {
 77888|       "source": "Autodesk.Revit.DB.Structure.BendingDetailCustomFieldProperties",
 77889|       "target": null,
 77890|       "member_name": "DiameterDimensionTypeId",
 77891|       "member_kind": "property",
 77892|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 77893|       "confidence": "unknown_reference",
 77894|       "confidence_tier": "unverified_reference",
 77895|       "target_resolution": "none",
 77896|       "evidence": [
 77897|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 77898|       ],
 77899|       "source_url": "https://www.revitapidocs.com/2025/81f1e17a-7ee5-d3ea-3aaf-6203bb2ff7cb.htm",
 77900|       "dll_signature_verified": true,
 77901|       "dll_relationship_scope": "declared",
 77902|       "dll_semantic_verified": null,
 77903|       "dll_verified_status": "signature_verified_declared",
 77904|       "revitlookup_referenced": null,
 77905|       "revitlookup_requires_document_context": null
 77906|     },
 77907|     {
 77908|       "source": "Autodesk.Revit.DB.Structure.BendingDetailCustomFieldProperties",
 77909|       "target": null,
 77910|       "member_name": "LineStyleId",
 77911|       "member_kind": "property",
 77912|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 77913|       "confidence": "unknown_reference",
 77914|       "confidence_tier": "unverified_reference",
 77915|       "target_resolution": "none",
 77916|       "evidence": [
 77917|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 77918|       ],
 77919|       "source_url": "https://www.revitapidocs.com/2025/c691f2b9-dc2b-d123-8374-4a9e34d67059.htm",
 77920|       "dll_signature_verified": true,
 77921|       "dll_relationship_scope": "declared",
 77922|       "dll_semantic_verified": null,
 77923|       "dll_verified_status": "signature_verified_declared",
 77924|       "revitlookup_referenced": null,
 77925|       "revitlookup_requires_document_context": null
 77926|     },
 77927|     {
 77928|       "source": "Autodesk.Revit.DB.Structure.BendingDetailCustomFieldProperties",
 77929|       "target": null,
 77930|       "member_name": "ParametersDisplayOption",
 77931|       "member_kind": "property",
 77932|       "edge_type": "HAS_PARAMETER",
 77933|       "confidence": "name_only_candidate",
 77934|       "confidence_tier": "likely",
 77935|       "target_resolution": "none",
 77936|       "evidence": [
 77937|         "member name 'ParametersDisplayOption' matches keyword pattern /Parameter/ but return type 'BendingDetailDisplayParametersOptions' gives no type-level confirmation"
 77938|       ],
 77939|       "source_url": "https://www.revitapidocs.com/2025/eae872b5-9687-1012-3a5e-5bcf9f3bf977.htm",
 77940|       "dll_signature_verified": true,
 77941|       "dll_relationship_scope": "declared",
 77942|       "dll_semantic_verified": null,
 77943|       "dll_verified_status": "signature_verified_declared",
 77944|       "revitlookup_referenced": null,
 77945|       "revitlookup_requires_document_context": null
 77946|     },
 77947|     {
 77948|       "source": "Autodesk.Revit.DB.Structure.BendingDetailCustomFieldProperties",
 77949|       "target": null,
 77950|       "member_name": "RadialDimensionTypeId",
 77951|       "member_kind": "property",
 77952|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 77953|       "confidence": "unknown_reference",
 77954|       "confidence_tier": "unverified_reference",
 77955|       "target_resolution": "none",
 77956|       "evidence": [
 77957|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 77958|       ],
 77959|       "source_url": "https://www.revitapidocs.com/2025/90b4d8ee-1165-0311-579b-99772b0368d7.htm",
 77960|       "dll_signature_verified": true,
 77961|       "dll_relationship_scope": "declared",
 77962|       "dll_semantic_verified": null,
 77963|       "dll_verified_status": "signature_verified_declared",
 77964|       "revitlookup_referenced": null,
 77965|       "revitlookup_requires_document_context": null
 77966|     },
 77967|     {
 77968|       "source": "Autodesk.Revit.DB.Structure.BendingDetailCustomFieldProperties",
 77969|       "target": null,
 77970|       "member_name": "SegmentLengthDimensionTypeId",
 77971|       "member_kind": "property",
 77972|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 77973|       "confidence": "unknown_reference",
 77974|       "confidence_tier": "unverified_reference",
 77975|       "target_resolution": "none",
 77976|       "evidence": [
 77977|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 77978|       ],
 77979|       "source_url": "https://www.revitapidocs.com/2025/6860f3f3-59b5-74e9-c718-4be700dbe8ac.htm",
 77980|       "dll_signature_verified": true,
 77981|       "dll_relationship_scope": "declared",
 77982|       "dll_semantic_verified": null,
 77983|       "dll_verified_status": "signature_verified_declared",
 77984|       "revitlookup_referenced": null,
 77985|       "revitlookup_requires_document_context": null
 77986|     },
 77987|     {
 77988|       "source": "Autodesk.Revit.DB.Structure.BoundaryConditions",
 77989|       "target": null,
 77990|       "member_name": "AssociatedLoadId",
 77991|       "member_kind": "property",
 77992|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 77993|       "confidence": "unknown_reference",
 77994|       "confidence_tier": "unverified_reference",
 77995|       "target_resolution": "none",
 77996|       "evidence": [
 77997|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 77998|       ],
 77999|       "source_url": "https://www.revitapidocs.com/2025/9e8cd409-59af-6426-ac76-d2cccab1908d.htm",
 78000|       "dll_signature_verified": true,
 78001|       "dll_relationship_scope": "declared",
 78002|       "dll_semantic_verified": null,
 78003|       "dll_verified_status": "signature_verified_declared",
 78004|       "revitlookup_referenced": null,
 78005|       "revitlookup_requires_document_context": null
 78006|     },
 78007|     {
 78008|       "source": "Autodesk.Revit.DB.Structure.BoundaryConditions",
 78009|       "target": null,
 78010|       "member_name": "HostElementId",
```
