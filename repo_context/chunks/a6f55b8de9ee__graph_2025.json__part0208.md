# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 208 of 216
- Original line range: 80731-81130
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
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
 80741|       "dll_relationship_scope": "declared",
 80742|       "dll_semantic_verified": null,
 80743|       "dll_verified_status": "signature_verified_declared",
 80744|       "revitlookup_referenced": null,
 80745|       "revitlookup_requires_document_context": null
 80746|     },
 80747|     {
 80748|       "source": "Autodesk.Revit.DB.Structure.RebarContainer",
 80749|       "target": null,
 80750|       "member_name": "SetHostId",
 80751|       "member_kind": "method",
 80752|       "edge_type": "HOSTED_BY",
 80753|       "confidence": "name_only_candidate",
 80754|       "confidence_tier": "likely",
 80755|       "target_resolution": "none",
 80756|       "evidence": [
 80757|         "member name 'SetHostId' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 80758|       ],
 80759|       "source_url": "https://www.revitapidocs.com/2025/affeac8e-7d19-ee94-4a29-3011ab93e8a0.htm",
 80760|       "dll_signature_verified": true,
 80761|       "dll_relationship_scope": "declared",
 80762|       "dll_semantic_verified": null,
 80763|       "dll_verified_status": "signature_verified_declared",
 80764|       "revitlookup_referenced": null,
 80765|       "revitlookup_requires_document_context": null
 80766|     },
 80767|     {
 80768|       "source": "Autodesk.Revit.DB.Structure.RebarContainerItem",
 80769|       "target": null,
 80770|       "member_name": "BarTypeId",
 80771|       "member_kind": "property",
 80772|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 80773|       "confidence": "unknown_reference",
 80774|       "confidence_tier": "unverified_reference",
 80775|       "target_resolution": "none",
 80776|       "evidence": [
 80777|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 80778|       ],
 80779|       "source_url": "https://www.revitapidocs.com/2025/e01fc1b7-7414-ee88-44b5-91ece090d12e.htm",
 80780|       "dll_signature_verified": true,
 80781|       "dll_relationship_scope": "declared",
 80782|       "dll_semantic_verified": null,
 80783|       "dll_verified_status": "signature_verified_declared",
 80784|       "revitlookup_referenced": null,
 80785|       "revitlookup_requires_document_context": null
 80786|     },
 80787|     {
 80788|       "source": "Autodesk.Revit.DB.Structure.RebarContainerItem",
 80789|       "target": null,
 80790|       "member_name": "RebarShapeId",
 80791|       "member_kind": "property",
 80792|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 80793|       "confidence": "unknown_reference",
 80794|       "confidence_tier": "unverified_reference",
 80795|       "target_resolution": "none",
 80796|       "evidence": [
 80797|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 80798|       ],
 80799|       "source_url": "https://www.revitapidocs.com/2025/4e755081-80ba-11c1-f428-ed1f0a1f580b.htm",
 80800|       "dll_signature_verified": true,
 80801|       "dll_relationship_scope": "declared",
 80802|       "dll_semantic_verified": null,
 80803|       "dll_verified_status": "signature_verified_declared",
 80804|       "revitlookup_referenced": null,
 80805|       "revitlookup_requires_document_context": null
 80806|     },
 80807|     {
 80808|       "source": "Autodesk.Revit.DB.Structure.RebarContainerItem",
 80809|       "target": "Autodesk.Revit.DB.Structure.RebarBendData",
 80810|       "member_name": "GetBendData",
 80811|       "member_kind": "method",
 80812|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80813|       "confidence": "direct_return_type",
 80814|       "confidence_tier": "unverified_reference",
 80815|       "target_resolution": "short_name_fallback",
 80816|       "evidence": [
 80817|         "return type 'RebarBendData' directly names a Revit DB object type"
 80818|       ],
 80819|       "source_url": "https://www.revitapidocs.com/2025/0b86dcae-5f5f-5cf4-f5f3-4d8bbf33f27c.htm",
 80820|       "dll_signature_verified": true,
 80821|       "dll_relationship_scope": "declared",
 80822|       "dll_semantic_verified": null,
 80823|       "dll_verified_status": "signature_verified_declared",
 80824|       "revitlookup_referenced": null,
 80825|       "revitlookup_requires_document_context": null
 80826|     },
 80827|     {
 80828|       "source": "Autodesk.Revit.DB.Structure.RebarContainerItem",
 80829|       "target": "Autodesk.Revit.DB.Line",
 80830|       "member_name": "GetDistributionPath",
 80831|       "member_kind": "method",
 80832|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80833|       "confidence": "direct_return_type",
 80834|       "confidence_tier": "unverified_reference",
 80835|       "target_resolution": "exact",
 80836|       "evidence": [
 80837|         "return type 'Line' directly names a Revit DB object type"
 80838|       ],
 80839|       "source_url": "https://www.revitapidocs.com/2025/0a43e43f-fc15-d257-b159-dc76aedc2743.htm",
 80840|       "dll_signature_verified": true,
 80841|       "dll_relationship_scope": "declared",
 80842|       "dll_semantic_verified": null,
 80843|       "dll_verified_status": "signature_verified_declared",
 80844|       "revitlookup_referenced": null,
 80845|       "revitlookup_requires_document_context": null
 80846|     },
 80847|     {
 80848|       "source": "Autodesk.Revit.DB.Structure.RebarContainerItem",
 80849|       "target": null,
 80850|       "member_name": "GetHookTypeId",
 80851|       "member_kind": "method",
 80852|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 80853|       "confidence": "unknown_reference",
 80854|       "confidence_tier": "unverified_reference",
 80855|       "target_resolution": "none",
 80856|       "evidence": [
 80857|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 80858|       ],
 80859|       "source_url": "https://www.revitapidocs.com/2025/96c3c52f-1d5a-f8a8-ce56-867a3fa38110.htm",
 80860|       "dll_signature_verified": true,
 80861|       "dll_relationship_scope": "declared",
 80862|       "dll_semantic_verified": null,
 80863|       "dll_verified_status": "signature_verified_declared",
 80864|       "revitlookup_referenced": null,
 80865|       "revitlookup_requires_document_context": null
 80866|     },
 80867|     {
 80868|       "source": "Autodesk.Revit.DB.Structure.RebarContainerIterator",
 80869|       "target": "Autodesk.Revit.DB.Structure.RebarContainerItem",
 80870|       "member_name": "Current",
 80871|       "member_kind": "property",
 80872|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80873|       "confidence": "direct_return_type",
 80874|       "confidence_tier": "unverified_reference",
 80875|       "target_resolution": "short_name_fallback",
 80876|       "evidence": [
 80877|         "return type 'RebarContainerItem' directly names a Revit DB object type"
 80878|       ],
 80879|       "source_url": "https://www.revitapidocs.com/2025/f1e07751-c94e-761f-20f3-9a294c4976fb.htm",
 80880|       "dll_signature_verified": true,
 80881|       "dll_relationship_scope": "declared",
 80882|       "dll_semantic_verified": null,
 80883|       "dll_verified_status": "signature_verified_declared",
 80884|       "revitlookup_referenced": null,
 80885|       "revitlookup_requires_document_context": null
 80886|     },
 80887|     {
 80888|       "source": "Autodesk.Revit.DB.Structure.RebarContainerParameterManager",
 80889|       "target": null,
 80890|       "member_name": "AddSharedParameterAsOverride",
 80891|       "member_kind": "method",
 80892|       "edge_type": "HAS_PARAMETER",
 80893|       "confidence": "name_only_candidate",
 80894|       "confidence_tier": "likely",
 80895|       "target_resolution": "none",
 80896|       "evidence": [
 80897|         "member name 'AddSharedParameterAsOverride' matches keyword pattern /Parameter/ but return type 'void' gives no type-level confirmation"
 80898|       ],
 80899|       "source_url": "https://www.revitapidocs.com/2025/0e4551e0-d6c6-3c71-812b-8ea6a82a9ea9.htm",
 80900|       "dll_signature_verified": true,
 80901|       "dll_relationship_scope": "declared",
 80902|       "dll_semantic_verified": null,
 80903|       "dll_verified_status": "signature_verified_declared",
 80904|       "revitlookup_referenced": null,
 80905|       "revitlookup_requires_document_context": null
 80906|     },
 80907|     {
 80908|       "source": "Autodesk.Revit.DB.Structure.RebarContainerParameterManager",
 80909|       "target": null,
 80910|       "member_name": "GetElementIdOverrideValue",
 80911|       "member_kind": "method",
 80912|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 80913|       "confidence": "unknown_reference",
 80914|       "confidence_tier": "unverified_reference",
 80915|       "target_resolution": "none",
 80916|       "evidence": [
 80917|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 80918|       ],
 80919|       "source_url": "https://www.revitapidocs.com/2025/5c9dca6a-77dd-9631-47f8-b0f02c8ca905.htm",
 80920|       "dll_signature_verified": true,
 80921|       "dll_relationship_scope": "declared",
 80922|       "dll_semantic_verified": null,
 80923|       "dll_verified_status": "signature_verified_declared",
 80924|       "revitlookup_referenced": null,
 80925|       "revitlookup_requires_document_context": null
 80926|     },
 80927|     {
 80928|       "source": "Autodesk.Revit.DB.Structure.RebarContainerParameterManager",
 80929|       "target": null,
 80930|       "member_name": "IsOverriddenParameterModifiable",
 80931|       "member_kind": "method",
 80932|       "edge_type": "HAS_PARAMETER",
 80933|       "confidence": "name_only_candidate",
 80934|       "confidence_tier": "likely",
 80935|       "target_resolution": "none",
 80936|       "evidence": [
 80937|         "member name 'IsOverriddenParameterModifiable' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 80938|       ],
 80939|       "source_url": "https://www.revitapidocs.com/2025/7d3b99fe-2028-3309-52cd-a3c8d4319d08.htm",
 80940|       "dll_signature_verified": true,
 80941|       "dll_relationship_scope": "declared",
 80942|       "dll_semantic_verified": null,
 80943|       "dll_verified_status": "signature_verified_declared",
 80944|       "revitlookup_referenced": null,
 80945|       "revitlookup_requires_document_context": null
 80946|     },
 80947|     {
 80948|       "source": "Autodesk.Revit.DB.Structure.RebarContainerParameterManager",
 80949|       "target": null,
 80950|       "member_name": "IsParameterOverridden",
 80951|       "member_kind": "method",
 80952|       "edge_type": "HAS_PARAMETER",
 80953|       "confidence": "name_only_candidate",
 80954|       "confidence_tier": "likely",
 80955|       "target_resolution": "none",
 80956|       "evidence": [
 80957|         "member name 'IsParameterOverridden' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 80958|       ],
 80959|       "source_url": "https://www.revitapidocs.com/2025/329a5321-cfa1-3924-e05b-6a51fcc08b81.htm",
 80960|       "dll_signature_verified": true,
 80961|       "dll_relationship_scope": "declared",
 80962|       "dll_semantic_verified": null,
 80963|       "dll_verified_status": "signature_verified_declared",
 80964|       "revitlookup_referenced": null,
 80965|       "revitlookup_requires_document_context": null
 80966|     },
 80967|     {
 80968|       "source": "Autodesk.Revit.DB.Structure.RebarContainerParameterManager",
 80969|       "target": null,
 80970|       "member_name": "IsRebarContainerParameter",
 80971|       "member_kind": "method",
 80972|       "edge_type": "HAS_PARAMETER",
 80973|       "confidence": "name_only_candidate",
 80974|       "confidence_tier": "likely",
 80975|       "target_resolution": "none",
 80976|       "evidence": [
 80977|         "member name 'IsRebarContainerParameter' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 80978|       ],
 80979|       "source_url": "https://www.revitapidocs.com/2025/f1af9db7-e66c-f8db-8526-1e52833cb830.htm",
 80980|       "dll_signature_verified": true,
 80981|       "dll_relationship_scope": "declared",
 80982|       "dll_semantic_verified": null,
 80983|       "dll_verified_status": "signature_verified_declared",
 80984|       "revitlookup_referenced": null,
 80985|       "revitlookup_requires_document_context": null
 80986|     },
 80987|     {
 80988|       "source": "Autodesk.Revit.DB.Structure.RebarContainerParameterManager",
 80989|       "target": null,
 80990|       "member_name": "SetOverriddenParameterModifiable",
 80991|       "member_kind": "method",
 80992|       "edge_type": "HAS_PARAMETER",
 80993|       "confidence": "name_only_candidate",
 80994|       "confidence_tier": "likely",
 80995|       "target_resolution": "none",
 80996|       "evidence": [
 80997|         "member name 'SetOverriddenParameterModifiable' matches keyword pattern /Parameter/ but return type 'void' gives no type-level confirmation"
 80998|       ],
 80999|       "source_url": "https://www.revitapidocs.com/2025/0b91fcec-09b4-8e89-01cf-24272512395f.htm",
 81000|       "dll_signature_verified": true,
 81001|       "dll_relationship_scope": "declared",
 81002|       "dll_semantic_verified": null,
 81003|       "dll_verified_status": "signature_verified_declared",
 81004|       "revitlookup_referenced": null,
 81005|       "revitlookup_requires_document_context": null
 81006|     },
 81007|     {
 81008|       "source": "Autodesk.Revit.DB.Structure.RebarContainerParameterManager",
 81009|       "target": null,
 81010|       "member_name": "SetOverriddenParameterReadonly",
 81011|       "member_kind": "method",
 81012|       "edge_type": "HAS_PARAMETER",
 81013|       "confidence": "name_only_candidate",
 81014|       "confidence_tier": "likely",
 81015|       "target_resolution": "none",
 81016|       "evidence": [
 81017|         "member name 'SetOverriddenParameterReadonly' matches keyword pattern /Parameter/ but return type 'void' gives no type-level confirmation"
 81018|       ],
 81019|       "source_url": "https://www.revitapidocs.com/2025/13dfe73c-aa3c-767d-c939-45feab28cd21.htm",
 81020|       "dll_signature_verified": true,
 81021|       "dll_relationship_scope": "declared",
 81022|       "dll_semantic_verified": null,
 81023|       "dll_verified_status": "signature_verified_declared",
 81024|       "revitlookup_referenced": null,
 81025|       "revitlookup_requires_document_context": null
 81026|     },
 81027|     {
 81028|       "source": "Autodesk.Revit.DB.Structure.RebarContainerType",
 81029|       "target": null,
 81030|       "member_name": "GetOrCreateRebarContainerType",
 81031|       "member_kind": "method",
 81032|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 81033|       "confidence": "unknown_reference",
 81034|       "confidence_tier": "unverified_reference",
 81035|       "target_resolution": "none",
 81036|       "evidence": [
 81037|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 81038|       ],
 81039|       "source_url": "https://www.revitapidocs.com/2025/741c9232-eb1a-c082-3433-04bdbf630766.htm",
 81040|       "dll_signature_verified": true,
 81041|       "dll_relationship_scope": "declared",
 81042|       "dll_semantic_verified": null,
 81043|       "dll_verified_status": "signature_verified_declared",
 81044|       "revitlookup_referenced": null,
 81045|       "revitlookup_requires_document_context": null
 81046|     },
 81047|     {
 81048|       "source": "Autodesk.Revit.DB.Structure.RebarCoupler",
 81049|       "target": "Autodesk.Revit.DB.Structure.ReinforcementData",
 81050|       "member_name": "GetCoupledReinforcementData",
 81051|       "member_kind": "method",
 81052|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 81053|       "confidence": "needs_runtime_validation",
 81054|       "confidence_tier": "needs_validation",
 81055|       "target_resolution": "short_name_fallback",
 81056|       "evidence": [
 81057|         "return type 'IList < ReinforcementData >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 81058|       ],
 81059|       "source_url": "https://www.revitapidocs.com/2025/f6b701e8-73e3-3ca1-bb8b-c9bbea755be1.htm",
 81060|       "dll_signature_verified": true,
 81061|       "dll_relationship_scope": "declared",
 81062|       "dll_semantic_verified": null,
 81063|       "dll_verified_status": "signature_verified_declared",
 81064|       "revitlookup_referenced": null,
 81065|       "revitlookup_requires_document_context": null
 81066|     },
 81067|     {
 81068|       "source": "Autodesk.Revit.DB.Structure.RebarCurvesData",
 81069|       "target": "Autodesk.Revit.DB.Structure.RebarUpdateCurvesData",
 81070|       "member_name": "GetRebarUpdateCurvesData",
 81071|       "member_kind": "method",
 81072|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 81073|       "confidence": "direct_return_type",
 81074|       "confidence_tier": "unverified_reference",
 81075|       "target_resolution": "short_name_fallback",
 81076|       "evidence": [
 81077|         "return type 'RebarUpdateCurvesData' directly names a Revit DB object type"
 81078|       ],
 81079|       "source_url": "https://www.revitapidocs.com/2025/89fe31e2-42bb-7a25-caa3-6254b0105173.htm",
 81080|       "dll_signature_verified": true,
 81081|       "dll_relationship_scope": "declared",
 81082|       "dll_semantic_verified": null,
 81083|       "dll_verified_status": "signature_verified_declared",
 81084|       "revitlookup_referenced": null,
 81085|       "revitlookup_requires_document_context": null
 81086|     },
 81087|     {
 81088|       "source": "Autodesk.Revit.DB.Structure.RebarFreeFormAccessor",
 81089|       "target": null,
 81090|       "member_name": "AddUpdatingSharedParameter",
 81091|       "member_kind": "method",
 81092|       "edge_type": "HAS_PARAMETER",
 81093|       "confidence": "name_only_candidate",
 81094|       "confidence_tier": "likely",
 81095|       "target_resolution": "none",
 81096|       "evidence": [
 81097|         "member name 'AddUpdatingSharedParameter' matches keyword pattern /Parameter/ but return type 'void' gives no type-level confirmation"
 81098|       ],
 81099|       "source_url": "https://www.revitapidocs.com/2025/6401f06c-476d-bacd-6173-9c7948d286ce.htm",
 81100|       "dll_signature_verified": true,
 81101|       "dll_relationship_scope": "declared",
 81102|       "dll_semantic_verified": null,
 81103|       "dll_verified_status": "signature_verified_declared",
 81104|       "revitlookup_referenced": null,
 81105|       "revitlookup_requires_document_context": null
 81106|     },
 81107|     {
 81108|       "source": "Autodesk.Revit.DB.Structure.RebarFreeFormAccessor",
 81109|       "target": null,
 81110|       "member_name": "GetCouplerIdAtIndex",
 81111|       "member_kind": "method",
 81112|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 81113|       "confidence": "unknown_reference",
 81114|       "confidence_tier": "unverified_reference",
 81115|       "target_resolution": "none",
 81116|       "evidence": [
 81117|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 81118|       ],
 81119|       "source_url": "https://www.revitapidocs.com/2025/aeed9fe2-0225-4940-0914-a47a9e6c61d5.htm",
 81120|       "dll_signature_verified": true,
 81121|       "dll_relationship_scope": "declared",
 81122|       "dll_semantic_verified": null,
 81123|       "dll_verified_status": "signature_verified_declared",
 81124|       "revitlookup_referenced": null,
 81125|       "revitlookup_requires_document_context": null
 81126|     },
 81127|     {
 81128|       "source": "Autodesk.Revit.DB.Structure.RebarFreeFormAccessor",
 81129|       "target": null,
 81130|       "member_name": "GetEndTreatmentTypeIdAtIndex",
```
