# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 162 of 216
- Original line range: 62791-63190
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 62791|     },
 62792|     {
 62793|       "source": "Autodesk.Revit.DB.View3D",
 62794|       "target": "Autodesk.Revit.DB.RenderingSettings",
 62795|       "member_name": "GetRenderingSettings",
 62796|       "member_kind": "method",
 62797|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 62798|       "confidence": "direct_return_type",
 62799|       "confidence_tier": "unverified_reference",
 62800|       "target_resolution": "exact",
 62801|       "evidence": [
 62802|         "return type 'RenderingSettings' directly names a Revit DB object type"
 62803|       ],
 62804|       "source_url": "https://www.revitapidocs.com/2025/a19af989-a2fd-7b22-2f88-0dd4d160b042.htm",
 62805|       "dll_signature_verified": true,
 62806|       "dll_relationship_scope": "declared",
 62807|       "dll_semantic_verified": null,
 62808|       "dll_verified_status": "signature_verified_declared",
 62809|       "revitlookup_referenced": null,
 62810|       "revitlookup_requires_document_context": null
 62811|     },
 62812|     {
 62813|       "source": "Autodesk.Revit.DB.View3D",
 62814|       "target": "Autodesk.Revit.DB.ViewOrientation3D",
 62815|       "member_name": "GetSavedOrientation",
 62816|       "member_kind": "method",
 62817|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 62818|       "confidence": "direct_return_type",
 62819|       "confidence_tier": "unverified_reference",
 62820|       "target_resolution": "exact",
 62821|       "evidence": [
 62822|         "return type 'ViewOrientation3D' directly names a Revit DB object type"
 62823|       ],
 62824|       "source_url": "https://www.revitapidocs.com/2025/68d4a6b9-79f1-8b7e-704d-a3bfe6329e70.htm",
 62825|       "dll_signature_verified": true,
 62826|       "dll_relationship_scope": "declared",
 62827|       "dll_semantic_verified": null,
 62828|       "dll_verified_status": "signature_verified_declared",
 62829|       "revitlookup_referenced": null,
 62830|       "revitlookup_requires_document_context": null
 62831|     },
 62832|     {
 62833|       "source": "Autodesk.Revit.DB.View3D",
 62834|       "target": "Autodesk.Revit.DB.Level",
 62835|       "member_name": "HideGridsOnLevel",
 62836|       "member_kind": "method",
 62837|       "edge_type": "ASSIGNED_TO_LEVEL",
 62838|       "confidence": "name_only_candidate",
 62839|       "confidence_tier": "likely",
 62840|       "target_resolution": "exact",
 62841|       "evidence": [
 62842|         "member name 'HideGridsOnLevel' matches keyword pattern /Level/ but return type 'void' gives no type-level confirmation"
 62843|       ],
 62844|       "source_url": "https://www.revitapidocs.com/2025/3ff8dc79-5d01-4d2b-8dcc-a9c28880048e.htm",
 62845|       "dll_signature_verified": true,
 62846|       "dll_relationship_scope": "declared",
 62847|       "dll_semantic_verified": null,
 62848|       "dll_verified_status": "signature_verified_declared",
 62849|       "revitlookup_referenced": null,
 62850|       "revitlookup_requires_document_context": null
 62851|     },
 62852|     {
 62853|       "source": "Autodesk.Revit.DB.View3D",
 62854|       "target": "Autodesk.Revit.DB.Level",
 62855|       "member_name": "ShowGridsOnLevel",
 62856|       "member_kind": "method",
 62857|       "edge_type": "ASSIGNED_TO_LEVEL",
 62858|       "confidence": "name_only_candidate",
 62859|       "confidence_tier": "likely",
 62860|       "target_resolution": "exact",
 62861|       "evidence": [
 62862|         "member name 'ShowGridsOnLevel' matches keyword pattern /Level/ but return type 'void' gives no type-level confirmation"
 62863|       ],
 62864|       "source_url": "https://www.revitapidocs.com/2025/72ed89c3-546e-c6d8-78ef-95ce7530de98.htm",
 62865|       "dll_signature_verified": true,
 62866|       "dll_relationship_scope": "declared",
 62867|       "dll_semantic_verified": null,
 62868|       "dll_verified_status": "signature_verified_declared",
 62869|       "revitlookup_referenced": null,
 62870|       "revitlookup_requires_document_context": null
 62871|     },
 62872|     {
 62873|       "source": "Autodesk.Revit.DB.View3D",
 62874|       "target": "Autodesk.Revit.DB.Level",
 62875|       "member_name": "ShowGridsOnLevels",
 62876|       "member_kind": "method",
 62877|       "edge_type": "ASSIGNED_TO_LEVEL",
 62878|       "confidence": "name_only_candidate",
 62879|       "confidence_tier": "likely",
 62880|       "target_resolution": "exact",
 62881|       "evidence": [
 62882|         "member name 'ShowGridsOnLevels' matches keyword pattern /Level/ but return type 'void' gives no type-level confirmation"
 62883|       ],
 62884|       "source_url": "https://www.revitapidocs.com/2025/688b29ae-f365-3395-8bb9-46ed14b45eb1.htm",
 62885|       "dll_signature_verified": true,
 62886|       "dll_relationship_scope": "declared",
 62887|       "dll_semantic_verified": null,
 62888|       "dll_verified_status": "signature_verified_declared",
 62889|       "revitlookup_referenced": null,
 62890|       "revitlookup_requires_document_context": null
 62891|     },
 62892|     {
 62893|       "source": "Autodesk.Revit.DB.ViewDisplayBackground",
 62894|       "target": null,
 62895|       "member_name": "Type",
 62896|       "member_kind": "property",
 62897|       "edge_type": "TYPE_OF",
 62898|       "confidence": "name_only_candidate",
 62899|       "confidence_tier": "likely",
 62900|       "target_resolution": "none",
 62901|       "evidence": [
 62902|         "member name 'Type' matches keyword pattern /^(Type|TypeId|GetTypeId)$/ but return type 'ViewDisplayBackgroundType' gives no type-level confirmation"
 62903|       ],
 62904|       "source_url": "https://www.revitapidocs.com/2025/87797b49-0256-72e8-9311-592173ba99bb.htm",
 62905|       "dll_signature_verified": true,
 62906|       "dll_relationship_scope": "declared",
 62907|       "dll_semantic_verified": null,
 62908|       "dll_verified_status": "signature_verified_declared",
 62909|       "revitlookup_referenced": null,
 62910|       "revitlookup_requires_document_context": null
 62911|     },
 62912|     {
 62913|       "source": "Autodesk.Revit.DB.ViewDisplayDepthCueing",
 62914|       "target": null,
 62915|       "member_name": "EndPercentage",
 62916|       "member_kind": "property",
 62917|       "edge_type": "TAGS_ELEMENT",
 62918|       "confidence": "name_only_candidate",
 62919|       "confidence_tier": "likely",
 62920|       "target_resolution": "none",
 62921|       "evidence": [
 62922|         "member name 'EndPercentage' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'int' gives no type-level confirmation"
 62923|       ],
 62924|       "source_url": "https://www.revitapidocs.com/2025/8b120fe1-09ab-d546-9eeb-71a3ddc6bc81.htm",
 62925|       "dll_signature_verified": true,
 62926|       "dll_relationship_scope": "declared",
 62927|       "dll_semantic_verified": null,
 62928|       "dll_verified_status": "signature_verified_declared",
 62929|       "revitlookup_referenced": null,
 62930|       "revitlookup_requires_document_context": null
 62931|     },
 62932|     {
 62933|       "source": "Autodesk.Revit.DB.ViewDisplayDepthCueing",
 62934|       "target": null,
 62935|       "member_name": "StartPercentage",
 62936|       "member_kind": "property",
 62937|       "edge_type": "TAGS_ELEMENT",
 62938|       "confidence": "name_only_candidate",
 62939|       "confidence_tier": "likely",
 62940|       "target_resolution": "none",
 62941|       "evidence": [
 62942|         "member name 'StartPercentage' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'int' gives no type-level confirmation"
 62943|       ],
 62944|       "source_url": "https://www.revitapidocs.com/2025/0aa4039d-4d7d-fc7c-224e-b09a3b853980.htm",
 62945|       "dll_signature_verified": true,
 62946|       "dll_relationship_scope": "declared",
 62947|       "dll_semantic_verified": null,
 62948|       "dll_verified_status": "signature_verified_declared",
 62949|       "revitlookup_referenced": null,
 62950|       "revitlookup_requires_document_context": null
 62951|     },
 62952|     {
 62953|       "source": "Autodesk.Revit.DB.ViewDisplayDepthCueing",
 62954|       "target": null,
 62955|       "member_name": "SetStartEndPercentages",
 62956|       "member_kind": "method",
 62957|       "edge_type": "TAGS_ELEMENT",
 62958|       "confidence": "name_only_candidate",
 62959|       "confidence_tier": "likely",
 62960|       "target_resolution": "none",
 62961|       "evidence": [
 62962|         "member name 'SetStartEndPercentages' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'void' gives no type-level confirmation"
 62963|       ],
 62964|       "source_url": "https://www.revitapidocs.com/2025/031e725f-2572-ec64-b3dd-810dba3f5188.htm",
 62965|       "dll_signature_verified": true,
 62966|       "dll_relationship_scope": "declared",
 62967|       "dll_semantic_verified": null,
 62968|       "dll_verified_status": "signature_verified_declared",
 62969|       "revitlookup_referenced": null,
 62970|       "revitlookup_requires_document_context": null
 62971|     },
 62972|     {
 62973|       "source": "Autodesk.Revit.DB.ViewDisplayModel",
 62974|       "target": null,
 62975|       "member_name": "SilhouetteEdgesGStyleId",
 62976|       "member_kind": "property",
 62977|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 62978|       "confidence": "unknown_reference",
 62979|       "confidence_tier": "unverified_reference",
 62980|       "target_resolution": "none",
 62981|       "evidence": [
 62982|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 62983|       ],
 62984|       "source_url": "https://www.revitapidocs.com/2025/51d10382-ff84-9fd2-c31e-7bf4ec9c1995.htm",
 62985|       "dll_signature_verified": true,
 62986|       "dll_relationship_scope": "declared",
 62987|       "dll_semantic_verified": null,
 62988|       "dll_verified_status": "signature_verified_declared",
 62989|       "revitlookup_referenced": null,
 62990|       "revitlookup_requires_document_context": null
 62991|     },
 62992|     {
 62993|       "source": "Autodesk.Revit.DB.ViewFamilyType",
 62994|       "target": "Autodesk.Revit.DB.View",
 62995|       "member_name": "DefaultTemplateId",
 62996|       "member_kind": "property",
 62997|       "edge_type": "CONTROLLED_BY_TEMPLATE",
 62998|       "confidence": "elementid_with_strong_name",
 62999|       "confidence_tier": "core",
 63000|       "target_resolution": "exact",
 63001|       "evidence": [
 63002|         "member name 'DefaultTemplateId' matches keyword pattern /Template/"
 63003|       ],
 63004|       "source_url": "https://www.revitapidocs.com/2025/0747cdc7-98e7-a8f5-92f4-6da70a0f8dec.htm",
 63005|       "dll_signature_verified": true,
 63006|       "dll_relationship_scope": "declared",
 63007|       "dll_semantic_verified": null,
 63008|       "dll_verified_status": "signature_verified_declared",
 63009|       "revitlookup_referenced": null,
 63010|       "revitlookup_requires_document_context": null
 63011|     },
 63012|     {
 63013|       "source": "Autodesk.Revit.DB.ViewFamilyType",
 63014|       "target": "Autodesk.Revit.DB.View",
 63015|       "member_name": "IsValidDefaultTemplate",
 63016|       "member_kind": "method",
 63017|       "edge_type": "CONTROLLED_BY_TEMPLATE",
 63018|       "confidence": "docs_semantic_hint",
 63019|       "confidence_tier": "core",
 63020|       "target_resolution": "exact",
 63021|       "evidence": [
 63022|         "member name 'IsValidDefaultTemplate' matches keyword pattern /Template/ but return type 'bool' gives no type-level confirmation",
 63023|         "docs text contains relationship phrase: 'template for'"
 63024|       ],
 63025|       "source_url": "https://www.revitapidocs.com/2025/f0d1b32d-8b7d-db6b-b7e9-393df29bd6bc.htm",
 63026|       "dll_signature_verified": true,
 63027|       "dll_relationship_scope": "declared",
 63028|       "dll_semantic_verified": null,
 63029|       "dll_verified_status": "signature_verified_declared",
 63030|       "revitlookup_referenced": null,
 63031|       "revitlookup_requires_document_context": null
 63032|     },
 63033|     {
 63034|       "source": "Autodesk.Revit.DB.ViewNavigationToolSettings",
 63035|       "target": "Autodesk.Revit.DB.HomeCamera",
 63036|       "member_name": "GetHomeCamera",
 63037|       "member_kind": "method",
 63038|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 63039|       "confidence": "direct_return_type",
 63040|       "confidence_tier": "unverified_reference",
 63041|       "target_resolution": "exact",
 63042|       "evidence": [
 63043|         "return type 'HomeCamera' directly names a Revit DB object type"
 63044|       ],
 63045|       "source_url": "https://www.revitapidocs.com/2025/dadadb67-e2f8-f2f4-0346-742cea12452f.htm",
 63046|       "dll_signature_verified": true,
 63047|       "dll_relationship_scope": "declared",
 63048|       "dll_semantic_verified": null,
 63049|       "dll_verified_status": "signature_verified_declared",
 63050|       "revitlookup_referenced": null,
 63051|       "revitlookup_requires_document_context": null
 63052|     },
 63053|     {
 63054|       "source": "Autodesk.Revit.DB.ViewNode",
 63055|       "target": "Autodesk.Revit.DB.Level",
 63056|       "member_name": "LevelOfDetail",
 63057|       "member_kind": "property",
 63058|       "edge_type": "ASSIGNED_TO_LEVEL",
 63059|       "confidence": "name_only_candidate",
 63060|       "confidence_tier": "likely",
 63061|       "target_resolution": "exact",
 63062|       "evidence": [
 63063|         "member name 'LevelOfDetail' matches keyword pattern /Level/ but return type 'int' gives no type-level confirmation"
 63064|       ],
 63065|       "source_url": "https://www.revitapidocs.com/2025/d98987f3-27a6-1893-3b7d-fc28e8ed5322.htm",
 63066|       "dll_signature_verified": true,
 63067|       "dll_relationship_scope": "declared",
 63068|       "dll_semantic_verified": null,
 63069|       "dll_verified_status": "signature_verified_declared",
 63070|       "revitlookup_referenced": null,
 63071|       "revitlookup_requires_document_context": null
 63072|     },
 63073|     {
 63074|       "source": "Autodesk.Revit.DB.ViewNode",
 63075|       "target": "Autodesk.Revit.DB.View",
 63076|       "member_name": "ViewId",
 63077|       "member_kind": "property",
 63078|       "edge_type": "REFERENCES",
 63079|       "confidence": "elementid_with_strong_name",
 63080|       "confidence_tier": "core",
 63081|       "target_resolution": "exact",
 63082|       "evidence": [
 63083|         "member name 'ViewId' matches keyword pattern /^(Get)?ViewId$/"
 63084|       ],
 63085|       "source_url": "https://www.revitapidocs.com/2025/a4efcf3d-f109-1e84-2010-025bebcff6bb.htm",
 63086|       "dll_signature_verified": true,
 63087|       "dll_relationship_scope": "declared",
 63088|       "dll_semantic_verified": null,
 63089|       "dll_verified_status": "signature_verified_declared",
 63090|       "revitlookup_referenced": null,
 63091|       "revitlookup_requires_document_context": null
 63092|     },
 63093|     {
 63094|       "source": "Autodesk.Revit.DB.ViewNode",
 63095|       "target": "Autodesk.Revit.DB.CameraInfo",
 63096|       "member_name": "GetCameraInfo",
 63097|       "member_kind": "method",
 63098|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 63099|       "confidence": "direct_return_type",
 63100|       "confidence_tier": "unverified_reference",
 63101|       "target_resolution": "exact",
 63102|       "evidence": [
 63103|         "return type 'CameraInfo' directly names a Revit DB object type"
 63104|       ],
 63105|       "source_url": "https://www.revitapidocs.com/2025/6f289011-b656-49be-023b-b5c07493e3ac.htm",
 63106|       "dll_signature_verified": true,
 63107|       "dll_relationship_scope": "declared",
 63108|       "dll_semantic_verified": null,
 63109|       "dll_verified_status": "signature_verified_declared",
 63110|       "revitlookup_referenced": null,
 63111|       "revitlookup_requires_document_context": null
 63112|     },
 63113|     {
 63114|       "source": "Autodesk.Revit.DB.ViewPlan",
 63115|       "target": "Autodesk.Revit.DB.AreaScheme",
 63116|       "member_name": "AreaScheme",
 63117|       "member_kind": "property",
 63118|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 63119|       "confidence": "direct_return_type",
 63120|       "confidence_tier": "unverified_reference",
 63121|       "target_resolution": "exact",
 63122|       "evidence": [
 63123|         "return type 'AreaScheme' directly names a Revit DB object type"
 63124|       ],
 63125|       "source_url": "https://www.revitapidocs.com/2025/c95cd019-a300-693d-dea8-cb92719cbca0.htm",
 63126|       "dll_signature_verified": true,
 63127|       "dll_relationship_scope": "declared",
 63128|       "dll_semantic_verified": null,
 63129|       "dll_verified_status": "signature_verified_declared",
 63130|       "revitlookup_referenced": null,
 63131|       "revitlookup_requires_document_context": null
 63132|     },
 63133|     {
 63134|       "source": "Autodesk.Revit.DB.ViewPlan",
 63135|       "target": "Autodesk.Revit.DB.PlanViewRangeError",
 63136|       "member_name": "CheckPlanViewRangeValidity",
 63137|       "member_kind": "method",
 63138|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 63139|       "confidence": "needs_runtime_validation",
 63140|       "confidence_tier": "needs_validation",
 63141|       "target_resolution": "exact",
 63142|       "evidence": [
 63143|         "return type 'IList < PlanViewRangeError >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 63144|       ],
 63145|       "source_url": "https://www.revitapidocs.com/2025/2a4f0854-2b24-1f86-423c-0e4ea4ddc9f7.htm",
 63146|       "dll_signature_verified": true,
 63147|       "dll_relationship_scope": "declared",
 63148|       "dll_semantic_verified": null,
 63149|       "dll_verified_status": "signature_verified_declared",
 63150|       "revitlookup_referenced": null,
 63151|       "revitlookup_requires_document_context": null
 63152|     },
 63153|     {
 63154|       "source": "Autodesk.Revit.DB.ViewPlan",
 63155|       "target": "Autodesk.Revit.DB.Level",
 63156|       "member_name": "GetUnderlayBaseLevel",
 63157|       "member_kind": "method",
 63158|       "edge_type": "ASSIGNED_TO_LEVEL",
 63159|       "confidence": "elementid_with_strong_name",
 63160|       "confidence_tier": "core",
 63161|       "target_resolution": "exact",
 63162|       "evidence": [
 63163|         "member name 'GetUnderlayBaseLevel' matches keyword pattern /Level/"
 63164|       ],
 63165|       "source_url": "https://www.revitapidocs.com/2025/5f57beea-7d43-f37f-6843-6e9ad882f90e.htm",
 63166|       "dll_signature_verified": true,
 63167|       "dll_relationship_scope": "declared",
 63168|       "dll_semantic_verified": null,
 63169|       "dll_verified_status": "signature_verified_declared",
 63170|       "revitlookup_referenced": null,
 63171|       "revitlookup_requires_document_context": null
 63172|     },
 63173|     {
 63174|       "source": "Autodesk.Revit.DB.ViewPlan",
 63175|       "target": "Autodesk.Revit.DB.Level",
 63176|       "member_name": "GetUnderlayTopLevel",
 63177|       "member_kind": "method",
 63178|       "edge_type": "ASSIGNED_TO_LEVEL",
 63179|       "confidence": "elementid_with_strong_name",
 63180|       "confidence_tier": "core",
 63181|       "target_resolution": "exact",
 63182|       "evidence": [
 63183|         "member name 'GetUnderlayTopLevel' matches keyword pattern /Level/"
 63184|       ],
 63185|       "source_url": "https://www.revitapidocs.com/2025/5d401ec0-ead1-39bd-459b-2dca075b2797.htm",
 63186|       "dll_signature_verified": true,
 63187|       "dll_relationship_scope": "declared",
 63188|       "dll_semantic_verified": null,
 63189|       "dll_verified_status": "signature_verified_declared",
 63190|       "revitlookup_referenced": null,
```
