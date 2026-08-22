# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 206 of 216
- Original line range: 79951-80350
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 79951|       "member_kind": "method",
 79952|       "edge_type": "TAGS_ELEMENT",
 79953|       "confidence": "name_only_candidate",
 79954|       "confidence_tier": "likely",
 79955|       "target_resolution": "none",
 79956|       "evidence": [
 79957|         "member name 'SetTagRelativePosition' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'void' gives no type-level confirmation"
 79958|       ],
 79959|       "source_url": "https://www.revitapidocs.com/2025/519b5d16-6641-bdd8-82b9-cac6df1a4c34.htm",
 79960|       "dll_signature_verified": true,
 79961|       "dll_relationship_scope": "declared",
 79962|       "dll_semantic_verified": null,
 79963|       "dll_verified_status": "signature_verified_declared",
 79964|       "revitlookup_referenced": null,
 79965|       "revitlookup_requires_document_context": null
 79966|     },
 79967|     {
 79968|       "source": "Autodesk.Revit.DB.Structure.RebarBendingDetail",
 79969|       "target": null,
 79970|       "member_name": "SetTagRelativeRotation",
 79971|       "member_kind": "method",
 79972|       "edge_type": "TAGS_ELEMENT",
 79973|       "confidence": "name_only_candidate",
 79974|       "confidence_tier": "likely",
 79975|       "target_resolution": "none",
 79976|       "evidence": [
 79977|         "member name 'SetTagRelativeRotation' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'void' gives no type-level confirmation"
 79978|       ],
 79979|       "source_url": "https://www.revitapidocs.com/2025/644b2d10-91f4-9bb4-2465-e76c574b4b7f.htm",
 79980|       "dll_signature_verified": true,
 79981|       "dll_relationship_scope": "declared",
 79982|       "dll_semantic_verified": null,
 79983|       "dll_verified_status": "signature_verified_declared",
 79984|       "revitlookup_referenced": null,
 79985|       "revitlookup_requires_document_context": null
 79986|     },
 79987|     {
 79988|       "source": "Autodesk.Revit.DB.Structure.RebarBendingDetailType",
 79989|       "target": null,
 79990|       "member_name": "AngularDimensionTypeId",
 79991|       "member_kind": "property",
 79992|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 79993|       "confidence": "unknown_reference",
 79994|       "confidence_tier": "unverified_reference",
 79995|       "target_resolution": "none",
 79996|       "evidence": [
 79997|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 79998|       ],
 79999|       "source_url": "https://www.revitapidocs.com/2025/6c5767a6-42da-c18f-b21a-785d9222e1bc.htm",
 80000|       "dll_signature_verified": true,
 80001|       "dll_relationship_scope": "declared",
 80002|       "dll_semantic_verified": null,
 80003|       "dll_verified_status": "signature_verified_declared",
 80004|       "revitlookup_referenced": null,
 80005|       "revitlookup_requires_document_context": null
 80006|     },
 80007|     {
 80008|       "source": "Autodesk.Revit.DB.Structure.RebarBendingDetailType",
 80009|       "target": "Autodesk.Revit.DB.Level",
 80010|       "member_name": "DetailLevel",
 80011|       "member_kind": "property",
 80012|       "edge_type": "ASSIGNED_TO_LEVEL",
 80013|       "confidence": "name_only_candidate",
 80014|       "confidence_tier": "likely",
 80015|       "target_resolution": "exact",
 80016|       "evidence": [
 80017|         "member name 'DetailLevel' matches keyword pattern /Level/ but return type 'BendingDetailLevelOfDetail' gives no type-level confirmation"
 80018|       ],
 80019|       "source_url": "https://www.revitapidocs.com/2025/29f3a6bf-aa33-8b8e-4e32-118e8bb71a34.htm",
 80020|       "dll_signature_verified": true,
 80021|       "dll_relationship_scope": "declared",
 80022|       "dll_semantic_verified": null,
 80023|       "dll_verified_status": "signature_verified_declared",
 80024|       "revitlookup_referenced": null,
 80025|       "revitlookup_requires_document_context": null
 80026|     },
 80027|     {
 80028|       "source": "Autodesk.Revit.DB.Structure.RebarBendingDetailType",
 80029|       "target": null,
 80030|       "member_name": "DiameterDimensionTypeId",
 80031|       "member_kind": "property",
 80032|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 80033|       "confidence": "unknown_reference",
 80034|       "confidence_tier": "unverified_reference",
 80035|       "target_resolution": "none",
 80036|       "evidence": [
 80037|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 80038|       ],
 80039|       "source_url": "https://www.revitapidocs.com/2025/ce5309b8-be6b-fdbd-49fe-8c3be5021dfc.htm",
 80040|       "dll_signature_verified": true,
 80041|       "dll_relationship_scope": "declared",
 80042|       "dll_semantic_verified": null,
 80043|       "dll_verified_status": "signature_verified_declared",
 80044|       "revitlookup_referenced": null,
 80045|       "revitlookup_requires_document_context": null
 80046|     },
 80047|     {
 80048|       "source": "Autodesk.Revit.DB.Structure.RebarBendingDetailType",
 80049|       "target": null,
 80050|       "member_name": "RadialDimensionTypeId",
 80051|       "member_kind": "property",
 80052|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 80053|       "confidence": "unknown_reference",
 80054|       "confidence_tier": "unverified_reference",
 80055|       "target_resolution": "none",
 80056|       "evidence": [
 80057|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 80058|       ],
 80059|       "source_url": "https://www.revitapidocs.com/2025/8153e630-206d-bdbb-dad3-764316ece9ca.htm",
 80060|       "dll_signature_verified": true,
 80061|       "dll_relationship_scope": "declared",
 80062|       "dll_semantic_verified": null,
 80063|       "dll_verified_status": "signature_verified_declared",
 80064|       "revitlookup_referenced": null,
 80065|       "revitlookup_requires_document_context": null
 80066|     },
 80067|     {
 80068|       "source": "Autodesk.Revit.DB.Structure.RebarBendingDetailType",
 80069|       "target": null,
 80070|       "member_name": "SegmentLengthDimensionTypeId",
 80071|       "member_kind": "property",
 80072|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 80073|       "confidence": "unknown_reference",
 80074|       "confidence_tier": "unverified_reference",
 80075|       "target_resolution": "none",
 80076|       "evidence": [
 80077|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 80078|       ],
 80079|       "source_url": "https://www.revitapidocs.com/2025/09404c84-c732-376c-6da7-146bd8c6f14e.htm",
 80080|       "dll_signature_verified": true,
 80081|       "dll_relationship_scope": "declared",
 80082|       "dll_semantic_verified": null,
 80083|       "dll_verified_status": "signature_verified_declared",
 80084|       "revitlookup_referenced": null,
 80085|       "revitlookup_requires_document_context": null
 80086|     },
 80087|     {
 80088|       "source": "Autodesk.Revit.DB.Structure.RebarBendingDetailType",
 80089|       "target": null,
 80090|       "member_name": "TagTypeId",
 80091|       "member_kind": "property",
 80092|       "edge_type": "TAGS_ELEMENT",
 80093|       "confidence": "elementid_with_strong_name",
 80094|       "confidence_tier": "core",
 80095|       "target_resolution": "none",
 80096|       "evidence": [
 80097|         "member name 'TagTypeId' matches keyword pattern /^GetTagged|Tag(ged)?/"
 80098|       ],
 80099|       "source_url": "https://www.revitapidocs.com/2025/be04318d-4da8-9bf5-cd47-62968c586ab4.htm",
 80100|       "dll_signature_verified": true,
 80101|       "dll_relationship_scope": "declared",
 80102|       "dll_semantic_verified": null,
 80103|       "dll_verified_status": "signature_verified_declared",
 80104|       "revitlookup_referenced": null,
 80105|       "revitlookup_requires_document_context": null
 80106|     },
 80107|     {
 80108|       "source": "Autodesk.Revit.DB.Structure.RebarConstrainedHandle",
 80109|       "target": null,
 80110|       "member_name": "GetCustomHandleTag",
 80111|       "member_kind": "method",
 80112|       "edge_type": "TAGS_ELEMENT",
 80113|       "confidence": "name_only_candidate",
 80114|       "confidence_tier": "likely",
 80115|       "target_resolution": "none",
 80116|       "evidence": [
 80117|         "member name 'GetCustomHandleTag' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'int' gives no type-level confirmation"
 80118|       ],
 80119|       "source_url": "https://www.revitapidocs.com/2025/d7552c41-e1e7-c891-c609-7da444492de7.htm",
 80120|       "dll_signature_verified": true,
 80121|       "dll_relationship_scope": "declared",
 80122|       "dll_semantic_verified": null,
 80123|       "dll_verified_status": "signature_verified_declared",
 80124|       "revitlookup_referenced": null,
 80125|       "revitlookup_requires_document_context": null
 80126|     },
 80127|     {
 80128|       "source": "Autodesk.Revit.DB.Structure.RebarConstrainedHandle",
 80129|       "target": "Autodesk.Revit.DB.Surface",
 80130|       "member_name": "GetHandleSurface",
 80131|       "member_kind": "method",
 80132|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80133|       "confidence": "direct_return_type",
 80134|       "confidence_tier": "unverified_reference",
 80135|       "target_resolution": "exact",
 80136|       "evidence": [
 80137|         "return type 'Surface' directly names a Revit DB object type"
 80138|       ],
 80139|       "source_url": "https://www.revitapidocs.com/2025/d290f074-1c4c-b7bf-2d2f-32741d37dc1d.htm",
 80140|       "dll_signature_verified": true,
 80141|       "dll_relationship_scope": "declared",
 80142|       "dll_semantic_verified": null,
 80143|       "dll_verified_status": "signature_verified_declared",
 80144|       "revitlookup_referenced": null,
 80145|       "revitlookup_requires_document_context": null
 80146|     },
 80147|     {
 80148|       "source": "Autodesk.Revit.DB.Structure.RebarConstrainedHandle",
 80149|       "target": "Autodesk.Revit.DB.Structure.RebarHandleBehavior",
 80150|       "member_name": "GetPossibleHandleBehaviors",
 80151|       "member_kind": "method",
 80152|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80153|       "confidence": "needs_runtime_validation",
 80154|       "confidence_tier": "needs_validation",
 80155|       "target_resolution": "short_name_fallback",
 80156|       "evidence": [
 80157|         "return type 'IList < RebarHandleBehavior >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 80158|       ],
 80159|       "source_url": "https://www.revitapidocs.com/2025/b24adfe4-ce0f-fcc9-7de2-4884431a67f4.htm",
 80160|       "dll_signature_verified": true,
 80161|       "dll_relationship_scope": "declared",
 80162|       "dll_semantic_verified": null,
 80163|       "dll_verified_status": "signature_verified_declared",
 80164|       "revitlookup_referenced": null,
 80165|       "revitlookup_requires_document_context": null
 80166|     },
 80167|     {
 80168|       "source": "Autodesk.Revit.DB.Structure.RebarConstraint",
 80169|       "target": null,
 80170|       "member_name": "GetCustomHandleTag",
 80171|       "member_kind": "method",
 80172|       "edge_type": "TAGS_ELEMENT",
 80173|       "confidence": "name_only_candidate",
 80174|       "confidence_tier": "likely",
 80175|       "target_resolution": "none",
 80176|       "evidence": [
 80177|         "member name 'GetCustomHandleTag' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'int' gives no type-level confirmation"
 80178|       ],
 80179|       "source_url": "https://www.revitapidocs.com/2025/019e2c30-c247-2d8a-476b-25be0f547dbb.htm",
 80180|       "dll_signature_verified": true,
 80181|       "dll_relationship_scope": "declared",
 80182|       "dll_semantic_verified": null,
 80183|       "dll_verified_status": "signature_verified_declared",
 80184|       "revitlookup_referenced": null,
 80185|       "revitlookup_requires_document_context": null
 80186|     },
 80187|     {
 80188|       "source": "Autodesk.Revit.DB.Structure.RebarConstraint",
 80189|       "target": null,
 80190|       "member_name": "GetDistanceToTargetHostFace",
 80191|       "member_kind": "method",
 80192|       "edge_type": "HOSTED_BY",
 80193|       "confidence": "name_only_candidate",
 80194|       "confidence_tier": "likely",
 80195|       "target_resolution": "none",
 80196|       "evidence": [
 80197|         "member name 'GetDistanceToTargetHostFace' matches keyword pattern /^GetHosted|Host/ but return type 'double' gives no type-level confirmation"
 80198|       ],
 80199|       "source_url": "https://www.revitapidocs.com/2025/9859e4a1-a5d4-a1e6-d28b-2e69799f2c10.htm",
 80200|       "dll_signature_verified": true,
 80201|       "dll_relationship_scope": "declared",
 80202|       "dll_semantic_verified": null,
 80203|       "dll_verified_status": "signature_verified_declared",
 80204|       "revitlookup_referenced": null,
 80205|       "revitlookup_requires_document_context": null
 80206|     },
 80207|     {
 80208|       "source": "Autodesk.Revit.DB.Structure.RebarConstraint",
 80209|       "target": "Autodesk.Revit.DB.Structure.RebarConstrainedHandle",
 80210|       "member_name": "GetRebarConstrainedHandle",
 80211|       "member_kind": "method",
 80212|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80213|       "confidence": "direct_return_type",
 80214|       "confidence_tier": "unverified_reference",
 80215|       "target_resolution": "short_name_fallback",
 80216|       "evidence": [
 80217|         "return type 'RebarConstrainedHandle' directly names a Revit DB object type"
 80218|       ],
 80219|       "source_url": "https://www.revitapidocs.com/2025/f8163c27-a777-a46b-9a24-2b31e6490291.htm",
 80220|       "dll_signature_verified": true,
 80221|       "dll_relationship_scope": "declared",
 80222|       "dll_semantic_verified": null,
 80223|       "dll_verified_status": "signature_verified_declared",
 80224|       "revitlookup_referenced": null,
 80225|       "revitlookup_requires_document_context": null
 80226|     },
 80227|     {
 80228|       "source": "Autodesk.Revit.DB.Structure.RebarConstraint",
 80229|       "target": null,
 80230|       "member_name": "GetRebarConstraintTargetHostFaceType",
 80231|       "member_kind": "method",
 80232|       "edge_type": "HOSTED_BY",
 80233|       "confidence": "name_only_candidate",
 80234|       "confidence_tier": "likely",
 80235|       "target_resolution": "none",
 80236|       "evidence": [
 80237|         "member name 'GetRebarConstraintTargetHostFaceType' matches keyword pattern /^GetHosted|Host/ but return type 'RebarConstraintTargetHostFaceType' gives no type-level confirmation"
 80238|       ],
 80239|       "source_url": "https://www.revitapidocs.com/2025/6446870e-2774-3d2f-cb78-0cb39e7dada4.htm",
 80240|       "dll_signature_verified": true,
 80241|       "dll_relationship_scope": "declared",
 80242|       "dll_semantic_verified": null,
 80243|       "dll_verified_status": "signature_verified_declared",
 80244|       "revitlookup_referenced": null,
 80245|       "revitlookup_requires_document_context": null
 80246|     },
 80247|     {
 80248|       "source": "Autodesk.Revit.DB.Structure.RebarConstraint",
 80249|       "target": null,
 80250|       "member_name": "GetRebarConstraintTargetHostFaceType",
 80251|       "member_kind": "method",
 80252|       "edge_type": "HOSTED_BY",
 80253|       "confidence": "name_only_candidate",
 80254|       "confidence_tier": "likely",
 80255|       "target_resolution": "none",
 80256|       "evidence": [
 80257|         "member name 'GetRebarConstraintTargetHostFaceType' matches keyword pattern /^GetHosted|Host/ but return type 'RebarConstraintTargetHostFaceType' gives no type-level confirmation"
 80258|       ],
 80259|       "source_url": "https://www.revitapidocs.com/2025/f58fe52a-639f-8101-8c9d-fe2354a755d0.htm",
 80260|       "dll_signature_verified": true,
 80261|       "dll_relationship_scope": "declared",
 80262|       "dll_semantic_verified": null,
 80263|       "dll_verified_status": "signature_verified_declared",
 80264|       "revitlookup_referenced": null,
 80265|       "revitlookup_requires_document_context": null
 80266|     },
 80267|     {
 80268|       "source": "Autodesk.Revit.DB.Structure.RebarConstraint",
 80269|       "target": "Autodesk.Revit.DB.Surface",
 80270|       "member_name": "GetSurfaceForConstraintToSurface",
 80271|       "member_kind": "method",
 80272|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80273|       "confidence": "direct_return_type",
 80274|       "confidence_tier": "unverified_reference",
 80275|       "target_resolution": "exact",
 80276|       "evidence": [
 80277|         "return type 'Surface' directly names a Revit DB object type"
 80278|       ],
 80279|       "source_url": "https://www.revitapidocs.com/2025/8c025eeb-6bae-2455-0fba-0f24ac3a45c6.htm",
 80280|       "dll_signature_verified": true,
 80281|       "dll_relationship_scope": "declared",
 80282|       "dll_semantic_verified": null,
 80283|       "dll_verified_status": "signature_verified_declared",
 80284|       "revitlookup_referenced": null,
 80285|       "revitlookup_requires_document_context": null
 80286|     },
 80287|     {
 80288|       "source": "Autodesk.Revit.DB.Structure.RebarConstraint",
 80289|       "target": "Autodesk.Revit.DB.Structure.RebarCoverType",
 80290|       "member_name": "GetTargetCoverType",
 80291|       "member_kind": "method",
 80292|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80293|       "confidence": "direct_return_type",
 80294|       "confidence_tier": "unverified_reference",
 80295|       "target_resolution": "short_name_fallback",
 80296|       "evidence": [
 80297|         "return type 'RebarCoverType' directly names a Revit DB object type"
 80298|       ],
 80299|       "source_url": "https://www.revitapidocs.com/2025/f5ef3d50-d753-2598-7a12-74cc1cb569fa.htm",
 80300|       "dll_signature_verified": true,
 80301|       "dll_relationship_scope": "declared",
 80302|       "dll_semantic_verified": null,
 80303|       "dll_verified_status": "signature_verified_declared",
 80304|       "revitlookup_referenced": null,
 80305|       "revitlookup_requires_document_context": null
 80306|     },
 80307|     {
 80308|       "source": "Autodesk.Revit.DB.Structure.RebarConstraint",
 80309|       "target": "Autodesk.Revit.DB.Element",
 80310|       "member_name": "GetTargetElement",
 80311|       "member_kind": "method",
 80312|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80313|       "confidence": "direct_return_type",
 80314|       "confidence_tier": "unverified_reference",
 80315|       "target_resolution": "exact",
 80316|       "evidence": [
 80317|         "return type 'Element' directly names a Revit DB object type"
 80318|       ],
 80319|       "source_url": "https://www.revitapidocs.com/2025/75975a79-d608-9210-dbd7-0099a046fa3d.htm",
 80320|       "dll_signature_verified": true,
 80321|       "dll_relationship_scope": "declared",
 80322|       "dll_semantic_verified": null,
 80323|       "dll_verified_status": "signature_verified_declared",
 80324|       "revitlookup_referenced": null,
 80325|       "revitlookup_requires_document_context": null
 80326|     },
 80327|     {
 80328|       "source": "Autodesk.Revit.DB.Structure.RebarConstraint",
 80329|       "target": "Autodesk.Revit.DB.Element",
 80330|       "member_name": "GetTargetElement",
 80331|       "member_kind": "method",
 80332|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 80333|       "confidence": "direct_return_type",
 80334|       "confidence_tier": "unverified_reference",
 80335|       "target_resolution": "exact",
 80336|       "evidence": [
 80337|         "return type 'Element' directly names a Revit DB object type"
 80338|       ],
 80339|       "source_url": "https://www.revitapidocs.com/2025/f20b6107-6c40-860d-2445-4c2fcbde3f29.htm",
 80340|       "dll_signature_verified": true,
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
```
