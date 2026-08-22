# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 175 of 216
- Original line range: 67861-68260
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 67861|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67862|       "confidence": "direct_return_type",
 67863|       "confidence_tier": "unverified_reference",
 67864|       "target_resolution": "short_name_fallback",
 67865|       "evidence": [
 67866|         "return type 'Stairs' directly names a Revit DB object type"
 67867|       ],
 67868|       "source_url": "https://www.revitapidocs.com/2025/f1eb4c84-2b7e-1b6a-dc8b-dfc6c0c994c9.htm",
 67869|       "dll_signature_verified": true,
 67870|       "dll_relationship_scope": "declared",
 67871|       "dll_semantic_verified": null,
 67872|       "dll_verified_status": "signature_verified_declared",
 67873|       "revitlookup_referenced": null,
 67874|       "revitlookup_requires_document_context": null
 67875|     },
 67876|     {
 67877|       "source": "Autodesk.Revit.DB.Architecture.NonContinuousRailInfo",
 67878|       "target": "Autodesk.Revit.DB.Material",
 67879|       "member_name": "MaterialId",
 67880|       "member_kind": "property",
 67881|       "edge_type": "USES_MATERIAL",
 67882|       "confidence": "elementid_with_strong_name",
 67883|       "confidence_tier": "core",
 67884|       "target_resolution": "exact",
 67885|       "evidence": [
 67886|         "member name 'MaterialId' matches keyword pattern /Material/"
 67887|       ],
 67888|       "source_url": "https://www.revitapidocs.com/2025/19ca7c1d-bcb8-dc0c-908a-da31bf61682c.htm",
 67889|       "dll_signature_verified": true,
 67890|       "dll_relationship_scope": "declared",
 67891|       "dll_semantic_verified": null,
 67892|       "dll_verified_status": "signature_verified_declared",
 67893|       "revitlookup_referenced": null,
 67894|       "revitlookup_requires_document_context": null
 67895|     },
 67896|     {
 67897|       "source": "Autodesk.Revit.DB.Architecture.NonContinuousRailInfo",
 67898|       "target": null,
 67899|       "member_name": "ProfileId",
 67900|       "member_kind": "property",
 67901|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 67902|       "confidence": "unknown_reference",
 67903|       "confidence_tier": "unverified_reference",
 67904|       "target_resolution": "none",
 67905|       "evidence": [
 67906|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 67907|       ],
 67908|       "source_url": "https://www.revitapidocs.com/2025/d7125deb-176d-1ff7-5b36-ace635da5703.htm",
 67909|       "dll_signature_verified": true,
 67910|       "dll_relationship_scope": "declared",
 67911|       "dll_semantic_verified": null,
 67912|       "dll_verified_status": "signature_verified_declared",
 67913|       "revitlookup_referenced": null,
 67914|       "revitlookup_requires_document_context": null
 67915|     },
 67916|     {
 67917|       "source": "Autodesk.Revit.DB.Architecture.NonContinuousRailInfo",
 67918|       "target": "Autodesk.Revit.DB.Material",
 67919|       "member_name": "IsValidNonContinuousRailMaterial",
 67920|       "member_kind": "method",
 67921|       "edge_type": "USES_MATERIAL",
 67922|       "confidence": "name_only_candidate",
 67923|       "confidence_tier": "likely",
 67924|       "target_resolution": "exact",
 67925|       "evidence": [
 67926|         "member name 'IsValidNonContinuousRailMaterial' matches keyword pattern /Material/ but return type 'bool' gives no type-level confirmation"
 67927|       ],
 67928|       "source_url": "https://www.revitapidocs.com/2025/646e7c56-e9e2-78db-9e2a-dda5b9cb04bd.htm",
 67929|       "dll_signature_verified": true,
 67930|       "dll_relationship_scope": "declared",
 67931|       "dll_semantic_verified": null,
 67932|       "dll_verified_status": "signature_verified_declared",
 67933|       "revitlookup_referenced": null,
 67934|       "revitlookup_requires_document_context": null
 67935|     },
 67936|     {
 67937|       "source": "Autodesk.Revit.DB.Architecture.NonContinuousRailStructure",
 67938|       "target": "Autodesk.Revit.DB.Architecture.NonContinuousRailInfo",
 67939|       "member_name": "AddNonContinuousRail",
 67940|       "member_kind": "method",
 67941|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67942|       "confidence": "direct_return_type",
 67943|       "confidence_tier": "unverified_reference",
 67944|       "target_resolution": "short_name_fallback",
 67945|       "evidence": [
 67946|         "return type 'NonContinuousRailInfo' directly names a Revit DB object type"
 67947|       ],
 67948|       "source_url": "https://www.revitapidocs.com/2025/91ed215d-bf6d-5398-4082-49e35a470785.htm",
 67949|       "dll_signature_verified": true,
 67950|       "dll_relationship_scope": "declared",
 67951|       "dll_semantic_verified": null,
 67952|       "dll_verified_status": "signature_verified_declared",
 67953|       "revitlookup_referenced": null,
 67954|       "revitlookup_requires_document_context": null
 67955|     },
 67956|     {
 67957|       "source": "Autodesk.Revit.DB.Architecture.NonContinuousRailStructure",
 67958|       "target": "Autodesk.Revit.DB.Architecture.NonContinuousRailInfo",
 67959|       "member_name": "GetNonContinuousRail",
 67960|       "member_kind": "method",
 67961|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67962|       "confidence": "direct_return_type",
 67963|       "confidence_tier": "unverified_reference",
 67964|       "target_resolution": "short_name_fallback",
 67965|       "evidence": [
 67966|         "return type 'NonContinuousRailInfo' directly names a Revit DB object type"
 67967|       ],
 67968|       "source_url": "https://www.revitapidocs.com/2025/813921fb-3f6c-9db9-a4df-1ddc3dc55fa7.htm",
 67969|       "dll_signature_verified": true,
 67970|       "dll_relationship_scope": "declared",
 67971|       "dll_semantic_verified": null,
 67972|       "dll_verified_status": "signature_verified_declared",
 67973|       "revitlookup_referenced": null,
 67974|       "revitlookup_requires_document_context": null
 67975|     },
 67976|     {
 67977|       "source": "Autodesk.Revit.DB.Architecture.PostPattern",
 67978|       "target": "Autodesk.Revit.DB.Architecture.BalusterInfo",
 67979|       "member_name": "CornerPost",
 67980|       "member_kind": "property",
 67981|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 67982|       "confidence": "direct_return_type",
 67983|       "confidence_tier": "unverified_reference",
 67984|       "target_resolution": "short_name_fallback",
 67985|       "evidence": [
 67986|         "return type 'BalusterInfo' directly names a Revit DB object type"
 67987|       ],
 67988|       "source_url": "https://www.revitapidocs.com/2025/e17620b0-0801-31a1-9ec1-5c4fdc417e9e.htm",
 67989|       "dll_signature_verified": true,
 67990|       "dll_relationship_scope": "declared",
 67991|       "dll_semantic_verified": null,
 67992|       "dll_verified_status": "signature_verified_declared",
 67993|       "revitlookup_referenced": null,
 67994|       "revitlookup_requires_document_context": null
 67995|     },
 67996|     {
 67997|       "source": "Autodesk.Revit.DB.Architecture.PostPattern",
 67998|       "target": "Autodesk.Revit.DB.Architecture.BalusterInfo",
 67999|       "member_name": "EndPost",
 68000|       "member_kind": "property",
 68001|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 68002|       "confidence": "direct_return_type",
 68003|       "confidence_tier": "unverified_reference",
 68004|       "target_resolution": "short_name_fallback",
 68005|       "evidence": [
 68006|         "return type 'BalusterInfo' directly names a Revit DB object type"
 68007|       ],
 68008|       "source_url": "https://www.revitapidocs.com/2025/644d5e7c-1e19-a1e6-d306-7bcd6bc7a2fc.htm",
 68009|       "dll_signature_verified": true,
 68010|       "dll_relationship_scope": "declared",
 68011|       "dll_semantic_verified": null,
 68012|       "dll_verified_status": "signature_verified_declared",
 68013|       "revitlookup_referenced": null,
 68014|       "revitlookup_requires_document_context": null
 68015|     },
 68016|     {
 68017|       "source": "Autodesk.Revit.DB.Architecture.PostPattern",
 68018|       "target": "Autodesk.Revit.DB.Architecture.BalusterInfo",
 68019|       "member_name": "StartPost",
 68020|       "member_kind": "property",
 68021|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 68022|       "confidence": "direct_return_type",
 68023|       "confidence_tier": "unverified_reference",
 68024|       "target_resolution": "short_name_fallback",
 68025|       "evidence": [
 68026|         "return type 'BalusterInfo' directly names a Revit DB object type"
 68027|       ],
 68028|       "source_url": "https://www.revitapidocs.com/2025/66b6c474-ba3a-6924-b00c-41b31702caf0.htm",
 68029|       "dll_signature_verified": true,
 68030|       "dll_relationship_scope": "declared",
 68031|       "dll_semantic_verified": null,
 68032|       "dll_verified_status": "signature_verified_declared",
 68033|       "revitlookup_referenced": null,
 68034|       "revitlookup_requires_document_context": null
 68035|     },
 68036|     {
 68037|       "source": "Autodesk.Revit.DB.Architecture.Railing",
 68038|       "target": null,
 68039|       "member_name": "HasHost",
 68040|       "member_kind": "property",
 68041|       "edge_type": "HOSTED_BY",
 68042|       "confidence": "name_only_candidate",
 68043|       "confidence_tier": "likely",
 68044|       "target_resolution": "none",
 68045|       "evidence": [
 68046|         "member name 'HasHost' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 68047|       ],
 68048|       "source_url": "https://www.revitapidocs.com/2025/c977dcea-a003-cd92-5381-91efe270d055.htm",
 68049|       "dll_signature_verified": true,
 68050|       "dll_relationship_scope": "declared",
 68051|       "dll_semantic_verified": null,
 68052|       "dll_verified_status": "signature_verified_declared",
 68053|       "revitlookup_referenced": null,
 68054|       "revitlookup_requires_document_context": null
 68055|     },
 68056|     {
 68057|       "source": "Autodesk.Revit.DB.Architecture.Railing",
 68058|       "target": null,
 68059|       "member_name": "HostId",
 68060|       "member_kind": "property",
 68061|       "edge_type": "HOSTED_BY",
 68062|       "confidence": "elementid_with_strong_name",
 68063|       "confidence_tier": "core",
 68064|       "target_resolution": "none",
 68065|       "evidence": [
 68066|         "member name 'HostId' matches keyword pattern /^GetHosted|Host/"
 68067|       ],
 68068|       "source_url": "https://www.revitapidocs.com/2025/4fe4b0e6-591d-70d1-10f7-e192393fae26.htm",
 68069|       "dll_signature_verified": true,
 68070|       "dll_relationship_scope": "declared",
 68071|       "dll_semantic_verified": null,
 68072|       "dll_verified_status": "signature_verified_declared",
 68073|       "revitlookup_referenced": null,
 68074|       "revitlookup_requires_document_context": null
 68075|     },
 68076|     {
 68077|       "source": "Autodesk.Revit.DB.Architecture.Railing",
 68078|       "target": null,
 68079|       "member_name": "TopRail",
 68080|       "member_kind": "property",
 68081|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 68082|       "confidence": "unknown_reference",
 68083|       "confidence_tier": "unverified_reference",
 68084|       "target_resolution": "none",
 68085|       "evidence": [
 68086|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 68087|       ],
 68088|       "source_url": "https://www.revitapidocs.com/2025/2cde1842-b57b-6864-2f4a-c4852303a152.htm",
 68089|       "dll_signature_verified": true,
 68090|       "dll_relationship_scope": "declared",
 68091|       "dll_semantic_verified": null,
 68092|       "dll_verified_status": "signature_verified_declared",
 68093|       "revitlookup_referenced": null,
 68094|       "revitlookup_requires_document_context": null
 68095|     },
 68096|     {
 68097|       "source": "Autodesk.Revit.DB.Architecture.Railing",
 68098|       "target": null,
 68099|       "member_name": "GetHandRails",
 68100|       "member_kind": "method",
 68101|       "edge_type": "RETURNS_ELEMENT_IDS",
 68102|       "confidence": "unknown_reference",
 68103|       "confidence_tier": "unverified_reference",
 68104|       "target_resolution": "none",
 68105|       "evidence": [
 68106|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 68107|       ],
 68108|       "source_url": "https://www.revitapidocs.com/2025/7c33a83c-7114-6caf-16eb-275e0c98c330.htm",
 68109|       "dll_signature_verified": true,
 68110|       "dll_relationship_scope": "declared",
 68111|       "dll_semantic_verified": null,
 68112|       "dll_verified_status": "signature_verified_declared",
 68113|       "revitlookup_referenced": null,
 68114|       "revitlookup_requires_document_context": null
 68115|     },
 68116|     {
 68117|       "source": "Autodesk.Revit.DB.Architecture.Railing",
 68118|       "target": "Autodesk.Revit.DB.Level",
 68119|       "member_name": "GetMultistoryStairsPlacementLevels",
 68120|       "member_kind": "method",
 68121|       "edge_type": "ASSIGNED_TO_LEVEL",
 68122|       "confidence": "elementid_collection_with_strong_name",
 68123|       "confidence_tier": "core",
 68124|       "target_resolution": "exact",
 68125|       "evidence": [
 68126|         "member name 'GetMultistoryStairsPlacementLevels' matches keyword pattern /Level/"
 68127|       ],
 68128|       "source_url": "https://www.revitapidocs.com/2025/aac188d3-7c62-e74c-3579-cdcb8dc5c7f3.htm",
 68129|       "dll_signature_verified": true,
 68130|       "dll_relationship_scope": "declared",
 68131|       "dll_semantic_verified": null,
 68132|       "dll_verified_status": "signature_verified_declared",
 68133|       "revitlookup_referenced": null,
 68134|       "revitlookup_requires_document_context": null
 68135|     },
 68136|     {
 68137|       "source": "Autodesk.Revit.DB.Architecture.Railing",
 68138|       "target": "Autodesk.Revit.DB.Subelement",
 68139|       "member_name": "GetSubelementOnLevel",
 68140|       "member_kind": "method",
 68141|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 68142|       "confidence": "direct_return_type",
 68143|       "confidence_tier": "unverified_reference",
 68144|       "target_resolution": "exact",
 68145|       "evidence": [
 68146|         "member name 'GetSubelementOnLevel' matches keyword pattern /Level/ implying target 'Level', but the actual return type 'Subelement' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 68147|         "return type 'Subelement' directly names a Revit DB object type"
 68148|       ],
 68149|       "source_url": "https://www.revitapidocs.com/2025/b9d35dc7-42d9-1699-4985-bc80fe2c7045.htm",
 68150|       "dll_signature_verified": true,
 68151|       "dll_relationship_scope": "declared",
 68152|       "dll_semantic_verified": null,
 68153|       "dll_verified_status": "signature_verified_declared",
 68154|       "revitlookup_referenced": null,
 68155|       "revitlookup_requires_document_context": null
 68156|     },
 68157|     {
 68158|       "source": "Autodesk.Revit.DB.Architecture.Railing",
 68159|       "target": null,
 68160|       "member_name": "IsValidHostForNewRailing",
 68161|       "member_kind": "method",
 68162|       "edge_type": "HOSTED_BY",
 68163|       "confidence": "name_only_candidate",
 68164|       "confidence_tier": "likely",
 68165|       "target_resolution": "none",
 68166|       "evidence": [
 68167|         "member name 'IsValidHostForNewRailing' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 68168|       ],
 68169|       "source_url": "https://www.revitapidocs.com/2025/cc1aaa43-f61a-e65d-eac9-43370200295a.htm",
 68170|       "dll_signature_verified": true,
 68171|       "dll_relationship_scope": "declared",
 68172|       "dll_semantic_verified": null,
 68173|       "dll_verified_status": "signature_verified_declared",
 68174|       "revitlookup_referenced": null,
 68175|       "revitlookup_requires_document_context": null
 68176|     },
 68177|     {
 68178|       "source": "Autodesk.Revit.DB.Architecture.Railing",
 68179|       "target": null,
 68180|       "member_name": "RailingCanBeHostedByElement",
 68181|       "member_kind": "method",
 68182|       "edge_type": "HOSTED_BY",
 68183|       "confidence": "name_only_candidate",
 68184|       "confidence_tier": "likely",
 68185|       "target_resolution": "none",
 68186|       "evidence": [
 68187|         "member name 'RailingCanBeHostedByElement' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 68188|       ],
 68189|       "source_url": "https://www.revitapidocs.com/2025/7f73d2b0-b1ad-9070-4330-6c9d7f28a835.htm",
 68190|       "dll_signature_verified": true,
 68191|       "dll_relationship_scope": "declared",
 68192|       "dll_semantic_verified": null,
 68193|       "dll_verified_status": "signature_verified_declared",
 68194|       "revitlookup_referenced": null,
 68195|       "revitlookup_requires_document_context": null
 68196|     },
 68197|     {
 68198|       "source": "Autodesk.Revit.DB.Architecture.Railing",
 68199|       "target": null,
 68200|       "member_name": "RemoveHost",
 68201|       "member_kind": "method",
 68202|       "edge_type": "HOSTED_BY",
 68203|       "confidence": "name_only_candidate",
 68204|       "confidence_tier": "likely",
 68205|       "target_resolution": "none",
 68206|       "evidence": [
 68207|         "member name 'RemoveHost' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 68208|       ],
 68209|       "source_url": "https://www.revitapidocs.com/2025/28367859-53d5-fbbe-2d55-16970e62e9b4.htm",
 68210|       "dll_signature_verified": true,
 68211|       "dll_relationship_scope": "declared",
 68212|       "dll_semantic_verified": null,
 68213|       "dll_verified_status": "signature_verified_declared",
 68214|       "revitlookup_referenced": null,
 68215|       "revitlookup_requires_document_context": null
 68216|     },
 68217|     {
 68218|       "source": "Autodesk.Revit.DB.Architecture.Railing",
 68219|       "target": "Autodesk.Revit.DB.Level",
 68220|       "member_name": "SetMultistoryStairsPlacementLevels",
 68221|       "member_kind": "method",
 68222|       "edge_type": "ASSIGNED_TO_LEVEL",
 68223|       "confidence": "name_only_candidate",
 68224|       "confidence_tier": "likely",
 68225|       "target_resolution": "exact",
 68226|       "evidence": [
 68227|         "member name 'SetMultistoryStairsPlacementLevels' matches keyword pattern /Level/ but return type 'void' gives no type-level confirmation"
 68228|       ],
 68229|       "source_url": "https://www.revitapidocs.com/2025/47227a43-299a-e762-1e2a-acafb14093a3.htm",
 68230|       "dll_signature_verified": true,
 68231|       "dll_relationship_scope": "declared",
 68232|       "dll_semantic_verified": null,
 68233|       "dll_verified_status": "signature_verified_declared",
 68234|       "revitlookup_referenced": null,
 68235|       "revitlookup_requires_document_context": null
 68236|     },
 68237|     {
 68238|       "source": "Autodesk.Revit.DB.Architecture.RailingType",
 68239|       "target": "Autodesk.Revit.DB.Architecture.BalusterPlacement",
 68240|       "member_name": "BalusterPlacement",
 68241|       "member_kind": "property",
 68242|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 68243|       "confidence": "direct_return_type",
 68244|       "confidence_tier": "unverified_reference",
 68245|       "target_resolution": "short_name_fallback",
 68246|       "evidence": [
 68247|         "return type 'BalusterPlacement' directly names a Revit DB object type"
 68248|       ],
 68249|       "source_url": "https://www.revitapidocs.com/2025/31f4da5f-efd9-8367-0256-9048d04954be.htm",
 68250|       "dll_signature_verified": true,
 68251|       "dll_relationship_scope": "declared",
 68252|       "dll_semantic_verified": null,
 68253|       "dll_verified_status": "signature_verified_declared",
 68254|       "revitlookup_referenced": null,
 68255|       "revitlookup_requires_document_context": null
 68256|     },
 68257|     {
 68258|       "source": "Autodesk.Revit.DB.Architecture.RailingType",
 68259|       "target": null,
 68260|       "member_name": "PrimaryHandrailType",
```
