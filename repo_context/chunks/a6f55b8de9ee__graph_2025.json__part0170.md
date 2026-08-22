# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 170 of 216
- Original line range: 65911-66310
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 65911|       "dll_verified_status": "signature_verified_declared",
 65912|       "revitlookup_referenced": null,
 65913|       "revitlookup_requires_document_context": null
 65914|     },
 65915|     {
 65916|       "source": "Autodesk.Revit.DB.Analysis.EnergyAnalysisDetailModel",
 65917|       "target": "Autodesk.Revit.DB.Analysis.EnergyAnalysisSurface",
 65918|       "member_name": "GetAnalyticalShadingSurfaces",
 65919|       "member_kind": "method",
 65920|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 65921|       "confidence": "needs_runtime_validation",
 65922|       "confidence_tier": "needs_validation",
 65923|       "target_resolution": "short_name_fallback",
 65924|       "evidence": [
 65925|         "return type 'IList < EnergyAnalysisSurface >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 65926|       ],
 65927|       "source_url": "https://www.revitapidocs.com/2025/077e6fbd-8634-346c-6f62-50b6dde554de.htm",
 65928|       "dll_signature_verified": true,
 65929|       "dll_relationship_scope": "declared",
 65930|       "dll_semantic_verified": null,
 65931|       "dll_verified_status": "signature_verified_declared",
 65932|       "revitlookup_referenced": null,
 65933|       "revitlookup_requires_document_context": null
 65934|     },
 65935|     {
 65936|       "source": "Autodesk.Revit.DB.Analysis.EnergyAnalysisDetailModel",
 65937|       "target": "Autodesk.Revit.DB.Analysis.EnergyAnalysisSpace",
 65938|       "member_name": "GetAnalyticalSpaces",
 65939|       "member_kind": "method",
 65940|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 65941|       "confidence": "needs_runtime_validation",
 65942|       "confidence_tier": "needs_validation",
 65943|       "target_resolution": "short_name_fallback",
 65944|       "evidence": [
 65945|         "return type 'IList < EnergyAnalysisSpace >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 65946|       ],
 65947|       "source_url": "https://www.revitapidocs.com/2025/eeb905e6-83f4-2097-8fc8-7b1d6b976be3.htm",
 65948|       "dll_signature_verified": true,
 65949|       "dll_relationship_scope": "declared",
 65950|       "dll_semantic_verified": null,
 65951|       "dll_verified_status": "signature_verified_declared",
 65952|       "revitlookup_referenced": null,
 65953|       "revitlookup_requires_document_context": null
 65954|     },
 65955|     {
 65956|       "source": "Autodesk.Revit.DB.Analysis.EnergyAnalysisDetailModel",
 65957|       "target": "Autodesk.Revit.DB.Analysis.EnergyAnalysisSurface",
 65958|       "member_name": "GetAnalyticalSurfaces",
 65959|       "member_kind": "method",
 65960|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 65961|       "confidence": "needs_runtime_validation",
 65962|       "confidence_tier": "needs_validation",
 65963|       "target_resolution": "short_name_fallback",
 65964|       "evidence": [
 65965|         "return type 'IList < EnergyAnalysisSurface >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 65966|       ],
 65967|       "source_url": "https://www.revitapidocs.com/2025/5faf1c49-1cb3-6692-2b19-73fa44903766.htm",
 65968|       "dll_signature_verified": true,
 65969|       "dll_relationship_scope": "declared",
 65970|       "dll_semantic_verified": null,
 65971|       "dll_verified_status": "signature_verified_declared",
 65972|       "revitlookup_referenced": null,
 65973|       "revitlookup_requires_document_context": null
 65974|     },
 65975|     {
 65976|       "source": "Autodesk.Revit.DB.Analysis.EnergyAnalysisMaterial",
 65977|       "target": "Autodesk.Revit.DB.Material",
 65978|       "member_name": "MaterialName",
 65979|       "member_kind": "property",
 65980|       "edge_type": "USES_MATERIAL",
 65981|       "confidence": "name_only_candidate",
 65982|       "confidence_tier": "likely",
 65983|       "target_resolution": "exact",
 65984|       "evidence": [
 65985|         "member name 'MaterialName' matches keyword pattern /Material/ but return type 'string' gives no type-level confirmation"
 65986|       ],
 65987|       "source_url": "https://www.revitapidocs.com/2025/7c6ffe70-0e1f-709a-7f3b-f6deaf3ffa7c.htm",
 65988|       "dll_signature_verified": true,
 65989|       "dll_relationship_scope": "declared",
 65990|       "dll_semantic_verified": null,
 65991|       "dll_verified_status": "signature_verified_declared",
 65992|       "revitlookup_referenced": null,
 65993|       "revitlookup_requires_document_context": null
 65994|     },
 65995|     {
 65996|       "source": "Autodesk.Revit.DB.Analysis.EnergyAnalysisOpening",
 65997|       "target": null,
 65998|       "member_name": "OriginatingElementId",
 65999|       "member_kind": "property",
 66000|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 66001|       "confidence": "unknown_reference",
 66002|       "confidence_tier": "unverified_reference",
 66003|       "target_resolution": "none",
 66004|       "evidence": [
 66005|         "return type is 'LinkElementId', an ID wrapper, but member name gives no strong hint of the target type"
 66006|       ],
 66007|       "source_url": "https://www.revitapidocs.com/2025/c8b95a80-0f70-4ad1-08f5-b3eb59f6bf95.htm",
 66008|       "dll_signature_verified": true,
 66009|       "dll_relationship_scope": "declared",
 66010|       "dll_semantic_verified": null,
 66011|       "dll_verified_status": "signature_verified_declared",
 66012|       "revitlookup_referenced": null,
 66013|       "revitlookup_requires_document_context": null
 66014|     },
 66015|     {
 66016|       "source": "Autodesk.Revit.DB.Analysis.EnergyAnalysisOpening",
 66017|       "target": null,
 66018|       "member_name": "Type",
 66019|       "member_kind": "property",
 66020|       "edge_type": "TYPE_OF",
 66021|       "confidence": "name_only_candidate",
 66022|       "confidence_tier": "likely",
 66023|       "target_resolution": "none",
 66024|       "evidence": [
 66025|         "member name 'Type' matches keyword pattern /^(Type|TypeId|GetTypeId)$/ but return type 'gbXMLOpeningType' gives no type-level confirmation"
 66026|       ],
 66027|       "source_url": "https://www.revitapidocs.com/2025/b23c3e9f-5abb-4b24-1f5c-40eec68a05f0.htm",
 66028|       "dll_signature_verified": true,
 66029|       "dll_relationship_scope": "declared",
 66030|       "dll_semantic_verified": null,
 66031|       "dll_verified_status": "signature_verified_declared",
 66032|       "revitlookup_referenced": null,
 66033|       "revitlookup_requires_document_context": null
 66034|     },
 66035|     {
 66036|       "source": "Autodesk.Revit.DB.Analysis.EnergyAnalysisOpening",
 66037|       "target": "Autodesk.Revit.DB.Analysis.EnergyAnalysisSurface",
 66038|       "member_name": "GetAnalyticalSurface",
 66039|       "member_kind": "method",
 66040|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 66041|       "confidence": "direct_return_type",
 66042|       "confidence_tier": "unverified_reference",
 66043|       "target_resolution": "short_name_fallback",
 66044|       "evidence": [
 66045|         "return type 'EnergyAnalysisSurface' directly names a Revit DB object type"
 66046|       ],
 66047|       "source_url": "https://www.revitapidocs.com/2025/e086125f-1a53-8c75-267f-b0e62d83fa4e.htm",
 66048|       "dll_signature_verified": true,
 66049|       "dll_relationship_scope": "declared",
 66050|       "dll_semantic_verified": null,
 66051|       "dll_verified_status": "signature_verified_declared",
 66052|       "revitlookup_referenced": null,
 66053|       "revitlookup_requires_document_context": null
 66054|     },
 66055|     {
 66056|       "source": "Autodesk.Revit.DB.Analysis.EnergyAnalysisOpening",
 66057|       "target": "Autodesk.Revit.DB.Analysis.EnergyAnalysisConstruction",
 66058|       "member_name": "GetConstruction",
 66059|       "member_kind": "method",
 66060|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 66061|       "confidence": "direct_return_type",
 66062|       "confidence_tier": "unverified_reference",
 66063|       "target_resolution": "short_name_fallback",
 66064|       "evidence": [
 66065|         "return type 'EnergyAnalysisConstruction' directly names a Revit DB object type"
 66066|       ],
 66067|       "source_url": "https://www.revitapidocs.com/2025/617b3b8e-ae52-4582-7c49-957fc70a9d14.htm",
 66068|       "dll_signature_verified": true,
 66069|       "dll_relationship_scope": "declared",
 66070|       "dll_semantic_verified": null,
 66071|       "dll_verified_status": "signature_verified_declared",
 66072|       "revitlookup_referenced": null,
 66073|       "revitlookup_requires_document_context": null
 66074|     },
 66075|     {
 66076|       "source": "Autodesk.Revit.DB.Analysis.EnergyAnalysisOpening",
 66077|       "target": "Autodesk.Revit.DB.Analysis.EnergyAnalysisWindowType",
 66078|       "member_name": "GetWindowType",
 66079|       "member_kind": "method",
 66080|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 66081|       "confidence": "direct_return_type",
 66082|       "confidence_tier": "unverified_reference",
 66083|       "target_resolution": "short_name_fallback",
 66084|       "evidence": [
 66085|         "return type 'EnergyAnalysisWindowType' directly names a Revit DB object type"
 66086|       ],
 66087|       "source_url": "https://www.revitapidocs.com/2025/0dcc038c-7d3f-a038-a53c-e8910683b16c.htm",
 66088|       "dll_signature_verified": true,
 66089|       "dll_relationship_scope": "declared",
 66090|       "dll_semantic_verified": null,
 66091|       "dll_verified_status": "signature_verified_declared",
 66092|       "revitlookup_referenced": null,
 66093|       "revitlookup_requires_document_context": null
 66094|     },
 66095|     {
 66096|       "source": "Autodesk.Revit.DB.Analysis.EnergyAnalysisSpace",
 66097|       "target": "Autodesk.Revit.DB.Analysis.EnergyAnalysisSurface",
 66098|       "member_name": "GetAnalyticalSurfaces",
 66099|       "member_kind": "method",
 66100|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 66101|       "confidence": "needs_runtime_validation",
 66102|       "confidence_tier": "needs_validation",
 66103|       "target_resolution": "short_name_fallback",
 66104|       "evidence": [
 66105|         "return type 'IList < EnergyAnalysisSurface >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 66106|       ],
 66107|       "source_url": "https://www.revitapidocs.com/2025/ed3a5d9b-8fbc-b17f-df57-6fc1033cb491.htm",
 66108|       "dll_signature_verified": true,
 66109|       "dll_relationship_scope": "declared",
 66110|       "dll_semantic_verified": null,
 66111|       "dll_verified_status": "signature_verified_declared",
 66112|       "revitlookup_referenced": null,
 66113|       "revitlookup_requires_document_context": null
 66114|     },
 66115|     {
 66116|       "source": "Autodesk.Revit.DB.Analysis.EnergyAnalysisSurface",
 66117|       "target": null,
 66118|       "member_name": "OriginatingElementId",
 66119|       "member_kind": "property",
 66120|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 66121|       "confidence": "unknown_reference",
 66122|       "confidence_tier": "unverified_reference",
 66123|       "target_resolution": "none",
 66124|       "evidence": [
 66125|         "return type is 'LinkElementId', an ID wrapper, but member name gives no strong hint of the target type"
 66126|       ],
 66127|       "source_url": "https://www.revitapidocs.com/2025/f1571d1a-7b10-af0e-cc0b-7872ed16eca2.htm",
 66128|       "dll_signature_verified": true,
 66129|       "dll_relationship_scope": "declared",
 66130|       "dll_semantic_verified": null,
 66131|       "dll_verified_status": "signature_verified_declared",
 66132|       "revitlookup_referenced": null,
 66133|       "revitlookup_requires_document_context": null
 66134|     },
 66135|     {
 66136|       "source": "Autodesk.Revit.DB.Analysis.EnergyAnalysisSurface",
 66137|       "target": null,
 66138|       "member_name": "Type",
 66139|       "member_kind": "property",
 66140|       "edge_type": "TYPE_OF",
 66141|       "confidence": "name_only_candidate",
 66142|       "confidence_tier": "likely",
 66143|       "target_resolution": "none",
 66144|       "evidence": [
 66145|         "member name 'Type' matches keyword pattern /^(Type|TypeId|GetTypeId)$/ but return type 'gbXMLSurfaceType' gives no type-level confirmation"
 66146|       ],
 66147|       "source_url": "https://www.revitapidocs.com/2025/ba638150-4be5-979d-15f4-e2885eb77c4b.htm",
 66148|       "dll_signature_verified": true,
 66149|       "dll_relationship_scope": "declared",
 66150|       "dll_semantic_verified": null,
 66151|       "dll_verified_status": "signature_verified_declared",
 66152|       "revitlookup_referenced": null,
 66153|       "revitlookup_requires_document_context": null
 66154|     },
 66155|     {
 66156|       "source": "Autodesk.Revit.DB.Analysis.EnergyAnalysisSurface",
 66157|       "target": "Autodesk.Revit.DB.Analysis.EnergyAnalysisSpace",
 66158|       "member_name": "GetAdjacentAnalyticalSpace",
 66159|       "member_kind": "method",
 66160|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 66161|       "confidence": "direct_return_type",
 66162|       "confidence_tier": "unverified_reference",
 66163|       "target_resolution": "short_name_fallback",
 66164|       "evidence": [
 66165|         "return type 'EnergyAnalysisSpace' directly names a Revit DB object type"
 66166|       ],
 66167|       "source_url": "https://www.revitapidocs.com/2025/d9b2876d-35ac-bcc6-5651-55db257b748a.htm",
 66168|       "dll_signature_verified": true,
 66169|       "dll_relationship_scope": "declared",
 66170|       "dll_semantic_verified": null,
 66171|       "dll_verified_status": "signature_verified_declared",
 66172|       "revitlookup_referenced": null,
 66173|       "revitlookup_requires_document_context": null
 66174|     },
 66175|     {
 66176|       "source": "Autodesk.Revit.DB.Analysis.EnergyAnalysisSurface",
 66177|       "target": "Autodesk.Revit.DB.Analysis.EnergyAnalysisOpening",
 66178|       "member_name": "GetAnalyticalOpenings",
 66179|       "member_kind": "method",
 66180|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 66181|       "confidence": "needs_runtime_validation",
 66182|       "confidence_tier": "needs_validation",
 66183|       "target_resolution": "short_name_fallback",
 66184|       "evidence": [
 66185|         "return type 'IList < EnergyAnalysisOpening >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 66186|       ],
 66187|       "source_url": "https://www.revitapidocs.com/2025/9d1a1633-17a0-8fd2-597f-9dec33350d88.htm",
 66188|       "dll_signature_verified": true,
 66189|       "dll_relationship_scope": "declared",
 66190|       "dll_semantic_verified": null,
 66191|       "dll_verified_status": "signature_verified_declared",
 66192|       "revitlookup_referenced": null,
 66193|       "revitlookup_requires_document_context": null
 66194|     },
 66195|     {
 66196|       "source": "Autodesk.Revit.DB.Analysis.EnergyAnalysisSurface",
 66197|       "target": "Autodesk.Revit.DB.Analysis.EnergyAnalysisSpace",
 66198|       "member_name": "GetAnalyticalSpace",
 66199|       "member_kind": "method",
 66200|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 66201|       "confidence": "direct_return_type",
 66202|       "confidence_tier": "unverified_reference",
 66203|       "target_resolution": "short_name_fallback",
 66204|       "evidence": [
 66205|         "return type 'EnergyAnalysisSpace' directly names a Revit DB object type"
 66206|       ],
 66207|       "source_url": "https://www.revitapidocs.com/2025/c9373502-fff8-00f8-0d29-10f2cce6d2ea.htm",
 66208|       "dll_signature_verified": true,
 66209|       "dll_relationship_scope": "declared",
 66210|       "dll_semantic_verified": null,
 66211|       "dll_verified_status": "signature_verified_declared",
 66212|       "revitlookup_referenced": null,
 66213|       "revitlookup_requires_document_context": null
 66214|     },
 66215|     {
 66216|       "source": "Autodesk.Revit.DB.Analysis.EnergyAnalysisSurface",
 66217|       "target": "Autodesk.Revit.DB.Analysis.EnergyAnalysisConstruction",
 66218|       "member_name": "GetConstruction",
 66219|       "member_kind": "method",
 66220|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 66221|       "confidence": "direct_return_type",
 66222|       "confidence_tier": "unverified_reference",
 66223|       "target_resolution": "short_name_fallback",
 66224|       "evidence": [
 66225|         "return type 'EnergyAnalysisConstruction' directly names a Revit DB object type"
 66226|       ],
 66227|       "source_url": "https://www.revitapidocs.com/2025/fa96b863-c35d-d0ea-42b1-032d619a3d5a.htm",
 66228|       "dll_signature_verified": true,
 66229|       "dll_relationship_scope": "declared",
 66230|       "dll_semantic_verified": null,
 66231|       "dll_verified_status": "signature_verified_declared",
 66232|       "revitlookup_referenced": null,
 66233|       "revitlookup_requires_document_context": null
 66234|     },
 66235|     {
 66236|       "source": "Autodesk.Revit.DB.Analysis.EnergyDataSettings",
 66237|       "target": null,
 66238|       "member_name": "BuildingTypeId",
 66239|       "member_kind": "property",
 66240|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 66241|       "confidence": "unknown_reference",
 66242|       "confidence_tier": "unverified_reference",
 66243|       "target_resolution": "none",
 66244|       "evidence": [
 66245|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 66246|       ],
 66247|       "source_url": "https://www.revitapidocs.com/2025/b20ad078-bbc6-3c0a-84eb-f4414af6e5d7.htm",
 66248|       "dll_signature_verified": true,
 66249|       "dll_relationship_scope": "declared",
 66250|       "dll_semantic_verified": null,
 66251|       "dll_verified_status": "signature_verified_declared",
 66252|       "revitlookup_referenced": null,
 66253|       "revitlookup_requires_document_context": null
 66254|     },
 66255|     {
 66256|       "source": "Autodesk.Revit.DB.Analysis.EnergyDataSettings",
 66257|       "target": "Autodesk.Revit.DB.Category",
 66258|       "member_name": "ExportCategory",
 66259|       "member_kind": "property",
 66260|       "edge_type": "HAS_CATEGORY",
 66261|       "confidence": "elementid_with_strong_name",
 66262|       "confidence_tier": "core",
 66263|       "target_resolution": "exact",
 66264|       "evidence": [
 66265|         "member name 'ExportCategory' matches keyword pattern /Category/"
 66266|       ],
 66267|       "source_url": "https://www.revitapidocs.com/2025/be269eca-22b1-9a1a-7e13-db7f8cdae1e0.htm",
 66268|       "dll_signature_verified": true,
 66269|       "dll_relationship_scope": "declared",
 66270|       "dll_semantic_verified": null,
 66271|       "dll_verified_status": "signature_verified_declared",
 66272|       "revitlookup_referenced": null,
 66273|       "revitlookup_requires_document_context": null
 66274|     },
 66275|     {
 66276|       "source": "Autodesk.Revit.DB.Analysis.EnergyDataSettings",
 66277|       "target": null,
 66278|       "member_name": "GroundPlane",
 66279|       "member_kind": "property",
 66280|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 66281|       "confidence": "unknown_reference",
 66282|       "confidence_tier": "unverified_reference",
 66283|       "target_resolution": "none",
 66284|       "evidence": [
 66285|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 66286|       ],
 66287|       "source_url": "https://www.revitapidocs.com/2025/3a6a5942-1fd7-cd1c-306b-c582da4f2abf.htm",
 66288|       "dll_signature_verified": true,
 66289|       "dll_relationship_scope": "declared",
 66290|       "dll_semantic_verified": null,
 66291|       "dll_verified_status": "signature_verified_declared",
 66292|       "revitlookup_referenced": null,
 66293|       "revitlookup_requires_document_context": null
 66294|     },
 66295|     {
 66296|       "source": "Autodesk.Revit.DB.Analysis.EnergyDataSettings",
 66297|       "target": null,
 66298|       "member_name": "PercentageGlazing",
 66299|       "member_kind": "property",
 66300|       "edge_type": "TAGS_ELEMENT",
 66301|       "confidence": "name_only_candidate",
 66302|       "confidence_tier": "likely",
 66303|       "target_resolution": "none",
 66304|       "evidence": [
 66305|         "member name 'PercentageGlazing' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'double' gives no type-level confirmation"
 66306|       ],
 66307|       "source_url": "https://www.revitapidocs.com/2025/3b754027-2f58-c902-e232-073a663ab85b.htm",
 66308|       "dll_signature_verified": true,
 66309|       "dll_relationship_scope": "declared",
 66310|       "dll_semantic_verified": null,
```
