# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 98 of 216
- Original line range: 37831-38230
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 37831|     },
 37832|     {
 37833|       "source": "Autodesk.Revit.DB.DividedSurface",
 37834|       "target": null,
 37835|       "member_name": "GetAllIntersectionElements",
 37836|       "member_kind": "method",
 37837|       "edge_type": "RETURNS_ELEMENT_IDS",
 37838|       "confidence": "elementid_collection_with_strong_name",
 37839|       "confidence_tier": "core",
 37840|       "target_resolution": "none",
 37841|       "evidence": [
 37842|         "member name 'GetAllIntersectionElements' matches keyword pattern /^GetAll/"
 37843|       ],
 37844|       "source_url": "https://www.revitapidocs.com/2025/d2debd59-e78a-e355-a89f-eee8de6f874b.htm",
 37845|       "dll_signature_verified": true,
 37846|       "dll_relationship_scope": "declared",
 37847|       "dll_semantic_verified": null,
 37848|       "dll_verified_status": "signature_verified_declared",
 37849|       "revitlookup_referenced": null,
 37850|       "revitlookup_requires_document_context": null
 37851|     },
 37852|     {
 37853|       "source": "Autodesk.Revit.DB.DividedSurface",
 37854|       "target": "Autodesk.Revit.DB.Reference",
 37855|       "member_name": "GetGridNodeReference",
 37856|       "member_kind": "method",
 37857|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37858|       "confidence": "direct_return_type",
 37859|       "confidence_tier": "unverified_reference",
 37860|       "target_resolution": "exact",
 37861|       "evidence": [
 37862|         "return type 'Reference' directly names a Revit DB object type"
 37863|       ],
 37864|       "source_url": "https://www.revitapidocs.com/2025/62738bc9-7201-894a-20bc-954e106b2061.htm",
 37865|       "dll_signature_verified": true,
 37866|       "dll_relationship_scope": "declared",
 37867|       "dll_semantic_verified": null,
 37868|       "dll_verified_status": "signature_verified_declared",
 37869|       "revitlookup_referenced": null,
 37870|       "revitlookup_requires_document_context": null
 37871|     },
 37872|     {
 37873|       "source": "Autodesk.Revit.DB.DividedSurface",
 37874|       "target": "Autodesk.Revit.DB.Reference",
 37875|       "member_name": "GetGridSegmentReference",
 37876|       "member_kind": "method",
 37877|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37878|       "confidence": "direct_return_type",
 37879|       "confidence_tier": "unverified_reference",
 37880|       "target_resolution": "exact",
 37881|       "evidence": [
 37882|         "return type 'Reference' directly names a Revit DB object type"
 37883|       ],
 37884|       "source_url": "https://www.revitapidocs.com/2025/ac22065a-c06b-a610-8274-b7e9f6e6dacd.htm",
 37885|       "dll_signature_verified": true,
 37886|       "dll_relationship_scope": "declared",
 37887|       "dll_semantic_verified": null,
 37888|       "dll_verified_status": "signature_verified_declared",
 37889|       "revitlookup_referenced": null,
 37890|       "revitlookup_requires_document_context": null
 37891|     },
 37892|     {
 37893|       "source": "Autodesk.Revit.DB.DividedSurface",
 37894|       "target": "Autodesk.Revit.DB.Reference",
 37895|       "member_name": "GetReferencesWithDividedSurfaces",
 37896|       "member_kind": "method",
 37897|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37898|       "confidence": "needs_runtime_validation",
 37899|       "confidence_tier": "needs_validation",
 37900|       "target_resolution": "exact",
 37901|       "evidence": [
 37902|         "return type 'IList < Reference >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 37903|       ],
 37904|       "source_url": "https://www.revitapidocs.com/2025/4714c725-4863-5e56-e23f-49e207942e02.htm",
 37905|       "dll_signature_verified": true,
 37906|       "dll_relationship_scope": "declared",
 37907|       "dll_semantic_verified": null,
 37908|       "dll_verified_status": "signature_verified_declared",
 37909|       "revitlookup_referenced": null,
 37910|       "revitlookup_requires_document_context": null
 37911|     },
 37912|     {
 37913|       "source": "Autodesk.Revit.DB.DividedSurface",
 37914|       "target": "Autodesk.Revit.DB.FamilyInstance",
 37915|       "member_name": "GetTileFamilyInstance",
 37916|       "member_kind": "method",
 37917|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37918|       "confidence": "direct_return_type",
 37919|       "confidence_tier": "unverified_reference",
 37920|       "target_resolution": "exact",
 37921|       "evidence": [
 37922|         "return type 'FamilyInstance' directly names a Revit DB object type"
 37923|       ],
 37924|       "source_url": "https://www.revitapidocs.com/2025/173b76c6-254c-a9a3-83de-b7384e1ecae7.htm",
 37925|       "dll_signature_verified": true,
 37926|       "dll_relationship_scope": "declared",
 37927|       "dll_semantic_verified": null,
 37928|       "dll_verified_status": "signature_verified_declared",
 37929|       "revitlookup_referenced": null,
 37930|       "revitlookup_requires_document_context": null
 37931|     },
 37932|     {
 37933|       "source": "Autodesk.Revit.DB.DividedSurface",
 37934|       "target": "Autodesk.Revit.DB.Reference",
 37935|       "member_name": "GetTileReference",
 37936|       "member_kind": "method",
 37937|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37938|       "confidence": "direct_return_type",
 37939|       "confidence_tier": "unverified_reference",
 37940|       "target_resolution": "exact",
 37941|       "evidence": [
 37942|         "return type 'Reference' directly names a Revit DB object type"
 37943|       ],
 37944|       "source_url": "https://www.revitapidocs.com/2025/2ef73679-a240-0240-90c5-d08d2593de73.htm",
 37945|       "dll_signature_verified": true,
 37946|       "dll_relationship_scope": "declared",
 37947|       "dll_semantic_verified": null,
 37948|       "dll_verified_status": "signature_verified_declared",
 37949|       "revitlookup_referenced": null,
 37950|       "revitlookup_requires_document_context": null
 37951|     },
 37952|     {
 37953|       "source": "Autodesk.Revit.DB.Document",
 37954|       "target": "Autodesk.Revit.DB.ProjectLocation",
 37955|       "member_name": "ActiveProjectLocation",
 37956|       "member_kind": "property",
 37957|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37958|       "confidence": "direct_return_type",
 37959|       "confidence_tier": "unverified_reference",
 37960|       "target_resolution": "exact",
 37961|       "evidence": [
 37962|         "return type 'ProjectLocation' directly names a Revit DB object type"
 37963|       ],
 37964|       "source_url": "https://www.revitapidocs.com/2025/cd6733bb-4510-bb58-5ca5-21ededb30cdf.htm",
 37965|       "dll_signature_verified": true,
 37966|       "dll_relationship_scope": "declared",
 37967|       "dll_semantic_verified": null,
 37968|       "dll_verified_status": "signature_verified_declared",
 37969|       "revitlookup_referenced": null,
 37970|       "revitlookup_requires_document_context": null
 37971|     },
 37972|     {
 37973|       "source": "Autodesk.Revit.DB.Document",
 37974|       "target": "Autodesk.Revit.DB.View",
 37975|       "member_name": "ActiveView",
 37976|       "member_kind": "property",
 37977|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37978|       "confidence": "direct_return_type",
 37979|       "confidence_tier": "unverified_reference",
 37980|       "target_resolution": "exact",
 37981|       "evidence": [
 37982|         "return type 'View' directly names a Revit DB object type"
 37983|       ],
 37984|       "source_url": "https://www.revitapidocs.com/2025/043960ac-dde4-0f45-249f-8161646a4362.htm",
 37985|       "dll_signature_verified": true,
 37986|       "dll_relationship_scope": "declared",
 37987|       "dll_semantic_verified": null,
 37988|       "dll_verified_status": "signature_verified_declared",
 37989|       "revitlookup_referenced": null,
 37990|       "revitlookup_requires_document_context": null
 37991|     },
 37992|     {
 37993|       "source": "Autodesk.Revit.DB.Document",
 37994|       "target": "Autodesk.Revit.DB.Document",
 37995|       "member_name": "Create",
 37996|       "member_kind": "property",
 37997|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37998|       "confidence": "direct_return_type",
 37999|       "confidence_tier": "unverified_reference",
 38000|       "target_resolution": "exact",
 38001|       "evidence": [
 38002|         "return type 'Document' directly names a Revit DB object type"
 38003|       ],
 38004|       "source_url": "https://www.revitapidocs.com/2025/f87f97d5-8402-62b5-4d6d-defe60c8f8ee.htm",
 38005|       "dll_signature_verified": true,
 38006|       "dll_relationship_scope": "declared",
 38007|       "dll_semantic_verified": null,
 38008|       "dll_verified_status": "signature_verified_declared",
 38009|       "revitlookup_referenced": null,
 38010|       "revitlookup_requires_document_context": null
 38011|     },
 38012|     {
 38013|       "source": "Autodesk.Revit.DB.Document",
 38014|       "target": "Autodesk.Revit.DB.FamilyManager",
 38015|       "member_name": "FamilyManager",
 38016|       "member_kind": "property",
 38017|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 38018|       "confidence": "direct_return_type",
 38019|       "confidence_tier": "unverified_reference",
 38020|       "target_resolution": "exact",
 38021|       "evidence": [
 38022|         "return type 'FamilyManager' directly names a Revit DB object type"
 38023|       ],
 38024|       "source_url": "https://www.revitapidocs.com/2025/478fde66-c9f0-86b5-204a-c95f18b69ca1.htm",
 38025|       "dll_signature_verified": true,
 38026|       "dll_relationship_scope": "declared",
 38027|       "dll_semantic_verified": null,
 38028|       "dll_verified_status": "signature_verified_declared",
 38029|       "revitlookup_referenced": null,
 38030|       "revitlookup_requires_document_context": null
 38031|     },
 38032|     {
 38033|       "source": "Autodesk.Revit.DB.Document",
 38034|       "target": "Autodesk.Revit.DB.MullionTypeSet",
 38035|       "member_name": "MullionTypes",
 38036|       "member_kind": "property",
 38037|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 38038|       "confidence": "direct_return_type",
 38039|       "confidence_tier": "unverified_reference",
 38040|       "target_resolution": "exact",
 38041|       "evidence": [
 38042|         "return type 'MullionTypeSet' directly names a Revit DB object type"
 38043|       ],
 38044|       "source_url": "https://www.revitapidocs.com/2025/ce7bf7aa-4e38-fad1-15ed-816529815dbf.htm",
 38045|       "dll_signature_verified": true,
 38046|       "dll_relationship_scope": "declared",
 38047|       "dll_semantic_verified": null,
 38048|       "dll_verified_status": "signature_verified_declared",
 38049|       "revitlookup_referenced": null,
 38050|       "revitlookup_requires_document_context": null
 38051|     },
 38052|     {
 38053|       "source": "Autodesk.Revit.DB.Document",
 38054|       "target": "Autodesk.Revit.DB.Family",
 38055|       "member_name": "OwnerFamily",
 38056|       "member_kind": "property",
 38057|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 38058|       "confidence": "direct_return_type",
 38059|       "confidence_tier": "unverified_reference",
 38060|       "target_resolution": "exact",
 38061|       "evidence": [
 38062|         "return type 'Family' directly names a Revit DB object type"
 38063|       ],
 38064|       "source_url": "https://www.revitapidocs.com/2025/95b303dc-1569-492d-6863-fbe49f6189b0.htm",
 38065|       "dll_signature_verified": true,
 38066|       "dll_relationship_scope": "declared",
 38067|       "dll_semantic_verified": null,
 38068|       "dll_verified_status": "signature_verified_declared",
 38069|       "revitlookup_referenced": null,
 38070|       "revitlookup_requires_document_context": null
 38071|     },
 38072|     {
 38073|       "source": "Autodesk.Revit.DB.Document",
 38074|       "target": "Autodesk.Revit.DB.PanelTypeSet",
 38075|       "member_name": "PanelTypes",
 38076|       "member_kind": "property",
 38077|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 38078|       "confidence": "direct_return_type",
 38079|       "confidence_tier": "unverified_reference",
 38080|       "target_resolution": "exact",
 38081|       "evidence": [
 38082|         "return type 'PanelTypeSet' directly names a Revit DB object type"
 38083|       ],
 38084|       "source_url": "https://www.revitapidocs.com/2025/8e6c5a87-528c-c13e-ed48-0e7c5af688d5.htm",
 38085|       "dll_signature_verified": true,
 38086|       "dll_relationship_scope": "declared",
 38087|       "dll_semantic_verified": null,
 38088|       "dll_verified_status": "signature_verified_declared",
 38089|       "revitlookup_referenced": null,
 38090|       "revitlookup_requires_document_context": null
 38091|     },
 38092|     {
 38093|       "source": "Autodesk.Revit.DB.Document",
 38094|       "target": "Autodesk.Revit.DB.BindingMap",
 38095|       "member_name": "ParameterBindings",
 38096|       "member_kind": "property",
 38097|       "edge_type": "HAS_PARAMETER",
 38098|       "confidence": "direct_return_type",
 38099|       "confidence_tier": "core",
 38100|       "target_resolution": "exact",
 38101|       "evidence": [
 38102|         "return type 'BindingMap' directly names a Revit DB object type"
 38103|       ],
 38104|       "source_url": "https://www.revitapidocs.com/2025/ce28ad7d-30b7-29d9-8f81-c75aebc03581.htm",
 38105|       "dll_signature_verified": true,
 38106|       "dll_relationship_scope": "declared",
 38107|       "dll_semantic_verified": null,
 38108|       "dll_verified_status": "signature_verified_declared",
 38109|       "revitlookup_referenced": null,
 38110|       "revitlookup_requires_document_context": null
 38111|     },
 38112|     {
 38113|       "source": "Autodesk.Revit.DB.Document",
 38114|       "target": "Autodesk.Revit.DB.PhaseArray",
 38115|       "member_name": "Phases",
 38116|       "member_kind": "property",
 38117|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 38118|       "confidence": "direct_return_type",
 38119|       "confidence_tier": "unverified_reference",
 38120|       "target_resolution": "exact",
 38121|       "evidence": [
 38122|         "member name 'Phases' matches keyword pattern /Phase/ implying target 'Phase', but the actual return type 'PhaseArray' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 38123|         "return type 'PhaseArray' directly names a Revit DB object type"
 38124|       ],
 38125|       "source_url": "https://www.revitapidocs.com/2025/362b427f-bf0d-6509-e541-9d5cc48e1837.htm",
 38126|       "dll_signature_verified": true,
 38127|       "dll_relationship_scope": "declared",
 38128|       "dll_semantic_verified": null,
 38129|       "dll_verified_status": "signature_verified_declared",
 38130|       "revitlookup_referenced": null,
 38131|       "revitlookup_requires_document_context": null
 38132|     },
 38133|     {
 38134|       "source": "Autodesk.Revit.DB.Document",
 38135|       "target": "Autodesk.Revit.DB.PlanTopologySet",
 38136|       "member_name": "PlanTopologies",
 38137|       "member_kind": "property",
 38138|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 38139|       "confidence": "direct_return_type",
 38140|       "confidence_tier": "unverified_reference",
 38141|       "target_resolution": "exact",
 38142|       "evidence": [
 38143|         "return type 'PlanTopologySet' directly names a Revit DB object type"
 38144|       ],
 38145|       "source_url": "https://www.revitapidocs.com/2025/b782b091-bcd9-6759-9e39-4cd7a5bf3143.htm",
 38146|       "dll_signature_verified": true,
 38147|       "dll_relationship_scope": "declared",
 38148|       "dll_semantic_verified": null,
 38149|       "dll_verified_status": "signature_verified_declared",
 38150|       "revitlookup_referenced": true,
 38151|       "revitlookup_requires_document_context": false
 38152|     },
 38153|     {
 38154|       "source": "Autodesk.Revit.DB.Document",
 38155|       "target": "Autodesk.Revit.DB.PrintManager",
 38156|       "member_name": "PrintManager",
 38157|       "member_kind": "property",
 38158|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 38159|       "confidence": "direct_return_type",
 38160|       "confidence_tier": "unverified_reference",
 38161|       "target_resolution": "exact",
 38162|       "evidence": [
 38163|         "return type 'PrintManager' directly names a Revit DB object type"
 38164|       ],
 38165|       "source_url": "https://www.revitapidocs.com/2025/514e70ec-341c-148a-aeea-eabcd8cf7ca1.htm",
 38166|       "dll_signature_verified": true,
 38167|       "dll_relationship_scope": "declared",
 38168|       "dll_semantic_verified": null,
 38169|       "dll_verified_status": "signature_verified_declared",
 38170|       "revitlookup_referenced": null,
 38171|       "revitlookup_requires_document_context": null
 38172|     },
 38173|     {
 38174|       "source": "Autodesk.Revit.DB.Document",
 38175|       "target": "Autodesk.Revit.DB.ProjectInfo",
 38176|       "member_name": "ProjectInformation",
 38177|       "member_kind": "property",
 38178|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 38179|       "confidence": "direct_return_type",
 38180|       "confidence_tier": "unverified_reference",
 38181|       "target_resolution": "exact",
 38182|       "evidence": [
 38183|         "return type 'ProjectInfo' directly names a Revit DB object type"
 38184|       ],
 38185|       "source_url": "https://www.revitapidocs.com/2025/4db36834-e324-b199-7a5a-e3218e95da37.htm",
 38186|       "dll_signature_verified": true,
 38187|       "dll_relationship_scope": "declared",
 38188|       "dll_semantic_verified": null,
 38189|       "dll_verified_status": "signature_verified_declared",
 38190|       "revitlookup_referenced": null,
 38191|       "revitlookup_requires_document_context": null
 38192|     },
 38193|     {
 38194|       "source": "Autodesk.Revit.DB.Document",
 38195|       "target": "Autodesk.Revit.DB.ProjectLocationSet",
 38196|       "member_name": "ProjectLocations",
 38197|       "member_kind": "property",
 38198|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 38199|       "confidence": "direct_return_type",
 38200|       "confidence_tier": "unverified_reference",
 38201|       "target_resolution": "exact",
 38202|       "evidence": [
 38203|         "return type 'ProjectLocationSet' directly names a Revit DB object type"
 38204|       ],
 38205|       "source_url": "https://www.revitapidocs.com/2025/87be3885-b1aa-ba8c-f82e-a5a0f7455c3a.htm",
 38206|       "dll_signature_verified": true,
 38207|       "dll_relationship_scope": "declared",
 38208|       "dll_semantic_verified": null,
 38209|       "dll_verified_status": "signature_verified_declared",
 38210|       "revitlookup_referenced": null,
 38211|       "revitlookup_requires_document_context": null
 38212|     },
 38213|     {
 38214|       "source": "Autodesk.Revit.DB.Document",
 38215|       "target": "Autodesk.Revit.DB.Settings",
 38216|       "member_name": "Settings",
 38217|       "member_kind": "property",
 38218|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 38219|       "confidence": "direct_return_type",
 38220|       "confidence_tier": "unverified_reference",
 38221|       "target_resolution": "exact",
 38222|       "evidence": [
 38223|         "return type 'Settings' directly names a Revit DB object type"
 38224|       ],
 38225|       "source_url": "https://www.revitapidocs.com/2025/1581b3ec-eaef-9ad9-3d57-cf75a4f09b58.htm",
 38226|       "dll_signature_verified": true,
 38227|       "dll_relationship_scope": "declared",
 38228|       "dll_semantic_verified": null,
 38229|       "dll_verified_status": "signature_verified_declared",
 38230|       "revitlookup_referenced": null,
```
