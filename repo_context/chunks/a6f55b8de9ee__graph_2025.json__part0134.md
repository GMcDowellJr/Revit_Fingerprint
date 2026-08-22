# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 134 of 216
- Original line range: 51871-52270
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 51871|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 51872|       "confidence": "needs_runtime_validation",
 51873|       "confidence_tier": "needs_validation",
 51874|       "target_resolution": "exact",
 51875|       "evidence": [
 51876|         "return type 'IList < Reference >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 51877|       ],
 51878|       "source_url": "https://www.revitapidocs.com/2025/a53c4ebf-0a34-31b5-9140-27dcd15f85f2.htm",
 51879|       "dll_signature_verified": true,
 51880|       "dll_relationship_scope": "declared",
 51881|       "dll_semantic_verified": null,
 51882|       "dll_verified_status": "signature_verified_declared",
 51883|       "revitlookup_referenced": null,
 51884|       "revitlookup_requires_document_context": null
 51885|     },
 51886|     {
 51887|       "source": "Autodesk.Revit.DB.MultiReferenceAnnotationOptions",
 51888|       "target": null,
 51889|       "member_name": "GetElementsToDimension",
 51890|       "member_kind": "method",
 51891|       "edge_type": "RETURNS_ELEMENT_IDS",
 51892|       "confidence": "unknown_reference",
 51893|       "confidence_tier": "unverified_reference",
 51894|       "target_resolution": "none",
 51895|       "evidence": [
 51896|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 51897|       ],
 51898|       "source_url": "https://www.revitapidocs.com/2025/72605bc0-cfe6-72be-1531-e0c86ff450f4.htm",
 51899|       "dll_signature_verified": true,
 51900|       "dll_relationship_scope": "declared",
 51901|       "dll_semantic_verified": null,
 51902|       "dll_verified_status": "signature_verified_declared",
 51903|       "revitlookup_referenced": null,
 51904|       "revitlookup_requires_document_context": null
 51905|     },
 51906|     {
 51907|       "source": "Autodesk.Revit.DB.MultiReferenceAnnotationOptions",
 51908|       "target": "Autodesk.Revit.DB.Category",
 51909|       "member_name": "ReferencesDontMatchReferenceCategory",
 51910|       "member_kind": "method",
 51911|       "edge_type": "HAS_CATEGORY",
 51912|       "confidence": "name_only_candidate",
 51913|       "confidence_tier": "likely",
 51914|       "target_resolution": "exact",
 51915|       "evidence": [
 51916|         "member name 'ReferencesDontMatchReferenceCategory' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 51917|       ],
 51918|       "source_url": "https://www.revitapidocs.com/2025/c79310d9-a47c-7ad8-3fb3-6f5ce88cde34.htm",
 51919|       "dll_signature_verified": true,
 51920|       "dll_relationship_scope": "declared",
 51921|       "dll_semantic_verified": null,
 51922|       "dll_verified_status": "signature_verified_declared",
 51923|       "revitlookup_referenced": null,
 51924|       "revitlookup_requires_document_context": null
 51925|     },
 51926|     {
 51927|       "source": "Autodesk.Revit.DB.MultiReferenceAnnotationType",
 51928|       "target": null,
 51929|       "member_name": "DimensionStyleId",
 51930|       "member_kind": "property",
 51931|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 51932|       "confidence": "unknown_reference",
 51933|       "confidence_tier": "unverified_reference",
 51934|       "target_resolution": "none",
 51935|       "evidence": [
 51936|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 51937|       ],
 51938|       "source_url": "https://www.revitapidocs.com/2025/2849b4f5-70fd-747b-e31c-d286ee518645.htm",
 51939|       "dll_signature_verified": true,
 51940|       "dll_relationship_scope": "declared",
 51941|       "dll_semantic_verified": null,
 51942|       "dll_verified_status": "signature_verified_declared",
 51943|       "revitlookup_referenced": null,
 51944|       "revitlookup_requires_document_context": null
 51945|     },
 51946|     {
 51947|       "source": "Autodesk.Revit.DB.MultiReferenceAnnotationType",
 51948|       "target": null,
 51949|       "member_name": "GroupTagHeads",
 51950|       "member_kind": "property",
 51951|       "edge_type": "TAGS_ELEMENT",
 51952|       "confidence": "name_only_candidate",
 51953|       "confidence_tier": "likely",
 51954|       "target_resolution": "none",
 51955|       "evidence": [
 51956|         "member name 'GroupTagHeads' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'bool' gives no type-level confirmation"
 51957|       ],
 51958|       "source_url": "https://www.revitapidocs.com/2025/c8bbdb4c-9c69-b4b8-7382-c3d9f54f380c.htm",
 51959|       "dll_signature_verified": true,
 51960|       "dll_relationship_scope": "declared",
 51961|       "dll_semantic_verified": null,
 51962|       "dll_verified_status": "signature_verified_declared",
 51963|       "revitlookup_referenced": null,
 51964|       "revitlookup_requires_document_context": null
 51965|     },
 51966|     {
 51967|       "source": "Autodesk.Revit.DB.MultiReferenceAnnotationType",
 51968|       "target": "Autodesk.Revit.DB.Category",
 51969|       "member_name": "ReferenceCategoryId",
 51970|       "member_kind": "property",
 51971|       "edge_type": "HAS_CATEGORY",
 51972|       "confidence": "elementid_with_strong_name",
 51973|       "confidence_tier": "core",
 51974|       "target_resolution": "exact",
 51975|       "evidence": [
 51976|         "member name 'ReferenceCategoryId' matches keyword pattern /Category/"
 51977|       ],
 51978|       "source_url": "https://www.revitapidocs.com/2025/5d89b166-9eb2-924b-7c8b-ec41508825d0.htm",
 51979|       "dll_signature_verified": true,
 51980|       "dll_relationship_scope": "declared",
 51981|       "dll_semantic_verified": null,
 51982|       "dll_verified_status": "signature_verified_declared",
 51983|       "revitlookup_referenced": null,
 51984|       "revitlookup_requires_document_context": null
 51985|     },
 51986|     {
 51987|       "source": "Autodesk.Revit.DB.MultiReferenceAnnotationType",
 51988|       "target": null,
 51989|       "member_name": "TagTypeId",
 51990|       "member_kind": "property",
 51991|       "edge_type": "TAGS_ELEMENT",
 51992|       "confidence": "elementid_with_strong_name",
 51993|       "confidence_tier": "core",
 51994|       "target_resolution": "none",
 51995|       "evidence": [
 51996|         "member name 'TagTypeId' matches keyword pattern /^GetTagged|Tag(ged)?/"
 51997|       ],
 51998|       "source_url": "https://www.revitapidocs.com/2025/27b53b40-5506-9a4a-321a-cd8db9b09ceb.htm",
 51999|       "dll_signature_verified": true,
 52000|       "dll_relationship_scope": "declared",
 52001|       "dll_semantic_verified": null,
 52002|       "dll_verified_status": "signature_verified_declared",
 52003|       "revitlookup_referenced": null,
 52004|       "revitlookup_requires_document_context": null
 52005|     },
 52006|     {
 52007|       "source": "Autodesk.Revit.DB.MultiReferenceAnnotationType",
 52008|       "target": null,
 52009|       "member_name": "GetAllowedTagCategory",
 52010|       "member_kind": "method",
 52011|       "edge_type": "TAGS_ELEMENT",
 52012|       "confidence": "elementid_with_strong_name",
 52013|       "confidence_tier": "core",
 52014|       "target_resolution": "none",
 52015|       "evidence": [
 52016|         "member name 'GetAllowedTagCategory' matches keyword pattern /^GetTagged|Tag(ged)?/"
 52017|       ],
 52018|       "source_url": "https://www.revitapidocs.com/2025/06cf5deb-d580-4ce1-f0fd-a17b5558b3c9.htm",
 52019|       "dll_signature_verified": true,
 52020|       "dll_relationship_scope": "declared",
 52021|       "dll_semantic_verified": null,
 52022|       "dll_verified_status": "signature_verified_declared",
 52023|       "revitlookup_referenced": null,
 52024|       "revitlookup_requires_document_context": null
 52025|     },
 52026|     {
 52027|       "source": "Autodesk.Revit.DB.MultiReferenceAnnotationType",
 52028|       "target": "Autodesk.Revit.DB.Category",
 52029|       "member_name": "IsAllowedReferenceCategory",
 52030|       "member_kind": "method",
 52031|       "edge_type": "HAS_CATEGORY",
 52032|       "confidence": "name_only_candidate",
 52033|       "confidence_tier": "likely",
 52034|       "target_resolution": "exact",
 52035|       "evidence": [
 52036|         "member name 'IsAllowedReferenceCategory' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 52037|       ],
 52038|       "source_url": "https://www.revitapidocs.com/2025/44d8f0d4-67f2-3c2a-0941-350401e9ec9b.htm",
 52039|       "dll_signature_verified": true,
 52040|       "dll_relationship_scope": "declared",
 52041|       "dll_semantic_verified": null,
 52042|       "dll_verified_status": "signature_verified_declared",
 52043|       "revitlookup_referenced": null,
 52044|       "revitlookup_requires_document_context": null
 52045|     },
 52046|     {
 52047|       "source": "Autodesk.Revit.DB.MultiReferenceAnnotationType",
 52048|       "target": null,
 52049|       "member_name": "IsAllowedTagCategory",
 52050|       "member_kind": "method",
 52051|       "edge_type": "TAGS_ELEMENT",
 52052|       "confidence": "name_only_candidate",
 52053|       "confidence_tier": "likely",
 52054|       "target_resolution": "none",
 52055|       "evidence": [
 52056|         "member name 'IsAllowedTagCategory' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'bool' gives no type-level confirmation"
 52057|       ],
 52058|       "source_url": "https://www.revitapidocs.com/2025/2bff739f-d084-8596-0633-28013e778e6c.htm",
 52059|       "dll_signature_verified": true,
 52060|       "dll_relationship_scope": "declared",
 52061|       "dll_semantic_verified": null,
 52062|       "dll_verified_status": "signature_verified_declared",
 52063|       "revitlookup_referenced": null,
 52064|       "revitlookup_requires_document_context": null
 52065|     },
 52066|     {
 52067|       "source": "Autodesk.Revit.DB.MultiReferenceAnnotationType",
 52068|       "target": null,
 52069|       "member_name": "IsAllowedTagType",
 52070|       "member_kind": "method",
 52071|       "edge_type": "TAGS_ELEMENT",
 52072|       "confidence": "name_only_candidate",
 52073|       "confidence_tier": "likely",
 52074|       "target_resolution": "none",
 52075|       "evidence": [
 52076|         "member name 'IsAllowedTagType' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'bool' gives no type-level confirmation"
 52077|       ],
 52078|       "source_url": "https://www.revitapidocs.com/2025/4f7f0cf5-ed38-ec2c-cfc9-7568b29a4601.htm",
 52079|       "dll_signature_verified": true,
 52080|       "dll_relationship_scope": "declared",
 52081|       "dll_semantic_verified": null,
 52082|       "dll_verified_status": "signature_verified_declared",
 52083|       "revitlookup_referenced": null,
 52084|       "revitlookup_requires_document_context": null
 52085|     },
 52086|     {
 52087|       "source": "Autodesk.Revit.DB.MultiSegmentGrid",
 52088|       "target": null,
 52089|       "member_name": "GetGridIds",
 52090|       "member_kind": "method",
 52091|       "edge_type": "RETURNS_ELEMENT_IDS",
 52092|       "confidence": "unknown_reference",
 52093|       "confidence_tier": "unverified_reference",
 52094|       "target_resolution": "none",
 52095|       "evidence": [
 52096|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 52097|       ],
 52098|       "source_url": "https://www.revitapidocs.com/2025/203790e1-56b4-52d3-83f5-14ccd3409466.htm",
 52099|       "dll_signature_verified": true,
 52100|       "dll_relationship_scope": "declared",
 52101|       "dll_semantic_verified": null,
 52102|       "dll_verified_status": "signature_verified_declared",
 52103|       "revitlookup_referenced": null,
 52104|       "revitlookup_requires_document_context": null
 52105|     },
 52106|     {
 52107|       "source": "Autodesk.Revit.DB.MultiSegmentGrid",
 52108|       "target": null,
 52109|       "member_name": "GetMultiSegementGridId",
 52110|       "member_kind": "method",
 52111|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 52112|       "confidence": "unknown_reference",
 52113|       "confidence_tier": "unverified_reference",
 52114|       "target_resolution": "none",
 52115|       "evidence": [
 52116|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 52117|       ],
 52118|       "source_url": "https://www.revitapidocs.com/2025/141c947b-7be3-4eee-9a92-627865023bf8.htm",
 52119|       "dll_signature_verified": true,
 52120|       "dll_relationship_scope": "declared",
 52121|       "dll_semantic_verified": null,
 52122|       "dll_verified_status": "signature_verified_declared",
 52123|       "revitlookup_referenced": null,
 52124|       "revitlookup_requires_document_context": null
 52125|     },
 52126|     {
 52127|       "source": "Autodesk.Revit.DB.NavisworksExportOptions",
 52128|       "target": "Autodesk.Revit.DB.Level",
 52129|       "member_name": "DivideFileIntoLevels",
 52130|       "member_kind": "property",
 52131|       "edge_type": "ASSIGNED_TO_LEVEL",
 52132|       "confidence": "name_only_candidate",
 52133|       "confidence_tier": "likely",
 52134|       "target_resolution": "exact",
 52135|       "evidence": [
 52136|         "member name 'DivideFileIntoLevels' matches keyword pattern /Level/ but return type 'bool' gives no type-level confirmation"
 52137|       ],
 52138|       "source_url": "https://www.revitapidocs.com/2025/55cd45cc-496e-70ad-0bef-636182dcd3e8.htm",
 52139|       "dll_signature_verified": true,
 52140|       "dll_relationship_scope": "declared",
 52141|       "dll_semantic_verified": null,
 52142|       "dll_verified_status": "signature_verified_declared",
 52143|       "revitlookup_referenced": null,
 52144|       "revitlookup_requires_document_context": null
 52145|     },
 52146|     {
 52147|       "source": "Autodesk.Revit.DB.NavisworksExportOptions",
 52148|       "target": "Autodesk.Revit.DB.Architecture.Room",
 52149|       "member_name": "ExportRoomAsAttribute",
 52150|       "member_kind": "property",
 52151|       "edge_type": "REFERENCES",
 52152|       "confidence": "name_only_candidate",
 52153|       "confidence_tier": "likely",
 52154|       "target_resolution": "exact",
 52155|       "evidence": [
 52156|         "member name 'ExportRoomAsAttribute' matches keyword pattern /Room/ but return type 'bool' gives no type-level confirmation"
 52157|       ],
 52158|       "source_url": "https://www.revitapidocs.com/2025/eef60c23-5cd6-8d69-d75e-54b8c8d24674.htm",
 52159|       "dll_signature_verified": true,
 52160|       "dll_relationship_scope": "declared",
 52161|       "dll_semantic_verified": null,
 52162|       "dll_verified_status": "signature_verified_declared",
 52163|       "revitlookup_referenced": null,
 52164|       "revitlookup_requires_document_context": null
 52165|     },
 52166|     {
 52167|       "source": "Autodesk.Revit.DB.NavisworksExportOptions",
 52168|       "target": "Autodesk.Revit.DB.Architecture.Room",
 52169|       "member_name": "ExportRoomGeometry",
 52170|       "member_kind": "property",
 52171|       "edge_type": "REFERENCES",
 52172|       "confidence": "name_only_candidate",
 52173|       "confidence_tier": "likely",
 52174|       "target_resolution": "exact",
 52175|       "evidence": [
 52176|         "member name 'ExportRoomGeometry' matches keyword pattern /Room/ but return type 'bool' gives no type-level confirmation"
 52177|       ],
 52178|       "source_url": "https://www.revitapidocs.com/2025/1f40544f-1f6a-24d6-6256-8f9f61e6114a.htm",
 52179|       "dll_signature_verified": true,
 52180|       "dll_relationship_scope": "declared",
 52181|       "dll_semantic_verified": null,
 52182|       "dll_verified_status": "signature_verified_declared",
 52183|       "revitlookup_referenced": null,
 52184|       "revitlookup_requires_document_context": null
 52185|     },
 52186|     {
 52187|       "source": "Autodesk.Revit.DB.NavisworksExportOptions",
 52188|       "target": "Autodesk.Revit.DB.Material",
 52189|       "member_name": "FindMissingMaterials",
 52190|       "member_kind": "property",
 52191|       "edge_type": "USES_MATERIAL",
 52192|       "confidence": "name_only_candidate",
 52193|       "confidence_tier": "likely",
 52194|       "target_resolution": "exact",
 52195|       "evidence": [
 52196|         "member name 'FindMissingMaterials' matches keyword pattern /Material/ but return type 'bool' gives no type-level confirmation"
 52197|       ],
 52198|       "source_url": "https://www.revitapidocs.com/2025/100be585-d3c7-344c-b407-8a240e08e233.htm",
 52199|       "dll_signature_verified": true,
 52200|       "dll_relationship_scope": "declared",
 52201|       "dll_semantic_verified": null,
 52202|       "dll_verified_status": "signature_verified_declared",
 52203|       "revitlookup_referenced": null,
 52204|       "revitlookup_requires_document_context": null
 52205|     },
 52206|     {
 52207|       "source": "Autodesk.Revit.DB.NavisworksExportOptions",
 52208|       "target": null,
 52209|       "member_name": "Parameters",
 52210|       "member_kind": "property",
 52211|       "edge_type": "HAS_PARAMETER",
 52212|       "confidence": "name_only_candidate",
 52213|       "confidence_tier": "likely",
 52214|       "target_resolution": "none",
 52215|       "evidence": [
 52216|         "member name 'Parameters' matches keyword pattern /Parameter/ but return type 'NavisworksParameters' gives no type-level confirmation"
 52217|       ],
 52218|       "source_url": "https://www.revitapidocs.com/2025/7bc7e2e4-535a-8975-636f-a3af2ba87d55.htm",
 52219|       "dll_signature_verified": true,
 52220|       "dll_relationship_scope": "declared",
 52221|       "dll_semantic_verified": null,
 52222|       "dll_verified_status": "signature_verified_declared",
 52223|       "revitlookup_referenced": null,
 52224|       "revitlookup_requires_document_context": null
 52225|     },
 52226|     {
 52227|       "source": "Autodesk.Revit.DB.NavisworksExportOptions",
 52228|       "target": "Autodesk.Revit.DB.View",
 52229|       "member_name": "ViewId",
 52230|       "member_kind": "property",
 52231|       "edge_type": "REFERENCES",
 52232|       "confidence": "elementid_with_strong_name",
 52233|       "confidence_tier": "core",
 52234|       "target_resolution": "exact",
 52235|       "evidence": [
 52236|         "member name 'ViewId' matches keyword pattern /^(Get)?ViewId$/"
 52237|       ],
 52238|       "source_url": "https://www.revitapidocs.com/2025/afec98fb-dba1-2413-baa4-6889550d8087.htm",
 52239|       "dll_signature_verified": true,
 52240|       "dll_relationship_scope": "declared",
 52241|       "dll_semantic_verified": null,
 52242|       "dll_verified_status": "signature_verified_declared",
 52243|       "revitlookup_referenced": null,
 52244|       "revitlookup_requires_document_context": null
 52245|     },
 52246|     {
 52247|       "source": "Autodesk.Revit.DB.NavisworksExportOptions",
 52248|       "target": null,
 52249|       "member_name": "GetSelectedElementIds",
 52250|       "member_kind": "method",
 52251|       "edge_type": "RETURNS_ELEMENT_IDS",
 52252|       "confidence": "unknown_reference",
 52253|       "confidence_tier": "unverified_reference",
 52254|       "target_resolution": "none",
 52255|       "evidence": [
 52256|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 52257|       ],
 52258|       "source_url": "https://www.revitapidocs.com/2025/071adb98-310b-3b6a-acc2-e98d9c94771f.htm",
 52259|       "dll_signature_verified": true,
 52260|       "dll_relationship_scope": "declared",
 52261|       "dll_semantic_verified": null,
 52262|       "dll_verified_status": "signature_verified_declared",
 52263|       "revitlookup_referenced": null,
 52264|       "revitlookup_requires_document_context": null
 52265|     },
 52266|     {
 52267|       "source": "Autodesk.Revit.DB.NestedFamilyTypeReference",
 52268|       "target": "Autodesk.Revit.DB.Category",
 52269|       "member_name": "CategoryId",
 52270|       "member_kind": "property",
```
