# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 147 of 216
- Original line range: 56941-57340
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 56941|       "source_url": "https://www.revitapidocs.com/2025/cc96a880-9f3b-08cf-7a31-e8301a817035.htm",
 56942|       "dll_signature_verified": true,
 56943|       "dll_relationship_scope": "declared",
 56944|       "dll_semantic_verified": null,
 56945|       "dll_verified_status": "signature_verified_declared",
 56946|       "revitlookup_referenced": null,
 56947|       "revitlookup_requires_document_context": null
 56948|     },
 56949|     {
 56950|       "source": "Autodesk.Revit.DB.RoutingConditions",
 56951|       "target": "Autodesk.Revit.DB.RoutingCondition",
 56952|       "member_name": "GetConditionAt",
 56953|       "member_kind": "method",
 56954|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 56955|       "confidence": "direct_return_type",
 56956|       "confidence_tier": "unverified_reference",
 56957|       "target_resolution": "exact",
 56958|       "evidence": [
 56959|         "return type 'RoutingCondition' directly names a Revit DB object type"
 56960|       ],
 56961|       "source_url": "https://www.revitapidocs.com/2025/263b2107-de30-fbdf-2951-3aa4391fc64c.htm",
 56962|       "dll_signature_verified": true,
 56963|       "dll_relationship_scope": "declared",
 56964|       "dll_semantic_verified": null,
 56965|       "dll_verified_status": "signature_verified_declared",
 56966|       "revitlookup_referenced": null,
 56967|       "revitlookup_requires_document_context": null
 56968|     },
 56969|     {
 56970|       "source": "Autodesk.Revit.DB.RoutingPreferenceManager",
 56971|       "target": null,
 56972|       "member_name": "OwnerId",
 56973|       "member_kind": "property",
 56974|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 56975|       "confidence": "unknown_reference",
 56976|       "confidence_tier": "unverified_reference",
 56977|       "target_resolution": "none",
 56978|       "evidence": [
 56979|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 56980|       ],
 56981|       "source_url": "https://www.revitapidocs.com/2025/b5870814-9de7-9b7d-783a-550b012239e6.htm",
 56982|       "dll_signature_verified": true,
 56983|       "dll_relationship_scope": "declared",
 56984|       "dll_semantic_verified": null,
 56985|       "dll_verified_status": "signature_verified_declared",
 56986|       "revitlookup_referenced": null,
 56987|       "revitlookup_requires_document_context": null
 56988|     },
 56989|     {
 56990|       "source": "Autodesk.Revit.DB.RoutingPreferenceManager",
 56991|       "target": null,
 56992|       "member_name": "GetMEPPartId",
 56993|       "member_kind": "method",
 56994|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 56995|       "confidence": "unknown_reference",
 56996|       "confidence_tier": "unverified_reference",
 56997|       "target_resolution": "none",
 56998|       "evidence": [
 56999|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 57000|       ],
 57001|       "source_url": "https://www.revitapidocs.com/2025/b0900fd7-828b-c0f7-0729-24191b0d43a3.htm",
 57002|       "dll_signature_verified": true,
 57003|       "dll_relationship_scope": "declared",
 57004|       "dll_semantic_verified": null,
 57005|       "dll_verified_status": "signature_verified_declared",
 57006|       "revitlookup_referenced": null,
 57007|       "revitlookup_requires_document_context": null
 57008|     },
 57009|     {
 57010|       "source": "Autodesk.Revit.DB.RoutingPreferenceManager",
 57011|       "target": "Autodesk.Revit.DB.RoutingPreferenceRule",
 57012|       "member_name": "GetRule",
 57013|       "member_kind": "method",
 57014|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57015|       "confidence": "direct_return_type",
 57016|       "confidence_tier": "unverified_reference",
 57017|       "target_resolution": "exact",
 57018|       "evidence": [
 57019|         "return type 'RoutingPreferenceRule' directly names a Revit DB object type"
 57020|       ],
 57021|       "source_url": "https://www.revitapidocs.com/2025/85f2dafa-381e-60fd-2596-8ebb383f149b.htm",
 57022|       "dll_signature_verified": true,
 57023|       "dll_relationship_scope": "declared",
 57024|       "dll_semantic_verified": null,
 57025|       "dll_verified_status": "signature_verified_declared",
 57026|       "revitlookup_referenced": null,
 57027|       "revitlookup_requires_document_context": null
 57028|     },
 57029|     {
 57030|       "source": "Autodesk.Revit.DB.RoutingPreferenceManager",
 57031|       "target": null,
 57032|       "member_name": "GetSharedSizes",
 57033|       "member_kind": "method",
 57034|       "edge_type": "RETURNS_ELEMENT_IDS",
 57035|       "confidence": "unknown_reference",
 57036|       "confidence_tier": "unverified_reference",
 57037|       "target_resolution": "none",
 57038|       "evidence": [
 57039|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 57040|       ],
 57041|       "source_url": "https://www.revitapidocs.com/2025/50ed8cf7-4dde-9723-0a99-73a90a6c07c0.htm",
 57042|       "dll_signature_verified": true,
 57043|       "dll_relationship_scope": "declared",
 57044|       "dll_semantic_verified": null,
 57045|       "dll_verified_status": "signature_verified_declared",
 57046|       "revitlookup_referenced": null,
 57047|       "revitlookup_requires_document_context": null
 57048|     },
 57049|     {
 57050|       "source": "Autodesk.Revit.DB.RoutingPreferenceRule",
 57051|       "target": null,
 57052|       "member_name": "MEPPartId",
 57053|       "member_kind": "property",
 57054|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 57055|       "confidence": "unknown_reference",
 57056|       "confidence_tier": "unverified_reference",
 57057|       "target_resolution": "none",
 57058|       "evidence": [
 57059|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 57060|       ],
 57061|       "source_url": "https://www.revitapidocs.com/2025/030e41b5-abab-6e90-d383-bf18de43c083.htm",
 57062|       "dll_signature_verified": true,
 57063|       "dll_relationship_scope": "declared",
 57064|       "dll_semantic_verified": null,
 57065|       "dll_verified_status": "signature_verified_declared",
 57066|       "revitlookup_referenced": null,
 57067|       "revitlookup_requires_document_context": null
 57068|     },
 57069|     {
 57070|       "source": "Autodesk.Revit.DB.RoutingPreferenceRule",
 57071|       "target": "Autodesk.Revit.DB.RoutingPreferenceManager",
 57072|       "member_name": "RoutingPreferenceManager",
 57073|       "member_kind": "property",
 57074|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57075|       "confidence": "direct_return_type",
 57076|       "confidence_tier": "unverified_reference",
 57077|       "target_resolution": "exact",
 57078|       "evidence": [
 57079|         "return type 'RoutingPreferenceManager' directly names a Revit DB object type"
 57080|       ],
 57081|       "source_url": "https://www.revitapidocs.com/2025/e340755c-76a9-019c-2d1e-91a221ec1ef4.htm",
 57082|       "dll_signature_verified": true,
 57083|       "dll_relationship_scope": "declared",
 57084|       "dll_semantic_verified": null,
 57085|       "dll_verified_status": "signature_verified_declared",
 57086|       "revitlookup_referenced": null,
 57087|       "revitlookup_requires_document_context": null
 57088|     },
 57089|     {
 57090|       "source": "Autodesk.Revit.DB.RoutingPreferenceRule",
 57091|       "target": "Autodesk.Revit.DB.RoutingCriterionBase",
 57092|       "member_name": "GetCriterion",
 57093|       "member_kind": "method",
 57094|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57095|       "confidence": "direct_return_type",
 57096|       "confidence_tier": "unverified_reference",
 57097|       "target_resolution": "exact",
 57098|       "evidence": [
 57099|         "return type 'RoutingCriterionBase' directly names a Revit DB object type"
 57100|       ],
 57101|       "source_url": "https://www.revitapidocs.com/2025/e682cffb-e451-b662-ab7f-532d2af3b25a.htm",
 57102|       "dll_signature_verified": true,
 57103|       "dll_relationship_scope": "declared",
 57104|       "dll_semantic_verified": null,
 57105|       "dll_verified_status": "signature_verified_declared",
 57106|       "revitlookup_referenced": null,
 57107|       "revitlookup_requires_document_context": null
 57108|     },
 57109|     {
 57110|       "source": "Autodesk.Revit.DB.SaveAsOptions",
 57111|       "target": null,
 57112|       "member_name": "PreviewViewId",
 57113|       "member_kind": "property",
 57114|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 57115|       "confidence": "unknown_reference",
 57116|       "confidence_tier": "unverified_reference",
 57117|       "target_resolution": "none",
 57118|       "evidence": [
 57119|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 57120|       ],
 57121|       "source_url": "https://www.revitapidocs.com/2025/d75283fb-868c-bda3-277b-b0c1d3a65893.htm",
 57122|       "dll_signature_verified": true,
 57123|       "dll_relationship_scope": "declared",
 57124|       "dll_semantic_verified": null,
 57125|       "dll_verified_status": "signature_verified_declared",
 57126|       "revitlookup_referenced": null,
 57127|       "revitlookup_requires_document_context": null
 57128|     },
 57129|     {
 57130|       "source": "Autodesk.Revit.DB.SaveAsOptions",
 57131|       "target": "Autodesk.Revit.DB.WorksharingSaveAsOptions",
 57132|       "member_name": "GetWorksharingOptions",
 57133|       "member_kind": "method",
 57134|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57135|       "confidence": "direct_return_type",
 57136|       "confidence_tier": "unverified_reference",
 57137|       "target_resolution": "exact",
 57138|       "evidence": [
 57139|         "return type 'WorksharingSaveAsOptions' directly names a Revit DB object type"
 57140|       ],
 57141|       "source_url": "https://www.revitapidocs.com/2025/78d0d082-d68d-aa07-2b5e-1e67d5322050.htm",
 57142|       "dll_signature_verified": true,
 57143|       "dll_relationship_scope": "declared",
 57144|       "dll_semantic_verified": null,
 57145|       "dll_verified_status": "signature_verified_declared",
 57146|       "revitlookup_referenced": null,
 57147|       "revitlookup_requires_document_context": null
 57148|     },
 57149|     {
 57150|       "source": "Autodesk.Revit.DB.SaveOptions",
 57151|       "target": null,
 57152|       "member_name": "PreviewViewId",
 57153|       "member_kind": "property",
 57154|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 57155|       "confidence": "unknown_reference",
 57156|       "confidence_tier": "unverified_reference",
 57157|       "target_resolution": "none",
 57158|       "evidence": [
 57159|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 57160|       ],
 57161|       "source_url": "https://www.revitapidocs.com/2025/0bd7569d-9d7e-5ac3-c783-b6db08a30c2d.htm",
 57162|       "dll_signature_verified": true,
 57163|       "dll_relationship_scope": "declared",
 57164|       "dll_semantic_verified": null,
 57165|       "dll_verified_status": "signature_verified_declared",
 57166|       "revitlookup_referenced": null,
 57167|       "revitlookup_requires_document_context": null
 57168|     },
 57169|     {
 57170|       "source": "Autodesk.Revit.DB.SchedulableField",
 57171|       "target": null,
 57172|       "member_name": "ParameterId",
 57173|       "member_kind": "property",
 57174|       "edge_type": "HAS_PARAMETER",
 57175|       "confidence": "elementid_with_strong_name",
 57176|       "confidence_tier": "core",
 57177|       "target_resolution": "none",
 57178|       "evidence": [
 57179|         "member name 'ParameterId' matches keyword pattern /Parameter/"
 57180|       ],
 57181|       "source_url": "https://www.revitapidocs.com/2025/99e6189b-59e1-100d-cca6-d8eb5e7e917a.htm",
 57182|       "dll_signature_verified": true,
 57183|       "dll_relationship_scope": "declared",
 57184|       "dll_semantic_verified": null,
 57185|       "dll_verified_status": "signature_verified_declared",
 57186|       "revitlookup_referenced": null,
 57187|       "revitlookup_requires_document_context": null
 57188|     },
 57189|     {
 57190|       "source": "Autodesk.Revit.DB.SchedulableField",
 57191|       "target": "Autodesk.Revit.DB.CustomFieldData",
 57192|       "member_name": "GetCustomFieldData",
 57193|       "member_kind": "method",
 57194|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57195|       "confidence": "direct_return_type",
 57196|       "confidence_tier": "unverified_reference",
 57197|       "target_resolution": "exact",
 57198|       "evidence": [
 57199|         "return type 'CustomFieldData' directly names a Revit DB object type"
 57200|       ],
 57201|       "source_url": "https://www.revitapidocs.com/2025/abc4328d-5691-043e-60b1-813d430af57b.htm",
 57202|       "dll_signature_verified": true,
 57203|       "dll_relationship_scope": "declared",
 57204|       "dll_semantic_verified": null,
 57205|       "dll_verified_status": "signature_verified_declared",
 57206|       "revitlookup_referenced": null,
 57207|       "revitlookup_requires_document_context": null
 57208|     },
 57209|     {
 57210|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57211|       "target": null,
 57212|       "member_name": "AreaSchemeId",
 57213|       "member_kind": "property",
 57214|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 57215|       "confidence": "unknown_reference",
 57216|       "confidence_tier": "unverified_reference",
 57217|       "target_resolution": "none",
 57218|       "evidence": [
 57219|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 57220|       ],
 57221|       "source_url": "https://www.revitapidocs.com/2025/9f7b2f19-1b20-fa58-72b8-0331bd8bb7bc.htm",
 57222|       "dll_signature_verified": true,
 57223|       "dll_relationship_scope": "declared",
 57224|       "dll_semantic_verified": null,
 57225|       "dll_verified_status": "signature_verified_declared",
 57226|       "revitlookup_referenced": null,
 57227|       "revitlookup_requires_document_context": null
 57228|     },
 57229|     {
 57230|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57231|       "target": "Autodesk.Revit.DB.Category",
 57232|       "member_name": "CategoryId",
 57233|       "member_kind": "property",
 57234|       "edge_type": "HAS_CATEGORY",
 57235|       "confidence": "elementid_with_strong_name",
 57236|       "confidence_tier": "core",
 57237|       "target_resolution": "exact",
 57238|       "evidence": [
 57239|         "member name 'CategoryId' matches keyword pattern /Category/"
 57240|       ],
 57241|       "source_url": "https://www.revitapidocs.com/2025/ec498b0e-3040-fc49-b3cf-9f81c842f271.htm",
 57242|       "dll_signature_verified": true,
 57243|       "dll_relationship_scope": "declared",
 57244|       "dll_semantic_verified": null,
 57245|       "dll_verified_status": "signature_verified_declared",
 57246|       "revitlookup_referenced": null,
 57247|       "revitlookup_requires_document_context": null
 57248|     },
 57249|     {
 57250|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57251|       "target": "Autodesk.Revit.DB.ScheduleDefinition",
 57252|       "member_name": "EmbeddedDefinition",
 57253|       "member_kind": "property",
 57254|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57255|       "confidence": "direct_return_type",
 57256|       "confidence_tier": "unverified_reference",
 57257|       "target_resolution": "exact",
 57258|       "evidence": [
 57259|         "return type 'ScheduleDefinition' directly names a Revit DB object type"
 57260|       ],
 57261|       "source_url": "https://www.revitapidocs.com/2025/a5b4bcde-651a-2579-1f8d-1b98c9fe2a85.htm",
 57262|       "dll_signature_verified": true,
 57263|       "dll_relationship_scope": "declared",
 57264|       "dll_semantic_verified": null,
 57265|       "dll_verified_status": "signature_verified_declared",
 57266|       "revitlookup_referenced": null,
 57267|       "revitlookup_requires_document_context": null
 57268|     },
 57269|     {
 57270|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57271|       "target": null,
 57272|       "member_name": "FamilyId",
 57273|       "member_kind": "property",
 57274|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 57275|       "confidence": "unknown_reference",
 57276|       "confidence_tier": "unverified_reference",
 57277|       "target_resolution": "none",
 57278|       "evidence": [
 57279|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 57280|       ],
 57281|       "source_url": "https://www.revitapidocs.com/2025/4ea3d108-7cf6-c557-89a9-81e52919e8b3.htm",
 57282|       "dll_signature_verified": true,
 57283|       "dll_relationship_scope": "declared",
 57284|       "dll_semantic_verified": null,
 57285|       "dll_verified_status": "signature_verified_declared",
 57286|       "revitlookup_referenced": null,
 57287|       "revitlookup_requires_document_context": null
 57288|     },
 57289|     {
 57290|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57291|       "target": "Autodesk.Revit.DB.ViewSheet",
 57292|       "member_name": "IsFilteredBySheet",
 57293|       "member_kind": "property",
 57294|       "edge_type": "PLACED_ON_SHEET",
 57295|       "confidence": "name_only_candidate",
 57296|       "confidence_tier": "likely",
 57297|       "target_resolution": "exact",
 57298|       "evidence": [
 57299|         "member name 'IsFilteredBySheet' matches keyword pattern /Sheet/ but return type 'bool' gives no type-level confirmation"
 57300|       ],
 57301|       "source_url": "https://www.revitapidocs.com/2025/cb831874-15f8-aaa0-f2e5-aafef137f0af.htm",
 57302|       "dll_signature_verified": true,
 57303|       "dll_relationship_scope": "declared",
 57304|       "dll_semantic_verified": null,
 57305|       "dll_verified_status": "signature_verified_declared",
 57306|       "revitlookup_referenced": null,
 57307|       "revitlookup_requires_document_context": null
 57308|     },
 57309|     {
 57310|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57311|       "target": "Autodesk.Revit.DB.Material",
 57312|       "member_name": "IsMaterialTakeoff",
 57313|       "member_kind": "property",
 57314|       "edge_type": "USES_MATERIAL",
 57315|       "confidence": "name_only_candidate",
 57316|       "confidence_tier": "likely",
 57317|       "target_resolution": "exact",
 57318|       "evidence": [
 57319|         "member name 'IsMaterialTakeoff' matches keyword pattern /Material/ but return type 'bool' gives no type-level confirmation"
 57320|       ],
 57321|       "source_url": "https://www.revitapidocs.com/2025/6e0766f0-1676-38b0-0582-5c0ed69e491a.htm",
 57322|       "dll_signature_verified": true,
 57323|       "dll_relationship_scope": "declared",
 57324|       "dll_semantic_verified": null,
 57325|       "dll_verified_status": "signature_verified_declared",
 57326|       "revitlookup_referenced": null,
 57327|       "revitlookup_requires_document_context": null
 57328|     },
 57329|     {
 57330|       "source": "Autodesk.Revit.DB.ScheduleDefinition",
 57331|       "target": "Autodesk.Revit.DB.ScheduleField",
 57332|       "member_name": "AddField",
 57333|       "member_kind": "method",
 57334|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 57335|       "confidence": "direct_return_type",
 57336|       "confidence_tier": "unverified_reference",
 57337|       "target_resolution": "exact",
 57338|       "evidence": [
 57339|         "return type 'ScheduleField' directly names a Revit DB object type"
 57340|       ],
```
