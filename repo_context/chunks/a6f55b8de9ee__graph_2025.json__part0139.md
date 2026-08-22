# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 139 of 216
- Original line range: 53821-54220
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 53821|       "dll_relationship_scope": "declared",
 53822|       "dll_semantic_verified": null,
 53823|       "dll_verified_status": "signature_verified_declared",
 53824|       "revitlookup_referenced": null,
 53825|       "revitlookup_requires_document_context": null
 53826|     },
 53827|     {
 53828|       "source": "Autodesk.Revit.DB.Part",
 53829|       "target": "Autodesk.Revit.DB.Category",
 53830|       "member_name": "GetSourceElementOriginalCategoryIds",
 53831|       "member_kind": "method",
 53832|       "edge_type": "HAS_CATEGORY",
 53833|       "confidence": "elementid_collection_with_strong_name",
 53834|       "confidence_tier": "core",
 53835|       "target_resolution": "exact",
 53836|       "evidence": [
 53837|         "member name 'GetSourceElementOriginalCategoryIds' matches keyword pattern /Category/"
 53838|       ],
 53839|       "source_url": "https://www.revitapidocs.com/2025/47f225e0-6be6-22fc-8005-4f0e68d95674.htm",
 53840|       "dll_signature_verified": true,
 53841|       "dll_relationship_scope": "declared",
 53842|       "dll_semantic_verified": null,
 53843|       "dll_verified_status": "signature_verified_declared",
 53844|       "revitlookup_referenced": null,
 53845|       "revitlookup_requires_document_context": null
 53846|     },
 53847|     {
 53848|       "source": "Autodesk.Revit.DB.PartMaker",
 53849|       "target": null,
 53850|       "member_name": "GetSourceElementIds",
 53851|       "member_kind": "method",
 53852|       "edge_type": "RETURNS_ELEMENT_IDS",
 53853|       "confidence": "unknown_reference",
 53854|       "confidence_tier": "unverified_reference",
 53855|       "target_resolution": "none",
 53856|       "evidence": [
 53857|         "return type 'ICollection < LinkElementId >' is a collection of ID wrappers with no strong name hint"
 53858|       ],
 53859|       "source_url": "https://www.revitapidocs.com/2025/2c7e323a-070c-977f-3de8-ea09fc86023a.htm",
 53860|       "dll_signature_verified": true,
 53861|       "dll_relationship_scope": "declared",
 53862|       "dll_semantic_verified": null,
 53863|       "dll_verified_status": "signature_verified_declared",
 53864|       "revitlookup_referenced": null,
 53865|       "revitlookup_requires_document_context": null
 53866|     },
 53867|     {
 53868|       "source": "Autodesk.Revit.DB.PartMakerMethodToDivideVolumes",
 53869|       "target": null,
 53870|       "member_name": "DivisionRuleId",
 53871|       "member_kind": "property",
 53872|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 53873|       "confidence": "unknown_reference",
 53874|       "confidence_tier": "unverified_reference",
 53875|       "target_resolution": "none",
 53876|       "evidence": [
 53877|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 53878|       ],
 53879|       "source_url": "https://www.revitapidocs.com/2025/fd62adc1-005d-59b9-cfde-ab413cc7d0f9.htm",
 53880|       "dll_signature_verified": true,
 53881|       "dll_relationship_scope": "declared",
 53882|       "dll_semantic_verified": null,
 53883|       "dll_verified_status": "signature_verified_declared",
 53884|       "revitlookup_referenced": null,
 53885|       "revitlookup_requires_document_context": null
 53886|     },
 53887|     {
 53888|       "source": "Autodesk.Revit.DB.PartMakerMethodToDivideVolumes",
 53889|       "target": null,
 53890|       "member_name": "ProfileType",
 53891|       "member_kind": "property",
 53892|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 53893|       "confidence": "unknown_reference",
 53894|       "confidence_tier": "unverified_reference",
 53895|       "target_resolution": "none",
 53896|       "evidence": [
 53897|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 53898|       ],
 53899|       "source_url": "https://www.revitapidocs.com/2025/34bd82bc-f067-f9f9-6aea-e5fc8990dbe7.htm",
 53900|       "dll_signature_verified": true,
 53901|       "dll_relationship_scope": "declared",
 53902|       "dll_semantic_verified": null,
 53903|       "dll_verified_status": "signature_verified_declared",
 53904|       "revitlookup_referenced": null,
 53905|       "revitlookup_requires_document_context": null
 53906|     },
 53907|     {
 53908|       "source": "Autodesk.Revit.DB.PartUtils",
 53909|       "target": "Autodesk.Revit.DB.PartMaker",
 53910|       "member_name": "DivideParts",
 53911|       "member_kind": "method",
 53912|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 53913|       "confidence": "direct_return_type",
 53914|       "confidence_tier": "unverified_reference",
 53915|       "target_resolution": "exact",
 53916|       "evidence": [
 53917|         "return type 'PartMaker' directly names a Revit DB object type"
 53918|       ],
 53919|       "source_url": "https://www.revitapidocs.com/2025/45950f87-1cd6-fdfa-5167-1f42fb7b2c6b.htm",
 53920|       "dll_signature_verified": true,
 53921|       "dll_relationship_scope": "declared",
 53922|       "dll_semantic_verified": null,
 53923|       "dll_verified_status": "signature_verified_declared",
 53924|       "revitlookup_referenced": null,
 53925|       "revitlookup_requires_document_context": null
 53926|     },
 53927|     {
 53928|       "source": "Autodesk.Revit.DB.PartUtils",
 53929|       "target": "Autodesk.Revit.DB.PartMaker",
 53930|       "member_name": "GetAssociatedPartMaker",
 53931|       "member_kind": "method",
 53932|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 53933|       "confidence": "direct_return_type",
 53934|       "confidence_tier": "unverified_reference",
 53935|       "target_resolution": "exact",
 53936|       "evidence": [
 53937|         "return type 'PartMaker' directly names a Revit DB object type"
 53938|       ],
 53939|       "source_url": "https://www.revitapidocs.com/2025/e0568bef-0c22-177d-a537-f1cf85285876.htm",
 53940|       "dll_signature_verified": true,
 53941|       "dll_relationship_scope": "declared",
 53942|       "dll_semantic_verified": null,
 53943|       "dll_verified_status": "signature_verified_declared",
 53944|       "revitlookup_referenced": null,
 53945|       "revitlookup_requires_document_context": null
 53946|     },
 53947|     {
 53948|       "source": "Autodesk.Revit.DB.PartUtils",
 53949|       "target": "Autodesk.Revit.DB.PartMaker",
 53950|       "member_name": "GetAssociatedPartMaker",
 53951|       "member_kind": "method",
 53952|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 53953|       "confidence": "direct_return_type",
 53954|       "confidence_tier": "unverified_reference",
 53955|       "target_resolution": "exact",
 53956|       "evidence": [
 53957|         "return type 'PartMaker' directly names a Revit DB object type"
 53958|       ],
 53959|       "source_url": "https://www.revitapidocs.com/2025/8eb8008e-fc94-3bed-4000-270041373bdb.htm",
 53960|       "dll_signature_verified": true,
 53961|       "dll_relationship_scope": "declared",
 53962|       "dll_semantic_verified": null,
 53963|       "dll_verified_status": "signature_verified_declared",
 53964|       "revitlookup_referenced": null,
 53965|       "revitlookup_requires_document_context": null
 53966|     },
 53967|     {
 53968|       "source": "Autodesk.Revit.DB.PartUtils",
 53969|       "target": null,
 53970|       "member_name": "GetAssociatedParts",
 53971|       "member_kind": "method",
 53972|       "edge_type": "RETURNS_ELEMENT_IDS",
 53973|       "confidence": "unknown_reference",
 53974|       "confidence_tier": "unverified_reference",
 53975|       "target_resolution": "none",
 53976|       "evidence": [
 53977|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 53978|       ],
 53979|       "source_url": "https://www.revitapidocs.com/2025/73e42274-0b32-4109-db26-7c980504264d.htm",
 53980|       "dll_signature_verified": true,
 53981|       "dll_relationship_scope": "declared",
 53982|       "dll_semantic_verified": null,
 53983|       "dll_verified_status": "signature_verified_declared",
 53984|       "revitlookup_referenced": null,
 53985|       "revitlookup_requires_document_context": null
 53986|     },
 53987|     {
 53988|       "source": "Autodesk.Revit.DB.PartUtils",
 53989|       "target": null,
 53990|       "member_name": "GetAssociatedParts",
 53991|       "member_kind": "method",
 53992|       "edge_type": "RETURNS_ELEMENT_IDS",
 53993|       "confidence": "unknown_reference",
 53994|       "confidence_tier": "unverified_reference",
 53995|       "target_resolution": "none",
 53996|       "evidence": [
 53997|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 53998|       ],
 53999|       "source_url": "https://www.revitapidocs.com/2025/a2eab773-d518-ad13-162c-1f5ff402aeef.htm",
 54000|       "dll_signature_verified": true,
 54001|       "dll_relationship_scope": "declared",
 54002|       "dll_semantic_verified": null,
 54003|       "dll_verified_status": "signature_verified_declared",
 54004|       "revitlookup_referenced": null,
 54005|       "revitlookup_requires_document_context": null
 54006|     },
 54007|     {
 54008|       "source": "Autodesk.Revit.DB.PartUtils",
 54009|       "target": null,
 54010|       "member_name": "GetMergedParts",
 54011|       "member_kind": "method",
 54012|       "edge_type": "RETURNS_ELEMENT_IDS",
 54013|       "confidence": "unknown_reference",
 54014|       "confidence_tier": "unverified_reference",
 54015|       "target_resolution": "none",
 54016|       "evidence": [
 54017|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 54018|       ],
 54019|       "source_url": "https://www.revitapidocs.com/2025/8d2c68f0-386b-75ae-c779-25c7050d4afc.htm",
 54020|       "dll_signature_verified": true,
 54021|       "dll_relationship_scope": "declared",
 54022|       "dll_semantic_verified": null,
 54023|       "dll_verified_status": "signature_verified_declared",
 54024|       "revitlookup_referenced": null,
 54025|       "revitlookup_requires_document_context": null
 54026|     },
 54027|     {
 54028|       "source": "Autodesk.Revit.DB.PartUtils",
 54029|       "target": "Autodesk.Revit.DB.PartMakerMethodToDivideVolumes",
 54030|       "member_name": "GetPartMakerMethodToDivideVolumeFW",
 54031|       "member_kind": "method",
 54032|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 54033|       "confidence": "direct_return_type",
 54034|       "confidence_tier": "unverified_reference",
 54035|       "target_resolution": "exact",
 54036|       "evidence": [
 54037|         "return type 'PartMakerMethodToDivideVolumes' directly names a Revit DB object type"
 54038|       ],
 54039|       "source_url": "https://www.revitapidocs.com/2025/3af27a3a-64c0-517f-f37d-601fae0e9fe1.htm",
 54040|       "dll_signature_verified": true,
 54041|       "dll_relationship_scope": "declared",
 54042|       "dll_semantic_verified": null,
 54043|       "dll_verified_status": "signature_verified_declared",
 54044|       "revitlookup_referenced": null,
 54045|       "revitlookup_requires_document_context": null
 54046|     },
 54047|     {
 54048|       "source": "Autodesk.Revit.DB.PartUtils",
 54049|       "target": null,
 54050|       "member_name": "GetSplittingElements",
 54051|       "member_kind": "method",
 54052|       "edge_type": "RETURNS_ELEMENT_IDS",
 54053|       "confidence": "unknown_reference",
 54054|       "confidence_tier": "unverified_reference",
 54055|       "target_resolution": "none",
 54056|       "evidence": [
 54057|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 54058|       ],
 54059|       "source_url": "https://www.revitapidocs.com/2025/bdf7331d-978c-3e94-bf05-43f2e6394aaf.htm",
 54060|       "dll_signature_verified": true,
 54061|       "dll_relationship_scope": "declared",
 54062|       "dll_semantic_verified": null,
 54063|       "dll_verified_status": "signature_verified_declared",
 54064|       "revitlookup_referenced": null,
 54065|       "revitlookup_requires_document_context": null
 54066|     },
 54067|     {
 54068|       "source": "Autodesk.Revit.DB.PDFExportOptions",
 54069|       "target": null,
 54070|       "member_name": "HideUnreferencedViewTags",
 54071|       "member_kind": "property",
 54072|       "edge_type": "TAGS_ELEMENT",
 54073|       "confidence": "name_only_candidate",
 54074|       "confidence_tier": "likely",
 54075|       "target_resolution": "none",
 54076|       "evidence": [
 54077|         "member name 'HideUnreferencedViewTags' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'bool' gives no type-level confirmation"
 54078|       ],
 54079|       "source_url": "https://www.revitapidocs.com/2025/ccdf2c36-37ca-4512-bd05-81c5a01c0361.htm",
 54080|       "dll_signature_verified": true,
 54081|       "dll_relationship_scope": "declared",
 54082|       "dll_semantic_verified": null,
 54083|       "dll_verified_status": "signature_verified_declared",
 54084|       "revitlookup_referenced": null,
 54085|       "revitlookup_requires_document_context": null
 54086|     },
 54087|     {
 54088|       "source": "Autodesk.Revit.DB.PDFExportOptions",
 54089|       "target": null,
 54090|       "member_name": "ZoomPercentage",
 54091|       "member_kind": "property",
 54092|       "edge_type": "TAGS_ELEMENT",
 54093|       "confidence": "name_only_candidate",
 54094|       "confidence_tier": "likely",
 54095|       "target_resolution": "none",
 54096|       "evidence": [
 54097|         "member name 'ZoomPercentage' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'int' gives no type-level confirmation"
 54098|       ],
 54099|       "source_url": "https://www.revitapidocs.com/2025/1e41aa52-cb4a-811e-d750-5d7a6f500299.htm",
 54100|       "dll_signature_verified": true,
 54101|       "dll_relationship_scope": "declared",
 54102|       "dll_semantic_verified": null,
 54103|       "dll_verified_status": "signature_verified_declared",
 54104|       "revitlookup_referenced": null,
 54105|       "revitlookup_requires_document_context": null
 54106|     },
 54107|     {
 54108|       "source": "Autodesk.Revit.DB.PDFExportOptions",
 54109|       "target": "Autodesk.Revit.DB.TableCellCombinedParameterData",
 54110|       "member_name": "GetNamingRule",
 54111|       "member_kind": "method",
 54112|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 54113|       "confidence": "needs_runtime_validation",
 54114|       "confidence_tier": "needs_validation",
 54115|       "target_resolution": "exact",
 54116|       "evidence": [
 54117|         "return type 'IList < TableCellCombinedParameterData >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 54118|       ],
 54119|       "source_url": "https://www.revitapidocs.com/2025/34fea483-aaa2-6762-a622-57fdc324499f.htm",
 54120|       "dll_signature_verified": true,
 54121|       "dll_relationship_scope": "declared",
 54122|       "dll_semantic_verified": null,
 54123|       "dll_verified_status": "signature_verified_declared",
 54124|       "revitlookup_referenced": null,
 54125|       "revitlookup_requires_document_context": null
 54126|     },
 54127|     {
 54128|       "source": "Autodesk.Revit.DB.PerformanceAdviser",
 54129|       "target": "Autodesk.Revit.DB.PerformanceAdviserRuleId",
 54130|       "member_name": "GetAllRuleIds",
 54131|       "member_kind": "method",
 54132|       "edge_type": "RETURNS_ELEMENT_IDS",
 54133|       "confidence": "needs_runtime_validation",
 54134|       "confidence_tier": "needs_validation",
 54135|       "target_resolution": "exact",
 54136|       "evidence": [
 54137|         "return type 'IList < PerformanceAdviserRuleId >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 54138|       ],
 54139|       "source_url": "https://www.revitapidocs.com/2025/ecad22df-ac2e-8aa6-6d6e-03736f72283a.htm",
 54140|       "dll_signature_verified": true,
 54141|       "dll_relationship_scope": "declared",
 54142|       "dll_semantic_verified": null,
 54143|       "dll_verified_status": "signature_verified_declared",
 54144|       "revitlookup_referenced": null,
 54145|       "revitlookup_requires_document_context": null
 54146|     },
 54147|     {
 54148|       "source": "Autodesk.Revit.DB.PerformanceAdviser",
 54149|       "target": "Autodesk.Revit.DB.ElementFilter",
 54150|       "member_name": "GetElementFilterFromRule",
 54151|       "member_kind": "method",
 54152|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 54153|       "confidence": "direct_return_type",
 54154|       "confidence_tier": "unverified_reference",
 54155|       "target_resolution": "exact",
 54156|       "evidence": [
 54157|         "return type 'ElementFilter' directly names a Revit DB object type"
 54158|       ],
 54159|       "source_url": "https://www.revitapidocs.com/2025/43950427-5e16-19e5-5c5b-96786094eeaa.htm",
 54160|       "dll_signature_verified": true,
 54161|       "dll_relationship_scope": "declared",
 54162|       "dll_semantic_verified": null,
 54163|       "dll_verified_status": "signature_verified_declared",
 54164|       "revitlookup_referenced": null,
 54165|       "revitlookup_requires_document_context": null
 54166|     },
 54167|     {
 54168|       "source": "Autodesk.Revit.DB.PerformanceAdviser",
 54169|       "target": "Autodesk.Revit.DB.ElementFilter",
 54170|       "member_name": "GetElementFilterFromRule",
 54171|       "member_kind": "method",
 54172|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 54173|       "confidence": "direct_return_type",
 54174|       "confidence_tier": "unverified_reference",
 54175|       "target_resolution": "exact",
 54176|       "evidence": [
 54177|         "return type 'ElementFilter' directly names a Revit DB object type"
 54178|       ],
 54179|       "source_url": "https://www.revitapidocs.com/2025/00d71deb-c805-5def-2205-87e20bd5de07.htm",
 54180|       "dll_signature_verified": true,
 54181|       "dll_relationship_scope": "declared",
 54182|       "dll_semantic_verified": null,
 54183|       "dll_verified_status": "signature_verified_declared",
 54184|       "revitlookup_referenced": null,
 54185|       "revitlookup_requires_document_context": null
 54186|     },
 54187|     {
 54188|       "source": "Autodesk.Revit.DB.PerformanceAdviser",
 54189|       "target": "Autodesk.Revit.DB.PerformanceAdviserRuleId",
 54190|       "member_name": "GetRuleId",
 54191|       "member_kind": "method",
 54192|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 54193|       "confidence": "direct_return_type",
 54194|       "confidence_tier": "unverified_reference",
 54195|       "target_resolution": "exact",
 54196|       "evidence": [
 54197|         "return type 'PerformanceAdviserRuleId' directly names a Revit DB object type"
 54198|       ],
 54199|       "source_url": "https://www.revitapidocs.com/2025/b9c94fdb-f4ed-ab9b-ea36-ff52c7725199.htm",
 54200|       "dll_signature_verified": true,
 54201|       "dll_relationship_scope": "declared",
 54202|       "dll_semantic_verified": null,
 54203|       "dll_verified_status": "signature_verified_declared",
 54204|       "revitlookup_referenced": null,
 54205|       "revitlookup_requires_document_context": null
 54206|     },
 54207|     {
 54208|       "source": "Autodesk.Revit.DB.PhaseArray",
 54209|       "target": "Autodesk.Revit.DB.PhaseArrayIterator",
 54210|       "member_name": "ForwardIterator",
 54211|       "member_kind": "method",
 54212|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 54213|       "confidence": "direct_return_type",
 54214|       "confidence_tier": "unverified_reference",
 54215|       "target_resolution": "exact",
 54216|       "evidence": [
 54217|         "return type 'PhaseArrayIterator' directly names a Revit DB object type"
 54218|       ],
 54219|       "source_url": "https://www.revitapidocs.com/2025/432aeda4-3638-52fa-69d5-aae94f3d61ac.htm",
 54220|       "dll_signature_verified": true,
```
