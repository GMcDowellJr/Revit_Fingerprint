# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 106 of 216
- Original line range: 40951-41350
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 40951|       "revitlookup_referenced": null,
 40952|       "revitlookup_requires_document_context": null
 40953|     },
 40954|     {
 40955|       "source": "Autodesk.Revit.DB.ElementOwnerViewFilter",
 40956|       "target": "Autodesk.Revit.DB.View",
 40957|       "member_name": "ViewId",
 40958|       "member_kind": "property",
 40959|       "edge_type": "REFERENCES",
 40960|       "confidence": "elementid_with_strong_name",
 40961|       "confidence_tier": "core",
 40962|       "target_resolution": "exact",
 40963|       "evidence": [
 40964|         "member name 'ViewId' matches keyword pattern /^(Get)?ViewId$/"
 40965|       ],
 40966|       "source_url": "https://www.revitapidocs.com/2025/38f07408-d634-20bd-a7bf-7a3a266277e5.htm",
 40967|       "dll_signature_verified": true,
 40968|       "dll_relationship_scope": "declared",
 40969|       "dll_semantic_verified": null,
 40970|       "dll_verified_status": "signature_verified_declared",
 40971|       "revitlookup_referenced": null,
 40972|       "revitlookup_requires_document_context": null
 40973|     },
 40974|     {
 40975|       "source": "Autodesk.Revit.DB.ElementParameterFilter",
 40976|       "target": "Autodesk.Revit.DB.FilterRule",
 40977|       "member_name": "GetRules",
 40978|       "member_kind": "method",
 40979|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 40980|       "confidence": "needs_runtime_validation",
 40981|       "confidence_tier": "needs_validation",
 40982|       "target_resolution": "exact",
 40983|       "evidence": [
 40984|         "return type 'IList < FilterRule >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 40985|       ],
 40986|       "source_url": "https://www.revitapidocs.com/2025/9442366a-20e8-5a36-39dc-79e0d1c98e41.htm",
 40987|       "dll_signature_verified": true,
 40988|       "dll_relationship_scope": "declared",
 40989|       "dll_semantic_verified": null,
 40990|       "dll_verified_status": "signature_verified_declared",
 40991|       "revitlookup_referenced": null,
 40992|       "revitlookup_requires_document_context": null
 40993|     },
 40994|     {
 40995|       "source": "Autodesk.Revit.DB.ElementPhaseStatusFilter",
 40996|       "target": "Autodesk.Revit.DB.Phase",
 40997|       "member_name": "PhaseId",
 40998|       "member_kind": "property",
 40999|       "edge_type": "ASSIGNED_TO_PHASE",
 41000|       "confidence": "elementid_with_strong_name",
 41001|       "confidence_tier": "core",
 41002|       "target_resolution": "exact",
 41003|       "evidence": [
 41004|         "member name 'PhaseId' matches keyword pattern /Phase/"
 41005|       ],
 41006|       "source_url": "https://www.revitapidocs.com/2025/d85cff23-7fd8-b5ec-343c-b6cc069e430a.htm",
 41007|       "dll_signature_verified": true,
 41008|       "dll_relationship_scope": "declared",
 41009|       "dll_semantic_verified": null,
 41010|       "dll_verified_status": "signature_verified_declared",
 41011|       "revitlookup_referenced": null,
 41012|       "revitlookup_requires_document_context": null
 41013|     },
 41014|     {
 41015|       "source": "Autodesk.Revit.DB.ElementPhaseStatusFilter",
 41016|       "target": "Autodesk.Revit.DB.ElementOnPhaseStatus",
 41017|       "member_name": "GetPhaseStatuses",
 41018|       "member_kind": "method",
 41019|       "edge_type": "ASSIGNED_TO_PHASE",
 41020|       "confidence": "needs_runtime_validation",
 41021|       "confidence_tier": "needs_validation",
 41022|       "target_resolution": "exact",
 41023|       "evidence": [
 41024|         "return type 'ICollection < ElementOnPhaseStatus >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 41025|       ],
 41026|       "source_url": "https://www.revitapidocs.com/2025/0cb36c92-6d1c-6715-9e68-fca6a889628e.htm",
 41027|       "dll_signature_verified": true,
 41028|       "dll_relationship_scope": "declared",
 41029|       "dll_semantic_verified": null,
 41030|       "dll_verified_status": "signature_verified_declared",
 41031|       "revitlookup_referenced": null,
 41032|       "revitlookup_requires_document_context": null
 41033|     },
 41034|     {
 41035|       "source": "Autodesk.Revit.DB.ElementRecord",
 41036|       "target": "Autodesk.Revit.DB.Workset",
 41037|       "member_name": "WorksetId",
 41038|       "member_kind": "property",
 41039|       "edge_type": "OWNED_BY_WORKSET",
 41040|       "confidence": "direct_return_type",
 41041|       "confidence_tier": "core",
 41042|       "target_resolution": "exact",
 41043|       "evidence": [
 41044|         "return type 'WorksetId' is a typed identifier that unambiguously names its own target ('Workset') through the type system alone"
 41045|       ],
 41046|       "source_url": "https://www.revitapidocs.com/2025/94731dcf-8f9a-15ba-d956-074ab95ad942.htm",
 41047|       "dll_signature_verified": true,
 41048|       "dll_relationship_scope": "declared",
 41049|       "dll_semantic_verified": null,
 41050|       "dll_verified_status": "signature_verified_declared",
 41051|       "revitlookup_referenced": null,
 41052|       "revitlookup_requires_document_context": null
 41053|     },
 41054|     {
 41055|       "source": "Autodesk.Revit.DB.ElementRecord",
 41056|       "target": "Autodesk.Revit.DB.Category",
 41057|       "member_name": "GetCategoryId",
 41058|       "member_kind": "method",
 41059|       "edge_type": "HAS_CATEGORY",
 41060|       "confidence": "elementid_with_strong_name",
 41061|       "confidence_tier": "core",
 41062|       "target_resolution": "exact",
 41063|       "evidence": [
 41064|         "member name 'GetCategoryId' matches keyword pattern /Category/"
 41065|       ],
 41066|       "source_url": "https://www.revitapidocs.com/2025/33a5395f-c9cd-2a97-7b98-d156f9b1232d.htm",
 41067|       "dll_signature_verified": true,
 41068|       "dll_relationship_scope": "declared",
 41069|       "dll_semantic_verified": null,
 41070|       "dll_verified_status": "signature_verified_declared",
 41071|       "revitlookup_referenced": null,
 41072|       "revitlookup_requires_document_context": null
 41073|     },
 41074|     {
 41075|       "source": "Autodesk.Revit.DB.ElementRecord",
 41076|       "target": "Autodesk.Revit.DB.DesignOption",
 41077|       "member_name": "GetDesignOptionId",
 41078|       "member_kind": "method",
 41079|       "edge_type": "ASSIGNED_TO_DESIGN_OPTION",
 41080|       "confidence": "elementid_with_strong_name",
 41081|       "confidence_tier": "core",
 41082|       "target_resolution": "exact",
 41083|       "evidence": [
 41084|         "member name 'GetDesignOptionId' matches keyword pattern /DesignOption/"
 41085|       ],
 41086|       "source_url": "https://www.revitapidocs.com/2025/5bbbca1e-5577-1ce0-6d74-01809d084d21.htm",
 41087|       "dll_signature_verified": true,
 41088|       "dll_relationship_scope": "declared",
 41089|       "dll_semantic_verified": null,
 41090|       "dll_verified_status": "signature_verified_declared",
 41091|       "revitlookup_referenced": null,
 41092|       "revitlookup_requires_document_context": null
 41093|     },
 41094|     {
 41095|       "source": "Autodesk.Revit.DB.ElementRecord",
 41096|       "target": null,
 41097|       "member_name": "GetId",
 41098|       "member_kind": "method",
 41099|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 41100|       "confidence": "unknown_reference",
 41101|       "confidence_tier": "unverified_reference",
 41102|       "target_resolution": "none",
 41103|       "evidence": [
 41104|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 41105|       ],
 41106|       "source_url": "https://www.revitapidocs.com/2025/ec06aa09-dac7-641a-e852-1cd6c10195dd.htm",
 41107|       "dll_signature_verified": true,
 41108|       "dll_relationship_scope": "declared",
 41109|       "dll_semantic_verified": null,
 41110|       "dll_verified_status": "signature_verified_declared",
 41111|       "revitlookup_referenced": null,
 41112|       "revitlookup_requires_document_context": null
 41113|     },
 41114|     {
 41115|       "source": "Autodesk.Revit.DB.ElementRecord",
 41116|       "target": null,
 41117|       "member_name": "GetOwnerViewId",
 41118|       "member_kind": "method",
 41119|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 41120|       "confidence": "unknown_reference",
 41121|       "confidence_tier": "unverified_reference",
 41122|       "target_resolution": "none",
 41123|       "evidence": [
 41124|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 41125|       ],
 41126|       "source_url": "https://www.revitapidocs.com/2025/24b1d13e-e45f-5594-1f48-1d3d02191eaf.htm",
 41127|       "dll_signature_verified": true,
 41128|       "dll_relationship_scope": "declared",
 41129|       "dll_semantic_verified": null,
 41130|       "dll_verified_status": "signature_verified_declared",
 41131|       "revitlookup_referenced": null,
 41132|       "revitlookup_requires_document_context": null
 41133|     },
 41134|     {
 41135|       "source": "Autodesk.Revit.DB.ElementSet",
 41136|       "target": "Autodesk.Revit.DB.ElementSetIterator",
 41137|       "member_name": "ForwardIterator",
 41138|       "member_kind": "method",
 41139|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 41140|       "confidence": "direct_return_type",
 41141|       "confidence_tier": "unverified_reference",
 41142|       "target_resolution": "exact",
 41143|       "evidence": [
 41144|         "return type 'ElementSetIterator' directly names a Revit DB object type"
 41145|       ],
 41146|       "source_url": "https://www.revitapidocs.com/2025/7669d355-7297-5076-7d40-93070483172a.htm",
 41147|       "dll_signature_verified": true,
 41148|       "dll_relationship_scope": "declared",
 41149|       "dll_semantic_verified": null,
 41150|       "dll_verified_status": "signature_verified_declared",
 41151|       "revitlookup_referenced": null,
 41152|       "revitlookup_requires_document_context": null
 41153|     },
 41154|     {
 41155|       "source": "Autodesk.Revit.DB.ElementSet",
 41156|       "target": "Autodesk.Revit.DB.ElementSetIterator",
 41157|       "member_name": "ReverseIterator",
 41158|       "member_kind": "method",
 41159|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 41160|       "confidence": "direct_return_type",
 41161|       "confidence_tier": "unverified_reference",
 41162|       "target_resolution": "exact",
 41163|       "evidence": [
 41164|         "return type 'ElementSetIterator' directly names a Revit DB object type"
 41165|       ],
 41166|       "source_url": "https://www.revitapidocs.com/2025/e82d9ecb-bc55-090e-08d1-baba5b02a392.htm",
 41167|       "dll_signature_verified": true,
 41168|       "dll_relationship_scope": "declared",
 41169|       "dll_semantic_verified": null,
 41170|       "dll_verified_status": "signature_verified_declared",
 41171|       "revitlookup_referenced": null,
 41172|       "revitlookup_requires_document_context": null
 41173|     },
 41174|     {
 41175|       "source": "Autodesk.Revit.DB.ElementTransformUtils",
 41176|       "target": null,
 41177|       "member_name": "CopyElement",
 41178|       "member_kind": "method",
 41179|       "edge_type": "RETURNS_ELEMENT_IDS",
 41180|       "confidence": "unknown_reference",
 41181|       "confidence_tier": "unverified_reference",
 41182|       "target_resolution": "none",
 41183|       "evidence": [
 41184|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 41185|       ],
 41186|       "source_url": "https://www.revitapidocs.com/2025/d0f532b7-2d30-c1d2-cd58-16237ec168e3.htm",
 41187|       "dll_signature_verified": true,
 41188|       "dll_relationship_scope": "declared",
 41189|       "dll_semantic_verified": null,
 41190|       "dll_verified_status": "signature_verified_declared",
 41191|       "revitlookup_referenced": null,
 41192|       "revitlookup_requires_document_context": null
 41193|     },
 41194|     {
 41195|       "source": "Autodesk.Revit.DB.ElementTransformUtils",
 41196|       "target": null,
 41197|       "member_name": "CopyElements",
 41198|       "member_kind": "method",
 41199|       "edge_type": "RETURNS_ELEMENT_IDS",
 41200|       "confidence": "unknown_reference",
 41201|       "confidence_tier": "unverified_reference",
 41202|       "target_resolution": "none",
 41203|       "evidence": [
 41204|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 41205|       ],
 41206|       "source_url": "https://www.revitapidocs.com/2025/0e533605-477f-dd92-2376-15ff7cd4411c.htm",
 41207|       "dll_signature_verified": true,
 41208|       "dll_relationship_scope": "declared",
 41209|       "dll_semantic_verified": null,
 41210|       "dll_verified_status": "signature_verified_declared",
 41211|       "revitlookup_referenced": null,
 41212|       "revitlookup_requires_document_context": null
 41213|     },
 41214|     {
 41215|       "source": "Autodesk.Revit.DB.ElementTransformUtils",
 41216|       "target": null,
 41217|       "member_name": "CopyElements",
 41218|       "member_kind": "method",
 41219|       "edge_type": "RETURNS_ELEMENT_IDS",
 41220|       "confidence": "unknown_reference",
 41221|       "confidence_tier": "unverified_reference",
 41222|       "target_resolution": "none",
 41223|       "evidence": [
 41224|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 41225|       ],
 41226|       "source_url": "https://www.revitapidocs.com/2025/b22df8f6-3fa3-e177-ffa5-ba6c639fb3dc.htm",
 41227|       "dll_signature_verified": true,
 41228|       "dll_relationship_scope": "declared",
 41229|       "dll_semantic_verified": null,
 41230|       "dll_verified_status": "signature_verified_declared",
 41231|       "revitlookup_referenced": null,
 41232|       "revitlookup_requires_document_context": null
 41233|     },
 41234|     {
 41235|       "source": "Autodesk.Revit.DB.ElementTransformUtils",
 41236|       "target": null,
 41237|       "member_name": "CopyElements",
 41238|       "member_kind": "method",
 41239|       "edge_type": "RETURNS_ELEMENT_IDS",
 41240|       "confidence": "unknown_reference",
 41241|       "confidence_tier": "unverified_reference",
 41242|       "target_resolution": "none",
 41243|       "evidence": [
 41244|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 41245|       ],
 41246|       "source_url": "https://www.revitapidocs.com/2025/0f6a7a2e-13b9-008a-4c41-951a0702d16b.htm",
 41247|       "dll_signature_verified": true,
 41248|       "dll_relationship_scope": "declared",
 41249|       "dll_semantic_verified": null,
 41250|       "dll_verified_status": "signature_verified_declared",
 41251|       "revitlookup_referenced": null,
 41252|       "revitlookup_requires_document_context": null
 41253|     },
 41254|     {
 41255|       "source": "Autodesk.Revit.DB.ElementTransformUtils",
 41256|       "target": null,
 41257|       "member_name": "MirrorElements",
 41258|       "member_kind": "method",
 41259|       "edge_type": "RETURNS_ELEMENT_IDS",
 41260|       "confidence": "unknown_reference",
 41261|       "confidence_tier": "unverified_reference",
 41262|       "target_resolution": "none",
 41263|       "evidence": [
 41264|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 41265|       ],
 41266|       "source_url": "https://www.revitapidocs.com/2025/bb533c52-171a-85f9-8896-c7bb661e129f.htm",
 41267|       "dll_signature_verified": true,
 41268|       "dll_relationship_scope": "declared",
 41269|       "dll_semantic_verified": null,
 41270|       "dll_verified_status": "signature_verified_declared",
 41271|       "revitlookup_referenced": null,
 41272|       "revitlookup_requires_document_context": null
 41273|     },
 41274|     {
 41275|       "source": "Autodesk.Revit.DB.ElementType",
 41276|       "target": null,
 41277|       "member_name": "GetSimilarTypes",
 41278|       "member_kind": "method",
 41279|       "edge_type": "RETURNS_ELEMENT_IDS",
 41280|       "confidence": "unknown_reference",
 41281|       "confidence_tier": "unverified_reference",
 41282|       "target_resolution": "none",
 41283|       "evidence": [
 41284|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 41285|       ],
 41286|       "source_url": "https://www.revitapidocs.com/2025/2719ca23-11c7-dda4-6291-9a4f0cebfb21.htm",
 41287|       "dll_signature_verified": true,
 41288|       "dll_relationship_scope": "declared",
 41289|       "dll_semantic_verified": null,
 41290|       "dll_verified_status": "signature_verified_declared",
 41291|       "revitlookup_referenced": null,
 41292|       "revitlookup_requires_document_context": null
 41293|     },
 41294|     {
 41295|       "source": "Autodesk.Revit.DB.ElementWorksetFilter",
 41296|       "target": "Autodesk.Revit.DB.Workset",
 41297|       "member_name": "WorksetId",
 41298|       "member_kind": "property",
 41299|       "edge_type": "OWNED_BY_WORKSET",
 41300|       "confidence": "direct_return_type",
 41301|       "confidence_tier": "core",
 41302|       "target_resolution": "exact",
 41303|       "evidence": [
 41304|         "return type 'WorksetId' is a typed identifier that unambiguously names its own target ('Workset') through the type system alone"
 41305|       ],
 41306|       "source_url": "https://www.revitapidocs.com/2025/d22c7537-8460-27e2-46b5-f6a115fe319f.htm",
 41307|       "dll_signature_verified": true,
 41308|       "dll_relationship_scope": "declared",
 41309|       "dll_semantic_verified": null,
 41310|       "dll_verified_status": "signature_verified_declared",
 41311|       "revitlookup_referenced": null,
 41312|       "revitlookup_requires_document_context": null
 41313|     },
 41314|     {
 41315|       "source": "Autodesk.Revit.DB.ElevationMarker",
 41316|       "target": "Autodesk.Revit.DB.View",
 41317|       "member_name": "GetViewId",
 41318|       "member_kind": "method",
 41319|       "edge_type": "REFERENCES",
 41320|       "confidence": "elementid_with_strong_name",
 41321|       "confidence_tier": "core",
 41322|       "target_resolution": "exact",
 41323|       "evidence": [
 41324|         "member name 'GetViewId' matches keyword pattern /^(Get)?ViewId$/"
 41325|       ],
 41326|       "source_url": "https://www.revitapidocs.com/2025/a66f506d-5a5f-9a14-5b7b-aacf46c8f08d.htm",
 41327|       "dll_signature_verified": true,
 41328|       "dll_relationship_scope": "declared",
 41329|       "dll_semantic_verified": null,
 41330|       "dll_verified_status": "signature_verified_declared",
 41331|       "revitlookup_referenced": true,
 41332|       "revitlookup_requires_document_context": false
 41333|     },
 41334|     {
 41335|       "source": "Autodesk.Revit.DB.EvaluatedParameter",
 41336|       "target": "Autodesk.Revit.DB.InternalDefinition",
 41337|       "member_name": "Definition",
 41338|       "member_kind": "property",
 41339|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 41340|       "confidence": "direct_return_type",
 41341|       "confidence_tier": "unverified_reference",
 41342|       "target_resolution": "exact",
 41343|       "evidence": [
 41344|         "return type 'InternalDefinition' directly names a Revit DB object type"
 41345|       ],
 41346|       "source_url": "https://www.revitapidocs.com/2025/d2184f58-82a5-472f-4cae-64cbaeeb36c9.htm",
 41347|       "dll_signature_verified": true,
 41348|       "dll_relationship_scope": "declared",
 41349|       "dll_semantic_verified": null,
 41350|       "dll_verified_status": "signature_verified_declared",
```
