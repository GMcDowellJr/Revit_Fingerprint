# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 129 of 216
- Original line range: 49921-50320
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 49921|       "revitlookup_requires_document_context": null
 49922|     },
 49923|     {
 49924|       "source": "Autodesk.Revit.DB.LevelAssociationData",
 49925|       "target": "Autodesk.Revit.DB.Level",
 49926|       "member_name": "GetAssociatedLevel",
 49927|       "member_kind": "method",
 49928|       "edge_type": "ASSIGNED_TO_LEVEL",
 49929|       "confidence": "elementid_with_strong_name",
 49930|       "confidence_tier": "core",
 49931|       "target_resolution": "exact",
 49932|       "evidence": [
 49933|         "member name 'GetAssociatedLevel' matches keyword pattern /Level/"
 49934|       ],
 49935|       "source_url": "https://www.revitapidocs.com/2025/edde8824-2335-4e54-f794-90b698b714a4.htm",
 49936|       "dll_signature_verified": true,
 49937|       "dll_relationship_scope": "declared",
 49938|       "dll_semantic_verified": null,
 49939|       "dll_verified_status": "signature_verified_declared",
 49940|       "revitlookup_referenced": null,
 49941|       "revitlookup_requires_document_context": null
 49942|     },
 49943|     {
 49944|       "source": "Autodesk.Revit.DB.LevelAssociationData",
 49945|       "target": "Autodesk.Revit.DB.Level",
 49946|       "member_name": "GetLevelOffset",
 49947|       "member_kind": "method",
 49948|       "edge_type": "ASSIGNED_TO_LEVEL",
 49949|       "confidence": "name_only_candidate",
 49950|       "confidence_tier": "likely",
 49951|       "target_resolution": "exact",
 49952|       "evidence": [
 49953|         "member name 'GetLevelOffset' matches keyword pattern /Level/ but return type 'double' gives no type-level confirmation"
 49954|       ],
 49955|       "source_url": "https://www.revitapidocs.com/2025/9014de80-9442-c1fe-f5ed-fb2c4a4d1eda.htm",
 49956|       "dll_signature_verified": true,
 49957|       "dll_relationship_scope": "declared",
 49958|       "dll_semantic_verified": null,
 49959|       "dll_verified_status": "signature_verified_declared",
 49960|       "revitlookup_referenced": null,
 49961|       "revitlookup_requires_document_context": null
 49962|     },
 49963|     {
 49964|       "source": "Autodesk.Revit.DB.LevelAssociationData",
 49965|       "target": "Autodesk.Revit.DB.Level",
 49966|       "member_name": "SetAssociatedLevel",
 49967|       "member_kind": "method",
 49968|       "edge_type": "ASSIGNED_TO_LEVEL",
 49969|       "confidence": "name_only_candidate",
 49970|       "confidence_tier": "likely",
 49971|       "target_resolution": "exact",
 49972|       "evidence": [
 49973|         "member name 'SetAssociatedLevel' matches keyword pattern /Level/ but return type 'void' gives no type-level confirmation"
 49974|       ],
 49975|       "source_url": "https://www.revitapidocs.com/2025/f9070334-e889-1686-fad3-ba2a9bf7a2d7.htm",
 49976|       "dll_signature_verified": true,
 49977|       "dll_relationship_scope": "declared",
 49978|       "dll_semantic_verified": null,
 49979|       "dll_verified_status": "signature_verified_declared",
 49980|       "revitlookup_referenced": null,
 49981|       "revitlookup_requires_document_context": null
 49982|     },
 49983|     {
 49984|       "source": "Autodesk.Revit.DB.LinearArray",
 49985|       "target": null,
 49986|       "member_name": "ArrayElementsWithoutAssociation",
 49987|       "member_kind": "method",
 49988|       "edge_type": "RETURNS_ELEMENT_IDS",
 49989|       "confidence": "unknown_reference",
 49990|       "confidence_tier": "unverified_reference",
 49991|       "target_resolution": "none",
 49992|       "evidence": [
 49993|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 49994|       ],
 49995|       "source_url": "https://www.revitapidocs.com/2025/5c97b79a-6d6b-5f74-fa3f-c35fbc96d01c.htm",
 49996|       "dll_signature_verified": true,
 49997|       "dll_relationship_scope": "declared",
 49998|       "dll_semantic_verified": null,
 49999|       "dll_verified_status": "signature_verified_declared",
 50000|       "revitlookup_referenced": null,
 50001|       "revitlookup_requires_document_context": null
 50002|     },
 50003|     {
 50004|       "source": "Autodesk.Revit.DB.LinearArray",
 50005|       "target": null,
 50006|       "member_name": "ArrayElementWithoutAssociation",
 50007|       "member_kind": "method",
 50008|       "edge_type": "RETURNS_ELEMENT_IDS",
 50009|       "confidence": "unknown_reference",
 50010|       "confidence_tier": "unverified_reference",
 50011|       "target_resolution": "none",
 50012|       "evidence": [
 50013|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 50014|       ],
 50015|       "source_url": "https://www.revitapidocs.com/2025/260ee73e-2196-79a3-19ac-f942aafc44f4.htm",
 50016|       "dll_signature_verified": true,
 50017|       "dll_relationship_scope": "declared",
 50018|       "dll_semantic_verified": null,
 50019|       "dll_verified_status": "signature_verified_declared",
 50020|       "revitlookup_referenced": null,
 50021|       "revitlookup_requires_document_context": null
 50022|     },
 50023|     {
 50024|       "source": "Autodesk.Revit.DB.LinearArray",
 50025|       "target": null,
 50026|       "member_name": "GetCopiedMemberIds",
 50027|       "member_kind": "method",
 50028|       "edge_type": "RETURNS_ELEMENT_IDS",
 50029|       "confidence": "unknown_reference",
 50030|       "confidence_tier": "unverified_reference",
 50031|       "target_resolution": "none",
 50032|       "evidence": [
 50033|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 50034|       ],
 50035|       "source_url": "https://www.revitapidocs.com/2025/1d424df0-983c-4ebd-b749-6bec157e30b5.htm",
 50036|       "dll_signature_verified": true,
 50037|       "dll_relationship_scope": "declared",
 50038|       "dll_semantic_verified": null,
 50039|       "dll_verified_status": "signature_verified_declared",
 50040|       "revitlookup_referenced": null,
 50041|       "revitlookup_requires_document_context": null
 50042|     },
 50043|     {
 50044|       "source": "Autodesk.Revit.DB.LinearArray",
 50045|       "target": null,
 50046|       "member_name": "GetOriginalMemberIds",
 50047|       "member_kind": "method",
 50048|       "edge_type": "RETURNS_ELEMENT_IDS",
 50049|       "confidence": "unknown_reference",
 50050|       "confidence_tier": "unverified_reference",
 50051|       "target_resolution": "none",
 50052|       "evidence": [
 50053|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 50054|       ],
 50055|       "source_url": "https://www.revitapidocs.com/2025/99315d93-cf2a-8619-49de-0864a5b742cc.htm",
 50056|       "dll_signature_verified": true,
 50057|       "dll_relationship_scope": "declared",
 50058|       "dll_semantic_verified": null,
 50059|       "dll_verified_status": "signature_verified_declared",
 50060|       "revitlookup_referenced": null,
 50061|       "revitlookup_requires_document_context": null
 50062|     },
 50063|     {
 50064|       "source": "Autodesk.Revit.DB.LinePattern",
 50065|       "target": "Autodesk.Revit.DB.LinePatternSegment",
 50066|       "member_name": "GetSegments",
 50067|       "member_kind": "method",
 50068|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 50069|       "confidence": "needs_runtime_validation",
 50070|       "confidence_tier": "needs_validation",
 50071|       "target_resolution": "exact",
 50072|       "evidence": [
 50073|         "return type 'IList < LinePatternSegment >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 50074|       ],
 50075|       "source_url": "https://www.revitapidocs.com/2025/45d93bfd-5217-a420-01a3-e493bcf6356b.htm",
 50076|       "dll_signature_verified": true,
 50077|       "dll_relationship_scope": "declared",
 50078|       "dll_semantic_verified": null,
 50079|       "dll_verified_status": "signature_verified_declared",
 50080|       "revitlookup_referenced": null,
 50081|       "revitlookup_requires_document_context": null
 50082|     },
 50083|     {
 50084|       "source": "Autodesk.Revit.DB.LinePatternElement",
 50085|       "target": "Autodesk.Revit.DB.LinePattern",
 50086|       "member_name": "GetLinePattern",
 50087|       "member_kind": "method",
 50088|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 50089|       "confidence": "direct_return_type",
 50090|       "confidence_tier": "unverified_reference",
 50091|       "target_resolution": "exact",
 50092|       "evidence": [
 50093|         "member name 'GetLinePattern' matches keyword pattern /LinePattern/ implying target 'LinePatternElement', but the actual return type 'LinePattern' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 50094|         "return type 'LinePattern' directly names a Revit DB object type"
 50095|       ],
 50096|       "source_url": "https://www.revitapidocs.com/2025/da768e39-4e08-7586-6665-95c13c69f7b4.htm",
 50097|       "dll_signature_verified": true,
 50098|       "dll_relationship_scope": "declared",
 50099|       "dll_semantic_verified": null,
 50100|       "dll_verified_status": "signature_verified_declared",
 50101|       "revitlookup_referenced": null,
 50102|       "revitlookup_requires_document_context": null
 50103|     },
 50104|     {
 50105|       "source": "Autodesk.Revit.DB.LinePatternElement",
 50106|       "target": "Autodesk.Revit.DB.LinePattern",
 50107|       "member_name": "GetLinePattern",
 50108|       "member_kind": "method",
 50109|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 50110|       "confidence": "direct_return_type",
 50111|       "confidence_tier": "unverified_reference",
 50112|       "target_resolution": "exact",
 50113|       "evidence": [
 50114|         "member name 'GetLinePattern' matches keyword pattern /LinePattern/ implying target 'LinePatternElement', but the actual return type 'LinePattern' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 50115|         "return type 'LinePattern' directly names a Revit DB object type"
 50116|       ],
 50117|       "source_url": "https://www.revitapidocs.com/2025/230a23fa-45d0-d698-2e2d-61d2ecfad0a6.htm",
 50118|       "dll_signature_verified": true,
 50119|       "dll_relationship_scope": "declared",
 50120|       "dll_semantic_verified": null,
 50121|       "dll_verified_status": "signature_verified_declared",
 50122|       "revitlookup_referenced": null,
 50123|       "revitlookup_requires_document_context": null
 50124|     },
 50125|     {
 50126|       "source": "Autodesk.Revit.DB.LinePatternElement",
 50127|       "target": null,
 50128|       "member_name": "GetSolidPatternId",
 50129|       "member_kind": "method",
 50130|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 50131|       "confidence": "unknown_reference",
 50132|       "confidence_tier": "unverified_reference",
 50133|       "target_resolution": "none",
 50134|       "evidence": [
 50135|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 50136|       ],
 50137|       "source_url": "https://www.revitapidocs.com/2025/e52c87b7-4544-372f-70a6-00188f6fd252.htm",
 50138|       "dll_signature_verified": true,
 50139|       "dll_relationship_scope": "declared",
 50140|       "dll_semantic_verified": null,
 50141|       "dll_verified_status": "signature_verified_declared",
 50142|       "revitlookup_referenced": null,
 50143|       "revitlookup_requires_document_context": null
 50144|     },
 50145|     {
 50146|       "source": "Autodesk.Revit.DB.LinePatternElement",
 50147|       "target": "Autodesk.Revit.DB.LinePatternElement",
 50148|       "member_name": "SetLinePattern",
 50149|       "member_kind": "method",
 50150|       "edge_type": "USES_LINE_PATTERN",
 50151|       "confidence": "name_only_candidate",
 50152|       "confidence_tier": "likely",
 50153|       "target_resolution": "exact",
 50154|       "evidence": [
 50155|         "member name 'SetLinePattern' matches keyword pattern /LinePattern/ but return type 'void' gives no type-level confirmation"
 50156|       ],
 50157|       "source_url": "https://www.revitapidocs.com/2025/b57a0bf6-8e27-1f58-8441-c8aee20f9073.htm",
 50158|       "dll_signature_verified": true,
 50159|       "dll_relationship_scope": "declared",
 50160|       "dll_semantic_verified": null,
 50161|       "dll_verified_status": "signature_verified_declared",
 50162|       "revitlookup_referenced": null,
 50163|       "revitlookup_requires_document_context": null
 50164|     },
 50165|     {
 50166|       "source": "Autodesk.Revit.DB.LinePatternSegment",
 50167|       "target": null,
 50168|       "member_name": "Type",
 50169|       "member_kind": "property",
 50170|       "edge_type": "TYPE_OF",
 50171|       "confidence": "name_only_candidate",
 50172|       "confidence_tier": "likely",
 50173|       "target_resolution": "none",
 50174|       "evidence": [
 50175|         "member name 'Type' matches keyword pattern /^(Type|TypeId|GetTypeId)$/ but return type 'LinePatternSegmentType' gives no type-level confirmation"
 50176|       ],
 50177|       "source_url": "https://www.revitapidocs.com/2025/0045c894-895c-978a-40bd-06bae49abc34.htm",
 50178|       "dll_signature_verified": true,
 50179|       "dll_relationship_scope": "declared",
 50180|       "dll_semantic_verified": null,
 50181|       "dll_verified_status": "signature_verified_declared",
 50182|       "revitlookup_referenced": null,
 50183|       "revitlookup_requires_document_context": null
 50184|     },
 50185|     {
 50186|       "source": "Autodesk.Revit.DB.LineProperties",
 50187|       "target": null,
 50188|       "member_name": "PatternId",
 50189|       "member_kind": "property",
 50190|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 50191|       "confidence": "unknown_reference",
 50192|       "confidence_tier": "unverified_reference",
 50193|       "target_resolution": "none",
 50194|       "evidence": [
 50195|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 50196|       ],
 50197|       "source_url": "https://www.revitapidocs.com/2025/1a6f1360-fc61-d312-43aa-7d95ea5032d1.htm",
 50198|       "dll_signature_verified": true,
 50199|       "dll_relationship_scope": "declared",
 50200|       "dll_semantic_verified": null,
 50201|       "dll_verified_status": "signature_verified_declared",
 50202|       "revitlookup_referenced": null,
 50203|       "revitlookup_requires_document_context": null
 50204|     },
 50205|     {
 50206|       "source": "Autodesk.Revit.DB.LineSegment",
 50207|       "target": null,
 50208|       "member_name": "EndParameter",
 50209|       "member_kind": "property",
 50210|       "edge_type": "HAS_PARAMETER",
 50211|       "confidence": "name_only_candidate",
 50212|       "confidence_tier": "likely",
 50213|       "target_resolution": "none",
 50214|       "evidence": [
 50215|         "member name 'EndParameter' matches keyword pattern /Parameter/ but return type 'double' gives no type-level confirmation"
 50216|       ],
 50217|       "source_url": "https://www.revitapidocs.com/2025/9a3656de-d950-4bd5-4efe-fc7ea4c5c934.htm",
 50218|       "dll_signature_verified": true,
 50219|       "dll_relationship_scope": "declared",
 50220|       "dll_semantic_verified": null,
 50221|       "dll_verified_status": "signature_verified_declared",
 50222|       "revitlookup_referenced": null,
 50223|       "revitlookup_requires_document_context": null
 50224|     },
 50225|     {
 50226|       "source": "Autodesk.Revit.DB.LineSegment",
 50227|       "target": "Autodesk.Revit.DB.LineProperties",
 50228|       "member_name": "LineProperties",
 50229|       "member_kind": "property",
 50230|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 50231|       "confidence": "direct_return_type",
 50232|       "confidence_tier": "unverified_reference",
 50233|       "target_resolution": "exact",
 50234|       "evidence": [
 50235|         "return type 'LineProperties' directly names a Revit DB object type"
 50236|       ],
 50237|       "source_url": "https://www.revitapidocs.com/2025/a163a075-7b43-68b0-756c-673b695b1ce1.htm",
 50238|       "dll_signature_verified": true,
 50239|       "dll_relationship_scope": "declared",
 50240|       "dll_semantic_verified": null,
 50241|       "dll_verified_status": "signature_verified_declared",
 50242|       "revitlookup_referenced": null,
 50243|       "revitlookup_requires_document_context": null
 50244|     },
 50245|     {
 50246|       "source": "Autodesk.Revit.DB.LineSegment",
 50247|       "target": null,
 50248|       "member_name": "StartParameter",
 50249|       "member_kind": "property",
 50250|       "edge_type": "HAS_PARAMETER",
 50251|       "confidence": "name_only_candidate",
 50252|       "confidence_tier": "likely",
 50253|       "target_resolution": "none",
 50254|       "evidence": [
 50255|         "member name 'StartParameter' matches keyword pattern /Parameter/ but return type 'double' gives no type-level confirmation"
 50256|       ],
 50257|       "source_url": "https://www.revitapidocs.com/2025/0b81c906-30de-5d34-fe23-1d30bf1c47bc.htm",
 50258|       "dll_signature_verified": true,
 50259|       "dll_relationship_scope": "declared",
 50260|       "dll_semantic_verified": null,
 50261|       "dll_verified_status": "signature_verified_declared",
 50262|       "revitlookup_referenced": null,
 50263|       "revitlookup_requires_document_context": null
 50264|     },
 50265|     {
 50266|       "source": "Autodesk.Revit.DB.LinkElementId",
 50267|       "target": null,
 50268|       "member_name": "HostElementId",
 50269|       "member_kind": "property",
 50270|       "edge_type": "HOSTED_BY",
 50271|       "confidence": "elementid_with_strong_name",
 50272|       "confidence_tier": "core",
 50273|       "target_resolution": "none",
 50274|       "evidence": [
 50275|         "member name 'HostElementId' matches keyword pattern /^GetHosted|Host/"
 50276|       ],
 50277|       "source_url": "https://www.revitapidocs.com/2025/71df8d73-bd2a-6462-fd00-ca1c637200af.htm",
 50278|       "dll_signature_verified": true,
 50279|       "dll_relationship_scope": "declared",
 50280|       "dll_semantic_verified": null,
 50281|       "dll_verified_status": "signature_verified_declared",
 50282|       "revitlookup_referenced": null,
 50283|       "revitlookup_requires_document_context": null
 50284|     },
 50285|     {
 50286|       "source": "Autodesk.Revit.DB.LinkElementId",
 50287|       "target": null,
 50288|       "member_name": "LinkedElementId",
 50289|       "member_kind": "property",
 50290|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 50291|       "confidence": "unknown_reference",
 50292|       "confidence_tier": "unverified_reference",
 50293|       "target_resolution": "none",
 50294|       "evidence": [
 50295|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 50296|       ],
 50297|       "source_url": "https://www.revitapidocs.com/2025/02246788-d4e8-0d71-95dc-95301e95f1a1.htm",
 50298|       "dll_signature_verified": true,
 50299|       "dll_relationship_scope": "declared",
 50300|       "dll_semantic_verified": null,
 50301|       "dll_verified_status": "signature_verified_declared",
 50302|       "revitlookup_referenced": null,
 50303|       "revitlookup_requires_document_context": null
 50304|     },
 50305|     {
 50306|       "source": "Autodesk.Revit.DB.LinkElementId",
 50307|       "target": null,
 50308|       "member_name": "LinkInstanceId",
 50309|       "member_kind": "property",
 50310|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 50311|       "confidence": "unknown_reference",
 50312|       "confidence_tier": "unverified_reference",
 50313|       "target_resolution": "none",
 50314|       "evidence": [
 50315|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 50316|       ],
 50317|       "source_url": "https://www.revitapidocs.com/2025/4d0358df-6aaa-53b0-ebe0-365cba628f03.htm",
 50318|       "dll_signature_verified": true,
 50319|       "dll_relationship_scope": "declared",
 50320|       "dll_semantic_verified": null,
```
