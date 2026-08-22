# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 119 of 216
- Original line range: 46021-46420
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 46021|       "member_kind": "property",
 46022|       "edge_type": "ASSIGNED_TO_LEVEL",
 46023|       "confidence": "name_only_candidate",
 46024|       "confidence_tier": "likely",
 46025|       "target_resolution": "exact",
 46026|       "evidence": [
 46027|         "member name 'LevelsOfDetailValue' matches keyword pattern /Level/ but return type 'int' gives no type-level confirmation"
 46028|       ],
 46029|       "source_url": "https://www.revitapidocs.com/2025/0e1686f0-8a05-eeb3-dd44-9d26e0ca8a09.htm",
 46030|       "dll_signature_verified": true,
 46031|       "dll_relationship_scope": "declared",
 46032|       "dll_semantic_verified": null,
 46033|       "dll_verified_status": "signature_verified_declared",
 46034|       "revitlookup_referenced": null,
 46035|       "revitlookup_requires_document_context": null
 46036|     },
 46037|     {
 46038|       "source": "Autodesk.Revit.DB.FBXExportOptions",
 46039|       "target": "Autodesk.Revit.DB.Level",
 46040|       "member_name": "UseLevelsOfDetail",
 46041|       "member_kind": "property",
 46042|       "edge_type": "ASSIGNED_TO_LEVEL",
 46043|       "confidence": "name_only_candidate",
 46044|       "confidence_tier": "likely",
 46045|       "target_resolution": "exact",
 46046|       "evidence": [
 46047|         "member name 'UseLevelsOfDetail' matches keyword pattern /Level/ but return type 'bool' gives no type-level confirmation"
 46048|       ],
 46049|       "source_url": "https://www.revitapidocs.com/2025/5682347f-0693-1e7b-e4d7-4e35ed5b7815.htm",
 46050|       "dll_signature_verified": true,
 46051|       "dll_relationship_scope": "declared",
 46052|       "dll_semantic_verified": null,
 46053|       "dll_verified_status": "signature_verified_declared",
 46054|       "revitlookup_referenced": null,
 46055|       "revitlookup_requires_document_context": null
 46056|     },
 46057|     {
 46058|       "source": "Autodesk.Revit.DB.FilledRegion",
 46059|       "target": null,
 46060|       "member_name": "GetValidLineStyleIdsForFilledRegion",
 46061|       "member_kind": "method",
 46062|       "edge_type": "RETURNS_ELEMENT_IDS",
 46063|       "confidence": "unknown_reference",
 46064|       "confidence_tier": "unverified_reference",
 46065|       "target_resolution": "none",
 46066|       "evidence": [
 46067|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 46068|       ],
 46069|       "source_url": "https://www.revitapidocs.com/2025/d85532a7-8165-1701-90ac-cf665b47b58a.htm",
 46070|       "dll_signature_verified": true,
 46071|       "dll_relationship_scope": "declared",
 46072|       "dll_semantic_verified": null,
 46073|       "dll_verified_status": "signature_verified_declared",
 46074|       "revitlookup_referenced": null,
 46075|       "revitlookup_requires_document_context": null
 46076|     },
 46077|     {
 46078|       "source": "Autodesk.Revit.DB.FilledRegionType",
 46079|       "target": null,
 46080|       "member_name": "BackgroundPatternId",
 46081|       "member_kind": "property",
 46082|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 46083|       "confidence": "unknown_reference",
 46084|       "confidence_tier": "unverified_reference",
 46085|       "target_resolution": "none",
 46086|       "evidence": [
 46087|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 46088|       ],
 46089|       "source_url": "https://www.revitapidocs.com/2025/eec4b9cd-f084-9732-097c-981620303fdd.htm",
 46090|       "dll_signature_verified": true,
 46091|       "dll_relationship_scope": "declared",
 46092|       "dll_semantic_verified": null,
 46093|       "dll_verified_status": "signature_verified_declared",
 46094|       "revitlookup_referenced": null,
 46095|       "revitlookup_requires_document_context": null
 46096|     },
 46097|     {
 46098|       "source": "Autodesk.Revit.DB.FilledRegionType",
 46099|       "target": null,
 46100|       "member_name": "ForegroundPatternId",
 46101|       "member_kind": "property",
 46102|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 46103|       "confidence": "unknown_reference",
 46104|       "confidence_tier": "unverified_reference",
 46105|       "target_resolution": "none",
 46106|       "evidence": [
 46107|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 46108|       ],
 46109|       "source_url": "https://www.revitapidocs.com/2025/5ac029c0-7345-16eb-3ea7-8028eff9a121.htm",
 46110|       "dll_signature_verified": true,
 46111|       "dll_relationship_scope": "declared",
 46112|       "dll_semantic_verified": null,
 46113|       "dll_verified_status": "signature_verified_declared",
 46114|       "revitlookup_referenced": null,
 46115|       "revitlookup_requires_document_context": null
 46116|     },
 46117|     {
 46118|       "source": "Autodesk.Revit.DB.FilledRegionType",
 46119|       "target": "Autodesk.Revit.DB.FillPatternElement",
 46120|       "member_name": "IsValidFillPatternId",
 46121|       "member_kind": "method",
 46122|       "edge_type": "USES_FILL_PATTERN",
 46123|       "confidence": "name_only_candidate",
 46124|       "confidence_tier": "likely",
 46125|       "target_resolution": "exact",
 46126|       "evidence": [
 46127|         "member name 'IsValidFillPatternId' matches keyword pattern /FillPattern/ but return type 'bool' gives no type-level confirmation"
 46128|       ],
 46129|       "source_url": "https://www.revitapidocs.com/2025/89d56b97-4909-03af-cda7-46e695394124.htm",
 46130|       "dll_signature_verified": true,
 46131|       "dll_relationship_scope": "declared",
 46132|       "dll_semantic_verified": null,
 46133|       "dll_verified_status": "signature_verified_declared",
 46134|       "revitlookup_referenced": null,
 46135|       "revitlookup_requires_document_context": null
 46136|     },
 46137|     {
 46138|       "source": "Autodesk.Revit.DB.FilledRegionType",
 46139|       "target": "Autodesk.Revit.DB.FillPatternElement",
 46140|       "member_name": "IsValidSolidFillPatternId",
 46141|       "member_kind": "method",
 46142|       "edge_type": "USES_FILL_PATTERN",
 46143|       "confidence": "name_only_candidate",
 46144|       "confidence_tier": "likely",
 46145|       "target_resolution": "exact",
 46146|       "evidence": [
 46147|         "member name 'IsValidSolidFillPatternId' matches keyword pattern /FillPattern/ but return type 'bool' gives no type-level confirmation"
 46148|       ],
 46149|       "source_url": "https://www.revitapidocs.com/2025/6daab179-221d-8844-0677-f256de956add.htm",
 46150|       "dll_signature_verified": true,
 46151|       "dll_relationship_scope": "declared",
 46152|       "dll_semantic_verified": null,
 46153|       "dll_verified_status": "signature_verified_declared",
 46154|       "revitlookup_referenced": null,
 46155|       "revitlookup_requires_document_context": null
 46156|     },
 46157|     {
 46158|       "source": "Autodesk.Revit.DB.FillPattern",
 46159|       "target": null,
 46160|       "member_name": "HostOrientation",
 46161|       "member_kind": "property",
 46162|       "edge_type": "HOSTED_BY",
 46163|       "confidence": "name_only_candidate",
 46164|       "confidence_tier": "likely",
 46165|       "target_resolution": "none",
 46166|       "evidence": [
 46167|         "member name 'HostOrientation' matches keyword pattern /^GetHosted|Host/ but return type 'FillPatternHostOrientation' gives no type-level confirmation"
 46168|       ],
 46169|       "source_url": "https://www.revitapidocs.com/2025/fa674429-b175-bfa5-8d63-45d8b3179983.htm",
 46170|       "dll_signature_verified": true,
 46171|       "dll_relationship_scope": "declared",
 46172|       "dll_semantic_verified": null,
 46173|       "dll_verified_status": "signature_verified_declared",
 46174|       "revitlookup_referenced": null,
 46175|       "revitlookup_requires_document_context": null
 46176|     },
 46177|     {
 46178|       "source": "Autodesk.Revit.DB.FillPattern",
 46179|       "target": "Autodesk.Revit.DB.FillGrid",
 46180|       "member_name": "GetFillGrid",
 46181|       "member_kind": "method",
 46182|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 46183|       "confidence": "direct_return_type",
 46184|       "confidence_tier": "unverified_reference",
 46185|       "target_resolution": "exact",
 46186|       "evidence": [
 46187|         "return type 'FillGrid' directly names a Revit DB object type"
 46188|       ],
 46189|       "source_url": "https://www.revitapidocs.com/2025/71be2141-457a-b6ce-9c67-ce7b21097316.htm",
 46190|       "dll_signature_verified": true,
 46191|       "dll_relationship_scope": "declared",
 46192|       "dll_semantic_verified": null,
 46193|       "dll_verified_status": "signature_verified_declared",
 46194|       "revitlookup_referenced": null,
 46195|       "revitlookup_requires_document_context": null
 46196|     },
 46197|     {
 46198|       "source": "Autodesk.Revit.DB.FillPattern",
 46199|       "target": "Autodesk.Revit.DB.FillGrid",
 46200|       "member_name": "GetFillGrids",
 46201|       "member_kind": "method",
 46202|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 46203|       "confidence": "needs_runtime_validation",
 46204|       "confidence_tier": "needs_validation",
 46205|       "target_resolution": "exact",
 46206|       "evidence": [
 46207|         "return type 'IList < FillGrid >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 46208|       ],
 46209|       "source_url": "https://www.revitapidocs.com/2025/d05f4cd1-d5df-093d-693e-545b4250ee29.htm",
 46210|       "dll_signature_verified": true,
 46211|       "dll_relationship_scope": "declared",
 46212|       "dll_semantic_verified": null,
 46213|       "dll_verified_status": "signature_verified_declared",
 46214|       "revitlookup_referenced": null,
 46215|       "revitlookup_requires_document_context": null
 46216|     },
 46217|     {
 46218|       "source": "Autodesk.Revit.DB.FillPatternElement",
 46219|       "target": "Autodesk.Revit.DB.FillPattern",
 46220|       "member_name": "GetFillPattern",
 46221|       "member_kind": "method",
 46222|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 46223|       "confidence": "direct_return_type",
 46224|       "confidence_tier": "unverified_reference",
 46225|       "target_resolution": "exact",
 46226|       "evidence": [
 46227|         "member name 'GetFillPattern' matches keyword pattern /FillPattern/ implying target 'FillPatternElement', but the actual return type 'FillPattern' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 46228|         "return type 'FillPattern' directly names a Revit DB object type"
 46229|       ],
 46230|       "source_url": "https://www.revitapidocs.com/2025/ab2f6e79-2aef-2cac-0d53-bb2651ced0bc.htm",
 46231|       "dll_signature_verified": true,
 46232|       "dll_relationship_scope": "declared",
 46233|       "dll_semantic_verified": null,
 46234|       "dll_verified_status": "signature_verified_declared",
 46235|       "revitlookup_referenced": null,
 46236|       "revitlookup_requires_document_context": null
 46237|     },
 46238|     {
 46239|       "source": "Autodesk.Revit.DB.FillPatternElement",
 46240|       "target": "Autodesk.Revit.DB.FillPatternElement",
 46241|       "member_name": "SetFillPattern",
 46242|       "member_kind": "method",
 46243|       "edge_type": "USES_FILL_PATTERN",
 46244|       "confidence": "name_only_candidate",
 46245|       "confidence_tier": "likely",
 46246|       "target_resolution": "exact",
 46247|       "evidence": [
 46248|         "member name 'SetFillPattern' matches keyword pattern /FillPattern/ but return type 'void' gives no type-level confirmation"
 46249|       ],
 46250|       "source_url": "https://www.revitapidocs.com/2025/86b24b5b-ef47-65e4-8661-bbb62f26a96f.htm",
 46251|       "dll_signature_verified": true,
 46252|       "dll_relationship_scope": "declared",
 46253|       "dll_semantic_verified": null,
 46254|       "dll_verified_status": "signature_verified_declared",
 46255|       "revitlookup_referenced": null,
 46256|       "revitlookup_requires_document_context": null
 46257|     },
 46258|     {
 46259|       "source": "Autodesk.Revit.DB.FilterableValueProvider",
 46260|       "target": null,
 46261|       "member_name": "GetAssociatedGlobalParameterValue",
 46262|       "member_kind": "method",
 46263|       "edge_type": "HAS_PARAMETER",
 46264|       "confidence": "elementid_with_strong_name",
 46265|       "confidence_tier": "core",
 46266|       "target_resolution": "none",
 46267|       "evidence": [
 46268|         "member name 'GetAssociatedGlobalParameterValue' matches keyword pattern /Parameter/"
 46269|       ],
 46270|       "source_url": "https://www.revitapidocs.com/2025/f66d222e-0b19-86fe-2c55-81745329bfcb.htm",
 46271|       "dll_signature_verified": true,
 46272|       "dll_relationship_scope": "declared",
 46273|       "dll_semantic_verified": null,
 46274|       "dll_verified_status": "signature_verified_declared",
 46275|       "revitlookup_referenced": null,
 46276|       "revitlookup_requires_document_context": null
 46277|     },
 46278|     {
 46279|       "source": "Autodesk.Revit.DB.FilterableValueProvider",
 46280|       "target": null,
 46281|       "member_name": "GetElementIdValue",
 46282|       "member_kind": "method",
 46283|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 46284|       "confidence": "unknown_reference",
 46285|       "confidence_tier": "unverified_reference",
 46286|       "target_resolution": "none",
 46287|       "evidence": [
 46288|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 46289|       ],
 46290|       "source_url": "https://www.revitapidocs.com/2025/ba7baf5a-ebf0-091a-7a43-3ef3c0d8d28f.htm",
 46291|       "dll_signature_verified": true,
 46292|       "dll_relationship_scope": "declared",
 46293|       "dll_semantic_verified": null,
 46294|       "dll_verified_status": "signature_verified_declared",
 46295|       "revitlookup_referenced": null,
 46296|       "revitlookup_requires_document_context": null
 46297|     },
 46298|     {
 46299|       "source": "Autodesk.Revit.DB.FilterCategoryRule",
 46300|       "target": null,
 46301|       "member_name": "GetCategories",
 46302|       "member_kind": "method",
 46303|       "edge_type": "RETURNS_ELEMENT_IDS",
 46304|       "confidence": "unknown_reference",
 46305|       "confidence_tier": "unverified_reference",
 46306|       "target_resolution": "none",
 46307|       "evidence": [
 46308|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 46309|       ],
 46310|       "source_url": "https://www.revitapidocs.com/2025/b6c50b64-8745-5453-44c3-726ea09b329c.htm",
 46311|       "dll_signature_verified": true,
 46312|       "dll_relationship_scope": "declared",
 46313|       "dll_semantic_verified": null,
 46314|       "dll_verified_status": "signature_verified_declared",
 46315|       "revitlookup_referenced": null,
 46316|       "revitlookup_requires_document_context": null
 46317|     },
 46318|     {
 46319|       "source": "Autodesk.Revit.DB.FilteredElementCollector",
 46320|       "target": "Autodesk.Revit.DB.Element",
 46321|       "member_name": "FirstElement",
 46322|       "member_kind": "method",
 46323|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 46324|       "confidence": "direct_return_type",
 46325|       "confidence_tier": "unverified_reference",
 46326|       "target_resolution": "exact",
 46327|       "evidence": [
 46328|         "return type 'Element' directly names a Revit DB object type"
 46329|       ],
 46330|       "source_url": "https://www.revitapidocs.com/2025/c8c1cae0-4ac8-a309-e915-6d491137d47e.htm",
 46331|       "dll_signature_verified": true,
 46332|       "dll_relationship_scope": "declared",
 46333|       "dll_semantic_verified": null,
 46334|       "dll_verified_status": "signature_verified_declared",
 46335|       "revitlookup_referenced": null,
 46336|       "revitlookup_requires_document_context": null
 46337|     },
 46338|     {
 46339|       "source": "Autodesk.Revit.DB.FilteredElementCollector",
 46340|       "target": null,
 46341|       "member_name": "FirstElementId",
 46342|       "member_kind": "method",
 46343|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 46344|       "confidence": "unknown_reference",
 46345|       "confidence_tier": "unverified_reference",
 46346|       "target_resolution": "none",
 46347|       "evidence": [
 46348|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 46349|       ],
 46350|       "source_url": "https://www.revitapidocs.com/2025/b1b42ac5-e816-983a-f44d-5cf441ca1ad9.htm",
 46351|       "dll_signature_verified": true,
 46352|       "dll_relationship_scope": "declared",
 46353|       "dll_semantic_verified": null,
 46354|       "dll_verified_status": "signature_verified_declared",
 46355|       "revitlookup_referenced": null,
 46356|       "revitlookup_requires_document_context": null
 46357|     },
 46358|     {
 46359|       "source": "Autodesk.Revit.DB.FilteredElementCollector",
 46360|       "target": "Autodesk.Revit.DB.FilteredElementIdIterator",
 46361|       "member_name": "GetElementIdIterator",
 46362|       "member_kind": "method",
 46363|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 46364|       "confidence": "direct_return_type",
 46365|       "confidence_tier": "unverified_reference",
 46366|       "target_resolution": "exact",
 46367|       "evidence": [
 46368|         "return type 'FilteredElementIdIterator' directly names a Revit DB object type"
 46369|       ],
 46370|       "source_url": "https://www.revitapidocs.com/2025/0b1cdbeb-21ce-a4c5-6cae-253595818085.htm",
 46371|       "dll_signature_verified": true,
 46372|       "dll_relationship_scope": "declared",
 46373|       "dll_semantic_verified": null,
 46374|       "dll_verified_status": "signature_verified_declared",
 46375|       "revitlookup_referenced": null,
 46376|       "revitlookup_requires_document_context": null
 46377|     },
 46378|     {
 46379|       "source": "Autodesk.Revit.DB.FilteredElementCollector",
 46380|       "target": "Autodesk.Revit.DB.FilteredElementIterator",
 46381|       "member_name": "GetElementIterator",
 46382|       "member_kind": "method",
 46383|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 46384|       "confidence": "direct_return_type",
 46385|       "confidence_tier": "unverified_reference",
 46386|       "target_resolution": "exact",
 46387|       "evidence": [
 46388|         "return type 'FilteredElementIterator' directly names a Revit DB object type"
 46389|       ],
 46390|       "source_url": "https://www.revitapidocs.com/2025/7113e21c-90f8-8f58-3b00-407fc1cd56e0.htm",
 46391|       "dll_signature_verified": true,
 46392|       "dll_relationship_scope": "declared",
 46393|       "dll_semantic_verified": null,
 46394|       "dll_verified_status": "signature_verified_declared",
 46395|       "revitlookup_referenced": null,
 46396|       "revitlookup_requires_document_context": null
 46397|     },
 46398|     {
 46399|       "source": "Autodesk.Revit.DB.FilteredElementCollector",
 46400|       "target": null,
 46401|       "member_name": "ToElementIds",
 46402|       "member_kind": "method",
 46403|       "edge_type": "RETURNS_ELEMENT_IDS",
 46404|       "confidence": "unknown_reference",
 46405|       "confidence_tier": "unverified_reference",
 46406|       "target_resolution": "none",
 46407|       "evidence": [
 46408|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 46409|       ],
 46410|       "source_url": "https://www.revitapidocs.com/2025/bfb8c8a2-aa2f-b1bc-7d57-7e3f7d39fcae.htm",
 46411|       "dll_signature_verified": true,
 46412|       "dll_relationship_scope": "declared",
 46413|       "dll_semantic_verified": null,
 46414|       "dll_verified_status": "signature_verified_declared",
 46415|       "revitlookup_referenced": null,
 46416|       "revitlookup_requires_document_context": null
 46417|     },
 46418|     {
 46419|       "source": "Autodesk.Revit.DB.FilteredElementCollector",
 46420|       "target": "Autodesk.Revit.DB.Element",
```
