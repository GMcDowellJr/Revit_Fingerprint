# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 124 of 216
- Original line range: 47971-48370
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 47971|       "evidence": [
 47972|         "member name 'GetAvailableAttachedDetailGroupTypeIds' matches keyword pattern /^GetMember|Group/"
 47973|       ],
 47974|       "source_url": "https://www.revitapidocs.com/2025/dd127374-e2c5-9c5e-3edd-c1b0ec60e30d.htm",
 47975|       "dll_signature_verified": true,
 47976|       "dll_relationship_scope": "declared",
 47977|       "dll_semantic_verified": null,
 47978|       "dll_verified_status": "signature_verified_declared",
 47979|       "revitlookup_referenced": null,
 47980|       "revitlookup_requires_document_context": null
 47981|     },
 47982|     {
 47983|       "source": "Autodesk.Revit.DB.Group",
 47984|       "target": null,
 47985|       "member_name": "GetMemberIds",
 47986|       "member_kind": "method",
 47987|       "edge_type": "MEMBER_OF_GROUP",
 47988|       "confidence": "elementid_collection_with_strong_name",
 47989|       "confidence_tier": "core",
 47990|       "target_resolution": "none",
 47991|       "evidence": [
 47992|         "member name 'GetMemberIds' matches keyword pattern /^GetMember|Group/"
 47993|       ],
 47994|       "source_url": "https://www.revitapidocs.com/2025/42a745b8-dc2e-ef06-0ad3-7e4c775eea9d.htm",
 47995|       "dll_signature_verified": true,
 47996|       "dll_relationship_scope": "declared",
 47997|       "dll_semantic_verified": null,
 47998|       "dll_verified_status": "signature_verified_declared",
 47999|       "revitlookup_referenced": null,
 48000|       "revitlookup_requires_document_context": null
 48001|     },
 48002|     {
 48003|       "source": "Autodesk.Revit.DB.Group",
 48004|       "target": null,
 48005|       "member_name": "GetShownAttachedDetailGroupTypeIds",
 48006|       "member_kind": "method",
 48007|       "edge_type": "MEMBER_OF_GROUP",
 48008|       "confidence": "elementid_collection_with_strong_name",
 48009|       "confidence_tier": "core",
 48010|       "target_resolution": "none",
 48011|       "evidence": [
 48012|         "member name 'GetShownAttachedDetailGroupTypeIds' matches keyword pattern /^GetMember|Group/"
 48013|       ],
 48014|       "source_url": "https://www.revitapidocs.com/2025/39290399-a9b1-52ed-cfcf-33c24b9b675c.htm",
 48015|       "dll_signature_verified": true,
 48016|       "dll_relationship_scope": "declared",
 48017|       "dll_semantic_verified": null,
 48018|       "dll_verified_status": "signature_verified_declared",
 48019|       "revitlookup_referenced": null,
 48020|       "revitlookup_requires_document_context": null
 48021|     },
 48022|     {
 48023|       "source": "Autodesk.Revit.DB.Group",
 48024|       "target": null,
 48025|       "member_name": "HideAllAttachedDetailGroups",
 48026|       "member_kind": "method",
 48027|       "edge_type": "MEMBER_OF_GROUP",
 48028|       "confidence": "name_only_candidate",
 48029|       "confidence_tier": "likely",
 48030|       "target_resolution": "none",
 48031|       "evidence": [
 48032|         "member name 'HideAllAttachedDetailGroups' matches keyword pattern /^GetMember|Group/ but return type 'void' gives no type-level confirmation"
 48033|       ],
 48034|       "source_url": "https://www.revitapidocs.com/2025/df7937f2-c48b-8549-a7e2-f2fd1cbafa7b.htm",
 48035|       "dll_signature_verified": true,
 48036|       "dll_relationship_scope": "declared",
 48037|       "dll_semantic_verified": null,
 48038|       "dll_verified_status": "signature_verified_declared",
 48039|       "revitlookup_referenced": null,
 48040|       "revitlookup_requires_document_context": null
 48041|     },
 48042|     {
 48043|       "source": "Autodesk.Revit.DB.Group",
 48044|       "target": null,
 48045|       "member_name": "HideAttachedDetailGroups",
 48046|       "member_kind": "method",
 48047|       "edge_type": "MEMBER_OF_GROUP",
 48048|       "confidence": "name_only_candidate",
 48049|       "confidence_tier": "likely",
 48050|       "target_resolution": "none",
 48051|       "evidence": [
 48052|         "member name 'HideAttachedDetailGroups' matches keyword pattern /^GetMember|Group/ but return type 'void' gives no type-level confirmation"
 48053|       ],
 48054|       "source_url": "https://www.revitapidocs.com/2025/660bd48f-dd60-562c-1935-8fcbd258669a.htm",
 48055|       "dll_signature_verified": true,
 48056|       "dll_relationship_scope": "declared",
 48057|       "dll_semantic_verified": null,
 48058|       "dll_verified_status": "signature_verified_declared",
 48059|       "revitlookup_referenced": null,
 48060|       "revitlookup_requires_document_context": null
 48061|     },
 48062|     {
 48063|       "source": "Autodesk.Revit.DB.Group",
 48064|       "target": null,
 48065|       "member_name": "IsCompatibleAttachedDetailGroupType",
 48066|       "member_kind": "method",
 48067|       "edge_type": "MEMBER_OF_GROUP",
 48068|       "confidence": "name_only_candidate",
 48069|       "confidence_tier": "likely",
 48070|       "target_resolution": "none",
 48071|       "evidence": [
 48072|         "member name 'IsCompatibleAttachedDetailGroupType' matches keyword pattern /^GetMember|Group/ but return type 'bool' gives no type-level confirmation"
 48073|       ],
 48074|       "source_url": "https://www.revitapidocs.com/2025/60562c31-ef34-4cbd-77bc-3fe89a8d2f38.htm",
 48075|       "dll_signature_verified": true,
 48076|       "dll_relationship_scope": "declared",
 48077|       "dll_semantic_verified": null,
 48078|       "dll_verified_status": "signature_verified_declared",
 48079|       "revitlookup_referenced": null,
 48080|       "revitlookup_requires_document_context": null
 48081|     },
 48082|     {
 48083|       "source": "Autodesk.Revit.DB.Group",
 48084|       "target": null,
 48085|       "member_name": "ShowAllAttachedDetailGroups",
 48086|       "member_kind": "method",
 48087|       "edge_type": "MEMBER_OF_GROUP",
 48088|       "confidence": "name_only_candidate",
 48089|       "confidence_tier": "likely",
 48090|       "target_resolution": "none",
 48091|       "evidence": [
 48092|         "member name 'ShowAllAttachedDetailGroups' matches keyword pattern /^GetMember|Group/ but return type 'void' gives no type-level confirmation"
 48093|       ],
 48094|       "source_url": "https://www.revitapidocs.com/2025/e6c7cae5-a513-e212-8139-20abf9f40ba1.htm",
 48095|       "dll_signature_verified": true,
 48096|       "dll_relationship_scope": "declared",
 48097|       "dll_semantic_verified": null,
 48098|       "dll_verified_status": "signature_verified_declared",
 48099|       "revitlookup_referenced": null,
 48100|       "revitlookup_requires_document_context": null
 48101|     },
 48102|     {
 48103|       "source": "Autodesk.Revit.DB.Group",
 48104|       "target": null,
 48105|       "member_name": "ShowAttachedDetailGroups",
 48106|       "member_kind": "method",
 48107|       "edge_type": "MEMBER_OF_GROUP",
 48108|       "confidence": "name_only_candidate",
 48109|       "confidence_tier": "likely",
 48110|       "target_resolution": "none",
 48111|       "evidence": [
 48112|         "member name 'ShowAttachedDetailGroups' matches keyword pattern /^GetMember|Group/ but return type 'void' gives no type-level confirmation"
 48113|       ],
 48114|       "source_url": "https://www.revitapidocs.com/2025/2e4d7640-92fa-fc3b-83a2-e492bc8b0269.htm",
 48115|       "dll_signature_verified": true,
 48116|       "dll_relationship_scope": "declared",
 48117|       "dll_semantic_verified": null,
 48118|       "dll_verified_status": "signature_verified_declared",
 48119|       "revitlookup_referenced": null,
 48120|       "revitlookup_requires_document_context": null
 48121|     },
 48122|     {
 48123|       "source": "Autodesk.Revit.DB.Group",
 48124|       "target": null,
 48125|       "member_name": "UngroupMembers",
 48126|       "member_kind": "method",
 48127|       "edge_type": "MEMBER_OF_GROUP",
 48128|       "confidence": "elementid_collection_with_strong_name",
 48129|       "confidence_tier": "core",
 48130|       "target_resolution": "none",
 48131|       "evidence": [
 48132|         "member name 'UngroupMembers' matches keyword pattern /^GetMember|Group/"
 48133|       ],
 48134|       "source_url": "https://www.revitapidocs.com/2025/086b03c7-6a46-a825-1cae-9739a7819d4f.htm",
 48135|       "dll_signature_verified": true,
 48136|       "dll_relationship_scope": "declared",
 48137|       "dll_semantic_verified": null,
 48138|       "dll_verified_status": "signature_verified_declared",
 48139|       "revitlookup_referenced": null,
 48140|       "revitlookup_requires_document_context": null
 48141|     },
 48142|     {
 48143|       "source": "Autodesk.Revit.DB.GroupLoadOptions",
 48144|       "target": "Autodesk.Revit.DB.Level",
 48145|       "member_name": "IncludeLevels",
 48146|       "member_kind": "property",
 48147|       "edge_type": "ASSIGNED_TO_LEVEL",
 48148|       "confidence": "name_only_candidate",
 48149|       "confidence_tier": "likely",
 48150|       "target_resolution": "exact",
 48151|       "evidence": [
 48152|         "member name 'IncludeLevels' matches keyword pattern /Level/ but return type 'bool' gives no type-level confirmation"
 48153|       ],
 48154|       "source_url": "https://www.revitapidocs.com/2025/e866ba7d-7e8b-9500-9e67-a14655904e1b.htm",
 48155|       "dll_signature_verified": true,
 48156|       "dll_relationship_scope": "declared",
 48157|       "dll_semantic_verified": null,
 48158|       "dll_verified_status": "signature_verified_declared",
 48159|       "revitlookup_referenced": null,
 48160|       "revitlookup_requires_document_context": null
 48161|     },
 48162|     {
 48163|       "source": "Autodesk.Revit.DB.GroupLoadOptions",
 48164|       "target": null,
 48165|       "member_name": "ReplaceDuplicatedGroups",
 48166|       "member_kind": "property",
 48167|       "edge_type": "MEMBER_OF_GROUP",
 48168|       "confidence": "name_only_candidate",
 48169|       "confidence_tier": "likely",
 48170|       "target_resolution": "none",
 48171|       "evidence": [
 48172|         "member name 'ReplaceDuplicatedGroups' matches keyword pattern /^GetMember|Group/ but return type 'bool' gives no type-level confirmation"
 48173|       ],
 48174|       "source_url": "https://www.revitapidocs.com/2025/4a2a99b6-42f2-ecb4-30a0-fdd0bf82929e.htm",
 48175|       "dll_signature_verified": true,
 48176|       "dll_relationship_scope": "declared",
 48177|       "dll_semantic_verified": null,
 48178|       "dll_verified_status": "signature_verified_declared",
 48179|       "revitlookup_referenced": null,
 48180|       "revitlookup_requires_document_context": null
 48181|     },
 48182|     {
 48183|       "source": "Autodesk.Revit.DB.GroupLoadOptions",
 48184|       "target": "Autodesk.Revit.DB.IDuplicateTypeNamesHandler",
 48185|       "member_name": "GetDuplicateTypeNamesHandler",
 48186|       "member_kind": "method",
 48187|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 48188|       "confidence": "direct_return_type",
 48189|       "confidence_tier": "unverified_reference",
 48190|       "target_resolution": "exact",
 48191|       "evidence": [
 48192|         "return type 'IDuplicateTypeNamesHandler' directly names a Revit DB object type"
 48193|       ],
 48194|       "source_url": "https://www.revitapidocs.com/2025/632172cb-da11-ab42-2b48-7d91fb783d10.htm",
 48195|       "dll_signature_verified": true,
 48196|       "dll_relationship_scope": "declared",
 48197|       "dll_semantic_verified": null,
 48198|       "dll_verified_status": "signature_verified_declared",
 48199|       "revitlookup_referenced": null,
 48200|       "revitlookup_requires_document_context": null
 48201|     },
 48202|     {
 48203|       "source": "Autodesk.Revit.DB.GroupSet",
 48204|       "target": "Autodesk.Revit.DB.GroupSetIterator",
 48205|       "member_name": "ForwardIterator",
 48206|       "member_kind": "method",
 48207|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 48208|       "confidence": "direct_return_type",
 48209|       "confidence_tier": "unverified_reference",
 48210|       "target_resolution": "exact",
 48211|       "evidence": [
 48212|         "return type 'GroupSetIterator' directly names a Revit DB object type"
 48213|       ],
 48214|       "source_url": "https://www.revitapidocs.com/2025/3e217ff7-7946-1036-78b3-ccab50f03c8b.htm",
 48215|       "dll_signature_verified": true,
 48216|       "dll_relationship_scope": "declared",
 48217|       "dll_semantic_verified": null,
 48218|       "dll_verified_status": "signature_verified_declared",
 48219|       "revitlookup_referenced": null,
 48220|       "revitlookup_requires_document_context": null
 48221|     },
 48222|     {
 48223|       "source": "Autodesk.Revit.DB.GroupSet",
 48224|       "target": "Autodesk.Revit.DB.GroupSetIterator",
 48225|       "member_name": "ReverseIterator",
 48226|       "member_kind": "method",
 48227|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 48228|       "confidence": "direct_return_type",
 48229|       "confidence_tier": "unverified_reference",
 48230|       "target_resolution": "exact",
 48231|       "evidence": [
 48232|         "return type 'GroupSetIterator' directly names a Revit DB object type"
 48233|       ],
 48234|       "source_url": "https://www.revitapidocs.com/2025/8ad9f77b-59b8-9f54-2e78-1d7ed9745087.htm",
 48235|       "dll_signature_verified": true,
 48236|       "dll_relationship_scope": "declared",
 48237|       "dll_semantic_verified": null,
 48238|       "dll_verified_status": "signature_verified_declared",
 48239|       "revitlookup_referenced": null,
 48240|       "revitlookup_requires_document_context": null
 48241|     },
 48242|     {
 48243|       "source": "Autodesk.Revit.DB.GroupType",
 48244|       "target": "Autodesk.Revit.DB.GroupSet",
 48245|       "member_name": "Groups",
 48246|       "member_kind": "property",
 48247|       "edge_type": "MEMBER_OF_GROUP",
 48248|       "confidence": "direct_return_type",
 48249|       "confidence_tier": "core",
 48250|       "target_resolution": "exact",
 48251|       "evidence": [
 48252|         "return type 'GroupSet' directly names a Revit DB object type"
 48253|       ],
 48254|       "source_url": "https://www.revitapidocs.com/2025/e16f237c-0055-ba0e-d6ee-5d8182fcfabd.htm",
 48255|       "dll_signature_verified": true,
 48256|       "dll_relationship_scope": "declared",
 48257|       "dll_semantic_verified": null,
 48258|       "dll_verified_status": "signature_verified_declared",
 48259|       "revitlookup_referenced": null,
 48260|       "revitlookup_requires_document_context": null
 48261|     },
 48262|     {
 48263|       "source": "Autodesk.Revit.DB.GroupType",
 48264|       "target": null,
 48265|       "member_name": "GetAvailableAttachedDetailGroupTypeIds",
 48266|       "member_kind": "method",
 48267|       "edge_type": "MEMBER_OF_GROUP",
 48268|       "confidence": "elementid_collection_with_strong_name",
 48269|       "confidence_tier": "core",
 48270|       "target_resolution": "none",
 48271|       "evidence": [
 48272|         "member name 'GetAvailableAttachedDetailGroupTypeIds' matches keyword pattern /^GetMember|Group/"
 48273|       ],
 48274|       "source_url": "https://www.revitapidocs.com/2025/f1ad6c3a-aea1-fa43-b322-1e90abea12fe.htm",
 48275|       "dll_signature_verified": true,
 48276|       "dll_relationship_scope": "declared",
 48277|       "dll_semantic_verified": null,
 48278|       "dll_verified_status": "signature_verified_declared",
 48279|       "revitlookup_referenced": null,
 48280|       "revitlookup_requires_document_context": null
 48281|     },
 48282|     {
 48283|       "source": "Autodesk.Revit.DB.HermiteSpline",
 48284|       "target": "Autodesk.Revit.DB.DoubleArray",
 48285|       "member_name": "Parameters",
 48286|       "member_kind": "property",
 48287|       "edge_type": "HAS_PARAMETER",
 48288|       "confidence": "direct_return_type",
 48289|       "confidence_tier": "core",
 48290|       "target_resolution": "exact",
 48291|       "evidence": [
 48292|         "return type 'DoubleArray' directly names a Revit DB object type"
 48293|       ],
 48294|       "source_url": "https://www.revitapidocs.com/2025/bcc4791d-e851-a3ca-8027-5882dd71e777.htm",
 48295|       "dll_signature_verified": true,
 48296|       "dll_relationship_scope": "declared",
 48297|       "dll_semantic_verified": null,
 48298|       "dll_verified_status": "signature_verified_declared",
 48299|       "revitlookup_referenced": null,
 48300|       "revitlookup_requires_document_context": null
 48301|     },
 48302|     {
 48303|       "source": "Autodesk.Revit.DB.HomeCamera",
 48304|       "target": "Autodesk.Revit.DB.View",
 48305|       "member_name": "ViewId",
 48306|       "member_kind": "property",
 48307|       "edge_type": "REFERENCES",
 48308|       "confidence": "elementid_with_strong_name",
 48309|       "confidence_tier": "core",
 48310|       "target_resolution": "exact",
 48311|       "evidence": [
 48312|         "member name 'ViewId' matches keyword pattern /^(Get)?ViewId$/"
 48313|       ],
 48314|       "source_url": "https://www.revitapidocs.com/2025/305e7397-71c4-8e01-2ece-06c0d7453873.htm",
 48315|       "dll_signature_verified": true,
 48316|       "dll_relationship_scope": "declared",
 48317|       "dll_semantic_verified": null,
 48318|       "dll_verified_status": "signature_verified_declared",
 48319|       "revitlookup_referenced": null,
 48320|       "revitlookup_requires_document_context": null
 48321|     },
 48322|     {
 48323|       "source": "Autodesk.Revit.DB.HostedSweep",
 48324|       "target": null,
 48325|       "member_name": "GetEndPointParameter",
 48326|       "member_kind": "method",
 48327|       "edge_type": "HAS_PARAMETER",
 48328|       "confidence": "name_only_candidate",
 48329|       "confidence_tier": "likely",
 48330|       "target_resolution": "none",
 48331|       "evidence": [
 48332|         "member name 'GetEndPointParameter' matches keyword pattern /Parameter/ but return type 'double' gives no type-level confirmation"
 48333|       ],
 48334|       "source_url": "https://www.revitapidocs.com/2025/9d932372-7418-6e37-6764-0cc0994959df.htm",
 48335|       "dll_signature_verified": true,
 48336|       "dll_relationship_scope": "declared",
 48337|       "dll_semantic_verified": null,
 48338|       "dll_verified_status": "signature_verified_declared",
 48339|       "revitlookup_referenced": null,
 48340|       "revitlookup_requires_document_context": null
 48341|     },
 48342|     {
 48343|       "source": "Autodesk.Revit.DB.HostedSweep",
 48344|       "target": null,
 48345|       "member_name": "SetEndPointParameter",
 48346|       "member_kind": "method",
 48347|       "edge_type": "HAS_PARAMETER",
 48348|       "confidence": "name_only_candidate",
 48349|       "confidence_tier": "likely",
 48350|       "target_resolution": "none",
 48351|       "evidence": [
 48352|         "member name 'SetEndPointParameter' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 48353|       ],
 48354|       "source_url": "https://www.revitapidocs.com/2025/de365ff6-a402-a803-43de-c9efc847fccd.htm",
 48355|       "dll_signature_verified": true,
 48356|       "dll_relationship_scope": "declared",
 48357|       "dll_semantic_verified": null,
 48358|       "dll_verified_status": "signature_verified_declared",
 48359|       "revitlookup_referenced": null,
 48360|       "revitlookup_requires_document_context": null
 48361|     },
 48362|     {
 48363|       "source": "Autodesk.Revit.DB.HostObjAttributes",
 48364|       "target": "Autodesk.Revit.DB.CompoundStructure",
 48365|       "member_name": "GetCompoundStructure",
 48366|       "member_kind": "method",
 48367|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 48368|       "confidence": "direct_return_type",
 48369|       "confidence_tier": "unverified_reference",
 48370|       "target_resolution": "exact",
```
