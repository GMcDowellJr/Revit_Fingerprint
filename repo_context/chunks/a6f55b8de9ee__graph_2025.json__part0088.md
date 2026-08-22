# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 88 of 216
- Original line range: 33931-34330
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 33931|     },
 33932|     {
 33933|       "source": "Autodesk.Revit.DB.Category",
 33934|       "target": "Autodesk.Revit.DB.Category",
 33935|       "member_name": "BuiltInCategory",
 33936|       "member_kind": "property",
 33937|       "edge_type": "HAS_CATEGORY",
 33938|       "confidence": "name_only_candidate",
 33939|       "confidence_tier": "likely",
 33940|       "target_resolution": "exact",
 33941|       "evidence": [
 33942|         "member name 'BuiltInCategory' matches keyword pattern /Category/ but return type 'BuiltInCategory' gives no type-level confirmation"
 33943|       ],
 33944|       "source_url": "https://www.revitapidocs.com/2025/c3359fec-7b8d-d106-9380-3ba232ac4d14.htm",
 33945|       "dll_signature_verified": true,
 33946|       "dll_relationship_scope": "declared",
 33947|       "dll_semantic_verified": null,
 33948|       "dll_verified_status": "signature_verified_declared",
 33949|       "revitlookup_referenced": null,
 33950|       "revitlookup_requires_document_context": null
 33951|     },
 33952|     {
 33953|       "source": "Autodesk.Revit.DB.Category",
 33954|       "target": "Autodesk.Revit.DB.Category",
 33955|       "member_name": "CanAddSubcategory",
 33956|       "member_kind": "property",
 33957|       "edge_type": "HAS_CATEGORY",
 33958|       "confidence": "name_only_candidate",
 33959|       "confidence_tier": "likely",
 33960|       "target_resolution": "exact",
 33961|       "evidence": [
 33962|         "member name 'CanAddSubcategory' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 33963|       ],
 33964|       "source_url": "https://www.revitapidocs.com/2025/b785e56e-97c6-88d4-fcc9-05a1169d675b.htm",
 33965|       "dll_signature_verified": true,
 33966|       "dll_relationship_scope": "declared",
 33967|       "dll_semantic_verified": null,
 33968|       "dll_verified_status": "signature_verified_declared",
 33969|       "revitlookup_referenced": null,
 33970|       "revitlookup_requires_document_context": null
 33971|     },
 33972|     {
 33973|       "source": "Autodesk.Revit.DB.Category",
 33974|       "target": "Autodesk.Revit.DB.Category",
 33975|       "member_name": "CategoryType",
 33976|       "member_kind": "property",
 33977|       "edge_type": "HAS_CATEGORY",
 33978|       "confidence": "name_only_candidate",
 33979|       "confidence_tier": "likely",
 33980|       "target_resolution": "exact",
 33981|       "evidence": [
 33982|         "member name 'CategoryType' matches keyword pattern /Category/ but return type 'CategoryType' gives no type-level confirmation"
 33983|       ],
 33984|       "source_url": "https://www.revitapidocs.com/2025/1d6672eb-82d6-f702-661b-a3c59fdbe67b.htm",
 33985|       "dll_signature_verified": true,
 33986|       "dll_relationship_scope": "declared",
 33987|       "dll_semantic_verified": null,
 33988|       "dll_verified_status": "signature_verified_declared",
 33989|       "revitlookup_referenced": null,
 33990|       "revitlookup_requires_document_context": null
 33991|     },
 33992|     {
 33993|       "source": "Autodesk.Revit.DB.Category",
 33994|       "target": "Autodesk.Revit.DB.Material",
 33995|       "member_name": "HasMaterialQuantities",
 33996|       "member_kind": "property",
 33997|       "edge_type": "USES_MATERIAL",
 33998|       "confidence": "name_only_candidate",
 33999|       "confidence_tier": "likely",
 34000|       "target_resolution": "exact",
 34001|       "evidence": [
 34002|         "member name 'HasMaterialQuantities' matches keyword pattern /Material/ but return type 'bool' gives no type-level confirmation"
 34003|       ],
 34004|       "source_url": "https://www.revitapidocs.com/2025/c28ed2ba-c91a-7eb9-94dd-48f802a41c8a.htm",
 34005|       "dll_signature_verified": true,
 34006|       "dll_relationship_scope": "declared",
 34007|       "dll_semantic_verified": null,
 34008|       "dll_verified_status": "signature_verified_declared",
 34009|       "revitlookup_referenced": null,
 34010|       "revitlookup_requires_document_context": null
 34011|     },
 34012|     {
 34013|       "source": "Autodesk.Revit.DB.Category",
 34014|       "target": null,
 34015|       "member_name": "Id",
 34016|       "member_kind": "property",
 34017|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 34018|       "confidence": "unknown_reference",
 34019|       "confidence_tier": "unverified_reference",
 34020|       "target_resolution": "none",
 34021|       "evidence": [
 34022|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 34023|       ],
 34024|       "source_url": "https://www.revitapidocs.com/2025/1588405d-eb7a-cd3d-60a0-f19cfc076109.htm",
 34025|       "dll_signature_verified": true,
 34026|       "dll_relationship_scope": "declared",
 34027|       "dll_semantic_verified": null,
 34028|       "dll_verified_status": "signature_verified_declared",
 34029|       "revitlookup_referenced": null,
 34030|       "revitlookup_requires_document_context": null
 34031|     },
 34032|     {
 34033|       "source": "Autodesk.Revit.DB.Category",
 34034|       "target": null,
 34035|       "member_name": "IsTagCategory",
 34036|       "member_kind": "property",
 34037|       "edge_type": "TAGS_ELEMENT",
 34038|       "confidence": "name_only_candidate",
 34039|       "confidence_tier": "likely",
 34040|       "target_resolution": "none",
 34041|       "evidence": [
 34042|         "member name 'IsTagCategory' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'bool' gives no type-level confirmation"
 34043|       ],
 34044|       "source_url": "https://www.revitapidocs.com/2025/6313ecca-201c-a257-27ee-d9925f49b779.htm",
 34045|       "dll_signature_verified": true,
 34046|       "dll_relationship_scope": "declared",
 34047|       "dll_semantic_verified": null,
 34048|       "dll_verified_status": "signature_verified_declared",
 34049|       "revitlookup_referenced": null,
 34050|       "revitlookup_requires_document_context": null
 34051|     },
 34052|     {
 34053|       "source": "Autodesk.Revit.DB.Category",
 34054|       "target": "Autodesk.Revit.DB.Material",
 34055|       "member_name": "Material",
 34056|       "member_kind": "property",
 34057|       "edge_type": "USES_MATERIAL",
 34058|       "confidence": "direct_return_type",
 34059|       "confidence_tier": "core",
 34060|       "target_resolution": "exact",
 34061|       "evidence": [
 34062|         "return type 'Material' directly names a Revit DB object type"
 34063|       ],
 34064|       "source_url": "https://www.revitapidocs.com/2025/00aa768a-fca2-172f-e5d4-a4d787803983.htm",
 34065|       "dll_signature_verified": true,
 34066|       "dll_relationship_scope": "declared",
 34067|       "dll_semantic_verified": null,
 34068|       "dll_verified_status": "signature_verified_declared",
 34069|       "revitlookup_referenced": null,
 34070|       "revitlookup_requires_document_context": null
 34071|     },
 34072|     {
 34073|       "source": "Autodesk.Revit.DB.Category",
 34074|       "target": "Autodesk.Revit.DB.Category",
 34075|       "member_name": "Parent",
 34076|       "member_kind": "property",
 34077|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 34078|       "confidence": "direct_return_type",
 34079|       "confidence_tier": "unverified_reference",
 34080|       "target_resolution": "exact",
 34081|       "evidence": [
 34082|         "return type 'Category' directly names a Revit DB object type"
 34083|       ],
 34084|       "source_url": "https://www.revitapidocs.com/2025/98caefd8-9d6a-a6f4-7570-f09a7d115276.htm",
 34085|       "dll_signature_verified": true,
 34086|       "dll_relationship_scope": "declared",
 34087|       "dll_semantic_verified": null,
 34088|       "dll_verified_status": "signature_verified_declared",
 34089|       "revitlookup_referenced": null,
 34090|       "revitlookup_requires_document_context": null
 34091|     },
 34092|     {
 34093|       "source": "Autodesk.Revit.DB.Category",
 34094|       "target": "Autodesk.Revit.DB.CategoryNameMap",
 34095|       "member_name": "SubCategories",
 34096|       "member_kind": "property",
 34097|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 34098|       "confidence": "direct_return_type",
 34099|       "confidence_tier": "unverified_reference",
 34100|       "target_resolution": "exact",
 34101|       "evidence": [
 34102|         "return type 'CategoryNameMap' directly names a Revit DB object type"
 34103|       ],
 34104|       "source_url": "https://www.revitapidocs.com/2025/e2f50049-832c-9b72-70b1-2a0a96e16a60.htm",
 34105|       "dll_signature_verified": true,
 34106|       "dll_relationship_scope": "declared",
 34107|       "dll_semantic_verified": null,
 34108|       "dll_verified_status": "signature_verified_declared",
 34109|       "revitlookup_referenced": null,
 34110|       "revitlookup_requires_document_context": null
 34111|     },
 34112|     {
 34113|       "source": "Autodesk.Revit.DB.Category",
 34114|       "target": "Autodesk.Revit.DB.Category",
 34115|       "member_name": "GetBuiltInCategory",
 34116|       "member_kind": "method",
 34117|       "edge_type": "HAS_CATEGORY",
 34118|       "confidence": "name_only_candidate",
 34119|       "confidence_tier": "likely",
 34120|       "target_resolution": "exact",
 34121|       "evidence": [
 34122|         "member name 'GetBuiltInCategory' matches keyword pattern /Category/ but return type 'BuiltInCategory' gives no type-level confirmation"
 34123|       ],
 34124|       "source_url": "https://www.revitapidocs.com/2025/7aa968aa-3c15-7937-c1e7-a899e35b4ee7.htm",
 34125|       "dll_signature_verified": true,
 34126|       "dll_relationship_scope": "declared",
 34127|       "dll_semantic_verified": null,
 34128|       "dll_verified_status": "signature_verified_declared",
 34129|       "revitlookup_referenced": null,
 34130|       "revitlookup_requires_document_context": null
 34131|     },
 34132|     {
 34133|       "source": "Autodesk.Revit.DB.Category",
 34134|       "target": "Autodesk.Revit.DB.GraphicsStyle",
 34135|       "member_name": "GetGraphicsStyle",
 34136|       "member_kind": "method",
 34137|       "edge_type": "REFERENCES",
 34138|       "confidence": "direct_return_type",
 34139|       "confidence_tier": "core",
 34140|       "target_resolution": "exact",
 34141|       "evidence": [
 34142|         "return type 'GraphicsStyle' directly names a Revit DB object type"
 34143|       ],
 34144|       "source_url": "https://www.revitapidocs.com/2025/4ab5abd4-6ffc-0506-079f-c9b2596ff138.htm",
 34145|       "dll_signature_verified": true,
 34146|       "dll_relationship_scope": "declared",
 34147|       "dll_semantic_verified": null,
 34148|       "dll_verified_status": "signature_verified_declared",
 34149|       "revitlookup_referenced": true,
 34150|       "revitlookup_requires_document_context": false
 34151|     },
 34152|     {
 34153|       "source": "Autodesk.Revit.DB.Category",
 34154|       "target": "Autodesk.Revit.DB.LinePatternElement",
 34155|       "member_name": "GetLinePatternId",
 34156|       "member_kind": "method",
 34157|       "edge_type": "USES_LINE_PATTERN",
 34158|       "confidence": "elementid_with_strong_name",
 34159|       "confidence_tier": "core",
 34160|       "target_resolution": "exact",
 34161|       "evidence": [
 34162|         "member name 'GetLinePatternId' matches keyword pattern /LinePattern/"
 34163|       ],
 34164|       "source_url": "https://www.revitapidocs.com/2025/fb42b3c0-86d2-ae03-a5c0-7d499f78e67d.htm",
 34165|       "dll_signature_verified": true,
 34166|       "dll_relationship_scope": "declared",
 34167|       "dll_semantic_verified": null,
 34168|       "dll_verified_status": "signature_verified_declared",
 34169|       "revitlookup_referenced": true,
 34170|       "revitlookup_requires_document_context": false
 34171|     },
 34172|     {
 34173|       "source": "Autodesk.Revit.DB.Category",
 34174|       "target": "Autodesk.Revit.DB.Category",
 34175|       "member_name": "IsBuiltInCategory",
 34176|       "member_kind": "method",
 34177|       "edge_type": "HAS_CATEGORY",
 34178|       "confidence": "name_only_candidate",
 34179|       "confidence_tier": "likely",
 34180|       "target_resolution": "exact",
 34181|       "evidence": [
 34182|         "member name 'IsBuiltInCategory' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 34183|       ],
 34184|       "source_url": "https://www.revitapidocs.com/2025/efbd8409-b82b-11d8-4b20-65ffc27b750e.htm",
 34185|       "dll_signature_verified": true,
 34186|       "dll_relationship_scope": "declared",
 34187|       "dll_semantic_verified": null,
 34188|       "dll_verified_status": "signature_verified_declared",
 34189|       "revitlookup_referenced": null,
 34190|       "revitlookup_requires_document_context": null
 34191|     },
 34192|     {
 34193|       "source": "Autodesk.Revit.DB.Category",
 34194|       "target": "Autodesk.Revit.DB.Category",
 34195|       "member_name": "IsBuiltInCategoryValid",
 34196|       "member_kind": "method",
 34197|       "edge_type": "HAS_CATEGORY",
 34198|       "confidence": "name_only_candidate",
 34199|       "confidence_tier": "likely",
 34200|       "target_resolution": "exact",
 34201|       "evidence": [
 34202|         "member name 'IsBuiltInCategoryValid' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 34203|       ],
 34204|       "source_url": "https://www.revitapidocs.com/2025/15f903ae-3cdf-52b0-4891-fa2d1002e481.htm",
 34205|       "dll_signature_verified": true,
 34206|       "dll_relationship_scope": "declared",
 34207|       "dll_semantic_verified": null,
 34208|       "dll_verified_status": "signature_verified_declared",
 34209|       "revitlookup_referenced": null,
 34210|       "revitlookup_requires_document_context": null
 34211|     },
 34212|     {
 34213|       "source": "Autodesk.Revit.DB.Category",
 34214|       "target": "Autodesk.Revit.DB.LinePatternElement",
 34215|       "member_name": "SetLinePatternId",
 34216|       "member_kind": "method",
 34217|       "edge_type": "USES_LINE_PATTERN",
 34218|       "confidence": "name_only_candidate",
 34219|       "confidence_tier": "likely",
 34220|       "target_resolution": "exact",
 34221|       "evidence": [
 34222|         "member name 'SetLinePatternId' matches keyword pattern /LinePattern/ but return type 'void' gives no type-level confirmation"
 34223|       ],
 34224|       "source_url": "https://www.revitapidocs.com/2025/bc84b2b6-fdaf-5949-244c-8a75cc688ec3.htm",
 34225|       "dll_signature_verified": true,
 34226|       "dll_relationship_scope": "declared",
 34227|       "dll_semantic_verified": null,
 34228|       "dll_verified_status": "signature_verified_declared",
 34229|       "revitlookup_referenced": null,
 34230|       "revitlookup_requires_document_context": null
 34231|     },
 34232|     {
 34233|       "source": "Autodesk.Revit.DB.CategoryNameMap",
 34234|       "target": "Autodesk.Revit.DB.CategoryNameMapIterator",
 34235|       "member_name": "ForwardIterator",
 34236|       "member_kind": "method",
 34237|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 34238|       "confidence": "direct_return_type",
 34239|       "confidence_tier": "unverified_reference",
 34240|       "target_resolution": "exact",
 34241|       "evidence": [
 34242|         "return type 'CategoryNameMapIterator' directly names a Revit DB object type"
 34243|       ],
 34244|       "source_url": "https://www.revitapidocs.com/2025/8dbe2846-e5c2-709d-55d8-eddb1ca17c6e.htm",
 34245|       "dll_signature_verified": true,
 34246|       "dll_relationship_scope": "declared",
 34247|       "dll_semantic_verified": null,
 34248|       "dll_verified_status": "signature_verified_declared",
 34249|       "revitlookup_referenced": null,
 34250|       "revitlookup_requires_document_context": null
 34251|     },
 34252|     {
 34253|       "source": "Autodesk.Revit.DB.CategoryNameMap",
 34254|       "target": "Autodesk.Revit.DB.CategoryNameMapIterator",
 34255|       "member_name": "ReverseIterator",
 34256|       "member_kind": "method",
 34257|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 34258|       "confidence": "direct_return_type",
 34259|       "confidence_tier": "unverified_reference",
 34260|       "target_resolution": "exact",
 34261|       "evidence": [
 34262|         "return type 'CategoryNameMapIterator' directly names a Revit DB object type"
 34263|       ],
 34264|       "source_url": "https://www.revitapidocs.com/2025/243317ae-9a4e-7bc2-57e1-5efddfaeaab6.htm",
 34265|       "dll_signature_verified": true,
 34266|       "dll_relationship_scope": "declared",
 34267|       "dll_semantic_verified": null,
 34268|       "dll_verified_status": "signature_verified_declared",
 34269|       "revitlookup_referenced": null,
 34270|       "revitlookup_requires_document_context": null
 34271|     },
 34272|     {
 34273|       "source": "Autodesk.Revit.DB.CategorySet",
 34274|       "target": "Autodesk.Revit.DB.CategorySetIterator",
 34275|       "member_name": "ForwardIterator",
 34276|       "member_kind": "method",
 34277|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 34278|       "confidence": "direct_return_type",
 34279|       "confidence_tier": "unverified_reference",
 34280|       "target_resolution": "exact",
 34281|       "evidence": [
 34282|         "return type 'CategorySetIterator' directly names a Revit DB object type"
 34283|       ],
 34284|       "source_url": "https://www.revitapidocs.com/2025/58615f4e-e18e-7c01-9047-71d76f052e35.htm",
 34285|       "dll_signature_verified": true,
 34286|       "dll_relationship_scope": "declared",
 34287|       "dll_semantic_verified": null,
 34288|       "dll_verified_status": "signature_verified_declared",
 34289|       "revitlookup_referenced": null,
 34290|       "revitlookup_requires_document_context": null
 34291|     },
 34292|     {
 34293|       "source": "Autodesk.Revit.DB.CategorySet",
 34294|       "target": "Autodesk.Revit.DB.CategorySetIterator",
 34295|       "member_name": "ReverseIterator",
 34296|       "member_kind": "method",
 34297|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 34298|       "confidence": "direct_return_type",
 34299|       "confidence_tier": "unverified_reference",
 34300|       "target_resolution": "exact",
 34301|       "evidence": [
 34302|         "return type 'CategorySetIterator' directly names a Revit DB object type"
 34303|       ],
 34304|       "source_url": "https://www.revitapidocs.com/2025/cc518c35-e736-c152-140a-534d6d68caea.htm",
 34305|       "dll_signature_verified": true,
 34306|       "dll_relationship_scope": "declared",
 34307|       "dll_semantic_verified": null,
 34308|       "dll_verified_status": "signature_verified_declared",
 34309|       "revitlookup_referenced": null,
 34310|       "revitlookup_requires_document_context": null
 34311|     },
 34312|     {
 34313|       "source": "Autodesk.Revit.DB.Ceiling",
 34314|       "target": "Autodesk.Revit.DB.Sketch",
 34315|       "member_name": "SketchId",
 34316|       "member_kind": "property",
 34317|       "edge_type": "DEPENDS_ON",
 34318|       "confidence": "elementid_with_strong_name",
 34319|       "confidence_tier": "core",
 34320|       "target_resolution": "exact",
 34321|       "evidence": [
 34322|         "member name 'SketchId' matches keyword pattern /Sketch(Id)?$/"
 34323|       ],
 34324|       "source_url": "https://www.revitapidocs.com/2025/6ddc8f5d-8090-9418-82fe-67f55649ebac.htm",
 34325|       "dll_signature_verified": true,
 34326|       "dll_relationship_scope": "declared",
 34327|       "dll_semantic_verified": null,
 34328|       "dll_verified_status": "signature_verified_declared",
 34329|       "revitlookup_referenced": null,
 34330|       "revitlookup_requires_document_context": null
```
