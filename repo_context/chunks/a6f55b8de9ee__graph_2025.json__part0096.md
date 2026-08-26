# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 96 of 216
- Original line range: 37051-37450
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 37051|     },
 37052|     {
 37053|       "source": "Autodesk.Revit.DB.DimensionSegmentArray",
 37054|       "target": "Autodesk.Revit.DB.DimensionSegmentArrayIterator",
 37055|       "member_name": "ForwardIterator",
 37056|       "member_kind": "method",
 37057|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37058|       "confidence": "direct_return_type",
 37059|       "confidence_tier": "unverified_reference",
 37060|       "target_resolution": "exact",
 37061|       "evidence": [
 37062|         "return type 'DimensionSegmentArrayIterator' directly names a Revit DB object type"
 37063|       ],
 37064|       "source_url": "https://www.revitapidocs.com/2025/7175c6d7-bb41-f6f6-d7b3-fcd38435b868.htm",
 37065|       "dll_signature_verified": true,
 37066|       "dll_relationship_scope": "declared",
 37067|       "dll_semantic_verified": null,
 37068|       "dll_verified_status": "signature_verified_declared",
 37069|       "revitlookup_referenced": null,
 37070|       "revitlookup_requires_document_context": null
 37071|     },
 37072|     {
 37073|       "source": "Autodesk.Revit.DB.DimensionSegmentArray",
 37074|       "target": "Autodesk.Revit.DB.DimensionSegmentArrayIterator",
 37075|       "member_name": "ReverseIterator",
 37076|       "member_kind": "method",
 37077|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37078|       "confidence": "direct_return_type",
 37079|       "confidence_tier": "unverified_reference",
 37080|       "target_resolution": "exact",
 37081|       "evidence": [
 37082|         "return type 'DimensionSegmentArrayIterator' directly names a Revit DB object type"
 37083|       ],
 37084|       "source_url": "https://www.revitapidocs.com/2025/3890dad2-3ca3-3f06-e182-7984c316d7e3.htm",
 37085|       "dll_signature_verified": true,
 37086|       "dll_relationship_scope": "declared",
 37087|       "dll_semantic_verified": null,
 37088|       "dll_verified_status": "signature_verified_declared",
 37089|       "revitlookup_referenced": null,
 37090|       "revitlookup_requires_document_context": null
 37091|     },
 37092|     {
 37093|       "source": "Autodesk.Revit.DB.DimensionType",
 37094|       "target": "Autodesk.Revit.DB.DimensionEqualityLabelFormatting",
 37095|       "member_name": "GetEqualityFormula",
 37096|       "member_kind": "method",
 37097|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37098|       "confidence": "needs_runtime_validation",
 37099|       "confidence_tier": "needs_validation",
 37100|       "target_resolution": "exact",
 37101|       "evidence": [
 37102|         "return type 'IList < DimensionEqualityLabelFormatting >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 37103|       ],
 37104|       "source_url": "https://www.revitapidocs.com/2025/086fb666-7be0-f093-516e-670b149be97d.htm",
 37105|       "dll_signature_verified": true,
 37106|       "dll_relationship_scope": "declared",
 37107|       "dll_semantic_verified": null,
 37108|       "dll_verified_status": "signature_verified_declared",
 37109|       "revitlookup_referenced": null,
 37110|       "revitlookup_requires_document_context": null
 37111|     },
 37112|     {
 37113|       "source": "Autodesk.Revit.DB.DimensionType",
 37114|       "target": "Autodesk.Revit.DB.OrdinateDimensionSetting",
 37115|       "member_name": "GetOrdinateDimensionSetting",
 37116|       "member_kind": "method",
 37117|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37118|       "confidence": "direct_return_type",
 37119|       "confidence_tier": "unverified_reference",
 37120|       "target_resolution": "exact",
 37121|       "evidence": [
 37122|         "return type 'OrdinateDimensionSetting' directly names a Revit DB object type"
 37123|       ],
 37124|       "source_url": "https://www.revitapidocs.com/2025/97c6109a-f5af-eccf-5db7-2a72f608b685.htm",
 37125|       "dll_signature_verified": true,
 37126|       "dll_relationship_scope": "declared",
 37127|       "dll_semantic_verified": null,
 37128|       "dll_verified_status": "signature_verified_declared",
 37129|       "revitlookup_referenced": null,
 37130|       "revitlookup_requires_document_context": null
 37131|     },
 37132|     {
 37133|       "source": "Autodesk.Revit.DB.DirectShape",
 37134|       "target": null,
 37135|       "member_name": "TypeId",
 37136|       "member_kind": "property",
 37137|       "edge_type": "TYPE_OF",
 37138|       "confidence": "elementid_with_strong_name",
 37139|       "confidence_tier": "core",
 37140|       "target_resolution": "none",
 37141|       "evidence": [
 37142|         "member name 'TypeId' matches keyword pattern /^(Type|TypeId|GetTypeId)$/"
 37143|       ],
 37144|       "source_url": "https://www.revitapidocs.com/2025/349629c7-eb65-d4bd-801b-115e1a52878d.htm",
 37145|       "dll_signature_verified": true,
 37146|       "dll_relationship_scope": "declared",
 37147|       "dll_semantic_verified": null,
 37148|       "dll_verified_status": "signature_verified_declared",
 37149|       "revitlookup_referenced": null,
 37150|       "revitlookup_requires_document_context": null
 37151|     },
 37152|     {
 37153|       "source": "Autodesk.Revit.DB.DirectShape",
 37154|       "target": null,
 37155|       "member_name": "AddExternallyTaggedGeometry",
 37156|       "member_kind": "method",
 37157|       "edge_type": "TAGS_ELEMENT",
 37158|       "confidence": "name_only_candidate",
 37159|       "confidence_tier": "likely",
 37160|       "target_resolution": "none",
 37161|       "evidence": [
 37162|         "member name 'AddExternallyTaggedGeometry' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'void' gives no type-level confirmation"
 37163|       ],
 37164|       "source_url": "https://www.revitapidocs.com/2025/2c551429-8b90-ed46-3e29-a6b3dbc1cb95.htm",
 37165|       "dll_signature_verified": true,
 37166|       "dll_relationship_scope": "declared",
 37167|       "dll_semantic_verified": null,
 37168|       "dll_verified_status": "signature_verified_declared",
 37169|       "revitlookup_referenced": null,
 37170|       "revitlookup_requires_document_context": null
 37171|     },
 37172|     {
 37173|       "source": "Autodesk.Revit.DB.DirectShape",
 37174|       "target": "Autodesk.Revit.DB.ExternallyTaggedGeometryObject",
 37175|       "member_name": "GetExternallyTaggedGeometry",
 37176|       "member_kind": "method",
 37177|       "edge_type": "TAGS_ELEMENT",
 37178|       "confidence": "direct_return_type",
 37179|       "confidence_tier": "core",
 37180|       "target_resolution": "exact",
 37181|       "evidence": [
 37182|         "return type 'ExternallyTaggedGeometryObject' directly names a Revit DB object type"
 37183|       ],
 37184|       "source_url": "https://www.revitapidocs.com/2025/a32792d0-1633-37c5-6ed0-02bdb9b6c4b5.htm",
 37185|       "dll_signature_verified": true,
 37186|       "dll_relationship_scope": "declared",
 37187|       "dll_semantic_verified": null,
 37188|       "dll_verified_status": "signature_verified_declared",
 37189|       "revitlookup_referenced": null,
 37190|       "revitlookup_requires_document_context": null
 37191|     },
 37192|     {
 37193|       "source": "Autodesk.Revit.DB.DirectShape",
 37194|       "target": "Autodesk.Revit.DB.Reference",
 37195|       "member_name": "GetExternallyTaggedReference",
 37196|       "member_kind": "method",
 37197|       "edge_type": "TAGS_ELEMENT",
 37198|       "confidence": "direct_return_type",
 37199|       "confidence_tier": "core",
 37200|       "target_resolution": "exact",
 37201|       "evidence": [
 37202|         "return type 'Reference' directly names a Revit DB object type"
 37203|       ],
 37204|       "source_url": "https://www.revitapidocs.com/2025/22cf5a24-787e-ced2-daa4-8f23d1f2b96b.htm",
 37205|       "dll_signature_verified": true,
 37206|       "dll_relationship_scope": "declared",
 37207|       "dll_semantic_verified": null,
 37208|       "dll_verified_status": "signature_verified_declared",
 37209|       "revitlookup_referenced": null,
 37210|       "revitlookup_requires_document_context": null
 37211|     },
 37212|     {
 37213|       "source": "Autodesk.Revit.DB.DirectShape",
 37214|       "target": "Autodesk.Revit.DB.DirectShapeOptions",
 37215|       "member_name": "GetOptions",
 37216|       "member_kind": "method",
 37217|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37218|       "confidence": "direct_return_type",
 37219|       "confidence_tier": "unverified_reference",
 37220|       "target_resolution": "exact",
 37221|       "evidence": [
 37222|         "return type 'DirectShapeOptions' directly names a Revit DB object type"
 37223|       ],
 37224|       "source_url": "https://www.revitapidocs.com/2025/2bc77946-040e-7c75-0258-373adb8d2966.htm",
 37225|       "dll_signature_verified": true,
 37226|       "dll_relationship_scope": "declared",
 37227|       "dll_semantic_verified": null,
 37228|       "dll_verified_status": "signature_verified_declared",
 37229|       "revitlookup_referenced": null,
 37230|       "revitlookup_requires_document_context": null
 37231|     },
 37232|     {
 37233|       "source": "Autodesk.Revit.DB.DirectShape",
 37234|       "target": null,
 37235|       "member_name": "HasExternallyTaggedReference",
 37236|       "member_kind": "method",
 37237|       "edge_type": "TAGS_ELEMENT",
 37238|       "confidence": "name_only_candidate",
 37239|       "confidence_tier": "likely",
 37240|       "target_resolution": "none",
 37241|       "evidence": [
 37242|         "member name 'HasExternallyTaggedReference' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'bool' gives no type-level confirmation"
 37243|       ],
 37244|       "source_url": "https://www.revitapidocs.com/2025/98a5037a-d40a-4073-30f1-f230982f7456.htm",
 37245|       "dll_signature_verified": true,
 37246|       "dll_relationship_scope": "declared",
 37247|       "dll_semantic_verified": null,
 37248|       "dll_verified_status": "signature_verified_declared",
 37249|       "revitlookup_referenced": null,
 37250|       "revitlookup_requires_document_context": null
 37251|     },
 37252|     {
 37253|       "source": "Autodesk.Revit.DB.DirectShape",
 37254|       "target": "Autodesk.Revit.DB.Category",
 37255|       "member_name": "IsValidCategoryId",
 37256|       "member_kind": "method",
 37257|       "edge_type": "HAS_CATEGORY",
 37258|       "confidence": "name_only_candidate",
 37259|       "confidence_tier": "likely",
 37260|       "target_resolution": "exact",
 37261|       "evidence": [
 37262|         "member name 'IsValidCategoryId' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 37263|       ],
 37264|       "source_url": "https://www.revitapidocs.com/2025/c8881d31-d410-842a-1375-5ba688191cf8.htm",
 37265|       "dll_signature_verified": true,
 37266|       "dll_relationship_scope": "declared",
 37267|       "dll_semantic_verified": null,
 37268|       "dll_verified_status": "signature_verified_declared",
 37269|       "revitlookup_referenced": null,
 37270|       "revitlookup_requires_document_context": null
 37271|     },
 37272|     {
 37273|       "source": "Autodesk.Revit.DB.DirectShape",
 37274|       "target": null,
 37275|       "member_name": "RemoveExternallyTaggedGeometry",
 37276|       "member_kind": "method",
 37277|       "edge_type": "TAGS_ELEMENT",
 37278|       "confidence": "name_only_candidate",
 37279|       "confidence_tier": "likely",
 37280|       "target_resolution": "none",
 37281|       "evidence": [
 37282|         "member name 'RemoveExternallyTaggedGeometry' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'void' gives no type-level confirmation"
 37283|       ],
 37284|       "source_url": "https://www.revitapidocs.com/2025/f25a46b4-5eb7-f503-1599-bfb3b549d632.htm",
 37285|       "dll_signature_verified": true,
 37286|       "dll_relationship_scope": "declared",
 37287|       "dll_semantic_verified": null,
 37288|       "dll_verified_status": "signature_verified_declared",
 37289|       "revitlookup_referenced": null,
 37290|       "revitlookup_requires_document_context": null
 37291|     },
 37292|     {
 37293|       "source": "Autodesk.Revit.DB.DirectShape",
 37294|       "target": null,
 37295|       "member_name": "ResetExternallyTaggedGeometry",
 37296|       "member_kind": "method",
 37297|       "edge_type": "TAGS_ELEMENT",
 37298|       "confidence": "name_only_candidate",
 37299|       "confidence_tier": "likely",
 37300|       "target_resolution": "none",
 37301|       "evidence": [
 37302|         "member name 'ResetExternallyTaggedGeometry' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'void' gives no type-level confirmation"
 37303|       ],
 37304|       "source_url": "https://www.revitapidocs.com/2025/c209aeeb-03c9-9dbb-6b85-dd7e26188c5c.htm",
 37305|       "dll_signature_verified": true,
 37306|       "dll_relationship_scope": "declared",
 37307|       "dll_semantic_verified": null,
 37308|       "dll_verified_status": "signature_verified_declared",
 37309|       "revitlookup_referenced": null,
 37310|       "revitlookup_requires_document_context": null
 37311|     },
 37312|     {
 37313|       "source": "Autodesk.Revit.DB.DirectShape",
 37314|       "target": null,
 37315|       "member_name": "UpdateExternallyTaggedGeometry",
 37316|       "member_kind": "method",
 37317|       "edge_type": "TAGS_ELEMENT",
 37318|       "confidence": "name_only_candidate",
 37319|       "confidence_tier": "likely",
 37320|       "target_resolution": "none",
 37321|       "evidence": [
 37322|         "member name 'UpdateExternallyTaggedGeometry' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'void' gives no type-level confirmation"
 37323|       ],
 37324|       "source_url": "https://www.revitapidocs.com/2025/c15c6964-0514-7c53-fdf0-9900ec68636f.htm",
 37325|       "dll_signature_verified": true,
 37326|       "dll_relationship_scope": "declared",
 37327|       "dll_semantic_verified": null,
 37328|       "dll_verified_status": "signature_verified_declared",
 37329|       "revitlookup_referenced": null,
 37330|       "revitlookup_requires_document_context": null
 37331|     },
 37332|     {
 37333|       "source": "Autodesk.Revit.DB.DirectShapeLibrary",
 37334|       "target": null,
 37335|       "member_name": "FindDefinitionType",
 37336|       "member_kind": "method",
 37337|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 37338|       "confidence": "unknown_reference",
 37339|       "confidence_tier": "unverified_reference",
 37340|       "target_resolution": "none",
 37341|       "evidence": [
 37342|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 37343|       ],
 37344|       "source_url": "https://www.revitapidocs.com/2025/c1a53b64-8ceb-e144-3f68-561c6f62a165.htm",
 37345|       "dll_signature_verified": true,
 37346|       "dll_relationship_scope": "declared",
 37347|       "dll_semantic_verified": null,
 37348|       "dll_verified_status": "signature_verified_declared",
 37349|       "revitlookup_referenced": null,
 37350|       "revitlookup_requires_document_context": null
 37351|     },
 37352|     {
 37353|       "source": "Autodesk.Revit.DB.DirectShapeOptions",
 37354|       "target": "Autodesk.Revit.DB.Architecture.Room",
 37355|       "member_name": "RoomBoundingOption",
 37356|       "member_kind": "property",
 37357|       "edge_type": "REFERENCES",
 37358|       "confidence": "name_only_candidate",
 37359|       "confidence_tier": "likely",
 37360|       "target_resolution": "exact",
 37361|       "evidence": [
 37362|         "member name 'RoomBoundingOption' matches keyword pattern /Room/ but return type 'DirectShapeRoomBoundingOption' gives no type-level confirmation"
 37363|       ],
 37364|       "source_url": "https://www.revitapidocs.com/2025/043a8376-b220-33b1-2707-c3eac7ccd2e3.htm",
 37365|       "dll_signature_verified": true,
 37366|       "dll_relationship_scope": "declared",
 37367|       "dll_semantic_verified": null,
 37368|       "dll_verified_status": "signature_verified_declared",
 37369|       "revitlookup_referenced": null,
 37370|       "revitlookup_requires_document_context": null
 37371|     },
 37372|     {
 37373|       "source": "Autodesk.Revit.DB.DirectShapeReferenceOptions",
 37374|       "target": "Autodesk.Revit.DB.ExternalGeometryId",
 37375|       "member_name": "GetExternalGeometryId",
 37376|       "member_kind": "method",
 37377|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37378|       "confidence": "direct_return_type",
 37379|       "confidence_tier": "unverified_reference",
 37380|       "target_resolution": "exact",
 37381|       "evidence": [
 37382|         "return type 'ExternalGeometryId' directly names a Revit DB object type"
 37383|       ],
 37384|       "source_url": "https://www.revitapidocs.com/2025/89e9092c-025e-4a51-d82d-bf2ff385a523.htm",
 37385|       "dll_signature_verified": true,
 37386|       "dll_relationship_scope": "declared",
 37387|       "dll_semantic_verified": null,
 37388|       "dll_verified_status": "signature_verified_declared",
 37389|       "revitlookup_referenced": null,
 37390|       "revitlookup_requires_document_context": null
 37391|     },
 37392|     {
 37393|       "source": "Autodesk.Revit.DB.DirectShapeType",
 37394|       "target": null,
 37395|       "member_name": "AddExternallyTaggedGeometry",
 37396|       "member_kind": "method",
 37397|       "edge_type": "TAGS_ELEMENT",
 37398|       "confidence": "name_only_candidate",
 37399|       "confidence_tier": "likely",
 37400|       "target_resolution": "none",
 37401|       "evidence": [
 37402|         "member name 'AddExternallyTaggedGeometry' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'void' gives no type-level confirmation"
 37403|       ],
 37404|       "source_url": "https://www.revitapidocs.com/2025/39c80387-f1ef-b57c-67d1-0231d0ec5068.htm",
 37405|       "dll_signature_verified": true,
 37406|       "dll_relationship_scope": "declared",
 37407|       "dll_semantic_verified": null,
 37408|       "dll_verified_status": "signature_verified_declared",
 37409|       "revitlookup_referenced": null,
 37410|       "revitlookup_requires_document_context": null
 37411|     },
 37412|     {
 37413|       "source": "Autodesk.Revit.DB.DirectShapeType",
 37414|       "target": "Autodesk.Revit.DB.ExternallyTaggedGeometryObject",
 37415|       "member_name": "GetExternallyTaggedGeometry",
 37416|       "member_kind": "method",
 37417|       "edge_type": "TAGS_ELEMENT",
 37418|       "confidence": "direct_return_type",
 37419|       "confidence_tier": "core",
 37420|       "target_resolution": "exact",
 37421|       "evidence": [
 37422|         "return type 'ExternallyTaggedGeometryObject' directly names a Revit DB object type"
 37423|       ],
 37424|       "source_url": "https://www.revitapidocs.com/2025/60d0ba59-5345-dbd0-e92a-0f2d71d709de.htm",
 37425|       "dll_signature_verified": true,
 37426|       "dll_relationship_scope": "declared",
 37427|       "dll_semantic_verified": null,
 37428|       "dll_verified_status": "signature_verified_declared",
 37429|       "revitlookup_referenced": null,
 37430|       "revitlookup_requires_document_context": null
 37431|     },
 37432|     {
 37433|       "source": "Autodesk.Revit.DB.DirectShapeType",
 37434|       "target": "Autodesk.Revit.DB.Reference",
 37435|       "member_name": "GetExternallyTaggedReference",
 37436|       "member_kind": "method",
 37437|       "edge_type": "TAGS_ELEMENT",
 37438|       "confidence": "direct_return_type",
 37439|       "confidence_tier": "core",
 37440|       "target_resolution": "exact",
 37441|       "evidence": [
 37442|         "return type 'Reference' directly names a Revit DB object type"
 37443|       ],
 37444|       "source_url": "https://www.revitapidocs.com/2025/612c51c5-c97d-19ce-2ced-209fd6e7a92a.htm",
 37445|       "dll_signature_verified": true,
 37446|       "dll_relationship_scope": "declared",
 37447|       "dll_semantic_verified": null,
 37448|       "dll_verified_status": "signature_verified_declared",
 37449|       "revitlookup_referenced": null,
 37450|       "revitlookup_requires_document_context": null
```
