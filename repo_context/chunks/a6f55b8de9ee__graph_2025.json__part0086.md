# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 86 of 216
- Original line range: 33151-33550
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 33151|     },
 33152|     {
 33153|       "source": "Autodesk.Revit.DB.AssemblyMemberDifferentType",
 33154|       "target": null,
 33155|       "member_name": "TypeId2",
 33156|       "member_kind": "property",
 33157|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 33158|       "confidence": "unknown_reference",
 33159|       "confidence_tier": "unverified_reference",
 33160|       "target_resolution": "none",
 33161|       "evidence": [
 33162|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 33163|       ],
 33164|       "source_url": "https://www.revitapidocs.com/2025/35312379-1947-3beb-f427-bd6ea921db4d.htm",
 33165|       "dll_signature_verified": true,
 33166|       "dll_relationship_scope": "declared",
 33167|       "dll_semantic_verified": null,
 33168|       "dll_verified_status": "signature_verified_declared",
 33169|       "revitlookup_referenced": null,
 33170|       "revitlookup_requires_document_context": null
 33171|     },
 33172|     {
 33173|       "source": "Autodesk.Revit.DB.AssemblyViewUtils",
 33174|       "target": null,
 33175|       "member_name": "AcquireAssemblyViews",
 33176|       "member_kind": "method",
 33177|       "edge_type": "MEMBER_OF_ASSEMBLY",
 33178|       "confidence": "name_only_candidate",
 33179|       "confidence_tier": "likely",
 33180|       "target_resolution": "none",
 33181|       "evidence": [
 33182|         "member name 'AcquireAssemblyViews' matches keyword pattern /Assembly/ but return type 'void' gives no type-level confirmation"
 33183|       ],
 33184|       "source_url": "https://www.revitapidocs.com/2025/9d899efa-112e-b169-fde8-303f0967593d.htm",
 33185|       "dll_signature_verified": true,
 33186|       "dll_relationship_scope": "declared",
 33187|       "dll_semantic_verified": null,
 33188|       "dll_verified_status": "signature_verified_declared",
 33189|       "revitlookup_referenced": null,
 33190|       "revitlookup_requires_document_context": null
 33191|     },
 33192|     {
 33193|       "source": "Autodesk.Revit.DB.AXMImportOptions",
 33194|       "target": "Autodesk.Revit.DB.Level",
 33195|       "member_name": "ImportLevels",
 33196|       "member_kind": "property",
 33197|       "edge_type": "ASSIGNED_TO_LEVEL",
 33198|       "confidence": "name_only_candidate",
 33199|       "confidence_tier": "likely",
 33200|       "target_resolution": "exact",
 33201|       "evidence": [
 33202|         "member name 'ImportLevels' matches keyword pattern /Level/ but return type 'bool' gives no type-level confirmation"
 33203|       ],
 33204|       "source_url": "https://www.revitapidocs.com/2025/c2a388c5-3ebe-695d-be45-8334f51a67f3.htm",
 33205|       "dll_signature_verified": true,
 33206|       "dll_relationship_scope": "declared",
 33207|       "dll_semantic_verified": null,
 33208|       "dll_verified_status": "signature_verified_declared",
 33209|       "revitlookup_referenced": null,
 33210|       "revitlookup_requires_document_context": null
 33211|     },
 33212|     {
 33213|       "source": "Autodesk.Revit.DB.BaseArray",
 33214|       "target": "Autodesk.Revit.DB.FamilyParameter",
 33215|       "member_name": "Label",
 33216|       "member_kind": "property",
 33217|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 33218|       "confidence": "direct_return_type",
 33219|       "confidence_tier": "unverified_reference",
 33220|       "target_resolution": "exact",
 33221|       "evidence": [
 33222|         "return type 'FamilyParameter' directly names a Revit DB object type"
 33223|       ],
 33224|       "source_url": "https://www.revitapidocs.com/2025/816d60e1-d4ea-cda1-4736-aa5f7d05594a.htm",
 33225|       "dll_signature_verified": true,
 33226|       "dll_relationship_scope": "declared",
 33227|       "dll_semantic_verified": null,
 33228|       "dll_verified_status": "signature_verified_declared",
 33229|       "revitlookup_referenced": null,
 33230|       "revitlookup_requires_document_context": null
 33231|     },
 33232|     {
 33233|       "source": "Autodesk.Revit.DB.BaseArray",
 33234|       "target": null,
 33235|       "member_name": "GetCopiedMemberIds",
 33236|       "member_kind": "method",
 33237|       "edge_type": "RETURNS_ELEMENT_IDS",
 33238|       "confidence": "unknown_reference",
 33239|       "confidence_tier": "unverified_reference",
 33240|       "target_resolution": "none",
 33241|       "evidence": [
 33242|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 33243|       ],
 33244|       "source_url": "https://www.revitapidocs.com/2025/ce0d8197-a99f-0ef2-32a8-51d4cc2aa101.htm",
 33245|       "dll_signature_verified": true,
 33246|       "dll_relationship_scope": "declared",
 33247|       "dll_semantic_verified": null,
 33248|       "dll_verified_status": "signature_verified_declared",
 33249|       "revitlookup_referenced": null,
 33250|       "revitlookup_requires_document_context": null
 33251|     },
 33252|     {
 33253|       "source": "Autodesk.Revit.DB.BaseArray",
 33254|       "target": null,
 33255|       "member_name": "GetOriginalMemberIds",
 33256|       "member_kind": "method",
 33257|       "edge_type": "RETURNS_ELEMENT_IDS",
 33258|       "confidence": "unknown_reference",
 33259|       "confidence_tier": "unverified_reference",
 33260|       "target_resolution": "none",
 33261|       "evidence": [
 33262|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 33263|       ],
 33264|       "source_url": "https://www.revitapidocs.com/2025/8c15fb58-a534-abd4-0715-841ea1c1447f.htm",
 33265|       "dll_signature_verified": true,
 33266|       "dll_relationship_scope": "declared",
 33267|       "dll_semantic_verified": null,
 33268|       "dll_verified_status": "signature_verified_declared",
 33269|       "revitlookup_referenced": null,
 33270|       "revitlookup_requires_document_context": null
 33271|     },
 33272|     {
 33273|       "source": "Autodesk.Revit.DB.BaseExportOptions",
 33274|       "target": null,
 33275|       "member_name": "HideUnreferenceViewTags",
 33276|       "member_kind": "property",
 33277|       "edge_type": "TAGS_ELEMENT",
 33278|       "confidence": "name_only_candidate",
 33279|       "confidence_tier": "likely",
 33280|       "target_resolution": "none",
 33281|       "evidence": [
 33282|         "member name 'HideUnreferenceViewTags' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'bool' gives no type-level confirmation"
 33283|       ],
 33284|       "source_url": "https://www.revitapidocs.com/2025/5507e467-964c-43fd-374e-50d341a25ee2.htm",
 33285|       "dll_signature_verified": true,
 33286|       "dll_relationship_scope": "declared",
 33287|       "dll_semantic_verified": null,
 33288|       "dll_verified_status": "signature_verified_declared",
 33289|       "revitlookup_referenced": null,
 33290|       "revitlookup_requires_document_context": null
 33291|     },
 33292|     {
 33293|       "source": "Autodesk.Revit.DB.BaseExportOptions",
 33294|       "target": "Autodesk.Revit.DB.ExportFontTable",
 33295|       "member_name": "GetExportFontTable",
 33296|       "member_kind": "method",
 33297|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 33298|       "confidence": "direct_return_type",
 33299|       "confidence_tier": "unverified_reference",
 33300|       "target_resolution": "exact",
 33301|       "evidence": [
 33302|         "return type 'ExportFontTable' directly names a Revit DB object type"
 33303|       ],
 33304|       "source_url": "https://www.revitapidocs.com/2025/6dc659b4-4131-c1bf-e418-4afc551095d0.htm",
 33305|       "dll_signature_verified": true,
 33306|       "dll_relationship_scope": "declared",
 33307|       "dll_semantic_verified": null,
 33308|       "dll_verified_status": "signature_verified_declared",
 33309|       "revitlookup_referenced": null,
 33310|       "revitlookup_requires_document_context": null
 33311|     },
 33312|     {
 33313|       "source": "Autodesk.Revit.DB.BaseExportOptions",
 33314|       "target": "Autodesk.Revit.DB.ExportLayerTable",
 33315|       "member_name": "GetExportLayerTable",
 33316|       "member_kind": "method",
 33317|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 33318|       "confidence": "direct_return_type",
 33319|       "confidence_tier": "unverified_reference",
 33320|       "target_resolution": "exact",
 33321|       "evidence": [
 33322|         "return type 'ExportLayerTable' directly names a Revit DB object type"
 33323|       ],
 33324|       "source_url": "https://www.revitapidocs.com/2025/1ce6b604-0b45-f05f-863e-952b85e5a862.htm",
 33325|       "dll_signature_verified": true,
 33326|       "dll_relationship_scope": "declared",
 33327|       "dll_semantic_verified": null,
 33328|       "dll_verified_status": "signature_verified_declared",
 33329|       "revitlookup_referenced": null,
 33330|       "revitlookup_requires_document_context": null
 33331|     },
 33332|     {
 33333|       "source": "Autodesk.Revit.DB.BaseExportOptions",
 33334|       "target": "Autodesk.Revit.DB.ExportLinetypeTable",
 33335|       "member_name": "GetExportLinetypeTable",
 33336|       "member_kind": "method",
 33337|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 33338|       "confidence": "direct_return_type",
 33339|       "confidence_tier": "unverified_reference",
 33340|       "target_resolution": "exact",
 33341|       "evidence": [
 33342|         "return type 'ExportLinetypeTable' directly names a Revit DB object type"
 33343|       ],
 33344|       "source_url": "https://www.revitapidocs.com/2025/eba17284-95da-cea8-6b24-4e99bf196629.htm",
 33345|       "dll_signature_verified": true,
 33346|       "dll_relationship_scope": "declared",
 33347|       "dll_semantic_verified": null,
 33348|       "dll_verified_status": "signature_verified_declared",
 33349|       "revitlookup_referenced": null,
 33350|       "revitlookup_requires_document_context": null
 33351|     },
 33352|     {
 33353|       "source": "Autodesk.Revit.DB.BaseExportOptions",
 33354|       "target": "Autodesk.Revit.DB.ExportPatternTable",
 33355|       "member_name": "GetExportPatternTable",
 33356|       "member_kind": "method",
 33357|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 33358|       "confidence": "direct_return_type",
 33359|       "confidence_tier": "unverified_reference",
 33360|       "target_resolution": "exact",
 33361|       "evidence": [
 33362|         "return type 'ExportPatternTable' directly names a Revit DB object type"
 33363|       ],
 33364|       "source_url": "https://www.revitapidocs.com/2025/6f852987-50c6-e44a-398a-b23a01a1a0a5.htm",
 33365|       "dll_signature_verified": true,
 33366|       "dll_relationship_scope": "declared",
 33367|       "dll_semantic_verified": null,
 33368|       "dll_verified_status": "signature_verified_declared",
 33369|       "revitlookup_referenced": null,
 33370|       "revitlookup_requires_document_context": null
 33371|     },
 33372|     {
 33373|       "source": "Autodesk.Revit.DB.BasicFileInfo",
 33374|       "target": "Autodesk.Revit.DB.DocumentVersion",
 33375|       "member_name": "GetDocumentVersion",
 33376|       "member_kind": "method",
 33377|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 33378|       "confidence": "direct_return_type",
 33379|       "confidence_tier": "unverified_reference",
 33380|       "target_resolution": "exact",
 33381|       "evidence": [
 33382|         "return type 'DocumentVersion' directly names a Revit DB object type"
 33383|       ],
 33384|       "source_url": "https://www.revitapidocs.com/2025/117de15c-6642-4216-cd85-8c31eb42cbca.htm",
 33385|       "dll_signature_verified": true,
 33386|       "dll_relationship_scope": "declared",
 33387|       "dll_semantic_verified": null,
 33388|       "dll_verified_status": "signature_verified_declared",
 33389|       "revitlookup_referenced": null,
 33390|       "revitlookup_requires_document_context": null
 33391|     },
 33392|     {
 33393|       "source": "Autodesk.Revit.DB.BeamSystem",
 33394|       "target": "Autodesk.Revit.DB.BeamSystemType",
 33395|       "member_name": "BeamSystemType",
 33396|       "member_kind": "property",
 33397|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 33398|       "confidence": "direct_return_type",
 33399|       "confidence_tier": "unverified_reference",
 33400|       "target_resolution": "exact",
 33401|       "evidence": [
 33402|         "return type 'BeamSystemType' directly names a Revit DB object type"
 33403|       ],
 33404|       "source_url": "https://www.revitapidocs.com/2025/f738ec01-4d36-884c-cc40-3a78c8b1b2bd.htm",
 33405|       "dll_signature_verified": true,
 33406|       "dll_relationship_scope": "declared",
 33407|       "dll_semantic_verified": null,
 33408|       "dll_verified_status": "signature_verified_declared",
 33409|       "revitlookup_referenced": null,
 33410|       "revitlookup_requires_document_context": null
 33411|     },
 33412|     {
 33413|       "source": "Autodesk.Revit.DB.BeamSystem",
 33414|       "target": "Autodesk.Revit.DB.FamilySymbol",
 33415|       "member_name": "BeamType",
 33416|       "member_kind": "property",
 33417|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 33418|       "confidence": "direct_return_type",
 33419|       "confidence_tier": "unverified_reference",
 33420|       "target_resolution": "exact",
 33421|       "evidence": [
 33422|         "return type 'FamilySymbol' directly names a Revit DB object type"
 33423|       ],
 33424|       "source_url": "https://www.revitapidocs.com/2025/c1f1e42c-b564-b968-6632-a014104b4877.htm",
 33425|       "dll_signature_verified": true,
 33426|       "dll_relationship_scope": "declared",
 33427|       "dll_semantic_verified": null,
 33428|       "dll_verified_status": "signature_verified_declared",
 33429|       "revitlookup_referenced": null,
 33430|       "revitlookup_requires_document_context": null
 33431|     },
 33432|     {
 33433|       "source": "Autodesk.Revit.DB.BeamSystem",
 33434|       "target": "Autodesk.Revit.DB.LayoutRule",
 33435|       "member_name": "LayoutRule",
 33436|       "member_kind": "property",
 33437|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 33438|       "confidence": "direct_return_type",
 33439|       "confidence_tier": "unverified_reference",
 33440|       "target_resolution": "exact",
 33441|       "evidence": [
 33442|         "return type 'LayoutRule' directly names a Revit DB object type"
 33443|       ],
 33444|       "source_url": "https://www.revitapidocs.com/2025/2db288d4-8e26-b710-6371-72c8054a3d8b.htm",
 33445|       "dll_signature_verified": true,
 33446|       "dll_relationship_scope": "declared",
 33447|       "dll_semantic_verified": null,
 33448|       "dll_verified_status": "signature_verified_declared",
 33449|       "revitlookup_referenced": null,
 33450|       "revitlookup_requires_document_context": null
 33451|     },
 33452|     {
 33453|       "source": "Autodesk.Revit.DB.BeamSystem",
 33454|       "target": "Autodesk.Revit.DB.Level",
 33455|       "member_name": "Level",
 33456|       "member_kind": "property",
 33457|       "edge_type": "ASSIGNED_TO_LEVEL",
 33458|       "confidence": "direct_return_type",
 33459|       "confidence_tier": "core",
 33460|       "target_resolution": "exact",
 33461|       "evidence": [
 33462|         "return type 'Level' directly names a Revit DB object type"
 33463|       ],
 33464|       "source_url": "https://www.revitapidocs.com/2025/ee3933b3-51ff-1476-cfe0-76042df715b0.htm",
 33465|       "dll_signature_verified": true,
 33466|       "dll_relationship_scope": "declared",
 33467|       "dll_semantic_verified": null,
 33468|       "dll_verified_status": "signature_verified_declared",
 33469|       "revitlookup_referenced": null,
 33470|       "revitlookup_requires_document_context": null
 33471|     },
 33472|     {
 33473|       "source": "Autodesk.Revit.DB.BeamSystem",
 33474|       "target": null,
 33475|       "member_name": "GetBeamIds",
 33476|       "member_kind": "method",
 33477|       "edge_type": "RETURNS_ELEMENT_IDS",
 33478|       "confidence": "unknown_reference",
 33479|       "confidence_tier": "unverified_reference",
 33480|       "target_resolution": "none",
 33481|       "evidence": [
 33482|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 33483|       ],
 33484|       "source_url": "https://www.revitapidocs.com/2025/0f489cca-4830-9016-fe8d-d17ba4b9f4d0.htm",
 33485|       "dll_signature_verified": true,
 33486|       "dll_relationship_scope": "declared",
 33487|       "dll_semantic_verified": null,
 33488|       "dll_verified_status": "signature_verified_declared",
 33489|       "revitlookup_referenced": null,
 33490|       "revitlookup_requires_document_context": null
 33491|     },
 33492|     {
 33493|       "source": "Autodesk.Revit.DB.BIMExportOptions",
 33494|       "target": "Autodesk.Revit.DB.View",
 33495|       "member_name": "ViewId",
 33496|       "member_kind": "property",
 33497|       "edge_type": "REFERENCES",
 33498|       "confidence": "elementid_with_strong_name",
 33499|       "confidence_tier": "core",
 33500|       "target_resolution": "exact",
 33501|       "evidence": [
 33502|         "member name 'ViewId' matches keyword pattern /^(Get)?ViewId$/"
 33503|       ],
 33504|       "source_url": "https://www.revitapidocs.com/2025/a5864496-b5d0-a1be-ca12-125b7ce9b40d.htm",
 33505|       "dll_signature_verified": true,
 33506|       "dll_relationship_scope": "declared",
 33507|       "dll_semantic_verified": null,
 33508|       "dll_verified_status": "signature_verified_declared",
 33509|       "revitlookup_referenced": null,
 33510|       "revitlookup_requires_document_context": null
 33511|     },
 33512|     {
 33513|       "source": "Autodesk.Revit.DB.Blend",
 33514|       "target": "Autodesk.Revit.DB.Sketch",
 33515|       "member_name": "BottomSketch",
 33516|       "member_kind": "property",
 33517|       "edge_type": "DEPENDS_ON",
 33518|       "confidence": "direct_return_type",
 33519|       "confidence_tier": "core",
 33520|       "target_resolution": "exact",
 33521|       "evidence": [
 33522|         "return type 'Sketch' directly names a Revit DB object type"
 33523|       ],
 33524|       "source_url": "https://www.revitapidocs.com/2025/c518f0a1-2368-3759-73fb-ca1c8cbd8810.htm",
 33525|       "dll_signature_verified": true,
 33526|       "dll_relationship_scope": "declared",
 33527|       "dll_semantic_verified": null,
 33528|       "dll_verified_status": "signature_verified_declared",
 33529|       "revitlookup_referenced": null,
 33530|       "revitlookup_requires_document_context": null
 33531|     },
 33532|     {
 33533|       "source": "Autodesk.Revit.DB.Blend",
 33534|       "target": "Autodesk.Revit.DB.Sketch",
 33535|       "member_name": "TopSketch",
 33536|       "member_kind": "property",
 33537|       "edge_type": "DEPENDS_ON",
 33538|       "confidence": "direct_return_type",
 33539|       "confidence_tier": "core",
 33540|       "target_resolution": "exact",
 33541|       "evidence": [
 33542|         "return type 'Sketch' directly names a Revit DB object type"
 33543|       ],
 33544|       "source_url": "https://www.revitapidocs.com/2025/4b60c6f8-55e2-376f-858d-d0309bd934e2.htm",
 33545|       "dll_signature_verified": true,
 33546|       "dll_relationship_scope": "declared",
 33547|       "dll_semantic_verified": null,
 33548|       "dll_verified_status": "signature_verified_declared",
 33549|       "revitlookup_referenced": null,
 33550|       "revitlookup_requires_document_context": null
```
