# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 212 of 216
- Original line range: 82291-82690
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 82291|       "revitlookup_requires_document_context": null
 82292|     },
 82293|     {
 82294|       "source": "Autodesk.Revit.DB.Structure.RebarSplice",
 82295|       "target": null,
 82296|       "member_name": "ConnectedRebarId",
 82297|       "member_kind": "property",
 82298|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 82299|       "confidence": "unknown_reference",
 82300|       "confidence_tier": "unverified_reference",
 82301|       "target_resolution": "none",
 82302|       "evidence": [
 82303|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 82304|       ],
 82305|       "source_url": "https://www.revitapidocs.com/2025/1738c1ba-5985-bc39-334f-a5c20325d039.htm",
 82306|       "dll_signature_verified": true,
 82307|       "dll_relationship_scope": "declared",
 82308|       "dll_semantic_verified": null,
 82309|       "dll_verified_status": "signature_verified_declared",
 82310|       "revitlookup_referenced": null,
 82311|       "revitlookup_requires_document_context": null
 82312|     },
 82313|     {
 82314|       "source": "Autodesk.Revit.DB.Structure.RebarSplice",
 82315|       "target": null,
 82316|       "member_name": "SourceRebarId",
 82317|       "member_kind": "property",
 82318|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 82319|       "confidence": "unknown_reference",
 82320|       "confidence_tier": "unverified_reference",
 82321|       "target_resolution": "none",
 82322|       "evidence": [
 82323|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 82324|       ],
 82325|       "source_url": "https://www.revitapidocs.com/2025/5000118e-e6be-b55a-f9fd-47a59fa3516a.htm",
 82326|       "dll_signature_verified": true,
 82327|       "dll_relationship_scope": "declared",
 82328|       "dll_semantic_verified": null,
 82329|       "dll_verified_status": "signature_verified_declared",
 82330|       "revitlookup_referenced": null,
 82331|       "revitlookup_requires_document_context": null
 82332|     },
 82333|     {
 82334|       "source": "Autodesk.Revit.DB.Structure.RebarSplice",
 82335|       "target": null,
 82336|       "member_name": "SpliceTypeId",
 82337|       "member_kind": "property",
 82338|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 82339|       "confidence": "unknown_reference",
 82340|       "confidence_tier": "unverified_reference",
 82341|       "target_resolution": "none",
 82342|       "evidence": [
 82343|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 82344|       ],
 82345|       "source_url": "https://www.revitapidocs.com/2025/3caf48ab-c999-8d6c-bfbf-f49967bbb415.htm",
 82346|       "dll_signature_verified": true,
 82347|       "dll_relationship_scope": "declared",
 82348|       "dll_semantic_verified": null,
 82349|       "dll_verified_status": "signature_verified_declared",
 82350|       "revitlookup_referenced": null,
 82351|       "revitlookup_requires_document_context": null
 82352|     },
 82353|     {
 82354|       "source": "Autodesk.Revit.DB.Structure.RebarSplice",
 82355|       "target": "Autodesk.Revit.DB.Structure.RebarSpliceGeometry",
 82356|       "member_name": "GetRebarSpliceGeometry",
 82357|       "member_kind": "method",
 82358|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 82359|       "confidence": "direct_return_type",
 82360|       "confidence_tier": "unverified_reference",
 82361|       "target_resolution": "short_name_fallback",
 82362|       "evidence": [
 82363|         "return type 'RebarSpliceGeometry' directly names a Revit DB object type"
 82364|       ],
 82365|       "source_url": "https://www.revitapidocs.com/2025/3be46d2f-942d-92fc-511f-e029aeaa5c52.htm",
 82366|       "dll_signature_verified": true,
 82367|       "dll_relationship_scope": "declared",
 82368|       "dll_semantic_verified": null,
 82369|       "dll_verified_status": "signature_verified_declared",
 82370|       "revitlookup_referenced": null,
 82371|       "revitlookup_requires_document_context": null
 82372|     },
 82373|     {
 82374|       "source": "Autodesk.Revit.DB.Structure.RebarSpliceByRulesResult",
 82375|       "target": "Autodesk.Revit.DB.Structure.RebarSpliceGeometry",
 82376|       "member_name": "GetSpliceGeometries",
 82377|       "member_kind": "method",
 82378|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 82379|       "confidence": "needs_runtime_validation",
 82380|       "confidence_tier": "needs_validation",
 82381|       "target_resolution": "short_name_fallback",
 82382|       "evidence": [
 82383|         "return type 'IList < RebarSpliceGeometry >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 82384|       ],
 82385|       "source_url": "https://www.revitapidocs.com/2025/5f3ed50a-ceb3-401f-7266-9c4338dcdc4f.htm",
 82386|       "dll_signature_verified": true,
 82387|       "dll_relationship_scope": "declared",
 82388|       "dll_semantic_verified": null,
 82389|       "dll_verified_status": "signature_verified_declared",
 82390|       "revitlookup_referenced": null,
 82391|       "revitlookup_requires_document_context": null
 82392|     },
 82393|     {
 82394|       "source": "Autodesk.Revit.DB.Structure.RebarSpliceOptions",
 82395|       "target": null,
 82396|       "member_name": "SpliceTypeId",
 82397|       "member_kind": "property",
 82398|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 82399|       "confidence": "unknown_reference",
 82400|       "confidence_tier": "unverified_reference",
 82401|       "target_resolution": "none",
 82402|       "evidence": [
 82403|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 82404|       ],
 82405|       "source_url": "https://www.revitapidocs.com/2025/c0b84daa-383b-b767-2031-5d9b6e6c1481.htm",
 82406|       "dll_signature_verified": true,
 82407|       "dll_relationship_scope": "declared",
 82408|       "dll_semantic_verified": null,
 82409|       "dll_verified_status": "signature_verified_declared",
 82410|       "revitlookup_referenced": null,
 82411|       "revitlookup_requires_document_context": null
 82412|     },
 82413|     {
 82414|       "source": "Autodesk.Revit.DB.Structure.RebarSpliceTypeUtils",
 82415|       "target": null,
 82416|       "member_name": "GetAllRebarSpliceTypes",
 82417|       "member_kind": "method",
 82418|       "edge_type": "RETURNS_ELEMENT_IDS",
 82419|       "confidence": "elementid_collection_with_strong_name",
 82420|       "confidence_tier": "core",
 82421|       "target_resolution": "none",
 82422|       "evidence": [
 82423|         "member name 'GetAllRebarSpliceTypes' matches keyword pattern /^GetAll/"
 82424|       ],
 82425|       "source_url": "https://www.revitapidocs.com/2025/698b54e8-4542-5670-64b5-68fd25d8a7e8.htm",
 82426|       "dll_signature_verified": true,
 82427|       "dll_relationship_scope": "declared",
 82428|       "dll_semantic_verified": null,
 82429|       "dll_verified_status": "signature_verified_declared",
 82430|       "revitlookup_referenced": null,
 82431|       "revitlookup_requires_document_context": null
 82432|     },
 82433|     {
 82434|       "source": "Autodesk.Revit.DB.Structure.RebarSpliceTypeUtils",
 82435|       "target": null,
 82436|       "member_name": "GetStaggerLengthMultiplier",
 82437|       "member_kind": "method",
 82438|       "edge_type": "TAGS_ELEMENT",
 82439|       "confidence": "name_only_candidate",
 82440|       "confidence_tier": "likely",
 82441|       "target_resolution": "none",
 82442|       "evidence": [
 82443|         "member name 'GetStaggerLengthMultiplier' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'double' gives no type-level confirmation"
 82444|       ],
 82445|       "source_url": "https://www.revitapidocs.com/2025/6f65eba1-57dd-e1a0-5bce-f8a40d84fadc.htm",
 82446|       "dll_signature_verified": true,
 82447|       "dll_relationship_scope": "declared",
 82448|       "dll_semantic_verified": null,
 82449|       "dll_verified_status": "signature_verified_declared",
 82450|       "revitlookup_referenced": null,
 82451|       "revitlookup_requires_document_context": null
 82452|     },
 82453|     {
 82454|       "source": "Autodesk.Revit.DB.Structure.RebarSpliceTypeUtils",
 82455|       "target": null,
 82456|       "member_name": "SetStaggerLengthMultiplier",
 82457|       "member_kind": "method",
 82458|       "edge_type": "TAGS_ELEMENT",
 82459|       "confidence": "name_only_candidate",
 82460|       "confidence_tier": "likely",
 82461|       "target_resolution": "none",
 82462|       "evidence": [
 82463|         "member name 'SetStaggerLengthMultiplier' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'void' gives no type-level confirmation"
 82464|       ],
 82465|       "source_url": "https://www.revitapidocs.com/2025/4aa9bb16-61c9-5a53-074d-a78cb2e2ccc7.htm",
 82466|       "dll_signature_verified": true,
 82467|       "dll_relationship_scope": "declared",
 82468|       "dll_semantic_verified": null,
 82469|       "dll_verified_status": "signature_verified_declared",
 82470|       "revitlookup_referenced": null,
 82471|       "revitlookup_requires_document_context": null
 82472|     },
 82473|     {
 82474|       "source": "Autodesk.Revit.DB.Structure.RebarSpliceUtils",
 82475|       "target": null,
 82476|       "member_name": "GetSpliceChain",
 82477|       "member_kind": "method",
 82478|       "edge_type": "RETURNS_ELEMENT_IDS",
 82479|       "confidence": "unknown_reference",
 82480|       "confidence_tier": "unverified_reference",
 82481|       "target_resolution": "none",
 82482|       "evidence": [
 82483|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 82484|       ],
 82485|       "source_url": "https://www.revitapidocs.com/2025/b3d1b9a4-bc28-e74e-b264-dc99cf9520ed.htm",
 82486|       "dll_signature_verified": true,
 82487|       "dll_relationship_scope": "declared",
 82488|       "dll_semantic_verified": null,
 82489|       "dll_verified_status": "signature_verified_declared",
 82490|       "revitlookup_referenced": null,
 82491|       "revitlookup_requires_document_context": null
 82492|     },
 82493|     {
 82494|       "source": "Autodesk.Revit.DB.Structure.RebarSpliceUtils",
 82495|       "target": "Autodesk.Revit.DB.Structure.RebarSpliceByRulesResult",
 82496|       "member_name": "GetSpliceGeometries",
 82497|       "member_kind": "method",
 82498|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 82499|       "confidence": "direct_return_type",
 82500|       "confidence_tier": "unverified_reference",
 82501|       "target_resolution": "short_name_fallback",
 82502|       "evidence": [
 82503|         "return type 'RebarSpliceByRulesResult' directly names a Revit DB object type"
 82504|       ],
 82505|       "source_url": "https://www.revitapidocs.com/2025/97b6224d-9eda-0bc7-c7fd-1b4199ff4ad8.htm",
 82506|       "dll_signature_verified": true,
 82507|       "dll_relationship_scope": "declared",
 82508|       "dll_semantic_verified": null,
 82509|       "dll_verified_status": "signature_verified_declared",
 82510|       "revitlookup_referenced": null,
 82511|       "revitlookup_requires_document_context": null
 82512|     },
 82513|     {
 82514|       "source": "Autodesk.Revit.DB.Structure.RebarSpliceUtils",
 82515|       "target": null,
 82516|       "member_name": "SpliceRebar(Document, ElementId, RebarSpliceOptions, IList<RebarSpliceGeometry>)",
 82517|       "member_kind": "method",
 82518|       "edge_type": "RETURNS_ELEMENT_IDS",
 82519|       "confidence": "unknown_reference",
 82520|       "confidence_tier": "unverified_reference",
 82521|       "target_resolution": "none",
 82522|       "evidence": [
 82523|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 82524|       ],
 82525|       "source_url": "https://www.revitapidocs.com/2025/8912b36d-6708-af09-bbdd-41c130992646.htm",
 82526|       "dll_signature_verified": false,
 82527|       "dll_relationship_scope": null,
 82528|       "dll_semantic_verified": null,
 82529|       "dll_verified_status": "member_not_found",
 82530|       "revitlookup_referenced": null,
 82531|       "revitlookup_requires_document_context": null
 82532|     },
 82533|     {
 82534|       "source": "Autodesk.Revit.DB.Structure.RebarSpliceUtils",
 82535|       "target": null,
 82536|       "member_name": "SpliceRebar(Document, ElementId, RebarSpliceOptions, Line, ElementId)",
 82537|       "member_kind": "method",
 82538|       "edge_type": "RETURNS_ELEMENT_IDS",
 82539|       "confidence": "unknown_reference",
 82540|       "confidence_tier": "unverified_reference",
 82541|       "target_resolution": "none",
 82542|       "evidence": [
 82543|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 82544|       ],
 82545|       "source_url": "https://www.revitapidocs.com/2025/bcd7293c-e9ad-83b8-b1bc-1c8e6e6d513a.htm",
 82546|       "dll_signature_verified": false,
 82547|       "dll_relationship_scope": null,
 82548|       "dll_semantic_verified": null,
 82549|       "dll_verified_status": "member_not_found",
 82550|       "revitlookup_referenced": null,
 82551|       "revitlookup_requires_document_context": null
 82552|     },
 82553|     {
 82554|       "source": "Autodesk.Revit.DB.Structure.RebarSpliceUtils",
 82555|       "target": null,
 82556|       "member_name": "SpliceRebar(Document, ElementId, RebarSpliceOptions, Line, XYZ)",
 82557|       "member_kind": "method",
 82558|       "edge_type": "RETURNS_ELEMENT_IDS",
 82559|       "confidence": "unknown_reference",
 82560|       "confidence_tier": "unverified_reference",
 82561|       "target_resolution": "none",
 82562|       "evidence": [
 82563|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 82564|       ],
 82565|       "source_url": "https://www.revitapidocs.com/2025/26f74acb-ff82-0349-6ec3-cf766184fb0b.htm",
 82566|       "dll_signature_verified": false,
 82567|       "dll_relationship_scope": null,
 82568|       "dll_semantic_verified": null,
 82569|       "dll_verified_status": "member_not_found",
 82570|       "revitlookup_referenced": null,
 82571|       "revitlookup_requires_document_context": null
 82572|     },
 82573|     {
 82574|       "source": "Autodesk.Revit.DB.Structure.RebarSpliceUtils",
 82575|       "target": null,
 82576|       "member_name": "UnifyRebarsIntoOne",
 82577|       "member_kind": "method",
 82578|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 82579|       "confidence": "unknown_reference",
 82580|       "confidence_tier": "unverified_reference",
 82581|       "target_resolution": "none",
 82582|       "evidence": [
 82583|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 82584|       ],
 82585|       "source_url": "https://www.revitapidocs.com/2025/23428557-b7d0-f3a7-1b89-4f819e2e0ac1.htm",
 82586|       "dll_signature_verified": true,
 82587|       "dll_relationship_scope": "declared",
 82588|       "dll_semantic_verified": null,
 82589|       "dll_verified_status": "signature_verified_declared",
 82590|       "revitlookup_referenced": null,
 82591|       "revitlookup_requires_document_context": null
 82592|     },
 82593|     {
 82594|       "source": "Autodesk.Revit.DB.Structure.RebarTrimExtendData",
 82595|       "target": "Autodesk.Revit.DB.Structure.RebarUpdateCurvesData",
 82596|       "member_name": "GetRebarUpdateCurvesData",
 82597|       "member_kind": "method",
 82598|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 82599|       "confidence": "direct_return_type",
 82600|       "confidence_tier": "unverified_reference",
 82601|       "target_resolution": "short_name_fallback",
 82602|       "evidence": [
 82603|         "return type 'RebarUpdateCurvesData' directly names a Revit DB object type"
 82604|       ],
 82605|       "source_url": "https://www.revitapidocs.com/2025/64b5059a-5cfe-bf9a-b13c-dea51ff40449.htm",
 82606|       "dll_signature_verified": true,
 82607|       "dll_relationship_scope": "declared",
 82608|       "dll_semantic_verified": null,
 82609|       "dll_verified_status": "signature_verified_declared",
 82610|       "revitlookup_referenced": null,
 82611|       "revitlookup_requires_document_context": null
 82612|     },
 82613|     {
 82614|       "source": "Autodesk.Revit.DB.Structure.RebarUpdateCurvesData",
 82615|       "target": null,
 82616|       "member_name": "HostMirrored",
 82617|       "member_kind": "property",
 82618|       "edge_type": "HOSTED_BY",
 82619|       "confidence": "name_only_candidate",
 82620|       "confidence_tier": "likely",
 82621|       "target_resolution": "none",
 82622|       "evidence": [
 82623|         "member name 'HostMirrored' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 82624|       ],
 82625|       "source_url": "https://www.revitapidocs.com/2025/7e5256ff-5025-9091-5056-c2261eadbc71.htm",
 82626|       "dll_signature_verified": true,
 82627|       "dll_relationship_scope": "declared",
 82628|       "dll_semantic_verified": null,
 82629|       "dll_verified_status": "signature_verified_declared",
 82630|       "revitlookup_referenced": null,
 82631|       "revitlookup_requires_document_context": null
 82632|     },
 82633|     {
 82634|       "source": "Autodesk.Revit.DB.Structure.RebarUpdateCurvesData",
 82635|       "target": "Autodesk.Revit.DB.Guid",
 82636|       "member_name": "GetChangedSharedParameterGUIDs",
 82637|       "member_kind": "method",
 82638|       "edge_type": "HAS_PARAMETER",
 82639|       "confidence": "needs_runtime_validation",
 82640|       "confidence_tier": "needs_validation",
 82641|       "target_resolution": "external",
 82642|       "evidence": [
 82643|         "return type 'IList < Guid >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 82644|       ],
 82645|       "source_url": "https://www.revitapidocs.com/2025/52f33c35-c8b3-0fce-7f05-5a6280e44a93.htm",
 82646|       "dll_signature_verified": true,
 82647|       "dll_relationship_scope": "declared",
 82648|       "dll_semantic_verified": null,
 82649|       "dll_verified_status": "signature_verified_declared",
 82650|       "revitlookup_referenced": null,
 82651|       "revitlookup_requires_document_context": null
 82652|     },
 82653|     {
 82654|       "source": "Autodesk.Revit.DB.Structure.RebarUpdateCurvesData",
 82655|       "target": "Autodesk.Revit.DB.Structure.RebarConstraint",
 82656|       "member_name": "GetCustomConstraints",
 82657|       "member_kind": "method",
 82658|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 82659|       "confidence": "needs_runtime_validation",
 82660|       "confidence_tier": "needs_validation",
 82661|       "target_resolution": "short_name_fallback",
 82662|       "evidence": [
 82663|         "return type 'IList < RebarConstraint >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 82664|       ],
 82665|       "source_url": "https://www.revitapidocs.com/2025/d753c390-8d4c-5193-eef8-ec7b9e7bd875.htm",
 82666|       "dll_signature_verified": true,
 82667|       "dll_relationship_scope": "declared",
 82668|       "dll_semantic_verified": null,
 82669|       "dll_verified_status": "signature_verified_declared",
 82670|       "revitlookup_referenced": null,
 82671|       "revitlookup_requires_document_context": null
 82672|     },
 82673|     {
 82674|       "source": "Autodesk.Revit.DB.Structure.RebarUpdateCurvesData",
 82675|       "target": "Autodesk.Revit.DB.Document",
 82676|       "member_name": "GetDocument",
 82677|       "member_kind": "method",
 82678|       "edge_type": "REFERENCES",
 82679|       "confidence": "direct_return_type",
 82680|       "confidence_tier": "core",
 82681|       "target_resolution": "exact",
 82682|       "evidence": [
 82683|         "return type 'Document' directly names a Revit DB object type"
 82684|       ],
 82685|       "source_url": "https://www.revitapidocs.com/2025/ced60288-464b-76e1-8d85-49b691c04a5f.htm",
 82686|       "dll_signature_verified": true,
 82687|       "dll_relationship_scope": "declared",
 82688|       "dll_semantic_verified": null,
 82689|       "dll_verified_status": "signature_verified_declared",
 82690|       "revitlookup_referenced": null,
```
