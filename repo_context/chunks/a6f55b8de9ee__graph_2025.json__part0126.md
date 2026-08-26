# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 126 of 216
- Original line range: 48751-49150
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 48751|       "target_resolution": "exact",
 48752|       "evidence": [
 48753|         "return type 'FamilyElementVisibility' directly names a Revit DB object type"
 48754|       ],
 48755|       "source_url": "https://www.revitapidocs.com/2025/08de9969-d4c6-1893-ce64-59e5692da23f.htm",
 48756|       "dll_signature_verified": true,
 48757|       "dll_relationship_scope": "declared",
 48758|       "dll_semantic_verified": null,
 48759|       "dll_verified_status": "signature_verified_declared",
 48760|       "revitlookup_referenced": null,
 48761|       "revitlookup_requires_document_context": null
 48762|     },
 48763|     {
 48764|       "source": "Autodesk.Revit.DB.IndependentTag",
 48765|       "target": "Autodesk.Revit.DB.Material",
 48766|       "member_name": "IsMaterialTag",
 48767|       "member_kind": "property",
 48768|       "edge_type": "USES_MATERIAL",
 48769|       "confidence": "name_only_candidate",
 48770|       "confidence_tier": "likely",
 48771|       "target_resolution": "exact",
 48772|       "evidence": [
 48773|         "member name 'IsMaterialTag' matches keyword pattern /Material/ but return type 'bool' gives no type-level confirmation"
 48774|       ],
 48775|       "source_url": "https://www.revitapidocs.com/2025/7670e365-227c-bc9d-a4b0-c77f793034e2.htm",
 48776|       "dll_signature_verified": true,
 48777|       "dll_relationship_scope": "declared",
 48778|       "dll_semantic_verified": null,
 48779|       "dll_verified_status": "signature_verified_declared",
 48780|       "revitlookup_referenced": null,
 48781|       "revitlookup_requires_document_context": null
 48782|     },
 48783|     {
 48784|       "source": "Autodesk.Revit.DB.IndependentTag",
 48785|       "target": null,
 48786|       "member_name": "IsMulticategoryTag",
 48787|       "member_kind": "property",
 48788|       "edge_type": "TAGS_ELEMENT",
 48789|       "confidence": "name_only_candidate",
 48790|       "confidence_tier": "likely",
 48791|       "target_resolution": "none",
 48792|       "evidence": [
 48793|         "member name 'IsMulticategoryTag' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'bool' gives no type-level confirmation"
 48794|       ],
 48795|       "source_url": "https://www.revitapidocs.com/2025/91f6e4f0-6a6b-efa6-5730-ea32a5cb203c.htm",
 48796|       "dll_signature_verified": true,
 48797|       "dll_relationship_scope": "declared",
 48798|       "dll_semantic_verified": null,
 48799|       "dll_verified_status": "signature_verified_declared",
 48800|       "revitlookup_referenced": null,
 48801|       "revitlookup_requires_document_context": null
 48802|     },
 48803|     {
 48804|       "source": "Autodesk.Revit.DB.IndependentTag",
 48805|       "target": null,
 48806|       "member_name": "MultiReferenceAnnotationId",
 48807|       "member_kind": "property",
 48808|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 48809|       "confidence": "unknown_reference",
 48810|       "confidence_tier": "unverified_reference",
 48811|       "target_resolution": "none",
 48812|       "evidence": [
 48813|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 48814|       ],
 48815|       "source_url": "https://www.revitapidocs.com/2025/30363f1b-0fd1-fa0e-5617-be77d683662e.htm",
 48816|       "dll_signature_verified": true,
 48817|       "dll_relationship_scope": "declared",
 48818|       "dll_semantic_verified": null,
 48819|       "dll_verified_status": "signature_verified_declared",
 48820|       "revitlookup_referenced": null,
 48821|       "revitlookup_requires_document_context": null
 48822|     },
 48823|     {
 48824|       "source": "Autodesk.Revit.DB.IndependentTag",
 48825|       "target": null,
 48826|       "member_name": "TagOrientation",
 48827|       "member_kind": "property",
 48828|       "edge_type": "TAGS_ELEMENT",
 48829|       "confidence": "name_only_candidate",
 48830|       "confidence_tier": "likely",
 48831|       "target_resolution": "none",
 48832|       "evidence": [
 48833|         "member name 'TagOrientation' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'TagOrientation' gives no type-level confirmation"
 48834|       ],
 48835|       "source_url": "https://www.revitapidocs.com/2025/d9d43a13-a972-3b69-2484-b6e336e9a0c5.htm",
 48836|       "dll_signature_verified": true,
 48837|       "dll_relationship_scope": "declared",
 48838|       "dll_semantic_verified": null,
 48839|       "dll_verified_status": "signature_verified_declared",
 48840|       "revitlookup_referenced": null,
 48841|       "revitlookup_requires_document_context": null
 48842|     },
 48843|     {
 48844|       "source": "Autodesk.Revit.DB.IndependentTag",
 48845|       "target": null,
 48846|       "member_name": "TagText",
 48847|       "member_kind": "property",
 48848|       "edge_type": "TAGS_ELEMENT",
 48849|       "confidence": "name_only_candidate",
 48850|       "confidence_tier": "likely",
 48851|       "target_resolution": "none",
 48852|       "evidence": [
 48853|         "member name 'TagText' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'string' gives no type-level confirmation"
 48854|       ],
 48855|       "source_url": "https://www.revitapidocs.com/2025/8e297dee-920d-f620-6198-0bed494e3f04.htm",
 48856|       "dll_signature_verified": true,
 48857|       "dll_relationship_scope": "declared",
 48858|       "dll_semantic_verified": null,
 48859|       "dll_verified_status": "signature_verified_declared",
 48860|       "revitlookup_referenced": true,
 48861|       "revitlookup_requires_document_context": false
 48862|     },
 48863|     {
 48864|       "source": "Autodesk.Revit.DB.IndependentTag",
 48865|       "target": null,
 48866|       "member_name": "GetTaggedElementIds",
 48867|       "member_kind": "method",
 48868|       "edge_type": "TAGS_ELEMENT",
 48869|       "confidence": "elementid_collection_with_strong_name",
 48870|       "confidence_tier": "core",
 48871|       "target_resolution": "none",
 48872|       "evidence": [
 48873|         "member name 'GetTaggedElementIds' matches keyword pattern /^GetTagged|Tag(ged)?/"
 48874|       ],
 48875|       "source_url": "https://www.revitapidocs.com/2025/43260944-f46c-d5df-c9ea-f3a44e24d1a2.htm",
 48876|       "dll_signature_verified": true,
 48877|       "dll_relationship_scope": "declared",
 48878|       "dll_semantic_verified": null,
 48879|       "dll_verified_status": "signature_verified_declared",
 48880|       "revitlookup_referenced": null,
 48881|       "revitlookup_requires_document_context": null
 48882|     },
 48883|     {
 48884|       "source": "Autodesk.Revit.DB.IndependentTag",
 48885|       "target": null,
 48886|       "member_name": "GetTaggedLocalElementIds",
 48887|       "member_kind": "method",
 48888|       "edge_type": "TAGS_ELEMENT",
 48889|       "confidence": "elementid_collection_with_strong_name",
 48890|       "confidence_tier": "core",
 48891|       "target_resolution": "none",
 48892|       "evidence": [
 48893|         "member name 'GetTaggedLocalElementIds' matches keyword pattern /^GetTagged|Tag(ged)?/"
 48894|       ],
 48895|       "source_url": "https://www.revitapidocs.com/2025/142026b1-69d5-7b1e-f5b3-3360abc0f9be.htm",
 48896|       "dll_signature_verified": true,
 48897|       "dll_relationship_scope": "declared",
 48898|       "dll_semantic_verified": null,
 48899|       "dll_verified_status": "signature_verified_declared",
 48900|       "revitlookup_referenced": null,
 48901|       "revitlookup_requires_document_context": null
 48902|     },
 48903|     {
 48904|       "source": "Autodesk.Revit.DB.IndependentTag",
 48905|       "target": "Autodesk.Revit.DB.Element",
 48906|       "member_name": "GetTaggedLocalElements",
 48907|       "member_kind": "method",
 48908|       "edge_type": "TAGS_ELEMENT",
 48909|       "confidence": "needs_runtime_validation",
 48910|       "confidence_tier": "needs_validation",
 48911|       "target_resolution": "exact",
 48912|       "evidence": [
 48913|         "return type 'ICollection < Element >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 48914|       ],
 48915|       "source_url": "https://www.revitapidocs.com/2025/f238e2c7-e184-172c-546a-7dd694857e56.htm",
 48916|       "dll_signature_verified": true,
 48917|       "dll_relationship_scope": "declared",
 48918|       "dll_semantic_verified": null,
 48919|       "dll_verified_status": "signature_verified_declared",
 48920|       "revitlookup_referenced": null,
 48921|       "revitlookup_requires_document_context": null
 48922|     },
 48923|     {
 48924|       "source": "Autodesk.Revit.DB.IndependentTag",
 48925|       "target": "Autodesk.Revit.DB.Reference",
 48926|       "member_name": "GetTaggedReferences",
 48927|       "member_kind": "method",
 48928|       "edge_type": "TAGS_ELEMENT",
 48929|       "confidence": "needs_runtime_validation",
 48930|       "confidence_tier": "needs_validation",
 48931|       "target_resolution": "exact",
 48932|       "evidence": [
 48933|         "return type 'IList < Reference >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 48934|       ],
 48935|       "source_url": "https://www.revitapidocs.com/2025/63a90870-6b37-57b2-26fe-6fa1e605a9da.htm",
 48936|       "dll_signature_verified": true,
 48937|       "dll_relationship_scope": "declared",
 48938|       "dll_semantic_verified": null,
 48939|       "dll_verified_status": "signature_verified_declared",
 48940|       "revitlookup_referenced": null,
 48941|       "revitlookup_requires_document_context": null
 48942|     },
 48943|     {
 48944|       "source": "Autodesk.Revit.DB.IndependentTag",
 48945|       "target": null,
 48946|       "member_name": "HasTagBehavior",
 48947|       "member_kind": "method",
 48948|       "edge_type": "TAGS_ELEMENT",
 48949|       "confidence": "name_only_candidate",
 48950|       "confidence_tier": "likely",
 48951|       "target_resolution": "none",
 48952|       "evidence": [
 48953|         "member name 'HasTagBehavior' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'bool' gives no type-level confirmation"
 48954|       ],
 48955|       "source_url": "https://www.revitapidocs.com/2025/fadc39b4-eb80-2e00-f9af-e5752a0bb498.htm",
 48956|       "dll_signature_verified": true,
 48957|       "dll_relationship_scope": "declared",
 48958|       "dll_semantic_verified": null,
 48959|       "dll_verified_status": "signature_verified_declared",
 48960|       "revitlookup_referenced": null,
 48961|       "revitlookup_requires_document_context": null
 48962|     },
 48963|     {
 48964|       "source": "Autodesk.Revit.DB.IndependentTag",
 48965|       "target": null,
 48966|       "member_name": "IsTaggedOnSubelement",
 48967|       "member_kind": "method",
 48968|       "edge_type": "TAGS_ELEMENT",
 48969|       "confidence": "name_only_candidate",
 48970|       "confidence_tier": "likely",
 48971|       "target_resolution": "none",
 48972|       "evidence": [
 48973|         "member name 'IsTaggedOnSubelement' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'bool' gives no type-level confirmation"
 48974|       ],
 48975|       "source_url": "https://www.revitapidocs.com/2025/8f6f8b8c-1b1b-1141-e165-1eecd4b765a1.htm",
 48976|       "dll_signature_verified": true,
 48977|       "dll_relationship_scope": "declared",
 48978|       "dll_semantic_verified": null,
 48979|       "dll_verified_status": "signature_verified_declared",
 48980|       "revitlookup_referenced": null,
 48981|       "revitlookup_requires_document_context": null
 48982|     },
 48983|     {
 48984|       "source": "Autodesk.Revit.DB.InSessionPrintSetting",
 48985|       "target": "Autodesk.Revit.DB.PrintParameters",
 48986|       "member_name": "PrintParameters",
 48987|       "member_kind": "property",
 48988|       "edge_type": "HAS_PARAMETER",
 48989|       "confidence": "direct_return_type",
 48990|       "confidence_tier": "core",
 48991|       "target_resolution": "exact",
 48992|       "evidence": [
 48993|         "return type 'PrintParameters' directly names a Revit DB object type"
 48994|       ],
 48995|       "source_url": "https://www.revitapidocs.com/2025/e593c73f-0550-bc9f-2bdf-639797f4c1a6.htm",
 48996|       "dll_signature_verified": true,
 48997|       "dll_relationship_scope": "declared",
 48998|       "dll_semantic_verified": null,
 48999|       "dll_verified_status": "signature_verified_declared",
 49000|       "revitlookup_referenced": null,
 49001|       "revitlookup_requires_document_context": null
 49002|     },
 49003|     {
 49004|       "source": "Autodesk.Revit.DB.InSessionViewSheetSet",
 49005|       "target": "Autodesk.Revit.DB.ViewSheet",
 49006|       "member_name": "SheetOrganizationId",
 49007|       "member_kind": "property",
 49008|       "edge_type": "PLACED_ON_SHEET",
 49009|       "confidence": "elementid_with_strong_name",
 49010|       "confidence_tier": "core",
 49011|       "target_resolution": "exact",
 49012|       "evidence": [
 49013|         "member name 'SheetOrganizationId' matches keyword pattern /Sheet/"
 49014|       ],
 49015|       "source_url": "https://www.revitapidocs.com/2025/54e9ea27-21d1-5fd7-114b-25b3b6854cfe.htm",
 49016|       "dll_signature_verified": true,
 49017|       "dll_relationship_scope": "declared",
 49018|       "dll_semantic_verified": null,
 49019|       "dll_verified_status": "signature_verified_declared",
 49020|       "revitlookup_referenced": null,
 49021|       "revitlookup_requires_document_context": null
 49022|     },
 49023|     {
 49024|       "source": "Autodesk.Revit.DB.InSessionViewSheetSet",
 49025|       "target": null,
 49026|       "member_name": "ViewOrganizationId",
 49027|       "member_kind": "property",
 49028|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 49029|       "confidence": "unknown_reference",
 49030|       "confidence_tier": "unverified_reference",
 49031|       "target_resolution": "none",
 49032|       "evidence": [
 49033|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 49034|       ],
 49035|       "source_url": "https://www.revitapidocs.com/2025/1b0df884-d8d4-e55f-f940-6c96fbea125b.htm",
 49036|       "dll_signature_verified": true,
 49037|       "dll_relationship_scope": "declared",
 49038|       "dll_semantic_verified": null,
 49039|       "dll_verified_status": "signature_verified_declared",
 49040|       "revitlookup_referenced": null,
 49041|       "revitlookup_requires_document_context": null
 49042|     },
 49043|     {
 49044|       "source": "Autodesk.Revit.DB.InSessionViewSheetSet",
 49045|       "target": "Autodesk.Revit.DB.ViewSet",
 49046|       "member_name": "Views",
 49047|       "member_kind": "property",
 49048|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49049|       "confidence": "direct_return_type",
 49050|       "confidence_tier": "unverified_reference",
 49051|       "target_resolution": "exact",
 49052|       "evidence": [
 49053|         "return type 'ViewSet' directly names a Revit DB object type"
 49054|       ],
 49055|       "source_url": "https://www.revitapidocs.com/2025/fadd0919-968c-26d7-b3e2-1454ad194312.htm",
 49056|       "dll_signature_verified": true,
 49057|       "dll_relationship_scope": "declared",
 49058|       "dll_semantic_verified": null,
 49059|       "dll_verified_status": "signature_verified_declared",
 49060|       "revitlookup_referenced": null,
 49061|       "revitlookup_requires_document_context": null
 49062|     },
 49063|     {
 49064|       "source": "Autodesk.Revit.DB.InstanceNode",
 49065|       "target": "Autodesk.Revit.DB.SymbolGeometryId",
 49066|       "member_name": "GetSymbolGeometryId",
 49067|       "member_kind": "method",
 49068|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 49069|       "confidence": "direct_return_type",
 49070|       "confidence_tier": "unverified_reference",
 49071|       "target_resolution": "exact",
 49072|       "evidence": [
 49073|         "return type 'SymbolGeometryId' directly names a Revit DB object type"
 49074|       ],
 49075|       "source_url": "https://www.revitapidocs.com/2025/6e6cbd6a-493f-7329-83e2-c28b333eede8.htm",
 49076|       "dll_signature_verified": true,
 49077|       "dll_relationship_scope": "declared",
 49078|       "dll_semantic_verified": null,
 49079|       "dll_verified_status": "signature_verified_declared",
 49080|       "revitlookup_referenced": null,
 49081|       "revitlookup_requires_document_context": null
 49082|     },
 49083|     {
 49084|       "source": "Autodesk.Revit.DB.InstanceVoidCutUtils",
 49085|       "target": null,
 49086|       "member_name": "GetCuttingVoidInstances",
 49087|       "member_kind": "method",
 49088|       "edge_type": "RETURNS_ELEMENT_IDS",
 49089|       "confidence": "unknown_reference",
 49090|       "confidence_tier": "unverified_reference",
 49091|       "target_resolution": "none",
 49092|       "evidence": [
 49093|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 49094|       ],
 49095|       "source_url": "https://www.revitapidocs.com/2025/79d10f4e-9ab1-adfb-f89d-c5c754712b23.htm",
 49096|       "dll_signature_verified": true,
 49097|       "dll_relationship_scope": "declared",
 49098|       "dll_semantic_verified": null,
 49099|       "dll_verified_status": "signature_verified_declared",
 49100|       "revitlookup_referenced": null,
 49101|       "revitlookup_requires_document_context": null
 49102|     },
 49103|     {
 49104|       "source": "Autodesk.Revit.DB.InstanceVoidCutUtils",
 49105|       "target": null,
 49106|       "member_name": "GetElementsBeingCut",
 49107|       "member_kind": "method",
 49108|       "edge_type": "RETURNS_ELEMENT_IDS",
 49109|       "confidence": "unknown_reference",
 49110|       "confidence_tier": "unverified_reference",
 49111|       "target_resolution": "none",
 49112|       "evidence": [
 49113|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 49114|       ],
 49115|       "source_url": "https://www.revitapidocs.com/2025/e709fbe6-5508-6212-07d6-cefd3c095d9e.htm",
 49116|       "dll_signature_verified": true,
 49117|       "dll_relationship_scope": "declared",
 49118|       "dll_semantic_verified": null,
 49119|       "dll_verified_status": "signature_verified_declared",
 49120|       "revitlookup_referenced": null,
 49121|       "revitlookup_requires_document_context": null
 49122|     },
 49123|     {
 49124|       "source": "Autodesk.Revit.DB.InsulationLiningBase",
 49125|       "target": null,
 49126|       "member_name": "HostElementId",
 49127|       "member_kind": "property",
 49128|       "edge_type": "HOSTED_BY",
 49129|       "confidence": "elementid_with_strong_name",
 49130|       "confidence_tier": "core",
 49131|       "target_resolution": "none",
 49132|       "evidence": [
 49133|         "member name 'HostElementId' matches keyword pattern /^GetHosted|Host/"
 49134|       ],
 49135|       "source_url": "https://www.revitapidocs.com/2025/3bd52d75-7aea-1d0f-0ff6-0eae6b9a4928.htm",
 49136|       "dll_signature_verified": true,
 49137|       "dll_relationship_scope": "declared",
 49138|       "dll_semantic_verified": null,
 49139|       "dll_verified_status": "signature_verified_declared",
 49140|       "revitlookup_referenced": null,
 49141|       "revitlookup_requires_document_context": null
 49142|     },
 49143|     {
 49144|       "source": "Autodesk.Revit.DB.InsulationLiningBase",
 49145|       "target": null,
 49146|       "member_name": "GetInsulationIds",
 49147|       "member_kind": "method",
 49148|       "edge_type": "RETURNS_ELEMENT_IDS",
 49149|       "confidence": "unknown_reference",
 49150|       "confidence_tier": "unverified_reference",
```
