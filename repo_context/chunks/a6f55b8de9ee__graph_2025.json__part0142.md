# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 142 of 216
- Original line range: 54991-55390
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 54991|       "member_kind": "method",
 54992|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 54993|       "confidence": "direct_return_type",
 54994|       "confidence_tier": "unverified_reference",
 54995|       "target_resolution": "exact",
 54996|       "evidence": [
 54997|         "return type 'PolymeshFacet' directly names a Revit DB object type"
 54998|       ],
 54999|       "source_url": "https://www.revitapidocs.com/2025/9ce0a3c4-8ad9-c445-9af2-a71c13dd6ca9.htm",
 55000|       "dll_signature_verified": true,
 55001|       "dll_relationship_scope": "declared",
 55002|       "dll_semantic_verified": null,
 55003|       "dll_verified_status": "signature_verified_declared",
 55004|       "revitlookup_referenced": null,
 55005|       "revitlookup_requires_document_context": null
 55006|     },
 55007|     {
 55008|       "source": "Autodesk.Revit.DB.PolymeshTopology",
 55009|       "target": "Autodesk.Revit.DB.PolymeshFacet",
 55010|       "member_name": "GetFacets",
 55011|       "member_kind": "method",
 55012|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 55013|       "confidence": "needs_runtime_validation",
 55014|       "confidence_tier": "needs_validation",
 55015|       "target_resolution": "exact",
 55016|       "evidence": [
 55017|         "return type 'IList < PolymeshFacet >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 55018|       ],
 55019|       "source_url": "https://www.revitapidocs.com/2025/6225dc0b-0764-5682-0acd-200b1feb19d4.htm",
 55020|       "dll_signature_verified": true,
 55021|       "dll_relationship_scope": "declared",
 55022|       "dll_semantic_verified": null,
 55023|       "dll_verified_status": "signature_verified_declared",
 55024|       "revitlookup_referenced": null,
 55025|       "revitlookup_requires_document_context": null
 55026|     },
 55027|     {
 55028|       "source": "Autodesk.Revit.DB.PrintManager",
 55029|       "target": "Autodesk.Revit.DB.PaperSizeSet",
 55030|       "member_name": "PaperSizes",
 55031|       "member_kind": "property",
 55032|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 55033|       "confidence": "direct_return_type",
 55034|       "confidence_tier": "unverified_reference",
 55035|       "target_resolution": "exact",
 55036|       "evidence": [
 55037|         "return type 'PaperSizeSet' directly names a Revit DB object type"
 55038|       ],
 55039|       "source_url": "https://www.revitapidocs.com/2025/af5bd003-9399-2d70-4197-bc440aefab30.htm",
 55040|       "dll_signature_verified": true,
 55041|       "dll_relationship_scope": "declared",
 55042|       "dll_semantic_verified": null,
 55043|       "dll_verified_status": "signature_verified_declared",
 55044|       "revitlookup_referenced": null,
 55045|       "revitlookup_requires_document_context": null
 55046|     },
 55047|     {
 55048|       "source": "Autodesk.Revit.DB.PrintManager",
 55049|       "target": "Autodesk.Revit.DB.PaperSourceSet",
 55050|       "member_name": "PaperSources",
 55051|       "member_kind": "property",
 55052|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 55053|       "confidence": "direct_return_type",
 55054|       "confidence_tier": "unverified_reference",
 55055|       "target_resolution": "exact",
 55056|       "evidence": [
 55057|         "return type 'PaperSourceSet' directly names a Revit DB object type"
 55058|       ],
 55059|       "source_url": "https://www.revitapidocs.com/2025/c22a7905-70b3-f31f-c832-531e6828b1fb.htm",
 55060|       "dll_signature_verified": true,
 55061|       "dll_relationship_scope": "declared",
 55062|       "dll_semantic_verified": null,
 55063|       "dll_verified_status": "signature_verified_declared",
 55064|       "revitlookup_referenced": null,
 55065|       "revitlookup_requires_document_context": null
 55066|     },
 55067|     {
 55068|       "source": "Autodesk.Revit.DB.PrintManager",
 55069|       "target": "Autodesk.Revit.DB.PrintSetup",
 55070|       "member_name": "PrintSetup",
 55071|       "member_kind": "property",
 55072|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 55073|       "confidence": "direct_return_type",
 55074|       "confidence_tier": "unverified_reference",
 55075|       "target_resolution": "exact",
 55076|       "evidence": [
 55077|         "return type 'PrintSetup' directly names a Revit DB object type"
 55078|       ],
 55079|       "source_url": "https://www.revitapidocs.com/2025/7604c459-c519-d9df-a99f-8374ab6e5ea5.htm",
 55080|       "dll_signature_verified": true,
 55081|       "dll_relationship_scope": "declared",
 55082|       "dll_semantic_verified": null,
 55083|       "dll_verified_status": "signature_verified_declared",
 55084|       "revitlookup_referenced": null,
 55085|       "revitlookup_requires_document_context": null
 55086|     },
 55087|     {
 55088|       "source": "Autodesk.Revit.DB.PrintManager",
 55089|       "target": "Autodesk.Revit.DB.ViewSheetSetting",
 55090|       "member_name": "ViewSheetSetting",
 55091|       "member_kind": "property",
 55092|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 55093|       "confidence": "direct_return_type",
 55094|       "confidence_tier": "unverified_reference",
 55095|       "target_resolution": "exact",
 55096|       "evidence": [
 55097|         "member name 'ViewSheetSetting' matches keyword pattern /Sheet/ implying target 'ViewSheet', but the actual return type 'ViewSheetSetting' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 55098|         "return type 'ViewSheetSetting' directly names a Revit DB object type"
 55099|       ],
 55100|       "source_url": "https://www.revitapidocs.com/2025/7f288fd8-6cfb-efa7-9611-26773b5ff492.htm",
 55101|       "dll_signature_verified": true,
 55102|       "dll_relationship_scope": "declared",
 55103|       "dll_semantic_verified": null,
 55104|       "dll_verified_status": "signature_verified_declared",
 55105|       "revitlookup_referenced": null,
 55106|       "revitlookup_requires_document_context": null
 55107|     },
 55108|     {
 55109|       "source": "Autodesk.Revit.DB.PrintParameters",
 55110|       "target": null,
 55111|       "member_name": "HideUnreferencedViewTags",
 55112|       "member_kind": "property",
 55113|       "edge_type": "TAGS_ELEMENT",
 55114|       "confidence": "name_only_candidate",
 55115|       "confidence_tier": "likely",
 55116|       "target_resolution": "none",
 55117|       "evidence": [
 55118|         "member name 'HideUnreferencedViewTags' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'bool' gives no type-level confirmation"
 55119|       ],
 55120|       "source_url": "https://www.revitapidocs.com/2025/1906a8c6-a13e-8d93-7d79-11ba3c09fdb7.htm",
 55121|       "dll_signature_verified": true,
 55122|       "dll_relationship_scope": "declared",
 55123|       "dll_semantic_verified": null,
 55124|       "dll_verified_status": "signature_verified_declared",
 55125|       "revitlookup_referenced": null,
 55126|       "revitlookup_requires_document_context": null
 55127|     },
 55128|     {
 55129|       "source": "Autodesk.Revit.DB.PrintParameters",
 55130|       "target": "Autodesk.Revit.DB.PaperSize",
 55131|       "member_name": "PaperSize",
 55132|       "member_kind": "property",
 55133|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 55134|       "confidence": "direct_return_type",
 55135|       "confidence_tier": "unverified_reference",
 55136|       "target_resolution": "exact",
 55137|       "evidence": [
 55138|         "return type 'PaperSize' directly names a Revit DB object type"
 55139|       ],
 55140|       "source_url": "https://www.revitapidocs.com/2025/7a3778e5-6a20-2ac7-0986-60effa282194.htm",
 55141|       "dll_signature_verified": true,
 55142|       "dll_relationship_scope": "declared",
 55143|       "dll_semantic_verified": null,
 55144|       "dll_verified_status": "signature_verified_declared",
 55145|       "revitlookup_referenced": null,
 55146|       "revitlookup_requires_document_context": null
 55147|     },
 55148|     {
 55149|       "source": "Autodesk.Revit.DB.PrintParameters",
 55150|       "target": "Autodesk.Revit.DB.PaperSource",
 55151|       "member_name": "PaperSource",
 55152|       "member_kind": "property",
 55153|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 55154|       "confidence": "direct_return_type",
 55155|       "confidence_tier": "unverified_reference",
 55156|       "target_resolution": "exact",
 55157|       "evidence": [
 55158|         "return type 'PaperSource' directly names a Revit DB object type"
 55159|       ],
 55160|       "source_url": "https://www.revitapidocs.com/2025/e3a46d35-cc18-9fb4-e325-fba0de25f471.htm",
 55161|       "dll_signature_verified": true,
 55162|       "dll_relationship_scope": "declared",
 55163|       "dll_semantic_verified": null,
 55164|       "dll_verified_status": "signature_verified_declared",
 55165|       "revitlookup_referenced": null,
 55166|       "revitlookup_requires_document_context": null
 55167|     },
 55168|     {
 55169|       "source": "Autodesk.Revit.DB.PrintSetting",
 55170|       "target": "Autodesk.Revit.DB.PrintParameters",
 55171|       "member_name": "PrintParameters",
 55172|       "member_kind": "property",
 55173|       "edge_type": "HAS_PARAMETER",
 55174|       "confidence": "direct_return_type",
 55175|       "confidence_tier": "core",
 55176|       "target_resolution": "exact",
 55177|       "evidence": [
 55178|         "return type 'PrintParameters' directly names a Revit DB object type"
 55179|       ],
 55180|       "source_url": "https://www.revitapidocs.com/2025/1238672f-fa00-4d80-50fd-3278c46411ad.htm",
 55181|       "dll_signature_verified": true,
 55182|       "dll_relationship_scope": "declared",
 55183|       "dll_semantic_verified": null,
 55184|       "dll_verified_status": "signature_verified_declared",
 55185|       "revitlookup_referenced": null,
 55186|       "revitlookup_requires_document_context": null
 55187|     },
 55188|     {
 55189|       "source": "Autodesk.Revit.DB.PrintSetup",
 55190|       "target": "Autodesk.Revit.DB.IPrintSetting",
 55191|       "member_name": "CurrentPrintSetting",
 55192|       "member_kind": "property",
 55193|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 55194|       "confidence": "direct_return_type",
 55195|       "confidence_tier": "unverified_reference",
 55196|       "target_resolution": "exact",
 55197|       "evidence": [
 55198|         "return type 'IPrintSetting' directly names a Revit DB object type"
 55199|       ],
 55200|       "source_url": "https://www.revitapidocs.com/2025/64b832f9-8c68-70eb-1ade-a20a6344525e.htm",
 55201|       "dll_signature_verified": true,
 55202|       "dll_relationship_scope": "declared",
 55203|       "dll_semantic_verified": null,
 55204|       "dll_verified_status": "signature_verified_declared",
 55205|       "revitlookup_referenced": null,
 55206|       "revitlookup_requires_document_context": null
 55207|     },
 55208|     {
 55209|       "source": "Autodesk.Revit.DB.PrintSetup",
 55210|       "target": "Autodesk.Revit.DB.InSessionPrintSetting",
 55211|       "member_name": "InSession",
 55212|       "member_kind": "property",
 55213|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 55214|       "confidence": "direct_return_type",
 55215|       "confidence_tier": "unverified_reference",
 55216|       "target_resolution": "exact",
 55217|       "evidence": [
 55218|         "return type 'InSessionPrintSetting' directly names a Revit DB object type"
 55219|       ],
 55220|       "source_url": "https://www.revitapidocs.com/2025/8fa68bd4-9e97-f772-629b-25ef129939e3.htm",
 55221|       "dll_signature_verified": true,
 55222|       "dll_relationship_scope": "declared",
 55223|       "dll_semantic_verified": null,
 55224|       "dll_verified_status": "signature_verified_declared",
 55225|       "revitlookup_referenced": null,
 55226|       "revitlookup_requires_document_context": null
 55227|     },
 55228|     {
 55229|       "source": "Autodesk.Revit.DB.ProjectLocation",
 55230|       "target": "Autodesk.Revit.DB.ProjectPosition",
 55231|       "member_name": "GetProjectPosition",
 55232|       "member_kind": "method",
 55233|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 55234|       "confidence": "direct_return_type",
 55235|       "confidence_tier": "unverified_reference",
 55236|       "target_resolution": "exact",
 55237|       "evidence": [
 55238|         "return type 'ProjectPosition' directly names a Revit DB object type"
 55239|       ],
 55240|       "source_url": "https://www.revitapidocs.com/2025/45724712-d710-eb12-5a48-63a31a54f09f.htm",
 55241|       "dll_signature_verified": true,
 55242|       "dll_relationship_scope": "declared",
 55243|       "dll_semantic_verified": null,
 55244|       "dll_verified_status": "signature_verified_declared",
 55245|       "revitlookup_referenced": null,
 55246|       "revitlookup_requires_document_context": null
 55247|     },
 55248|     {
 55249|       "source": "Autodesk.Revit.DB.ProjectLocation",
 55250|       "target": "Autodesk.Revit.DB.SiteLocation",
 55251|       "member_name": "GetSiteLocation",
 55252|       "member_kind": "method",
 55253|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 55254|       "confidence": "direct_return_type",
 55255|       "confidence_tier": "unverified_reference",
 55256|       "target_resolution": "exact",
 55257|       "evidence": [
 55258|         "return type 'SiteLocation' directly names a Revit DB object type"
 55259|       ],
 55260|       "source_url": "https://www.revitapidocs.com/2025/b15628ab-b246-233d-6587-9205d2ad04a3.htm",
 55261|       "dll_signature_verified": true,
 55262|       "dll_relationship_scope": "declared",
 55263|       "dll_semantic_verified": null,
 55264|       "dll_verified_status": "signature_verified_declared",
 55265|       "revitlookup_referenced": null,
 55266|       "revitlookup_requires_document_context": null
 55267|     },
 55268|     {
 55269|       "source": "Autodesk.Revit.DB.ProjectLocationSet",
 55270|       "target": "Autodesk.Revit.DB.ProjectLocationSetIterator",
 55271|       "member_name": "ForwardIterator",
 55272|       "member_kind": "method",
 55273|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 55274|       "confidence": "direct_return_type",
 55275|       "confidence_tier": "unverified_reference",
 55276|       "target_resolution": "exact",
 55277|       "evidence": [
 55278|         "return type 'ProjectLocationSetIterator' directly names a Revit DB object type"
 55279|       ],
 55280|       "source_url": "https://www.revitapidocs.com/2025/9002a9ac-76e1-f7ca-adac-a59f2e1c1ccc.htm",
 55281|       "dll_signature_verified": true,
 55282|       "dll_relationship_scope": "declared",
 55283|       "dll_semantic_verified": null,
 55284|       "dll_verified_status": "signature_verified_declared",
 55285|       "revitlookup_referenced": null,
 55286|       "revitlookup_requires_document_context": null
 55287|     },
 55288|     {
 55289|       "source": "Autodesk.Revit.DB.ProjectLocationSet",
 55290|       "target": "Autodesk.Revit.DB.ProjectLocationSetIterator",
 55291|       "member_name": "ReverseIterator",
 55292|       "member_kind": "method",
 55293|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 55294|       "confidence": "direct_return_type",
 55295|       "confidence_tier": "unverified_reference",
 55296|       "target_resolution": "exact",
 55297|       "evidence": [
 55298|         "return type 'ProjectLocationSetIterator' directly names a Revit DB object type"
 55299|       ],
 55300|       "source_url": "https://www.revitapidocs.com/2025/7742c477-dc91-aba0-1513-a5187ff39540.htm",
 55301|       "dll_signature_verified": true,
 55302|       "dll_relationship_scope": "declared",
 55303|       "dll_semantic_verified": null,
 55304|       "dll_verified_status": "signature_verified_declared",
 55305|       "revitlookup_referenced": null,
 55306|       "revitlookup_requires_document_context": null
 55307|     },
 55308|     {
 55309|       "source": "Autodesk.Revit.DB.PropertySetElement",
 55310|       "target": "Autodesk.Revit.DB.StructuralAsset",
 55311|       "member_name": "GetStructuralAsset",
 55312|       "member_kind": "method",
 55313|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 55314|       "confidence": "direct_return_type",
 55315|       "confidence_tier": "unverified_reference",
 55316|       "target_resolution": "exact",
 55317|       "evidence": [
 55318|         "return type 'StructuralAsset' directly names a Revit DB object type"
 55319|       ],
 55320|       "source_url": "https://www.revitapidocs.com/2025/20f9ad9d-2fe7-dffe-dc9d-968dbc2bb9dd.htm",
 55321|       "dll_signature_verified": true,
 55322|       "dll_relationship_scope": "declared",
 55323|       "dll_semantic_verified": null,
 55324|       "dll_verified_status": "signature_verified_declared",
 55325|       "revitlookup_referenced": null,
 55326|       "revitlookup_requires_document_context": null
 55327|     },
 55328|     {
 55329|       "source": "Autodesk.Revit.DB.PropertySetElement",
 55330|       "target": "Autodesk.Revit.DB.ThermalAsset",
 55331|       "member_name": "GetThermalAsset",
 55332|       "member_kind": "method",
 55333|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 55334|       "confidence": "direct_return_type",
 55335|       "confidence_tier": "unverified_reference",
 55336|       "target_resolution": "exact",
 55337|       "evidence": [
 55338|         "return type 'ThermalAsset' directly names a Revit DB object type"
 55339|       ],
 55340|       "source_url": "https://www.revitapidocs.com/2025/583969ee-8bae-89f7-9a1c-40716ab359c0.htm",
 55341|       "dll_signature_verified": true,
 55342|       "dll_relationship_scope": "declared",
 55343|       "dll_semantic_verified": null,
 55344|       "dll_verified_status": "signature_verified_declared",
 55345|       "revitlookup_referenced": null,
 55346|       "revitlookup_requires_document_context": null
 55347|     },
 55348|     {
 55349|       "source": "Autodesk.Revit.DB.RadialArray",
 55350|       "target": null,
 55351|       "member_name": "ArrayElementsWithoutAssociation",
 55352|       "member_kind": "method",
 55353|       "edge_type": "RETURNS_ELEMENT_IDS",
 55354|       "confidence": "unknown_reference",
 55355|       "confidence_tier": "unverified_reference",
 55356|       "target_resolution": "none",
 55357|       "evidence": [
 55358|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 55359|       ],
 55360|       "source_url": "https://www.revitapidocs.com/2025/9a06b1f5-4894-09a8-cd90-b2ca8814511a.htm",
 55361|       "dll_signature_verified": true,
 55362|       "dll_relationship_scope": "declared",
 55363|       "dll_semantic_verified": null,
 55364|       "dll_verified_status": "signature_verified_declared",
 55365|       "revitlookup_referenced": null,
 55366|       "revitlookup_requires_document_context": null
 55367|     },
 55368|     {
 55369|       "source": "Autodesk.Revit.DB.RadialArray",
 55370|       "target": null,
 55371|       "member_name": "ArrayElementWithoutAssociation",
 55372|       "member_kind": "method",
 55373|       "edge_type": "RETURNS_ELEMENT_IDS",
 55374|       "confidence": "unknown_reference",
 55375|       "confidence_tier": "unverified_reference",
 55376|       "target_resolution": "none",
 55377|       "evidence": [
 55378|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 55379|       ],
 55380|       "source_url": "https://www.revitapidocs.com/2025/248559b9-c065-e05b-e8df-d60e8ed5e47d.htm",
 55381|       "dll_signature_verified": true,
 55382|       "dll_relationship_scope": "declared",
 55383|       "dll_semantic_verified": null,
 55384|       "dll_verified_status": "signature_verified_declared",
 55385|       "revitlookup_referenced": null,
 55386|       "revitlookup_requires_document_context": null
 55387|     },
 55388|     {
 55389|       "source": "Autodesk.Revit.DB.RadialArray",
 55390|       "target": null,
```
