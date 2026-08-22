# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 188 of 216
- Original line range: 72931-73330
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 72931|       "edge_type": "RETURNS_ELEMENT_IDS",
 72932|       "confidence": "unknown_reference",
 72933|       "confidence_tier": "unverified_reference",
 72934|       "target_resolution": "none",
 72935|       "evidence": [
 72936|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 72937|       ],
 72938|       "source_url": "https://www.revitapidocs.com/2025/3a67af27-5ec8-d59a-4a05-367c724e5cd9.htm",
 72939|       "dll_signature_verified": true,
 72940|       "dll_relationship_scope": "declared",
 72941|       "dll_semantic_verified": null,
 72942|       "dll_verified_status": "signature_verified_declared",
 72943|       "revitlookup_referenced": null,
 72944|       "revitlookup_requires_document_context": null
 72945|     },
 72946|     {
 72947|       "source": "Autodesk.Revit.DB.Events.DocumentPrintedEventArgs",
 72948|       "target": null,
 72949|       "member_name": "GetPrintedViewElementIds",
 72950|       "member_kind": "method",
 72951|       "edge_type": "RETURNS_ELEMENT_IDS",
 72952|       "confidence": "unknown_reference",
 72953|       "confidence_tier": "unverified_reference",
 72954|       "target_resolution": "none",
 72955|       "evidence": [
 72956|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 72957|       ],
 72958|       "source_url": "https://www.revitapidocs.com/2025/679b375e-216c-5cee-d431-dc593649bac4.htm",
 72959|       "dll_signature_verified": true,
 72960|       "dll_relationship_scope": "declared",
 72961|       "dll_semantic_verified": null,
 72962|       "dll_verified_status": "signature_verified_declared",
 72963|       "revitlookup_referenced": null,
 72964|       "revitlookup_requires_document_context": null
 72965|     },
 72966|     {
 72967|       "source": "Autodesk.Revit.DB.Events.DocumentPrintingEventArgs",
 72968|       "target": "Autodesk.Revit.DB.IPrintSetting",
 72969|       "member_name": "GetSettings",
 72970|       "member_kind": "method",
 72971|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 72972|       "confidence": "direct_return_type",
 72973|       "confidence_tier": "unverified_reference",
 72974|       "target_resolution": "exact",
 72975|       "evidence": [
 72976|         "return type 'IPrintSetting' directly names a Revit DB object type"
 72977|       ],
 72978|       "source_url": "https://www.revitapidocs.com/2025/6f41396e-1ebf-f600-a8f3-fd2a6837b2a1.htm",
 72979|       "dll_signature_verified": true,
 72980|       "dll_relationship_scope": "declared",
 72981|       "dll_semantic_verified": null,
 72982|       "dll_verified_status": "signature_verified_declared",
 72983|       "revitlookup_referenced": null,
 72984|       "revitlookup_requires_document_context": null
 72985|     },
 72986|     {
 72987|       "source": "Autodesk.Revit.DB.Events.DocumentPrintingEventArgs",
 72988|       "target": null,
 72989|       "member_name": "GetViewElementIds",
 72990|       "member_kind": "method",
 72991|       "edge_type": "RETURNS_ELEMENT_IDS",
 72992|       "confidence": "unknown_reference",
 72993|       "confidence_tier": "unverified_reference",
 72994|       "target_resolution": "none",
 72995|       "evidence": [
 72996|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 72997|       ],
 72998|       "source_url": "https://www.revitapidocs.com/2025/32d6fdf7-3bd7-41b8-d0e7-1b5c5e5b442c.htm",
 72999|       "dll_signature_verified": true,
 73000|       "dll_relationship_scope": "declared",
 73001|       "dll_semantic_verified": null,
 73002|       "dll_verified_status": "signature_verified_declared",
 73003|       "revitlookup_referenced": null,
 73004|       "revitlookup_requires_document_context": null
 73005|     },
 73006|     {
 73007|       "source": "Autodesk.Revit.DB.Events.DocumentReloadedLatestEventArgs",
 73008|       "target": "Autodesk.Revit.DB.Location",
 73009|       "member_name": "Location",
 73010|       "member_kind": "property",
 73011|       "edge_type": "REFERENCES",
 73012|       "confidence": "name_only_candidate",
 73013|       "confidence_tier": "likely",
 73014|       "target_resolution": "exact",
 73015|       "evidence": [
 73016|         "member name 'Location' matches keyword pattern /^Location$/ but return type 'string' gives no type-level confirmation"
 73017|       ],
 73018|       "source_url": "https://www.revitapidocs.com/2025/d7619ab0-cad6-42f9-4092-bb047a45cce9.htm",
 73019|       "dll_signature_verified": true,
 73020|       "dll_relationship_scope": "declared",
 73021|       "dll_semantic_verified": null,
 73022|       "dll_verified_status": "signature_verified_declared",
 73023|       "revitlookup_referenced": null,
 73024|       "revitlookup_requires_document_context": null
 73025|     },
 73026|     {
 73027|       "source": "Autodesk.Revit.DB.Events.DocumentSynchronizingWithCentralEventArgs",
 73028|       "target": "Autodesk.Revit.DB.Location",
 73029|       "member_name": "Location",
 73030|       "member_kind": "property",
 73031|       "edge_type": "REFERENCES",
 73032|       "confidence": "name_only_candidate",
 73033|       "confidence_tier": "likely",
 73034|       "target_resolution": "exact",
 73035|       "evidence": [
 73036|         "member name 'Location' matches keyword pattern /^Location$/ but return type 'string' gives no type-level confirmation"
 73037|       ],
 73038|       "source_url": "https://www.revitapidocs.com/2025/c13b3202-8d75-08c1-1f3b-0ee1f81721c9.htm",
 73039|       "dll_signature_verified": true,
 73040|       "dll_relationship_scope": "declared",
 73041|       "dll_semantic_verified": null,
 73042|       "dll_verified_status": "signature_verified_declared",
 73043|       "revitlookup_referenced": null,
 73044|       "revitlookup_requires_document_context": null
 73045|     },
 73046|     {
 73047|       "source": "Autodesk.Revit.DB.Events.DocumentSynchronizingWithCentralEventArgs",
 73048|       "target": "Autodesk.Revit.DB.SynchronizeWithCentralOptions",
 73049|       "member_name": "Options",
 73050|       "member_kind": "property",
 73051|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 73052|       "confidence": "direct_return_type",
 73053|       "confidence_tier": "unverified_reference",
 73054|       "target_resolution": "exact",
 73055|       "evidence": [
 73056|         "return type 'SynchronizeWithCentralOptions' directly names a Revit DB object type"
 73057|       ],
 73058|       "source_url": "https://www.revitapidocs.com/2025/7aa7c50c-37cb-ca2c-76c5-860a00b527b4.htm",
 73059|       "dll_signature_verified": true,
 73060|       "dll_relationship_scope": "declared",
 73061|       "dll_semantic_verified": null,
 73062|       "dll_verified_status": "signature_verified_declared",
 73063|       "revitlookup_referenced": null,
 73064|       "revitlookup_requires_document_context": null
 73065|     },
 73066|     {
 73067|       "source": "Autodesk.Revit.DB.Events.DocumentWorksharingEnabledEventArgs",
 73068|       "target": "Autodesk.Revit.DB.Document",
 73069|       "member_name": "GetDocument",
 73070|       "member_kind": "method",
 73071|       "edge_type": "REFERENCES",
 73072|       "confidence": "direct_return_type",
 73073|       "confidence_tier": "core",
 73074|       "target_resolution": "exact",
 73075|       "evidence": [
 73076|         "return type 'Document' directly names a Revit DB object type"
 73077|       ],
 73078|       "source_url": "https://www.revitapidocs.com/2025/ec85ce1b-7767-0d6f-3ce5-c29a19468a7a.htm",
 73079|       "dll_signature_verified": true,
 73080|       "dll_relationship_scope": "declared",
 73081|       "dll_semantic_verified": null,
 73082|       "dll_verified_status": "signature_verified_declared",
 73083|       "revitlookup_referenced": null,
 73084|       "revitlookup_requires_document_context": null
 73085|     },
 73086|     {
 73087|       "source": "Autodesk.Revit.DB.Events.ElementTypeDuplicatedEventArgs",
 73088|       "target": null,
 73089|       "member_name": "NewElementTypeId",
 73090|       "member_kind": "property",
 73091|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 73092|       "confidence": "unknown_reference",
 73093|       "confidence_tier": "unverified_reference",
 73094|       "target_resolution": "none",
 73095|       "evidence": [
 73096|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 73097|       ],
 73098|       "source_url": "https://www.revitapidocs.com/2025/31163df2-d06e-cef6-9830-2b2702a88047.htm",
 73099|       "dll_signature_verified": true,
 73100|       "dll_relationship_scope": "declared",
 73101|       "dll_semantic_verified": null,
 73102|       "dll_verified_status": "signature_verified_declared",
 73103|       "revitlookup_referenced": null,
 73104|       "revitlookup_requires_document_context": null
 73105|     },
 73106|     {
 73107|       "source": "Autodesk.Revit.DB.Events.ElementTypeDuplicatedEventArgs",
 73108|       "target": null,
 73109|       "member_name": "OriginalElementTypeId",
 73110|       "member_kind": "property",
 73111|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 73112|       "confidence": "unknown_reference",
 73113|       "confidence_tier": "unverified_reference",
 73114|       "target_resolution": "none",
 73115|       "evidence": [
 73116|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 73117|       ],
 73118|       "source_url": "https://www.revitapidocs.com/2025/1b324b35-0617-b47c-8d05-5af63e72cd7c.htm",
 73119|       "dll_signature_verified": true,
 73120|       "dll_relationship_scope": "declared",
 73121|       "dll_semantic_verified": null,
 73122|       "dll_verified_status": "signature_verified_declared",
 73123|       "revitlookup_referenced": null,
 73124|       "revitlookup_requires_document_context": null
 73125|     },
 73126|     {
 73127|       "source": "Autodesk.Revit.DB.Events.ElementTypeDuplicatingEventArgs",
 73128|       "target": null,
 73129|       "member_name": "ElementTypeId",
 73130|       "member_kind": "property",
 73131|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 73132|       "confidence": "unknown_reference",
 73133|       "confidence_tier": "unverified_reference",
 73134|       "target_resolution": "none",
 73135|       "evidence": [
 73136|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 73137|       ],
 73138|       "source_url": "https://www.revitapidocs.com/2025/ef0229e3-2e15-2b36-538e-487b8b136a2d.htm",
 73139|       "dll_signature_verified": true,
 73140|       "dll_relationship_scope": "declared",
 73141|       "dll_semantic_verified": null,
 73142|       "dll_verified_status": "signature_verified_declared",
 73143|       "revitlookup_referenced": null,
 73144|       "revitlookup_requires_document_context": null
 73145|     },
 73146|     {
 73147|       "source": "Autodesk.Revit.DB.Events.ExternalDataInstanceAddedIntoDocumentEventArgs",
 73148|       "target": null,
 73149|       "member_name": "NewInstanceId",
 73150|       "member_kind": "property",
 73151|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 73152|       "confidence": "unknown_reference",
 73153|       "confidence_tier": "unverified_reference",
 73154|       "target_resolution": "none",
 73155|       "evidence": [
 73156|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 73157|       ],
 73158|       "source_url": "https://www.revitapidocs.com/2025/4859f4fa-bcc6-b7c9-bae0-cefdca0980a2.htm",
 73159|       "dll_signature_verified": true,
 73160|       "dll_relationship_scope": "declared",
 73161|       "dll_semantic_verified": null,
 73162|       "dll_verified_status": "signature_verified_declared",
 73163|       "revitlookup_referenced": null,
 73164|       "revitlookup_requires_document_context": null
 73165|     },
 73166|     {
 73167|       "source": "Autodesk.Revit.DB.Events.ExternalDataInstanceAddedIntoDocumentEventArgs",
 73168|       "target": null,
 73169|       "member_name": "TypeId",
 73170|       "member_kind": "property",
 73171|       "edge_type": "TYPE_OF",
 73172|       "confidence": "elementid_with_strong_name",
 73173|       "confidence_tier": "core",
 73174|       "target_resolution": "none",
 73175|       "evidence": [
 73176|         "member name 'TypeId' matches keyword pattern /^(Type|TypeId|GetTypeId)$/"
 73177|       ],
 73178|       "source_url": "https://www.revitapidocs.com/2025/233f75f8-96f5-abb0-8694-467106227b15.htm",
 73179|       "dll_signature_verified": true,
 73180|       "dll_relationship_scope": "declared",
 73181|       "dll_semantic_verified": null,
 73182|       "dll_verified_status": "signature_verified_declared",
 73183|       "revitlookup_referenced": null,
 73184|       "revitlookup_requires_document_context": null
 73185|     },
 73186|     {
 73187|       "source": "Autodesk.Revit.DB.Events.ExternalDataInstanceAddingIntoDocumentEventArgs",
 73188|       "target": null,
 73189|       "member_name": "TypeId",
 73190|       "member_kind": "property",
 73191|       "edge_type": "TYPE_OF",
 73192|       "confidence": "elementid_with_strong_name",
 73193|       "confidence_tier": "core",
 73194|       "target_resolution": "none",
 73195|       "evidence": [
 73196|         "member name 'TypeId' matches keyword pattern /^(Type|TypeId|GetTypeId)$/"
 73197|       ],
 73198|       "source_url": "https://www.revitapidocs.com/2025/8483e71e-0fec-c382-7b13-f53ac5c9741d.htm",
 73199|       "dll_signature_verified": true,
 73200|       "dll_relationship_scope": "declared",
 73201|       "dll_semantic_verified": null,
 73202|       "dll_verified_status": "signature_verified_declared",
 73203|       "revitlookup_referenced": null,
 73204|       "revitlookup_requires_document_context": null
 73205|     },
 73206|     {
 73207|       "source": "Autodesk.Revit.DB.Events.ExternalDataInstanceRemovedFromDocumentEventArgs",
 73208|       "target": null,
 73209|       "member_name": "TypeId",
 73210|       "member_kind": "property",
 73211|       "edge_type": "TYPE_OF",
 73212|       "confidence": "elementid_with_strong_name",
 73213|       "confidence_tier": "core",
 73214|       "target_resolution": "none",
 73215|       "evidence": [
 73216|         "member name 'TypeId' matches keyword pattern /^(Type|TypeId|GetTypeId)$/"
 73217|       ],
 73218|       "source_url": "https://www.revitapidocs.com/2025/e25fd057-595b-6121-c699-670850c7e5e5.htm",
 73219|       "dll_signature_verified": true,
 73220|       "dll_relationship_scope": "declared",
 73221|       "dll_semantic_verified": null,
 73222|       "dll_verified_status": "signature_verified_declared",
 73223|       "revitlookup_referenced": null,
 73224|       "revitlookup_requires_document_context": null
 73225|     },
 73226|     {
 73227|       "source": "Autodesk.Revit.DB.Events.ExternalDataInstanceRemovingFromDocumentEventArgs",
 73228|       "target": null,
 73229|       "member_name": "InstanceId",
 73230|       "member_kind": "property",
 73231|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 73232|       "confidence": "unknown_reference",
 73233|       "confidence_tier": "unverified_reference",
 73234|       "target_resolution": "none",
 73235|       "evidence": [
 73236|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 73237|       ],
 73238|       "source_url": "https://www.revitapidocs.com/2025/abd05aa9-aa4a-3cb2-48fa-f5f0243951f6.htm",
 73239|       "dll_signature_verified": true,
 73240|       "dll_relationship_scope": "declared",
 73241|       "dll_semantic_verified": null,
 73242|       "dll_verified_status": "signature_verified_declared",
 73243|       "revitlookup_referenced": null,
 73244|       "revitlookup_requires_document_context": null
 73245|     },
 73246|     {
 73247|       "source": "Autodesk.Revit.DB.Events.ExternalDataInstanceRemovingFromDocumentEventArgs",
 73248|       "target": null,
 73249|       "member_name": "TypeId",
 73250|       "member_kind": "property",
 73251|       "edge_type": "TYPE_OF",
 73252|       "confidence": "elementid_with_strong_name",
 73253|       "confidence_tier": "core",
 73254|       "target_resolution": "none",
 73255|       "evidence": [
 73256|         "member name 'TypeId' matches keyword pattern /^(Type|TypeId|GetTypeId)$/"
 73257|       ],
 73258|       "source_url": "https://www.revitapidocs.com/2025/58b38c79-648d-782f-e077-ea11e6907d95.htm",
 73259|       "dll_signature_verified": true,
 73260|       "dll_relationship_scope": "declared",
 73261|       "dll_semantic_verified": null,
 73262|       "dll_verified_status": "signature_verified_declared",
 73263|       "revitlookup_referenced": null,
 73264|       "revitlookup_requires_document_context": null
 73265|     },
 73266|     {
 73267|       "source": "Autodesk.Revit.DB.Events.FailuresProcessingEventArgs",
 73268|       "target": "Autodesk.Revit.DB.FailuresAccessor",
 73269|       "member_name": "GetFailuresAccessor",
 73270|       "member_kind": "method",
 73271|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 73272|       "confidence": "direct_return_type",
 73273|       "confidence_tier": "unverified_reference",
 73274|       "target_resolution": "exact",
 73275|       "evidence": [
 73276|         "return type 'FailuresAccessor' directly names a Revit DB object type"
 73277|       ],
 73278|       "source_url": "https://www.revitapidocs.com/2025/a47a6c09-80c9-8748-47e0-4142f149b8c6.htm",
 73279|       "dll_signature_verified": true,
 73280|       "dll_relationship_scope": "declared",
 73281|       "dll_semantic_verified": null,
 73282|       "dll_verified_status": "signature_verified_declared",
 73283|       "revitlookup_referenced": null,
 73284|       "revitlookup_requires_document_context": null
 73285|     },
 73286|     {
 73287|       "source": "Autodesk.Revit.DB.Events.FamilyLoadedIntoDocumentEventArgs",
 73288|       "target": null,
 73289|       "member_name": "NewFamilyId",
 73290|       "member_kind": "property",
 73291|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 73292|       "confidence": "unknown_reference",
 73293|       "confidence_tier": "unverified_reference",
 73294|       "target_resolution": "none",
 73295|       "evidence": [
 73296|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 73297|       ],
 73298|       "source_url": "https://www.revitapidocs.com/2025/98344b60-1514-1cc0-dbd2-0a8032d902d3.htm",
 73299|       "dll_signature_verified": true,
 73300|       "dll_relationship_scope": "declared",
 73301|       "dll_semantic_verified": null,
 73302|       "dll_verified_status": "signature_verified_declared",
 73303|       "revitlookup_referenced": null,
 73304|       "revitlookup_requires_document_context": null
 73305|     },
 73306|     {
 73307|       "source": "Autodesk.Revit.DB.Events.FamilyLoadedIntoDocumentEventArgs",
 73308|       "target": null,
 73309|       "member_name": "OriginalFamilyId",
 73310|       "member_kind": "property",
 73311|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 73312|       "confidence": "unknown_reference",
 73313|       "confidence_tier": "unverified_reference",
 73314|       "target_resolution": "none",
 73315|       "evidence": [
 73316|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 73317|       ],
 73318|       "source_url": "https://www.revitapidocs.com/2025/2ec3e15e-4496-4c6c-4fb5-3d056cd28680.htm",
 73319|       "dll_signature_verified": true,
 73320|       "dll_relationship_scope": "declared",
 73321|       "dll_semantic_verified": null,
 73322|       "dll_verified_status": "signature_verified_declared",
 73323|       "revitlookup_referenced": null,
 73324|       "revitlookup_requires_document_context": null
 73325|     },
 73326|     {
 73327|       "source": "Autodesk.Revit.DB.Events.FileImportedEventArgs",
 73328|       "target": null,
 73329|       "member_name": "ImportedInstanceId",
 73330|       "member_kind": "property",
```
