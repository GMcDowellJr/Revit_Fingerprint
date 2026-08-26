# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 189 of 216
- Original line range: 73321-73720
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
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
 73331|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 73332|       "confidence": "unknown_reference",
 73333|       "confidence_tier": "unverified_reference",
 73334|       "target_resolution": "none",
 73335|       "evidence": [
 73336|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 73337|       ],
 73338|       "source_url": "https://www.revitapidocs.com/2025/3966f998-181c-c215-d7c3-2c0abeba879d.htm",
 73339|       "dll_signature_verified": true,
 73340|       "dll_relationship_scope": "declared",
 73341|       "dll_semantic_verified": null,
 73342|       "dll_verified_status": "signature_verified_declared",
 73343|       "revitlookup_referenced": null,
 73344|       "revitlookup_requires_document_context": null
 73345|     },
 73346|     {
 73347|       "source": "Autodesk.Revit.DB.Events.LinkedResourceOpenedEventArgs",
 73348|       "target": null,
 73349|       "member_name": "ResourceTypeId",
 73350|       "member_kind": "property",
 73351|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 73352|       "confidence": "unknown_reference",
 73353|       "confidence_tier": "unverified_reference",
 73354|       "target_resolution": "none",
 73355|       "evidence": [
 73356|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 73357|       ],
 73358|       "source_url": "https://www.revitapidocs.com/2025/f2894a9c-a514-ef1c-0368-1f491f36d221.htm",
 73359|       "dll_signature_verified": true,
 73360|       "dll_relationship_scope": "declared",
 73361|       "dll_semantic_verified": null,
 73362|       "dll_verified_status": "signature_verified_declared",
 73363|       "revitlookup_referenced": null,
 73364|       "revitlookup_requires_document_context": null
 73365|     },
 73366|     {
 73367|       "source": "Autodesk.Revit.DB.Events.PostDocEventArgs",
 73368|       "target": "Autodesk.Revit.DB.Document",
 73369|       "member_name": "Document",
 73370|       "member_kind": "property",
 73371|       "edge_type": "REFERENCES",
 73372|       "confidence": "direct_return_type",
 73373|       "confidence_tier": "core",
 73374|       "target_resolution": "exact",
 73375|       "evidence": [
 73376|         "return type 'Document' directly names a Revit DB object type"
 73377|       ],
 73378|       "source_url": "https://www.revitapidocs.com/2025/b1f41f00-2a6f-99f6-c68e-7b51f82fedcb.htm",
 73379|       "dll_signature_verified": true,
 73380|       "dll_relationship_scope": "declared",
 73381|       "dll_semantic_verified": null,
 73382|       "dll_verified_status": "signature_verified_declared",
 73383|       "revitlookup_referenced": null,
 73384|       "revitlookup_requires_document_context": null
 73385|     },
 73386|     {
 73387|       "source": "Autodesk.Revit.DB.Events.PreDocEventArgs",
 73388|       "target": "Autodesk.Revit.DB.Document",
 73389|       "member_name": "Document",
 73390|       "member_kind": "property",
 73391|       "edge_type": "REFERENCES",
 73392|       "confidence": "direct_return_type",
 73393|       "confidence_tier": "core",
 73394|       "target_resolution": "exact",
 73395|       "evidence": [
 73396|         "return type 'Document' directly names a Revit DB object type"
 73397|       ],
 73398|       "source_url": "https://www.revitapidocs.com/2025/ce65e6e8-2bcb-20cf-3927-4eac5b1efdc1.htm",
 73399|       "dll_signature_verified": true,
 73400|       "dll_relationship_scope": "declared",
 73401|       "dll_semantic_verified": null,
 73402|       "dll_verified_status": "signature_verified_declared",
 73403|       "revitlookup_referenced": null,
 73404|       "revitlookup_requires_document_context": null
 73405|     },
 73406|     {
 73407|       "source": "Autodesk.Revit.DB.Events.ProgressChangedEventArgs",
 73408|       "target": null,
 73409|       "member_name": "Stage",
 73410|       "member_kind": "property",
 73411|       "edge_type": "TAGS_ELEMENT",
 73412|       "confidence": "name_only_candidate",
 73413|       "confidence_tier": "likely",
 73414|       "target_resolution": "none",
 73415|       "evidence": [
 73416|         "member name 'Stage' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'ProgressStage' gives no type-level confirmation"
 73417|       ],
 73418|       "source_url": "https://www.revitapidocs.com/2025/3d8ca3f6-2e0d-c45a-c58d-40170ea037d9.htm",
 73419|       "dll_signature_verified": true,
 73420|       "dll_relationship_scope": "declared",
 73421|       "dll_semantic_verified": null,
 73422|       "dll_verified_status": "signature_verified_declared",
 73423|       "revitlookup_referenced": null,
 73424|       "revitlookup_requires_document_context": null
 73425|     },
 73426|     {
 73427|       "source": "Autodesk.Revit.DB.Events.RevitAPIPostDocEventArgs",
 73428|       "target": "Autodesk.Revit.DB.Document",
 73429|       "member_name": "Document",
 73430|       "member_kind": "property",
 73431|       "edge_type": "REFERENCES",
 73432|       "confidence": "direct_return_type",
 73433|       "confidence_tier": "core",
 73434|       "target_resolution": "exact",
 73435|       "evidence": [
 73436|         "return type 'Document' directly names a Revit DB object type"
 73437|       ],
 73438|       "source_url": "https://www.revitapidocs.com/2025/b0a5235e-b2b3-0a29-799c-2ef535a51909.htm",
 73439|       "dll_signature_verified": true,
 73440|       "dll_relationship_scope": "declared",
 73441|       "dll_semantic_verified": null,
 73442|       "dll_verified_status": "signature_verified_declared",
 73443|       "revitlookup_referenced": null,
 73444|       "revitlookup_requires_document_context": null
 73445|     },
 73446|     {
 73447|       "source": "Autodesk.Revit.DB.Events.RevitAPIPreDocEventArgs",
 73448|       "target": "Autodesk.Revit.DB.Document",
 73449|       "member_name": "Document",
 73450|       "member_kind": "property",
 73451|       "edge_type": "REFERENCES",
 73452|       "confidence": "direct_return_type",
 73453|       "confidence_tier": "core",
 73454|       "target_resolution": "exact",
 73455|       "evidence": [
 73456|         "return type 'Document' directly names a Revit DB object type"
 73457|       ],
 73458|       "source_url": "https://www.revitapidocs.com/2025/ccbc5e47-3964-cf1e-4cac-fa023d3b8e63.htm",
 73459|       "dll_signature_verified": true,
 73460|       "dll_relationship_scope": "declared",
 73461|       "dll_semantic_verified": null,
 73462|       "dll_verified_status": "signature_verified_declared",
 73463|       "revitlookup_referenced": null,
 73464|       "revitlookup_requires_document_context": null
 73465|     },
 73466|     {
 73467|       "source": "Autodesk.Revit.DB.Events.ViewExportedEventArgs",
 73468|       "target": "Autodesk.Revit.DB.View",
 73469|       "member_name": "ViewId",
 73470|       "member_kind": "property",
 73471|       "edge_type": "REFERENCES",
 73472|       "confidence": "elementid_with_strong_name",
 73473|       "confidence_tier": "core",
 73474|       "target_resolution": "exact",
 73475|       "evidence": [
 73476|         "member name 'ViewId' matches keyword pattern /^(Get)?ViewId$/"
 73477|       ],
 73478|       "source_url": "https://www.revitapidocs.com/2025/3f63bfcc-a969-159f-93e7-377c49abcdd5.htm",
 73479|       "dll_signature_verified": true,
 73480|       "dll_relationship_scope": "declared",
 73481|       "dll_semantic_verified": null,
 73482|       "dll_verified_status": "signature_verified_declared",
 73483|       "revitlookup_referenced": null,
 73484|       "revitlookup_requires_document_context": null
 73485|     },
 73486|     {
 73487|       "source": "Autodesk.Revit.DB.Events.ViewExportingEventArgs",
 73488|       "target": "Autodesk.Revit.DB.View",
 73489|       "member_name": "ViewId",
 73490|       "member_kind": "property",
 73491|       "edge_type": "REFERENCES",
 73492|       "confidence": "elementid_with_strong_name",
 73493|       "confidence_tier": "core",
 73494|       "target_resolution": "exact",
 73495|       "evidence": [
 73496|         "member name 'ViewId' matches keyword pattern /^(Get)?ViewId$/"
 73497|       ],
 73498|       "source_url": "https://www.revitapidocs.com/2025/7dbf14b5-3001-86c5-a3ec-e492dd58c135.htm",
 73499|       "dll_signature_verified": true,
 73500|       "dll_relationship_scope": "declared",
 73501|       "dll_semantic_verified": null,
 73502|       "dll_verified_status": "signature_verified_declared",
 73503|       "revitlookup_referenced": null,
 73504|       "revitlookup_requires_document_context": null
 73505|     },
 73506|     {
 73507|       "source": "Autodesk.Revit.DB.Events.ViewPrintedEventArgs",
 73508|       "target": "Autodesk.Revit.DB.View",
 73509|       "member_name": "View",
 73510|       "member_kind": "property",
 73511|       "edge_type": "REFERENCES",
 73512|       "confidence": "direct_return_type",
 73513|       "confidence_tier": "core",
 73514|       "target_resolution": "exact",
 73515|       "evidence": [
 73516|         "return type 'View' directly names a Revit DB object type"
 73517|       ],
 73518|       "source_url": "https://www.revitapidocs.com/2025/4d2465d0-94b2-c3b5-b22d-50aa42113401.htm",
 73519|       "dll_signature_verified": true,
 73520|       "dll_relationship_scope": "declared",
 73521|       "dll_semantic_verified": null,
 73522|       "dll_verified_status": "signature_verified_declared",
 73523|       "revitlookup_referenced": null,
 73524|       "revitlookup_requires_document_context": null
 73525|     },
 73526|     {
 73527|       "source": "Autodesk.Revit.DB.Events.ViewPrintingEventArgs",
 73528|       "target": "Autodesk.Revit.DB.View",
 73529|       "member_name": "View",
 73530|       "member_kind": "property",
 73531|       "edge_type": "REFERENCES",
 73532|       "confidence": "direct_return_type",
 73533|       "confidence_tier": "core",
 73534|       "target_resolution": "exact",
 73535|       "evidence": [
 73536|         "return type 'View' directly names a Revit DB object type"
 73537|       ],
 73538|       "source_url": "https://www.revitapidocs.com/2025/b8d78c0e-31c4-0060-60a7-fb76f92da1bc.htm",
 73539|       "dll_signature_verified": true,
 73540|       "dll_relationship_scope": "declared",
 73541|       "dll_semantic_verified": null,
 73542|       "dll_verified_status": "signature_verified_declared",
 73543|       "revitlookup_referenced": null,
 73544|       "revitlookup_requires_document_context": null
 73545|     },
 73546|     {
 73547|       "source": "Autodesk.Revit.DB.Events.ViewPrintingEventArgs",
 73548|       "target": "Autodesk.Revit.DB.IPrintSetting",
 73549|       "member_name": "GetSettings",
 73550|       "member_kind": "method",
 73551|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 73552|       "confidence": "direct_return_type",
 73553|       "confidence_tier": "unverified_reference",
 73554|       "target_resolution": "exact",
 73555|       "evidence": [
 73556|         "return type 'IPrintSetting' directly names a Revit DB object type"
 73557|       ],
 73558|       "source_url": "https://www.revitapidocs.com/2025/f196b3b3-9786-974d-88ca-3f3c5e6245fd.htm",
 73559|       "dll_signature_verified": true,
 73560|       "dll_relationship_scope": "declared",
 73561|       "dll_semantic_verified": null,
 73562|       "dll_verified_status": "signature_verified_declared",
 73563|       "revitlookup_referenced": null,
 73564|       "revitlookup_requires_document_context": null
 73565|     },
 73566|     {
 73567|       "source": "Autodesk.Revit.DB.Events.ViewsExportedByContextEventArgs",
 73568|       "target": null,
 73569|       "member_name": "GetViewIds",
 73570|       "member_kind": "method",
 73571|       "edge_type": "RETURNS_ELEMENT_IDS",
 73572|       "confidence": "unknown_reference",
 73573|       "confidence_tier": "unverified_reference",
 73574|       "target_resolution": "none",
 73575|       "evidence": [
 73576|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 73577|       ],
 73578|       "source_url": "https://www.revitapidocs.com/2025/df6605f2-a998-0f9d-bc71-3171c1c966ae.htm",
 73579|       "dll_signature_verified": true,
 73580|       "dll_relationship_scope": "declared",
 73581|       "dll_semantic_verified": null,
 73582|       "dll_verified_status": "signature_verified_declared",
 73583|       "revitlookup_referenced": null,
 73584|       "revitlookup_requires_document_context": null
 73585|     },
 73586|     {
 73587|       "source": "Autodesk.Revit.DB.Events.ViewsExportingByContextEventArgs",
 73588|       "target": null,
 73589|       "member_name": "GetViewIds",
 73590|       "member_kind": "method",
 73591|       "edge_type": "RETURNS_ELEMENT_IDS",
 73592|       "confidence": "unknown_reference",
 73593|       "confidence_tier": "unverified_reference",
 73594|       "target_resolution": "none",
 73595|       "evidence": [
 73596|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 73597|       ],
 73598|       "source_url": "https://www.revitapidocs.com/2025/9fbae1ad-926b-f67a-716f-299bf13ddf4a.htm",
 73599|       "dll_signature_verified": true,
 73600|       "dll_relationship_scope": "declared",
 73601|       "dll_semantic_verified": null,
 73602|       "dll_verified_status": "signature_verified_declared",
 73603|       "revitlookup_referenced": null,
 73604|       "revitlookup_requires_document_context": null
 73605|     },
 73606|     {
 73607|       "source": "Autodesk.Revit.DB.Events.WorksharedOperationProgressChangedEventArgs",
 73608|       "target": "Autodesk.Revit.DB.Location",
 73609|       "member_name": "Location",
 73610|       "member_kind": "property",
 73611|       "edge_type": "REFERENCES",
 73612|       "confidence": "name_only_candidate",
 73613|       "confidence_tier": "likely",
 73614|       "target_resolution": "exact",
 73615|       "evidence": [
 73616|         "member name 'Location' matches keyword pattern /^Location$/ but return type 'string' gives no type-level confirmation"
 73617|       ],
 73618|       "source_url": "https://www.revitapidocs.com/2025/3fcaf146-8612-2046-e6f1-7fff92f4246b.htm",
 73619|       "dll_signature_verified": true,
 73620|       "dll_relationship_scope": "declared",
 73621|       "dll_semantic_verified": null,
 73622|       "dll_verified_status": "signature_verified_declared",
 73623|       "revitlookup_referenced": null,
 73624|       "revitlookup_requires_document_context": null
 73625|     },
 73626|     {
 73627|       "source": "Autodesk.Revit.DB.ExtensibleStorage.Entity",
 73628|       "target": "Autodesk.Revit.DB.ExtensibleStorage.Schema",
 73629|       "member_name": "Schema",
 73630|       "member_kind": "property",
 73631|       "edge_type": "REFERENCES",
 73632|       "confidence": "direct_return_type",
 73633|       "confidence_tier": "core",
 73634|       "target_resolution": "exact",
 73635|       "evidence": [
 73636|         "return type 'Schema' directly names a Revit DB object type"
 73637|       ],
 73638|       "source_url": "https://www.revitapidocs.com/2025/fe5fb340-9386-06b2-37d3-c587208d8ba6.htm",
 73639|       "dll_signature_verified": true,
 73640|       "dll_relationship_scope": "declared",
 73641|       "dll_semantic_verified": null,
 73642|       "dll_verified_status": "signature_verified_declared",
 73643|       "revitlookup_referenced": null,
 73644|       "revitlookup_requires_document_context": null
 73645|     },
 73646|     {
 73647|       "source": "Autodesk.Revit.DB.ExtensibleStorage.Field",
 73648|       "target": "Autodesk.Revit.DB.ExtensibleStorage.Schema",
 73649|       "member_name": "Schema",
 73650|       "member_kind": "property",
 73651|       "edge_type": "REFERENCES",
 73652|       "confidence": "direct_return_type",
 73653|       "confidence_tier": "core",
 73654|       "target_resolution": "exact",
 73655|       "evidence": [
 73656|         "return type 'Schema' directly names a Revit DB object type"
 73657|       ],
 73658|       "source_url": "https://www.revitapidocs.com/2025/31fb75ad-8cac-c473-0037-c802868aa6d5.htm",
 73659|       "dll_signature_verified": true,
 73660|       "dll_relationship_scope": "declared",
 73661|       "dll_semantic_verified": null,
 73662|       "dll_verified_status": "signature_verified_declared",
 73663|       "revitlookup_referenced": null,
 73664|       "revitlookup_requires_document_context": null
 73665|     },
 73666|     {
 73667|       "source": "Autodesk.Revit.DB.ExtensibleStorage.Field",
 73668|       "target": "Autodesk.Revit.DB.ExtensibleStorage.Schema",
 73669|       "member_name": "SubSchema",
 73670|       "member_kind": "property",
 73671|       "edge_type": "REFERENCES",
 73672|       "confidence": "direct_return_type",
 73673|       "confidence_tier": "core",
 73674|       "target_resolution": "exact",
 73675|       "evidence": [
 73676|         "return type 'Schema' directly names a Revit DB object type"
 73677|       ],
 73678|       "source_url": "https://www.revitapidocs.com/2025/1e4023ee-1b03-9617-db92-8ee3f6258f82.htm",
 73679|       "dll_signature_verified": true,
 73680|       "dll_relationship_scope": "declared",
 73681|       "dll_semantic_verified": null,
 73682|       "dll_verified_status": "signature_verified_declared",
 73683|       "revitlookup_referenced": null,
 73684|       "revitlookup_requires_document_context": null
 73685|     },
 73686|     {
 73687|       "source": "Autodesk.Revit.DB.ExtensibleStorage.Schema",
 73688|       "target": "Autodesk.Revit.DB.Level",
 73689|       "member_name": "ReadAccessLevel",
 73690|       "member_kind": "property",
 73691|       "edge_type": "ASSIGNED_TO_LEVEL",
 73692|       "confidence": "name_only_candidate",
 73693|       "confidence_tier": "likely",
 73694|       "target_resolution": "exact",
 73695|       "evidence": [
 73696|         "member name 'ReadAccessLevel' matches keyword pattern /Level/ but return type 'AccessLevel' gives no type-level confirmation"
 73697|       ],
 73698|       "source_url": "https://www.revitapidocs.com/2025/784a6bed-58cb-d3e2-84ea-971863f1ae37.htm",
 73699|       "dll_signature_verified": true,
 73700|       "dll_relationship_scope": "declared",
 73701|       "dll_semantic_verified": null,
 73702|       "dll_verified_status": "signature_verified_declared",
 73703|       "revitlookup_referenced": null,
 73704|       "revitlookup_requires_document_context": null
 73705|     },
 73706|     {
 73707|       "source": "Autodesk.Revit.DB.ExtensibleStorage.Schema",
 73708|       "target": "Autodesk.Revit.DB.Level",
 73709|       "member_name": "WriteAccessLevel",
 73710|       "member_kind": "property",
 73711|       "edge_type": "ASSIGNED_TO_LEVEL",
 73712|       "confidence": "name_only_candidate",
 73713|       "confidence_tier": "likely",
 73714|       "target_resolution": "exact",
 73715|       "evidence": [
 73716|         "member name 'WriteAccessLevel' matches keyword pattern /Level/ but return type 'AccessLevel' gives no type-level confirmation"
 73717|       ],
 73718|       "source_url": "https://www.revitapidocs.com/2025/d03286f0-aa98-d5c3-83e8-fffb245321e5.htm",
 73719|       "dll_signature_verified": true,
 73720|       "dll_relationship_scope": "declared",
```
