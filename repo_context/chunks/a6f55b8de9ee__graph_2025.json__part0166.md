# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 166 of 216
- Original line range: 64351-64750
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 64351|       "dll_verified_status": "signature_verified_declared",
 64352|       "revitlookup_referenced": null,
 64353|       "revitlookup_requires_document_context": null
 64354|     },
 64355|     {
 64356|       "source": "Autodesk.Revit.DB.WallFoundation",
 64357|       "target": null,
 64358|       "member_name": "WallId",
 64359|       "member_kind": "property",
 64360|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 64361|       "confidence": "unknown_reference",
 64362|       "confidence_tier": "unverified_reference",
 64363|       "target_resolution": "none",
 64364|       "evidence": [
 64365|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 64366|       ],
 64367|       "source_url": "https://www.revitapidocs.com/2025/01ace328-75ec-0fa9-d4a0-708136bbaaaa.htm",
 64368|       "dll_signature_verified": true,
 64369|       "dll_relationship_scope": "declared",
 64370|       "dll_semantic_verified": null,
 64371|       "dll_verified_status": "signature_verified_declared",
 64372|       "revitlookup_referenced": null,
 64373|       "revitlookup_requires_document_context": null
 64374|     },
 64375|     {
 64376|       "source": "Autodesk.Revit.DB.WallSweep",
 64377|       "target": null,
 64378|       "member_name": "GetHostIds",
 64379|       "member_kind": "method",
 64380|       "edge_type": "HOSTED_BY",
 64381|       "confidence": "elementid_collection_with_strong_name",
 64382|       "confidence_tier": "core",
 64383|       "target_resolution": "none",
 64384|       "evidence": [
 64385|         "member name 'GetHostIds' matches keyword pattern /^GetHosted|Host/"
 64386|       ],
 64387|       "source_url": "https://www.revitapidocs.com/2025/d1db8e46-2fea-5b9c-a5c1-cdc8c519a56e.htm",
 64388|       "dll_signature_verified": true,
 64389|       "dll_relationship_scope": "declared",
 64390|       "dll_semantic_verified": null,
 64391|       "dll_verified_status": "signature_verified_declared",
 64392|       "revitlookup_referenced": null,
 64393|       "revitlookup_requires_document_context": null
 64394|     },
 64395|     {
 64396|       "source": "Autodesk.Revit.DB.WallSweep",
 64397|       "target": "Autodesk.Revit.DB.WallSweepInfo",
 64398|       "member_name": "GetWallSweepInfo",
 64399|       "member_kind": "method",
 64400|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 64401|       "confidence": "direct_return_type",
 64402|       "confidence_tier": "unverified_reference",
 64403|       "target_resolution": "exact",
 64404|       "evidence": [
 64405|         "return type 'WallSweepInfo' directly names a Revit DB object type"
 64406|       ],
 64407|       "source_url": "https://www.revitapidocs.com/2025/112964bb-a1ae-1c0e-44af-d721e763c8e0.htm",
 64408|       "dll_signature_verified": true,
 64409|       "dll_relationship_scope": "declared",
 64410|       "dll_semantic_verified": null,
 64411|       "dll_verified_status": "signature_verified_declared",
 64412|       "revitlookup_referenced": null,
 64413|       "revitlookup_requires_document_context": null
 64414|     },
 64415|     {
 64416|       "source": "Autodesk.Revit.DB.WallSweepInfo",
 64417|       "target": "Autodesk.Revit.DB.Material",
 64418|       "member_name": "MaterialId",
 64419|       "member_kind": "property",
 64420|       "edge_type": "USES_MATERIAL",
 64421|       "confidence": "elementid_with_strong_name",
 64422|       "confidence_tier": "core",
 64423|       "target_resolution": "exact",
 64424|       "evidence": [
 64425|         "member name 'MaterialId' matches keyword pattern /Material/"
 64426|       ],
 64427|       "source_url": "https://www.revitapidocs.com/2025/18f06a02-b0d6-40cf-3682-a88db9fb086b.htm",
 64428|       "dll_signature_verified": true,
 64429|       "dll_relationship_scope": "declared",
 64430|       "dll_semantic_verified": null,
 64431|       "dll_verified_status": "signature_verified_declared",
 64432|       "revitlookup_referenced": null,
 64433|       "revitlookup_requires_document_context": null
 64434|     },
 64435|     {
 64436|       "source": "Autodesk.Revit.DB.WallSweepInfo",
 64437|       "target": null,
 64438|       "member_name": "ProfileId",
 64439|       "member_kind": "property",
 64440|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 64441|       "confidence": "unknown_reference",
 64442|       "confidence_tier": "unverified_reference",
 64443|       "target_resolution": "none",
 64444|       "evidence": [
 64445|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 64446|       ],
 64447|       "source_url": "https://www.revitapidocs.com/2025/cb72ef85-5bb7-a89b-f054-8af475455958.htm",
 64448|       "dll_signature_verified": true,
 64449|       "dll_relationship_scope": "declared",
 64450|       "dll_semantic_verified": null,
 64451|       "dll_verified_status": "signature_verified_declared",
 64452|       "revitlookup_referenced": null,
 64453|       "revitlookup_requires_document_context": null
 64454|     },
 64455|     {
 64456|       "source": "Autodesk.Revit.DB.WallType",
 64457|       "target": "Autodesk.Revit.DB.ThermalProperties",
 64458|       "member_name": "ThermalProperties",
 64459|       "member_kind": "property",
 64460|       "edge_type": "REFERENCES",
 64461|       "confidence": "direct_return_type",
 64462|       "confidence_tier": "core",
 64463|       "target_resolution": "exact",
 64464|       "evidence": [
 64465|         "return type 'ThermalProperties' directly names a Revit DB object type"
 64466|       ],
 64467|       "source_url": "https://www.revitapidocs.com/2025/d7024079-2d74-5758-4de7-ffb1c170f168.htm",
 64468|       "dll_signature_verified": true,
 64469|       "dll_relationship_scope": "declared",
 64470|       "dll_semantic_verified": null,
 64471|       "dll_verified_status": "signature_verified_declared",
 64472|       "revitlookup_referenced": null,
 64473|       "revitlookup_requires_document_context": null
 64474|     },
 64475|     {
 64476|       "source": "Autodesk.Revit.DB.WorksetDefaultVisibilitySettings",
 64477|       "target": "Autodesk.Revit.DB.Workset",
 64478|       "member_name": "IsWorksetVisible",
 64479|       "member_kind": "method",
 64480|       "edge_type": "OWNED_BY_WORKSET",
 64481|       "confidence": "name_only_candidate",
 64482|       "confidence_tier": "likely",
 64483|       "target_resolution": "exact",
 64484|       "evidence": [
 64485|         "member name 'IsWorksetVisible' matches keyword pattern /Workset/ but return type 'bool' gives no type-level confirmation"
 64486|       ],
 64487|       "source_url": "https://www.revitapidocs.com/2025/1b15a741-d1a0-e791-5d93-d59773ecc137.htm",
 64488|       "dll_signature_verified": true,
 64489|       "dll_relationship_scope": "declared",
 64490|       "dll_semantic_verified": null,
 64491|       "dll_verified_status": "signature_verified_declared",
 64492|       "revitlookup_referenced": null,
 64493|       "revitlookup_requires_document_context": null
 64494|     },
 64495|     {
 64496|       "source": "Autodesk.Revit.DB.WorksetDefaultVisibilitySettings",
 64497|       "target": "Autodesk.Revit.DB.Workset",
 64498|       "member_name": "SetWorksetVisibility",
 64499|       "member_kind": "method",
 64500|       "edge_type": "OWNED_BY_WORKSET",
 64501|       "confidence": "name_only_candidate",
 64502|       "confidence_tier": "likely",
 64503|       "target_resolution": "exact",
 64504|       "evidence": [
 64505|         "member name 'SetWorksetVisibility' matches keyword pattern /Workset/ but return type 'void' gives no type-level confirmation"
 64506|       ],
 64507|       "source_url": "https://www.revitapidocs.com/2025/cc477191-cc03-6dbf-48b7-fd08f83d75b6.htm",
 64508|       "dll_signature_verified": true,
 64509|       "dll_relationship_scope": "declared",
 64510|       "dll_semantic_verified": null,
 64511|       "dll_verified_status": "signature_verified_declared",
 64512|       "revitlookup_referenced": null,
 64513|       "revitlookup_requires_document_context": null
 64514|     },
 64515|     {
 64516|       "source": "Autodesk.Revit.DB.WorksetFilter",
 64517|       "target": "Autodesk.Revit.DB.Workset",
 64518|       "member_name": "IncludeStandaloneWorksetsOnly",
 64519|       "member_kind": "property",
 64520|       "edge_type": "OWNED_BY_WORKSET",
 64521|       "confidence": "name_only_candidate",
 64522|       "confidence_tier": "likely",
 64523|       "target_resolution": "exact",
 64524|       "evidence": [
 64525|         "member name 'IncludeStandaloneWorksetsOnly' matches keyword pattern /Workset/ but return type 'bool' gives no type-level confirmation"
 64526|       ],
 64527|       "source_url": "https://www.revitapidocs.com/2025/7e636a4d-7b22-1100-9527-5cbf82c5d0e1.htm",
 64528|       "dll_signature_verified": true,
 64529|       "dll_relationship_scope": "declared",
 64530|       "dll_semantic_verified": null,
 64531|       "dll_verified_status": "signature_verified_declared",
 64532|       "revitlookup_referenced": null,
 64533|       "revitlookup_requires_document_context": null
 64534|     },
 64535|     {
 64536|       "source": "Autodesk.Revit.DB.WorksetId",
 64537|       "target": "Autodesk.Revit.DB.Workset",
 64538|       "member_name": "InvalidWorksetId",
 64539|       "member_kind": "property",
 64540|       "edge_type": "OWNED_BY_WORKSET",
 64541|       "confidence": "direct_return_type",
 64542|       "confidence_tier": "core",
 64543|       "target_resolution": "exact",
 64544|       "evidence": [
 64545|         "return type 'WorksetId' is a typed identifier that unambiguously names its own target ('Workset') through the type system alone"
 64546|       ],
 64547|       "source_url": "https://www.revitapidocs.com/2025/0d2108ef-a296-8205-6487-9493085e4142.htm",
 64548|       "dll_signature_verified": true,
 64549|       "dll_relationship_scope": "declared",
 64550|       "dll_semantic_verified": null,
 64551|       "dll_verified_status": "signature_verified_declared",
 64552|       "revitlookup_referenced": null,
 64553|       "revitlookup_requires_document_context": null
 64554|     },
 64555|     {
 64556|       "source": "Autodesk.Revit.DB.WorksetKindFilter",
 64557|       "target": "Autodesk.Revit.DB.Workset",
 64558|       "member_name": "WorksetKind",
 64559|       "member_kind": "property",
 64560|       "edge_type": "OWNED_BY_WORKSET",
 64561|       "confidence": "name_only_candidate",
 64562|       "confidence_tier": "likely",
 64563|       "target_resolution": "exact",
 64564|       "evidence": [
 64565|         "member name 'WorksetKind' matches keyword pattern /Workset/ but return type 'WorksetKind' gives no type-level confirmation"
 64566|       ],
 64567|       "source_url": "https://www.revitapidocs.com/2025/865e3e7e-6761-3b44-2b44-d0f6fd03646b.htm",
 64568|       "dll_signature_verified": true,
 64569|       "dll_relationship_scope": "declared",
 64570|       "dll_semantic_verified": null,
 64571|       "dll_verified_status": "signature_verified_declared",
 64572|       "revitlookup_referenced": null,
 64573|       "revitlookup_requires_document_context": null
 64574|     },
 64575|     {
 64576|       "source": "Autodesk.Revit.DB.WorksetPreview",
 64577|       "target": "Autodesk.Revit.DB.Workset",
 64578|       "member_name": "Id",
 64579|       "member_kind": "property",
 64580|       "edge_type": "OWNED_BY_WORKSET",
 64581|       "confidence": "direct_return_type",
 64582|       "confidence_tier": "core",
 64583|       "target_resolution": "exact",
 64584|       "evidence": [
 64585|         "return type 'WorksetId' is a typed identifier that unambiguously names its own target ('Workset') through the type system alone"
 64586|       ],
 64587|       "source_url": "https://www.revitapidocs.com/2025/a0bd368d-c9ca-017f-63b1-0a811ed4598f.htm",
 64588|       "dll_signature_verified": true,
 64589|       "dll_relationship_scope": "declared",
 64590|       "dll_semantic_verified": null,
 64591|       "dll_verified_status": "signature_verified_declared",
 64592|       "revitlookup_referenced": null,
 64593|       "revitlookup_requires_document_context": null
 64594|     },
 64595|     {
 64596|       "source": "Autodesk.Revit.DB.WorksetPreview",
 64597|       "target": "Autodesk.Revit.DB.Workset",
 64598|       "member_name": "IsDefaultWorkset",
 64599|       "member_kind": "property",
 64600|       "edge_type": "OWNED_BY_WORKSET",
 64601|       "confidence": "name_only_candidate",
 64602|       "confidence_tier": "likely",
 64603|       "target_resolution": "exact",
 64604|       "evidence": [
 64605|         "member name 'IsDefaultWorkset' matches keyword pattern /Workset/ but return type 'bool' gives no type-level confirmation"
 64606|       ],
 64607|       "source_url": "https://www.revitapidocs.com/2025/a3359438-79eb-8930-8160-c68f23f5334f.htm",
 64608|       "dll_signature_verified": true,
 64609|       "dll_relationship_scope": "declared",
 64610|       "dll_semantic_verified": null,
 64611|       "dll_verified_status": "signature_verified_declared",
 64612|       "revitlookup_referenced": null,
 64613|       "revitlookup_requires_document_context": null
 64614|     },
 64615|     {
 64616|       "source": "Autodesk.Revit.DB.WorksetTable",
 64617|       "target": "Autodesk.Revit.DB.Workset",
 64618|       "member_name": "CanDeleteWorkset",
 64619|       "member_kind": "method",
 64620|       "edge_type": "OWNED_BY_WORKSET",
 64621|       "confidence": "name_only_candidate",
 64622|       "confidence_tier": "likely",
 64623|       "target_resolution": "exact",
 64624|       "evidence": [
 64625|         "member name 'CanDeleteWorkset' matches keyword pattern /Workset/ but return type 'bool' gives no type-level confirmation"
 64626|       ],
 64627|       "source_url": "https://www.revitapidocs.com/2025/6a120bcb-6b51-f8c4-2f59-e21b15c31b6a.htm",
 64628|       "dll_signature_verified": true,
 64629|       "dll_relationship_scope": "declared",
 64630|       "dll_semantic_verified": null,
 64631|       "dll_verified_status": "signature_verified_declared",
 64632|       "revitlookup_referenced": null,
 64633|       "revitlookup_requires_document_context": null
 64634|     },
 64635|     {
 64636|       "source": "Autodesk.Revit.DB.WorksetTable",
 64637|       "target": "Autodesk.Revit.DB.Workset",
 64638|       "member_name": "DeleteWorkset",
 64639|       "member_kind": "method",
 64640|       "edge_type": "OWNED_BY_WORKSET",
 64641|       "confidence": "name_only_candidate",
 64642|       "confidence_tier": "likely",
 64643|       "target_resolution": "exact",
 64644|       "evidence": [
 64645|         "member name 'DeleteWorkset' matches keyword pattern /Workset/ but return type 'void' gives no type-level confirmation"
 64646|       ],
 64647|       "source_url": "https://www.revitapidocs.com/2025/45c1d58c-f523-26ae-acc6-7ddc3c321d4a.htm",
 64648|       "dll_signature_verified": true,
 64649|       "dll_relationship_scope": "declared",
 64650|       "dll_semantic_verified": null,
 64651|       "dll_verified_status": "signature_verified_declared",
 64652|       "revitlookup_referenced": null,
 64653|       "revitlookup_requires_document_context": null
 64654|     },
 64655|     {
 64656|       "source": "Autodesk.Revit.DB.WorksetTable",
 64657|       "target": "Autodesk.Revit.DB.Workset",
 64658|       "member_name": "GetActiveWorksetId",
 64659|       "member_kind": "method",
 64660|       "edge_type": "OWNED_BY_WORKSET",
 64661|       "confidence": "direct_return_type",
 64662|       "confidence_tier": "core",
 64663|       "target_resolution": "exact",
 64664|       "evidence": [
 64665|         "return type 'WorksetId' is a typed identifier that unambiguously names its own target ('Workset') through the type system alone"
 64666|       ],
 64667|       "source_url": "https://www.revitapidocs.com/2025/4755e6d9-c31c-32cc-7cf5-aac19dc12dff.htm",
 64668|       "dll_signature_verified": true,
 64669|       "dll_relationship_scope": "declared",
 64670|       "dll_semantic_verified": null,
 64671|       "dll_verified_status": "signature_verified_declared",
 64672|       "revitlookup_referenced": null,
 64673|       "revitlookup_requires_document_context": null
 64674|     },
 64675|     {
 64676|       "source": "Autodesk.Revit.DB.WorksetTable",
 64677|       "target": "Autodesk.Revit.DB.Workset",
 64678|       "member_name": "GetWorkset",
 64679|       "member_kind": "method",
 64680|       "edge_type": "OWNED_BY_WORKSET",
 64681|       "confidence": "direct_return_type",
 64682|       "confidence_tier": "core",
 64683|       "target_resolution": "exact",
 64684|       "evidence": [
 64685|         "return type 'Workset' directly names a Revit DB object type"
 64686|       ],
 64687|       "source_url": "https://www.revitapidocs.com/2025/55244a65-68b3-0c65-1282-f3c338f052ed.htm",
 64688|       "dll_signature_verified": true,
 64689|       "dll_relationship_scope": "declared",
 64690|       "dll_semantic_verified": null,
 64691|       "dll_verified_status": "signature_verified_declared",
 64692|       "revitlookup_referenced": true,
 64693|       "revitlookup_requires_document_context": true
 64694|     },
 64695|     {
 64696|       "source": "Autodesk.Revit.DB.WorksetTable",
 64697|       "target": "Autodesk.Revit.DB.Workset",
 64698|       "member_name": "GetWorkset",
 64699|       "member_kind": "method",
 64700|       "edge_type": "OWNED_BY_WORKSET",
 64701|       "confidence": "direct_return_type",
 64702|       "confidence_tier": "core",
 64703|       "target_resolution": "exact",
 64704|       "evidence": [
 64705|         "return type 'Workset' directly names a Revit DB object type"
 64706|       ],
 64707|       "source_url": "https://www.revitapidocs.com/2025/229ca8bb-5356-2d95-1e4b-5d3557092647.htm",
 64708|       "dll_signature_verified": true,
 64709|       "dll_relationship_scope": "declared",
 64710|       "dll_semantic_verified": null,
 64711|       "dll_verified_status": "signature_verified_declared",
 64712|       "revitlookup_referenced": true,
 64713|       "revitlookup_requires_document_context": true
 64714|     },
 64715|     {
 64716|       "source": "Autodesk.Revit.DB.WorksetTable",
 64717|       "target": "Autodesk.Revit.DB.Workset",
 64718|       "member_name": "IsWorksetNameUnique",
 64719|       "member_kind": "method",
 64720|       "edge_type": "OWNED_BY_WORKSET",
 64721|       "confidence": "name_only_candidate",
 64722|       "confidence_tier": "likely",
 64723|       "target_resolution": "exact",
 64724|       "evidence": [
 64725|         "member name 'IsWorksetNameUnique' matches keyword pattern /Workset/ but return type 'bool' gives no type-level confirmation"
 64726|       ],
 64727|       "source_url": "https://www.revitapidocs.com/2025/6728440e-41db-179d-2b5c-1184f8decf8d.htm",
 64728|       "dll_signature_verified": true,
 64729|       "dll_relationship_scope": "declared",
 64730|       "dll_semantic_verified": null,
 64731|       "dll_verified_status": "signature_verified_declared",
 64732|       "revitlookup_referenced": null,
 64733|       "revitlookup_requires_document_context": null
 64734|     },
 64735|     {
 64736|       "source": "Autodesk.Revit.DB.WorksetTable",
 64737|       "target": "Autodesk.Revit.DB.Workset",
 64738|       "member_name": "RenameWorkset",
 64739|       "member_kind": "method",
 64740|       "edge_type": "OWNED_BY_WORKSET",
 64741|       "confidence": "name_only_candidate",
 64742|       "confidence_tier": "likely",
 64743|       "target_resolution": "exact",
 64744|       "evidence": [
 64745|         "member name 'RenameWorkset' matches keyword pattern /Workset/ but return type 'void' gives no type-level confirmation"
 64746|       ],
 64747|       "source_url": "https://www.revitapidocs.com/2025/aa6f8625-cf32-cad1-bf9a-eec33abab957.htm",
 64748|       "dll_signature_verified": true,
 64749|       "dll_relationship_scope": "declared",
 64750|       "dll_semantic_verified": null,
```
