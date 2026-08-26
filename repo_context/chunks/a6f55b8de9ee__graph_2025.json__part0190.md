# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 190 of 216
- Original line range: 73711-74110
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
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
 73721|       "dll_semantic_verified": null,
 73722|       "dll_verified_status": "signature_verified_declared",
 73723|       "revitlookup_referenced": null,
 73724|       "revitlookup_requires_document_context": null
 73725|     },
 73726|     {
 73727|       "source": "Autodesk.Revit.DB.ExtensibleStorage.Schema",
 73728|       "target": "Autodesk.Revit.DB.ExtensibleStorage.Field",
 73729|       "member_name": "GetField",
 73730|       "member_kind": "method",
 73731|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 73732|       "confidence": "direct_return_type",
 73733|       "confidence_tier": "unverified_reference",
 73734|       "target_resolution": "short_name_fallback",
 73735|       "evidence": [
 73736|         "return type 'Field' directly names a Revit DB object type"
 73737|       ],
 73738|       "source_url": "https://www.revitapidocs.com/2025/e706cd01-bc50-9a3c-68c1-9bd4507c85e0.htm",
 73739|       "dll_signature_verified": true,
 73740|       "dll_relationship_scope": "declared",
 73741|       "dll_semantic_verified": null,
 73742|       "dll_verified_status": "signature_verified_declared",
 73743|       "revitlookup_referenced": null,
 73744|       "revitlookup_requires_document_context": null
 73745|     },
 73746|     {
 73747|       "source": "Autodesk.Revit.DB.ExtensibleStorage.Schema",
 73748|       "target": "Autodesk.Revit.DB.ExtensibleStorage.Field",
 73749|       "member_name": "ListFields",
 73750|       "member_kind": "method",
 73751|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 73752|       "confidence": "needs_runtime_validation",
 73753|       "confidence_tier": "needs_validation",
 73754|       "target_resolution": "short_name_fallback",
 73755|       "evidence": [
 73756|         "return type 'IList < Field >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 73757|       ],
 73758|       "source_url": "https://www.revitapidocs.com/2025/c1f24aca-c2a6-4a16-0440-2bd8296aa04e.htm",
 73759|       "dll_signature_verified": true,
 73760|       "dll_relationship_scope": "declared",
 73761|       "dll_semantic_verified": null,
 73762|       "dll_verified_status": "signature_verified_declared",
 73763|       "revitlookup_referenced": null,
 73764|       "revitlookup_requires_document_context": null
 73765|     },
 73766|     {
 73767|       "source": "Autodesk.Revit.DB.ExtensibleStorage.Schema",
 73768|       "target": "Autodesk.Revit.DB.ExtensibleStorage.Schema",
 73769|       "member_name": "ListSchemas",
 73770|       "member_kind": "method",
 73771|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 73772|       "confidence": "needs_runtime_validation",
 73773|       "confidence_tier": "needs_validation",
 73774|       "target_resolution": "exact",
 73775|       "evidence": [
 73776|         "return type 'IList < Schema >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 73777|       ],
 73778|       "source_url": "https://www.revitapidocs.com/2025/0f49289a-ca96-089e-a1c2-6f5bf80e29eb.htm",
 73779|       "dll_signature_verified": true,
 73780|       "dll_relationship_scope": "declared",
 73781|       "dll_semantic_verified": null,
 73782|       "dll_verified_status": "signature_verified_declared",
 73783|       "revitlookup_referenced": null,
 73784|       "revitlookup_requires_document_context": null
 73785|     },
 73786|     {
 73787|       "source": "Autodesk.Revit.DB.ExtensibleStorage.SchemaBuilder",
 73788|       "target": "Autodesk.Revit.DB.ExtensibleStorage.FieldBuilder",
 73789|       "member_name": "AddArrayField",
 73790|       "member_kind": "method",
 73791|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 73792|       "confidence": "direct_return_type",
 73793|       "confidence_tier": "unverified_reference",
 73794|       "target_resolution": "short_name_fallback",
 73795|       "evidence": [
 73796|         "return type 'FieldBuilder' directly names a Revit DB object type"
 73797|       ],
 73798|       "source_url": "https://www.revitapidocs.com/2025/f20f39f5-152c-98e9-32b7-b8c3bd575e4b.htm",
 73799|       "dll_signature_verified": true,
 73800|       "dll_relationship_scope": "declared",
 73801|       "dll_semantic_verified": null,
 73802|       "dll_verified_status": "signature_verified_declared",
 73803|       "revitlookup_referenced": null,
 73804|       "revitlookup_requires_document_context": null
 73805|     },
 73806|     {
 73807|       "source": "Autodesk.Revit.DB.ExtensibleStorage.SchemaBuilder",
 73808|       "target": "Autodesk.Revit.DB.ExtensibleStorage.FieldBuilder",
 73809|       "member_name": "AddMapField",
 73810|       "member_kind": "method",
 73811|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 73812|       "confidence": "direct_return_type",
 73813|       "confidence_tier": "unverified_reference",
 73814|       "target_resolution": "short_name_fallback",
 73815|       "evidence": [
 73816|         "return type 'FieldBuilder' directly names a Revit DB object type"
 73817|       ],
 73818|       "source_url": "https://www.revitapidocs.com/2025/ed30389b-a527-c867-3903-ce033f55552c.htm",
 73819|       "dll_signature_verified": true,
 73820|       "dll_relationship_scope": "declared",
 73821|       "dll_semantic_verified": null,
 73822|       "dll_verified_status": "signature_verified_declared",
 73823|       "revitlookup_referenced": null,
 73824|       "revitlookup_requires_document_context": null
 73825|     },
 73826|     {
 73827|       "source": "Autodesk.Revit.DB.ExtensibleStorage.SchemaBuilder",
 73828|       "target": "Autodesk.Revit.DB.ExtensibleStorage.FieldBuilder",
 73829|       "member_name": "AddSimpleField",
 73830|       "member_kind": "method",
 73831|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 73832|       "confidence": "direct_return_type",
 73833|       "confidence_tier": "unverified_reference",
 73834|       "target_resolution": "short_name_fallback",
 73835|       "evidence": [
 73836|         "return type 'FieldBuilder' directly names a Revit DB object type"
 73837|       ],
 73838|       "source_url": "https://www.revitapidocs.com/2025/5de0ea30-a58e-4db2-373c-05222a139465.htm",
 73839|       "dll_signature_verified": true,
 73840|       "dll_relationship_scope": "declared",
 73841|       "dll_semantic_verified": null,
 73842|       "dll_verified_status": "signature_verified_declared",
 73843|       "revitlookup_referenced": null,
 73844|       "revitlookup_requires_document_context": null
 73845|     },
 73846|     {
 73847|       "source": "Autodesk.Revit.DB.ExtensibleStorage.SchemaBuilder",
 73848|       "target": "Autodesk.Revit.DB.ExtensibleStorage.Schema",
 73849|       "member_name": "Finish",
 73850|       "member_kind": "method",
 73851|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 73852|       "confidence": "direct_return_type",
 73853|       "confidence_tier": "unverified_reference",
 73854|       "target_resolution": "exact",
 73855|       "evidence": [
 73856|         "return type 'Schema' directly names a Revit DB object type"
 73857|       ],
 73858|       "source_url": "https://www.revitapidocs.com/2025/399ce458-d43f-57a1-52f4-f862b243edec.htm",
 73859|       "dll_signature_verified": true,
 73860|       "dll_relationship_scope": "declared",
 73861|       "dll_semantic_verified": null,
 73862|       "dll_verified_status": "signature_verified_declared",
 73863|       "revitlookup_referenced": null,
 73864|       "revitlookup_requires_document_context": null
 73865|     },
 73866|     {
 73867|       "source": "Autodesk.Revit.DB.ExternalService.ExternalService",
 73868|       "target": "Autodesk.Revit.DB.ExternalService.ExternalServiceOptions",
 73869|       "member_name": "GetOptions",
 73870|       "member_kind": "method",
 73871|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 73872|       "confidence": "direct_return_type",
 73873|       "confidence_tier": "unverified_reference",
 73874|       "target_resolution": "short_name_fallback",
 73875|       "evidence": [
 73876|         "return type 'ExternalServiceOptions' directly names a Revit DB object type"
 73877|       ],
 73878|       "source_url": "https://www.revitapidocs.com/2025/492cc7a7-9493-732e-a6a7-fd00b3b85773.htm",
 73879|       "dll_signature_verified": true,
 73880|       "dll_relationship_scope": "declared",
 73881|       "dll_semantic_verified": null,
 73882|       "dll_verified_status": "signature_verified_declared",
 73883|       "revitlookup_referenced": null,
 73884|       "revitlookup_requires_document_context": null
 73885|     },
 73886|     {
 73887|       "source": "Autodesk.Revit.DB.ExternalService.ExternalService",
 73888|       "target": "Autodesk.Revit.DB.Guid",
 73889|       "member_name": "GetRegisteredServerIds",
 73890|       "member_kind": "method",
 73891|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 73892|       "confidence": "needs_runtime_validation",
 73893|       "confidence_tier": "needs_validation",
 73894|       "target_resolution": "external",
 73895|       "evidence": [
 73896|         "return type 'IList < Guid >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 73897|       ],
 73898|       "source_url": "https://www.revitapidocs.com/2025/230b50ac-8db7-cf62-2502-3cb0fd217b35.htm",
 73899|       "dll_signature_verified": true,
 73900|       "dll_relationship_scope": "declared",
 73901|       "dll_semantic_verified": null,
 73902|       "dll_verified_status": "signature_verified_declared",
 73903|       "revitlookup_referenced": null,
 73904|       "revitlookup_requires_document_context": null
 73905|     },
 73906|     {
 73907|       "source": "Autodesk.Revit.DB.ExternalService.ExternalService",
 73908|       "target": "Autodesk.Revit.DB.ExternalService.IExternalServer",
 73909|       "member_name": "GetServer",
 73910|       "member_kind": "method",
 73911|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 73912|       "confidence": "direct_return_type",
 73913|       "confidence_tier": "unverified_reference",
 73914|       "target_resolution": "short_name_fallback",
 73915|       "evidence": [
 73916|         "return type 'IExternalServer' directly names a Revit DB object type"
 73917|       ],
 73918|       "source_url": "https://www.revitapidocs.com/2025/839e6c3d-1f70-4668-781f-823baf005ff5.htm",
 73919|       "dll_signature_verified": true,
 73920|       "dll_relationship_scope": "declared",
 73921|       "dll_semantic_verified": null,
 73922|       "dll_verified_status": "signature_verified_declared",
 73923|       "revitlookup_referenced": true,
 73924|       "revitlookup_requires_document_context": false
 73925|     },
 73926|     {
 73927|       "source": "Autodesk.Revit.DB.ExternalService.ExternalServiceRegistry",
 73928|       "target": "Autodesk.Revit.DB.ExternalService.ExternalService",
 73929|       "member_name": "GetService",
 73930|       "member_kind": "method",
 73931|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 73932|       "confidence": "direct_return_type",
 73933|       "confidence_tier": "unverified_reference",
 73934|       "target_resolution": "short_name_fallback",
 73935|       "evidence": [
 73936|         "return type 'ExternalService' directly names a Revit DB object type"
 73937|       ],
 73938|       "source_url": "https://www.revitapidocs.com/2025/d12b0501-12a5-0d65-ac98-215c35dd0c0b.htm",
 73939|       "dll_signature_verified": true,
 73940|       "dll_relationship_scope": "declared",
 73941|       "dll_semantic_verified": null,
 73942|       "dll_verified_status": "signature_verified_declared",
 73943|       "revitlookup_referenced": null,
 73944|       "revitlookup_requires_document_context": null
 73945|     },
 73946|     {
 73947|       "source": "Autodesk.Revit.DB.ExternalService.ExternalServiceRegistry",
 73948|       "target": "Autodesk.Revit.DB.ExternalService.ExternalService",
 73949|       "member_name": "GetServices",
 73950|       "member_kind": "method",
 73951|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 73952|       "confidence": "needs_runtime_validation",
 73953|       "confidence_tier": "needs_validation",
 73954|       "target_resolution": "short_name_fallback",
 73955|       "evidence": [
 73956|         "return type 'IList < ExternalService >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 73957|       ],
 73958|       "source_url": "https://www.revitapidocs.com/2025/1c4ae954-af57-329b-f18d-f4f670b79eec.htm",
 73959|       "dll_signature_verified": true,
 73960|       "dll_relationship_scope": "declared",
 73961|       "dll_semantic_verified": null,
 73962|       "dll_verified_status": "signature_verified_declared",
 73963|       "revitlookup_referenced": null,
 73964|       "revitlookup_requires_document_context": null
 73965|     },
 73966|     {
 73967|       "source": "Autodesk.Revit.DB.ExternalService.MultiServerService",
 73968|       "target": "Autodesk.Revit.DB.Guid",
 73969|       "member_name": "GetActiveServerIds",
 73970|       "member_kind": "method",
 73971|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 73972|       "confidence": "needs_runtime_validation",
 73973|       "confidence_tier": "needs_validation",
 73974|       "target_resolution": "external",
 73975|       "evidence": [
 73976|         "return type 'IList < Guid >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 73977|       ],
 73978|       "source_url": "https://www.revitapidocs.com/2025/d3e87992-9ae7-7ad0-3e0b-0931d015b2d7.htm",
 73979|       "dll_signature_verified": true,
 73980|       "dll_relationship_scope": "declared",
 73981|       "dll_semantic_verified": null,
 73982|       "dll_verified_status": "signature_verified_declared",
 73983|       "revitlookup_referenced": null,
 73984|       "revitlookup_requires_document_context": null
 73985|     },
 73986|     {
 73987|       "source": "Autodesk.Revit.DB.ExternalService.MultiServerService",
 73988|       "target": "Autodesk.Revit.DB.Guid",
 73989|       "member_name": "GetActiveServerIds",
 73990|       "member_kind": "method",
 73991|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 73992|       "confidence": "needs_runtime_validation",
 73993|       "confidence_tier": "needs_validation",
 73994|       "target_resolution": "external",
 73995|       "evidence": [
 73996|         "return type 'IList < Guid >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 73997|       ],
 73998|       "source_url": "https://www.revitapidocs.com/2025/dcfcdc0f-8926-b6b5-8337-5b71bd6a8719.htm",
 73999|       "dll_signature_verified": true,
 74000|       "dll_relationship_scope": "declared",
 74001|       "dll_semantic_verified": null,
 74002|       "dll_verified_status": "signature_verified_declared",
 74003|       "revitlookup_referenced": null,
 74004|       "revitlookup_requires_document_context": null
 74005|     },
 74006|     {
 74007|       "source": "Autodesk.Revit.DB.Fabrication.DesignToFabricationConverter",
 74008|       "target": null,
 74009|       "member_name": "GetConvertedFabricationParts",
 74010|       "member_kind": "method",
 74011|       "edge_type": "RETURNS_ELEMENT_IDS",
 74012|       "confidence": "unknown_reference",
 74013|       "confidence_tier": "unverified_reference",
 74014|       "target_resolution": "none",
 74015|       "evidence": [
 74016|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 74017|       ],
 74018|       "source_url": "https://www.revitapidocs.com/2025/3461800e-90b9-2961-313d-ceb5c6e48b69.htm",
 74019|       "dll_signature_verified": true,
 74020|       "dll_relationship_scope": "declared",
 74021|       "dll_semantic_verified": null,
 74022|       "dll_verified_status": "signature_verified_declared",
 74023|       "revitlookup_referenced": null,
 74024|       "revitlookup_requires_document_context": null
 74025|     },
 74026|     {
 74027|       "source": "Autodesk.Revit.DB.Fabrication.DesignToFabricationConverter",
 74028|       "target": null,
 74029|       "member_name": "GetElementsWithOpenConnector",
 74030|       "member_kind": "method",
 74031|       "edge_type": "RETURNS_ELEMENT_IDS",
 74032|       "confidence": "unknown_reference",
 74033|       "confidence_tier": "unverified_reference",
 74034|       "target_resolution": "none",
 74035|       "evidence": [
 74036|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 74037|       ],
 74038|       "source_url": "https://www.revitapidocs.com/2025/1b8323a9-dd24-c818-e74c-e29b346000d3.htm",
 74039|       "dll_signature_verified": true,
 74040|       "dll_relationship_scope": "declared",
 74041|       "dll_semantic_verified": null,
 74042|       "dll_verified_status": "signature_verified_declared",
 74043|       "revitlookup_referenced": null,
 74044|       "revitlookup_requires_document_context": null
 74045|     },
 74046|     {
 74047|       "source": "Autodesk.Revit.DB.Fabrication.DesignToFabricationConverter",
 74048|       "target": "Autodesk.Revit.DB.Fabrication.PartialFailureResults",
 74049|       "member_name": "GetPartialConvertFailureResults",
 74050|       "member_kind": "method",
 74051|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74052|       "confidence": "needs_runtime_validation",
 74053|       "confidence_tier": "needs_validation",
 74054|       "target_resolution": "short_name_fallback",
 74055|       "evidence": [
 74056|         "return type 'IList < PartialFailureResults >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 74057|       ],
 74058|       "source_url": "https://www.revitapidocs.com/2025/fb8d7344-20ba-0b94-3fab-8855ebc76871.htm",
 74059|       "dll_signature_verified": true,
 74060|       "dll_relationship_scope": "declared",
 74061|       "dll_semantic_verified": null,
 74062|       "dll_verified_status": "signature_verified_declared",
 74063|       "revitlookup_referenced": null,
 74064|       "revitlookup_requires_document_context": null
 74065|     },
 74066|     {
 74067|       "source": "Autodesk.Revit.DB.Fabrication.FabricationNetworkChangeService",
 74068|       "target": null,
 74069|       "member_name": "GetElementsThatFailed",
 74070|       "member_kind": "method",
 74071|       "edge_type": "RETURNS_ELEMENT_IDS",
 74072|       "confidence": "unknown_reference",
 74073|       "confidence_tier": "unverified_reference",
 74074|       "target_resolution": "none",
 74075|       "evidence": [
 74076|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 74077|       ],
 74078|       "source_url": "https://www.revitapidocs.com/2025/7bc30db4-1cae-1acb-c346-d164d5b90822.htm",
 74079|       "dll_signature_verified": true,
 74080|       "dll_relationship_scope": "declared",
 74081|       "dll_semantic_verified": null,
 74082|       "dll_verified_status": "signature_verified_declared",
 74083|       "revitlookup_referenced": null,
 74084|       "revitlookup_requires_document_context": null
 74085|     },
 74086|     {
 74087|       "source": "Autodesk.Revit.DB.Fabrication.FabricationNetworkChangeService",
 74088|       "target": null,
 74089|       "member_name": "GetInLinePartTypes",
 74090|       "member_kind": "method",
 74091|       "edge_type": "RETURNS_ELEMENT_IDS",
 74092|       "confidence": "unknown_reference",
 74093|       "confidence_tier": "unverified_reference",
 74094|       "target_resolution": "none",
 74095|       "evidence": [
 74096|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 74097|       ],
 74098|       "source_url": "https://www.revitapidocs.com/2025/c7ae31f2-0158-7673-64a8-2b983f8b37bf.htm",
 74099|       "dll_signature_verified": true,
 74100|       "dll_relationship_scope": "declared",
 74101|       "dll_semantic_verified": null,
 74102|       "dll_verified_status": "signature_verified_declared",
 74103|       "revitlookup_referenced": null,
 74104|       "revitlookup_requires_document_context": null
 74105|     },
 74106|     {
 74107|       "source": "Autodesk.Revit.DB.Fabrication.FabricationNetworkChangeService",
 74108|       "target": "Autodesk.Revit.DB.Fabrication.FabricationPartSizeMap",
 74109|       "member_name": "GetMapOfAllSizesForStraights",
 74110|       "member_kind": "method",
```
