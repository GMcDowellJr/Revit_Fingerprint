# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 103 of 216
- Original line range: 39781-40180
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 39781|       "confidence_tier": "unverified_reference",
 39782|       "target_resolution": "none",
 39783|       "evidence": [
 39784|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 39785|       ],
 39786|       "source_url": "https://www.revitapidocs.com/2025/9235095b-b7ae-b6e5-6cc2-2b8d397644de.htm",
 39787|       "dll_signature_verified": true,
 39788|       "dll_relationship_scope": "declared",
 39789|       "dll_semantic_verified": null,
 39790|       "dll_verified_status": "signature_verified_declared",
 39791|       "revitlookup_referenced": null,
 39792|       "revitlookup_requires_document_context": null
 39793|     },
 39794|     {
 39795|       "source": "Autodesk.Revit.DB.Element",
 39796|       "target": "Autodesk.Revit.DB.Level",
 39797|       "member_name": "LevelId",
 39798|       "member_kind": "property",
 39799|       "edge_type": "ASSIGNED_TO_LEVEL",
 39800|       "confidence": "elementid_with_strong_name",
 39801|       "confidence_tier": "core",
 39802|       "target_resolution": "exact",
 39803|       "evidence": [
 39804|         "member name 'LevelId' matches keyword pattern /Level/"
 39805|       ],
 39806|       "source_url": "https://www.revitapidocs.com/2025/27033fe3-6740-61e3-be82-47a6b8ae77db.htm",
 39807|       "dll_signature_verified": true,
 39808|       "dll_relationship_scope": "declared",
 39809|       "dll_semantic_verified": null,
 39810|       "dll_verified_status": "signature_verified_declared",
 39811|       "revitlookup_referenced": null,
 39812|       "revitlookup_requires_document_context": null
 39813|     },
 39814|     {
 39815|       "source": "Autodesk.Revit.DB.Element",
 39816|       "target": "Autodesk.Revit.DB.Location",
 39817|       "member_name": "Location",
 39818|       "member_kind": "property",
 39819|       "edge_type": "REFERENCES",
 39820|       "confidence": "direct_return_type",
 39821|       "confidence_tier": "core",
 39822|       "target_resolution": "exact",
 39823|       "evidence": [
 39824|         "return type 'Location' directly names a Revit DB object type"
 39825|       ],
 39826|       "source_url": "https://www.revitapidocs.com/2025/89438f4f-7e15-835a-0c66-d6adbc8dd00c.htm",
 39827|       "dll_signature_verified": true,
 39828|       "dll_relationship_scope": "declared",
 39829|       "dll_semantic_verified": null,
 39830|       "dll_verified_status": "signature_verified_declared",
 39831|       "revitlookup_referenced": null,
 39832|       "revitlookup_requires_document_context": null
 39833|     },
 39834|     {
 39835|       "source": "Autodesk.Revit.DB.Element",
 39836|       "target": null,
 39837|       "member_name": "OwnerViewId",
 39838|       "member_kind": "property",
 39839|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 39840|       "confidence": "unknown_reference",
 39841|       "confidence_tier": "unverified_reference",
 39842|       "target_resolution": "none",
 39843|       "evidence": [
 39844|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 39845|       ],
 39846|       "source_url": "https://www.revitapidocs.com/2025/174c1adf-0be8-a4b0-41f3-9e3ea1d6b1f1.htm",
 39847|       "dll_signature_verified": true,
 39848|       "dll_relationship_scope": "declared",
 39849|       "dll_semantic_verified": null,
 39850|       "dll_verified_status": "signature_verified_declared",
 39851|       "revitlookup_referenced": null,
 39852|       "revitlookup_requires_document_context": null
 39853|     },
 39854|     {
 39855|       "source": "Autodesk.Revit.DB.Element",
 39856|       "target": "Autodesk.Revit.DB.ParameterSet",
 39857|       "member_name": "Parameters",
 39858|       "member_kind": "property",
 39859|       "edge_type": "HAS_PARAMETER",
 39860|       "confidence": "direct_return_type",
 39861|       "confidence_tier": "core",
 39862|       "target_resolution": "exact",
 39863|       "evidence": [
 39864|         "return type 'ParameterSet' directly names a Revit DB object type"
 39865|       ],
 39866|       "source_url": "https://www.revitapidocs.com/2025/7af5d66f-4533-33d2-dd82-d9573eaabf15.htm",
 39867|       "dll_signature_verified": true,
 39868|       "dll_relationship_scope": "declared",
 39869|       "dll_semantic_verified": null,
 39870|       "dll_verified_status": "signature_verified_declared",
 39871|       "revitlookup_referenced": null,
 39872|       "revitlookup_requires_document_context": null
 39873|     },
 39874|     {
 39875|       "source": "Autodesk.Revit.DB.Element",
 39876|       "target": "Autodesk.Revit.DB.ParameterMap",
 39877|       "member_name": "ParametersMap",
 39878|       "member_kind": "property",
 39879|       "edge_type": "HAS_PARAMETER",
 39880|       "confidence": "direct_return_type",
 39881|       "confidence_tier": "core",
 39882|       "target_resolution": "exact",
 39883|       "evidence": [
 39884|         "return type 'ParameterMap' directly names a Revit DB object type"
 39885|       ],
 39886|       "source_url": "https://www.revitapidocs.com/2025/82c45482-a018-32e4-d8e5-9751e10ffeb9.htm",
 39887|       "dll_signature_verified": true,
 39888|       "dll_relationship_scope": "declared",
 39889|       "dll_semantic_verified": null,
 39890|       "dll_verified_status": "signature_verified_declared",
 39891|       "revitlookup_referenced": null,
 39892|       "revitlookup_requires_document_context": null
 39893|     },
 39894|     {
 39895|       "source": "Autodesk.Revit.DB.Element",
 39896|       "target": "Autodesk.Revit.DB.Workset",
 39897|       "member_name": "WorksetId",
 39898|       "member_kind": "property",
 39899|       "edge_type": "OWNED_BY_WORKSET",
 39900|       "confidence": "direct_return_type",
 39901|       "confidence_tier": "core",
 39902|       "target_resolution": "exact",
 39903|       "evidence": [
 39904|         "return type 'WorksetId' is a typed identifier that unambiguously names its own target ('Workset') through the type system alone"
 39905|       ],
 39906|       "source_url": "https://www.revitapidocs.com/2025/4b45250a-7a07-a89a-0f63-cf8d142a7b93.htm",
 39907|       "dll_signature_verified": true,
 39908|       "dll_relationship_scope": "declared",
 39909|       "dll_semantic_verified": null,
 39910|       "dll_verified_status": "signature_verified_declared",
 39911|       "revitlookup_referenced": null,
 39912|       "revitlookup_requires_document_context": null
 39913|     },
 39914|     {
 39915|       "source": "Autodesk.Revit.DB.Element",
 39916|       "target": "Autodesk.Revit.DB.Phase",
 39917|       "member_name": "ArePhasesModifiable",
 39918|       "member_kind": "method",
 39919|       "edge_type": "ASSIGNED_TO_PHASE",
 39920|       "confidence": "name_only_candidate",
 39921|       "confidence_tier": "likely",
 39922|       "target_resolution": "exact",
 39923|       "evidence": [
 39924|         "member name 'ArePhasesModifiable' matches keyword pattern /Phase/ but return type 'bool' gives no type-level confirmation"
 39925|       ],
 39926|       "source_url": "https://www.revitapidocs.com/2025/329b02eb-5ee4-1715-2fbf-2cbbc0d3ff2a.htm",
 39927|       "dll_signature_verified": true,
 39928|       "dll_relationship_scope": "declared",
 39929|       "dll_semantic_verified": null,
 39930|       "dll_verified_status": "signature_verified_declared",
 39931|       "revitlookup_referenced": null,
 39932|       "revitlookup_requires_document_context": null
 39933|     },
 39934|     {
 39935|       "source": "Autodesk.Revit.DB.Element",
 39936|       "target": null,
 39937|       "member_name": "ChangeTypeId",
 39938|       "member_kind": "method",
 39939|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 39940|       "confidence": "unknown_reference",
 39941|       "confidence_tier": "unverified_reference",
 39942|       "target_resolution": "none",
 39943|       "evidence": [
 39944|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 39945|       ],
 39946|       "source_url": "https://www.revitapidocs.com/2025/479b5d94-abd3-db42-27d7-6a3eda12f285.htm",
 39947|       "dll_signature_verified": true,
 39948|       "dll_relationship_scope": "declared",
 39949|       "dll_semantic_verified": null,
 39950|       "dll_verified_status": "signature_verified_declared",
 39951|       "revitlookup_referenced": null,
 39952|       "revitlookup_requires_document_context": null
 39953|     },
 39954|     {
 39955|       "source": "Autodesk.Revit.DB.Element",
 39956|       "target": "Autodesk.Revit.DB.EvaluatedParameter",
 39957|       "member_name": "EvaluateAllParameterValues",
 39958|       "member_kind": "method",
 39959|       "edge_type": "HAS_PARAMETER",
 39960|       "confidence": "needs_runtime_validation",
 39961|       "confidence_tier": "needs_validation",
 39962|       "target_resolution": "exact",
 39963|       "evidence": [
 39964|         "return type 'IList < EvaluatedParameter >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 39965|       ],
 39966|       "source_url": "https://www.revitapidocs.com/2025/5250da77-1e16-13c6-fed6-5ef29997e6f9.htm",
 39967|       "dll_signature_verified": true,
 39968|       "dll_relationship_scope": "declared",
 39969|       "dll_semantic_verified": null,
 39970|       "dll_verified_status": "signature_verified_declared",
 39971|       "revitlookup_referenced": null,
 39972|       "revitlookup_requires_document_context": null
 39973|     },
 39974|     {
 39975|       "source": "Autodesk.Revit.DB.Element",
 39976|       "target": "Autodesk.Revit.DB.EvaluatedParameter",
 39977|       "member_name": "EvaluateParameterValues",
 39978|       "member_kind": "method",
 39979|       "edge_type": "HAS_PARAMETER",
 39980|       "confidence": "needs_runtime_validation",
 39981|       "confidence_tier": "needs_validation",
 39982|       "target_resolution": "exact",
 39983|       "evidence": [
 39984|         "return type 'IList < EvaluatedParameter >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 39985|       ],
 39986|       "source_url": "https://www.revitapidocs.com/2025/1a6ca65f-09d9-a4e6-9365-3ed64e3097fc.htm",
 39987|       "dll_signature_verified": true,
 39988|       "dll_relationship_scope": "declared",
 39989|       "dll_semantic_verified": null,
 39990|       "dll_verified_status": "signature_verified_declared",
 39991|       "revitlookup_referenced": null,
 39992|       "revitlookup_requires_document_context": null
 39993|     },
 39994|     {
 39995|       "source": "Autodesk.Revit.DB.Element",
 39996|       "target": "Autodesk.Revit.DB.ChangeType",
 39997|       "member_name": "GetChangeTypeAny",
 39998|       "member_kind": "method",
 39999|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 40000|       "confidence": "direct_return_type",
 40001|       "confidence_tier": "unverified_reference",
 40002|       "target_resolution": "exact",
 40003|       "evidence": [
 40004|         "return type 'ChangeType' directly names a Revit DB object type"
 40005|       ],
 40006|       "source_url": "https://www.revitapidocs.com/2025/e9a0bce4-b289-1ea1-05d0-c0fc2943f8dd.htm",
 40007|       "dll_signature_verified": true,
 40008|       "dll_relationship_scope": "declared",
 40009|       "dll_semantic_verified": null,
 40010|       "dll_verified_status": "signature_verified_declared",
 40011|       "revitlookup_referenced": null,
 40012|       "revitlookup_requires_document_context": null
 40013|     },
 40014|     {
 40015|       "source": "Autodesk.Revit.DB.Element",
 40016|       "target": "Autodesk.Revit.DB.ChangeType",
 40017|       "member_name": "GetChangeTypeElementAddition",
 40018|       "member_kind": "method",
 40019|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 40020|       "confidence": "direct_return_type",
 40021|       "confidence_tier": "unverified_reference",
 40022|       "target_resolution": "exact",
 40023|       "evidence": [
 40024|         "return type 'ChangeType' directly names a Revit DB object type"
 40025|       ],
 40026|       "source_url": "https://www.revitapidocs.com/2025/9f7a0758-21b5-bba6-5d26-9e1f40d29f7f.htm",
 40027|       "dll_signature_verified": true,
 40028|       "dll_relationship_scope": "declared",
 40029|       "dll_semantic_verified": null,
 40030|       "dll_verified_status": "signature_verified_declared",
 40031|       "revitlookup_referenced": null,
 40032|       "revitlookup_requires_document_context": null
 40033|     },
 40034|     {
 40035|       "source": "Autodesk.Revit.DB.Element",
 40036|       "target": "Autodesk.Revit.DB.ChangeType",
 40037|       "member_name": "GetChangeTypeElementDeletion",
 40038|       "member_kind": "method",
 40039|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 40040|       "confidence": "direct_return_type",
 40041|       "confidence_tier": "unverified_reference",
 40042|       "target_resolution": "exact",
 40043|       "evidence": [
 40044|         "return type 'ChangeType' directly names a Revit DB object type"
 40045|       ],
 40046|       "source_url": "https://www.revitapidocs.com/2025/d2f0d0dd-d01b-3296-8248-068baec486cf.htm",
 40047|       "dll_signature_verified": true,
 40048|       "dll_relationship_scope": "declared",
 40049|       "dll_semantic_verified": null,
 40050|       "dll_verified_status": "signature_verified_declared",
 40051|       "revitlookup_referenced": null,
 40052|       "revitlookup_requires_document_context": null
 40053|     },
 40054|     {
 40055|       "source": "Autodesk.Revit.DB.Element",
 40056|       "target": "Autodesk.Revit.DB.ChangeType",
 40057|       "member_name": "GetChangeTypeGeometry",
 40058|       "member_kind": "method",
 40059|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 40060|       "confidence": "direct_return_type",
 40061|       "confidence_tier": "unverified_reference",
 40062|       "target_resolution": "exact",
 40063|       "evidence": [
 40064|         "return type 'ChangeType' directly names a Revit DB object type"
 40065|       ],
 40066|       "source_url": "https://www.revitapidocs.com/2025/45751c5b-6d10-657a-a017-04219d1a5ac8.htm",
 40067|       "dll_signature_verified": true,
 40068|       "dll_relationship_scope": "declared",
 40069|       "dll_semantic_verified": null,
 40070|       "dll_verified_status": "signature_verified_declared",
 40071|       "revitlookup_referenced": null,
 40072|       "revitlookup_requires_document_context": null
 40073|     },
 40074|     {
 40075|       "source": "Autodesk.Revit.DB.Element",
 40076|       "target": "Autodesk.Revit.DB.ChangeType",
 40077|       "member_name": "GetChangeTypeParameter",
 40078|       "member_kind": "method",
 40079|       "edge_type": "HAS_PARAMETER",
 40080|       "confidence": "direct_return_type",
 40081|       "confidence_tier": "core",
 40082|       "target_resolution": "exact",
 40083|       "evidence": [
 40084|         "return type 'ChangeType' directly names a Revit DB object type"
 40085|       ],
 40086|       "source_url": "https://www.revitapidocs.com/2025/6b0460f6-8db3-970c-d2d9-a1b5e470eb1e.htm",
 40087|       "dll_signature_verified": true,
 40088|       "dll_relationship_scope": "declared",
 40089|       "dll_semantic_verified": null,
 40090|       "dll_verified_status": "signature_verified_declared",
 40091|       "revitlookup_referenced": null,
 40092|       "revitlookup_requires_document_context": null
 40093|     },
 40094|     {
 40095|       "source": "Autodesk.Revit.DB.Element",
 40096|       "target": "Autodesk.Revit.DB.ChangeType",
 40097|       "member_name": "GetChangeTypeParameter",
 40098|       "member_kind": "method",
 40099|       "edge_type": "HAS_PARAMETER",
 40100|       "confidence": "direct_return_type",
 40101|       "confidence_tier": "core",
 40102|       "target_resolution": "exact",
 40103|       "evidence": [
 40104|         "return type 'ChangeType' directly names a Revit DB object type"
 40105|       ],
 40106|       "source_url": "https://www.revitapidocs.com/2025/19ee7026-0e04-5bd2-b046-b14b59d4bc4e.htm",
 40107|       "dll_signature_verified": true,
 40108|       "dll_relationship_scope": "declared",
 40109|       "dll_semantic_verified": null,
 40110|       "dll_verified_status": "signature_verified_declared",
 40111|       "revitlookup_referenced": null,
 40112|       "revitlookup_requires_document_context": null
 40113|     },
 40114|     {
 40115|       "source": "Autodesk.Revit.DB.Element",
 40116|       "target": null,
 40117|       "member_name": "GetDependentElements",
 40118|       "member_kind": "method",
 40119|       "edge_type": "DEPENDS_ON",
 40120|       "confidence": "elementid_collection_with_strong_name",
 40121|       "confidence_tier": "core",
 40122|       "target_resolution": "none",
 40123|       "evidence": [
 40124|         "member name 'GetDependentElements' matches keyword pattern /^GetDependent|Dependent/"
 40125|       ],
 40126|       "source_url": "https://www.revitapidocs.com/2025/56e875d3-014b-a996-69c3-e6ed9b885f5c.htm",
 40127|       "dll_signature_verified": true,
 40128|       "dll_relationship_scope": "declared",
 40129|       "dll_semantic_verified": null,
 40130|       "dll_verified_status": "signature_verified_declared",
 40131|       "revitlookup_referenced": true,
 40132|       "revitlookup_requires_document_context": false
 40133|     },
 40134|     {
 40135|       "source": "Autodesk.Revit.DB.Element",
 40136|       "target": "Autodesk.Revit.DB.ExtensibleStorage.Entity",
 40137|       "member_name": "GetEntity",
 40138|       "member_kind": "method",
 40139|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 40140|       "confidence": "direct_return_type",
 40141|       "confidence_tier": "unverified_reference",
 40142|       "target_resolution": "short_name_fallback",
 40143|       "evidence": [
 40144|         "return type 'Entity' directly names a Revit DB object type"
 40145|       ],
 40146|       "source_url": "https://www.revitapidocs.com/2025/09d80bf1-c1d0-aa2e-4f18-e5a5e9c9d93f.htm",
 40147|       "dll_signature_verified": true,
 40148|       "dll_relationship_scope": "declared",
 40149|       "dll_semantic_verified": null,
 40150|       "dll_verified_status": "signature_verified_declared",
 40151|       "revitlookup_referenced": true,
 40152|       "revitlookup_requires_document_context": true
 40153|     },
 40154|     {
 40155|       "source": "Autodesk.Revit.DB.Element",
 40156|       "target": "Autodesk.Revit.DB.Guid",
 40157|       "member_name": "GetEntitySchemaGuids",
 40158|       "member_kind": "method",
 40159|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 40160|       "confidence": "needs_runtime_validation",
 40161|       "confidence_tier": "needs_validation",
 40162|       "target_resolution": "external",
 40163|       "evidence": [
 40164|         "return type 'IList < Guid >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 40165|       ],
 40166|       "source_url": "https://www.revitapidocs.com/2025/742313cb-1bea-f873-e5ca-1bfac782286b.htm",
 40167|       "dll_signature_verified": true,
 40168|       "dll_relationship_scope": "declared",
 40169|       "dll_semantic_verified": null,
 40170|       "dll_verified_status": "signature_verified_declared",
 40171|       "revitlookup_referenced": null,
 40172|       "revitlookup_requires_document_context": null
 40173|     },
 40174|     {
 40175|       "source": "Autodesk.Revit.DB.Element",
 40176|       "target": "Autodesk.Revit.DB.ExternalFileReference",
 40177|       "member_name": "GetExternalFileReference",
 40178|       "member_kind": "method",
 40179|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 40180|       "confidence": "direct_return_type",
```
