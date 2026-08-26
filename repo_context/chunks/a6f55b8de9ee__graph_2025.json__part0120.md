# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 120 of 216
- Original line range: 46411-46810
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 46411|       "dll_signature_verified": true,
 46412|       "dll_relationship_scope": "declared",
 46413|       "dll_semantic_verified": null,
 46414|       "dll_verified_status": "signature_verified_declared",
 46415|       "revitlookup_referenced": null,
 46416|       "revitlookup_requires_document_context": null
 46417|     },
 46418|     {
 46419|       "source": "Autodesk.Revit.DB.FilteredElementCollector",
 46420|       "target": "Autodesk.Revit.DB.Element",
 46421|       "member_name": "ToElements",
 46422|       "member_kind": "method",
 46423|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 46424|       "confidence": "needs_runtime_validation",
 46425|       "confidence_tier": "needs_validation",
 46426|       "target_resolution": "exact",
 46427|       "evidence": [
 46428|         "return type 'IList < Element >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 46429|       ],
 46430|       "source_url": "https://www.revitapidocs.com/2025/732b4a0d-62d8-b86d-120b-8ea3d9713b34.htm",
 46431|       "dll_signature_verified": true,
 46432|       "dll_relationship_scope": "declared",
 46433|       "dll_semantic_verified": null,
 46434|       "dll_verified_status": "signature_verified_declared",
 46435|       "revitlookup_referenced": null,
 46436|       "revitlookup_requires_document_context": null
 46437|     },
 46438|     {
 46439|       "source": "Autodesk.Revit.DB.FilteredElementIdIterator",
 46440|       "target": null,
 46441|       "member_name": "Current",
 46442|       "member_kind": "property",
 46443|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 46444|       "confidence": "unknown_reference",
 46445|       "confidence_tier": "unverified_reference",
 46446|       "target_resolution": "none",
 46447|       "evidence": [
 46448|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 46449|       ],
 46450|       "source_url": "https://www.revitapidocs.com/2025/d37acf89-a76e-f310-ff9e-056c5857172f.htm",
 46451|       "dll_signature_verified": true,
 46452|       "dll_relationship_scope": "declared",
 46453|       "dll_semantic_verified": null,
 46454|       "dll_verified_status": "signature_verified_declared",
 46455|       "revitlookup_referenced": null,
 46456|       "revitlookup_requires_document_context": null
 46457|     },
 46458|     {
 46459|       "source": "Autodesk.Revit.DB.FilteredElementIdIterator",
 46460|       "target": null,
 46461|       "member_name": "GetCurrent",
 46462|       "member_kind": "method",
 46463|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 46464|       "confidence": "unknown_reference",
 46465|       "confidence_tier": "unverified_reference",
 46466|       "target_resolution": "none",
 46467|       "evidence": [
 46468|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 46469|       ],
 46470|       "source_url": "https://www.revitapidocs.com/2025/4622b4be-e533-d633-26e8-2c4ea5d63742.htm",
 46471|       "dll_signature_verified": true,
 46472|       "dll_relationship_scope": "declared",
 46473|       "dll_semantic_verified": null,
 46474|       "dll_verified_status": "signature_verified_declared",
 46475|       "revitlookup_referenced": null,
 46476|       "revitlookup_requires_document_context": null
 46477|     },
 46478|     {
 46479|       "source": "Autodesk.Revit.DB.FilteredElementIterator",
 46480|       "target": "Autodesk.Revit.DB.Element",
 46481|       "member_name": "Current",
 46482|       "member_kind": "property",
 46483|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 46484|       "confidence": "direct_return_type",
 46485|       "confidence_tier": "unverified_reference",
 46486|       "target_resolution": "exact",
 46487|       "evidence": [
 46488|         "return type 'Element' directly names a Revit DB object type"
 46489|       ],
 46490|       "source_url": "https://www.revitapidocs.com/2025/43c20ff6-06fb-0b0e-0313-da296ab54fb7.htm",
 46491|       "dll_signature_verified": true,
 46492|       "dll_relationship_scope": "declared",
 46493|       "dll_semantic_verified": null,
 46494|       "dll_verified_status": "signature_verified_declared",
 46495|       "revitlookup_referenced": null,
 46496|       "revitlookup_requires_document_context": null
 46497|     },
 46498|     {
 46499|       "source": "Autodesk.Revit.DB.FilteredElementIterator",
 46500|       "target": "Autodesk.Revit.DB.Element",
 46501|       "member_name": "GetCurrent",
 46502|       "member_kind": "method",
 46503|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 46504|       "confidence": "direct_return_type",
 46505|       "confidence_tier": "unverified_reference",
 46506|       "target_resolution": "exact",
 46507|       "evidence": [
 46508|         "return type 'Element' directly names a Revit DB object type"
 46509|       ],
 46510|       "source_url": "https://www.revitapidocs.com/2025/05e73775-334c-a708-7493-a9489ef03e45.htm",
 46511|       "dll_signature_verified": true,
 46512|       "dll_relationship_scope": "declared",
 46513|       "dll_semantic_verified": null,
 46514|       "dll_verified_status": "signature_verified_declared",
 46515|       "revitlookup_referenced": null,
 46516|       "revitlookup_requires_document_context": null
 46517|     },
 46518|     {
 46519|       "source": "Autodesk.Revit.DB.FilteredWorksetCollector",
 46520|       "target": "Autodesk.Revit.DB.Workset",
 46521|       "member_name": "FirstWorkset",
 46522|       "member_kind": "method",
 46523|       "edge_type": "OWNED_BY_WORKSET",
 46524|       "confidence": "direct_return_type",
 46525|       "confidence_tier": "core",
 46526|       "target_resolution": "exact",
 46527|       "evidence": [
 46528|         "return type 'Workset' directly names a Revit DB object type"
 46529|       ],
 46530|       "source_url": "https://www.revitapidocs.com/2025/2bec8a78-762f-3c54-8f9d-3df46e1d133b.htm",
 46531|       "dll_signature_verified": true,
 46532|       "dll_relationship_scope": "declared",
 46533|       "dll_semantic_verified": null,
 46534|       "dll_verified_status": "signature_verified_declared",
 46535|       "revitlookup_referenced": null,
 46536|       "revitlookup_requires_document_context": null
 46537|     },
 46538|     {
 46539|       "source": "Autodesk.Revit.DB.FilteredWorksetCollector",
 46540|       "target": "Autodesk.Revit.DB.Workset",
 46541|       "member_name": "FirstWorksetId",
 46542|       "member_kind": "method",
 46543|       "edge_type": "OWNED_BY_WORKSET",
 46544|       "confidence": "direct_return_type",
 46545|       "confidence_tier": "core",
 46546|       "target_resolution": "exact",
 46547|       "evidence": [
 46548|         "return type 'WorksetId' is a typed identifier that unambiguously names its own target ('Workset') through the type system alone"
 46549|       ],
 46550|       "source_url": "https://www.revitapidocs.com/2025/dc790ba3-0477-1e2f-cc76-1ee64747d5a8.htm",
 46551|       "dll_signature_verified": true,
 46552|       "dll_relationship_scope": "declared",
 46553|       "dll_semantic_verified": null,
 46554|       "dll_verified_status": "signature_verified_declared",
 46555|       "revitlookup_referenced": null,
 46556|       "revitlookup_requires_document_context": null
 46557|     },
 46558|     {
 46559|       "source": "Autodesk.Revit.DB.FilteredWorksetCollector",
 46560|       "target": "Autodesk.Revit.DB.FilteredWorksetIdIterator",
 46561|       "member_name": "GetWorksetIdIterator",
 46562|       "member_kind": "method",
 46563|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 46564|       "confidence": "direct_return_type",
 46565|       "confidence_tier": "unverified_reference",
 46566|       "target_resolution": "exact",
 46567|       "evidence": [
 46568|         "member name 'GetWorksetIdIterator' matches keyword pattern /Workset/ implying target 'Workset', but the actual return type 'FilteredWorksetIdIterator' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 46569|         "return type 'FilteredWorksetIdIterator' directly names a Revit DB object type"
 46570|       ],
 46571|       "source_url": "https://www.revitapidocs.com/2025/21ebbe3f-f9d2-0030-5d99-ebb43be66b2d.htm",
 46572|       "dll_signature_verified": true,
 46573|       "dll_relationship_scope": "declared",
 46574|       "dll_semantic_verified": null,
 46575|       "dll_verified_status": "signature_verified_declared",
 46576|       "revitlookup_referenced": null,
 46577|       "revitlookup_requires_document_context": null
 46578|     },
 46579|     {
 46580|       "source": "Autodesk.Revit.DB.FilteredWorksetCollector",
 46581|       "target": "Autodesk.Revit.DB.FilteredWorksetIterator",
 46582|       "member_name": "GetWorksetIterator",
 46583|       "member_kind": "method",
 46584|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 46585|       "confidence": "direct_return_type",
 46586|       "confidence_tier": "unverified_reference",
 46587|       "target_resolution": "exact",
 46588|       "evidence": [
 46589|         "member name 'GetWorksetIterator' matches keyword pattern /Workset/ implying target 'Workset', but the actual return type 'FilteredWorksetIterator' conflicts -- treating the name match as a coincidental collision rather than relationship evidence",
 46590|         "return type 'FilteredWorksetIterator' directly names a Revit DB object type"
 46591|       ],
 46592|       "source_url": "https://www.revitapidocs.com/2025/70daa82a-8893-bc5c-fa4a-85737f5c261a.htm",
 46593|       "dll_signature_verified": true,
 46594|       "dll_relationship_scope": "declared",
 46595|       "dll_semantic_verified": null,
 46596|       "dll_verified_status": "signature_verified_declared",
 46597|       "revitlookup_referenced": null,
 46598|       "revitlookup_requires_document_context": null
 46599|     },
 46600|     {
 46601|       "source": "Autodesk.Revit.DB.FilteredWorksetCollector",
 46602|       "target": "Autodesk.Revit.DB.WorksetId",
 46603|       "member_name": "ToWorksetIds",
 46604|       "member_kind": "method",
 46605|       "edge_type": "OWNED_BY_WORKSET",
 46606|       "confidence": "needs_runtime_validation",
 46607|       "confidence_tier": "needs_validation",
 46608|       "target_resolution": "exact",
 46609|       "evidence": [
 46610|         "return type 'ICollection < WorksetId >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 46611|       ],
 46612|       "source_url": "https://www.revitapidocs.com/2025/1760f71f-d481-5d97-beb8-cfbc96ea2db5.htm",
 46613|       "dll_signature_verified": true,
 46614|       "dll_relationship_scope": "declared",
 46615|       "dll_semantic_verified": null,
 46616|       "dll_verified_status": "signature_verified_declared",
 46617|       "revitlookup_referenced": null,
 46618|       "revitlookup_requires_document_context": null
 46619|     },
 46620|     {
 46621|       "source": "Autodesk.Revit.DB.FilteredWorksetCollector",
 46622|       "target": "Autodesk.Revit.DB.Workset",
 46623|       "member_name": "ToWorksets",
 46624|       "member_kind": "method",
 46625|       "edge_type": "OWNED_BY_WORKSET",
 46626|       "confidence": "needs_runtime_validation",
 46627|       "confidence_tier": "needs_validation",
 46628|       "target_resolution": "exact",
 46629|       "evidence": [
 46630|         "return type 'IList < Workset >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 46631|       ],
 46632|       "source_url": "https://www.revitapidocs.com/2025/32db1fdd-6679-1e33-d3d2-9057b6a26e91.htm",
 46633|       "dll_signature_verified": true,
 46634|       "dll_relationship_scope": "declared",
 46635|       "dll_semantic_verified": null,
 46636|       "dll_verified_status": "signature_verified_declared",
 46637|       "revitlookup_referenced": null,
 46638|       "revitlookup_requires_document_context": null
 46639|     },
 46640|     {
 46641|       "source": "Autodesk.Revit.DB.FilteredWorksetIdIterator",
 46642|       "target": "Autodesk.Revit.DB.Workset",
 46643|       "member_name": "Current",
 46644|       "member_kind": "property",
 46645|       "edge_type": "OWNED_BY_WORKSET",
 46646|       "confidence": "direct_return_type",
 46647|       "confidence_tier": "core",
 46648|       "target_resolution": "exact",
 46649|       "evidence": [
 46650|         "return type 'WorksetId' is a typed identifier that unambiguously names its own target ('Workset') through the type system alone"
 46651|       ],
 46652|       "source_url": "https://www.revitapidocs.com/2025/ba1f2d85-898b-2985-7a0d-2fc7bbb29430.htm",
 46653|       "dll_signature_verified": true,
 46654|       "dll_relationship_scope": "declared",
 46655|       "dll_semantic_verified": null,
 46656|       "dll_verified_status": "signature_verified_declared",
 46657|       "revitlookup_referenced": null,
 46658|       "revitlookup_requires_document_context": null
 46659|     },
 46660|     {
 46661|       "source": "Autodesk.Revit.DB.FilteredWorksetIdIterator",
 46662|       "target": "Autodesk.Revit.DB.Workset",
 46663|       "member_name": "GetCurrent",
 46664|       "member_kind": "method",
 46665|       "edge_type": "OWNED_BY_WORKSET",
 46666|       "confidence": "direct_return_type",
 46667|       "confidence_tier": "core",
 46668|       "target_resolution": "exact",
 46669|       "evidence": [
 46670|         "return type 'WorksetId' is a typed identifier that unambiguously names its own target ('Workset') through the type system alone"
 46671|       ],
 46672|       "source_url": "https://www.revitapidocs.com/2025/ef99d539-5039-bf16-ee24-91daef82355d.htm",
 46673|       "dll_signature_verified": true,
 46674|       "dll_relationship_scope": "declared",
 46675|       "dll_semantic_verified": null,
 46676|       "dll_verified_status": "signature_verified_declared",
 46677|       "revitlookup_referenced": null,
 46678|       "revitlookup_requires_document_context": null
 46679|     },
 46680|     {
 46681|       "source": "Autodesk.Revit.DB.FilteredWorksetIterator",
 46682|       "target": "Autodesk.Revit.DB.Workset",
 46683|       "member_name": "Current",
 46684|       "member_kind": "property",
 46685|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 46686|       "confidence": "direct_return_type",
 46687|       "confidence_tier": "unverified_reference",
 46688|       "target_resolution": "exact",
 46689|       "evidence": [
 46690|         "return type 'Workset' directly names a Revit DB object type"
 46691|       ],
 46692|       "source_url": "https://www.revitapidocs.com/2025/7c3c158e-3da6-4e7e-ec0a-88381c825ba8.htm",
 46693|       "dll_signature_verified": true,
 46694|       "dll_relationship_scope": "declared",
 46695|       "dll_semantic_verified": null,
 46696|       "dll_verified_status": "signature_verified_declared",
 46697|       "revitlookup_referenced": null,
 46698|       "revitlookup_requires_document_context": null
 46699|     },
 46700|     {
 46701|       "source": "Autodesk.Revit.DB.FilteredWorksetIterator",
 46702|       "target": "Autodesk.Revit.DB.Workset",
 46703|       "member_name": "GetCurrent",
 46704|       "member_kind": "method",
 46705|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 46706|       "confidence": "direct_return_type",
 46707|       "confidence_tier": "unverified_reference",
 46708|       "target_resolution": "exact",
 46709|       "evidence": [
 46710|         "return type 'Workset' directly names a Revit DB object type"
 46711|       ],
 46712|       "source_url": "https://www.revitapidocs.com/2025/f0ca7ab5-3cfb-3bdb-f23e-08aba7fdd9b3.htm",
 46713|       "dll_signature_verified": true,
 46714|       "dll_relationship_scope": "declared",
 46715|       "dll_semantic_verified": null,
 46716|       "dll_verified_status": "signature_verified_declared",
 46717|       "revitlookup_referenced": null,
 46718|       "revitlookup_requires_document_context": null
 46719|     },
 46720|     {
 46721|       "source": "Autodesk.Revit.DB.FilterElementIdRule",
 46722|       "target": null,
 46723|       "member_name": "RuleValue",
 46724|       "member_kind": "property",
 46725|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 46726|       "confidence": "unknown_reference",
 46727|       "confidence_tier": "unverified_reference",
 46728|       "target_resolution": "none",
 46729|       "evidence": [
 46730|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 46731|       ],
 46732|       "source_url": "https://www.revitapidocs.com/2025/b7d362be-4b42-7cff-569a-c52b9652b760.htm",
 46733|       "dll_signature_verified": true,
 46734|       "dll_relationship_scope": "declared",
 46735|       "dll_semantic_verified": null,
 46736|       "dll_verified_status": "signature_verified_declared",
 46737|       "revitlookup_referenced": null,
 46738|       "revitlookup_requires_document_context": null
 46739|     },
 46740|     {
 46741|       "source": "Autodesk.Revit.DB.FilterElementIdRule",
 46742|       "target": "Autodesk.Revit.DB.Level",
 46743|       "member_name": "UsesLevelFiltering",
 46744|       "member_kind": "method",
 46745|       "edge_type": "ASSIGNED_TO_LEVEL",
 46746|       "confidence": "name_only_candidate",
 46747|       "confidence_tier": "likely",
 46748|       "target_resolution": "exact",
 46749|       "evidence": [
 46750|         "member name 'UsesLevelFiltering' matches keyword pattern /Level/ but return type 'bool' gives no type-level confirmation"
 46751|       ],
 46752|       "source_url": "https://www.revitapidocs.com/2025/bfa83fa8-304f-edd9-b74e-d9d60d689ade.htm",
 46753|       "dll_signature_verified": true,
 46754|       "dll_relationship_scope": "declared",
 46755|       "dll_semantic_verified": null,
 46756|       "dll_verified_status": "signature_verified_declared",
 46757|       "revitlookup_referenced": null,
 46758|       "revitlookup_requires_document_context": null
 46759|     },
 46760|     {
 46761|       "source": "Autodesk.Revit.DB.FilterGlobalParameterAssociationRule",
 46762|       "target": null,
 46763|       "member_name": "RuleValue",
 46764|       "member_kind": "property",
 46765|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 46766|       "confidence": "unknown_reference",
 46767|       "confidence_tier": "unverified_reference",
 46768|       "target_resolution": "none",
 46769|       "evidence": [
 46770|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 46771|       ],
 46772|       "source_url": "https://www.revitapidocs.com/2025/efa39aad-910b-b036-255d-3ad2e8a6f0dc.htm",
 46773|       "dll_signature_verified": true,
 46774|       "dll_relationship_scope": "declared",
 46775|       "dll_semantic_verified": null,
 46776|       "dll_verified_status": "signature_verified_declared",
 46777|       "revitlookup_referenced": null,
 46778|       "revitlookup_requires_document_context": null
 46779|     },
 46780|     {
 46781|       "source": "Autodesk.Revit.DB.FilterInverseRule",
 46782|       "target": "Autodesk.Revit.DB.FilterRule",
 46783|       "member_name": "GetInnerRule",
 46784|       "member_kind": "method",
 46785|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 46786|       "confidence": "direct_return_type",
 46787|       "confidence_tier": "unverified_reference",
 46788|       "target_resolution": "exact",
 46789|       "evidence": [
 46790|         "return type 'FilterRule' directly names a Revit DB object type"
 46791|       ],
 46792|       "source_url": "https://www.revitapidocs.com/2025/3efb775d-b129-0e0b-5f57-5290eb12fa43.htm",
 46793|       "dll_signature_verified": true,
 46794|       "dll_relationship_scope": "declared",
 46795|       "dll_semantic_verified": null,
 46796|       "dll_verified_status": "signature_verified_declared",
 46797|       "revitlookup_referenced": null,
 46798|       "revitlookup_requires_document_context": null
 46799|     },
 46800|     {
 46801|       "source": "Autodesk.Revit.DB.FilterNumericValueRule",
 46802|       "target": "Autodesk.Revit.DB.FilterNumericRuleEvaluator",
 46803|       "member_name": "GetEvaluator",
 46804|       "member_kind": "method",
 46805|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 46806|       "confidence": "direct_return_type",
 46807|       "confidence_tier": "unverified_reference",
 46808|       "target_resolution": "exact",
 46809|       "evidence": [
 46810|         "return type 'FilterNumericRuleEvaluator' directly names a Revit DB object type"
```
