# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 145 of 216
- Original line range: 56161-56560
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 56161|       "dll_signature_verified": true,
 56162|       "dll_relationship_scope": "declared",
 56163|       "dll_semantic_verified": null,
 56164|       "dll_verified_status": "signature_verified_declared",
 56165|       "revitlookup_referenced": null,
 56166|       "revitlookup_requires_document_context": null
 56167|     },
 56168|     {
 56169|       "source": "Autodesk.Revit.DB.RepeatingReferenceSource",
 56170|       "target": "Autodesk.Revit.DB.RepeaterBounds",
 56171|       "member_name": "GetBounds",
 56172|       "member_kind": "method",
 56173|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 56174|       "confidence": "direct_return_type",
 56175|       "confidence_tier": "unverified_reference",
 56176|       "target_resolution": "exact",
 56177|       "evidence": [
 56178|         "return type 'RepeaterBounds' directly names a Revit DB object type"
 56179|       ],
 56180|       "source_url": "https://www.revitapidocs.com/2025/967a1bea-609d-0da3-c5ff-b37efbf45686.htm",
 56181|       "dll_signature_verified": true,
 56182|       "dll_relationship_scope": "declared",
 56183|       "dll_semantic_verified": null,
 56184|       "dll_verified_status": "signature_verified_declared",
 56185|       "revitlookup_referenced": null,
 56186|       "revitlookup_requires_document_context": null
 56187|     },
 56188|     {
 56189|       "source": "Autodesk.Revit.DB.RepeatingReferenceSource",
 56190|       "target": "Autodesk.Revit.DB.Reference",
 56191|       "member_name": "GetReference",
 56192|       "member_kind": "method",
 56193|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 56194|       "confidence": "direct_return_type",
 56195|       "confidence_tier": "unverified_reference",
 56196|       "target_resolution": "exact",
 56197|       "evidence": [
 56198|         "return type 'Reference' directly names a Revit DB object type"
 56199|       ],
 56200|       "source_url": "https://www.revitapidocs.com/2025/e8d034c9-e440-4aab-7c6d-1ad80a509704.htm",
 56201|       "dll_signature_verified": true,
 56202|       "dll_relationship_scope": "declared",
 56203|       "dll_semantic_verified": null,
 56204|       "dll_verified_status": "signature_verified_declared",
 56205|       "revitlookup_referenced": null,
 56206|       "revitlookup_requires_document_context": null
 56207|     },
 56208|     {
 56209|       "source": "Autodesk.Revit.DB.Revision",
 56210|       "target": null,
 56211|       "member_name": "RevisionNumberingSequenceId",
 56212|       "member_kind": "property",
 56213|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 56214|       "confidence": "unknown_reference",
 56215|       "confidence_tier": "unverified_reference",
 56216|       "target_resolution": "none",
 56217|       "evidence": [
 56218|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 56219|       ],
 56220|       "source_url": "https://www.revitapidocs.com/2025/1ee59a94-2e37-b280-a8eb-20663a4f6143.htm",
 56221|       "dll_signature_verified": true,
 56222|       "dll_relationship_scope": "declared",
 56223|       "dll_semantic_verified": null,
 56224|       "dll_verified_status": "signature_verified_declared",
 56225|       "revitlookup_referenced": null,
 56226|       "revitlookup_requires_document_context": null
 56227|     },
 56228|     {
 56229|       "source": "Autodesk.Revit.DB.Revision",
 56230|       "target": null,
 56231|       "member_name": "CombineWithNext",
 56232|       "member_kind": "method",
 56233|       "edge_type": "RETURNS_ELEMENT_IDS",
 56234|       "confidence": "unknown_reference",
 56235|       "confidence_tier": "unverified_reference",
 56236|       "target_resolution": "none",
 56237|       "evidence": [
 56238|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 56239|       ],
 56240|       "source_url": "https://www.revitapidocs.com/2025/9f2ee71b-6e13-c6a8-306a-cfe493c39f96.htm",
 56241|       "dll_signature_verified": true,
 56242|       "dll_relationship_scope": "declared",
 56243|       "dll_semantic_verified": null,
 56244|       "dll_verified_status": "signature_verified_declared",
 56245|       "revitlookup_referenced": null,
 56246|       "revitlookup_requires_document_context": null
 56247|     },
 56248|     {
 56249|       "source": "Autodesk.Revit.DB.Revision",
 56250|       "target": null,
 56251|       "member_name": "CombineWithPrevious",
 56252|       "member_kind": "method",
 56253|       "edge_type": "RETURNS_ELEMENT_IDS",
 56254|       "confidence": "unknown_reference",
 56255|       "confidence_tier": "unverified_reference",
 56256|       "target_resolution": "none",
 56257|       "evidence": [
 56258|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 56259|       ],
 56260|       "source_url": "https://www.revitapidocs.com/2025/e66cf817-a4c4-ce4e-fa79-b7359000d24c.htm",
 56261|       "dll_signature_verified": true,
 56262|       "dll_relationship_scope": "declared",
 56263|       "dll_semantic_verified": null,
 56264|       "dll_verified_status": "signature_verified_declared",
 56265|       "revitlookup_referenced": null,
 56266|       "revitlookup_requires_document_context": null
 56267|     },
 56268|     {
 56269|       "source": "Autodesk.Revit.DB.Revision",
 56270|       "target": null,
 56271|       "member_name": "GetAllRevisionIds",
 56272|       "member_kind": "method",
 56273|       "edge_type": "RETURNS_ELEMENT_IDS",
 56274|       "confidence": "elementid_collection_with_strong_name",
 56275|       "confidence_tier": "core",
 56276|       "target_resolution": "none",
 56277|       "evidence": [
 56278|         "member name 'GetAllRevisionIds' matches keyword pattern /^GetAll/"
 56279|       ],
 56280|       "source_url": "https://www.revitapidocs.com/2025/1d7ae44e-1a2f-32ea-fe16-daa34ee3b481.htm",
 56281|       "dll_signature_verified": true,
 56282|       "dll_relationship_scope": "declared",
 56283|       "dll_semantic_verified": null,
 56284|       "dll_verified_status": "signature_verified_declared",
 56285|       "revitlookup_referenced": null,
 56286|       "revitlookup_requires_document_context": null
 56287|     },
 56288|     {
 56289|       "source": "Autodesk.Revit.DB.RevisionCloud",
 56290|       "target": null,
 56291|       "member_name": "RevisionId",
 56292|       "member_kind": "property",
 56293|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 56294|       "confidence": "unknown_reference",
 56295|       "confidence_tier": "unverified_reference",
 56296|       "target_resolution": "none",
 56297|       "evidence": [
 56298|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 56299|       ],
 56300|       "source_url": "https://www.revitapidocs.com/2025/611e9a5a-f968-152d-707b-de7395f65819.htm",
 56301|       "dll_signature_verified": true,
 56302|       "dll_relationship_scope": "declared",
 56303|       "dll_semantic_verified": null,
 56304|       "dll_verified_status": "signature_verified_declared",
 56305|       "revitlookup_referenced": null,
 56306|       "revitlookup_requires_document_context": null
 56307|     },
 56308|     {
 56309|       "source": "Autodesk.Revit.DB.RevisionCloud",
 56310|       "target": "Autodesk.Revit.DB.ViewSheet",
 56311|       "member_name": "GetSheetIds",
 56312|       "member_kind": "method",
 56313|       "edge_type": "PLACED_ON_SHEET",
 56314|       "confidence": "elementid_collection_with_strong_name",
 56315|       "confidence_tier": "core",
 56316|       "target_resolution": "exact",
 56317|       "evidence": [
 56318|         "member name 'GetSheetIds' matches keyword pattern /Sheet/"
 56319|       ],
 56320|       "source_url": "https://www.revitapidocs.com/2025/07ff2f25-9201-24aa-e025-502200af0379.htm",
 56321|       "dll_signature_verified": true,
 56322|       "dll_relationship_scope": "declared",
 56323|       "dll_semantic_verified": null,
 56324|       "dll_verified_status": "signature_verified_declared",
 56325|       "revitlookup_referenced": null,
 56326|       "revitlookup_requires_document_context": null
 56327|     },
 56328|     {
 56329|       "source": "Autodesk.Revit.DB.RevisionNumberingSequence",
 56330|       "target": null,
 56331|       "member_name": "GetAllRevisionNumberingSequences",
 56332|       "member_kind": "method",
 56333|       "edge_type": "RETURNS_ELEMENT_IDS",
 56334|       "confidence": "elementid_collection_with_strong_name",
 56335|       "confidence_tier": "core",
 56336|       "target_resolution": "none",
 56337|       "evidence": [
 56338|         "member name 'GetAllRevisionNumberingSequences' matches keyword pattern /^GetAll/"
 56339|       ],
 56340|       "source_url": "https://www.revitapidocs.com/2025/dd3dd3e5-ed85-a7e4-6280-1a0e5464f5d0.htm",
 56341|       "dll_signature_verified": true,
 56342|       "dll_relationship_scope": "declared",
 56343|       "dll_semantic_verified": null,
 56344|       "dll_verified_status": "signature_verified_declared",
 56345|       "revitlookup_referenced": true,
 56346|       "revitlookup_requires_document_context": false
 56347|     },
 56348|     {
 56349|       "source": "Autodesk.Revit.DB.RevisionNumberingSequence",
 56350|       "target": "Autodesk.Revit.DB.AlphanumericRevisionSettings",
 56351|       "member_name": "GetAlphanumericRevisionSettings",
 56352|       "member_kind": "method",
 56353|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 56354|       "confidence": "direct_return_type",
 56355|       "confidence_tier": "unverified_reference",
 56356|       "target_resolution": "exact",
 56357|       "evidence": [
 56358|         "return type 'AlphanumericRevisionSettings' directly names a Revit DB object type"
 56359|       ],
 56360|       "source_url": "https://www.revitapidocs.com/2025/46b8cc2b-f31d-f4e3-e140-7b05ce50730c.htm",
 56361|       "dll_signature_verified": true,
 56362|       "dll_relationship_scope": "declared",
 56363|       "dll_semantic_verified": null,
 56364|       "dll_verified_status": "signature_verified_declared",
 56365|       "revitlookup_referenced": null,
 56366|       "revitlookup_requires_document_context": null
 56367|     },
 56368|     {
 56369|       "source": "Autodesk.Revit.DB.RevisionNumberingSequence",
 56370|       "target": "Autodesk.Revit.DB.NumericRevisionSettings",
 56371|       "member_name": "GetNumericRevisionSettings",
 56372|       "member_kind": "method",
 56373|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 56374|       "confidence": "direct_return_type",
 56375|       "confidence_tier": "unverified_reference",
 56376|       "target_resolution": "exact",
 56377|       "evidence": [
 56378|         "return type 'NumericRevisionSettings' directly names a Revit DB object type"
 56379|       ],
 56380|       "source_url": "https://www.revitapidocs.com/2025/63cbff5e-855c-9237-a160-e90f7e1f7a17.htm",
 56381|       "dll_signature_verified": true,
 56382|       "dll_relationship_scope": "declared",
 56383|       "dll_semantic_verified": null,
 56384|       "dll_verified_status": "signature_verified_declared",
 56385|       "revitlookup_referenced": null,
 56386|       "revitlookup_requires_document_context": null
 56387|     },
 56388|     {
 56389|       "source": "Autodesk.Revit.DB.RevitLinkGraphicsSettings",
 56390|       "target": null,
 56391|       "member_name": "LinkedViewId",
 56392|       "member_kind": "property",
 56393|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 56394|       "confidence": "unknown_reference",
 56395|       "confidence_tier": "unverified_reference",
 56396|       "target_resolution": "none",
 56397|       "evidence": [
 56398|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 56399|       ],
 56400|       "source_url": "https://www.revitapidocs.com/2025/053b6a8c-2212-322c-8f21-b7d95e089b42.htm",
 56401|       "dll_signature_verified": true,
 56402|       "dll_relationship_scope": "declared",
 56403|       "dll_semantic_verified": null,
 56404|       "dll_verified_status": "signature_verified_declared",
 56405|       "revitlookup_referenced": null,
 56406|       "revitlookup_requires_document_context": null
 56407|     },
 56408|     {
 56409|       "source": "Autodesk.Revit.DB.RevitLinkGraphicsSettings",
 56410|       "target": "Autodesk.Revit.DB.Phase",
 56411|       "member_name": "GetPhaseFilterId",
 56412|       "member_kind": "method",
 56413|       "edge_type": "ASSIGNED_TO_PHASE",
 56414|       "confidence": "elementid_with_strong_name",
 56415|       "confidence_tier": "core",
 56416|       "target_resolution": "exact",
 56417|       "evidence": [
 56418|         "member name 'GetPhaseFilterId' matches keyword pattern /Phase/"
 56419|       ],
 56420|       "source_url": "https://www.revitapidocs.com/2025/38e2413a-13e1-b1e6-0fd1-c65ff09948b5.htm",
 56421|       "dll_signature_verified": true,
 56422|       "dll_relationship_scope": "declared",
 56423|       "dll_semantic_verified": null,
 56424|       "dll_verified_status": "signature_verified_declared",
 56425|       "revitlookup_referenced": null,
 56426|       "revitlookup_requires_document_context": null
 56427|     },
 56428|     {
 56429|       "source": "Autodesk.Revit.DB.RevitLinkGraphicsSettings",
 56430|       "target": "Autodesk.Revit.DB.Phase",
 56431|       "member_name": "GetPhaseFilterType",
 56432|       "member_kind": "method",
 56433|       "edge_type": "ASSIGNED_TO_PHASE",
 56434|       "confidence": "name_only_candidate",
 56435|       "confidence_tier": "likely",
 56436|       "target_resolution": "exact",
 56437|       "evidence": [
 56438|         "member name 'GetPhaseFilterType' matches keyword pattern /Phase/ but return type 'LinkVisibility' gives no type-level confirmation"
 56439|       ],
 56440|       "source_url": "https://www.revitapidocs.com/2025/4ab9d29c-c109-d1c6-bab1-9d1edb2e2076.htm",
 56441|       "dll_signature_verified": true,
 56442|       "dll_relationship_scope": "declared",
 56443|       "dll_semantic_verified": null,
 56444|       "dll_verified_status": "signature_verified_declared",
 56445|       "revitlookup_referenced": null,
 56446|       "revitlookup_requires_document_context": null
 56447|     },
 56448|     {
 56449|       "source": "Autodesk.Revit.DB.RevitLinkGraphicsSettings",
 56450|       "target": "Autodesk.Revit.DB.Phase",
 56451|       "member_name": "GetPhaseId",
 56452|       "member_kind": "method",
 56453|       "edge_type": "ASSIGNED_TO_PHASE",
 56454|       "confidence": "elementid_with_strong_name",
 56455|       "confidence_tier": "core",
 56456|       "target_resolution": "exact",
 56457|       "evidence": [
 56458|         "member name 'GetPhaseId' matches keyword pattern /Phase/"
 56459|       ],
 56460|       "source_url": "https://www.revitapidocs.com/2025/350216d7-b0ab-1db3-c531-9853b0073014.htm",
 56461|       "dll_signature_verified": true,
 56462|       "dll_relationship_scope": "declared",
 56463|       "dll_semantic_verified": null,
 56464|       "dll_verified_status": "signature_verified_declared",
 56465|       "revitlookup_referenced": null,
 56466|       "revitlookup_requires_document_context": null
 56467|     },
 56468|     {
 56469|       "source": "Autodesk.Revit.DB.RevitLinkGraphicsSettings",
 56470|       "target": "Autodesk.Revit.DB.Phase",
 56471|       "member_name": "GetPhaseType",
 56472|       "member_kind": "method",
 56473|       "edge_type": "ASSIGNED_TO_PHASE",
 56474|       "confidence": "name_only_candidate",
 56475|       "confidence_tier": "likely",
 56476|       "target_resolution": "exact",
 56477|       "evidence": [
 56478|         "member name 'GetPhaseType' matches keyword pattern /Phase/ but return type 'LinkVisibility' gives no type-level confirmation"
 56479|       ],
 56480|       "source_url": "https://www.revitapidocs.com/2025/963a2e3b-8481-faa3-6b00-406f29eafc6c.htm",
 56481|       "dll_signature_verified": true,
 56482|       "dll_relationship_scope": "declared",
 56483|       "dll_semantic_verified": null,
 56484|       "dll_verified_status": "signature_verified_declared",
 56485|       "revitlookup_referenced": null,
 56486|       "revitlookup_requires_document_context": null
 56487|     },
 56488|     {
 56489|       "source": "Autodesk.Revit.DB.RevitLinkGraphicsSettings",
 56490|       "target": "Autodesk.Revit.DB.Level",
 56491|       "member_name": "GetViewDetailLevel",
 56492|       "member_kind": "method",
 56493|       "edge_type": "ASSIGNED_TO_LEVEL",
 56494|       "confidence": "name_only_candidate",
 56495|       "confidence_tier": "likely",
 56496|       "target_resolution": "exact",
 56497|       "evidence": [
 56498|         "member name 'GetViewDetailLevel' matches keyword pattern /Level/ but return type 'ViewDetailLevel' gives no type-level confirmation"
 56499|       ],
 56500|       "source_url": "https://www.revitapidocs.com/2025/c4cd9e42-ec1d-374a-5e0f-fe9d8eca9e5e.htm",
 56501|       "dll_signature_verified": true,
 56502|       "dll_relationship_scope": "declared",
 56503|       "dll_semantic_verified": null,
 56504|       "dll_verified_status": "signature_verified_declared",
 56505|       "revitlookup_referenced": null,
 56506|       "revitlookup_requires_document_context": null
 56507|     },
 56508|     {
 56509|       "source": "Autodesk.Revit.DB.RevitLinkGraphicsSettings",
 56510|       "target": "Autodesk.Revit.DB.Level",
 56511|       "member_name": "GetViewDetailLevelType",
 56512|       "member_kind": "method",
 56513|       "edge_type": "ASSIGNED_TO_LEVEL",
 56514|       "confidence": "name_only_candidate",
 56515|       "confidence_tier": "likely",
 56516|       "target_resolution": "exact",
 56517|       "evidence": [
 56518|         "member name 'GetViewDetailLevelType' matches keyword pattern /Level/ but return type 'LinkVisibility' gives no type-level confirmation"
 56519|       ],
 56520|       "source_url": "https://www.revitapidocs.com/2025/e6418565-d0d2-f7df-f80f-bdfd2b275019.htm",
 56521|       "dll_signature_verified": true,
 56522|       "dll_relationship_scope": "declared",
 56523|       "dll_semantic_verified": null,
 56524|       "dll_verified_status": "signature_verified_declared",
 56525|       "revitlookup_referenced": null,
 56526|       "revitlookup_requires_document_context": null
 56527|     },
 56528|     {
 56529|       "source": "Autodesk.Revit.DB.RevitLinkGraphicsSettings",
 56530|       "target": "Autodesk.Revit.DB.Phase",
 56531|       "member_name": "SetPhase",
 56532|       "member_kind": "method",
 56533|       "edge_type": "ASSIGNED_TO_PHASE",
 56534|       "confidence": "name_only_candidate",
 56535|       "confidence_tier": "likely",
 56536|       "target_resolution": "exact",
 56537|       "evidence": [
 56538|         "member name 'SetPhase' matches keyword pattern /Phase/ but return type 'void' gives no type-level confirmation"
 56539|       ],
 56540|       "source_url": "https://www.revitapidocs.com/2025/fe00f9a8-69c4-d8cf-fb84-5c0b6f3dd8b1.htm",
 56541|       "dll_signature_verified": true,
 56542|       "dll_relationship_scope": "declared",
 56543|       "dll_semantic_verified": null,
 56544|       "dll_verified_status": "signature_verified_declared",
 56545|       "revitlookup_referenced": null,
 56546|       "revitlookup_requires_document_context": null
 56547|     },
 56548|     {
 56549|       "source": "Autodesk.Revit.DB.RevitLinkGraphicsSettings",
 56550|       "target": "Autodesk.Revit.DB.Phase",
 56551|       "member_name": "SetPhaseFilter",
 56552|       "member_kind": "method",
 56553|       "edge_type": "ASSIGNED_TO_PHASE",
 56554|       "confidence": "name_only_candidate",
 56555|       "confidence_tier": "likely",
 56556|       "target_resolution": "exact",
 56557|       "evidence": [
 56558|         "member name 'SetPhaseFilter' matches keyword pattern /Phase/ but return type 'void' gives no type-level confirmation"
 56559|       ],
 56560|       "source_url": "https://www.revitapidocs.com/2025/505099f1-4435-01c1-7ce7-ee59edf50c9a.htm",
```
