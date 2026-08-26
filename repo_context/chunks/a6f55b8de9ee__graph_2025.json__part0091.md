# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 91 of 216
- Original line range: 35101-35500
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 35101|       "evidence": [
 35102|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 35103|       ],
 35104|       "source_url": "https://www.revitapidocs.com/2025/7bd864e9-3e9d-f4dd-ddf7-57e70ce8c8ba.htm",
 35105|       "dll_signature_verified": true,
 35106|       "dll_relationship_scope": "declared",
 35107|       "dll_semantic_verified": null,
 35108|       "dll_verified_status": "signature_verified_declared",
 35109|       "revitlookup_referenced": null,
 35110|       "revitlookup_requires_document_context": null
 35111|     },
 35112|     {
 35113|       "source": "Autodesk.Revit.DB.CompoundStructureLayer",
 35114|       "target": "Autodesk.Revit.DB.Material",
 35115|       "member_name": "MaterialId",
 35116|       "member_kind": "property",
 35117|       "edge_type": "USES_MATERIAL",
 35118|       "confidence": "elementid_with_strong_name",
 35119|       "confidence_tier": "core",
 35120|       "target_resolution": "exact",
 35121|       "evidence": [
 35122|         "member name 'MaterialId' matches keyword pattern /Material/"
 35123|       ],
 35124|       "source_url": "https://www.revitapidocs.com/2025/c5a502aa-217c-b76b-b1ad-33f57cc7b24d.htm",
 35125|       "dll_signature_verified": true,
 35126|       "dll_relationship_scope": "declared",
 35127|       "dll_semantic_verified": null,
 35128|       "dll_verified_status": "signature_verified_declared",
 35129|       "revitlookup_referenced": null,
 35130|       "revitlookup_requires_document_context": null
 35131|     },
 35132|     {
 35133|       "source": "Autodesk.Revit.DB.ConfigurationReloadInfo",
 35134|       "target": "Autodesk.Revit.DB.ConnectionValidationInfo",
 35135|       "member_name": "GetConnectivityValidation",
 35136|       "member_kind": "method",
 35137|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35138|       "confidence": "direct_return_type",
 35139|       "confidence_tier": "unverified_reference",
 35140|       "target_resolution": "exact",
 35141|       "evidence": [
 35142|         "return type 'ConnectionValidationInfo' directly names a Revit DB object type"
 35143|       ],
 35144|       "source_url": "https://www.revitapidocs.com/2025/d4e50d7c-3e1d-37cf-bbcb-ee98d987d182.htm",
 35145|       "dll_signature_verified": true,
 35146|       "dll_relationship_scope": "declared",
 35147|       "dll_semantic_verified": null,
 35148|       "dll_verified_status": "signature_verified_declared",
 35149|       "revitlookup_referenced": null,
 35150|       "revitlookup_requires_document_context": null
 35151|     },
 35152|     {
 35153|       "source": "Autodesk.Revit.DB.ConfigurationReloadInfo",
 35154|       "target": null,
 35155|       "member_name": "GetCustomDataChangedElements",
 35156|       "member_kind": "method",
 35157|       "edge_type": "RETURNS_ELEMENT_IDS",
 35158|       "confidence": "unknown_reference",
 35159|       "confidence_tier": "unverified_reference",
 35160|       "target_resolution": "none",
 35161|       "evidence": [
 35162|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 35163|       ],
 35164|       "source_url": "https://www.revitapidocs.com/2025/c3679f73-8888-0dc0-69e2-64bf09a09fd8.htm",
 35165|       "dll_signature_verified": true,
 35166|       "dll_relationship_scope": "declared",
 35167|       "dll_semantic_verified": null,
 35168|       "dll_verified_status": "signature_verified_declared",
 35169|       "revitlookup_referenced": null,
 35170|       "revitlookup_requires_document_context": null
 35171|     },
 35172|     {
 35173|       "source": "Autodesk.Revit.DB.ConfigurationReloadInfo",
 35174|       "target": "Autodesk.Revit.DB.ReloadSwapOutInfo",
 35175|       "member_name": "GetOutOfDatePartStatus",
 35176|       "member_kind": "method",
 35177|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35178|       "confidence": "direct_return_type",
 35179|       "confidence_tier": "unverified_reference",
 35180|       "target_resolution": "exact",
 35181|       "evidence": [
 35182|         "return type 'ReloadSwapOutInfo' directly names a Revit DB object type"
 35183|       ],
 35184|       "source_url": "https://www.revitapidocs.com/2025/7281d696-b9c8-130e-a165-9906dd5aad29.htm",
 35185|       "dll_signature_verified": true,
 35186|       "dll_relationship_scope": "declared",
 35187|       "dll_semantic_verified": null,
 35188|       "dll_verified_status": "signature_verified_declared",
 35189|       "revitlookup_referenced": null,
 35190|       "revitlookup_requires_document_context": null
 35191|     },
 35192|     {
 35193|       "source": "Autodesk.Revit.DB.ConicalSurface",
 35194|       "target": "Autodesk.Revit.DB.Frame",
 35195|       "member_name": "GetFrameOfReference",
 35196|       "member_kind": "method",
 35197|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35198|       "confidence": "direct_return_type",
 35199|       "confidence_tier": "unverified_reference",
 35200|       "target_resolution": "exact",
 35201|       "evidence": [
 35202|         "return type 'Frame' directly names a Revit DB object type"
 35203|       ],
 35204|       "source_url": "https://www.revitapidocs.com/2025/59ae1045-6128-4daa-ca37-db36309d9905.htm",
 35205|       "dll_signature_verified": true,
 35206|       "dll_relationship_scope": "declared",
 35207|       "dll_semantic_verified": null,
 35208|       "dll_verified_status": "signature_verified_declared",
 35209|       "revitlookup_referenced": null,
 35210|       "revitlookup_requires_document_context": null
 35211|     },
 35212|     {
 35213|       "source": "Autodesk.Revit.DB.ConnectionValidationInfo",
 35214|       "target": "Autodesk.Revit.DB.ConnectionValidationWarning",
 35215|       "member_name": "GetWarning",
 35216|       "member_kind": "method",
 35217|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35218|       "confidence": "direct_return_type",
 35219|       "confidence_tier": "unverified_reference",
 35220|       "target_resolution": "exact",
 35221|       "evidence": [
 35222|         "return type 'ConnectionValidationWarning' directly names a Revit DB object type"
 35223|       ],
 35224|       "source_url": "https://www.revitapidocs.com/2025/8f6f93f2-661d-568d-3ea7-f68af8948826.htm",
 35225|       "dll_signature_verified": true,
 35226|       "dll_relationship_scope": "declared",
 35227|       "dll_semantic_verified": null,
 35228|       "dll_verified_status": "signature_verified_declared",
 35229|       "revitlookup_referenced": null,
 35230|       "revitlookup_requires_document_context": null
 35231|     },
 35232|     {
 35233|       "source": "Autodesk.Revit.DB.ConnectionValidationWarning",
 35234|       "target": null,
 35235|       "member_name": "GetParts",
 35236|       "member_kind": "method",
 35237|       "edge_type": "RETURNS_ELEMENT_IDS",
 35238|       "confidence": "unknown_reference",
 35239|       "confidence_tier": "unverified_reference",
 35240|       "target_resolution": "none",
 35241|       "evidence": [
 35242|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 35243|       ],
 35244|       "source_url": "https://www.revitapidocs.com/2025/5cd40e6c-3912-6189-87bf-9eb7d9e131dd.htm",
 35245|       "dll_signature_verified": true,
 35246|       "dll_relationship_scope": "declared",
 35247|       "dll_semantic_verified": null,
 35248|       "dll_verified_status": "signature_verified_declared",
 35249|       "revitlookup_referenced": null,
 35250|       "revitlookup_requires_document_context": null
 35251|     },
 35252|     {
 35253|       "source": "Autodesk.Revit.DB.Connector",
 35254|       "target": "Autodesk.Revit.DB.ConnectorSet",
 35255|       "member_name": "AllRefs",
 35256|       "member_kind": "property",
 35257|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35258|       "confidence": "direct_return_type",
 35259|       "confidence_tier": "unverified_reference",
 35260|       "target_resolution": "exact",
 35261|       "evidence": [
 35262|         "return type 'ConnectorSet' directly names a Revit DB object type"
 35263|       ],
 35264|       "source_url": "https://www.revitapidocs.com/2025/bfd0a83e-c6a4-cec6-8428-b5b8b4357ee5.htm",
 35265|       "dll_signature_verified": true,
 35266|       "dll_relationship_scope": "declared",
 35267|       "dll_semantic_verified": null,
 35268|       "dll_verified_status": "signature_verified_declared",
 35269|       "revitlookup_referenced": null,
 35270|       "revitlookup_requires_document_context": null
 35271|     },
 35272|     {
 35273|       "source": "Autodesk.Revit.DB.Connector",
 35274|       "target": "Autodesk.Revit.DB.ConnectorManager",
 35275|       "member_name": "ConnectorManager",
 35276|       "member_kind": "property",
 35277|       "edge_type": "REFERENCES",
 35278|       "confidence": "direct_return_type",
 35279|       "confidence_tier": "core",
 35280|       "target_resolution": "exact",
 35281|       "evidence": [
 35282|         "return type 'ConnectorManager' directly names a Revit DB object type"
 35283|       ],
 35284|       "source_url": "https://www.revitapidocs.com/2025/61339b71-5d90-c53d-bec4-2209bab97787.htm",
 35285|       "dll_signature_verified": true,
 35286|       "dll_relationship_scope": "declared",
 35287|       "dll_semantic_verified": null,
 35288|       "dll_verified_status": "signature_verified_declared",
 35289|       "revitlookup_referenced": null,
 35290|       "revitlookup_requires_document_context": null
 35291|     },
 35292|     {
 35293|       "source": "Autodesk.Revit.DB.Connector",
 35294|       "target": "Autodesk.Revit.DB.MEPSystem",
 35295|       "member_name": "MEPSystem",
 35296|       "member_kind": "property",
 35297|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35298|       "confidence": "direct_return_type",
 35299|       "confidence_tier": "unverified_reference",
 35300|       "target_resolution": "exact",
 35301|       "evidence": [
 35302|         "return type 'MEPSystem' directly names a Revit DB object type"
 35303|       ],
 35304|       "source_url": "https://www.revitapidocs.com/2025/20d05e83-1b7f-a7d5-3498-8c6b8627ff67.htm",
 35305|       "dll_signature_verified": true,
 35306|       "dll_relationship_scope": "declared",
 35307|       "dll_semantic_verified": null,
 35308|       "dll_verified_status": "signature_verified_declared",
 35309|       "revitlookup_referenced": null,
 35310|       "revitlookup_requires_document_context": null
 35311|     },
 35312|     {
 35313|       "source": "Autodesk.Revit.DB.Connector",
 35314|       "target": "Autodesk.Revit.DB.Element",
 35315|       "member_name": "Owner",
 35316|       "member_kind": "property",
 35317|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35318|       "confidence": "direct_return_type",
 35319|       "confidence_tier": "unverified_reference",
 35320|       "target_resolution": "exact",
 35321|       "evidence": [
 35322|         "return type 'Element' directly names a Revit DB object type"
 35323|       ],
 35324|       "source_url": "https://www.revitapidocs.com/2025/8a4a393c-f2e1-0a23-d5b2-ea9680f4fbf5.htm",
 35325|       "dll_signature_verified": true,
 35326|       "dll_relationship_scope": "declared",
 35327|       "dll_semantic_verified": null,
 35328|       "dll_verified_status": "signature_verified_declared",
 35329|       "revitlookup_referenced": null,
 35330|       "revitlookup_requires_document_context": null
 35331|     },
 35332|     {
 35333|       "source": "Autodesk.Revit.DB.Connector",
 35334|       "target": "Autodesk.Revit.DB.FabricationConnectorInfo",
 35335|       "member_name": "GetFabricationConnectorInfo",
 35336|       "member_kind": "method",
 35337|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35338|       "confidence": "direct_return_type",
 35339|       "confidence_tier": "unverified_reference",
 35340|       "target_resolution": "exact",
 35341|       "evidence": [
 35342|         "return type 'FabricationConnectorInfo' directly names a Revit DB object type"
 35343|       ],
 35344|       "source_url": "https://www.revitapidocs.com/2025/6cff3851-aad9-ae38-fdc5-4f3554d03709.htm",
 35345|       "dll_signature_verified": true,
 35346|       "dll_relationship_scope": "declared",
 35347|       "dll_semantic_verified": null,
 35348|       "dll_verified_status": "signature_verified_declared",
 35349|       "revitlookup_referenced": null,
 35350|       "revitlookup_requires_document_context": null
 35351|     },
 35352|     {
 35353|       "source": "Autodesk.Revit.DB.Connector",
 35354|       "target": "Autodesk.Revit.DB.MEPConnectorInfo",
 35355|       "member_name": "GetMEPConnectorInfo",
 35356|       "member_kind": "method",
 35357|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35358|       "confidence": "direct_return_type",
 35359|       "confidence_tier": "unverified_reference",
 35360|       "target_resolution": "exact",
 35361|       "evidence": [
 35362|         "return type 'MEPConnectorInfo' directly names a Revit DB object type"
 35363|       ],
 35364|       "source_url": "https://www.revitapidocs.com/2025/fc07b9ff-9101-90d5-740d-0c19357c6919.htm",
 35365|       "dll_signature_verified": true,
 35366|       "dll_relationship_scope": "declared",
 35367|       "dll_semantic_verified": null,
 35368|       "dll_verified_status": "signature_verified_declared",
 35369|       "revitlookup_referenced": null,
 35370|       "revitlookup_requires_document_context": null
 35371|     },
 35372|     {
 35373|       "source": "Autodesk.Revit.DB.ConnectorElement",
 35374|       "target": null,
 35375|       "member_name": "ChangeHostReference",
 35376|       "member_kind": "method",
 35377|       "edge_type": "HOSTED_BY",
 35378|       "confidence": "name_only_candidate",
 35379|       "confidence_tier": "likely",
 35380|       "target_resolution": "none",
 35381|       "evidence": [
 35382|         "member name 'ChangeHostReference' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 35383|       ],
 35384|       "source_url": "https://www.revitapidocs.com/2025/ab25dbec-9993-98b3-78e6-527bbd27fd1c.htm",
 35385|       "dll_signature_verified": true,
 35386|       "dll_relationship_scope": "declared",
 35387|       "dll_semantic_verified": null,
 35388|       "dll_verified_status": "signature_verified_declared",
 35389|       "revitlookup_referenced": null,
 35390|       "revitlookup_requires_document_context": null
 35391|     },
 35392|     {
 35393|       "source": "Autodesk.Revit.DB.ConnectorElement",
 35394|       "target": null,
 35395|       "member_name": "ChangeHostReference",
 35396|       "member_kind": "method",
 35397|       "edge_type": "HOSTED_BY",
 35398|       "confidence": "name_only_candidate",
 35399|       "confidence_tier": "likely",
 35400|       "target_resolution": "none",
 35401|       "evidence": [
 35402|         "member name 'ChangeHostReference' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 35403|       ],
 35404|       "source_url": "https://www.revitapidocs.com/2025/1761f9f3-3967-9d02-befb-401d16fd100d.htm",
 35405|       "dll_signature_verified": true,
 35406|       "dll_relationship_scope": "declared",
 35407|       "dll_semantic_verified": null,
 35408|       "dll_verified_status": "signature_verified_declared",
 35409|       "revitlookup_referenced": null,
 35410|       "revitlookup_requires_document_context": null
 35411|     },
 35412|     {
 35413|       "source": "Autodesk.Revit.DB.ConnectorManager",
 35414|       "target": "Autodesk.Revit.DB.ConnectorSet",
 35415|       "member_name": "Connectors",
 35416|       "member_kind": "property",
 35417|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35418|       "confidence": "direct_return_type",
 35419|       "confidence_tier": "unverified_reference",
 35420|       "target_resolution": "exact",
 35421|       "evidence": [
 35422|         "return type 'ConnectorSet' directly names a Revit DB object type"
 35423|       ],
 35424|       "source_url": "https://www.revitapidocs.com/2025/ddefb4b0-881a-bb7a-6824-c610ac2d293b.htm",
 35425|       "dll_signature_verified": true,
 35426|       "dll_relationship_scope": "declared",
 35427|       "dll_semantic_verified": null,
 35428|       "dll_verified_status": "signature_verified_declared",
 35429|       "revitlookup_referenced": null,
 35430|       "revitlookup_requires_document_context": null
 35431|     },
 35432|     {
 35433|       "source": "Autodesk.Revit.DB.ConnectorManager",
 35434|       "target": "Autodesk.Revit.DB.Element",
 35435|       "member_name": "Owner",
 35436|       "member_kind": "property",
 35437|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35438|       "confidence": "direct_return_type",
 35439|       "confidence_tier": "unverified_reference",
 35440|       "target_resolution": "exact",
 35441|       "evidence": [
 35442|         "return type 'Element' directly names a Revit DB object type"
 35443|       ],
 35444|       "source_url": "https://www.revitapidocs.com/2025/606dbe85-2985-88c2-823e-f1e84d348bb5.htm",
 35445|       "dll_signature_verified": true,
 35446|       "dll_relationship_scope": "declared",
 35447|       "dll_semantic_verified": null,
 35448|       "dll_verified_status": "signature_verified_declared",
 35449|       "revitlookup_referenced": null,
 35450|       "revitlookup_requires_document_context": null
 35451|     },
 35452|     {
 35453|       "source": "Autodesk.Revit.DB.ConnectorManager",
 35454|       "target": "Autodesk.Revit.DB.ConnectorSet",
 35455|       "member_name": "UnusedConnectors",
 35456|       "member_kind": "property",
 35457|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35458|       "confidence": "direct_return_type",
 35459|       "confidence_tier": "unverified_reference",
 35460|       "target_resolution": "exact",
 35461|       "evidence": [
 35462|         "return type 'ConnectorSet' directly names a Revit DB object type"
 35463|       ],
 35464|       "source_url": "https://www.revitapidocs.com/2025/1cb54fcb-75c5-d09b-f4c3-026146fbd455.htm",
 35465|       "dll_signature_verified": true,
 35466|       "dll_relationship_scope": "declared",
 35467|       "dll_semantic_verified": null,
 35468|       "dll_verified_status": "signature_verified_declared",
 35469|       "revitlookup_referenced": null,
 35470|       "revitlookup_requires_document_context": null
 35471|     },
 35472|     {
 35473|       "source": "Autodesk.Revit.DB.ConnectorManager",
 35474|       "target": "Autodesk.Revit.DB.Connector",
 35475|       "member_name": "Lookup",
 35476|       "member_kind": "method",
 35477|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35478|       "confidence": "direct_return_type",
 35479|       "confidence_tier": "unverified_reference",
 35480|       "target_resolution": "exact",
 35481|       "evidence": [
 35482|         "return type 'Connector' directly names a Revit DB object type"
 35483|       ],
 35484|       "source_url": "https://www.revitapidocs.com/2025/346ec8af-1e85-6b68-6417-29a27a0d0978.htm",
 35485|       "dll_signature_verified": true,
 35486|       "dll_relationship_scope": "declared",
 35487|       "dll_semantic_verified": null,
 35488|       "dll_verified_status": "signature_verified_declared",
 35489|       "revitlookup_referenced": true,
 35490|       "revitlookup_requires_document_context": false
 35491|     },
 35492|     {
 35493|       "source": "Autodesk.Revit.DB.ConnectorSet",
 35494|       "target": "Autodesk.Revit.DB.ConnectorSetIterator",
 35495|       "member_name": "ForwardIterator",
 35496|       "member_kind": "method",
 35497|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 35498|       "confidence": "direct_return_type",
 35499|       "confidence_tier": "unverified_reference",
 35500|       "target_resolution": "exact",
```
