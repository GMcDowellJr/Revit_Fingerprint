# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 132 of 216
- Original line range: 51091-51490
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 51091|       "confidence": "direct_return_type",
 51092|       "confidence_tier": "unverified_reference",
 51093|       "target_resolution": "exact",
 51094|       "evidence": [
 51095|         "return type 'FamilySymbol' directly names a Revit DB object type"
 51096|       ],
 51097|       "source_url": "https://www.revitapidocs.com/2025/e74b59ea-3367-c980-030a-831c1733c4e3.htm",
 51098|       "dll_signature_verified": true,
 51099|       "dll_relationship_scope": "declared",
 51100|       "dll_semantic_verified": null,
 51101|       "dll_verified_status": "signature_verified_declared",
 51102|       "revitlookup_referenced": null,
 51103|       "revitlookup_requires_document_context": null
 51104|     },
 51105|     {
 51106|       "source": "Autodesk.Revit.DB.MEPFamilyConnectorInfo",
 51107|       "target": null,
 51108|       "member_name": "GetAssociateFamilyParameterId",
 51109|       "member_kind": "method",
 51110|       "edge_type": "HAS_PARAMETER",
 51111|       "confidence": "elementid_with_strong_name",
 51112|       "confidence_tier": "core",
 51113|       "target_resolution": "none",
 51114|       "evidence": [
 51115|         "member name 'GetAssociateFamilyParameterId' matches keyword pattern /Parameter/"
 51116|       ],
 51117|       "source_url": "https://www.revitapidocs.com/2025/0184cc98-e638-a351-8886-4f7ab3f76cd6.htm",
 51118|       "dll_signature_verified": true,
 51119|       "dll_relationship_scope": "declared",
 51120|       "dll_semantic_verified": null,
 51121|       "dll_verified_status": "signature_verified_declared",
 51122|       "revitlookup_referenced": null,
 51123|       "revitlookup_requires_document_context": null
 51124|     },
 51125|     {
 51126|       "source": "Autodesk.Revit.DB.MEPFamilyConnectorInfo",
 51127|       "target": "Autodesk.Revit.DB.ParameterValue",
 51128|       "member_name": "GetConnectorParameterValue",
 51129|       "member_kind": "method",
 51130|       "edge_type": "HAS_PARAMETER",
 51131|       "confidence": "direct_return_type",
 51132|       "confidence_tier": "core",
 51133|       "target_resolution": "exact",
 51134|       "evidence": [
 51135|         "return type 'ParameterValue' directly names a Revit DB object type"
 51136|       ],
 51137|       "source_url": "https://www.revitapidocs.com/2025/a46564d4-0e40-2a83-7c57-9560e9876db7.htm",
 51138|       "dll_signature_verified": true,
 51139|       "dll_relationship_scope": "declared",
 51140|       "dll_semantic_verified": null,
 51141|       "dll_verified_status": "signature_verified_declared",
 51142|       "revitlookup_referenced": null,
 51143|       "revitlookup_requires_document_context": null
 51144|     },
 51145|     {
 51146|       "source": "Autodesk.Revit.DB.MEPModel",
 51147|       "target": "Autodesk.Revit.DB.ConnectorManager",
 51148|       "member_name": "ConnectorManager",
 51149|       "member_kind": "property",
 51150|       "edge_type": "REFERENCES",
 51151|       "confidence": "direct_return_type",
 51152|       "confidence_tier": "core",
 51153|       "target_resolution": "exact",
 51154|       "evidence": [
 51155|         "return type 'ConnectorManager' directly names a Revit DB object type"
 51156|       ],
 51157|       "source_url": "https://www.revitapidocs.com/2025/ee6d27a2-5c57-8e13-f0c1-504028545220.htm",
 51158|       "dll_signature_verified": true,
 51159|       "dll_relationship_scope": "declared",
 51160|       "dll_semantic_verified": null,
 51161|       "dll_verified_status": "signature_verified_declared",
 51162|       "revitlookup_referenced": null,
 51163|       "revitlookup_requires_document_context": null
 51164|     },
 51165|     {
 51166|       "source": "Autodesk.Revit.DB.MEPModel",
 51167|       "target": "Autodesk.Revit.DB.Electrical.ElectricalSystem",
 51168|       "member_name": "GetAssignedElectricalSystems",
 51169|       "member_kind": "method",
 51170|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 51171|       "confidence": "needs_runtime_validation",
 51172|       "confidence_tier": "needs_validation",
 51173|       "target_resolution": "short_name_fallback",
 51174|       "evidence": [
 51175|         "return type 'ISet < ElectricalSystem >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 51176|       ],
 51177|       "source_url": "https://www.revitapidocs.com/2025/dbdec982-fe0c-ada0-bf5c-cc9d7967f6f0.htm",
 51178|       "dll_signature_verified": true,
 51179|       "dll_relationship_scope": "declared",
 51180|       "dll_semantic_verified": null,
 51181|       "dll_verified_status": "signature_verified_declared",
 51182|       "revitlookup_referenced": null,
 51183|       "revitlookup_requires_document_context": null
 51184|     },
 51185|     {
 51186|       "source": "Autodesk.Revit.DB.MEPModel",
 51187|       "target": "Autodesk.Revit.DB.Electrical.ElectricalSystem",
 51188|       "member_name": "GetElectricalSystems",
 51189|       "member_kind": "method",
 51190|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 51191|       "confidence": "needs_runtime_validation",
 51192|       "confidence_tier": "needs_validation",
 51193|       "target_resolution": "short_name_fallback",
 51194|       "evidence": [
 51195|         "return type 'ISet < ElectricalSystem >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 51196|       ],
 51197|       "source_url": "https://www.revitapidocs.com/2025/ef8e7119-9fa4-3024-d43d-591759b6098b.htm",
 51198|       "dll_signature_verified": true,
 51199|       "dll_relationship_scope": "declared",
 51200|       "dll_semantic_verified": null,
 51201|       "dll_verified_status": "signature_verified_declared",
 51202|       "revitlookup_referenced": null,
 51203|       "revitlookup_requires_document_context": null
 51204|     },
 51205|     {
 51206|       "source": "Autodesk.Revit.DB.MEPSystem",
 51207|       "target": "Autodesk.Revit.DB.FamilyInstance",
 51208|       "member_name": "BaseEquipment",
 51209|       "member_kind": "property",
 51210|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 51211|       "confidence": "direct_return_type",
 51212|       "confidence_tier": "unverified_reference",
 51213|       "target_resolution": "exact",
 51214|       "evidence": [
 51215|         "return type 'FamilyInstance' directly names a Revit DB object type"
 51216|       ],
 51217|       "source_url": "https://www.revitapidocs.com/2025/00d90c4f-1946-1069-7887-f12899846481.htm",
 51218|       "dll_signature_verified": true,
 51219|       "dll_relationship_scope": "declared",
 51220|       "dll_semantic_verified": null,
 51221|       "dll_verified_status": "signature_verified_declared",
 51222|       "revitlookup_referenced": null,
 51223|       "revitlookup_requires_document_context": null
 51224|     },
 51225|     {
 51226|       "source": "Autodesk.Revit.DB.MEPSystem",
 51227|       "target": "Autodesk.Revit.DB.Connector",
 51228|       "member_name": "BaseEquipmentConnector",
 51229|       "member_kind": "property",
 51230|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 51231|       "confidence": "direct_return_type",
 51232|       "confidence_tier": "unverified_reference",
 51233|       "target_resolution": "exact",
 51234|       "evidence": [
 51235|         "return type 'Connector' directly names a Revit DB object type"
 51236|       ],
 51237|       "source_url": "https://www.revitapidocs.com/2025/ba498d21-cdd8-ef07-e906-410443befc47.htm",
 51238|       "dll_signature_verified": true,
 51239|       "dll_relationship_scope": "declared",
 51240|       "dll_semantic_verified": null,
 51241|       "dll_verified_status": "signature_verified_declared",
 51242|       "revitlookup_referenced": null,
 51243|       "revitlookup_requires_document_context": null
 51244|     },
 51245|     {
 51246|       "source": "Autodesk.Revit.DB.MEPSystem",
 51247|       "target": "Autodesk.Revit.DB.ConnectorManager",
 51248|       "member_name": "ConnectorManager",
 51249|       "member_kind": "property",
 51250|       "edge_type": "REFERENCES",
 51251|       "confidence": "direct_return_type",
 51252|       "confidence_tier": "core",
 51253|       "target_resolution": "exact",
 51254|       "evidence": [
 51255|         "return type 'ConnectorManager' directly names a Revit DB object type"
 51256|       ],
 51257|       "source_url": "https://www.revitapidocs.com/2025/891c572c-e7de-8593-330d-5afc8aed9e63.htm",
 51258|       "dll_signature_verified": true,
 51259|       "dll_relationship_scope": "declared",
 51260|       "dll_semantic_verified": null,
 51261|       "dll_verified_status": "signature_verified_declared",
 51262|       "revitlookup_referenced": null,
 51263|       "revitlookup_requires_document_context": null
 51264|     },
 51265|     {
 51266|       "source": "Autodesk.Revit.DB.MEPSystem",
 51267|       "target": "Autodesk.Revit.DB.ElementSet",
 51268|       "member_name": "Elements",
 51269|       "member_kind": "property",
 51270|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 51271|       "confidence": "direct_return_type",
 51272|       "confidence_tier": "unverified_reference",
 51273|       "target_resolution": "exact",
 51274|       "evidence": [
 51275|         "return type 'ElementSet' directly names a Revit DB object type"
 51276|       ],
 51277|       "source_url": "https://www.revitapidocs.com/2025/c1cb0808-3b81-87a5-b804-84a733686f2d.htm",
 51278|       "dll_signature_verified": true,
 51279|       "dll_relationship_scope": "declared",
 51280|       "dll_semantic_verified": null,
 51281|       "dll_verified_status": "signature_verified_declared",
 51282|       "revitlookup_referenced": null,
 51283|       "revitlookup_requires_document_context": null
 51284|     },
 51285|     {
 51286|       "source": "Autodesk.Revit.DB.MEPSystem",
 51287|       "target": null,
 51288|       "member_name": "DivideSystem",
 51289|       "member_kind": "method",
 51290|       "edge_type": "RETURNS_ELEMENT_IDS",
 51291|       "confidence": "unknown_reference",
 51292|       "confidence_tier": "unverified_reference",
 51293|       "target_resolution": "none",
 51294|       "evidence": [
 51295|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 51296|       ],
 51297|       "source_url": "https://www.revitapidocs.com/2025/1bb1e7d5-a9f6-0c2d-e413-064bd4aa2c02.htm",
 51298|       "dll_signature_verified": true,
 51299|       "dll_relationship_scope": "declared",
 51300|       "dll_semantic_verified": null,
 51301|       "dll_verified_status": "signature_verified_declared",
 51302|       "revitlookup_referenced": null,
 51303|       "revitlookup_requires_document_context": null
 51304|     },
 51305|     {
 51306|       "source": "Autodesk.Revit.DB.MEPSystem",
 51307|       "target": "Autodesk.Revit.DB.Mechanical.MEPSection",
 51308|       "member_name": "GetSectionByIndex",
 51309|       "member_kind": "method",
 51310|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 51311|       "confidence": "direct_return_type",
 51312|       "confidence_tier": "unverified_reference",
 51313|       "target_resolution": "short_name_fallback",
 51314|       "evidence": [
 51315|         "return type 'MEPSection' directly names a Revit DB object type"
 51316|       ],
 51317|       "source_url": "https://www.revitapidocs.com/2025/dd53cbe2-37e6-19a1-e627-74a2aacb3433.htm",
 51318|       "dll_signature_verified": true,
 51319|       "dll_relationship_scope": "declared",
 51320|       "dll_semantic_verified": null,
 51321|       "dll_verified_status": "signature_verified_declared",
 51322|       "revitlookup_referenced": true,
 51323|       "revitlookup_requires_document_context": false
 51324|     },
 51325|     {
 51326|       "source": "Autodesk.Revit.DB.MEPSystem",
 51327|       "target": "Autodesk.Revit.DB.Mechanical.MEPSection",
 51328|       "member_name": "GetSectionByNumber",
 51329|       "member_kind": "method",
 51330|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 51331|       "confidence": "direct_return_type",
 51332|       "confidence_tier": "unverified_reference",
 51333|       "target_resolution": "short_name_fallback",
 51334|       "evidence": [
 51335|         "return type 'MEPSection' directly names a Revit DB object type"
 51336|       ],
 51337|       "source_url": "https://www.revitapidocs.com/2025/11cc783a-32f8-b0e0-2da6-5ecc6b9e57a6.htm",
 51338|       "dll_signature_verified": true,
 51339|       "dll_relationship_scope": "declared",
 51340|       "dll_semantic_verified": null,
 51341|       "dll_verified_status": "signature_verified_declared",
 51342|       "revitlookup_referenced": true,
 51343|       "revitlookup_requires_document_context": false
 51344|     },
 51345|     {
 51346|       "source": "Autodesk.Revit.DB.MEPSystemType",
 51347|       "target": "Autodesk.Revit.DB.Level",
 51348|       "member_name": "CalculationLevel",
 51349|       "member_kind": "property",
 51350|       "edge_type": "ASSIGNED_TO_LEVEL",
 51351|       "confidence": "name_only_candidate",
 51352|       "confidence_tier": "likely",
 51353|       "target_resolution": "exact",
 51354|       "evidence": [
 51355|         "member name 'CalculationLevel' matches keyword pattern /Level/ but return type 'SystemCalculationLevel' gives no type-level confirmation"
 51356|       ],
 51357|       "source_url": "https://www.revitapidocs.com/2025/76631989-d84f-3954-8743-f42b58ba4f43.htm",
 51358|       "dll_signature_verified": true,
 51359|       "dll_relationship_scope": "declared",
 51360|       "dll_semantic_verified": null,
 51361|       "dll_verified_status": "signature_verified_declared",
 51362|       "revitlookup_referenced": null,
 51363|       "revitlookup_requires_document_context": null
 51364|     },
 51365|     {
 51366|       "source": "Autodesk.Revit.DB.MEPSystemType",
 51367|       "target": "Autodesk.Revit.DB.FillPatternElement",
 51368|       "member_name": "FillPatternId",
 51369|       "member_kind": "property",
 51370|       "edge_type": "USES_FILL_PATTERN",
 51371|       "confidence": "elementid_with_strong_name",
 51372|       "confidence_tier": "core",
 51373|       "target_resolution": "exact",
 51374|       "evidence": [
 51375|         "member name 'FillPatternId' matches keyword pattern /FillPattern/"
 51376|       ],
 51377|       "source_url": "https://www.revitapidocs.com/2025/cc5413da-3984-5580-8303-70d790118cf3.htm",
 51378|       "dll_signature_verified": true,
 51379|       "dll_relationship_scope": "declared",
 51380|       "dll_semantic_verified": null,
 51381|       "dll_verified_status": "signature_verified_declared",
 51382|       "revitlookup_referenced": null,
 51383|       "revitlookup_requires_document_context": null
 51384|     },
 51385|     {
 51386|       "source": "Autodesk.Revit.DB.MEPSystemType",
 51387|       "target": "Autodesk.Revit.DB.LinePatternElement",
 51388|       "member_name": "LinePatternId",
 51389|       "member_kind": "property",
 51390|       "edge_type": "USES_LINE_PATTERN",
 51391|       "confidence": "elementid_with_strong_name",
 51392|       "confidence_tier": "core",
 51393|       "target_resolution": "exact",
 51394|       "evidence": [
 51395|         "member name 'LinePatternId' matches keyword pattern /LinePattern/"
 51396|       ],
 51397|       "source_url": "https://www.revitapidocs.com/2025/99830c67-ba7a-e28b-0d65-18b57c7f93af.htm",
 51398|       "dll_signature_verified": true,
 51399|       "dll_relationship_scope": "declared",
 51400|       "dll_semantic_verified": null,
 51401|       "dll_verified_status": "signature_verified_declared",
 51402|       "revitlookup_referenced": null,
 51403|       "revitlookup_requires_document_context": null
 51404|     },
 51405|     {
 51406|       "source": "Autodesk.Revit.DB.MEPSystemType",
 51407|       "target": "Autodesk.Revit.DB.Material",
 51408|       "member_name": "MaterialId",
 51409|       "member_kind": "property",
 51410|       "edge_type": "USES_MATERIAL",
 51411|       "confidence": "elementid_with_strong_name",
 51412|       "confidence_tier": "core",
 51413|       "target_resolution": "exact",
 51414|       "evidence": [
 51415|         "member name 'MaterialId' matches keyword pattern /Material/"
 51416|       ],
 51417|       "source_url": "https://www.revitapidocs.com/2025/8da78a66-2b7d-2231-a112-64cf1cee6c19.htm",
 51418|       "dll_signature_verified": true,
 51419|       "dll_relationship_scope": "declared",
 51420|       "dll_semantic_verified": null,
 51421|       "dll_verified_status": "signature_verified_declared",
 51422|       "revitlookup_referenced": null,
 51423|       "revitlookup_requires_document_context": null
 51424|     },
 51425|     {
 51426|       "source": "Autodesk.Revit.DB.Mesh",
 51427|       "target": "Autodesk.Revit.DB.Material",
 51428|       "member_name": "MaterialElementId",
 51429|       "member_kind": "property",
 51430|       "edge_type": "USES_MATERIAL",
 51431|       "confidence": "elementid_with_strong_name",
 51432|       "confidence_tier": "core",
 51433|       "target_resolution": "exact",
 51434|       "evidence": [
 51435|         "member name 'MaterialElementId' matches keyword pattern /Material/"
 51436|       ],
 51437|       "source_url": "https://www.revitapidocs.com/2025/284bb1fe-561d-a534-16ef-cf371e4da848.htm",
 51438|       "dll_signature_verified": true,
 51439|       "dll_relationship_scope": "declared",
 51440|       "dll_semantic_verified": null,
 51441|       "dll_verified_status": "signature_verified_declared",
 51442|       "revitlookup_referenced": null,
 51443|       "revitlookup_requires_document_context": null
 51444|     },
 51445|     {
 51446|       "source": "Autodesk.Revit.DB.MeshFromGeometryOperationResult",
 51447|       "target": "Autodesk.Revit.DB.MeshFromGeometryOperationIssue",
 51448|       "member_name": "GetIssues",
 51449|       "member_kind": "method",
 51450|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 51451|       "confidence": "needs_runtime_validation",
 51452|       "confidence_tier": "needs_validation",
 51453|       "target_resolution": "exact",
 51454|       "evidence": [
 51455|         "return type 'IList < MeshFromGeometryOperationIssue >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 51456|       ],
 51457|       "source_url": "https://www.revitapidocs.com/2025/0a0dded2-d7d7-9d7e-424d-ffb09051a690.htm",
 51458|       "dll_signature_verified": true,
 51459|       "dll_relationship_scope": "declared",
 51460|       "dll_semantic_verified": null,
 51461|       "dll_verified_status": "signature_verified_declared",
 51462|       "revitlookup_referenced": null,
 51463|       "revitlookup_requires_document_context": null
 51464|     },
 51465|     {
 51466|       "source": "Autodesk.Revit.DB.MeshFromGeometryOperationResult",
 51467|       "target": "Autodesk.Revit.DB.Mesh",
 51468|       "member_name": "GetMesh",
 51469|       "member_kind": "method",
 51470|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 51471|       "confidence": "direct_return_type",
 51472|       "confidence_tier": "unverified_reference",
 51473|       "target_resolution": "exact",
 51474|       "evidence": [
 51475|         "return type 'Mesh' directly names a Revit DB object type"
 51476|       ],
 51477|       "source_url": "https://www.revitapidocs.com/2025/bd2901fe-9510-612a-f383-cb8caaee62ed.htm",
 51478|       "dll_signature_verified": true,
 51479|       "dll_relationship_scope": "declared",
 51480|       "dll_semantic_verified": null,
 51481|       "dll_verified_status": "signature_verified_declared",
 51482|       "revitlookup_referenced": null,
 51483|       "revitlookup_requires_document_context": null
 51484|     },
 51485|     {
 51486|       "source": "Autodesk.Revit.DB.ModelCurve",
 51487|       "target": "Autodesk.Revit.DB.GraphicsStyle",
 51488|       "member_name": "Subcategory",
 51489|       "member_kind": "property",
 51490|       "edge_type": "REFERENCES",
```
