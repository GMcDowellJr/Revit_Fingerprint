# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 196 of 216
- Original line range: 76051-76450
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 76051|       "member_kind": "method",
 76052|       "edge_type": "RETURNS_ELEMENT_IDS",
 76053|       "confidence": "unknown_reference",
 76054|       "confidence_tier": "unverified_reference",
 76055|       "target_resolution": "none",
 76056|       "evidence": [
 76057|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 76058|       ],
 76059|       "source_url": "https://www.revitapidocs.com/2025/8305d265-b824-98d7-2084-8a8eb0c49208.htm",
 76060|       "dll_signature_verified": true,
 76061|       "dll_relationship_scope": "declared",
 76062|       "dll_semantic_verified": null,
 76063|       "dll_verified_status": "signature_verified_declared",
 76064|       "revitlookup_referenced": null,
 76065|       "revitlookup_requires_document_context": null
 76066|     },
 76067|     {
 76068|       "source": "Autodesk.Revit.DB.Mechanical.MEPAnalyticalSystem",
 76069|       "target": "Autodesk.Revit.DB.Mechanical.AirSystemData",
 76070|       "member_name": "GetAirSystemData",
 76071|       "member_kind": "method",
 76072|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76073|       "confidence": "direct_return_type",
 76074|       "confidence_tier": "unverified_reference",
 76075|       "target_resolution": "short_name_fallback",
 76076|       "evidence": [
 76077|         "return type 'AirSystemData' directly names a Revit DB object type"
 76078|       ],
 76079|       "source_url": "https://www.revitapidocs.com/2025/aae8a5b1-1302-fac4-1bb2-799179d3d18e.htm",
 76080|       "dll_signature_verified": true,
 76081|       "dll_relationship_scope": "declared",
 76082|       "dll_semantic_verified": null,
 76083|       "dll_verified_status": "signature_verified_declared",
 76084|       "revitlookup_referenced": null,
 76085|       "revitlookup_requires_document_context": null
 76086|     },
 76087|     {
 76088|       "source": "Autodesk.Revit.DB.Mechanical.MEPAnalyticalSystem",
 76089|       "target": "Autodesk.Revit.DB.Mechanical.WaterLoopData",
 76090|       "member_name": "GetWaterLoopData",
 76091|       "member_kind": "method",
 76092|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76093|       "confidence": "direct_return_type",
 76094|       "confidence_tier": "unverified_reference",
 76095|       "target_resolution": "short_name_fallback",
 76096|       "evidence": [
 76097|         "return type 'WaterLoopData' directly names a Revit DB object type"
 76098|       ],
 76099|       "source_url": "https://www.revitapidocs.com/2025/ef47f29c-aec1-03d7-385b-5c238302e8fd.htm",
 76100|       "dll_signature_verified": true,
 76101|       "dll_relationship_scope": "declared",
 76102|       "dll_semantic_verified": null,
 76103|       "dll_verified_status": "signature_verified_declared",
 76104|       "revitlookup_referenced": null,
 76105|       "revitlookup_requires_document_context": null
 76106|     },
 76107|     {
 76108|       "source": "Autodesk.Revit.DB.Mechanical.MEPBuildingConstruction",
 76109|       "target": "Autodesk.Revit.DB.Construction",
 76110|       "member_name": "GetBuildingConstruction",
 76111|       "member_kind": "method",
 76112|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76113|       "confidence": "direct_return_type",
 76114|       "confidence_tier": "unverified_reference",
 76115|       "target_resolution": "exact",
 76116|       "evidence": [
 76117|         "return type 'Construction' directly names a Revit DB object type"
 76118|       ],
 76119|       "source_url": "https://www.revitapidocs.com/2025/c4a62707-01bd-a107-ccad-5cf499d79e25.htm",
 76120|       "dll_signature_verified": true,
 76121|       "dll_relationship_scope": "declared",
 76122|       "dll_semantic_verified": null,
 76123|       "dll_verified_status": "signature_verified_declared",
 76124|       "revitlookup_referenced": null,
 76125|       "revitlookup_requires_document_context": null
 76126|     },
 76127|     {
 76128|       "source": "Autodesk.Revit.DB.Mechanical.MEPBuildingConstruction",
 76129|       "target": "Autodesk.Revit.DB.Construction",
 76130|       "member_name": "GetConstructions",
 76131|       "member_kind": "method",
 76132|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76133|       "confidence": "needs_runtime_validation",
 76134|       "confidence_tier": "needs_validation",
 76135|       "target_resolution": "exact",
 76136|       "evidence": [
 76137|         "return type 'ICollection < Construction >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 76138|       ],
 76139|       "source_url": "https://www.revitapidocs.com/2025/6b06dc30-43d1-477c-b525-334065bd43e9.htm",
 76140|       "dll_signature_verified": true,
 76141|       "dll_relationship_scope": "declared",
 76142|       "dll_semantic_verified": null,
 76143|       "dll_verified_status": "signature_verified_declared",
 76144|       "revitlookup_referenced": null,
 76145|       "revitlookup_requires_document_context": null
 76146|     },
 76147|     {
 76148|       "source": "Autodesk.Revit.DB.Mechanical.MEPBuildingConstructionSet",
 76149|       "target": "Autodesk.Revit.DB.Mechanical.MEPBuildingConstructionSetIterator",
 76150|       "member_name": "ForwardIterator",
 76151|       "member_kind": "method",
 76152|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76153|       "confidence": "direct_return_type",
 76154|       "confidence_tier": "unverified_reference",
 76155|       "target_resolution": "short_name_fallback",
 76156|       "evidence": [
 76157|         "return type 'MEPBuildingConstructionSetIterator' directly names a Revit DB object type"
 76158|       ],
 76159|       "source_url": "https://www.revitapidocs.com/2025/3b49b235-96f1-7215-004f-f0b779ba1571.htm",
 76160|       "dll_signature_verified": true,
 76161|       "dll_relationship_scope": "declared",
 76162|       "dll_semantic_verified": null,
 76163|       "dll_verified_status": "signature_verified_declared",
 76164|       "revitlookup_referenced": null,
 76165|       "revitlookup_requires_document_context": null
 76166|     },
 76167|     {
 76168|       "source": "Autodesk.Revit.DB.Mechanical.MEPBuildingConstructionSet",
 76169|       "target": "Autodesk.Revit.DB.Mechanical.MEPBuildingConstructionSetIterator",
 76170|       "member_name": "ReverseIterator",
 76171|       "member_kind": "method",
 76172|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76173|       "confidence": "direct_return_type",
 76174|       "confidence_tier": "unverified_reference",
 76175|       "target_resolution": "short_name_fallback",
 76176|       "evidence": [
 76177|         "return type 'MEPBuildingConstructionSetIterator' directly names a Revit DB object type"
 76178|       ],
 76179|       "source_url": "https://www.revitapidocs.com/2025/66b3618d-614c-ba19-7cf6-ca0ebb648ecf.htm",
 76180|       "dll_signature_verified": true,
 76181|       "dll_relationship_scope": "declared",
 76182|       "dll_semantic_verified": null,
 76183|       "dll_verified_status": "signature_verified_declared",
 76184|       "revitlookup_referenced": null,
 76185|       "revitlookup_requires_document_context": null
 76186|     },
 76187|     {
 76188|       "source": "Autodesk.Revit.DB.Mechanical.MEPHiddenLineSettings",
 76189|       "target": null,
 76190|       "member_name": "LineStyle",
 76191|       "member_kind": "property",
 76192|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 76193|       "confidence": "unknown_reference",
 76194|       "confidence_tier": "unverified_reference",
 76195|       "target_resolution": "none",
 76196|       "evidence": [
 76197|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 76198|       ],
 76199|       "source_url": "https://www.revitapidocs.com/2025/2c028b70-ea27-ff38-0788-f2c4dd211bd5.htm",
 76200|       "dll_signature_verified": true,
 76201|       "dll_relationship_scope": "declared",
 76202|       "dll_semantic_verified": null,
 76203|       "dll_verified_status": "signature_verified_declared",
 76204|       "revitlookup_referenced": null,
 76205|       "revitlookup_requires_document_context": null
 76206|     },
 76207|     {
 76208|       "source": "Autodesk.Revit.DB.Mechanical.MEPSection",
 76209|       "target": null,
 76210|       "member_name": "GetElementIds",
 76211|       "member_kind": "method",
 76212|       "edge_type": "RETURNS_ELEMENT_IDS",
 76213|       "confidence": "unknown_reference",
 76214|       "confidence_tier": "unverified_reference",
 76215|       "target_resolution": "none",
 76216|       "evidence": [
 76217|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 76218|       ],
 76219|       "source_url": "https://www.revitapidocs.com/2025/a09a0a4c-d28d-c0d4-ed85-b2481a0ac9dd.htm",
 76220|       "dll_signature_verified": true,
 76221|       "dll_relationship_scope": "declared",
 76222|       "dll_semantic_verified": null,
 76223|       "dll_verified_status": "signature_verified_declared",
 76224|       "revitlookup_referenced": true,
 76225|       "revitlookup_requires_document_context": false
 76226|     },
 76227|     {
 76228|       "source": "Autodesk.Revit.DB.Mechanical.MEPSpaceConstruction",
 76229|       "target": "Autodesk.Revit.DB.Mechanical.MEPBuildingConstruction",
 76230|       "member_name": "CurrentConstruction",
 76231|       "member_kind": "property",
 76232|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76233|       "confidence": "direct_return_type",
 76234|       "confidence_tier": "unverified_reference",
 76235|       "target_resolution": "short_name_fallback",
 76236|       "evidence": [
 76237|         "return type 'MEPBuildingConstruction' directly names a Revit DB object type"
 76238|       ],
 76239|       "source_url": "https://www.revitapidocs.com/2025/9343345f-f3ce-474e-88f8-d0b709d55a84.htm",
 76240|       "dll_signature_verified": true,
 76241|       "dll_relationship_scope": "declared",
 76242|       "dll_semantic_verified": null,
 76243|       "dll_verified_status": "signature_verified_declared",
 76244|       "revitlookup_referenced": null,
 76245|       "revitlookup_requires_document_context": null
 76246|     },
 76247|     {
 76248|       "source": "Autodesk.Revit.DB.Mechanical.MEPSpaceConstruction",
 76249|       "target": "Autodesk.Revit.DB.Mechanical.MEPBuildingConstructionSet",
 76250|       "member_name": "SpaceConstructions",
 76251|       "member_kind": "property",
 76252|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76253|       "confidence": "direct_return_type",
 76254|       "confidence_tier": "unverified_reference",
 76255|       "target_resolution": "short_name_fallback",
 76256|       "evidence": [
 76257|         "return type 'MEPBuildingConstructionSet' directly names a Revit DB object type"
 76258|       ],
 76259|       "source_url": "https://www.revitapidocs.com/2025/48f24c65-a46d-9c62-3b8b-1c5233bd7d65.htm",
 76260|       "dll_signature_verified": true,
 76261|       "dll_relationship_scope": "declared",
 76262|       "dll_semantic_verified": null,
 76263|       "dll_verified_status": "signature_verified_declared",
 76264|       "revitlookup_referenced": null,
 76265|       "revitlookup_requires_document_context": null
 76266|     },
 76267|     {
 76268|       "source": "Autodesk.Revit.DB.Mechanical.MEPSpaceConstruction",
 76269|       "target": "Autodesk.Revit.DB.Mechanical.MEPBuildingConstruction",
 76270|       "member_name": "DuplicateConstruction",
 76271|       "member_kind": "method",
 76272|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76273|       "confidence": "direct_return_type",
 76274|       "confidence_tier": "unverified_reference",
 76275|       "target_resolution": "short_name_fallback",
 76276|       "evidence": [
 76277|         "return type 'MEPBuildingConstruction' directly names a Revit DB object type"
 76278|       ],
 76279|       "source_url": "https://www.revitapidocs.com/2025/e472b09f-7080-381d-7b27-18257366ca3a.htm",
 76280|       "dll_signature_verified": true,
 76281|       "dll_relationship_scope": "declared",
 76282|       "dll_semantic_verified": null,
 76283|       "dll_verified_status": "signature_verified_declared",
 76284|       "revitlookup_referenced": null,
 76285|       "revitlookup_requires_document_context": null
 76286|     },
 76287|     {
 76288|       "source": "Autodesk.Revit.DB.Mechanical.MEPSpaceConstruction",
 76289|       "target": "Autodesk.Revit.DB.Mechanical.MEPBuildingConstruction",
 76290|       "member_name": "NewConstruction",
 76291|       "member_kind": "method",
 76292|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76293|       "confidence": "direct_return_type",
 76294|       "confidence_tier": "unverified_reference",
 76295|       "target_resolution": "short_name_fallback",
 76296|       "evidence": [
 76297|         "return type 'MEPBuildingConstruction' directly names a Revit DB object type"
 76298|       ],
 76299|       "source_url": "https://www.revitapidocs.com/2025/dbf60a91-d9f9-1a5b-2437-106001715233.htm",
 76300|       "dll_signature_verified": true,
 76301|       "dll_relationship_scope": "declared",
 76302|       "dll_semantic_verified": null,
 76303|       "dll_verified_status": "signature_verified_declared",
 76304|       "revitlookup_referenced": null,
 76305|       "revitlookup_requires_document_context": null
 76306|     },
 76307|     {
 76308|       "source": "Autodesk.Revit.DB.Mechanical.Space",
 76309|       "target": "Autodesk.Revit.DB.Architecture.Room",
 76310|       "member_name": "Room",
 76311|       "member_kind": "property",
 76312|       "edge_type": "REFERENCES",
 76313|       "confidence": "direct_return_type",
 76314|       "confidence_tier": "core",
 76315|       "target_resolution": "exact",
 76316|       "evidence": [
 76317|         "return type 'Room' directly names a Revit DB object type"
 76318|       ],
 76319|       "source_url": "https://www.revitapidocs.com/2025/e6b482cd-2466-0bc0-77ca-c40d2adaa3c7.htm",
 76320|       "dll_signature_verified": true,
 76321|       "dll_relationship_scope": "declared",
 76322|       "dll_semantic_verified": null,
 76323|       "dll_verified_status": "signature_verified_declared",
 76324|       "revitlookup_referenced": null,
 76325|       "revitlookup_requires_document_context": null
 76326|     },
 76327|     {
 76328|       "source": "Autodesk.Revit.DB.Mechanical.Space",
 76329|       "target": "Autodesk.Revit.DB.Mechanical.MEPSpaceConstruction",
 76330|       "member_name": "SpaceConstruction",
 76331|       "member_kind": "property",
 76332|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76333|       "confidence": "direct_return_type",
 76334|       "confidence_tier": "unverified_reference",
 76335|       "target_resolution": "short_name_fallback",
 76336|       "evidence": [
 76337|         "return type 'MEPSpaceConstruction' directly names a Revit DB object type"
 76338|       ],
 76339|       "source_url": "https://www.revitapidocs.com/2025/28db4dc0-4c8e-2d93-11aa-6a1b9f1da589.htm",
 76340|       "dll_signature_verified": true,
 76341|       "dll_relationship_scope": "declared",
 76342|       "dll_semantic_verified": null,
 76343|       "dll_verified_status": "signature_verified_declared",
 76344|       "revitlookup_referenced": null,
 76345|       "revitlookup_requires_document_context": null
 76346|     },
 76347|     {
 76348|       "source": "Autodesk.Revit.DB.Mechanical.Space",
 76349|       "target": null,
 76350|       "member_name": "SpaceTypeId",
 76351|       "member_kind": "property",
 76352|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 76353|       "confidence": "unknown_reference",
 76354|       "confidence_tier": "unverified_reference",
 76355|       "target_resolution": "none",
 76356|       "evidence": [
 76357|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 76358|       ],
 76359|       "source_url": "https://www.revitapidocs.com/2025/51ca46cc-b91f-68cc-3960-4a43bd41a154.htm",
 76360|       "dll_signature_verified": true,
 76361|       "dll_relationship_scope": "declared",
 76362|       "dll_semantic_verified": null,
 76363|       "dll_verified_status": "signature_verified_declared",
 76364|       "revitlookup_referenced": null,
 76365|       "revitlookup_requires_document_context": null
 76366|     },
 76367|     {
 76368|       "source": "Autodesk.Revit.DB.Mechanical.Space",
 76369|       "target": "Autodesk.Revit.DB.Level",
 76370|       "member_name": "UpperLimit",
 76371|       "member_kind": "property",
 76372|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76373|       "confidence": "direct_return_type",
 76374|       "confidence_tier": "unverified_reference",
 76375|       "target_resolution": "exact",
 76376|       "evidence": [
 76377|         "return type 'Level' directly names a Revit DB object type"
 76378|       ],
 76379|       "source_url": "https://www.revitapidocs.com/2025/0ade95ab-f644-7255-0d11-6a28ad7c7cab.htm",
 76380|       "dll_signature_verified": true,
 76381|       "dll_relationship_scope": "declared",
 76382|       "dll_semantic_verified": null,
 76383|       "dll_verified_status": "signature_verified_declared",
 76384|       "revitlookup_referenced": null,
 76385|       "revitlookup_requires_document_context": null
 76386|     },
 76387|     {
 76388|       "source": "Autodesk.Revit.DB.Mechanical.Space",
 76389|       "target": "Autodesk.Revit.DB.Mechanical.Zone",
 76390|       "member_name": "Zone",
 76391|       "member_kind": "property",
 76392|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76393|       "confidence": "direct_return_type",
 76394|       "confidence_tier": "unverified_reference",
 76395|       "target_resolution": "short_name_fallback",
 76396|       "evidence": [
 76397|         "return type 'Zone' directly names a Revit DB object type"
 76398|       ],
 76399|       "source_url": "https://www.revitapidocs.com/2025/4189677f-4334-de6e-c36d-07fd2ae07cd2.htm",
 76400|       "dll_signature_verified": true,
 76401|       "dll_relationship_scope": "declared",
 76402|       "dll_semantic_verified": null,
 76403|       "dll_verified_status": "signature_verified_declared",
 76404|       "revitlookup_referenced": null,
 76405|       "revitlookup_requires_document_context": null
 76406|     },
 76407|     {
 76408|       "source": "Autodesk.Revit.DB.Mechanical.SpaceSet",
 76409|       "target": "Autodesk.Revit.DB.Mechanical.SpaceSetIterator",
 76410|       "member_name": "ForwardIterator",
 76411|       "member_kind": "method",
 76412|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76413|       "confidence": "direct_return_type",
 76414|       "confidence_tier": "unverified_reference",
 76415|       "target_resolution": "short_name_fallback",
 76416|       "evidence": [
 76417|         "return type 'SpaceSetIterator' directly names a Revit DB object type"
 76418|       ],
 76419|       "source_url": "https://www.revitapidocs.com/2025/2dbd2aae-3da6-8f11-cfa8-32d919e3f2e2.htm",
 76420|       "dll_signature_verified": true,
 76421|       "dll_relationship_scope": "declared",
 76422|       "dll_semantic_verified": null,
 76423|       "dll_verified_status": "signature_verified_declared",
 76424|       "revitlookup_referenced": null,
 76425|       "revitlookup_requires_document_context": null
 76426|     },
 76427|     {
 76428|       "source": "Autodesk.Revit.DB.Mechanical.SpaceSet",
 76429|       "target": "Autodesk.Revit.DB.Mechanical.SpaceSetIterator",
 76430|       "member_name": "ReverseIterator",
 76431|       "member_kind": "method",
 76432|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 76433|       "confidence": "direct_return_type",
 76434|       "confidence_tier": "unverified_reference",
 76435|       "target_resolution": "short_name_fallback",
 76436|       "evidence": [
 76437|         "return type 'SpaceSetIterator' directly names a Revit DB object type"
 76438|       ],
 76439|       "source_url": "https://www.revitapidocs.com/2025/c8220f18-fab2-263b-4af6-2a2c23b6b0ed.htm",
 76440|       "dll_signature_verified": true,
 76441|       "dll_relationship_scope": "declared",
 76442|       "dll_semantic_verified": null,
 76443|       "dll_verified_status": "signature_verified_declared",
 76444|       "revitlookup_referenced": null,
 76445|       "revitlookup_requires_document_context": null
 76446|     },
 76447|     {
 76448|       "source": "Autodesk.Revit.DB.Mechanical.SpaceTag",
 76449|       "target": "Autodesk.Revit.DB.Mechanical.Space",
 76450|       "member_name": "Space",
```
