# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 155 of 216
- Original line range: 60061-60460
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 60061|       "source_url": "https://www.revitapidocs.com/2025/9c2689ac-51fc-7f8f-a18e-d4f7ee3e161e.htm",
 60062|       "dll_signature_verified": true,
 60063|       "dll_relationship_scope": "declared",
 60064|       "dll_semantic_verified": null,
 60065|       "dll_verified_status": "signature_verified_declared",
 60066|       "revitlookup_referenced": null,
 60067|       "revitlookup_requires_document_context": null
 60068|     },
 60069|     {
 60070|       "source": "Autodesk.Revit.DB.SynchronizeWithCentralOptions",
 60071|       "target": "Autodesk.Revit.DB.Workset",
 60072|       "member_name": "RelinquishFamilyWorksets",
 60073|       "member_kind": "property",
 60074|       "edge_type": "OWNED_BY_WORKSET",
 60075|       "confidence": "name_only_candidate",
 60076|       "confidence_tier": "likely",
 60077|       "target_resolution": "exact",
 60078|       "evidence": [
 60079|         "member name 'RelinquishFamilyWorksets' matches keyword pattern /Workset/ but return type 'bool' gives no type-level confirmation"
 60080|       ],
 60081|       "source_url": "https://www.revitapidocs.com/2025/31e99e30-5a40-d2c8-f5e0-1c639b392beb.htm",
 60082|       "dll_signature_verified": true,
 60083|       "dll_relationship_scope": "declared",
 60084|       "dll_semantic_verified": null,
 60085|       "dll_verified_status": "signature_verified_declared",
 60086|       "revitlookup_referenced": null,
 60087|       "revitlookup_requires_document_context": null
 60088|     },
 60089|     {
 60090|       "source": "Autodesk.Revit.DB.SynchronizeWithCentralOptions",
 60091|       "target": "Autodesk.Revit.DB.Workset",
 60092|       "member_name": "RelinquishProjectStandardWorksets",
 60093|       "member_kind": "property",
 60094|       "edge_type": "OWNED_BY_WORKSET",
 60095|       "confidence": "name_only_candidate",
 60096|       "confidence_tier": "likely",
 60097|       "target_resolution": "exact",
 60098|       "evidence": [
 60099|         "member name 'RelinquishProjectStandardWorksets' matches keyword pattern /Workset/ but return type 'bool' gives no type-level confirmation"
 60100|       ],
 60101|       "source_url": "https://www.revitapidocs.com/2025/c4643179-2e12-dba9-45e2-ac45c4f2014d.htm",
 60102|       "dll_signature_verified": true,
 60103|       "dll_relationship_scope": "declared",
 60104|       "dll_semantic_verified": null,
 60105|       "dll_verified_status": "signature_verified_declared",
 60106|       "revitlookup_referenced": null,
 60107|       "revitlookup_requires_document_context": null
 60108|     },
 60109|     {
 60110|       "source": "Autodesk.Revit.DB.SynchronizeWithCentralOptions",
 60111|       "target": "Autodesk.Revit.DB.Workset",
 60112|       "member_name": "RelinquishUserCreatedWorksets",
 60113|       "member_kind": "property",
 60114|       "edge_type": "OWNED_BY_WORKSET",
 60115|       "confidence": "name_only_candidate",
 60116|       "confidence_tier": "likely",
 60117|       "target_resolution": "exact",
 60118|       "evidence": [
 60119|         "member name 'RelinquishUserCreatedWorksets' matches keyword pattern /Workset/ but return type 'bool' gives no type-level confirmation"
 60120|       ],
 60121|       "source_url": "https://www.revitapidocs.com/2025/680601bc-19b8-2ff6-2b8a-75814650b464.htm",
 60122|       "dll_signature_verified": true,
 60123|       "dll_relationship_scope": "declared",
 60124|       "dll_semantic_verified": null,
 60125|       "dll_verified_status": "signature_verified_declared",
 60126|       "revitlookup_referenced": null,
 60127|       "revitlookup_requires_document_context": null
 60128|     },
 60129|     {
 60130|       "source": "Autodesk.Revit.DB.SynchronizeWithCentralOptions",
 60131|       "target": "Autodesk.Revit.DB.Workset",
 60132|       "member_name": "RelinquishViewWorksets",
 60133|       "member_kind": "property",
 60134|       "edge_type": "OWNED_BY_WORKSET",
 60135|       "confidence": "name_only_candidate",
 60136|       "confidence_tier": "likely",
 60137|       "target_resolution": "exact",
 60138|       "evidence": [
 60139|         "member name 'RelinquishViewWorksets' matches keyword pattern /Workset/ but return type 'bool' gives no type-level confirmation"
 60140|       ],
 60141|       "source_url": "https://www.revitapidocs.com/2025/38ead435-3f4a-993e-9095-e55be8a7e537.htm",
 60142|       "dll_signature_verified": true,
 60143|       "dll_relationship_scope": "declared",
 60144|       "dll_semantic_verified": null,
 60145|       "dll_verified_status": "signature_verified_declared",
 60146|       "revitlookup_referenced": null,
 60147|       "revitlookup_requires_document_context": null
 60148|     },
 60149|     {
 60150|       "source": "Autodesk.Revit.DB.SynchronizeWithCentralOptions",
 60151|       "target": "Autodesk.Revit.DB.RelinquishOptions",
 60152|       "member_name": "GetRelinquishOptions",
 60153|       "member_kind": "method",
 60154|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 60155|       "confidence": "direct_return_type",
 60156|       "confidence_tier": "unverified_reference",
 60157|       "target_resolution": "exact",
 60158|       "evidence": [
 60159|         "return type 'RelinquishOptions' directly names a Revit DB object type"
 60160|       ],
 60161|       "source_url": "https://www.revitapidocs.com/2025/445e3c8a-6a98-0c88-a5f7-7c38d0d57fec.htm",
 60162|       "dll_signature_verified": true,
 60163|       "dll_relationship_scope": "declared",
 60164|       "dll_semantic_verified": null,
 60165|       "dll_verified_status": "signature_verified_declared",
 60166|       "revitlookup_referenced": null,
 60167|       "revitlookup_requires_document_context": null
 60168|     },
 60169|     {
 60170|       "source": "Autodesk.Revit.DB.TableCellCombinedParameterData",
 60171|       "target": "Autodesk.Revit.DB.Category",
 60172|       "member_name": "CategoryId",
 60173|       "member_kind": "property",
 60174|       "edge_type": "HAS_CATEGORY",
 60175|       "confidence": "elementid_with_strong_name",
 60176|       "confidence_tier": "core",
 60177|       "target_resolution": "exact",
 60178|       "evidence": [
 60179|         "member name 'CategoryId' matches keyword pattern /Category/"
 60180|       ],
 60181|       "source_url": "https://www.revitapidocs.com/2025/e4a3f32e-61a1-3b57-ec1f-5f289bf7c25c.htm",
 60182|       "dll_signature_verified": true,
 60183|       "dll_relationship_scope": "declared",
 60184|       "dll_semantic_verified": null,
 60185|       "dll_verified_status": "signature_verified_declared",
 60186|       "revitlookup_referenced": null,
 60187|       "revitlookup_requires_document_context": null
 60188|     },
 60189|     {
 60190|       "source": "Autodesk.Revit.DB.TableCellCombinedParameterData",
 60191|       "target": null,
 60192|       "member_name": "ParamId",
 60193|       "member_kind": "property",
 60194|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 60195|       "confidence": "unknown_reference",
 60196|       "confidence_tier": "unverified_reference",
 60197|       "target_resolution": "none",
 60198|       "evidence": [
 60199|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 60200|       ],
 60201|       "source_url": "https://www.revitapidocs.com/2025/c1eed84d-4eb2-6ee1-b181-4713c5ba2e60.htm",
 60202|       "dll_signature_verified": true,
 60203|       "dll_relationship_scope": "declared",
 60204|       "dll_semantic_verified": null,
 60205|       "dll_verified_status": "signature_verified_declared",
 60206|       "revitlookup_referenced": null,
 60207|       "revitlookup_requires_document_context": null
 60208|     },
 60209|     {
 60210|       "source": "Autodesk.Revit.DB.TableCellStyle",
 60211|       "target": null,
 60212|       "member_name": "BorderBottomLineStyle",
 60213|       "member_kind": "property",
 60214|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 60215|       "confidence": "unknown_reference",
 60216|       "confidence_tier": "unverified_reference",
 60217|       "target_resolution": "none",
 60218|       "evidence": [
 60219|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 60220|       ],
 60221|       "source_url": "https://www.revitapidocs.com/2025/43c56db0-3f2b-8b8b-397f-8e271cc44008.htm",
 60222|       "dll_signature_verified": true,
 60223|       "dll_relationship_scope": "declared",
 60224|       "dll_semantic_verified": null,
 60225|       "dll_verified_status": "signature_verified_declared",
 60226|       "revitlookup_referenced": null,
 60227|       "revitlookup_requires_document_context": null
 60228|     },
 60229|     {
 60230|       "source": "Autodesk.Revit.DB.TableCellStyle",
 60231|       "target": null,
 60232|       "member_name": "BorderLeftLineStyle",
 60233|       "member_kind": "property",
 60234|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 60235|       "confidence": "unknown_reference",
 60236|       "confidence_tier": "unverified_reference",
 60237|       "target_resolution": "none",
 60238|       "evidence": [
 60239|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 60240|       ],
 60241|       "source_url": "https://www.revitapidocs.com/2025/cd24dd5f-2c1d-4cf6-cdb2-7c0d75781b02.htm",
 60242|       "dll_signature_verified": true,
 60243|       "dll_relationship_scope": "declared",
 60244|       "dll_semantic_verified": null,
 60245|       "dll_verified_status": "signature_verified_declared",
 60246|       "revitlookup_referenced": null,
 60247|       "revitlookup_requires_document_context": null
 60248|     },
 60249|     {
 60250|       "source": "Autodesk.Revit.DB.TableCellStyle",
 60251|       "target": null,
 60252|       "member_name": "BorderRightLineStyle",
 60253|       "member_kind": "property",
 60254|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 60255|       "confidence": "unknown_reference",
 60256|       "confidence_tier": "unverified_reference",
 60257|       "target_resolution": "none",
 60258|       "evidence": [
 60259|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 60260|       ],
 60261|       "source_url": "https://www.revitapidocs.com/2025/88e85cfe-217f-9a7e-fd47-19bc8e033b0a.htm",
 60262|       "dll_signature_verified": true,
 60263|       "dll_relationship_scope": "declared",
 60264|       "dll_semantic_verified": null,
 60265|       "dll_verified_status": "signature_verified_declared",
 60266|       "revitlookup_referenced": null,
 60267|       "revitlookup_requires_document_context": null
 60268|     },
 60269|     {
 60270|       "source": "Autodesk.Revit.DB.TableCellStyle",
 60271|       "target": null,
 60272|       "member_name": "BorderTopLineStyle",
 60273|       "member_kind": "property",
 60274|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 60275|       "confidence": "unknown_reference",
 60276|       "confidence_tier": "unverified_reference",
 60277|       "target_resolution": "none",
 60278|       "evidence": [
 60279|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 60280|       ],
 60281|       "source_url": "https://www.revitapidocs.com/2025/1e89b157-e871-6dec-cc8c-bb76e97f1c90.htm",
 60282|       "dll_signature_verified": true,
 60283|       "dll_relationship_scope": "declared",
 60284|       "dll_semantic_verified": null,
 60285|       "dll_verified_status": "signature_verified_declared",
 60286|       "revitlookup_referenced": null,
 60287|       "revitlookup_requires_document_context": null
 60288|     },
 60289|     {
 60290|       "source": "Autodesk.Revit.DB.TableCellStyle",
 60291|       "target": "Autodesk.Revit.DB.Phase",
 60292|       "member_name": "IsInactivePhaseload",
 60293|       "member_kind": "property",
 60294|       "edge_type": "ASSIGNED_TO_PHASE",
 60295|       "confidence": "name_only_candidate",
 60296|       "confidence_tier": "likely",
 60297|       "target_resolution": "exact",
 60298|       "evidence": [
 60299|         "member name 'IsInactivePhaseload' matches keyword pattern /Phase/ but return type 'bool' gives no type-level confirmation"
 60300|       ],
 60301|       "source_url": "https://www.revitapidocs.com/2025/fe7cf50b-6348-2922-f191-29a03a2cdb9d.htm",
 60302|       "dll_signature_verified": true,
 60303|       "dll_relationship_scope": "declared",
 60304|       "dll_semantic_verified": null,
 60305|       "dll_verified_status": "signature_verified_declared",
 60306|       "revitlookup_referenced": null,
 60307|       "revitlookup_requires_document_context": null
 60308|     },
 60309|     {
 60310|       "source": "Autodesk.Revit.DB.TableCellStyle",
 60311|       "target": "Autodesk.Revit.DB.TableCellStyleOverrideOptions",
 60312|       "member_name": "GetCellStyleOverrideOptions",
 60313|       "member_kind": "method",
 60314|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 60315|       "confidence": "direct_return_type",
 60316|       "confidence_tier": "unverified_reference",
 60317|       "target_resolution": "exact",
 60318|       "evidence": [
 60319|         "return type 'TableCellStyleOverrideOptions' directly names a Revit DB object type"
 60320|       ],
 60321|       "source_url": "https://www.revitapidocs.com/2025/4c375ee0-f561-b92d-549d-2ffb75a85880.htm",
 60322|       "dll_signature_verified": true,
 60323|       "dll_relationship_scope": "declared",
 60324|       "dll_semantic_verified": null,
 60325|       "dll_verified_status": "signature_verified_declared",
 60326|       "revitlookup_referenced": null,
 60327|       "revitlookup_requires_document_context": null
 60328|     },
 60329|     {
 60330|       "source": "Autodesk.Revit.DB.TableData",
 60331|       "target": "Autodesk.Revit.DB.Level",
 60332|       "member_name": "ZoomLevel",
 60333|       "member_kind": "property",
 60334|       "edge_type": "ASSIGNED_TO_LEVEL",
 60335|       "confidence": "name_only_candidate",
 60336|       "confidence_tier": "likely",
 60337|       "target_resolution": "exact",
 60338|       "evidence": [
 60339|         "member name 'ZoomLevel' matches keyword pattern /Level/ but return type 'int' gives no type-level confirmation"
 60340|       ],
 60341|       "source_url": "https://www.revitapidocs.com/2025/ead726cc-7695-e71d-e4a6-919319bb58db.htm",
 60342|       "dll_signature_verified": true,
 60343|       "dll_relationship_scope": "declared",
 60344|       "dll_semantic_verified": null,
 60345|       "dll_verified_status": "signature_verified_declared",
 60346|       "revitlookup_referenced": null,
 60347|       "revitlookup_requires_document_context": null
 60348|     },
 60349|     {
 60350|       "source": "Autodesk.Revit.DB.TableData",
 60351|       "target": "Autodesk.Revit.DB.TableSectionData",
 60352|       "member_name": "GetSectionData",
 60353|       "member_kind": "method",
 60354|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 60355|       "confidence": "direct_return_type",
 60356|       "confidence_tier": "unverified_reference",
 60357|       "target_resolution": "exact",
 60358|       "evidence": [
 60359|         "return type 'TableSectionData' directly names a Revit DB object type"
 60360|       ],
 60361|       "source_url": "https://www.revitapidocs.com/2025/ac5594a4-3b6e-9a85-ac7c-363340f09aac.htm",
 60362|       "dll_signature_verified": true,
 60363|       "dll_relationship_scope": "declared",
 60364|       "dll_semantic_verified": null,
 60365|       "dll_verified_status": "signature_verified_declared",
 60366|       "revitlookup_referenced": null,
 60367|       "revitlookup_requires_document_context": null
 60368|     },
 60369|     {
 60370|       "source": "Autodesk.Revit.DB.TableData",
 60371|       "target": "Autodesk.Revit.DB.TableSectionData",
 60372|       "member_name": "GetSectionData",
 60373|       "member_kind": "method",
 60374|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 60375|       "confidence": "direct_return_type",
 60376|       "confidence_tier": "unverified_reference",
 60377|       "target_resolution": "exact",
 60378|       "evidence": [
 60379|         "return type 'TableSectionData' directly names a Revit DB object type"
 60380|       ],
 60381|       "source_url": "https://www.revitapidocs.com/2025/154fcb09-0a96-d795-5df2-e2ec6ad244d5.htm",
 60382|       "dll_signature_verified": true,
 60383|       "dll_relationship_scope": "declared",
 60384|       "dll_semantic_verified": null,
 60385|       "dll_verified_status": "signature_verified_declared",
 60386|       "revitlookup_referenced": null,
 60387|       "revitlookup_requires_document_context": null
 60388|     },
 60389|     {
 60390|       "source": "Autodesk.Revit.DB.TableData",
 60391|       "target": "Autodesk.Revit.DB.Level",
 60392|       "member_name": "IsValidZoomLevel",
 60393|       "member_kind": "method",
 60394|       "edge_type": "ASSIGNED_TO_LEVEL",
 60395|       "confidence": "name_only_candidate",
 60396|       "confidence_tier": "likely",
 60397|       "target_resolution": "exact",
 60398|       "evidence": [
 60399|         "member name 'IsValidZoomLevel' matches keyword pattern /Level/ but return type 'bool' gives no type-level confirmation"
 60400|       ],
 60401|       "source_url": "https://www.revitapidocs.com/2025/0b29dd56-50f3-1768-513f-545bfa4db09f.htm",
 60402|       "dll_signature_verified": true,
 60403|       "dll_relationship_scope": "declared",
 60404|       "dll_semantic_verified": null,
 60405|       "dll_verified_status": "signature_verified_declared",
 60406|       "revitlookup_referenced": true,
 60407|       "revitlookup_requires_document_context": false
 60408|     },
 60409|     {
 60410|       "source": "Autodesk.Revit.DB.TableSectionData",
 60411|       "target": "Autodesk.Revit.DB.TableCellCalculatedValueData",
 60412|       "member_name": "GetCellCalculatedValue",
 60413|       "member_kind": "method",
 60414|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 60415|       "confidence": "direct_return_type",
 60416|       "confidence_tier": "unverified_reference",
 60417|       "target_resolution": "exact",
 60418|       "evidence": [
 60419|         "return type 'TableCellCalculatedValueData' directly names a Revit DB object type"
 60420|       ],
 60421|       "source_url": "https://www.revitapidocs.com/2025/974af8b8-9e02-ce8d-6b95-16fb8deb7fb1.htm",
 60422|       "dll_signature_verified": true,
 60423|       "dll_relationship_scope": "declared",
 60424|       "dll_semantic_verified": null,
 60425|       "dll_verified_status": "signature_verified_declared",
 60426|       "revitlookup_referenced": true,
 60427|       "revitlookup_requires_document_context": false
 60428|     },
 60429|     {
 60430|       "source": "Autodesk.Revit.DB.TableSectionData",
 60431|       "target": "Autodesk.Revit.DB.TableCellCalculatedValueData",
 60432|       "member_name": "GetCellCalculatedValue",
 60433|       "member_kind": "method",
 60434|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 60435|       "confidence": "direct_return_type",
 60436|       "confidence_tier": "unverified_reference",
 60437|       "target_resolution": "exact",
 60438|       "evidence": [
 60439|         "return type 'TableCellCalculatedValueData' directly names a Revit DB object type"
 60440|       ],
 60441|       "source_url": "https://www.revitapidocs.com/2025/639a0d4c-96ea-3b73-570e-456e84fa30fc.htm",
 60442|       "dll_signature_verified": true,
 60443|       "dll_relationship_scope": "declared",
 60444|       "dll_semantic_verified": null,
 60445|       "dll_verified_status": "signature_verified_declared",
 60446|       "revitlookup_referenced": true,
 60447|       "revitlookup_requires_document_context": false
 60448|     },
 60449|     {
 60450|       "source": "Autodesk.Revit.DB.TableSectionData",
 60451|       "target": "Autodesk.Revit.DB.Category",
 60452|       "member_name": "GetCellCategoryId",
 60453|       "member_kind": "method",
 60454|       "edge_type": "HAS_CATEGORY",
 60455|       "confidence": "elementid_with_strong_name",
 60456|       "confidence_tier": "core",
 60457|       "target_resolution": "exact",
 60458|       "evidence": [
 60459|         "member name 'GetCellCategoryId' matches keyword pattern /Category/"
 60460|       ],
```
