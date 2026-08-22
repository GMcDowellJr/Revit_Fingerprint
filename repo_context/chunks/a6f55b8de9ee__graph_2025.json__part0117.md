# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 117 of 216
- Original line range: 45241-45640
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 45241|       "edge_type": "HAS_PARAMETER",
 45242|       "confidence": "direct_return_type",
 45243|       "confidence_tier": "core",
 45244|       "target_resolution": "exact",
 45245|       "evidence": [
 45246|         "return type 'FamilyParameter' directly names a Revit DB object type"
 45247|       ],
 45248|       "source_url": "https://www.revitapidocs.com/2025/3ac89d60-4b71-694f-002f-125d2e6565fc.htm",
 45249|       "dll_signature_verified": true,
 45250|       "dll_relationship_scope": "declared",
 45251|       "dll_semantic_verified": null,
 45252|       "dll_verified_status": "signature_verified_declared",
 45253|       "revitlookup_referenced": null,
 45254|       "revitlookup_requires_document_context": null
 45255|     },
 45256|     {
 45257|       "source": "Autodesk.Revit.DB.FamilyManager",
 45258|       "target": null,
 45259|       "member_name": "AssociateElementParameterToFamilyParameter",
 45260|       "member_kind": "method",
 45261|       "edge_type": "HAS_PARAMETER",
 45262|       "confidence": "name_only_candidate",
 45263|       "confidence_tier": "likely",
 45264|       "target_resolution": "none",
 45265|       "evidence": [
 45266|         "member name 'AssociateElementParameterToFamilyParameter' matches keyword pattern /Parameter/ but return type 'void' gives no type-level confirmation"
 45267|       ],
 45268|       "source_url": "https://www.revitapidocs.com/2025/a047ea58-0351-b419-d856-85ed23734ee8.htm",
 45269|       "dll_signature_verified": true,
 45270|       "dll_relationship_scope": "declared",
 45271|       "dll_semantic_verified": null,
 45272|       "dll_verified_status": "signature_verified_declared",
 45273|       "revitlookup_referenced": null,
 45274|       "revitlookup_requires_document_context": null
 45275|     },
 45276|     {
 45277|       "source": "Autodesk.Revit.DB.FamilyManager",
 45278|       "target": null,
 45279|       "member_name": "CanElementParameterBeAssociated",
 45280|       "member_kind": "method",
 45281|       "edge_type": "HAS_PARAMETER",
 45282|       "confidence": "name_only_candidate",
 45283|       "confidence_tier": "likely",
 45284|       "target_resolution": "none",
 45285|       "evidence": [
 45286|         "member name 'CanElementParameterBeAssociated' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 45287|       ],
 45288|       "source_url": "https://www.revitapidocs.com/2025/ee0cd1df-2342-9e91-cf57-d7eb9d240b90.htm",
 45289|       "dll_signature_verified": true,
 45290|       "dll_relationship_scope": "declared",
 45291|       "dll_semantic_verified": null,
 45292|       "dll_verified_status": "signature_verified_declared",
 45293|       "revitlookup_referenced": null,
 45294|       "revitlookup_requires_document_context": null
 45295|     },
 45296|     {
 45297|       "source": "Autodesk.Revit.DB.FamilyManager",
 45298|       "target": "Autodesk.Revit.DB.FamilyParameter",
 45299|       "member_name": "GetAssociatedFamilyParameter",
 45300|       "member_kind": "method",
 45301|       "edge_type": "HAS_PARAMETER",
 45302|       "confidence": "direct_return_type",
 45303|       "confidence_tier": "core",
 45304|       "target_resolution": "exact",
 45305|       "evidence": [
 45306|         "return type 'FamilyParameter' directly names a Revit DB object type"
 45307|       ],
 45308|       "source_url": "https://www.revitapidocs.com/2025/ada33bdc-f484-c4a6-3713-6946dabd5fcf.htm",
 45309|       "dll_signature_verified": true,
 45310|       "dll_relationship_scope": "declared",
 45311|       "dll_semantic_verified": null,
 45312|       "dll_verified_status": "signature_verified_declared",
 45313|       "revitlookup_referenced": true,
 45314|       "revitlookup_requires_document_context": true
 45315|     },
 45316|     {
 45317|       "source": "Autodesk.Revit.DB.FamilyManager",
 45318|       "target": "Autodesk.Revit.DB.FamilyParameter",
 45319|       "member_name": "GetParameter",
 45320|       "member_kind": "method",
 45321|       "edge_type": "HAS_PARAMETER",
 45322|       "confidence": "direct_return_type",
 45323|       "confidence_tier": "core",
 45324|       "target_resolution": "exact",
 45325|       "evidence": [
 45326|         "return type 'FamilyParameter' directly names a Revit DB object type"
 45327|       ],
 45328|       "source_url": "https://www.revitapidocs.com/2025/9c22c68a-8fd5-850e-9aa8-cf7298ceebd0.htm",
 45329|       "dll_signature_verified": true,
 45330|       "dll_relationship_scope": "declared",
 45331|       "dll_semantic_verified": null,
 45332|       "dll_verified_status": "signature_verified_declared",
 45333|       "revitlookup_referenced": null,
 45334|       "revitlookup_requires_document_context": null
 45335|     },
 45336|     {
 45337|       "source": "Autodesk.Revit.DB.FamilyManager",
 45338|       "target": "Autodesk.Revit.DB.FamilyParameter",
 45339|       "member_name": "GetParameters",
 45340|       "member_kind": "method",
 45341|       "edge_type": "HAS_PARAMETER",
 45342|       "confidence": "needs_runtime_validation",
 45343|       "confidence_tier": "needs_validation",
 45344|       "target_resolution": "exact",
 45345|       "evidence": [
 45346|         "return type 'IList < FamilyParameter >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 45347|       ],
 45348|       "source_url": "https://www.revitapidocs.com/2025/86e30f63-4894-aed9-c6df-0074cdfa89a7.htm",
 45349|       "dll_signature_verified": true,
 45350|       "dll_relationship_scope": "declared",
 45351|       "dll_semantic_verified": null,
 45352|       "dll_verified_status": "signature_verified_declared",
 45353|       "revitlookup_referenced": null,
 45354|       "revitlookup_requires_document_context": null
 45355|     },
 45356|     {
 45357|       "source": "Autodesk.Revit.DB.FamilyManager",
 45358|       "target": null,
 45359|       "member_name": "IsParameterLockable",
 45360|       "member_kind": "method",
 45361|       "edge_type": "HAS_PARAMETER",
 45362|       "confidence": "name_only_candidate",
 45363|       "confidence_tier": "likely",
 45364|       "target_resolution": "none",
 45365|       "evidence": [
 45366|         "member name 'IsParameterLockable' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 45367|       ],
 45368|       "source_url": "https://www.revitapidocs.com/2025/b0ab3d1e-01e7-dc91-373b-c14d396c1a3e.htm",
 45369|       "dll_signature_verified": true,
 45370|       "dll_relationship_scope": "declared",
 45371|       "dll_semantic_verified": null,
 45372|       "dll_verified_status": "signature_verified_declared",
 45373|       "revitlookup_referenced": true,
 45374|       "revitlookup_requires_document_context": false
 45375|     },
 45376|     {
 45377|       "source": "Autodesk.Revit.DB.FamilyManager",
 45378|       "target": null,
 45379|       "member_name": "IsParameterLocked",
 45380|       "member_kind": "method",
 45381|       "edge_type": "HAS_PARAMETER",
 45382|       "confidence": "name_only_candidate",
 45383|       "confidence_tier": "likely",
 45384|       "target_resolution": "none",
 45385|       "evidence": [
 45386|         "member name 'IsParameterLocked' matches keyword pattern /Parameter/ but return type 'bool' gives no type-level confirmation"
 45387|       ],
 45388|       "source_url": "https://www.revitapidocs.com/2025/b2b1deb8-e2c0-8f48-7b03-368ec43746c5.htm",
 45389|       "dll_signature_verified": true,
 45390|       "dll_relationship_scope": "declared",
 45391|       "dll_semantic_verified": null,
 45392|       "dll_verified_status": "signature_verified_declared",
 45393|       "revitlookup_referenced": true,
 45394|       "revitlookup_requires_document_context": true
 45395|     },
 45396|     {
 45397|       "source": "Autodesk.Revit.DB.FamilyManager",
 45398|       "target": null,
 45399|       "member_name": "IsUserAssignableParameterGroup",
 45400|       "member_kind": "method",
 45401|       "edge_type": "MEMBER_OF_GROUP",
 45402|       "confidence": "name_only_candidate",
 45403|       "confidence_tier": "likely",
 45404|       "target_resolution": "none",
 45405|       "evidence": [
 45406|         "member name 'IsUserAssignableParameterGroup' matches keyword pattern /^GetMember|Group/ but return type 'bool' gives no type-level confirmation"
 45407|       ],
 45408|       "source_url": "https://www.revitapidocs.com/2025/414a0359-4e22-26ed-c01c-f52a81dccf06.htm",
 45409|       "dll_signature_verified": true,
 45410|       "dll_relationship_scope": "declared",
 45411|       "dll_semantic_verified": null,
 45412|       "dll_verified_status": "signature_verified_declared",
 45413|       "revitlookup_referenced": null,
 45414|       "revitlookup_requires_document_context": null
 45415|     },
 45416|     {
 45417|       "source": "Autodesk.Revit.DB.FamilyManager",
 45418|       "target": "Autodesk.Revit.DB.FamilyType",
 45419|       "member_name": "NewType",
 45420|       "member_kind": "method",
 45421|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45422|       "confidence": "direct_return_type",
 45423|       "confidence_tier": "unverified_reference",
 45424|       "target_resolution": "exact",
 45425|       "evidence": [
 45426|         "return type 'FamilyType' directly names a Revit DB object type"
 45427|       ],
 45428|       "source_url": "https://www.revitapidocs.com/2025/b46e98b1-54a1-7e04-66b7-a35efe5bc3f8.htm",
 45429|       "dll_signature_verified": true,
 45430|       "dll_relationship_scope": "declared",
 45431|       "dll_semantic_verified": null,
 45432|       "dll_verified_status": "signature_verified_declared",
 45433|       "revitlookup_referenced": null,
 45434|       "revitlookup_requires_document_context": null
 45435|     },
 45436|     {
 45437|       "source": "Autodesk.Revit.DB.FamilyManager",
 45438|       "target": null,
 45439|       "member_name": "RemoveParameter",
 45440|       "member_kind": "method",
 45441|       "edge_type": "HAS_PARAMETER",
 45442|       "confidence": "name_only_candidate",
 45443|       "confidence_tier": "likely",
 45444|       "target_resolution": "none",
 45445|       "evidence": [
 45446|         "member name 'RemoveParameter' matches keyword pattern /Parameter/ but return type 'void' gives no type-level confirmation"
 45447|       ],
 45448|       "source_url": "https://www.revitapidocs.com/2025/cb266197-b76e-66db-ea15-2cf14bcb4f85.htm",
 45449|       "dll_signature_verified": true,
 45450|       "dll_relationship_scope": "declared",
 45451|       "dll_semantic_verified": null,
 45452|       "dll_verified_status": "signature_verified_declared",
 45453|       "revitlookup_referenced": null,
 45454|       "revitlookup_requires_document_context": null
 45455|     },
 45456|     {
 45457|       "source": "Autodesk.Revit.DB.FamilyManager",
 45458|       "target": null,
 45459|       "member_name": "RenameParameter",
 45460|       "member_kind": "method",
 45461|       "edge_type": "HAS_PARAMETER",
 45462|       "confidence": "name_only_candidate",
 45463|       "confidence_tier": "likely",
 45464|       "target_resolution": "none",
 45465|       "evidence": [
 45466|         "member name 'RenameParameter' matches keyword pattern /Parameter/ but return type 'void' gives no type-level confirmation"
 45467|       ],
 45468|       "source_url": "https://www.revitapidocs.com/2025/19e7d857-9243-95a0-726c-50b5b7482c3e.htm",
 45469|       "dll_signature_verified": true,
 45470|       "dll_relationship_scope": "declared",
 45471|       "dll_semantic_verified": null,
 45472|       "dll_verified_status": "signature_verified_declared",
 45473|       "revitlookup_referenced": null,
 45474|       "revitlookup_requires_document_context": null
 45475|     },
 45476|     {
 45477|       "source": "Autodesk.Revit.DB.FamilyManager",
 45478|       "target": null,
 45479|       "member_name": "ReorderParameters",
 45480|       "member_kind": "method",
 45481|       "edge_type": "HAS_PARAMETER",
 45482|       "confidence": "name_only_candidate",
 45483|       "confidence_tier": "likely",
 45484|       "target_resolution": "none",
 45485|       "evidence": [
 45486|         "member name 'ReorderParameters' matches keyword pattern /Parameter/ but return type 'void' gives no type-level confirmation"
 45487|       ],
 45488|       "source_url": "https://www.revitapidocs.com/2025/f3e5375b-28d7-d6c6-ea49-bf6f6289fd9a.htm",
 45489|       "dll_signature_verified": true,
 45490|       "dll_relationship_scope": "declared",
 45491|       "dll_semantic_verified": null,
 45492|       "dll_verified_status": "signature_verified_declared",
 45493|       "revitlookup_referenced": null,
 45494|       "revitlookup_requires_document_context": null
 45495|     },
 45496|     {
 45497|       "source": "Autodesk.Revit.DB.FamilyManager",
 45498|       "target": "Autodesk.Revit.DB.FamilyParameter",
 45499|       "member_name": "ReplaceParameter",
 45500|       "member_kind": "method",
 45501|       "edge_type": "HAS_PARAMETER",
 45502|       "confidence": "direct_return_type",
 45503|       "confidence_tier": "core",
 45504|       "target_resolution": "exact",
 45505|       "evidence": [
 45506|         "return type 'FamilyParameter' directly names a Revit DB object type"
 45507|       ],
 45508|       "source_url": "https://www.revitapidocs.com/2025/9ddbd75b-887d-397a-14aa-3e4052a2a2eb.htm",
 45509|       "dll_signature_verified": true,
 45510|       "dll_relationship_scope": "declared",
 45511|       "dll_semantic_verified": null,
 45512|       "dll_verified_status": "signature_verified_declared",
 45513|       "revitlookup_referenced": null,
 45514|       "revitlookup_requires_document_context": null
 45515|     },
 45516|     {
 45517|       "source": "Autodesk.Revit.DB.FamilyManager",
 45518|       "target": "Autodesk.Revit.DB.FamilyParameter",
 45519|       "member_name": "ReplaceParameter",
 45520|       "member_kind": "method",
 45521|       "edge_type": "HAS_PARAMETER",
 45522|       "confidence": "direct_return_type",
 45523|       "confidence_tier": "core",
 45524|       "target_resolution": "exact",
 45525|       "evidence": [
 45526|         "return type 'FamilyParameter' directly names a Revit DB object type"
 45527|       ],
 45528|       "source_url": "https://www.revitapidocs.com/2025/b276c350-b06f-69fe-c9e2-a9d938c3e973.htm",
 45529|       "dll_signature_verified": true,
 45530|       "dll_relationship_scope": "declared",
 45531|       "dll_semantic_verified": null,
 45532|       "dll_verified_status": "signature_verified_declared",
 45533|       "revitlookup_referenced": null,
 45534|       "revitlookup_requires_document_context": null
 45535|     },
 45536|     {
 45537|       "source": "Autodesk.Revit.DB.FamilyManager",
 45538|       "target": null,
 45539|       "member_name": "SetParameterLocked",
 45540|       "member_kind": "method",
 45541|       "edge_type": "HAS_PARAMETER",
 45542|       "confidence": "name_only_candidate",
 45543|       "confidence_tier": "likely",
 45544|       "target_resolution": "none",
 45545|       "evidence": [
 45546|         "member name 'SetParameterLocked' matches keyword pattern /Parameter/ but return type 'void' gives no type-level confirmation"
 45547|       ],
 45548|       "source_url": "https://www.revitapidocs.com/2025/9ee4b404-c9e9-7d52-389a-a5fa21eae2e5.htm",
 45549|       "dll_signature_verified": true,
 45550|       "dll_relationship_scope": "declared",
 45551|       "dll_semantic_verified": null,
 45552|       "dll_verified_status": "signature_verified_declared",
 45553|       "revitlookup_referenced": null,
 45554|       "revitlookup_requires_document_context": null
 45555|     },
 45556|     {
 45557|       "source": "Autodesk.Revit.DB.FamilyManager",
 45558|       "target": null,
 45559|       "member_name": "SortParameters",
 45560|       "member_kind": "method",
 45561|       "edge_type": "HAS_PARAMETER",
 45562|       "confidence": "name_only_candidate",
 45563|       "confidence_tier": "likely",
 45564|       "target_resolution": "none",
 45565|       "evidence": [
 45566|         "member name 'SortParameters' matches keyword pattern /Parameter/ but return type 'void' gives no type-level confirmation"
 45567|       ],
 45568|       "source_url": "https://www.revitapidocs.com/2025/329ceb60-b9b5-d603-a23c-e9fcfc9d2f62.htm",
 45569|       "dll_signature_verified": true,
 45570|       "dll_relationship_scope": "declared",
 45571|       "dll_semantic_verified": null,
 45572|       "dll_verified_status": "signature_verified_declared",
 45573|       "revitlookup_referenced": null,
 45574|       "revitlookup_requires_document_context": null
 45575|     },
 45576|     {
 45577|       "source": "Autodesk.Revit.DB.FamilyParameter",
 45578|       "target": "Autodesk.Revit.DB.ParameterSet",
 45579|       "member_name": "AssociatedParameters",
 45580|       "member_kind": "property",
 45581|       "edge_type": "HAS_PARAMETER",
 45582|       "confidence": "direct_return_type",
 45583|       "confidence_tier": "core",
 45584|       "target_resolution": "exact",
 45585|       "evidence": [
 45586|         "return type 'ParameterSet' directly names a Revit DB object type"
 45587|       ],
 45588|       "source_url": "https://www.revitapidocs.com/2025/7c53d717-b977-3ce3-2ee8-ecf2da61ffb5.htm",
 45589|       "dll_signature_verified": true,
 45590|       "dll_relationship_scope": "declared",
 45591|       "dll_semantic_verified": null,
 45592|       "dll_verified_status": "signature_verified_declared",
 45593|       "revitlookup_referenced": null,
 45594|       "revitlookup_requires_document_context": null
 45595|     },
 45596|     {
 45597|       "source": "Autodesk.Revit.DB.FamilyParameter",
 45598|       "target": "Autodesk.Revit.DB.Definition",
 45599|       "member_name": "Definition",
 45600|       "member_kind": "property",
 45601|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 45602|       "confidence": "direct_return_type",
 45603|       "confidence_tier": "unverified_reference",
 45604|       "target_resolution": "exact",
 45605|       "evidence": [
 45606|         "return type 'Definition' directly names a Revit DB object type"
 45607|       ],
 45608|       "source_url": "https://www.revitapidocs.com/2025/00308c2a-5024-e7d8-5e6a-90c656ea2db9.htm",
 45609|       "dll_signature_verified": true,
 45610|       "dll_relationship_scope": "declared",
 45611|       "dll_semantic_verified": null,
 45612|       "dll_verified_status": "signature_verified_declared",
 45613|       "revitlookup_referenced": null,
 45614|       "revitlookup_requires_document_context": null
 45615|     },
 45616|     {
 45617|       "source": "Autodesk.Revit.DB.FamilyParameter",
 45618|       "target": null,
 45619|       "member_name": "Id",
 45620|       "member_kind": "property",
 45621|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 45622|       "confidence": "unknown_reference",
 45623|       "confidence_tier": "unverified_reference",
 45624|       "target_resolution": "none",
 45625|       "evidence": [
 45626|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 45627|       ],
 45628|       "source_url": "https://www.revitapidocs.com/2025/7766aa41-e740-3659-cd45-e54f0e3c1f0c.htm",
 45629|       "dll_signature_verified": true,
 45630|       "dll_relationship_scope": "declared",
 45631|       "dll_semantic_verified": null,
 45632|       "dll_verified_status": "signature_verified_declared",
 45633|       "revitlookup_referenced": null,
 45634|       "revitlookup_requires_document_context": null
 45635|     },
 45636|     {
 45637|       "source": "Autodesk.Revit.DB.FamilyParameterSet",
 45638|       "target": "Autodesk.Revit.DB.FamilyParameterSetIterator",
 45639|       "member_name": "ForwardIterator",
 45640|       "member_kind": "method",
```
