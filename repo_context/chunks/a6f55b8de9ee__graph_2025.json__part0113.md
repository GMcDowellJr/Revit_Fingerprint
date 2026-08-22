# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 113 of 216
- Original line range: 43681-44080
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 43681|       "confidence_tier": "core",
 43682|       "target_resolution": "exact",
 43683|       "evidence": [
 43684|         "return type 'FabricationHostedInfo' directly names a Revit DB object type"
 43685|       ],
 43686|       "source_url": "https://www.revitapidocs.com/2025/e11c4774-dc2e-0b85-5511-503d8aabf764.htm",
 43687|       "dll_signature_verified": true,
 43688|       "dll_relationship_scope": "declared",
 43689|       "dll_semantic_verified": null,
 43690|       "dll_verified_status": "signature_verified_declared",
 43691|       "revitlookup_referenced": null,
 43692|       "revitlookup_requires_document_context": null
 43693|     },
 43694|     {
 43695|       "source": "Autodesk.Revit.DB.FabricationPart",
 43696|       "target": "Autodesk.Revit.DB.FabricationAncillaryUsage",
 43697|       "member_name": "GetPartAncillaryUsage",
 43698|       "member_kind": "method",
 43699|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 43700|       "confidence": "needs_runtime_validation",
 43701|       "confidence_tier": "needs_validation",
 43702|       "target_resolution": "exact",
 43703|       "evidence": [
 43704|         "return type 'IList < FabricationAncillaryUsage >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 43705|       ],
 43706|       "source_url": "https://www.revitapidocs.com/2025/dcd90020-8b5e-0d73-8de4-923ef0a4ba14.htm",
 43707|       "dll_signature_verified": true,
 43708|       "dll_relationship_scope": "declared",
 43709|       "dll_semantic_verified": null,
 43710|       "dll_verified_status": "signature_verified_declared",
 43711|       "revitlookup_referenced": null,
 43712|       "revitlookup_requires_document_context": null
 43713|     },
 43714|     {
 43715|       "source": "Autodesk.Revit.DB.FabricationPart",
 43716|       "target": "Autodesk.Revit.DB.FabricationRodInfo",
 43717|       "member_name": "GetRodInfo",
 43718|       "member_kind": "method",
 43719|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 43720|       "confidence": "direct_return_type",
 43721|       "confidence_tier": "unverified_reference",
 43722|       "target_resolution": "exact",
 43723|       "evidence": [
 43724|         "return type 'FabricationRodInfo' directly names a Revit DB object type"
 43725|       ],
 43726|       "source_url": "https://www.revitapidocs.com/2025/d1b925eb-9fde-0c61-e416-25b4e98a8fd4.htm",
 43727|       "dll_signature_verified": true,
 43728|       "dll_relationship_scope": "declared",
 43729|       "dll_semantic_verified": null,
 43730|       "dll_verified_status": "signature_verified_declared",
 43731|       "revitlookup_referenced": null,
 43732|       "revitlookup_requires_document_context": null
 43733|     },
 43734|     {
 43735|       "source": "Autodesk.Revit.DB.FabricationPart",
 43736|       "target": "Autodesk.Revit.DB.FabricationVersionInfo",
 43737|       "member_name": "GetVersionHistory",
 43738|       "member_kind": "method",
 43739|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 43740|       "confidence": "needs_runtime_validation",
 43741|       "confidence_tier": "needs_validation",
 43742|       "target_resolution": "exact",
 43743|       "evidence": [
 43744|         "return type 'IList < FabricationVersionInfo >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 43745|       ],
 43746|       "source_url": "https://www.revitapidocs.com/2025/0d114b0b-a839-1158-b570-d264cca43100.htm",
 43747|       "dll_signature_verified": true,
 43748|       "dll_relationship_scope": "declared",
 43749|       "dll_semantic_verified": null,
 43750|       "dll_verified_status": "signature_verified_declared",
 43751|       "revitlookup_referenced": null,
 43752|       "revitlookup_requires_document_context": null
 43753|     },
 43754|     {
 43755|       "source": "Autodesk.Revit.DB.FabricationPart",
 43756|       "target": null,
 43757|       "member_name": "OptimizeLengths",
 43758|       "member_kind": "method",
 43759|       "edge_type": "RETURNS_ELEMENT_IDS",
 43760|       "confidence": "unknown_reference",
 43761|       "confidence_tier": "unverified_reference",
 43762|       "target_resolution": "none",
 43763|       "evidence": [
 43764|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 43765|       ],
 43766|       "source_url": "https://www.revitapidocs.com/2025/23222651-0817-d314-cb3e-8dd70a261167.htm",
 43767|       "dll_signature_verified": true,
 43768|       "dll_relationship_scope": "declared",
 43769|       "dll_semantic_verified": null,
 43770|       "dll_verified_status": "signature_verified_declared",
 43771|       "revitlookup_referenced": null,
 43772|       "revitlookup_requires_document_context": null
 43773|     },
 43774|     {
 43775|       "source": "Autodesk.Revit.DB.FabricationPart",
 43776|       "target": null,
 43777|       "member_name": "SaveAsFabricationJob",
 43778|       "member_kind": "method",
 43779|       "edge_type": "RETURNS_ELEMENT_IDS",
 43780|       "confidence": "unknown_reference",
 43781|       "confidence_tier": "unverified_reference",
 43782|       "target_resolution": "none",
 43783|       "evidence": [
 43784|         "return type 'ISet < ElementId >' is a collection of ID wrappers with no strong name hint"
 43785|       ],
 43786|       "source_url": "https://www.revitapidocs.com/2025/c443a64a-7541-ed57-c641-ec54d3576a00.htm",
 43787|       "dll_signature_verified": true,
 43788|       "dll_relationship_scope": "declared",
 43789|       "dll_semantic_verified": null,
 43790|       "dll_verified_status": "signature_verified_declared",
 43791|       "revitlookup_referenced": null,
 43792|       "revitlookup_requires_document_context": null
 43793|     },
 43794|     {
 43795|       "source": "Autodesk.Revit.DB.FabricationPart",
 43796|       "target": null,
 43797|       "member_name": "SplitStraight",
 43798|       "member_kind": "method",
 43799|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 43800|       "confidence": "unknown_reference",
 43801|       "confidence_tier": "unverified_reference",
 43802|       "target_resolution": "none",
 43803|       "evidence": [
 43804|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 43805|       ],
 43806|       "source_url": "https://www.revitapidocs.com/2025/0815006b-c24c-56f7-2781-ac01d1bc6ad6.htm",
 43807|       "dll_signature_verified": true,
 43808|       "dll_relationship_scope": "declared",
 43809|       "dll_semantic_verified": null,
 43810|       "dll_verified_status": "signature_verified_declared",
 43811|       "revitlookup_referenced": null,
 43812|       "revitlookup_requires_document_context": null
 43813|     },
 43814|     {
 43815|       "source": "Autodesk.Revit.DB.FabricationPart",
 43816|       "target": null,
 43817|       "member_name": "SplitStraight",
 43818|       "member_kind": "method",
 43819|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 43820|       "confidence": "unknown_reference",
 43821|       "confidence_tier": "unverified_reference",
 43822|       "target_resolution": "none",
 43823|       "evidence": [
 43824|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 43825|       ],
 43826|       "source_url": "https://www.revitapidocs.com/2025/6f5e92db-db24-b4a0-2578-f232c8cedf94.htm",
 43827|       "dll_signature_verified": true,
 43828|       "dll_relationship_scope": "declared",
 43829|       "dll_semantic_verified": null,
 43830|       "dll_verified_status": "signature_verified_declared",
 43831|       "revitlookup_referenced": null,
 43832|       "revitlookup_requires_document_context": null
 43833|     },
 43834|     {
 43835|       "source": "Autodesk.Revit.DB.FabricationPartType",
 43836|       "target": null,
 43837|       "member_name": "Lookup",
 43838|       "member_kind": "method",
 43839|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 43840|       "confidence": "unknown_reference",
 43841|       "confidence_tier": "unverified_reference",
 43842|       "target_resolution": "none",
 43843|       "evidence": [
 43844|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 43845|       ],
 43846|       "source_url": "https://www.revitapidocs.com/2025/5b71bce4-f161-6f8c-48dd-96f745990157.htm",
 43847|       "dll_signature_verified": true,
 43848|       "dll_relationship_scope": "declared",
 43849|       "dll_semantic_verified": null,
 43850|       "dll_verified_status": "signature_verified_declared",
 43851|       "revitlookup_referenced": null,
 43852|       "revitlookup_requires_document_context": null
 43853|     },
 43854|     {
 43855|       "source": "Autodesk.Revit.DB.FabricationRodInfo",
 43856|       "target": null,
 43857|       "member_name": "CanRodsBeHosted",
 43858|       "member_kind": "property",
 43859|       "edge_type": "HOSTED_BY",
 43860|       "confidence": "docs_semantic_hint",
 43861|       "confidence_tier": "core",
 43862|       "target_resolution": "none",
 43863|       "evidence": [
 43864|         "member name 'CanRodsBeHosted' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation",
 43865|         "docs text contains relationship phrase: 'hosted by'"
 43866|       ],
 43867|       "source_url": "https://www.revitapidocs.com/2025/4ee179bc-4345-82e0-258b-5f40a15e5948.htm",
 43868|       "dll_signature_verified": true,
 43869|       "dll_relationship_scope": "declared",
 43870|       "dll_semantic_verified": null,
 43871|       "dll_verified_status": "signature_verified_declared",
 43872|       "revitlookup_referenced": null,
 43873|       "revitlookup_requires_document_context": null
 43874|     },
 43875|     {
 43876|       "source": "Autodesk.Revit.DB.FabricationRodInfo",
 43877|       "target": null,
 43878|       "member_name": "GetRodAttachedElementId",
 43879|       "member_kind": "method",
 43880|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 43881|       "confidence": "unknown_reference",
 43882|       "confidence_tier": "unverified_reference",
 43883|       "target_resolution": "none",
 43884|       "evidence": [
 43885|         "return type is 'LinkElementId', an ID wrapper, but member name gives no strong hint of the target type"
 43886|       ],
 43887|       "source_url": "https://www.revitapidocs.com/2025/05e88086-4c9a-aa3e-316d-9eaa1c19cb93.htm",
 43888|       "dll_signature_verified": true,
 43889|       "dll_relationship_scope": "declared",
 43890|       "dll_semantic_verified": null,
 43891|       "dll_verified_status": "signature_verified_declared",
 43892|       "revitlookup_referenced": null,
 43893|       "revitlookup_requires_document_context": null
 43894|     },
 43895|     {
 43896|       "source": "Autodesk.Revit.DB.FabricationRodInfo",
 43897|       "target": null,
 43898|       "member_name": "IsRodLockedWithHost",
 43899|       "member_kind": "method",
 43900|       "edge_type": "HOSTED_BY",
 43901|       "confidence": "name_only_candidate",
 43902|       "confidence_tier": "likely",
 43903|       "target_resolution": "none",
 43904|       "evidence": [
 43905|         "member name 'IsRodLockedWithHost' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 43906|       ],
 43907|       "source_url": "https://www.revitapidocs.com/2025/8908e152-46ef-6526-10aa-2f6afbe1a0aa.htm",
 43908|       "dll_signature_verified": true,
 43909|       "dll_relationship_scope": "declared",
 43910|       "dll_semantic_verified": null,
 43911|       "dll_verified_status": "signature_verified_declared",
 43912|       "revitlookup_referenced": null,
 43913|       "revitlookup_requires_document_context": null
 43914|     },
 43915|     {
 43916|       "source": "Autodesk.Revit.DB.FabricationRodInfo",
 43917|       "target": null,
 43918|       "member_name": "SetRodLockedWithHost",
 43919|       "member_kind": "method",
 43920|       "edge_type": "HOSTED_BY",
 43921|       "confidence": "name_only_candidate",
 43922|       "confidence_tier": "likely",
 43923|       "target_resolution": "none",
 43924|       "evidence": [
 43925|         "member name 'SetRodLockedWithHost' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 43926|       ],
 43927|       "source_url": "https://www.revitapidocs.com/2025/77082f81-5eff-e5b6-edb0-11feded0e506.htm",
 43928|       "dll_signature_verified": true,
 43929|       "dll_relationship_scope": "declared",
 43930|       "dll_semantic_verified": null,
 43931|       "dll_verified_status": "signature_verified_declared",
 43932|       "revitlookup_referenced": null,
 43933|       "revitlookup_requires_document_context": null
 43934|     },
 43935|     {
 43936|       "source": "Autodesk.Revit.DB.FabricationService",
 43937|       "target": "Autodesk.Revit.DB.FabricationServiceButton",
 43938|       "member_name": "GetButton",
 43939|       "member_kind": "method",
 43940|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 43941|       "confidence": "direct_return_type",
 43942|       "confidence_tier": "unverified_reference",
 43943|       "target_resolution": "exact",
 43944|       "evidence": [
 43945|         "return type 'FabricationServiceButton' directly names a Revit DB object type"
 43946|       ],
 43947|       "source_url": "https://www.revitapidocs.com/2025/a07bb5f7-6c08-3d6b-25ea-5891cc2dfc5e.htm",
 43948|       "dll_signature_verified": true,
 43949|       "dll_relationship_scope": "declared",
 43950|       "dll_semantic_verified": null,
 43951|       "dll_verified_status": "signature_verified_declared",
 43952|       "revitlookup_referenced": null,
 43953|       "revitlookup_requires_document_context": null
 43954|     },
 43955|     {
 43956|       "source": "Autodesk.Revit.DB.FabricationServiceSettings",
 43957|       "target": null,
 43958|       "member_name": "AirFluidType",
 43959|       "member_kind": "property",
 43960|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 43961|       "confidence": "unknown_reference",
 43962|       "confidence_tier": "unverified_reference",
 43963|       "target_resolution": "none",
 43964|       "evidence": [
 43965|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 43966|       ],
 43967|       "source_url": "https://www.revitapidocs.com/2025/87ee74d9-77d8-3517-5ae8-45eb95e07524.htm",
 43968|       "dll_signature_verified": true,
 43969|       "dll_relationship_scope": "declared",
 43970|       "dll_semantic_verified": null,
 43971|       "dll_verified_status": "signature_verified_declared",
 43972|       "revitlookup_referenced": null,
 43973|       "revitlookup_requires_document_context": null
 43974|     },
 43975|     {
 43976|       "source": "Autodesk.Revit.DB.FabricationServiceSettings",
 43977|       "target": null,
 43978|       "member_name": "GetFluidType",
 43979|       "member_kind": "method",
 43980|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 43981|       "confidence": "unknown_reference",
 43982|       "confidence_tier": "unverified_reference",
 43983|       "target_resolution": "none",
 43984|       "evidence": [
 43985|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 43986|       ],
 43987|       "source_url": "https://www.revitapidocs.com/2025/a68293fe-39d0-207d-e222-a4b49c22b65a.htm",
 43988|       "dll_signature_verified": true,
 43989|       "dll_relationship_scope": "declared",
 43990|       "dll_semantic_verified": null,
 43991|       "dll_verified_status": "signature_verified_declared",
 43992|       "revitlookup_referenced": null,
 43993|       "revitlookup_requires_document_context": null
 43994|     },
 43995|     {
 43996|       "source": "Autodesk.Revit.DB.Face",
 43997|       "target": "Autodesk.Revit.DB.EdgeArrayArray",
 43998|       "member_name": "EdgeLoops",
 43999|       "member_kind": "property",
 44000|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 44001|       "confidence": "direct_return_type",
 44002|       "confidence_tier": "unverified_reference",
 44003|       "target_resolution": "exact",
 44004|       "evidence": [
 44005|         "return type 'EdgeArrayArray' directly names a Revit DB object type"
 44006|       ],
 44007|       "source_url": "https://www.revitapidocs.com/2025/2ccb511d-b5df-8d17-bd9e-3c9aff8cf234.htm",
 44008|       "dll_signature_verified": true,
 44009|       "dll_relationship_scope": "declared",
 44010|       "dll_semantic_verified": null,
 44011|       "dll_verified_status": "signature_verified_declared",
 44012|       "revitlookup_referenced": null,
 44013|       "revitlookup_requires_document_context": null
 44014|     },
 44015|     {
 44016|       "source": "Autodesk.Revit.DB.Face",
 44017|       "target": "Autodesk.Revit.DB.Material",
 44018|       "member_name": "MaterialElementId",
 44019|       "member_kind": "property",
 44020|       "edge_type": "USES_MATERIAL",
 44021|       "confidence": "elementid_with_strong_name",
 44022|       "confidence_tier": "core",
 44023|       "target_resolution": "exact",
 44024|       "evidence": [
 44025|         "member name 'MaterialElementId' matches keyword pattern /Material/"
 44026|       ],
 44027|       "source_url": "https://www.revitapidocs.com/2025/0f496bcd-fd05-f1dc-6cc7-d496541fd6ae.htm",
 44028|       "dll_signature_verified": true,
 44029|       "dll_relationship_scope": "declared",
 44030|       "dll_semantic_verified": null,
 44031|       "dll_verified_status": "signature_verified_declared",
 44032|       "revitlookup_referenced": null,
 44033|       "revitlookup_requires_document_context": null
 44034|     },
 44035|     {
 44036|       "source": "Autodesk.Revit.DB.Face",
 44037|       "target": "Autodesk.Revit.DB.Reference",
 44038|       "member_name": "Reference",
 44039|       "member_kind": "property",
 44040|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 44041|       "confidence": "direct_return_type",
 44042|       "confidence_tier": "unverified_reference",
 44043|       "target_resolution": "exact",
 44044|       "evidence": [
 44045|         "return type 'Reference' directly names a Revit DB object type"
 44046|       ],
 44047|       "source_url": "https://www.revitapidocs.com/2025/f3d5d2fe-96bf-8528-4628-78d8d5e6705f.htm",
 44048|       "dll_signature_verified": true,
 44049|       "dll_relationship_scope": "declared",
 44050|       "dll_semantic_verified": null,
 44051|       "dll_verified_status": "signature_verified_declared",
 44052|       "revitlookup_referenced": null,
 44053|       "revitlookup_requires_document_context": null
 44054|     },
 44055|     {
 44056|       "source": "Autodesk.Revit.DB.Face",
 44057|       "target": "Autodesk.Revit.DB.FaceSecondDerivatives",
 44058|       "member_name": "ComputeSecondDerivatives",
 44059|       "member_kind": "method",
 44060|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 44061|       "confidence": "direct_return_type",
 44062|       "confidence_tier": "unverified_reference",
 44063|       "target_resolution": "exact",
 44064|       "evidence": [
 44065|         "return type 'FaceSecondDerivatives' directly names a Revit DB object type"
 44066|       ],
 44067|       "source_url": "https://www.revitapidocs.com/2025/e1b6ec4d-cc6b-16dc-d442-fe0ee9491a8b.htm",
 44068|       "dll_signature_verified": true,
 44069|       "dll_relationship_scope": "declared",
 44070|       "dll_semantic_verified": null,
 44071|       "dll_verified_status": "signature_verified_declared",
 44072|       "revitlookup_referenced": null,
 44073|       "revitlookup_requires_document_context": null
 44074|     },
 44075|     {
 44076|       "source": "Autodesk.Revit.DB.Face",
 44077|       "target": "Autodesk.Revit.DB.BoundingBoxUV",
 44078|       "member_name": "GetBoundingBox",
 44079|       "member_kind": "method",
 44080|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
```
