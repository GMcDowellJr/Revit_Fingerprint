# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 193 of 216
- Original line range: 74881-75280
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 74881|       "dll_relationship_scope": "declared",
 74882|       "dll_semantic_verified": null,
 74883|       "dll_verified_status": "signature_verified_declared",
 74884|       "revitlookup_referenced": null,
 74885|       "revitlookup_requires_document_context": null
 74886|     },
 74887|     {
 74888|       "source": "Autodesk.Revit.DB.IFC.ImporterIFC",
 74889|       "target": "Autodesk.Revit.DB.Document",
 74890|       "member_name": "Document",
 74891|       "member_kind": "property",
 74892|       "edge_type": "REFERENCES",
 74893|       "confidence": "direct_return_type",
 74894|       "confidence_tier": "core",
 74895|       "target_resolution": "exact",
 74896|       "evidence": [
 74897|         "return type 'Document' directly names a Revit DB object type"
 74898|       ],
 74899|       "source_url": "https://www.revitapidocs.com/2025/5b2ccc42-7130-5d2c-38e8-b6e84a290b35.htm",
 74900|       "dll_signature_verified": true,
 74901|       "dll_relationship_scope": "declared",
 74902|       "dll_semantic_verified": null,
 74903|       "dll_verified_status": "signature_verified_declared",
 74904|       "revitlookup_referenced": null,
 74905|       "revitlookup_requires_document_context": null
 74906|     },
 74907|     {
 74908|       "source": "Autodesk.Revit.DB.IFC.ImporterIFCUtils",
 74909|       "target": "Autodesk.Revit.DB.Category",
 74910|       "member_name": "UpdateDirectShapeCategory",
 74911|       "member_kind": "method",
 74912|       "edge_type": "HAS_CATEGORY",
 74913|       "confidence": "name_only_candidate",
 74914|       "confidence_tier": "likely",
 74915|       "target_resolution": "exact",
 74916|       "evidence": [
 74917|         "member name 'UpdateDirectShapeCategory' matches keyword pattern /Category/ but return type 'void' gives no type-level confirmation"
 74918|       ],
 74919|       "source_url": "https://www.revitapidocs.com/2025/f7f5e7a4-8200-8ef8-c769-f68ab86d3886.htm",
 74920|       "dll_signature_verified": true,
 74921|       "dll_relationship_scope": "declared",
 74922|       "dll_semantic_verified": null,
 74923|       "dll_verified_status": "signature_verified_declared",
 74924|       "revitlookup_referenced": null,
 74925|       "revitlookup_requires_document_context": null
 74926|     },
 74927|     {
 74928|       "source": "Autodesk.Revit.DB.Infrastructure.Alignment",
 74929|       "target": "Autodesk.Revit.DB.Element",
 74930|       "member_name": "Element",
 74931|       "member_kind": "property",
 74932|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74933|       "confidence": "direct_return_type",
 74934|       "confidence_tier": "unverified_reference",
 74935|       "target_resolution": "exact",
 74936|       "evidence": [
 74937|         "return type 'Element' directly names a Revit DB object type"
 74938|       ],
 74939|       "source_url": "https://www.revitapidocs.com/2025/5bbd827f-37de-bfd5-de3f-ecac0179eb3b.htm",
 74940|       "dll_signature_verified": true,
 74941|       "dll_relationship_scope": "declared",
 74942|       "dll_semantic_verified": null,
 74943|       "dll_verified_status": "signature_verified_declared",
 74944|       "revitlookup_referenced": null,
 74945|       "revitlookup_requires_document_context": null
 74946|     },
 74947|     {
 74948|       "source": "Autodesk.Revit.DB.Infrastructure.Alignment",
 74949|       "target": "Autodesk.Revit.DB.Infrastructure.Alignment",
 74950|       "member_name": "GetAlignments",
 74951|       "member_kind": "method",
 74952|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74953|       "confidence": "needs_runtime_validation",
 74954|       "confidence_tier": "needs_validation",
 74955|       "target_resolution": "short_name_fallback",
 74956|       "evidence": [
 74957|         "return type 'ICollection < Alignment >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 74958|       ],
 74959|       "source_url": "https://www.revitapidocs.com/2025/baa67b6f-82df-1d4b-3c38-6cea587d3ae9.htm",
 74960|       "dll_signature_verified": true,
 74961|       "dll_relationship_scope": "declared",
 74962|       "dll_semantic_verified": null,
 74963|       "dll_verified_status": "signature_verified_declared",
 74964|       "revitlookup_referenced": null,
 74965|       "revitlookup_requires_document_context": null
 74966|     },
 74967|     {
 74968|       "source": "Autodesk.Revit.DB.Infrastructure.Alignment",
 74969|       "target": "Autodesk.Revit.DB.Infrastructure.Alignment",
 74970|       "member_name": "GetAlignments",
 74971|       "member_kind": "method",
 74972|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74973|       "confidence": "needs_runtime_validation",
 74974|       "confidence_tier": "needs_validation",
 74975|       "target_resolution": "short_name_fallback",
 74976|       "evidence": [
 74977|         "return type 'ICollection < Alignment >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 74978|       ],
 74979|       "source_url": "https://www.revitapidocs.com/2025/f999b171-5b95-f3c6-df18-a9f0c12c69d3.htm",
 74980|       "dll_signature_verified": true,
 74981|       "dll_relationship_scope": "declared",
 74982|       "dll_semantic_verified": null,
 74983|       "dll_verified_status": "signature_verified_declared",
 74984|       "revitlookup_referenced": null,
 74985|       "revitlookup_requires_document_context": null
 74986|     },
 74987|     {
 74988|       "source": "Autodesk.Revit.DB.Infrastructure.Alignment",
 74989|       "target": "Autodesk.Revit.DB.Infrastructure.HorizontalCurveEndpoint",
 74990|       "member_name": "GetDisplayedHorizontalCurveEndpoints",
 74991|       "member_kind": "method",
 74992|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74993|       "confidence": "needs_runtime_validation",
 74994|       "confidence_tier": "needs_validation",
 74995|       "target_resolution": "short_name_fallback",
 74996|       "evidence": [
 74997|         "return type 'IList < HorizontalCurveEndpoint >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 74998|       ],
 74999|       "source_url": "https://www.revitapidocs.com/2025/b5b51314-be8e-2d68-7920-a51af4366c27.htm",
 75000|       "dll_signature_verified": true,
 75001|       "dll_relationship_scope": "declared",
 75002|       "dll_semantic_verified": null,
 75003|       "dll_verified_status": "signature_verified_declared",
 75004|       "revitlookup_referenced": null,
 75005|       "revitlookup_requires_document_context": null
 75006|     },
 75007|     {
 75008|       "source": "Autodesk.Revit.DB.Infrastructure.AlignmentStationLabel",
 75009|       "target": null,
 75010|       "member_name": "AlignmentId",
 75011|       "member_kind": "property",
 75012|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 75013|       "confidence": "unknown_reference",
 75014|       "confidence_tier": "unverified_reference",
 75015|       "target_resolution": "none",
 75016|       "evidence": [
 75017|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 75018|       ],
 75019|       "source_url": "https://www.revitapidocs.com/2025/0546b98b-a6c8-d23e-275b-8e78ea0594e8.htm",
 75020|       "dll_signature_verified": true,
 75021|       "dll_relationship_scope": "declared",
 75022|       "dll_semantic_verified": null,
 75023|       "dll_verified_status": "signature_verified_declared",
 75024|       "revitlookup_referenced": null,
 75025|       "revitlookup_requires_document_context": null
 75026|     },
 75027|     {
 75028|       "source": "Autodesk.Revit.DB.Infrastructure.AlignmentStationLabel",
 75029|       "target": "Autodesk.Revit.DB.Element",
 75030|       "member_name": "Element",
 75031|       "member_kind": "property",
 75032|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75033|       "confidence": "direct_return_type",
 75034|       "confidence_tier": "unverified_reference",
 75035|       "target_resolution": "exact",
 75036|       "evidence": [
 75037|         "return type 'Element' directly names a Revit DB object type"
 75038|       ],
 75039|       "source_url": "https://www.revitapidocs.com/2025/29b63668-f113-d2fe-7d79-0f30ecac4d89.htm",
 75040|       "dll_signature_verified": true,
 75041|       "dll_relationship_scope": "declared",
 75042|       "dll_semantic_verified": null,
 75043|       "dll_verified_status": "signature_verified_declared",
 75044|       "revitlookup_referenced": null,
 75045|       "revitlookup_requires_document_context": null
 75046|     },
 75047|     {
 75048|       "source": "Autodesk.Revit.DB.Infrastructure.AlignmentStationLabel",
 75049|       "target": "Autodesk.Revit.DB.Infrastructure.AlignmentStationLabel",
 75050|       "member_name": "GetAlignmentStationLabels",
 75051|       "member_kind": "method",
 75052|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75053|       "confidence": "needs_runtime_validation",
 75054|       "confidence_tier": "needs_validation",
 75055|       "target_resolution": "short_name_fallback",
 75056|       "evidence": [
 75057|         "return type 'ICollection < AlignmentStationLabel >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 75058|       ],
 75059|       "source_url": "https://www.revitapidocs.com/2025/0cae55cf-ef69-817a-d284-65f2141761f9.htm",
 75060|       "dll_signature_verified": true,
 75061|       "dll_relationship_scope": "declared",
 75062|       "dll_semantic_verified": null,
 75063|       "dll_verified_status": "signature_verified_declared",
 75064|       "revitlookup_referenced": null,
 75065|       "revitlookup_requires_document_context": null
 75066|     },
 75067|     {
 75068|       "source": "Autodesk.Revit.DB.Infrastructure.AlignmentStationLabel",
 75069|       "target": "Autodesk.Revit.DB.Infrastructure.AlignmentStationLabel",
 75070|       "member_name": "GetAlignmentStationLabels",
 75071|       "member_kind": "method",
 75072|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75073|       "confidence": "needs_runtime_validation",
 75074|       "confidence_tier": "needs_validation",
 75075|       "target_resolution": "short_name_fallback",
 75076|       "evidence": [
 75077|         "return type 'ICollection < AlignmentStationLabel >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 75078|       ],
 75079|       "source_url": "https://www.revitapidocs.com/2025/e078bd76-8b9d-d02b-5fb7-ddfafa988f65.htm",
 75080|       "dll_signature_verified": true,
 75081|       "dll_relationship_scope": "declared",
 75082|       "dll_semantic_verified": null,
 75083|       "dll_verified_status": "signature_verified_declared",
 75084|       "revitlookup_referenced": null,
 75085|       "revitlookup_requires_document_context": null
 75086|     },
 75087|     {
 75088|       "source": "Autodesk.Revit.DB.Infrastructure.AlignmentStationLabelOptions",
 75089|       "target": null,
 75090|       "member_name": "TypeId",
 75091|       "member_kind": "property",
 75092|       "edge_type": "TYPE_OF",
 75093|       "confidence": "elementid_with_strong_name",
 75094|       "confidence_tier": "core",
 75095|       "target_resolution": "none",
 75096|       "evidence": [
 75097|         "member name 'TypeId' matches keyword pattern /^(Type|TypeId|GetTypeId)$/"
 75098|       ],
 75099|       "source_url": "https://www.revitapidocs.com/2025/86352cb2-b367-a806-7427-2fb08e50b425.htm",
 75100|       "dll_signature_verified": true,
 75101|       "dll_relationship_scope": "declared",
 75102|       "dll_semantic_verified": null,
 75103|       "dll_verified_status": "signature_verified_declared",
 75104|       "revitlookup_referenced": null,
 75105|       "revitlookup_requires_document_context": null
 75106|     },
 75107|     {
 75108|       "source": "Autodesk.Revit.DB.Infrastructure.AlignmentStationLabelSetOptions",
 75109|       "target": null,
 75110|       "member_name": "TypeId",
 75111|       "member_kind": "property",
 75112|       "edge_type": "TYPE_OF",
 75113|       "confidence": "elementid_with_strong_name",
 75114|       "confidence_tier": "core",
 75115|       "target_resolution": "none",
 75116|       "evidence": [
 75117|         "member name 'TypeId' matches keyword pattern /^(Type|TypeId|GetTypeId)$/"
 75118|       ],
 75119|       "source_url": "https://www.revitapidocs.com/2025/5b0dfa5d-bc2f-b097-a8d6-c5e78c569add.htm",
 75120|       "dll_signature_verified": true,
 75121|       "dll_relationship_scope": "declared",
 75122|       "dll_semantic_verified": null,
 75123|       "dll_verified_status": "signature_verified_declared",
 75124|       "revitlookup_referenced": null,
 75125|       "revitlookup_requires_document_context": null
 75126|     },
 75127|     {
 75128|       "source": "Autodesk.Revit.DB.Lighting.AdvancedLossFactor",
 75129|       "target": null,
 75130|       "member_name": "VoltageLossFactor",
 75131|       "member_kind": "property",
 75132|       "edge_type": "TAGS_ELEMENT",
 75133|       "confidence": "name_only_candidate",
 75134|       "confidence_tier": "likely",
 75135|       "target_resolution": "none",
 75136|       "evidence": [
 75137|         "member name 'VoltageLossFactor' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'double' gives no type-level confirmation"
 75138|       ],
 75139|       "source_url": "https://www.revitapidocs.com/2025/a21ac531-1639-37f2-314e-ab11e1e23b1c.htm",
 75140|       "dll_signature_verified": true,
 75141|       "dll_relationship_scope": "declared",
 75142|       "dll_semantic_verified": null,
 75143|       "dll_verified_status": "signature_verified_declared",
 75144|       "revitlookup_referenced": null,
 75145|       "revitlookup_requires_document_context": null
 75146|     },
 75147|     {
 75148|       "source": "Autodesk.Revit.DB.Lighting.InitialWattageIntensity",
 75149|       "target": null,
 75150|       "member_name": "Wattage",
 75151|       "member_kind": "property",
 75152|       "edge_type": "TAGS_ELEMENT",
 75153|       "confidence": "name_only_candidate",
 75154|       "confidence_tier": "likely",
 75155|       "target_resolution": "none",
 75156|       "evidence": [
 75157|         "member name 'Wattage' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'double' gives no type-level confirmation"
 75158|       ],
 75159|       "source_url": "https://www.revitapidocs.com/2025/3e602e58-3059-4c7f-f158-575d42984137.htm",
 75160|       "dll_signature_verified": true,
 75161|       "dll_relationship_scope": "declared",
 75162|       "dll_semantic_verified": null,
 75163|       "dll_verified_status": "signature_verified_declared",
 75164|       "revitlookup_referenced": null,
 75165|       "revitlookup_requires_document_context": null
 75166|     },
 75167|     {
 75168|       "source": "Autodesk.Revit.DB.Lighting.LightFamily",
 75169|       "target": "Autodesk.Revit.DB.Lighting.LightType",
 75170|       "member_name": "GetLightType",
 75171|       "member_kind": "method",
 75172|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 75173|       "confidence": "direct_return_type",
 75174|       "confidence_tier": "unverified_reference",
 75175|       "target_resolution": "short_name_fallback",
 75176|       "evidence": [
 75177|         "return type 'LightType' directly names a Revit DB object type"
 75178|       ],
 75179|       "source_url": "https://www.revitapidocs.com/2025/4418e7fd-50f4-22ed-9655-067e406af4b3.htm",
 75180|       "dll_signature_verified": true,
 75181|       "dll_relationship_scope": "declared",
 75182|       "dll_semantic_verified": null,
 75183|       "dll_verified_status": "signature_verified_declared",
 75184|       "revitlookup_referenced": true,
 75185|       "revitlookup_requires_document_context": false
 75186|     },
 75187|     {
 75188|       "source": "Autodesk.Revit.DB.Lighting.LightGroup",
 75189|       "target": null,
 75190|       "member_name": "Id",
 75191|       "member_kind": "property",
 75192|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 75193|       "confidence": "unknown_reference",
 75194|       "confidence_tier": "unverified_reference",
 75195|       "target_resolution": "none",
 75196|       "evidence": [
 75197|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 75198|       ],
 75199|       "source_url": "https://www.revitapidocs.com/2025/5788fc2a-90cf-ee4d-2ed0-fd56844ec5fd.htm",
 75200|       "dll_signature_verified": true,
 75201|       "dll_relationship_scope": "declared",
 75202|       "dll_semantic_verified": null,
 75203|       "dll_verified_status": "signature_verified_declared",
 75204|       "revitlookup_referenced": null,
 75205|       "revitlookup_requires_document_context": null
 75206|     },
 75207|     {
 75208|       "source": "Autodesk.Revit.DB.Lighting.LightGroup",
 75209|       "target": null,
 75210|       "member_name": "GetLights",
 75211|       "member_kind": "method",
 75212|       "edge_type": "RETURNS_ELEMENT_IDS",
 75213|       "confidence": "unknown_reference",
 75214|       "confidence_tier": "unverified_reference",
 75215|       "target_resolution": "none",
 75216|       "evidence": [
 75217|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 75218|       ],
 75219|       "source_url": "https://www.revitapidocs.com/2025/52e4acca-0194-8337-488f-95c7a9b29229.htm",
 75220|       "dll_signature_verified": true,
 75221|       "dll_relationship_scope": "declared",
 75222|       "dll_semantic_verified": null,
 75223|       "dll_verified_status": "signature_verified_declared",
 75224|       "revitlookup_referenced": null,
 75225|       "revitlookup_requires_document_context": null
 75226|     },
 75227|     {
 75228|       "source": "Autodesk.Revit.DB.Lighting.LightGroupManager",
 75229|       "target": null,
 75230|       "member_name": "DeleteGroup",
 75231|       "member_kind": "method",
 75232|       "edge_type": "MEMBER_OF_GROUP",
 75233|       "confidence": "name_only_candidate",
 75234|       "confidence_tier": "likely",
 75235|       "target_resolution": "none",
 75236|       "evidence": [
 75237|         "member name 'DeleteGroup' matches keyword pattern /^GetMember|Group/ but return type 'void' gives no type-level confirmation"
 75238|       ],
 75239|       "source_url": "https://www.revitapidocs.com/2025/64437c97-488a-d75f-6159-01be84f93ba5.htm",
 75240|       "dll_signature_verified": true,
 75241|       "dll_relationship_scope": "declared",
 75242|       "dll_semantic_verified": null,
 75243|       "dll_verified_status": "signature_verified_declared",
 75244|       "revitlookup_referenced": null,
 75245|       "revitlookup_requires_document_context": null
 75246|     },
 75247|     {
 75248|       "source": "Autodesk.Revit.DB.Lighting.LightGroupManager",
 75249|       "target": "Autodesk.Revit.DB.Lighting.LightGroup",
 75250|       "member_name": "GetGroups",
 75251|       "member_kind": "method",
 75252|       "edge_type": "MEMBER_OF_GROUP",
 75253|       "confidence": "needs_runtime_validation",
 75254|       "confidence_tier": "needs_validation",
 75255|       "target_resolution": "short_name_fallback",
 75256|       "evidence": [
 75257|         "return type 'IList < LightGroup >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 75258|       ],
 75259|       "source_url": "https://www.revitapidocs.com/2025/677017f7-5e44-3578-7ffe-184cc8d99d31.htm",
 75260|       "dll_signature_verified": true,
 75261|       "dll_relationship_scope": "declared",
 75262|       "dll_semantic_verified": null,
 75263|       "dll_verified_status": "signature_verified_declared",
 75264|       "revitlookup_referenced": null,
 75265|       "revitlookup_requires_document_context": null
 75266|     },
 75267|     {
 75268|       "source": "Autodesk.Revit.DB.Lighting.LightGroupManager",
 75269|       "target": null,
 75270|       "member_name": "IsLightGroupOn",
 75271|       "member_kind": "method",
 75272|       "edge_type": "MEMBER_OF_GROUP",
 75273|       "confidence": "name_only_candidate",
 75274|       "confidence_tier": "likely",
 75275|       "target_resolution": "none",
 75276|       "evidence": [
 75277|         "member name 'IsLightGroupOn' matches keyword pattern /^GetMember|Group/ but return type 'bool' gives no type-level confirmation"
 75278|       ],
 75279|       "source_url": "https://www.revitapidocs.com/2025/3214ec82-7ec9-ecab-e687-4e282ffe57b5.htm",
 75280|       "dll_signature_verified": true,
```
