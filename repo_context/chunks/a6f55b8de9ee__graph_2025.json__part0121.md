# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 121 of 216
- Original line range: 46801-47200
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
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
 46811|       ],
 46812|       "source_url": "https://www.revitapidocs.com/2025/0d712507-44f1-e2e3-807a-0a36a325bc09.htm",
 46813|       "dll_signature_verified": true,
 46814|       "dll_relationship_scope": "declared",
 46815|       "dll_semantic_verified": null,
 46816|       "dll_verified_status": "signature_verified_declared",
 46817|       "revitlookup_referenced": null,
 46818|       "revitlookup_requires_document_context": null
 46819|     },
 46820|     {
 46821|       "source": "Autodesk.Revit.DB.FilterRule",
 46822|       "target": null,
 46823|       "member_name": "GetRuleParameter",
 46824|       "member_kind": "method",
 46825|       "edge_type": "HAS_PARAMETER",
 46826|       "confidence": "elementid_with_strong_name",
 46827|       "confidence_tier": "core",
 46828|       "target_resolution": "none",
 46829|       "evidence": [
 46830|         "member name 'GetRuleParameter' matches keyword pattern /Parameter/"
 46831|       ],
 46832|       "source_url": "https://www.revitapidocs.com/2025/f30e47b9-df2f-8baa-ffeb-b957c8810156.htm",
 46833|       "dll_signature_verified": true,
 46834|       "dll_relationship_scope": "declared",
 46835|       "dll_semantic_verified": null,
 46836|       "dll_verified_status": "signature_verified_declared",
 46837|       "revitlookup_referenced": null,
 46838|       "revitlookup_requires_document_context": null
 46839|     },
 46840|     {
 46841|       "source": "Autodesk.Revit.DB.FilterStringRule",
 46842|       "target": "Autodesk.Revit.DB.FilterStringRuleEvaluator",
 46843|       "member_name": "GetEvaluator",
 46844|       "member_kind": "method",
 46845|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 46846|       "confidence": "direct_return_type",
 46847|       "confidence_tier": "unverified_reference",
 46848|       "target_resolution": "exact",
 46849|       "evidence": [
 46850|         "return type 'FilterStringRuleEvaluator' directly names a Revit DB object type"
 46851|       ],
 46852|       "source_url": "https://www.revitapidocs.com/2025/00247f33-3684-dc0e-e371-954d14c19536.htm",
 46853|       "dll_signature_verified": true,
 46854|       "dll_relationship_scope": "declared",
 46855|       "dll_semantic_verified": null,
 46856|       "dll_verified_status": "signature_verified_declared",
 46857|       "revitlookup_referenced": null,
 46858|       "revitlookup_requires_document_context": null
 46859|     },
 46860|     {
 46861|       "source": "Autodesk.Revit.DB.Floor",
 46862|       "target": "Autodesk.Revit.DB.FloorType",
 46863|       "member_name": "FloorType",
 46864|       "member_kind": "property",
 46865|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 46866|       "confidence": "direct_return_type",
 46867|       "confidence_tier": "unverified_reference",
 46868|       "target_resolution": "exact",
 46869|       "evidence": [
 46870|         "return type 'FloorType' directly names a Revit DB object type"
 46871|       ],
 46872|       "source_url": "https://www.revitapidocs.com/2025/18c20723-c74e-6924-406d-fd0d8bae7ae1.htm",
 46873|       "dll_signature_verified": true,
 46874|       "dll_relationship_scope": "declared",
 46875|       "dll_semantic_verified": null,
 46876|       "dll_verified_status": "signature_verified_declared",
 46877|       "revitlookup_referenced": null,
 46878|       "revitlookup_requires_document_context": null
 46879|     },
 46880|     {
 46881|       "source": "Autodesk.Revit.DB.Floor",
 46882|       "target": "Autodesk.Revit.DB.Sketch",
 46883|       "member_name": "SketchId",
 46884|       "member_kind": "property",
 46885|       "edge_type": "DEPENDS_ON",
 46886|       "confidence": "elementid_with_strong_name",
 46887|       "confidence_tier": "core",
 46888|       "target_resolution": "exact",
 46889|       "evidence": [
 46890|         "member name 'SketchId' matches keyword pattern /Sketch(Id)?$/"
 46891|       ],
 46892|       "source_url": "https://www.revitapidocs.com/2025/33130afb-7bc9-0229-0c54-b99a3edc21dd.htm",
 46893|       "dll_signature_verified": true,
 46894|       "dll_relationship_scope": "declared",
 46895|       "dll_semantic_verified": null,
 46896|       "dll_verified_status": "signature_verified_declared",
 46897|       "revitlookup_referenced": null,
 46898|       "revitlookup_requires_document_context": null
 46899|     },
 46900|     {
 46901|       "source": "Autodesk.Revit.DB.Floor",
 46902|       "target": null,
 46903|       "member_name": "GetDefaultFloorType",
 46904|       "member_kind": "method",
 46905|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 46906|       "confidence": "unknown_reference",
 46907|       "confidence_tier": "unverified_reference",
 46908|       "target_resolution": "none",
 46909|       "evidence": [
 46910|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 46911|       ],
 46912|       "source_url": "https://www.revitapidocs.com/2025/3eebff6a-ccfa-d4ab-fcf8-239d4d2ec8de.htm",
 46913|       "dll_signature_verified": true,
 46914|       "dll_relationship_scope": "declared",
 46915|       "dll_semantic_verified": null,
 46916|       "dll_verified_status": "signature_verified_declared",
 46917|       "revitlookup_referenced": null,
 46918|       "revitlookup_requires_document_context": null
 46919|     },
 46920|     {
 46921|       "source": "Autodesk.Revit.DB.Floor",
 46922|       "target": "Autodesk.Revit.DB.SlabShapeEditor",
 46923|       "member_name": "GetSlabShapeEditor",
 46924|       "member_kind": "method",
 46925|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 46926|       "confidence": "direct_return_type",
 46927|       "confidence_tier": "unverified_reference",
 46928|       "target_resolution": "exact",
 46929|       "evidence": [
 46930|         "return type 'SlabShapeEditor' directly names a Revit DB object type"
 46931|       ],
 46932|       "source_url": "https://www.revitapidocs.com/2025/0542ebc4-7ce6-023c-3801-860c4bf98134.htm",
 46933|       "dll_signature_verified": true,
 46934|       "dll_relationship_scope": "declared",
 46935|       "dll_semantic_verified": null,
 46936|       "dll_verified_status": "signature_verified_declared",
 46937|       "revitlookup_referenced": null,
 46938|       "revitlookup_requires_document_context": null
 46939|     },
 46940|     {
 46941|       "source": "Autodesk.Revit.DB.Floor",
 46942|       "target": null,
 46943|       "member_name": "GetSpanDirectionSymbolIds",
 46944|       "member_kind": "method",
 46945|       "edge_type": "RETURNS_ELEMENT_IDS",
 46946|       "confidence": "unknown_reference",
 46947|       "confidence_tier": "unverified_reference",
 46948|       "target_resolution": "none",
 46949|       "evidence": [
 46950|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 46951|       ],
 46952|       "source_url": "https://www.revitapidocs.com/2025/253ac763-7b9b-7efb-8eca-5b6599fd8d5f.htm",
 46953|       "dll_signature_verified": true,
 46954|       "dll_relationship_scope": "declared",
 46955|       "dll_semantic_verified": null,
 46956|       "dll_verified_status": "signature_verified_declared",
 46957|       "revitlookup_referenced": null,
 46958|       "revitlookup_requires_document_context": null
 46959|     },
 46960|     {
 46961|       "source": "Autodesk.Revit.DB.FloorType",
 46962|       "target": "Autodesk.Revit.DB.Material",
 46963|       "member_name": "StructuralMaterialId",
 46964|       "member_kind": "property",
 46965|       "edge_type": "USES_MATERIAL",
 46966|       "confidence": "elementid_with_strong_name",
 46967|       "confidence_tier": "core",
 46968|       "target_resolution": "exact",
 46969|       "evidence": [
 46970|         "member name 'StructuralMaterialId' matches keyword pattern /Material/"
 46971|       ],
 46972|       "source_url": "https://www.revitapidocs.com/2025/32e9a9f7-5d15-2391-c794-c038bc657f7d.htm",
 46973|       "dll_signature_verified": true,
 46974|       "dll_relationship_scope": "declared",
 46975|       "dll_semantic_verified": null,
 46976|       "dll_verified_status": "signature_verified_declared",
 46977|       "revitlookup_referenced": null,
 46978|       "revitlookup_requires_document_context": null
 46979|     },
 46980|     {
 46981|       "source": "Autodesk.Revit.DB.FloorType",
 46982|       "target": "Autodesk.Revit.DB.ThermalProperties",
 46983|       "member_name": "ThermalProperties",
 46984|       "member_kind": "property",
 46985|       "edge_type": "REFERENCES",
 46986|       "confidence": "direct_return_type",
 46987|       "confidence_tier": "core",
 46988|       "target_resolution": "exact",
 46989|       "evidence": [
 46990|         "return type 'ThermalProperties' directly names a Revit DB object type"
 46991|       ],
 46992|       "source_url": "https://www.revitapidocs.com/2025/6f9d4f62-7790-f885-da06-7711588359be.htm",
 46993|       "dll_signature_verified": true,
 46994|       "dll_relationship_scope": "declared",
 46995|       "dll_semantic_verified": null,
 46996|       "dll_verified_status": "signature_verified_declared",
 46997|       "revitlookup_referenced": null,
 46998|       "revitlookup_requires_document_context": null
 46999|     },
 47000|     {
 47001|       "source": "Autodesk.Revit.DB.FolderItemInfo",
 47002|       "target": null,
 47003|       "member_name": "ElementId",
 47004|       "member_kind": "property",
 47005|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 47006|       "confidence": "unknown_reference",
 47007|       "confidence_tier": "unverified_reference",
 47008|       "target_resolution": "none",
 47009|       "evidence": [
 47010|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 47011|       ],
 47012|       "source_url": "https://www.revitapidocs.com/2025/7f892bab-a50d-f9ce-f7a4-6b1f43e6c31c.htm",
 47013|       "dll_signature_verified": true,
 47014|       "dll_relationship_scope": "declared",
 47015|       "dll_semantic_verified": null,
 47016|       "dll_verified_status": "signature_verified_declared",
 47017|       "revitlookup_referenced": null,
 47018|       "revitlookup_requires_document_context": null
 47019|     },
 47020|     {
 47021|       "source": "Autodesk.Revit.DB.FootPrintRoof",
 47022|       "target": "Autodesk.Revit.DB.CurtainGridSet",
 47023|       "member_name": "CurtainGrids",
 47024|       "member_kind": "property",
 47025|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 47026|       "confidence": "direct_return_type",
 47027|       "confidence_tier": "unverified_reference",
 47028|       "target_resolution": "exact",
 47029|       "evidence": [
 47030|         "return type 'CurtainGridSet' directly names a Revit DB object type"
 47031|       ],
 47032|       "source_url": "https://www.revitapidocs.com/2025/4f8a36ba-c169-59c5-54cc-9dfbbd099b82.htm",
 47033|       "dll_signature_verified": true,
 47034|       "dll_relationship_scope": "declared",
 47035|       "dll_semantic_verified": null,
 47036|       "dll_verified_status": "signature_verified_declared",
 47037|       "revitlookup_referenced": null,
 47038|       "revitlookup_requires_document_context": null
 47039|     },
 47040|     {
 47041|       "source": "Autodesk.Revit.DB.FootPrintRoof",
 47042|       "target": "Autodesk.Revit.DB.ModelCurveArrArray",
 47043|       "member_name": "GetProfiles",
 47044|       "member_kind": "method",
 47045|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 47046|       "confidence": "direct_return_type",
 47047|       "confidence_tier": "unverified_reference",
 47048|       "target_resolution": "exact",
 47049|       "evidence": [
 47050|         "return type 'ModelCurveArrArray' directly names a Revit DB object type"
 47051|       ],
 47052|       "source_url": "https://www.revitapidocs.com/2025/72204bf1-21a8-0cbb-673c-630cc125b05c.htm",
 47053|       "dll_signature_verified": true,
 47054|       "dll_relationship_scope": "declared",
 47055|       "dll_semantic_verified": null,
 47056|       "dll_verified_status": "signature_verified_declared",
 47057|       "revitlookup_referenced": null,
 47058|       "revitlookup_requires_document_context": null
 47059|     },
 47060|     {
 47061|       "source": "Autodesk.Revit.DB.ForgeTypeId",
 47062|       "target": null,
 47063|       "member_name": "TypeId",
 47064|       "member_kind": "property",
 47065|       "edge_type": "TYPE_OF",
 47066|       "confidence": "name_only_candidate",
 47067|       "confidence_tier": "likely",
 47068|       "target_resolution": "none",
 47069|       "evidence": [
 47070|         "member name 'TypeId' matches keyword pattern /^(Type|TypeId|GetTypeId)$/ but return type 'string' gives no type-level confirmation"
 47071|       ],
 47072|       "source_url": "https://www.revitapidocs.com/2025/166624ff-caa2-3af3-694c-5fd89ce79865.htm",
 47073|       "dll_signature_verified": true,
 47074|       "dll_relationship_scope": "declared",
 47075|       "dll_semantic_verified": null,
 47076|       "dll_verified_status": "signature_verified_declared",
 47077|       "revitlookup_referenced": null,
 47078|       "revitlookup_requires_document_context": null
 47079|     },
 47080|     {
 47081|       "source": "Autodesk.Revit.DB.Form",
 47082|       "target": "Autodesk.Revit.DB.ReferenceArray",
 47083|       "member_name": "GetControlPoints",
 47084|       "member_kind": "method",
 47085|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 47086|       "confidence": "direct_return_type",
 47087|       "confidence_tier": "unverified_reference",
 47088|       "target_resolution": "exact",
 47089|       "evidence": [
 47090|         "return type 'ReferenceArray' directly names a Revit DB object type"
 47091|       ],
 47092|       "source_url": "https://www.revitapidocs.com/2025/04cad91d-8ab0-a4cb-05e6-801266a9cdf9.htm",
 47093|       "dll_signature_verified": true,
 47094|       "dll_relationship_scope": "declared",
 47095|       "dll_semantic_verified": null,
 47096|       "dll_verified_status": "signature_verified_declared",
 47097|       "revitlookup_referenced": null,
 47098|       "revitlookup_requires_document_context": null
 47099|     },
 47100|     {
 47101|       "source": "Autodesk.Revit.DB.Form",
 47102|       "target": "Autodesk.Revit.DB.ReferenceArray",
 47103|       "member_name": "GetCurvesAndEdgesReference",
 47104|       "member_kind": "method",
 47105|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 47106|       "confidence": "direct_return_type",
 47107|       "confidence_tier": "unverified_reference",
 47108|       "target_resolution": "exact",
 47109|       "evidence": [
 47110|         "return type 'ReferenceArray' directly names a Revit DB object type"
 47111|       ],
 47112|       "source_url": "https://www.revitapidocs.com/2025/9d505fbd-7fa2-937c-d0b8-7f4d78d97b51.htm",
 47113|       "dll_signature_verified": true,
 47114|       "dll_relationship_scope": "declared",
 47115|       "dll_semantic_verified": null,
 47116|       "dll_verified_status": "signature_verified_declared",
 47117|       "revitlookup_referenced": null,
 47118|       "revitlookup_requires_document_context": null
 47119|     },
 47120|     {
 47121|       "source": "Autodesk.Revit.DB.Form",
 47122|       "target": null,
 47123|       "member_name": "Rehost",
 47124|       "member_kind": "method",
 47125|       "edge_type": "HOSTED_BY",
 47126|       "confidence": "name_only_candidate",
 47127|       "confidence_tier": "likely",
 47128|       "target_resolution": "none",
 47129|       "evidence": [
 47130|         "member name 'Rehost' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 47131|       ],
 47132|       "source_url": "https://www.revitapidocs.com/2025/a222958c-4b12-075b-ade4-d78642c40d90.htm",
 47133|       "dll_signature_verified": true,
 47134|       "dll_relationship_scope": "declared",
 47135|       "dll_semantic_verified": null,
 47136|       "dll_verified_status": "signature_verified_declared",
 47137|       "revitlookup_referenced": null,
 47138|       "revitlookup_requires_document_context": null
 47139|     },
 47140|     {
 47141|       "source": "Autodesk.Revit.DB.Form",
 47142|       "target": null,
 47143|       "member_name": "Rehost",
 47144|       "member_kind": "method",
 47145|       "edge_type": "HOSTED_BY",
 47146|       "confidence": "name_only_candidate",
 47147|       "confidence_tier": "likely",
 47148|       "target_resolution": "none",
 47149|       "evidence": [
 47150|         "member name 'Rehost' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 47151|       ],
 47152|       "source_url": "https://www.revitapidocs.com/2025/80d96216-f5fd-0aa7-954b-33b7b0ddcf9b.htm",
 47153|       "dll_signature_verified": true,
 47154|       "dll_relationship_scope": "declared",
 47155|       "dll_semantic_verified": null,
 47156|       "dll_verified_status": "signature_verified_declared",
 47157|       "revitlookup_referenced": null,
 47158|       "revitlookup_requires_document_context": null
 47159|     },
 47160|     {
 47161|       "source": "Autodesk.Revit.DB.FormArray",
 47162|       "target": "Autodesk.Revit.DB.FormArrayIterator",
 47163|       "member_name": "ForwardIterator",
 47164|       "member_kind": "method",
 47165|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 47166|       "confidence": "direct_return_type",
 47167|       "confidence_tier": "unverified_reference",
 47168|       "target_resolution": "exact",
 47169|       "evidence": [
 47170|         "return type 'FormArrayIterator' directly names a Revit DB object type"
 47171|       ],
 47172|       "source_url": "https://www.revitapidocs.com/2025/8bc5330b-a4d3-0d88-a343-e9be04d33429.htm",
 47173|       "dll_signature_verified": true,
 47174|       "dll_relationship_scope": "declared",
 47175|       "dll_semantic_verified": null,
 47176|       "dll_verified_status": "signature_verified_declared",
 47177|       "revitlookup_referenced": null,
 47178|       "revitlookup_requires_document_context": null
 47179|     },
 47180|     {
 47181|       "source": "Autodesk.Revit.DB.FormArray",
 47182|       "target": "Autodesk.Revit.DB.FormArrayIterator",
 47183|       "member_name": "ReverseIterator",
 47184|       "member_kind": "method",
 47185|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 47186|       "confidence": "direct_return_type",
 47187|       "confidence_tier": "unverified_reference",
 47188|       "target_resolution": "exact",
 47189|       "evidence": [
 47190|         "return type 'FormArrayIterator' directly names a Revit DB object type"
 47191|       ],
 47192|       "source_url": "https://www.revitapidocs.com/2025/82d038e6-9a72-dda8-e0e7-a2b41c3e2640.htm",
 47193|       "dll_signature_verified": true,
 47194|       "dll_relationship_scope": "declared",
 47195|       "dll_semantic_verified": null,
 47196|       "dll_verified_status": "signature_verified_declared",
 47197|       "revitlookup_referenced": null,
 47198|       "revitlookup_requires_document_context": null
 47199|     },
 47200|     {
```
