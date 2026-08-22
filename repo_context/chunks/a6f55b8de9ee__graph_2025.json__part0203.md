# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 203 of 216
- Original line range: 78781-79180
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 78781|       "dll_relationship_scope": "declared",
 78782|       "dll_semantic_verified": null,
 78783|       "dll_verified_status": "signature_verified_declared",
 78784|       "revitlookup_referenced": null,
 78785|       "revitlookup_requires_document_context": null
 78786|     },
 78787|     {
 78788|       "source": "Autodesk.Revit.DB.Structure.LoadBase",
 78789|       "target": null,
 78790|       "member_name": "HostElementId",
 78791|       "member_kind": "property",
 78792|       "edge_type": "HOSTED_BY",
 78793|       "confidence": "elementid_with_strong_name",
 78794|       "confidence_tier": "core",
 78795|       "target_resolution": "none",
 78796|       "evidence": [
 78797|         "member name 'HostElementId' matches keyword pattern /^GetHosted|Host/"
 78798|       ],
 78799|       "source_url": "https://www.revitapidocs.com/2025/c0fc581d-431e-0749-e7d9-7218e9b426c8.htm",
 78800|       "dll_signature_verified": true,
 78801|       "dll_relationship_scope": "declared",
 78802|       "dll_semantic_verified": null,
 78803|       "dll_verified_status": "signature_verified_declared",
 78804|       "revitlookup_referenced": null,
 78805|       "revitlookup_requires_document_context": null
 78806|     },
 78807|     {
 78808|       "source": "Autodesk.Revit.DB.Structure.LoadBase",
 78809|       "target": null,
 78810|       "member_name": "IsConstrainedOnHost",
 78811|       "member_kind": "property",
 78812|       "edge_type": "HOSTED_BY",
 78813|       "confidence": "name_only_candidate",
 78814|       "confidence_tier": "likely",
 78815|       "target_resolution": "none",
 78816|       "evidence": [
 78817|         "member name 'IsConstrainedOnHost' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 78818|       ],
 78819|       "source_url": "https://www.revitapidocs.com/2025/355eed18-c9de-4a00-29fa-82ca04462a6a.htm",
 78820|       "dll_signature_verified": true,
 78821|       "dll_relationship_scope": "declared",
 78822|       "dll_semantic_verified": null,
 78823|       "dll_verified_status": "signature_verified_declared",
 78824|       "revitlookup_referenced": null,
 78825|       "revitlookup_requires_document_context": null
 78826|     },
 78827|     {
 78828|       "source": "Autodesk.Revit.DB.Structure.LoadBase",
 78829|       "target": null,
 78830|       "member_name": "IsHosted",
 78831|       "member_kind": "property",
 78832|       "edge_type": "HOSTED_BY",
 78833|       "confidence": "name_only_candidate",
 78834|       "confidence_tier": "likely",
 78835|       "target_resolution": "none",
 78836|       "evidence": [
 78837|         "member name 'IsHosted' matches keyword pattern /^GetHosted|Host/ but return type 'bool' gives no type-level confirmation"
 78838|       ],
 78839|       "source_url": "https://www.revitapidocs.com/2025/76965c6d-473a-9ad9-8a72-baa7a47b055a.htm",
 78840|       "dll_signature_verified": true,
 78841|       "dll_relationship_scope": "declared",
 78842|       "dll_semantic_verified": null,
 78843|       "dll_verified_status": "signature_verified_declared",
 78844|       "revitlookup_referenced": null,
 78845|       "revitlookup_requires_document_context": null
 78846|     },
 78847|     {
 78848|       "source": "Autodesk.Revit.DB.Structure.LoadBase",
 78849|       "target": "Autodesk.Revit.DB.Structure.LoadCase",
 78850|       "member_name": "LoadCase",
 78851|       "member_kind": "property",
 78852|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 78853|       "confidence": "direct_return_type",
 78854|       "confidence_tier": "unverified_reference",
 78855|       "target_resolution": "short_name_fallback",
 78856|       "evidence": [
 78857|         "return type 'LoadCase' directly names a Revit DB object type"
 78858|       ],
 78859|       "source_url": "https://www.revitapidocs.com/2025/f0d56d0d-dc16-1fb2-f27f-70a989f6bbcf.htm",
 78860|       "dll_signature_verified": true,
 78861|       "dll_relationship_scope": "declared",
 78862|       "dll_semantic_verified": null,
 78863|       "dll_verified_status": "signature_verified_declared",
 78864|       "revitlookup_referenced": null,
 78865|       "revitlookup_requires_document_context": null
 78866|     },
 78867|     {
 78868|       "source": "Autodesk.Revit.DB.Structure.LoadBase",
 78869|       "target": null,
 78870|       "member_name": "LoadCaseId",
 78871|       "member_kind": "property",
 78872|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 78873|       "confidence": "unknown_reference",
 78874|       "confidence_tier": "unverified_reference",
 78875|       "target_resolution": "none",
 78876|       "evidence": [
 78877|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 78878|       ],
 78879|       "source_url": "https://www.revitapidocs.com/2025/8bb0908c-30a7-f090-7586-b1bab8e8cc42.htm",
 78880|       "dll_signature_verified": true,
 78881|       "dll_relationship_scope": "declared",
 78882|       "dll_semantic_verified": null,
 78883|       "dll_verified_status": "signature_verified_declared",
 78884|       "revitlookup_referenced": null,
 78885|       "revitlookup_requires_document_context": null
 78886|     },
 78887|     {
 78888|       "source": "Autodesk.Revit.DB.Structure.LoadBase",
 78889|       "target": "Autodesk.Revit.DB.Category",
 78890|       "member_name": "LoadCategoryName",
 78891|       "member_kind": "property",
 78892|       "edge_type": "HAS_CATEGORY",
 78893|       "confidence": "name_only_candidate",
 78894|       "confidence_tier": "likely",
 78895|       "target_resolution": "exact",
 78896|       "evidence": [
 78897|         "member name 'LoadCategoryName' matches keyword pattern /Category/ but return type 'string' gives no type-level confirmation"
 78898|       ],
 78899|       "source_url": "https://www.revitapidocs.com/2025/4a1c7865-8f07-b291-6cc8-fb23a842dad8.htm",
 78900|       "dll_signature_verified": true,
 78901|       "dll_relationship_scope": "declared",
 78902|       "dll_semantic_verified": null,
 78903|       "dll_verified_status": "signature_verified_declared",
 78904|       "revitlookup_referenced": null,
 78905|       "revitlookup_requires_document_context": null
 78906|     },
 78907|     {
 78908|       "source": "Autodesk.Revit.DB.Structure.LoadBase",
 78909|       "target": null,
 78910|       "member_name": "WorkPlaneId",
 78911|       "member_kind": "property",
 78912|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 78913|       "confidence": "unknown_reference",
 78914|       "confidence_tier": "unverified_reference",
 78915|       "target_resolution": "none",
 78916|       "evidence": [
 78917|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 78918|       ],
 78919|       "source_url": "https://www.revitapidocs.com/2025/19d3d2d4-82e9-465a-5cf4-d3e613ad238f.htm",
 78920|       "dll_signature_verified": true,
 78921|       "dll_relationship_scope": "declared",
 78922|       "dll_semantic_verified": null,
 78923|       "dll_verified_status": "signature_verified_declared",
 78924|       "revitlookup_referenced": null,
 78925|       "revitlookup_requires_document_context": null
 78926|     },
 78927|     {
 78928|       "source": "Autodesk.Revit.DB.Structure.LoadBase",
 78929|       "target": null,
 78930|       "member_name": "RemoveHostConstraint",
 78931|       "member_kind": "method",
 78932|       "edge_type": "HOSTED_BY",
 78933|       "confidence": "name_only_candidate",
 78934|       "confidence_tier": "likely",
 78935|       "target_resolution": "none",
 78936|       "evidence": [
 78937|         "member name 'RemoveHostConstraint' matches keyword pattern /^GetHosted|Host/ but return type 'void' gives no type-level confirmation"
 78938|       ],
 78939|       "source_url": "https://www.revitapidocs.com/2025/f94ac73c-c975-70e5-f905-966439f5163a.htm",
 78940|       "dll_signature_verified": true,
 78941|       "dll_relationship_scope": "declared",
 78942|       "dll_semantic_verified": null,
 78943|       "dll_verified_status": "signature_verified_declared",
 78944|       "revitlookup_referenced": null,
 78945|       "revitlookup_requires_document_context": null
 78946|     },
 78947|     {
 78948|       "source": "Autodesk.Revit.DB.Structure.LoadCase",
 78949|       "target": null,
 78950|       "member_name": "NatureId",
 78951|       "member_kind": "property",
 78952|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 78953|       "confidence": "unknown_reference",
 78954|       "confidence_tier": "unverified_reference",
 78955|       "target_resolution": "none",
 78956|       "evidence": [
 78957|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 78958|       ],
 78959|       "source_url": "https://www.revitapidocs.com/2025/afb018a6-fe54-38af-1fbd-f6ff15c7ad8f.htm",
 78960|       "dll_signature_verified": true,
 78961|       "dll_relationship_scope": "declared",
 78962|       "dll_semantic_verified": null,
 78963|       "dll_verified_status": "signature_verified_declared",
 78964|       "revitlookup_referenced": null,
 78965|       "revitlookup_requires_document_context": null
 78966|     },
 78967|     {
 78968|       "source": "Autodesk.Revit.DB.Structure.LoadCase",
 78969|       "target": "Autodesk.Revit.DB.Category",
 78970|       "member_name": "SubcategoryId",
 78971|       "member_kind": "property",
 78972|       "edge_type": "HAS_CATEGORY",
 78973|       "confidence": "elementid_with_strong_name",
 78974|       "confidence_tier": "core",
 78975|       "target_resolution": "exact",
 78976|       "evidence": [
 78977|         "member name 'SubcategoryId' matches keyword pattern /Category/"
 78978|       ],
 78979|       "source_url": "https://www.revitapidocs.com/2025/1497f43e-609a-7cbc-4a08-2cd4e8f14ec7.htm",
 78980|       "dll_signature_verified": true,
 78981|       "dll_relationship_scope": "declared",
 78982|       "dll_semantic_verified": null,
 78983|       "dll_verified_status": "signature_verified_declared",
 78984|       "revitlookup_referenced": null,
 78985|       "revitlookup_requires_document_context": null
 78986|     },
 78987|     {
 78988|       "source": "Autodesk.Revit.DB.Structure.LoadCase",
 78989|       "target": "Autodesk.Revit.DB.Category",
 78990|       "member_name": "IsLoadCaseSubcategoryId",
 78991|       "member_kind": "method",
 78992|       "edge_type": "HAS_CATEGORY",
 78993|       "confidence": "name_only_candidate",
 78994|       "confidence_tier": "likely",
 78995|       "target_resolution": "exact",
 78996|       "evidence": [
 78997|         "member name 'IsLoadCaseSubcategoryId' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 78998|       ],
 78999|       "source_url": "https://www.revitapidocs.com/2025/734e2dd6-2686-2ec5-2b5c-ed0566f97630.htm",
 79000|       "dll_signature_verified": true,
 79001|       "dll_relationship_scope": "declared",
 79002|       "dll_semantic_verified": null,
 79003|       "dll_verified_status": "signature_verified_declared",
 79004|       "revitlookup_referenced": null,
 79005|       "revitlookup_requires_document_context": null
 79006|     },
 79007|     {
 79008|       "source": "Autodesk.Revit.DB.Structure.LoadCombination",
 79009|       "target": null,
 79010|       "member_name": "Type",
 79011|       "member_kind": "property",
 79012|       "edge_type": "TYPE_OF",
 79013|       "confidence": "name_only_candidate",
 79014|       "confidence_tier": "likely",
 79015|       "target_resolution": "none",
 79016|       "evidence": [
 79017|         "member name 'Type' matches keyword pattern /^(Type|TypeId|GetTypeId)$/ but return type 'LoadCombinationType' gives no type-level confirmation"
 79018|       ],
 79019|       "source_url": "https://www.revitapidocs.com/2025/a8032183-6581-bc85-17b6-73be32d84207.htm",
 79020|       "dll_signature_verified": true,
 79021|       "dll_relationship_scope": "declared",
 79022|       "dll_semantic_verified": null,
 79023|       "dll_verified_status": "signature_verified_declared",
 79024|       "revitlookup_referenced": null,
 79025|       "revitlookup_requires_document_context": null
 79026|     },
 79027|     {
 79028|       "source": "Autodesk.Revit.DB.Structure.LoadCombination",
 79029|       "target": null,
 79030|       "member_name": "GetCaseAndCombinationIds",
 79031|       "member_kind": "method",
 79032|       "edge_type": "RETURNS_ELEMENT_IDS",
 79033|       "confidence": "unknown_reference",
 79034|       "confidence_tier": "unverified_reference",
 79035|       "target_resolution": "none",
 79036|       "evidence": [
 79037|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 79038|       ],
 79039|       "source_url": "https://www.revitapidocs.com/2025/c1dc497b-5eaf-5ec9-2cb3-70eb5242a1ed.htm",
 79040|       "dll_signature_verified": true,
 79041|       "dll_relationship_scope": "declared",
 79042|       "dll_semantic_verified": null,
 79043|       "dll_verified_status": "signature_verified_declared",
 79044|       "revitlookup_referenced": null,
 79045|       "revitlookup_requires_document_context": null
 79046|     },
 79047|     {
 79048|       "source": "Autodesk.Revit.DB.Structure.LoadCombination",
 79049|       "target": "Autodesk.Revit.DB.Structure.LoadComponent",
 79050|       "member_name": "GetComponents",
 79051|       "member_kind": "method",
 79052|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 79053|       "confidence": "needs_runtime_validation",
 79054|       "confidence_tier": "needs_validation",
 79055|       "target_resolution": "short_name_fallback",
 79056|       "evidence": [
 79057|         "return type 'IList < LoadComponent >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 79058|       ],
 79059|       "source_url": "https://www.revitapidocs.com/2025/f90551cc-930a-2b43-7965-109c16833c78.htm",
 79060|       "dll_signature_verified": true,
 79061|       "dll_relationship_scope": "declared",
 79062|       "dll_semantic_verified": null,
 79063|       "dll_verified_status": "signature_verified_declared",
 79064|       "revitlookup_referenced": null,
 79065|       "revitlookup_requires_document_context": null
 79066|     },
 79067|     {
 79068|       "source": "Autodesk.Revit.DB.Structure.LoadCombination",
 79069|       "target": null,
 79070|       "member_name": "GetUsageIds",
 79071|       "member_kind": "method",
 79072|       "edge_type": "RETURNS_ELEMENT_IDS",
 79073|       "confidence": "unknown_reference",
 79074|       "confidence_tier": "unverified_reference",
 79075|       "target_resolution": "none",
 79076|       "evidence": [
 79077|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 79078|       ],
 79079|       "source_url": "https://www.revitapidocs.com/2025/f39b9703-4c1b-5188-0cba-eeaf04003b4e.htm",
 79080|       "dll_signature_verified": true,
 79081|       "dll_relationship_scope": "declared",
 79082|       "dll_semantic_verified": null,
 79083|       "dll_verified_status": "signature_verified_declared",
 79084|       "revitlookup_referenced": null,
 79085|       "revitlookup_requires_document_context": null
 79086|     },
 79087|     {
 79088|       "source": "Autodesk.Revit.DB.Structure.LoadComponent",
 79089|       "target": null,
 79090|       "member_name": "LoadCaseOrCombinationId",
 79091|       "member_kind": "property",
 79092|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 79093|       "confidence": "unknown_reference",
 79094|       "confidence_tier": "unverified_reference",
 79095|       "target_resolution": "none",
 79096|       "evidence": [
 79097|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 79098|       ],
 79099|       "source_url": "https://www.revitapidocs.com/2025/39b5a86b-e63a-9ea6-0e8c-17f6ab2a48ae.htm",
 79100|       "dll_signature_verified": true,
 79101|       "dll_relationship_scope": "declared",
 79102|       "dll_semantic_verified": null,
 79103|       "dll_verified_status": "signature_verified_declared",
 79104|       "revitlookup_referenced": null,
 79105|       "revitlookup_requires_document_context": null
 79106|     },
 79107|     {
 79108|       "source": "Autodesk.Revit.DB.Structure.MemberForcesServiceData",
 79109|       "target": "Autodesk.Revit.DB.Document",
 79110|       "member_name": "Document",
 79111|       "member_kind": "property",
 79112|       "edge_type": "REFERENCES",
 79113|       "confidence": "direct_return_type",
 79114|       "confidence_tier": "core",
 79115|       "target_resolution": "exact",
 79116|       "evidence": [
 79117|         "return type 'Document' directly names a Revit DB object type"
 79118|       ],
 79119|       "source_url": "https://www.revitapidocs.com/2025/10f89224-a663-9eca-0824-b8258c9a2bf0.htm",
 79120|       "dll_signature_verified": true,
 79121|       "dll_relationship_scope": "declared",
 79122|       "dll_semantic_verified": null,
 79123|       "dll_verified_status": "signature_verified_declared",
 79124|       "revitlookup_referenced": null,
 79125|       "revitlookup_requires_document_context": null
 79126|     },
 79127|     {
 79128|       "source": "Autodesk.Revit.DB.Structure.MemberForcesServiceData",
 79129|       "target": null,
 79130|       "member_name": "GetCurrentElements",
 79131|       "member_kind": "method",
 79132|       "edge_type": "RETURNS_ELEMENT_IDS",
 79133|       "confidence": "unknown_reference",
 79134|       "confidence_tier": "unverified_reference",
 79135|       "target_resolution": "none",
 79136|       "evidence": [
 79137|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 79138|       ],
 79139|       "source_url": "https://www.revitapidocs.com/2025/f7440101-4db4-5db4-e6cb-8ef6520a7298.htm",
 79140|       "dll_signature_verified": true,
 79141|       "dll_relationship_scope": "declared",
 79142|       "dll_semantic_verified": null,
 79143|       "dll_verified_status": "signature_verified_declared",
 79144|       "revitlookup_referenced": null,
 79145|       "revitlookup_requires_document_context": null
 79146|     },
 79147|     {
 79148|       "source": "Autodesk.Revit.DB.Structure.PathReinforcement",
 79149|       "target": null,
 79150|       "member_name": "AlternatingBarShapeId",
 79151|       "member_kind": "property",
 79152|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 79153|       "confidence": "unknown_reference",
 79154|       "confidence_tier": "unverified_reference",
 79155|       "target_resolution": "none",
 79156|       "evidence": [
 79157|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 79158|       ],
 79159|       "source_url": "https://www.revitapidocs.com/2025/192a6b79-2b79-b4f3-a084-8dd0940d9e42.htm",
 79160|       "dll_signature_verified": true,
 79161|       "dll_relationship_scope": "declared",
 79162|       "dll_semantic_verified": null,
 79163|       "dll_verified_status": "signature_verified_declared",
 79164|       "revitlookup_referenced": null,
 79165|       "revitlookup_requires_document_context": null
 79166|     },
 79167|     {
 79168|       "source": "Autodesk.Revit.DB.Structure.PathReinforcement",
 79169|       "target": "Autodesk.Revit.DB.Structure.PathReinforcementType",
 79170|       "member_name": "PathReinforcementType",
 79171|       "member_kind": "property",
 79172|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 79173|       "confidence": "direct_return_type",
 79174|       "confidence_tier": "unverified_reference",
 79175|       "target_resolution": "short_name_fallback",
 79176|       "evidence": [
 79177|         "return type 'PathReinforcementType' directly names a Revit DB object type"
 79178|       ],
 79179|       "source_url": "https://www.revitapidocs.com/2025/01592a23-de13-fd3e-7508-55e1d83994d1.htm",
 79180|       "dll_signature_verified": true,
```
