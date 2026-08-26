# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 185 of 216
- Original line range: 71761-72160
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 71761|       "revitlookup_requires_document_context": null
 71762|     },
 71763|     {
 71764|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleData",
 71765|       "target": null,
 71766|       "member_name": "GetLoadClassifications",
 71767|       "member_kind": "method",
 71768|       "edge_type": "RETURNS_ELEMENT_IDS",
 71769|       "confidence": "unknown_reference",
 71770|       "confidence_tier": "unverified_reference",
 71771|       "target_resolution": "none",
 71772|       "evidence": [
 71773|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 71774|       ],
 71775|       "source_url": "https://www.revitapidocs.com/2025/bdcb0fa5-478c-c968-d224-9444bc850fd7.htm",
 71776|       "dll_signature_verified": true,
 71777|       "dll_relationship_scope": "declared",
 71778|       "dll_semantic_verified": null,
 71779|       "dll_verified_status": "signature_verified_declared",
 71780|       "revitlookup_referenced": null,
 71781|       "revitlookup_requires_document_context": null
 71782|     },
 71783|     {
 71784|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleData",
 71785|       "target": "Autodesk.Revit.DB.View",
 71786|       "member_name": "UpdateCircuitTableForTemplate",
 71787|       "member_kind": "method",
 71788|       "edge_type": "CONTROLLED_BY_TEMPLATE",
 71789|       "confidence": "name_only_candidate",
 71790|       "confidence_tier": "likely",
 71791|       "target_resolution": "exact",
 71792|       "evidence": [
 71793|         "member name 'UpdateCircuitTableForTemplate' matches keyword pattern /Template/ but return type 'void' gives no type-level confirmation"
 71794|       ],
 71795|       "source_url": "https://www.revitapidocs.com/2025/9453a173-c467-0c49-9c1e-a5f413d95dec.htm",
 71796|       "dll_signature_verified": true,
 71797|       "dll_relationship_scope": "declared",
 71798|       "dll_semantic_verified": null,
 71799|       "dll_verified_status": "signature_verified_declared",
 71800|       "revitlookup_referenced": null,
 71801|       "revitlookup_requires_document_context": null
 71802|     },
 71803|     {
 71804|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleSheetInstance",
 71805|       "target": null,
 71806|       "member_name": "ScheduleId",
 71807|       "member_kind": "property",
 71808|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 71809|       "confidence": "unknown_reference",
 71810|       "confidence_tier": "unverified_reference",
 71811|       "target_resolution": "none",
 71812|       "evidence": [
 71813|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 71814|       ],
 71815|       "source_url": "https://www.revitapidocs.com/2025/ac8663cf-52bf-f4f4-b457-4f1b443f6840.htm",
 71816|       "dll_signature_verified": true,
 71817|       "dll_relationship_scope": "declared",
 71818|       "dll_semantic_verified": null,
 71819|       "dll_verified_status": "signature_verified_declared",
 71820|       "revitlookup_referenced": null,
 71821|       "revitlookup_requires_document_context": null
 71822|     },
 71823|     {
 71824|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleSheetInstance",
 71825|       "target": "Autodesk.Revit.DB.Electrical.PanelScheduleView",
 71826|       "member_name": "GetSchedule",
 71827|       "member_kind": "method",
 71828|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 71829|       "confidence": "direct_return_type",
 71830|       "confidence_tier": "unverified_reference",
 71831|       "target_resolution": "short_name_fallback",
 71832|       "evidence": [
 71833|         "return type 'PanelScheduleView' directly names a Revit DB object type"
 71834|       ],
 71835|       "source_url": "https://www.revitapidocs.com/2025/13e276fc-7ce3-ec0e-2667-6f8c38cd3865.htm",
 71836|       "dll_signature_verified": true,
 71837|       "dll_relationship_scope": "declared",
 71838|       "dll_semantic_verified": null,
 71839|       "dll_verified_status": "signature_verified_declared",
 71840|       "revitlookup_referenced": null,
 71841|       "revitlookup_requires_document_context": null
 71842|     },
 71843|     {
 71844|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleTemplate",
 71845|       "target": "Autodesk.Revit.DB.TableSectionData",
 71846|       "member_name": "GetSectionData",
 71847|       "member_kind": "method",
 71848|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 71849|       "confidence": "direct_return_type",
 71850|       "confidence_tier": "unverified_reference",
 71851|       "target_resolution": "exact",
 71852|       "evidence": [
 71853|         "return type 'TableSectionData' directly names a Revit DB object type"
 71854|       ],
 71855|       "source_url": "https://www.revitapidocs.com/2025/232a1d1e-9112-362c-7539-4ce3b68689b9.htm",
 71856|       "dll_signature_verified": true,
 71857|       "dll_relationship_scope": "declared",
 71858|       "dll_semantic_verified": null,
 71859|       "dll_verified_status": "signature_verified_declared",
 71860|       "revitlookup_referenced": null,
 71861|       "revitlookup_requires_document_context": null
 71862|     },
 71863|     {
 71864|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleTemplate",
 71865|       "target": "Autodesk.Revit.DB.Electrical.PanelScheduleData",
 71866|       "member_name": "GetTableData",
 71867|       "member_kind": "method",
 71868|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 71869|       "confidence": "direct_return_type",
 71870|       "confidence_tier": "unverified_reference",
 71871|       "target_resolution": "short_name_fallback",
 71872|       "evidence": [
 71873|         "return type 'PanelScheduleData' directly names a Revit DB object type"
 71874|       ],
 71875|       "source_url": "https://www.revitapidocs.com/2025/0e1fec60-7429-5eba-b2a4-25ac0439e3c5.htm",
 71876|       "dll_signature_verified": true,
 71877|       "dll_relationship_scope": "declared",
 71878|       "dll_semantic_verified": null,
 71879|       "dll_verified_status": "signature_verified_declared",
 71880|       "revitlookup_referenced": null,
 71881|       "revitlookup_requires_document_context": null
 71882|     },
 71883|     {
 71884|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleView",
 71885|       "target": "Autodesk.Revit.DB.View",
 71886|       "member_name": "GenerateInstanceFromTemplate",
 71887|       "member_kind": "method",
 71888|       "edge_type": "CONTROLLED_BY_TEMPLATE",
 71889|       "confidence": "name_only_candidate",
 71890|       "confidence_tier": "likely",
 71891|       "target_resolution": "exact",
 71892|       "evidence": [
 71893|         "member name 'GenerateInstanceFromTemplate' matches keyword pattern /Template/ but return type 'void' gives no type-level confirmation"
 71894|       ],
 71895|       "source_url": "https://www.revitapidocs.com/2025/542faccb-031b-6579-8d70-3f166a1821ca.htm",
 71896|       "dll_signature_verified": true,
 71897|       "dll_relationship_scope": "declared",
 71898|       "dll_semantic_verified": null,
 71899|       "dll_verified_status": "signature_verified_declared",
 71900|       "revitlookup_referenced": null,
 71901|       "revitlookup_requires_document_context": null
 71902|     },
 71903|     {
 71904|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleView",
 71905|       "target": "Autodesk.Revit.DB.Phase",
 71906|       "member_name": "GetApparentPhaseValue",
 71907|       "member_kind": "method",
 71908|       "edge_type": "ASSIGNED_TO_PHASE",
 71909|       "confidence": "name_only_candidate",
 71910|       "confidence_tier": "likely",
 71911|       "target_resolution": "exact",
 71912|       "evidence": [
 71913|         "member name 'GetApparentPhaseValue' matches keyword pattern /Phase/ but return type 'double' gives no type-level confirmation"
 71914|       ],
 71915|       "source_url": "https://www.revitapidocs.com/2025/dcc2ccdd-f1c6-1eec-aed2-4824c5355280.htm",
 71916|       "dll_signature_verified": true,
 71917|       "dll_relationship_scope": "declared",
 71918|       "dll_semantic_verified": null,
 71919|       "dll_verified_status": "signature_verified_declared",
 71920|       "revitlookup_referenced": null,
 71921|       "revitlookup_requires_document_context": null
 71922|     },
 71923|     {
 71924|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleView",
 71925|       "target": "Autodesk.Revit.DB.Electrical.ElectricalSystem",
 71926|       "member_name": "GetCircuitByCell",
 71927|       "member_kind": "method",
 71928|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 71929|       "confidence": "direct_return_type",
 71930|       "confidence_tier": "unverified_reference",
 71931|       "target_resolution": "short_name_fallback",
 71932|       "evidence": [
 71933|         "return type 'ElectricalSystem' directly names a Revit DB object type"
 71934|       ],
 71935|       "source_url": "https://www.revitapidocs.com/2025/cb09df9c-a670-ac4d-d9b6-111641a99c03.htm",
 71936|       "dll_signature_verified": true,
 71937|       "dll_relationship_scope": "declared",
 71938|       "dll_semantic_verified": null,
 71939|       "dll_verified_status": "signature_verified_declared",
 71940|       "revitlookup_referenced": null,
 71941|       "revitlookup_requires_document_context": null
 71942|     },
 71943|     {
 71944|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleView",
 71945|       "target": null,
 71946|       "member_name": "GetCircuitIdByCell",
 71947|       "member_kind": "method",
 71948|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 71949|       "confidence": "unknown_reference",
 71950|       "confidence_tier": "unverified_reference",
 71951|       "target_resolution": "none",
 71952|       "evidence": [
 71953|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 71954|       ],
 71955|       "source_url": "https://www.revitapidocs.com/2025/a71fd35c-6945-fadd-40b8-1db70844b440.htm",
 71956|       "dll_signature_verified": true,
 71957|       "dll_relationship_scope": "declared",
 71958|       "dll_semantic_verified": null,
 71959|       "dll_verified_status": "signature_verified_declared",
 71960|       "revitlookup_referenced": null,
 71961|       "revitlookup_requires_document_context": null
 71962|     },
 71963|     {
 71964|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleView",
 71965|       "target": null,
 71966|       "member_name": "GetLoadClassificationId",
 71967|       "member_kind": "method",
 71968|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 71969|       "confidence": "unknown_reference",
 71970|       "confidence_tier": "unverified_reference",
 71971|       "target_resolution": "none",
 71972|       "evidence": [
 71973|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 71974|       ],
 71975|       "source_url": "https://www.revitapidocs.com/2025/0818a43a-e096-8959-b470-21c8acb23f68.htm",
 71976|       "dll_signature_verified": true,
 71977|       "dll_relationship_scope": "declared",
 71978|       "dll_semantic_verified": null,
 71979|       "dll_verified_status": "signature_verified_declared",
 71980|       "revitlookup_referenced": null,
 71981|       "revitlookup_requires_document_context": null
 71982|     },
 71983|     {
 71984|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleView",
 71985|       "target": null,
 71986|       "member_name": "GetPanel",
 71987|       "member_kind": "method",
 71988|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 71989|       "confidence": "unknown_reference",
 71990|       "confidence_tier": "unverified_reference",
 71991|       "target_resolution": "none",
 71992|       "evidence": [
 71993|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 71994|       ],
 71995|       "source_url": "https://www.revitapidocs.com/2025/6a2eca1f-9e8c-53d5-a3cb-fa483d721223.htm",
 71996|       "dll_signature_verified": true,
 71997|       "dll_relationship_scope": "declared",
 71998|       "dll_semantic_verified": null,
 71999|       "dll_verified_status": "signature_verified_declared",
 72000|       "revitlookup_referenced": null,
 72001|       "revitlookup_requires_document_context": null
 72002|     },
 72003|     {
 72004|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleView",
 72005|       "target": "Autodesk.Revit.DB.TableSectionData",
 72006|       "member_name": "GetSectionData",
 72007|       "member_kind": "method",
 72008|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 72009|       "confidence": "direct_return_type",
 72010|       "confidence_tier": "unverified_reference",
 72011|       "target_resolution": "exact",
 72012|       "evidence": [
 72013|         "return type 'TableSectionData' directly names a Revit DB object type"
 72014|       ],
 72015|       "source_url": "https://www.revitapidocs.com/2025/67b163c9-8a6a-08bd-3abf-ae2d54e3c7e0.htm",
 72016|       "dll_signature_verified": true,
 72017|       "dll_relationship_scope": "declared",
 72018|       "dll_semantic_verified": null,
 72019|       "dll_verified_status": "signature_verified_declared",
 72020|       "revitlookup_referenced": null,
 72021|       "revitlookup_requires_document_context": null
 72022|     },
 72023|     {
 72024|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleView",
 72025|       "target": "Autodesk.Revit.DB.Electrical.PanelScheduleData",
 72026|       "member_name": "GetTableData",
 72027|       "member_kind": "method",
 72028|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 72029|       "confidence": "direct_return_type",
 72030|       "confidence_tier": "unverified_reference",
 72031|       "target_resolution": "short_name_fallback",
 72032|       "evidence": [
 72033|         "return type 'PanelScheduleData' directly names a Revit DB object type"
 72034|       ],
 72035|       "source_url": "https://www.revitapidocs.com/2025/5369a4b6-3b0b-e880-956c-0c2520e7022d.htm",
 72036|       "dll_signature_verified": true,
 72037|       "dll_relationship_scope": "declared",
 72038|       "dll_semantic_verified": null,
 72039|       "dll_verified_status": "signature_verified_declared",
 72040|       "revitlookup_referenced": null,
 72041|       "revitlookup_requires_document_context": null
 72042|     },
 72043|     {
 72044|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleView",
 72045|       "target": "Autodesk.Revit.DB.View",
 72046|       "member_name": "GetTemplate",
 72047|       "member_kind": "method",
 72048|       "edge_type": "CONTROLLED_BY_TEMPLATE",
 72049|       "confidence": "elementid_with_strong_name",
 72050|       "confidence_tier": "core",
 72051|       "target_resolution": "exact",
 72052|       "evidence": [
 72053|         "member name 'GetTemplate' matches keyword pattern /Template/",
 72054|         "docs text contains relationship phrase: 'template for'"
 72055|       ],
 72056|       "source_url": "https://www.revitapidocs.com/2025/0150a58d-4d48-6552-384f-d5faa08d08e0.htm",
 72057|       "dll_signature_verified": true,
 72058|       "dll_relationship_scope": "declared",
 72059|       "dll_semantic_verified": null,
 72060|       "dll_verified_status": "signature_verified_declared",
 72061|       "revitlookup_referenced": null,
 72062|       "revitlookup_requires_document_context": null
 72063|     },
 72064|     {
 72065|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleView",
 72066|       "target": "Autodesk.Revit.DB.Phase",
 72067|       "member_name": "IsCellInPhaseLoads",
 72068|       "member_kind": "method",
 72069|       "edge_type": "ASSIGNED_TO_PHASE",
 72070|       "confidence": "name_only_candidate",
 72071|       "confidence_tier": "likely",
 72072|       "target_resolution": "exact",
 72073|       "evidence": [
 72074|         "member name 'IsCellInPhaseLoads' matches keyword pattern /Phase/ but return type 'bool' gives no type-level confirmation"
 72075|       ],
 72076|       "source_url": "https://www.revitapidocs.com/2025/947fed15-730f-cb7f-bcef-43a112ec516c.htm",
 72077|       "dll_signature_verified": true,
 72078|       "dll_relationship_scope": "declared",
 72079|       "dll_semantic_verified": null,
 72080|       "dll_verified_status": "signature_verified_declared",
 72081|       "revitlookup_referenced": null,
 72082|       "revitlookup_requires_document_context": null
 72083|     },
 72084|     {
 72085|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleView",
 72086|       "target": "Autodesk.Revit.DB.View",
 72087|       "member_name": "IsPanelScheduleTemplate",
 72088|       "member_kind": "method",
 72089|       "edge_type": "CONTROLLED_BY_TEMPLATE",
 72090|       "confidence": "name_only_candidate",
 72091|       "confidence_tier": "likely",
 72092|       "target_resolution": "exact",
 72093|       "evidence": [
 72094|         "member name 'IsPanelScheduleTemplate' matches keyword pattern /Template/ but return type 'bool' gives no type-level confirmation"
 72095|       ],
 72096|       "source_url": "https://www.revitapidocs.com/2025/77fe7249-7ce9-7f81-0a49-58ae007e4ce8.htm",
 72097|       "dll_signature_verified": true,
 72098|       "dll_relationship_scope": "declared",
 72099|       "dll_semantic_verified": null,
 72100|       "dll_verified_status": "signature_verified_declared",
 72101|       "revitlookup_referenced": null,
 72102|       "revitlookup_requires_document_context": null
 72103|     },
 72104|     {
 72105|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleView",
 72106|       "target": null,
 72107|       "member_name": "IsSlotGrouped",
 72108|       "member_kind": "method",
 72109|       "edge_type": "MEMBER_OF_GROUP",
 72110|       "confidence": "name_only_candidate",
 72111|       "confidence_tier": "likely",
 72112|       "target_resolution": "none",
 72113|       "evidence": [
 72114|         "member name 'IsSlotGrouped' matches keyword pattern /^GetMember|Group/ but return type 'int' gives no type-level confirmation"
 72115|       ],
 72116|       "source_url": "https://www.revitapidocs.com/2025/f9d08d83-873d-8917-abef-497f0b1072d4.htm",
 72117|       "dll_signature_verified": true,
 72118|       "dll_relationship_scope": "declared",
 72119|       "dll_semantic_verified": null,
 72120|       "dll_verified_status": "signature_verified_declared",
 72121|       "revitlookup_referenced": null,
 72122|       "revitlookup_requires_document_context": null
 72123|     },
 72124|     {
 72125|       "source": "Autodesk.Revit.DB.Electrical.PanelScheduleView",
 72126|       "target": "Autodesk.Revit.DB.Phase",
 72127|       "member_name": "SwitchPhases",
 72128|       "member_kind": "method",
 72129|       "edge_type": "ASSIGNED_TO_PHASE",
 72130|       "confidence": "name_only_candidate",
 72131|       "confidence_tier": "likely",
 72132|       "target_resolution": "exact",
 72133|       "evidence": [
 72134|         "member name 'SwitchPhases' matches keyword pattern /Phase/ but return type 'void' gives no type-level confirmation"
 72135|       ],
 72136|       "source_url": "https://www.revitapidocs.com/2025/88042b4b-cbe1-95c6-babf-fee553edb451.htm",
 72137|       "dll_signature_verified": true,
 72138|       "dll_relationship_scope": "declared",
 72139|       "dll_semantic_verified": null,
 72140|       "dll_verified_status": "signature_verified_declared",
 72141|       "revitlookup_referenced": null,
 72142|       "revitlookup_requires_document_context": null
 72143|     },
 72144|     {
 72145|       "source": "Autodesk.Revit.DB.Electrical.TemperatureRatingType",
 72146|       "target": "Autodesk.Revit.DB.Electrical.CorrectionFactorSet",
 72147|       "member_name": "CorrectionFactors",
 72148|       "member_kind": "property",
 72149|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 72150|       "confidence": "direct_return_type",
 72151|       "confidence_tier": "unverified_reference",
 72152|       "target_resolution": "short_name_fallback",
 72153|       "evidence": [
 72154|         "return type 'CorrectionFactorSet' directly names a Revit DB object type"
 72155|       ],
 72156|       "source_url": "https://www.revitapidocs.com/2025/e0b0df88-e174-3f9d-c33a-5a7f510a4c3a.htm",
 72157|       "dll_signature_verified": true,
 72158|       "dll_relationship_scope": "declared",
 72159|       "dll_semantic_verified": null,
 72160|       "dll_verified_status": "signature_verified_declared",
```
