# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 154 of 216
- Original line range: 59671-60070
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 59671|       "target": null,
 59672|       "member_name": "ProjectLocationId",
 59673|       "member_kind": "property",
 59674|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 59675|       "confidence": "unknown_reference",
 59676|       "confidence_tier": "unverified_reference",
 59677|       "target_resolution": "none",
 59678|       "evidence": [
 59679|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 59680|       ],
 59681|       "source_url": "https://www.revitapidocs.com/2025/f57ae482-95dd-3398-6997-97199c5bad20.htm",
 59682|       "dll_signature_verified": true,
 59683|       "dll_relationship_scope": "declared",
 59684|       "dll_semantic_verified": null,
 59685|       "dll_verified_status": "signature_verified_declared",
 59686|       "revitlookup_referenced": null,
 59687|       "revitlookup_requires_document_context": null
 59688|     },
 59689|     {
 59690|       "source": "Autodesk.Revit.DB.SunAndShadowSettings",
 59691|       "target": "Autodesk.Revit.DB.Element",
 59692|       "member_name": "GetActiveSunAndShadowSettings",
 59693|       "member_kind": "method",
 59694|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 59695|       "confidence": "direct_return_type",
 59696|       "confidence_tier": "unverified_reference",
 59697|       "target_resolution": "exact",
 59698|       "evidence": [
 59699|         "return type 'Element' directly names a Revit DB object type"
 59700|       ],
 59701|       "source_url": "https://www.revitapidocs.com/2025/172fb455-c62c-7baf-6d11-a458a1c9b725.htm",
 59702|       "dll_signature_verified": true,
 59703|       "dll_relationship_scope": "declared",
 59704|       "dll_semantic_verified": null,
 59705|       "dll_verified_status": "signature_verified_declared",
 59706|       "revitlookup_referenced": true,
 59707|       "revitlookup_requires_document_context": true
 59708|     },
 59709|     {
 59710|       "source": "Autodesk.Revit.DB.SunAndShadowSettings",
 59711|       "target": "Autodesk.Revit.DB.Level",
 59712|       "member_name": "IsGroundPlaneLevelValid",
 59713|       "member_kind": "method",
 59714|       "edge_type": "ASSIGNED_TO_LEVEL",
 59715|       "confidence": "name_only_candidate",
 59716|       "confidence_tier": "likely",
 59717|       "target_resolution": "exact",
 59718|       "evidence": [
 59719|         "member name 'IsGroundPlaneLevelValid' matches keyword pattern /Level/ but return type 'bool' gives no type-level confirmation"
 59720|       ],
 59721|       "source_url": "https://www.revitapidocs.com/2025/55c05757-98af-05dd-24c3-9793d595af51.htm",
 59722|       "dll_signature_verified": true,
 59723|       "dll_relationship_scope": "declared",
 59724|       "dll_semantic_verified": null,
 59725|       "dll_verified_status": "signature_verified_declared",
 59726|       "revitlookup_referenced": null,
 59727|       "revitlookup_requires_document_context": null
 59728|     },
 59729|     {
 59730|       "source": "Autodesk.Revit.DB.Surface",
 59731|       "target": "Autodesk.Revit.DB.BoundingBoxUV",
 59732|       "member_name": "GetBoundingBoxUV",
 59733|       "member_kind": "method",
 59734|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 59735|       "confidence": "direct_return_type",
 59736|       "confidence_tier": "unverified_reference",
 59737|       "target_resolution": "exact",
 59738|       "evidence": [
 59739|         "return type 'BoundingBoxUV' directly names a Revit DB object type"
 59740|       ],
 59741|       "source_url": "https://www.revitapidocs.com/2025/5084214f-219f-780f-fe03-f16b62b2660d.htm",
 59742|       "dll_signature_verified": true,
 59743|       "dll_relationship_scope": "declared",
 59744|       "dll_semantic_verified": null,
 59745|       "dll_verified_status": "signature_verified_declared",
 59746|       "revitlookup_referenced": null,
 59747|       "revitlookup_requires_document_context": null
 59748|     },
 59749|     {
 59750|       "source": "Autodesk.Revit.DB.Sweep",
 59751|       "target": "Autodesk.Revit.DB.Path3d",
 59752|       "member_name": "Path3d",
 59753|       "member_kind": "property",
 59754|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 59755|       "confidence": "direct_return_type",
 59756|       "confidence_tier": "unverified_reference",
 59757|       "target_resolution": "exact",
 59758|       "evidence": [
 59759|         "return type 'Path3d' directly names a Revit DB object type"
 59760|       ],
 59761|       "source_url": "https://www.revitapidocs.com/2025/c61556c8-000d-028f-e18e-d2fbe71ff425.htm",
 59762|       "dll_signature_verified": true,
 59763|       "dll_relationship_scope": "declared",
 59764|       "dll_semantic_verified": null,
 59765|       "dll_verified_status": "signature_verified_declared",
 59766|       "revitlookup_referenced": null,
 59767|       "revitlookup_requires_document_context": null
 59768|     },
 59769|     {
 59770|       "source": "Autodesk.Revit.DB.Sweep",
 59771|       "target": "Autodesk.Revit.DB.Sketch",
 59772|       "member_name": "PathSketch",
 59773|       "member_kind": "property",
 59774|       "edge_type": "DEPENDS_ON",
 59775|       "confidence": "direct_return_type",
 59776|       "confidence_tier": "core",
 59777|       "target_resolution": "exact",
 59778|       "evidence": [
 59779|         "return type 'Sketch' directly names a Revit DB object type"
 59780|       ],
 59781|       "source_url": "https://www.revitapidocs.com/2025/ebb2a7ee-bf45-b71b-524e-0a7067817cd1.htm",
 59782|       "dll_signature_verified": true,
 59783|       "dll_relationship_scope": "declared",
 59784|       "dll_semantic_verified": null,
 59785|       "dll_verified_status": "signature_verified_declared",
 59786|       "revitlookup_referenced": null,
 59787|       "revitlookup_requires_document_context": null
 59788|     },
 59789|     {
 59790|       "source": "Autodesk.Revit.DB.Sweep",
 59791|       "target": "Autodesk.Revit.DB.Sketch",
 59792|       "member_name": "ProfileSketch",
 59793|       "member_kind": "property",
 59794|       "edge_type": "DEPENDS_ON",
 59795|       "confidence": "direct_return_type",
 59796|       "confidence_tier": "core",
 59797|       "target_resolution": "exact",
 59798|       "evidence": [
 59799|         "return type 'Sketch' directly names a Revit DB object type"
 59800|       ],
 59801|       "source_url": "https://www.revitapidocs.com/2025/84a0db17-c3d3-bf8b-e80b-e8d5b419ce1c.htm",
 59802|       "dll_signature_verified": true,
 59803|       "dll_relationship_scope": "declared",
 59804|       "dll_semantic_verified": null,
 59805|       "dll_verified_status": "signature_verified_declared",
 59806|       "revitlookup_referenced": null,
 59807|       "revitlookup_requires_document_context": null
 59808|     },
 59809|     {
 59810|       "source": "Autodesk.Revit.DB.Sweep",
 59811|       "target": "Autodesk.Revit.DB.FamilySymbolProfile",
 59812|       "member_name": "ProfileSymbol",
 59813|       "member_kind": "property",
 59814|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 59815|       "confidence": "direct_return_type",
 59816|       "confidence_tier": "unverified_reference",
 59817|       "target_resolution": "exact",
 59818|       "evidence": [
 59819|         "return type 'FamilySymbolProfile' directly names a Revit DB object type"
 59820|       ],
 59821|       "source_url": "https://www.revitapidocs.com/2025/35171c21-5219-c271-8e34-baa756344e19.htm",
 59822|       "dll_signature_verified": true,
 59823|       "dll_relationship_scope": "declared",
 59824|       "dll_semantic_verified": null,
 59825|       "dll_verified_status": "signature_verified_declared",
 59826|       "revitlookup_referenced": null,
 59827|       "revitlookup_requires_document_context": null
 59828|     },
 59829|     {
 59830|       "source": "Autodesk.Revit.DB.SweptBlend",
 59831|       "target": "Autodesk.Revit.DB.FamilySymbolProfile",
 59832|       "member_name": "BottomProfileSymbol",
 59833|       "member_kind": "property",
 59834|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 59835|       "confidence": "direct_return_type",
 59836|       "confidence_tier": "unverified_reference",
 59837|       "target_resolution": "exact",
 59838|       "evidence": [
 59839|         "return type 'FamilySymbolProfile' directly names a Revit DB object type"
 59840|       ],
 59841|       "source_url": "https://www.revitapidocs.com/2025/b6580b2d-f43e-67de-a3e6-f79151700c33.htm",
 59842|       "dll_signature_verified": true,
 59843|       "dll_relationship_scope": "declared",
 59844|       "dll_semantic_verified": null,
 59845|       "dll_verified_status": "signature_verified_declared",
 59846|       "revitlookup_referenced": null,
 59847|       "revitlookup_requires_document_context": null
 59848|     },
 59849|     {
 59850|       "source": "Autodesk.Revit.DB.SweptBlend",
 59851|       "target": "Autodesk.Revit.DB.Sketch",
 59852|       "member_name": "BottomSketch",
 59853|       "member_kind": "property",
 59854|       "edge_type": "DEPENDS_ON",
 59855|       "confidence": "direct_return_type",
 59856|       "confidence_tier": "core",
 59857|       "target_resolution": "exact",
 59858|       "evidence": [
 59859|         "return type 'Sketch' directly names a Revit DB object type"
 59860|       ],
 59861|       "source_url": "https://www.revitapidocs.com/2025/968170b5-f638-5149-0fb4-eb995ab8cfac.htm",
 59862|       "dll_signature_verified": true,
 59863|       "dll_relationship_scope": "declared",
 59864|       "dll_semantic_verified": null,
 59865|       "dll_verified_status": "signature_verified_declared",
 59866|       "revitlookup_referenced": null,
 59867|       "revitlookup_requires_document_context": null
 59868|     },
 59869|     {
 59870|       "source": "Autodesk.Revit.DB.SweptBlend",
 59871|       "target": "Autodesk.Revit.DB.Sketch",
 59872|       "member_name": "PathSketch",
 59873|       "member_kind": "property",
 59874|       "edge_type": "DEPENDS_ON",
 59875|       "confidence": "direct_return_type",
 59876|       "confidence_tier": "core",
 59877|       "target_resolution": "exact",
 59878|       "evidence": [
 59879|         "return type 'Sketch' directly names a Revit DB object type"
 59880|       ],
 59881|       "source_url": "https://www.revitapidocs.com/2025/413241cc-aef5-24cc-1348-9bc926ceb1e5.htm",
 59882|       "dll_signature_verified": true,
 59883|       "dll_relationship_scope": "declared",
 59884|       "dll_semantic_verified": null,
 59885|       "dll_verified_status": "signature_verified_declared",
 59886|       "revitlookup_referenced": null,
 59887|       "revitlookup_requires_document_context": null
 59888|     },
 59889|     {
 59890|       "source": "Autodesk.Revit.DB.SweptBlend",
 59891|       "target": "Autodesk.Revit.DB.FamilySymbolProfile",
 59892|       "member_name": "TopProfileSymbol",
 59893|       "member_kind": "property",
 59894|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 59895|       "confidence": "direct_return_type",
 59896|       "confidence_tier": "unverified_reference",
 59897|       "target_resolution": "exact",
 59898|       "evidence": [
 59899|         "return type 'FamilySymbolProfile' directly names a Revit DB object type"
 59900|       ],
 59901|       "source_url": "https://www.revitapidocs.com/2025/711dc6b4-f9ed-d97d-7a92-189e1918176e.htm",
 59902|       "dll_signature_verified": true,
 59903|       "dll_relationship_scope": "declared",
 59904|       "dll_semantic_verified": null,
 59905|       "dll_verified_status": "signature_verified_declared",
 59906|       "revitlookup_referenced": null,
 59907|       "revitlookup_requires_document_context": null
 59908|     },
 59909|     {
 59910|       "source": "Autodesk.Revit.DB.SweptBlend",
 59911|       "target": "Autodesk.Revit.DB.Sketch",
 59912|       "member_name": "TopSketch",
 59913|       "member_kind": "property",
 59914|       "edge_type": "DEPENDS_ON",
 59915|       "confidence": "direct_return_type",
 59916|       "confidence_tier": "core",
 59917|       "target_resolution": "exact",
 59918|       "evidence": [
 59919|         "return type 'Sketch' directly names a Revit DB object type"
 59920|       ],
 59921|       "source_url": "https://www.revitapidocs.com/2025/6faa9e0c-ce3b-3e05-1c1a-355aba8259e8.htm",
 59922|       "dll_signature_verified": true,
 59923|       "dll_relationship_scope": "declared",
 59924|       "dll_semantic_verified": null,
 59925|       "dll_verified_status": "signature_verified_declared",
 59926|       "revitlookup_referenced": null,
 59927|       "revitlookup_requires_document_context": null
 59928|     },
 59929|     {
 59930|       "source": "Autodesk.Revit.DB.SweptBlend",
 59931|       "target": "Autodesk.Revit.DB.VertexIndexPairArray",
 59932|       "member_name": "GetVertexConnectionMap",
 59933|       "member_kind": "method",
 59934|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 59935|       "confidence": "direct_return_type",
 59936|       "confidence_tier": "unverified_reference",
 59937|       "target_resolution": "exact",
 59938|       "evidence": [
 59939|         "return type 'VertexIndexPairArray' directly names a Revit DB object type"
 59940|       ],
 59941|       "source_url": "https://www.revitapidocs.com/2025/ec201a92-0f7e-5b90-f34d-d68757268cdf.htm",
 59942|       "dll_signature_verified": true,
 59943|       "dll_relationship_scope": "declared",
 59944|       "dll_semantic_verified": null,
 59945|       "dll_verified_status": "signature_verified_declared",
 59946|       "revitlookup_referenced": null,
 59947|       "revitlookup_requires_document_context": null
 59948|     },
 59949|     {
 59950|       "source": "Autodesk.Revit.DB.SweptProfile",
 59951|       "target": "Autodesk.Revit.DB.Profile",
 59952|       "member_name": "GetSweptProfile",
 59953|       "member_kind": "method",
 59954|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 59955|       "confidence": "direct_return_type",
 59956|       "confidence_tier": "unverified_reference",
 59957|       "target_resolution": "exact",
 59958|       "evidence": [
 59959|         "return type 'Profile' directly names a Revit DB object type"
 59960|       ],
 59961|       "source_url": "https://www.revitapidocs.com/2025/b09a4a81-a2b8-4d9b-9ac8-3b983ebb3115.htm",
 59962|       "dll_signature_verified": true,
 59963|       "dll_relationship_scope": "declared",
 59964|       "dll_semantic_verified": null,
 59965|       "dll_verified_status": "signature_verified_declared",
 59966|       "revitlookup_referenced": null,
 59967|       "revitlookup_requires_document_context": null
 59968|     },
 59969|     {
 59970|       "source": "Autodesk.Revit.DB.SymbolGeometryId",
 59971|       "target": null,
 59972|       "member_name": "SymbolId",
 59973|       "member_kind": "property",
 59974|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 59975|       "confidence": "unknown_reference",
 59976|       "confidence_tier": "unverified_reference",
 59977|       "target_resolution": "none",
 59978|       "evidence": [
 59979|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 59980|       ],
 59981|       "source_url": "https://www.revitapidocs.com/2025/39b90860-d958-7b9e-2975-90c41a36b9e1.htm",
 59982|       "dll_signature_verified": true,
 59983|       "dll_relationship_scope": "declared",
 59984|       "dll_semantic_verified": null,
 59985|       "dll_verified_status": "signature_verified_declared",
 59986|       "revitlookup_referenced": null,
 59987|       "revitlookup_requires_document_context": null
 59988|     },
 59989|     {
 59990|       "source": "Autodesk.Revit.DB.SymbolicCurve",
 59991|       "target": "Autodesk.Revit.DB.GraphicsStyle",
 59992|       "member_name": "Subcategory",
 59993|       "member_kind": "property",
 59994|       "edge_type": "REFERENCES",
 59995|       "confidence": "direct_return_type",
 59996|       "confidence_tier": "core",
 59997|       "target_resolution": "exact",
 59998|       "evidence": [
 59999|         "return type 'GraphicsStyle' directly names a Revit DB object type"
 60000|       ],
 60001|       "source_url": "https://www.revitapidocs.com/2025/dd07e20f-b6de-278e-3323-37c68306afd6.htm",
 60002|       "dll_signature_verified": true,
 60003|       "dll_relationship_scope": "declared",
 60004|       "dll_semantic_verified": null,
 60005|       "dll_verified_status": "signature_verified_declared",
 60006|       "revitlookup_referenced": null,
 60007|       "revitlookup_requires_document_context": null
 60008|     },
 60009|     {
 60010|       "source": "Autodesk.Revit.DB.SymbolicCurve",
 60011|       "target": "Autodesk.Revit.DB.FamilyElementVisibility",
 60012|       "member_name": "GetVisibility",
 60013|       "member_kind": "method",
 60014|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 60015|       "confidence": "direct_return_type",
 60016|       "confidence_tier": "unverified_reference",
 60017|       "target_resolution": "exact",
 60018|       "evidence": [
 60019|         "return type 'FamilyElementVisibility' directly names a Revit DB object type"
 60020|       ],
 60021|       "source_url": "https://www.revitapidocs.com/2025/161d8b5b-c1e4-6d56-15ee-697ef002609a.htm",
 60022|       "dll_signature_verified": true,
 60023|       "dll_relationship_scope": "declared",
 60024|       "dll_semantic_verified": null,
 60025|       "dll_verified_status": "signature_verified_declared",
 60026|       "revitlookup_referenced": null,
 60027|       "revitlookup_requires_document_context": null
 60028|     },
 60029|     {
 60030|       "source": "Autodesk.Revit.DB.SymbolicCurveArray",
 60031|       "target": "Autodesk.Revit.DB.SymbolicCurveArrayIterator",
 60032|       "member_name": "ForwardIterator",
 60033|       "member_kind": "method",
 60034|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 60035|       "confidence": "direct_return_type",
 60036|       "confidence_tier": "unverified_reference",
 60037|       "target_resolution": "exact",
 60038|       "evidence": [
 60039|         "return type 'SymbolicCurveArrayIterator' directly names a Revit DB object type"
 60040|       ],
 60041|       "source_url": "https://www.revitapidocs.com/2025/663d1a29-b38d-b3e6-e7f5-7b5e6a901713.htm",
 60042|       "dll_signature_verified": true,
 60043|       "dll_relationship_scope": "declared",
 60044|       "dll_semantic_verified": null,
 60045|       "dll_verified_status": "signature_verified_declared",
 60046|       "revitlookup_referenced": null,
 60047|       "revitlookup_requires_document_context": null
 60048|     },
 60049|     {
 60050|       "source": "Autodesk.Revit.DB.SymbolicCurveArray",
 60051|       "target": "Autodesk.Revit.DB.SymbolicCurveArrayIterator",
 60052|       "member_name": "ReverseIterator",
 60053|       "member_kind": "method",
 60054|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 60055|       "confidence": "direct_return_type",
 60056|       "confidence_tier": "unverified_reference",
 60057|       "target_resolution": "exact",
 60058|       "evidence": [
 60059|         "return type 'SymbolicCurveArrayIterator' directly names a Revit DB object type"
 60060|       ],
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
```
