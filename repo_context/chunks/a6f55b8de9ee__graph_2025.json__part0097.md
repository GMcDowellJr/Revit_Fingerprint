# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 97 of 216
- Original line range: 37441-37840
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 37441|       "evidence": [
 37442|         "return type 'Reference' directly names a Revit DB object type"
 37443|       ],
 37444|       "source_url": "https://www.revitapidocs.com/2025/612c51c5-c97d-19ce-2ced-209fd6e7a92a.htm",
 37445|       "dll_signature_verified": true,
 37446|       "dll_relationship_scope": "declared",
 37447|       "dll_semantic_verified": null,
 37448|       "dll_verified_status": "signature_verified_declared",
 37449|       "revitlookup_referenced": null,
 37450|       "revitlookup_requires_document_context": null
 37451|     },
 37452|     {
 37453|       "source": "Autodesk.Revit.DB.DirectShapeType",
 37454|       "target": "Autodesk.Revit.DB.DirectShapeTypeOptions",
 37455|       "member_name": "GetOptions",
 37456|       "member_kind": "method",
 37457|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37458|       "confidence": "direct_return_type",
 37459|       "confidence_tier": "unverified_reference",
 37460|       "target_resolution": "exact",
 37461|       "evidence": [
 37462|         "return type 'DirectShapeTypeOptions' directly names a Revit DB object type"
 37463|       ],
 37464|       "source_url": "https://www.revitapidocs.com/2025/9f0e48d9-9007-340b-51c6-5fefe3f5379b.htm",
 37465|       "dll_signature_verified": true,
 37466|       "dll_relationship_scope": "declared",
 37467|       "dll_semantic_verified": null,
 37468|       "dll_verified_status": "signature_verified_declared",
 37469|       "revitlookup_referenced": null,
 37470|       "revitlookup_requires_document_context": null
 37471|     },
 37472|     {
 37473|       "source": "Autodesk.Revit.DB.DirectShapeType",
 37474|       "target": null,
 37475|       "member_name": "HasExternallyTaggedReference",
 37476|       "member_kind": "method",
 37477|       "edge_type": "TAGS_ELEMENT",
 37478|       "confidence": "name_only_candidate",
 37479|       "confidence_tier": "likely",
 37480|       "target_resolution": "none",
 37481|       "evidence": [
 37482|         "member name 'HasExternallyTaggedReference' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'bool' gives no type-level confirmation"
 37483|       ],
 37484|       "source_url": "https://www.revitapidocs.com/2025/984c365c-9b92-bdc1-c7a3-423b795f073c.htm",
 37485|       "dll_signature_verified": true,
 37486|       "dll_relationship_scope": "declared",
 37487|       "dll_semantic_verified": null,
 37488|       "dll_verified_status": "signature_verified_declared",
 37489|       "revitlookup_referenced": null,
 37490|       "revitlookup_requires_document_context": null
 37491|     },
 37492|     {
 37493|       "source": "Autodesk.Revit.DB.DirectShapeType",
 37494|       "target": null,
 37495|       "member_name": "RemoveExternallyTaggedGeometry",
 37496|       "member_kind": "method",
 37497|       "edge_type": "TAGS_ELEMENT",
 37498|       "confidence": "name_only_candidate",
 37499|       "confidence_tier": "likely",
 37500|       "target_resolution": "none",
 37501|       "evidence": [
 37502|         "member name 'RemoveExternallyTaggedGeometry' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'void' gives no type-level confirmation"
 37503|       ],
 37504|       "source_url": "https://www.revitapidocs.com/2025/0fc3b749-bcc3-1472-092d-38475fe2c81d.htm",
 37505|       "dll_signature_verified": true,
 37506|       "dll_relationship_scope": "declared",
 37507|       "dll_semantic_verified": null,
 37508|       "dll_verified_status": "signature_verified_declared",
 37509|       "revitlookup_referenced": null,
 37510|       "revitlookup_requires_document_context": null
 37511|     },
 37512|     {
 37513|       "source": "Autodesk.Revit.DB.DirectShapeType",
 37514|       "target": null,
 37515|       "member_name": "ResetExternallyTaggedGeometry",
 37516|       "member_kind": "method",
 37517|       "edge_type": "TAGS_ELEMENT",
 37518|       "confidence": "name_only_candidate",
 37519|       "confidence_tier": "likely",
 37520|       "target_resolution": "none",
 37521|       "evidence": [
 37522|         "member name 'ResetExternallyTaggedGeometry' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'void' gives no type-level confirmation"
 37523|       ],
 37524|       "source_url": "https://www.revitapidocs.com/2025/7303e22c-72da-9667-fdaf-521534c444f8.htm",
 37525|       "dll_signature_verified": true,
 37526|       "dll_relationship_scope": "declared",
 37527|       "dll_semantic_verified": null,
 37528|       "dll_verified_status": "signature_verified_declared",
 37529|       "revitlookup_referenced": null,
 37530|       "revitlookup_requires_document_context": null
 37531|     },
 37532|     {
 37533|       "source": "Autodesk.Revit.DB.DirectShapeType",
 37534|       "target": null,
 37535|       "member_name": "UpdateExternallyTaggedGeometry",
 37536|       "member_kind": "method",
 37537|       "edge_type": "TAGS_ELEMENT",
 37538|       "confidence": "name_only_candidate",
 37539|       "confidence_tier": "likely",
 37540|       "target_resolution": "none",
 37541|       "evidence": [
 37542|         "member name 'UpdateExternallyTaggedGeometry' matches keyword pattern /^GetTagged|Tag(ged)?/ but return type 'void' gives no type-level confirmation"
 37543|       ],
 37544|       "source_url": "https://www.revitapidocs.com/2025/0acd0330-9e79-8be8-ff3c-740ed053ea82.htm",
 37545|       "dll_signature_verified": true,
 37546|       "dll_relationship_scope": "declared",
 37547|       "dll_semantic_verified": null,
 37548|       "dll_verified_status": "signature_verified_declared",
 37549|       "revitlookup_referenced": null,
 37550|       "revitlookup_requires_document_context": null
 37551|     },
 37552|     {
 37553|       "source": "Autodesk.Revit.DB.DisplacementElement",
 37554|       "target": null,
 37555|       "member_name": "ParentId",
 37556|       "member_kind": "property",
 37557|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 37558|       "confidence": "unknown_reference",
 37559|       "confidence_tier": "unverified_reference",
 37560|       "target_resolution": "none",
 37561|       "evidence": [
 37562|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 37563|       ],
 37564|       "source_url": "https://www.revitapidocs.com/2025/2faf0cca-a05a-45dc-d7e4-e61443a53623.htm",
 37565|       "dll_signature_verified": true,
 37566|       "dll_relationship_scope": "declared",
 37567|       "dll_semantic_verified": null,
 37568|       "dll_verified_status": "signature_verified_declared",
 37569|       "revitlookup_referenced": null,
 37570|       "revitlookup_requires_document_context": null
 37571|     },
 37572|     {
 37573|       "source": "Autodesk.Revit.DB.DisplacementElement",
 37574|       "target": "Autodesk.Revit.DB.Category",
 37575|       "member_name": "CanCategoryBeDisplaced",
 37576|       "member_kind": "method",
 37577|       "edge_type": "HAS_CATEGORY",
 37578|       "confidence": "name_only_candidate",
 37579|       "confidence_tier": "likely",
 37580|       "target_resolution": "exact",
 37581|       "evidence": [
 37582|         "member name 'CanCategoryBeDisplaced' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 37583|       ],
 37584|       "source_url": "https://www.revitapidocs.com/2025/cdab956b-de93-bf5a-1698-5d5746447934.htm",
 37585|       "dll_signature_verified": true,
 37586|       "dll_relationship_scope": "declared",
 37587|       "dll_semantic_verified": null,
 37588|       "dll_verified_status": "signature_verified_declared",
 37589|       "revitlookup_referenced": null,
 37590|       "revitlookup_requires_document_context": null
 37591|     },
 37592|     {
 37593|       "source": "Autodesk.Revit.DB.DisplacementElement",
 37594|       "target": null,
 37595|       "member_name": "GetAdditionalElementsToDisplace",
 37596|       "member_kind": "method",
 37597|       "edge_type": "RETURNS_ELEMENT_IDS",
 37598|       "confidence": "unknown_reference",
 37599|       "confidence_tier": "unverified_reference",
 37600|       "target_resolution": "none",
 37601|       "evidence": [
 37602|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 37603|       ],
 37604|       "source_url": "https://www.revitapidocs.com/2025/97d02f11-7308-579c-031a-5102dc40ae4f.htm",
 37605|       "dll_signature_verified": true,
 37606|       "dll_relationship_scope": "declared",
 37607|       "dll_semantic_verified": null,
 37608|       "dll_verified_status": "signature_verified_declared",
 37609|       "revitlookup_referenced": null,
 37610|       "revitlookup_requires_document_context": null
 37611|     },
 37612|     {
 37613|       "source": "Autodesk.Revit.DB.DisplacementElement",
 37614|       "target": "Autodesk.Revit.DB.DisplacementElement",
 37615|       "member_name": "GetChildren",
 37616|       "member_kind": "method",
 37617|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37618|       "confidence": "needs_runtime_validation",
 37619|       "confidence_tier": "needs_validation",
 37620|       "target_resolution": "exact",
 37621|       "evidence": [
 37622|         "return type 'IList < DisplacementElement >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 37623|       ],
 37624|       "source_url": "https://www.revitapidocs.com/2025/d7e84ab1-d970-b926-eea8-00032962431a.htm",
 37625|       "dll_signature_verified": true,
 37626|       "dll_relationship_scope": "declared",
 37627|       "dll_semantic_verified": null,
 37628|       "dll_verified_status": "signature_verified_declared",
 37629|       "revitlookup_referenced": null,
 37630|       "revitlookup_requires_document_context": null
 37631|     },
 37632|     {
 37633|       "source": "Autodesk.Revit.DB.DisplacementElement",
 37634|       "target": null,
 37635|       "member_name": "GetDisplacedElementIds",
 37636|       "member_kind": "method",
 37637|       "edge_type": "RETURNS_ELEMENT_IDS",
 37638|       "confidence": "unknown_reference",
 37639|       "confidence_tier": "unverified_reference",
 37640|       "target_resolution": "none",
 37641|       "evidence": [
 37642|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 37643|       ],
 37644|       "source_url": "https://www.revitapidocs.com/2025/653f228c-8b72-aa94-4ab0-131d1f1b322f.htm",
 37645|       "dll_signature_verified": true,
 37646|       "dll_relationship_scope": "declared",
 37647|       "dll_semantic_verified": null,
 37648|       "dll_verified_status": "signature_verified_declared",
 37649|       "revitlookup_referenced": null,
 37650|       "revitlookup_requires_document_context": null
 37651|     },
 37652|     {
 37653|       "source": "Autodesk.Revit.DB.DisplacementElement",
 37654|       "target": null,
 37655|       "member_name": "GetDisplacedElementIds",
 37656|       "member_kind": "method",
 37657|       "edge_type": "RETURNS_ELEMENT_IDS",
 37658|       "confidence": "unknown_reference",
 37659|       "confidence_tier": "unverified_reference",
 37660|       "target_resolution": "none",
 37661|       "evidence": [
 37662|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 37663|       ],
 37664|       "source_url": "https://www.revitapidocs.com/2025/e77c4376-b546-9939-53d8-af17afa16bd9.htm",
 37665|       "dll_signature_verified": true,
 37666|       "dll_relationship_scope": "declared",
 37667|       "dll_semantic_verified": null,
 37668|       "dll_verified_status": "signature_verified_declared",
 37669|       "revitlookup_referenced": null,
 37670|       "revitlookup_requires_document_context": null
 37671|     },
 37672|     {
 37673|       "source": "Autodesk.Revit.DB.DisplacementElement",
 37674|       "target": null,
 37675|       "member_name": "GetDisplacedElementIdsFromAllChildren",
 37676|       "member_kind": "method",
 37677|       "edge_type": "RETURNS_ELEMENT_IDS",
 37678|       "confidence": "unknown_reference",
 37679|       "confidence_tier": "unverified_reference",
 37680|       "target_resolution": "none",
 37681|       "evidence": [
 37682|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 37683|       ],
 37684|       "source_url": "https://www.revitapidocs.com/2025/63740fb6-7911-9756-e4e1-c1377083216b.htm",
 37685|       "dll_signature_verified": true,
 37686|       "dll_relationship_scope": "declared",
 37687|       "dll_semantic_verified": null,
 37688|       "dll_verified_status": "signature_verified_declared",
 37689|       "revitlookup_referenced": null,
 37690|       "revitlookup_requires_document_context": null
 37691|     },
 37692|     {
 37693|       "source": "Autodesk.Revit.DB.DisplacementElement",
 37694|       "target": null,
 37695|       "member_name": "GetDisplacementElementId",
 37696|       "member_kind": "method",
 37697|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 37698|       "confidence": "unknown_reference",
 37699|       "confidence_tier": "unverified_reference",
 37700|       "target_resolution": "none",
 37701|       "evidence": [
 37702|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 37703|       ],
 37704|       "source_url": "https://www.revitapidocs.com/2025/c4790d71-c7a4-b298-8428-035de9981392.htm",
 37705|       "dll_signature_verified": true,
 37706|       "dll_relationship_scope": "declared",
 37707|       "dll_semantic_verified": null,
 37708|       "dll_verified_status": "signature_verified_declared",
 37709|       "revitlookup_referenced": null,
 37710|       "revitlookup_requires_document_context": null
 37711|     },
 37712|     {
 37713|       "source": "Autodesk.Revit.DB.DisplacementElement",
 37714|       "target": null,
 37715|       "member_name": "GetDisplacementElementIds",
 37716|       "member_kind": "method",
 37717|       "edge_type": "RETURNS_ELEMENT_IDS",
 37718|       "confidence": "unknown_reference",
 37719|       "confidence_tier": "unverified_reference",
 37720|       "target_resolution": "none",
 37721|       "evidence": [
 37722|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 37723|       ],
 37724|       "source_url": "https://www.revitapidocs.com/2025/a8f8e350-c1c1-c3f7-ff73-660388b40098.htm",
 37725|       "dll_signature_verified": true,
 37726|       "dll_relationship_scope": "declared",
 37727|       "dll_semantic_verified": null,
 37728|       "dll_verified_status": "signature_verified_declared",
 37729|       "revitlookup_referenced": null,
 37730|       "revitlookup_requires_document_context": null
 37731|     },
 37732|     {
 37733|       "source": "Autodesk.Revit.DB.DividedPath",
 37734|       "target": null,
 37735|       "member_name": "GetIntersectingElements",
 37736|       "member_kind": "method",
 37737|       "edge_type": "RETURNS_ELEMENT_IDS",
 37738|       "confidence": "unknown_reference",
 37739|       "confidence_tier": "unverified_reference",
 37740|       "target_resolution": "none",
 37741|       "evidence": [
 37742|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 37743|       ],
 37744|       "source_url": "https://www.revitapidocs.com/2025/fbdf6393-21be-362e-4033-56b62b2aff2c.htm",
 37745|       "dll_signature_verified": true,
 37746|       "dll_relationship_scope": "declared",
 37747|       "dll_semantic_verified": null,
 37748|       "dll_verified_status": "signature_verified_declared",
 37749|       "revitlookup_referenced": null,
 37750|       "revitlookup_requires_document_context": null
 37751|     },
 37752|     {
 37753|       "source": "Autodesk.Revit.DB.DividedSurface",
 37754|       "target": "Autodesk.Revit.DB.Element",
 37755|       "member_name": "Host",
 37756|       "member_kind": "property",
 37757|       "edge_type": "HOSTED_BY",
 37758|       "confidence": "direct_return_type",
 37759|       "confidence_tier": "core",
 37760|       "target_resolution": "exact",
 37761|       "evidence": [
 37762|         "return type 'Element' directly names a Revit DB object type"
 37763|       ],
 37764|       "source_url": "https://www.revitapidocs.com/2025/cf5cae80-7977-ad86-0381-3a56ee3c2b6c.htm",
 37765|       "dll_signature_verified": true,
 37766|       "dll_relationship_scope": "declared",
 37767|       "dll_semantic_verified": null,
 37768|       "dll_verified_status": "signature_verified_declared",
 37769|       "revitlookup_referenced": null,
 37770|       "revitlookup_requires_document_context": null
 37771|     },
 37772|     {
 37773|       "source": "Autodesk.Revit.DB.DividedSurface",
 37774|       "target": "Autodesk.Revit.DB.Reference",
 37775|       "member_name": "HostReference",
 37776|       "member_kind": "property",
 37777|       "edge_type": "HOSTED_BY",
 37778|       "confidence": "direct_return_type",
 37779|       "confidence_tier": "core",
 37780|       "target_resolution": "exact",
 37781|       "evidence": [
 37782|         "return type 'Reference' directly names a Revit DB object type"
 37783|       ],
 37784|       "source_url": "https://www.revitapidocs.com/2025/c90a0505-ae70-eac4-94a4-d72289a90e62.htm",
 37785|       "dll_signature_verified": true,
 37786|       "dll_relationship_scope": "declared",
 37787|       "dll_semantic_verified": null,
 37788|       "dll_verified_status": "signature_verified_declared",
 37789|       "revitlookup_referenced": null,
 37790|       "revitlookup_requires_document_context": null
 37791|     },
 37792|     {
 37793|       "source": "Autodesk.Revit.DB.DividedSurface",
 37794|       "target": "Autodesk.Revit.DB.SpacingRule",
 37795|       "member_name": "USpacingRule",
 37796|       "member_kind": "property",
 37797|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37798|       "confidence": "direct_return_type",
 37799|       "confidence_tier": "unverified_reference",
 37800|       "target_resolution": "exact",
 37801|       "evidence": [
 37802|         "return type 'SpacingRule' directly names a Revit DB object type"
 37803|       ],
 37804|       "source_url": "https://www.revitapidocs.com/2025/bcb80bc9-33d9-2718-3080-500a9d8119de.htm",
 37805|       "dll_signature_verified": true,
 37806|       "dll_relationship_scope": "declared",
 37807|       "dll_semantic_verified": null,
 37808|       "dll_verified_status": "signature_verified_declared",
 37809|       "revitlookup_referenced": null,
 37810|       "revitlookup_requires_document_context": null
 37811|     },
 37812|     {
 37813|       "source": "Autodesk.Revit.DB.DividedSurface",
 37814|       "target": "Autodesk.Revit.DB.SpacingRule",
 37815|       "member_name": "VSpacingRule",
 37816|       "member_kind": "property",
 37817|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 37818|       "confidence": "direct_return_type",
 37819|       "confidence_tier": "unverified_reference",
 37820|       "target_resolution": "exact",
 37821|       "evidence": [
 37822|         "return type 'SpacingRule' directly names a Revit DB object type"
 37823|       ],
 37824|       "source_url": "https://www.revitapidocs.com/2025/b5dabf3b-96ff-26e5-0d91-314391ceddb2.htm",
 37825|       "dll_signature_verified": true,
 37826|       "dll_relationship_scope": "declared",
 37827|       "dll_semantic_verified": null,
 37828|       "dll_verified_status": "signature_verified_declared",
 37829|       "revitlookup_referenced": null,
 37830|       "revitlookup_requires_document_context": null
 37831|     },
 37832|     {
 37833|       "source": "Autodesk.Revit.DB.DividedSurface",
 37834|       "target": null,
 37835|       "member_name": "GetAllIntersectionElements",
 37836|       "member_kind": "method",
 37837|       "edge_type": "RETURNS_ELEMENT_IDS",
 37838|       "confidence": "elementid_collection_with_strong_name",
 37839|       "confidence_tier": "core",
 37840|       "target_resolution": "none",
```
