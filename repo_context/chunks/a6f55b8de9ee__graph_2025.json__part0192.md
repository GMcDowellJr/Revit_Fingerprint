# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 192 of 216
- Original line range: 74491-74890
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 74491|       "member_kind": "method",
 74492|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74493|       "confidence": "direct_return_type",
 74494|       "confidence_tier": "unverified_reference",
 74495|       "target_resolution": "short_name_fallback",
 74496|       "evidence": [
 74497|         "return type 'RoofComponents' directly names a Revit DB object type"
 74498|       ],
 74499|       "source_url": "https://www.revitapidocs.com/2025/5f69588b-fe06-ca39-cf72-145e580d839b.htm",
 74500|       "dll_signature_verified": true,
 74501|       "dll_relationship_scope": "declared",
 74502|       "dll_semantic_verified": null,
 74503|       "dll_verified_status": "signature_verified_declared",
 74504|       "revitlookup_referenced": null,
 74505|       "revitlookup_requires_document_context": null
 74506|     },
 74507|     {
 74508|       "source": "Autodesk.Revit.DB.IFC.ExporterIFCUtils",
 74509|       "target": "Autodesk.Revit.DB.Sketch",
 74510|       "member_name": "IsCurveFromOtherElementSketch",
 74511|       "member_kind": "method",
 74512|       "edge_type": "DEPENDS_ON",
 74513|       "confidence": "name_only_candidate",
 74514|       "confidence_tier": "likely",
 74515|       "target_resolution": "exact",
 74516|       "evidence": [
 74517|         "member name 'IsCurveFromOtherElementSketch' matches keyword pattern /Sketch(Id)?$/ but return type 'bool' gives no type-level confirmation"
 74518|       ],
 74519|       "source_url": "https://www.revitapidocs.com/2025/62b1086e-f5ee-fca2-8aac-4cf0c54a6a35.htm",
 74520|       "dll_signature_verified": true,
 74521|       "dll_relationship_scope": "declared",
 74522|       "dll_semantic_verified": null,
 74523|       "dll_verified_status": "signature_verified_declared",
 74524|       "revitlookup_referenced": null,
 74525|       "revitlookup_requires_document_context": null
 74526|     },
 74527|     {
 74528|       "source": "Autodesk.Revit.DB.IFC.IFCAggregate",
 74529|       "target": "Autodesk.Revit.DB.IFC.IFCAggregateIterator",
 74530|       "member_name": "GetIFCAggregateIterator",
 74531|       "member_kind": "method",
 74532|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74533|       "confidence": "direct_return_type",
 74534|       "confidence_tier": "unverified_reference",
 74535|       "target_resolution": "short_name_fallback",
 74536|       "evidence": [
 74537|         "return type 'IFCAggregateIterator' directly names a Revit DB object type"
 74538|       ],
 74539|       "source_url": "https://www.revitapidocs.com/2025/fa84ad86-7874-4893-206f-41bf627ba3b3.htm",
 74540|       "dll_signature_verified": true,
 74541|       "dll_relationship_scope": "declared",
 74542|       "dll_semantic_verified": null,
 74543|       "dll_verified_status": "signature_verified_declared",
 74544|       "revitlookup_referenced": null,
 74545|       "revitlookup_requires_document_context": null
 74546|     },
 74547|     {
 74548|       "source": "Autodesk.Revit.DB.IFC.IFCConnectedWallData",
 74549|       "target": null,
 74550|       "member_name": "ElementId",
 74551|       "member_kind": "property",
 74552|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 74553|       "confidence": "unknown_reference",
 74554|       "confidence_tier": "unverified_reference",
 74555|       "target_resolution": "none",
 74556|       "evidence": [
 74557|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 74558|       ],
 74559|       "source_url": "https://www.revitapidocs.com/2025/d935a90c-e2c0-cd2b-8e55-1044676bb570.htm",
 74560|       "dll_signature_verified": true,
 74561|       "dll_relationship_scope": "declared",
 74562|       "dll_semantic_verified": null,
 74563|       "dll_verified_status": "signature_verified_declared",
 74564|       "revitlookup_referenced": null,
 74565|       "revitlookup_requires_document_context": null
 74566|     },
 74567|     {
 74568|       "source": "Autodesk.Revit.DB.IFC.IFCConnectedWallData",
 74569|       "target": "Autodesk.Revit.DB.Location",
 74570|       "member_name": "Location",
 74571|       "member_kind": "property",
 74572|       "edge_type": "REFERENCES",
 74573|       "confidence": "name_only_candidate",
 74574|       "confidence_tier": "likely",
 74575|       "target_resolution": "exact",
 74576|       "evidence": [
 74577|         "member name 'Location' matches keyword pattern /^Location$/ but return type 'IFCConnectedWallDataLocation' gives no type-level confirmation"
 74578|       ],
 74579|       "source_url": "https://www.revitapidocs.com/2025/4d6beedd-0637-9f02-61cd-fa71b7656e35.htm",
 74580|       "dll_signature_verified": true,
 74581|       "dll_relationship_scope": "declared",
 74582|       "dll_semantic_verified": null,
 74583|       "dll_verified_status": "signature_verified_declared",
 74584|       "revitlookup_referenced": null,
 74585|       "revitlookup_requires_document_context": null
 74586|     },
 74587|     {
 74588|       "source": "Autodesk.Revit.DB.IFC.IFCData",
 74589|       "target": "Autodesk.Revit.DB.IFC.IFCAggregate",
 74590|       "member_name": "AsAggregate",
 74591|       "member_kind": "method",
 74592|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74593|       "confidence": "direct_return_type",
 74594|       "confidence_tier": "unverified_reference",
 74595|       "target_resolution": "short_name_fallback",
 74596|       "evidence": [
 74597|         "return type 'IFCAggregate' directly names a Revit DB object type"
 74598|       ],
 74599|       "source_url": "https://www.revitapidocs.com/2025/abd9c207-2707-9dce-72c0-dda56cb0e96a.htm",
 74600|       "dll_signature_verified": true,
 74601|       "dll_relationship_scope": "declared",
 74602|       "dll_semantic_verified": null,
 74603|       "dll_verified_status": "signature_verified_declared",
 74604|       "revitlookup_referenced": null,
 74605|       "revitlookup_requires_document_context": null
 74606|     },
 74607|     {
 74608|       "source": "Autodesk.Revit.DB.IFC.IFCExtrusionCalculatorUtils",
 74609|       "target": "Autodesk.Revit.DB.IFC.IFCExtrusionData",
 74610|       "member_name": "CalculateExtrusionData",
 74611|       "member_kind": "method",
 74612|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74613|       "confidence": "needs_runtime_validation",
 74614|       "confidence_tier": "needs_validation",
 74615|       "target_resolution": "short_name_fallback",
 74616|       "evidence": [
 74617|         "return type 'IList < IFCExtrusionData >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 74618|       ],
 74619|       "source_url": "https://www.revitapidocs.com/2025/2586cb51-7d19-2f3a-d4e6-cc9cfc913a3c.htm",
 74620|       "dll_signature_verified": true,
 74621|       "dll_relationship_scope": "declared",
 74622|       "dll_semantic_verified": null,
 74623|       "dll_verified_status": "signature_verified_declared",
 74624|       "revitlookup_referenced": null,
 74625|       "revitlookup_requires_document_context": null
 74626|     },
 74627|     {
 74628|       "source": "Autodesk.Revit.DB.IFC.IFCExtrusionCalculatorUtils",
 74629|       "target": "Autodesk.Revit.DB.IFC.IFCExtrusionData",
 74630|       "member_name": "CalculateExtrusionData",
 74631|       "member_kind": "method",
 74632|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74633|       "confidence": "needs_runtime_validation",
 74634|       "confidence_tier": "needs_validation",
 74635|       "target_resolution": "short_name_fallback",
 74636|       "evidence": [
 74637|         "return type 'IList < IFCExtrusionData >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 74638|       ],
 74639|       "source_url": "https://www.revitapidocs.com/2025/09db98bd-c3ac-7977-7ad5-3c1178f1d633.htm",
 74640|       "dll_signature_verified": true,
 74641|       "dll_relationship_scope": "declared",
 74642|       "dll_semantic_verified": null,
 74643|       "dll_verified_status": "signature_verified_declared",
 74644|       "revitlookup_referenced": null,
 74645|       "revitlookup_requires_document_context": null
 74646|     },
 74647|     {
 74648|       "source": "Autodesk.Revit.DB.IFC.IFCExtrusionCreationData",
 74649|       "target": "Autodesk.Revit.DB.IFC.IFCExtrusionData",
 74650|       "member_name": "GetOpenings",
 74651|       "member_kind": "method",
 74652|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74653|       "confidence": "needs_runtime_validation",
 74654|       "confidence_tier": "needs_validation",
 74655|       "target_resolution": "short_name_fallback",
 74656|       "evidence": [
 74657|         "return type 'IList < IFCExtrusionData >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 74658|       ],
 74659|       "source_url": "https://www.revitapidocs.com/2025/a338038d-e7d2-d89e-bad5-5249dbf63baa.htm",
 74660|       "dll_signature_verified": true,
 74661|       "dll_relationship_scope": "declared",
 74662|       "dll_semantic_verified": null,
 74663|       "dll_verified_status": "signature_verified_declared",
 74664|       "revitlookup_referenced": null,
 74665|       "revitlookup_requires_document_context": null
 74666|     },
 74667|     {
 74668|       "source": "Autodesk.Revit.DB.IFC.IFCFamilyInstanceExtrusionExportResults",
 74669|       "target": "Autodesk.Revit.DB.Material",
 74670|       "member_name": "MaterialId",
 74671|       "member_kind": "property",
 74672|       "edge_type": "USES_MATERIAL",
 74673|       "confidence": "elementid_with_strong_name",
 74674|       "confidence_tier": "core",
 74675|       "target_resolution": "exact",
 74676|       "evidence": [
 74677|         "member name 'MaterialId' matches keyword pattern /Material/"
 74678|       ],
 74679|       "source_url": "https://www.revitapidocs.com/2025/bec28291-cbc9-69ea-1196-5ae2f5418886.htm",
 74680|       "dll_signature_verified": true,
 74681|       "dll_relationship_scope": "declared",
 74682|       "dll_semantic_verified": null,
 74683|       "dll_verified_status": "signature_verified_declared",
 74684|       "revitlookup_referenced": null,
 74685|       "revitlookup_requires_document_context": null
 74686|     },
 74687|     {
 74688|       "source": "Autodesk.Revit.DB.IFC.IFCFamilyInstanceExtrusionExportResults",
 74689|       "target": "Autodesk.Revit.DB.IFC.IFCExtrusionData",
 74690|       "member_name": "GetCutPairOpenings",
 74691|       "member_kind": "method",
 74692|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74693|       "confidence": "needs_runtime_validation",
 74694|       "confidence_tier": "needs_validation",
 74695|       "target_resolution": "short_name_fallback",
 74696|       "evidence": [
 74697|         "return type 'IList < IFCExtrusionData >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 74698|       ],
 74699|       "source_url": "https://www.revitapidocs.com/2025/d4f0caa2-a21a-18e1-325c-67b026e9790b.htm",
 74700|       "dll_signature_verified": true,
 74701|       "dll_relationship_scope": "declared",
 74702|       "dll_semantic_verified": null,
 74703|       "dll_verified_status": "signature_verified_declared",
 74704|       "revitlookup_referenced": null,
 74705|       "revitlookup_requires_document_context": null
 74706|     },
 74707|     {
 74708|       "source": "Autodesk.Revit.DB.IFC.IFCHybridImport",
 74709|       "target": null,
 74710|       "member_name": "ImportElements",
 74711|       "member_kind": "method",
 74712|       "edge_type": "RETURNS_ELEMENT_IDS",
 74713|       "confidence": "unknown_reference",
 74714|       "confidence_tier": "unverified_reference",
 74715|       "target_resolution": "none",
 74716|       "evidence": [
 74717|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 74718|       ],
 74719|       "source_url": "https://www.revitapidocs.com/2025/7d0a704f-1f57-ffe4-8853-a2326dfaf88d.htm",
 74720|       "dll_signature_verified": true,
 74721|       "dll_relationship_scope": "declared",
 74722|       "dll_semantic_verified": null,
 74723|       "dll_verified_status": "signature_verified_declared",
 74724|       "revitlookup_referenced": null,
 74725|       "revitlookup_requires_document_context": null
 74726|     },
 74727|     {
 74728|       "source": "Autodesk.Revit.DB.IFC.IFCHybridImport",
 74729|       "target": null,
 74730|       "member_name": "UpdateElements",
 74731|       "member_kind": "method",
 74732|       "edge_type": "RETURNS_ELEMENT_IDS",
 74733|       "confidence": "unknown_reference",
 74734|       "confidence_tier": "unverified_reference",
 74735|       "target_resolution": "none",
 74736|       "evidence": [
 74737|         "return type 'IList < ElementId >' is a collection of ID wrappers with no strong name hint"
 74738|       ],
 74739|       "source_url": "https://www.revitapidocs.com/2025/c92a4f1c-c644-bcd3-97fc-338fac6a2464.htm",
 74740|       "dll_signature_verified": true,
 74741|       "dll_relationship_scope": "declared",
 74742|       "dll_semantic_verified": null,
 74743|       "dll_verified_status": "signature_verified_declared",
 74744|       "revitlookup_referenced": null,
 74745|       "revitlookup_requires_document_context": null
 74746|     },
 74747|     {
 74748|       "source": "Autodesk.Revit.DB.IFC.IFCImportOptions",
 74749|       "target": "Autodesk.Revit.DB.LinkConversionData",
 74750|       "member_name": "GetConversionData",
 74751|       "member_kind": "method",
 74752|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74753|       "confidence": "direct_return_type",
 74754|       "confidence_tier": "unverified_reference",
 74755|       "target_resolution": "exact",
 74756|       "evidence": [
 74757|         "return type 'LinkConversionData' directly names a Revit DB object type"
 74758|       ],
 74759|       "source_url": "https://www.revitapidocs.com/2025/e4cd397e-8e29-ca75-4a92-ab8efd557ea1.htm",
 74760|       "dll_signature_verified": true,
 74761|       "dll_relationship_scope": "declared",
 74762|       "dll_semantic_verified": null,
 74763|       "dll_verified_status": "signature_verified_declared",
 74764|       "revitlookup_referenced": null,
 74765|       "revitlookup_requires_document_context": null
 74766|     },
 74767|     {
 74768|       "source": "Autodesk.Revit.DB.IFC.IFCLevelInfo",
 74769|       "target": "Autodesk.Revit.DB.Level",
 74770|       "member_name": "DistanceToNextLevel",
 74771|       "member_kind": "property",
 74772|       "edge_type": "ASSIGNED_TO_LEVEL",
 74773|       "confidence": "name_only_candidate",
 74774|       "confidence_tier": "likely",
 74775|       "target_resolution": "exact",
 74776|       "evidence": [
 74777|         "member name 'DistanceToNextLevel' matches keyword pattern /Level/ but return type 'double' gives no type-level confirmation"
 74778|       ],
 74779|       "source_url": "https://www.revitapidocs.com/2025/fa514233-5851-b663-852b-06a994323d98.htm",
 74780|       "dll_signature_verified": true,
 74781|       "dll_relationship_scope": "declared",
 74782|       "dll_semantic_verified": null,
 74783|       "dll_verified_status": "signature_verified_declared",
 74784|       "revitlookup_referenced": null,
 74785|       "revitlookup_requires_document_context": null
 74786|     },
 74787|     {
 74788|       "source": "Autodesk.Revit.DB.IFC.IFCOpeningData",
 74789|       "target": null,
 74790|       "member_name": "OpeningElementId",
 74791|       "member_kind": "property",
 74792|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 74793|       "confidence": "unknown_reference",
 74794|       "confidence_tier": "unverified_reference",
 74795|       "target_resolution": "none",
 74796|       "evidence": [
 74797|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 74798|       ],
 74799|       "source_url": "https://www.revitapidocs.com/2025/8990c5d6-2007-5d60-95ff-3f59fac806d5.htm",
 74800|       "dll_signature_verified": true,
 74801|       "dll_relationship_scope": "declared",
 74802|       "dll_semantic_verified": null,
 74803|       "dll_verified_status": "signature_verified_declared",
 74804|       "revitlookup_referenced": null,
 74805|       "revitlookup_requires_document_context": null
 74806|     },
 74807|     {
 74808|       "source": "Autodesk.Revit.DB.IFC.IFCOpeningData",
 74809|       "target": "Autodesk.Revit.DB.IFC.IFCExtrusionData",
 74810|       "member_name": "GetExtrusionData",
 74811|       "member_kind": "method",
 74812|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 74813|       "confidence": "needs_runtime_validation",
 74814|       "confidence_tier": "needs_validation",
 74815|       "target_resolution": "short_name_fallback",
 74816|       "evidence": [
 74817|         "return type 'IList < IFCExtrusionData >' is a generic collection whose element type cannot be statically confirmed as reference-bearing"
 74818|       ],
 74819|       "source_url": "https://www.revitapidocs.com/2025/0fa821c5-37df-2e15-833f-09174983d2f0.htm",
 74820|       "dll_signature_verified": true,
 74821|       "dll_relationship_scope": "declared",
 74822|       "dll_semantic_verified": null,
 74823|       "dll_verified_status": "signature_verified_declared",
 74824|       "revitlookup_referenced": null,
 74825|       "revitlookup_requires_document_context": null
 74826|     },
 74827|     {
 74828|       "source": "Autodesk.Revit.DB.IFC.IFCProductWrapper",
 74829|       "target": "Autodesk.Revit.DB.Material",
 74830|       "member_name": "AddFinishMaterial",
 74831|       "member_kind": "method",
 74832|       "edge_type": "USES_MATERIAL",
 74833|       "confidence": "name_only_candidate",
 74834|       "confidence_tier": "likely",
 74835|       "target_resolution": "exact",
 74836|       "evidence": [
 74837|         "member name 'AddFinishMaterial' matches keyword pattern /Material/ but return type 'void' gives no type-level confirmation"
 74838|       ],
 74839|       "source_url": "https://www.revitapidocs.com/2025/68339fc8-168c-1780-d477-155a7204a37c.htm",
 74840|       "dll_signature_verified": true,
 74841|       "dll_relationship_scope": "declared",
 74842|       "dll_semantic_verified": null,
 74843|       "dll_verified_status": "signature_verified_declared",
 74844|       "revitlookup_referenced": null,
 74845|       "revitlookup_requires_document_context": null
 74846|     },
 74847|     {
 74848|       "source": "Autodesk.Revit.DB.IFC.IFCProductWrapper",
 74849|       "target": "Autodesk.Revit.DB.Material",
 74850|       "member_name": "ClearFinishMaterials",
 74851|       "member_kind": "method",
 74852|       "edge_type": "USES_MATERIAL",
 74853|       "confidence": "name_only_candidate",
 74854|       "confidence_tier": "likely",
 74855|       "target_resolution": "exact",
 74856|       "evidence": [
 74857|         "member name 'ClearFinishMaterials' matches keyword pattern /Material/ but return type 'void' gives no type-level confirmation"
 74858|       ],
 74859|       "source_url": "https://www.revitapidocs.com/2025/ead3e7cf-76e8-eb9e-ab68-2105b54b2726.htm",
 74860|       "dll_signature_verified": true,
 74861|       "dll_relationship_scope": "declared",
 74862|       "dll_semantic_verified": null,
 74863|       "dll_verified_status": "signature_verified_declared",
 74864|       "revitlookup_referenced": null,
 74865|       "revitlookup_requires_document_context": null
 74866|     },
 74867|     {
 74868|       "source": "Autodesk.Revit.DB.IFC.IFCProductWrapper",
 74869|       "target": "Autodesk.Revit.DB.IFC.IFCExtrusionCreationData",
 74870|       "member_name": "FindExtrusionCreationParameters",
 74871|       "member_kind": "method",
 74872|       "edge_type": "HAS_PARAMETER",
 74873|       "confidence": "direct_return_type",
 74874|       "confidence_tier": "core",
 74875|       "target_resolution": "short_name_fallback",
 74876|       "evidence": [
 74877|         "return type 'IFCExtrusionCreationData' directly names a Revit DB object type"
 74878|       ],
 74879|       "source_url": "https://www.revitapidocs.com/2025/dab9ac70-4f27-9013-d067-8e21acbfccdb.htm",
 74880|       "dll_signature_verified": true,
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
```
