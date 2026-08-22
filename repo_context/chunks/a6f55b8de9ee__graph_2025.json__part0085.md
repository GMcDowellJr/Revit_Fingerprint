# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 85 of 216
- Original line range: 32761-33160
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
 32761|       "evidence": [
 32762|         "return type 'Area' directly names a Revit DB object type"
 32763|       ],
 32764|       "source_url": "https://www.revitapidocs.com/2025/16888d1b-a66b-e379-b3f4-2cf62425494d.htm",
 32765|       "dll_signature_verified": true,
 32766|       "dll_relationship_scope": "declared",
 32767|       "dll_semantic_verified": null,
 32768|       "dll_verified_status": "signature_verified_declared",
 32769|       "revitlookup_referenced": null,
 32770|       "revitlookup_requires_document_context": null
 32771|     },
 32772|     {
 32773|       "source": "Autodesk.Revit.DB.AreaTag",
 32774|       "target": "Autodesk.Revit.DB.AreaTagType",
 32775|       "member_name": "AreaTagType",
 32776|       "member_kind": "property",
 32777|       "edge_type": "TAGS_ELEMENT",
 32778|       "confidence": "direct_return_type",
 32779|       "confidence_tier": "core",
 32780|       "target_resolution": "exact",
 32781|       "evidence": [
 32782|         "return type 'AreaTagType' directly names a Revit DB object type"
 32783|       ],
 32784|       "source_url": "https://www.revitapidocs.com/2025/a0fcc0aa-7f05-915c-0dc5-d02640b5d1ec.htm",
 32785|       "dll_signature_verified": true,
 32786|       "dll_relationship_scope": "declared",
 32787|       "dll_semantic_verified": null,
 32788|       "dll_verified_status": "signature_verified_declared",
 32789|       "revitlookup_referenced": null,
 32790|       "revitlookup_requires_document_context": null
 32791|     },
 32792|     {
 32793|       "source": "Autodesk.Revit.DB.AssemblyDifferenceMemberDifference",
 32794|       "target": "Autodesk.Revit.DB.AssemblyMemberDifference",
 32795|       "member_name": "MemberDifference",
 32796|       "member_kind": "property",
 32797|       "edge_type": "UNKNOWN_DB_OBJECT_REFERENCE",
 32798|       "confidence": "direct_return_type",
 32799|       "confidence_tier": "unverified_reference",
 32800|       "target_resolution": "exact",
 32801|       "evidence": [
 32802|         "return type 'AssemblyMemberDifference' directly names a Revit DB object type"
 32803|       ],
 32804|       "source_url": "https://www.revitapidocs.com/2025/1fc71893-03b7-dc07-e58b-4bdb145ffe50.htm",
 32805|       "dll_signature_verified": true,
 32806|       "dll_relationship_scope": "declared",
 32807|       "dll_semantic_verified": null,
 32808|       "dll_verified_status": "signature_verified_declared",
 32809|       "revitlookup_referenced": null,
 32810|       "revitlookup_requires_document_context": null
 32811|     },
 32812|     {
 32813|       "source": "Autodesk.Revit.DB.AssemblyDifferenceMemberDifference",
 32814|       "target": null,
 32815|       "member_name": "MemberId1",
 32816|       "member_kind": "property",
 32817|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 32818|       "confidence": "unknown_reference",
 32819|       "confidence_tier": "unverified_reference",
 32820|       "target_resolution": "none",
 32821|       "evidence": [
 32822|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 32823|       ],
 32824|       "source_url": "https://www.revitapidocs.com/2025/97cf9036-a2c3-b6c8-54ee-a0ed54f26b6b.htm",
 32825|       "dll_signature_verified": true,
 32826|       "dll_relationship_scope": "declared",
 32827|       "dll_semantic_verified": null,
 32828|       "dll_verified_status": "signature_verified_declared",
 32829|       "revitlookup_referenced": null,
 32830|       "revitlookup_requires_document_context": null
 32831|     },
 32832|     {
 32833|       "source": "Autodesk.Revit.DB.AssemblyDifferenceMemberDifference",
 32834|       "target": null,
 32835|       "member_name": "MemberId2",
 32836|       "member_kind": "property",
 32837|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 32838|       "confidence": "unknown_reference",
 32839|       "confidence_tier": "unverified_reference",
 32840|       "target_resolution": "none",
 32841|       "evidence": [
 32842|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 32843|       ],
 32844|       "source_url": "https://www.revitapidocs.com/2025/46592146-bce7-c113-6c50-5f973442bfc7.htm",
 32845|       "dll_signature_verified": true,
 32846|       "dll_relationship_scope": "declared",
 32847|       "dll_semantic_verified": null,
 32848|       "dll_verified_status": "signature_verified_declared",
 32849|       "revitlookup_referenced": null,
 32850|       "revitlookup_requires_document_context": null
 32851|     },
 32852|     {
 32853|       "source": "Autodesk.Revit.DB.AssemblyDifferenceNamingCategory",
 32854|       "target": "Autodesk.Revit.DB.Category",
 32855|       "member_name": "NamingCategoryId1",
 32856|       "member_kind": "property",
 32857|       "edge_type": "HAS_CATEGORY",
 32858|       "confidence": "elementid_with_strong_name",
 32859|       "confidence_tier": "core",
 32860|       "target_resolution": "exact",
 32861|       "evidence": [
 32862|         "member name 'NamingCategoryId1' matches keyword pattern /Category/"
 32863|       ],
 32864|       "source_url": "https://www.revitapidocs.com/2025/a26eccfe-27f6-e78c-4889-2be7f3c8f420.htm",
 32865|       "dll_signature_verified": true,
 32866|       "dll_relationship_scope": "declared",
 32867|       "dll_semantic_verified": null,
 32868|       "dll_verified_status": "signature_verified_declared",
 32869|       "revitlookup_referenced": null,
 32870|       "revitlookup_requires_document_context": null
 32871|     },
 32872|     {
 32873|       "source": "Autodesk.Revit.DB.AssemblyDifferenceNamingCategory",
 32874|       "target": "Autodesk.Revit.DB.Category",
 32875|       "member_name": "NamingCategoryId2",
 32876|       "member_kind": "property",
 32877|       "edge_type": "HAS_CATEGORY",
 32878|       "confidence": "elementid_with_strong_name",
 32879|       "confidence_tier": "core",
 32880|       "target_resolution": "exact",
 32881|       "evidence": [
 32882|         "member name 'NamingCategoryId2' matches keyword pattern /Category/"
 32883|       ],
 32884|       "source_url": "https://www.revitapidocs.com/2025/15c03cc2-ed08-eea4-498f-8f1e0ce5d7de.htm",
 32885|       "dll_signature_verified": true,
 32886|       "dll_relationship_scope": "declared",
 32887|       "dll_semantic_verified": null,
 32888|       "dll_verified_status": "signature_verified_declared",
 32889|       "revitlookup_referenced": null,
 32890|       "revitlookup_requires_document_context": null
 32891|     },
 32892|     {
 32893|       "source": "Autodesk.Revit.DB.AssemblyInstance",
 32894|       "target": null,
 32895|       "member_name": "AssemblyTypeName",
 32896|       "member_kind": "property",
 32897|       "edge_type": "MEMBER_OF_ASSEMBLY",
 32898|       "confidence": "name_only_candidate",
 32899|       "confidence_tier": "likely",
 32900|       "target_resolution": "none",
 32901|       "evidence": [
 32902|         "member name 'AssemblyTypeName' matches keyword pattern /Assembly/ but return type 'string' gives no type-level confirmation"
 32903|       ],
 32904|       "source_url": "https://www.revitapidocs.com/2025/182e1b2e-a25d-4597-ef28-68df80023a5d.htm",
 32905|       "dll_signature_verified": true,
 32906|       "dll_relationship_scope": "declared",
 32907|       "dll_semantic_verified": null,
 32908|       "dll_verified_status": "signature_verified_declared",
 32909|       "revitlookup_referenced": null,
 32910|       "revitlookup_requires_document_context": null
 32911|     },
 32912|     {
 32913|       "source": "Autodesk.Revit.DB.AssemblyInstance",
 32914|       "target": "Autodesk.Revit.DB.Location",
 32915|       "member_name": "Location",
 32916|       "member_kind": "property",
 32917|       "edge_type": "REFERENCES",
 32918|       "confidence": "direct_return_type",
 32919|       "confidence_tier": "core",
 32920|       "target_resolution": "exact",
 32921|       "evidence": [
 32922|         "return type 'Location' directly names a Revit DB object type"
 32923|       ],
 32924|       "source_url": "https://www.revitapidocs.com/2025/f1ffc6ac-24ce-4d10-9c9a-24a99ffaf94d.htm",
 32925|       "dll_signature_verified": true,
 32926|       "dll_relationship_scope": "declared",
 32927|       "dll_semantic_verified": null,
 32928|       "dll_verified_status": "signature_verified_declared",
 32929|       "revitlookup_referenced": null,
 32930|       "revitlookup_requires_document_context": null
 32931|     },
 32932|     {
 32933|       "source": "Autodesk.Revit.DB.AssemblyInstance",
 32934|       "target": "Autodesk.Revit.DB.Category",
 32935|       "member_name": "NamingCategoryId",
 32936|       "member_kind": "property",
 32937|       "edge_type": "HAS_CATEGORY",
 32938|       "confidence": "elementid_with_strong_name",
 32939|       "confidence_tier": "core",
 32940|       "target_resolution": "exact",
 32941|       "evidence": [
 32942|         "member name 'NamingCategoryId' matches keyword pattern /Category/"
 32943|       ],
 32944|       "source_url": "https://www.revitapidocs.com/2025/5a4c70fa-83cc-d594-2dda-84f3386ceae7.htm",
 32945|       "dll_signature_verified": true,
 32946|       "dll_relationship_scope": "declared",
 32947|       "dll_semantic_verified": null,
 32948|       "dll_verified_status": "signature_verified_declared",
 32949|       "revitlookup_referenced": null,
 32950|       "revitlookup_requires_document_context": null
 32951|     },
 32952|     {
 32953|       "source": "Autodesk.Revit.DB.AssemblyInstance",
 32954|       "target": null,
 32955|       "member_name": "AllowsAssemblyViewCreation",
 32956|       "member_kind": "method",
 32957|       "edge_type": "MEMBER_OF_ASSEMBLY",
 32958|       "confidence": "name_only_candidate",
 32959|       "confidence_tier": "likely",
 32960|       "target_resolution": "none",
 32961|       "evidence": [
 32962|         "member name 'AllowsAssemblyViewCreation' matches keyword pattern /Assembly/ but return type 'bool' gives no type-level confirmation"
 32963|       ],
 32964|       "source_url": "https://www.revitapidocs.com/2025/63d643a0-7da2-eecb-d895-4e4603ede331.htm",
 32965|       "dll_signature_verified": true,
 32966|       "dll_relationship_scope": "declared",
 32967|       "dll_semantic_verified": null,
 32968|       "dll_verified_status": "signature_verified_declared",
 32969|       "revitlookup_referenced": null,
 32970|       "revitlookup_requires_document_context": null
 32971|     },
 32972|     {
 32973|       "source": "Autodesk.Revit.DB.AssemblyInstance",
 32974|       "target": null,
 32975|       "member_name": "AreElementsValidForAssembly",
 32976|       "member_kind": "method",
 32977|       "edge_type": "MEMBER_OF_ASSEMBLY",
 32978|       "confidence": "name_only_candidate",
 32979|       "confidence_tier": "likely",
 32980|       "target_resolution": "none",
 32981|       "evidence": [
 32982|         "member name 'AreElementsValidForAssembly' matches keyword pattern /Assembly/ but return type 'bool' gives no type-level confirmation"
 32983|       ],
 32984|       "source_url": "https://www.revitapidocs.com/2025/b86d920d-dd9e-db71-c650-cdfbf623942d.htm",
 32985|       "dll_signature_verified": true,
 32986|       "dll_relationship_scope": "declared",
 32987|       "dll_semantic_verified": null,
 32988|       "dll_verified_status": "signature_verified_declared",
 32989|       "revitlookup_referenced": null,
 32990|       "revitlookup_requires_document_context": null
 32991|     },
 32992|     {
 32993|       "source": "Autodesk.Revit.DB.AssemblyInstance",
 32994|       "target": null,
 32995|       "member_name": "CanRemoveElementsFromAssembly",
 32996|       "member_kind": "method",
 32997|       "edge_type": "MEMBER_OF_ASSEMBLY",
 32998|       "confidence": "name_only_candidate",
 32999|       "confidence_tier": "likely",
 33000|       "target_resolution": "none",
 33001|       "evidence": [
 33002|         "member name 'CanRemoveElementsFromAssembly' matches keyword pattern /Assembly/ but return type 'bool' gives no type-level confirmation"
 33003|       ],
 33004|       "source_url": "https://www.revitapidocs.com/2025/6eadcc05-6ac5-81f4-79ee-4893a050d34b.htm",
 33005|       "dll_signature_verified": true,
 33006|       "dll_relationship_scope": "declared",
 33007|       "dll_semantic_verified": null,
 33008|       "dll_verified_status": "signature_verified_declared",
 33009|       "revitlookup_referenced": null,
 33010|       "revitlookup_requires_document_context": null
 33011|     },
 33012|     {
 33013|       "source": "Autodesk.Revit.DB.AssemblyInstance",
 33014|       "target": "Autodesk.Revit.DB.AssemblyDifference",
 33015|       "member_name": "CompareAssemblyInstances",
 33016|       "member_kind": "method",
 33017|       "edge_type": "MEMBER_OF_ASSEMBLY",
 33018|       "confidence": "direct_return_type",
 33019|       "confidence_tier": "core",
 33020|       "target_resolution": "exact",
 33021|       "evidence": [
 33022|         "return type 'AssemblyDifference' directly names a Revit DB object type"
 33023|       ],
 33024|       "source_url": "https://www.revitapidocs.com/2025/d7253e56-f112-45d0-8b59-f6cb86b42159.htm",
 33025|       "dll_signature_verified": true,
 33026|       "dll_relationship_scope": "declared",
 33027|       "dll_semantic_verified": null,
 33028|       "dll_verified_status": "signature_verified_declared",
 33029|       "revitlookup_referenced": null,
 33030|       "revitlookup_requires_document_context": null
 33031|     },
 33032|     {
 33033|       "source": "Autodesk.Revit.DB.AssemblyInstance",
 33034|       "target": null,
 33035|       "member_name": "Disassemble",
 33036|       "member_kind": "method",
 33037|       "edge_type": "RETURNS_ELEMENT_IDS",
 33038|       "confidence": "unknown_reference",
 33039|       "confidence_tier": "unverified_reference",
 33040|       "target_resolution": "none",
 33041|       "evidence": [
 33042|         "return type 'ICollection < ElementId >' is a collection of ID wrappers with no strong name hint"
 33043|       ],
 33044|       "source_url": "https://www.revitapidocs.com/2025/a2c65159-bd72-1a16-f95e-c2a5fac474b0.htm",
 33045|       "dll_signature_verified": true,
 33046|       "dll_relationship_scope": "declared",
 33047|       "dll_semantic_verified": null,
 33048|       "dll_verified_status": "signature_verified_declared",
 33049|       "revitlookup_referenced": null,
 33050|       "revitlookup_requires_document_context": null
 33051|     },
 33052|     {
 33053|       "source": "Autodesk.Revit.DB.AssemblyInstance",
 33054|       "target": null,
 33055|       "member_name": "GetMemberIds",
 33056|       "member_kind": "method",
 33057|       "edge_type": "MEMBER_OF_GROUP",
 33058|       "confidence": "elementid_collection_with_strong_name",
 33059|       "confidence_tier": "core",
 33060|       "target_resolution": "none",
 33061|       "evidence": [
 33062|         "member name 'GetMemberIds' matches keyword pattern /^GetMember|Group/"
 33063|       ],
 33064|       "source_url": "https://www.revitapidocs.com/2025/09acaa13-d13a-776b-6019-fd5840dad996.htm",
 33065|       "dll_signature_verified": true,
 33066|       "dll_relationship_scope": "declared",
 33067|       "dll_semantic_verified": null,
 33068|       "dll_verified_status": "signature_verified_declared",
 33069|       "revitlookup_referenced": null,
 33070|       "revitlookup_requires_document_context": null
 33071|     },
 33072|     {
 33073|       "source": "Autodesk.Revit.DB.AssemblyInstance",
 33074|       "target": "Autodesk.Revit.DB.Category",
 33075|       "member_name": "IsValidNamingCategory",
 33076|       "member_kind": "method",
 33077|       "edge_type": "HAS_CATEGORY",
 33078|       "confidence": "name_only_candidate",
 33079|       "confidence_tier": "likely",
 33080|       "target_resolution": "exact",
 33081|       "evidence": [
 33082|         "member name 'IsValidNamingCategory' matches keyword pattern /Category/ but return type 'bool' gives no type-level confirmation"
 33083|       ],
 33084|       "source_url": "https://www.revitapidocs.com/2025/787779c8-a2ed-d7c4-5cf2-0cc5c20de50e.htm",
 33085|       "dll_signature_verified": true,
 33086|       "dll_relationship_scope": "declared",
 33087|       "dll_semantic_verified": null,
 33088|       "dll_verified_status": "signature_verified_declared",
 33089|       "revitlookup_referenced": null,
 33090|       "revitlookup_requires_document_context": null
 33091|     },
 33092|     {
 33093|       "source": "Autodesk.Revit.DB.AssemblyMemberDifferentCategory",
 33094|       "target": "Autodesk.Revit.DB.Category",
 33095|       "member_name": "CategoryId1",
 33096|       "member_kind": "property",
 33097|       "edge_type": "HAS_CATEGORY",
 33098|       "confidence": "elementid_with_strong_name",
 33099|       "confidence_tier": "core",
 33100|       "target_resolution": "exact",
 33101|       "evidence": [
 33102|         "member name 'CategoryId1' matches keyword pattern /Category/"
 33103|       ],
 33104|       "source_url": "https://www.revitapidocs.com/2025/aed40ee6-4d73-d3c0-a2f4-38cb290a0a26.htm",
 33105|       "dll_signature_verified": true,
 33106|       "dll_relationship_scope": "declared",
 33107|       "dll_semantic_verified": null,
 33108|       "dll_verified_status": "signature_verified_declared",
 33109|       "revitlookup_referenced": null,
 33110|       "revitlookup_requires_document_context": null
 33111|     },
 33112|     {
 33113|       "source": "Autodesk.Revit.DB.AssemblyMemberDifferentCategory",
 33114|       "target": "Autodesk.Revit.DB.Category",
 33115|       "member_name": "CategoryId2",
 33116|       "member_kind": "property",
 33117|       "edge_type": "HAS_CATEGORY",
 33118|       "confidence": "elementid_with_strong_name",
 33119|       "confidence_tier": "core",
 33120|       "target_resolution": "exact",
 33121|       "evidence": [
 33122|         "member name 'CategoryId2' matches keyword pattern /Category/"
 33123|       ],
 33124|       "source_url": "https://www.revitapidocs.com/2025/2f687142-fdc9-f422-c904-c7705bec7331.htm",
 33125|       "dll_signature_verified": true,
 33126|       "dll_relationship_scope": "declared",
 33127|       "dll_semantic_verified": null,
 33128|       "dll_verified_status": "signature_verified_declared",
 33129|       "revitlookup_referenced": null,
 33130|       "revitlookup_requires_document_context": null
 33131|     },
 33132|     {
 33133|       "source": "Autodesk.Revit.DB.AssemblyMemberDifferentType",
 33134|       "target": null,
 33135|       "member_name": "TypeId1",
 33136|       "member_kind": "property",
 33137|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 33138|       "confidence": "unknown_reference",
 33139|       "confidence_tier": "unverified_reference",
 33140|       "target_resolution": "none",
 33141|       "evidence": [
 33142|         "return type is 'ElementId', an ID wrapper, but member name gives no strong hint of the target type"
 33143|       ],
 33144|       "source_url": "https://www.revitapidocs.com/2025/e9afb7bd-c252-9a49-7cc6-af7567132242.htm",
 33145|       "dll_signature_verified": true,
 33146|       "dll_relationship_scope": "declared",
 33147|       "dll_semantic_verified": null,
 33148|       "dll_verified_status": "signature_verified_declared",
 33149|       "revitlookup_referenced": null,
 33150|       "revitlookup_requires_document_context": null
 33151|     },
 33152|     {
 33153|       "source": "Autodesk.Revit.DB.AssemblyMemberDifferentType",
 33154|       "target": null,
 33155|       "member_name": "TypeId2",
 33156|       "member_kind": "property",
 33157|       "edge_type": "UNKNOWN_ELEMENTID_REFERENCE",
 33158|       "confidence": "unknown_reference",
 33159|       "confidence_tier": "unverified_reference",
 33160|       "target_resolution": "none",
```
