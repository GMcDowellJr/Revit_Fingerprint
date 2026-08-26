# Chunk of domains/graph_2025.json

- Source relative path: `domains/graph_2025.json`
- Chunk: 1 of 216
- Original line range: 1-400
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: 4cd4a9219ca8075cb6b91806e4bdc5724084e9c1a9ad4448d027f581dbd42cab
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| {
     2|   "metadata": {
     3|     "revit_version": "2025",
     4|     "node_count": 2461,
     5|     "edge_count": 2561,
     6|     "external_node_count": 1,
     7|     "target_resolution_counts": {
     8|       "none": 924,
     9|       "exact": 1299,
    10|       "short_name_fallback": 333,
    11|       "external": 5
    12|     },
    13|     "confidence_tier_counts": {
    14|       "unverified_reference": 1373,
    15|       "needs_validation": 181,
    16|       "core": 417,
    17|       "likely": 590
    18|     },
    19|     "dll_verified_status_counts": {
    20|       "signature_verified_declared": 2549,
    21|       "member_not_found": 11,
    22|       "signature_mismatch": 1
    23|     },
    24|     "revitlookup_referenced_counts": {
    25|       "not_checked": 2474,
    26|       "referenced": 87
    27|     },
    28|     "community_count": 43
    29|   },
    30|   "communities": [
    31|     {
    32|       "id": 0,
    33|       "label": "Category \u00b7 ConceptualConstructionType \u00b7 Family",
    34|       "label_source": "heuristic",
    35|       "size": 24,
    36|       "member_ids": [
    37|         "Autodesk.Revit.DB.Analysis.ConceptualConstructionType",
    38|         "Autodesk.Revit.DB.Analysis.ConceptualSurfaceType",
    39|         "Autodesk.Revit.DB.Analysis.EnergyAnalysisDetailModel",
    40|         "Autodesk.Revit.DB.Analysis.RouteAnalysisSettings",
    41|         "Autodesk.Revit.DB.AssemblyDifferenceNamingCategory",
    42|         "Autodesk.Revit.DB.AssemblyMemberDifferentCategory",
    43|         "Autodesk.Revit.DB.Categories",
    44|         "Autodesk.Revit.DB.Category",
    45|         "Autodesk.Revit.DB.ClassificationEntry",
    46|         "Autodesk.Revit.DB.ColorFillLegend",
    47|         "Autodesk.Revit.DB.ColorFillScheme",
    48|         "Autodesk.Revit.DB.ContourSettingItem",
    49|         "Autodesk.Revit.DB.ElementCategoryFilter",
    50|         "Autodesk.Revit.DB.ElementMulticategoryFilter",
    51|         "Autodesk.Revit.DB.Family",
    52|         "Autodesk.Revit.DB.Mechanical.DuctPressureDropData",
    53|         "Autodesk.Revit.DB.MultiReferenceAnnotationType",
    54|         "Autodesk.Revit.DB.NestedFamilyTypeReference",
    55|         "Autodesk.Revit.DB.ParameterFilterElement",
    56|         "Autodesk.Revit.DB.Part",
    57|         "Autodesk.Revit.DB.Plumbing.PipePressureDropData",
    58|         "Autodesk.Revit.DB.Structure.LoadCase",
    59|         "Autodesk.Revit.DB.TableCellCombinedParameterData",
    60|         "Autodesk.Revit.DB.TableSectionData"
    61|       ]
    62|     },
    63|     {
    64|       "id": 1,
    65|       "label": "Document \u00b7 Room \u00b7 FamilyInstance",
    66|       "label_source": "heuristic",
    67|       "size": 23,
    68|       "member_ids": [
    69|         "Autodesk.Revit.DB.Architecture.Room",
    70|         "Autodesk.Revit.DB.BindingMap",
    71|         "Autodesk.Revit.DB.Document",
    72|         "Autodesk.Revit.DB.DuplicateTypeNamesHandlerArgs",
    73|         "Autodesk.Revit.DB.ElementNode",
    74|         "Autodesk.Revit.DB.Events.DocumentChangedEventArgs",
    75|         "Autodesk.Revit.DB.Events.DocumentWorksharingEnabledEventArgs",
    76|         "Autodesk.Revit.DB.Events.PostDocEventArgs",
    77|         "Autodesk.Revit.DB.Events.PreDocEventArgs",
    78|         "Autodesk.Revit.DB.Events.RevitAPIPostDocEventArgs",
    79|         "Autodesk.Revit.DB.Events.RevitAPIPreDocEventArgs",
    80|         "Autodesk.Revit.DB.FailuresAccessor",
    81|         "Autodesk.Revit.DB.FamilyInstance",
    82|         "Autodesk.Revit.DB.FamilySymbol",
    83|         "Autodesk.Revit.DB.GeometryInstance",
    84|         "Autodesk.Revit.DB.IFC.ImporterIFC",
    85|         "Autodesk.Revit.DB.LinkNode",
    86|         "Autodesk.Revit.DB.Mechanical.Space",
    87|         "Autodesk.Revit.DB.Structure.CodeCheckingParameterServiceData",
    88|         "Autodesk.Revit.DB.Structure.MemberForcesServiceData",
    89|         "Autodesk.Revit.DB.Structure.RebarUpdateCurvesData",
    90|         "Autodesk.Revit.DB.Structure.StructuralSectionsServiceData",
    91|         "Autodesk.Revit.DB.UpdaterData"
    92|       ]
    93|     },
    94|     {
    95|       "id": 2,
    96|       "label": "Material \u00b7 MassSurfaceData \u00b7 EnergyAnalysisConstruction",
    97|       "label_source": "heuristic",
    98|       "size": 20,
    99|       "member_ids": [
   100|         "Autodesk.Revit.DB.Analysis.EnergyAnalysisConstruction",
   101|         "Autodesk.Revit.DB.Analysis.MassLevelData",
   102|         "Autodesk.Revit.DB.Analysis.MassSurfaceData",
   103|         "Autodesk.Revit.DB.Architecture.NonContinuousRailInfo",
   104|         "Autodesk.Revit.DB.Architecture.StairsRunType",
   105|         "Autodesk.Revit.DB.Architecture.TopographySurface",
   106|         "Autodesk.Revit.DB.CompoundStructure",
   107|         "Autodesk.Revit.DB.CompoundStructureLayer",
   108|         "Autodesk.Revit.DB.Face",
   109|         "Autodesk.Revit.DB.GeometryElement",
   110|         "Autodesk.Revit.DB.IFC.ExporterIFC",
   111|         "Autodesk.Revit.DB.IFC.IFCFamilyInstanceExtrusionExportResults",
   112|         "Autodesk.Revit.DB.Material",
   113|         "Autodesk.Revit.DB.MaterialNode",
   114|         "Autodesk.Revit.DB.Mesh",
   115|         "Autodesk.Revit.DB.Segment",
   116|         "Autodesk.Revit.DB.Structure.AnalyticalElement",
   117|         "Autodesk.Revit.DB.TessellatedFace",
   118|         "Autodesk.Revit.DB.ViewSchedule",
   119|         "Autodesk.Revit.DB.WallSweepInfo"
   120|       ]
   121|     },
   122|     {
   123|       "id": 3,
   124|       "label": "View \u00b7 ViewFamilyType \u00b7 BIMExportOptions",
   125|       "label_source": "heuristic",
   126|       "size": 19,
   127|       "member_ids": [
   128|         "Autodesk.Revit.DB.BIMExportOptions",
   129|         "Autodesk.Revit.DB.CADLinkOptions",
   130|         "Autodesk.Revit.DB.Control",
   131|         "Autodesk.Revit.DB.Dimension",
   132|         "Autodesk.Revit.DB.Electrical.PanelScheduleView",
   133|         "Autodesk.Revit.DB.ElementOwnerViewFilter",
   134|         "Autodesk.Revit.DB.ElevationMarker",
   135|         "Autodesk.Revit.DB.Events.ViewExportedEventArgs",
   136|         "Autodesk.Revit.DB.Events.ViewExportingEventArgs",
   137|         "Autodesk.Revit.DB.Events.ViewPrintedEventArgs",
   138|         "Autodesk.Revit.DB.Events.ViewPrintingEventArgs",
   139|         "Autodesk.Revit.DB.HomeCamera",
   140|         "Autodesk.Revit.DB.NavisworksExportOptions",
   141|         "Autodesk.Revit.DB.Options",
   142|         "Autodesk.Revit.DB.ReferenceIntersector",
   143|         "Autodesk.Revit.DB.StartingViewSettings",
   144|         "Autodesk.Revit.DB.View",
   145|         "Autodesk.Revit.DB.ViewFamilyType",
   146|         "Autodesk.Revit.DB.ViewNode"
   147|       ]
   148|     },
   149|     {
   150|       "id": 4,
   151|       "label": "Level \u00b7 AreaBasedLoadBoundaryLineData \u00b7 MassInstanceUtils",
   152|       "label_source": "heuristic",
   153|       "size": 15,
   154|       "member_ids": [
   155|         "Autodesk.Revit.DB.Architecture.MultistoryStairs",
   156|         "Autodesk.Revit.DB.Architecture.Railing",
   157|         "Autodesk.Revit.DB.BeamSystem",
   158|         "Autodesk.Revit.DB.Electrical.AreaBasedLoadBoundaryLineData",
   159|         "Autodesk.Revit.DB.ElementLevelFilter",
   160|         "Autodesk.Revit.DB.IFC.ExporterIFCUtils",
   161|         "Autodesk.Revit.DB.Level",
   162|         "Autodesk.Revit.DB.LevelAssociationData",
   163|         "Autodesk.Revit.DB.MassInstanceUtils",
   164|         "Autodesk.Revit.DB.NumberSystem",
   165|         "Autodesk.Revit.DB.PlanViewRange",
   166|         "Autodesk.Revit.DB.RevitLinkType",
   167|         "Autodesk.Revit.DB.SunAndShadowSettings",
   168|         "Autodesk.Revit.DB.View3D",
   169|         "Autodesk.Revit.DB.ViewPlan"
   170|       ]
   171|     },
   172|     {
   173|       "id": 5,
   174|       "label": "Sketch \u00b7 SweptBlend \u00b7 Blend",
   175|       "label_source": "heuristic",
   176|       "size": 11,
   177|       "member_ids": [
   178|         "Autodesk.Revit.DB.Blend",
   179|         "Autodesk.Revit.DB.Ceiling",
   180|         "Autodesk.Revit.DB.Extrusion",
   181|         "Autodesk.Revit.DB.Floor",
   182|         "Autodesk.Revit.DB.Revolution",
   183|         "Autodesk.Revit.DB.Sketch",
   184|         "Autodesk.Revit.DB.Structure.AnalyticalSurfaceBase",
   185|         "Autodesk.Revit.DB.Sweep",
   186|         "Autodesk.Revit.DB.SweptBlend",
   187|         "Autodesk.Revit.DB.Toposolid",
   188|         "Autodesk.Revit.DB.Wall"
   189|       ]
   190|     },
   191|     {
   192|       "id": 6,
   193|       "label": "ViewSheet \u00b7 FabricArea \u00b7 Viewport",
   194|       "label_source": "heuristic",
   195|       "size": 9,
   196|       "member_ids": [
   197|         "Autodesk.Revit.DB.DWFImportOptions",
   198|         "Autodesk.Revit.DB.IViewSheetSet",
   199|         "Autodesk.Revit.DB.ImageExportOptions",
   200|         "Autodesk.Revit.DB.InSessionViewSheetSet",
   201|         "Autodesk.Revit.DB.RevisionCloud",
   202|         "Autodesk.Revit.DB.Structure.FabricArea",
   203|         "Autodesk.Revit.DB.ViewSheet",
   204|         "Autodesk.Revit.DB.ViewSheetSet",
   205|         "Autodesk.Revit.DB.Viewport"
   206|       ]
   207|     },
   208|     {
   209|       "id": 7,
   210|       "label": "Workset \u00b7 WorksetTable \u00b7 FilteredWorksetCollector",
   211|       "label_source": "heuristic",
   212|       "size": 8,
   213|       "member_ids": [
   214|         "Autodesk.Revit.DB.DeleteWorksetSettings",
   215|         "Autodesk.Revit.DB.ElementWorksetFilter",
   216|         "Autodesk.Revit.DB.FilteredWorksetCollector",
   217|         "Autodesk.Revit.DB.FilteredWorksetIdIterator",
   218|         "Autodesk.Revit.DB.Workset",
   219|         "Autodesk.Revit.DB.WorksetId",
   220|         "Autodesk.Revit.DB.WorksetPreview",
   221|         "Autodesk.Revit.DB.WorksetTable"
   222|       ]
   223|     },
   224|     {
   225|       "id": 8,
   226|       "label": "Phase \u00b7 PlanTopology \u00b7 EnergyDataSettings",
   227|       "label_source": "heuristic",
   228|       "size": 6,
   229|       "member_ids": [
   230|         "Autodesk.Revit.DB.Analysis.EnergyDataSettings",
   231|         "Autodesk.Revit.DB.ElementPhaseStatusFilter",
   232|         "Autodesk.Revit.DB.Mechanical.Zone",
   233|         "Autodesk.Revit.DB.Phase",
   234|         "Autodesk.Revit.DB.PlanTopology",
   235|         "Autodesk.Revit.DB.RevitLinkGraphicsSettings"
   236|       ]
   237|     },
   238|     {
   239|       "id": 9,
   240|       "label": "ThermalProperties \u00b7 FloorType \u00b7 BuildingPadType",
   241|       "label_source": "heuristic",
   242|       "size": 6,
   243|       "member_ids": [
   244|         "Autodesk.Revit.DB.BuildingPadType",
   245|         "Autodesk.Revit.DB.CeilingType",
   246|         "Autodesk.Revit.DB.FloorType",
   247|         "Autodesk.Revit.DB.RoofType",
   248|         "Autodesk.Revit.DB.ThermalProperties",
   249|         "Autodesk.Revit.DB.WallType"
   250|       ]
   251|     },
   252|     {
   253|       "id": 10,
   254|       "label": "Reference \u00b7 DividedSurface \u00b7 RebarConstraint",
   255|       "label_source": "heuristic",
   256|       "size": 6,
   257|       "member_ids": [
   258|         "Autodesk.Revit.DB.CurveByPointsUtils",
   259|         "Autodesk.Revit.DB.DividedSurface",
   260|         "Autodesk.Revit.DB.PointRelativeToPoint",
   261|         "Autodesk.Revit.DB.Reference",
   262|         "Autodesk.Revit.DB.Structure.RebarBendingDetail",
   263|         "Autodesk.Revit.DB.Structure.RebarConstraint"
   264|       ]
   265|     },
   266|     {
   267|       "id": 11,
   268|       "label": "GraphicsStyle \u00b7 SolidOptions \u00b7 GeometryObject",
   269|       "label_source": "heuristic",
   270|       "size": 6,
   271|       "member_ids": [
   272|         "Autodesk.Revit.DB.GeometryObject",
   273|         "Autodesk.Revit.DB.GraphicsStyle",
   274|         "Autodesk.Revit.DB.ModelCurve",
   275|         "Autodesk.Revit.DB.SolidOptions",
   276|         "Autodesk.Revit.DB.SymbolicCurve",
   277|         "Autodesk.Revit.DB.TessellatedShapeBuilder"
   278|       ]
   279|     },
   280|     {
   281|       "id": 12,
   282|       "label": "Element \u00b7 ChangeType \u00b7 Opening",
   283|       "label_source": "heuristic",
   284|       "size": 5,
   285|       "member_ids": [
   286|         "Autodesk.Revit.DB.ChangeType",
   287|         "Autodesk.Revit.DB.Element",
   288|         "Autodesk.Revit.DB.Opening",
   289|         "Autodesk.Revit.DB.Parameter",
   290|         "Autodesk.Revit.DB.ParameterMap"
   291|       ]
   292|     },
   293|     {
   294|       "id": 13,
   295|       "label": "ConnectorManager \u00b7 MEPCurve \u00b7 Connector",
   296|       "label_source": "heuristic",
   297|       "size": 5,
   298|       "member_ids": [
   299|         "Autodesk.Revit.DB.Connector",
   300|         "Autodesk.Revit.DB.ConnectorManager",
   301|         "Autodesk.Revit.DB.MEPCurve",
   302|         "Autodesk.Revit.DB.MEPModel",
   303|         "Autodesk.Revit.DB.MEPSystem"
   304|       ]
   305|     },
   306|     {
   307|       "id": 14,
   308|       "label": "FamilyManager \u00b7 FamilyParameter \u00b7 ParameterSet",
   309|       "label_source": "heuristic",
   310|       "size": 4,
   311|       "member_ids": [
   312|         "Autodesk.Revit.DB.FamilyManager",
   313|         "Autodesk.Revit.DB.FamilyParameter",
   314|         "Autodesk.Revit.DB.FamilyParameterSet",
   315|         "Autodesk.Revit.DB.ParameterSet"
   316|       ]
   317|     },
   318|     {
   319|       "id": 15,
   320|       "label": "PrintParameters \u00b7 IPrintSetting \u00b7 InSessionPrintSetting",
   321|       "label_source": "heuristic",
   322|       "size": 4,
   323|       "member_ids": [
   324|         "Autodesk.Revit.DB.IPrintSetting",
   325|         "Autodesk.Revit.DB.InSessionPrintSetting",
   326|         "Autodesk.Revit.DB.PrintParameters",
   327|         "Autodesk.Revit.DB.PrintSetting"
   328|       ]
   329|     },
   330|     {
   331|       "id": 16,
   332|       "label": "Location \u00b7 SpatialElement \u00b7 SpatialElementTag",
   333|       "label_source": "heuristic",
   334|       "size": 4,
   335|       "member_ids": [
   336|         "Autodesk.Revit.DB.Location",
   337|         "Autodesk.Revit.DB.ModelText",
   338|         "Autodesk.Revit.DB.SpatialElement",
   339|         "Autodesk.Revit.DB.SpatialElementTag"
   340|       ]
   341|     },
   342|     {
   343|       "id": 17,
   344|       "label": "ParameterValue \u00b7 Subelement \u00b7 Rebar",
   345|       "label_source": "heuristic",
   346|       "size": 4,
   347|       "member_ids": [
   348|         "Autodesk.Revit.DB.MEPFamilyConnectorInfo",
   349|         "Autodesk.Revit.DB.ParameterValue",
   350|         "Autodesk.Revit.DB.Structure.Rebar",
   351|         "Autodesk.Revit.DB.Subelement"
   352|       ]
   353|     },
   354|     {
   355|       "id": 18,
   356|       "label": "RebarRoundingManager \u00b7 ReinforcementSettings \u00b7 RebarBarType",
   357|       "label_source": "heuristic",
   358|       "size": 4,
   359|       "member_ids": [
   360|         "Autodesk.Revit.DB.Structure.RebarBarType",
   361|         "Autodesk.Revit.DB.Structure.RebarInSystem",
   362|         "Autodesk.Revit.DB.Structure.RebarRoundingManager",
   363|         "Autodesk.Revit.DB.Structure.ReinforcementSettings"
   364|       ]
   365|     },
   366|     {
   367|       "id": 19,
   368|       "label": "MEPSystemType \u00b7 FillPatternElement \u00b7 ColorFillSchemeEntry",
   369|       "label_source": "heuristic",
   370|       "size": 3,
   371|       "member_ids": [
   372|         "Autodesk.Revit.DB.ColorFillSchemeEntry",
   373|         "Autodesk.Revit.DB.FillPatternElement",
   374|         "Autodesk.Revit.DB.MEPSystemType"
   375|       ]
   376|     },
   377|     {
   378|       "id": 20,
   379|       "label": "SketchPlane \u00b7 CurveByPoints \u00b7 CurveElement",
   380|       "label_source": "heuristic",
   381|       "size": 3,
   382|       "member_ids": [
   383|         "Autodesk.Revit.DB.CurveByPoints",
   384|         "Autodesk.Revit.DB.CurveElement",
   385|         "Autodesk.Revit.DB.SketchPlane"
   386|       ]
   387|     },
   388|     {
   389|       "id": 21,
   390|       "label": "DesignOption \u00b7 ElementRecord \u00b7 ElementDesignOptionFilter",
   391|       "label_source": "heuristic",
   392|       "size": 3,
   393|       "member_ids": [
   394|         "Autodesk.Revit.DB.DesignOption",
   395|         "Autodesk.Revit.DB.ElementDesignOptionFilter",
   396|         "Autodesk.Revit.DB.ElementRecord"
   397|       ]
   398|     },
   399|     {
   400|       "id": 22,
```
