# Join Key Shape-Gating Schema Extension

## Overview

Shape-gating extends the join key policy schema to support conditional required/optional items based on a discriminator value (typically shape or type). This enables domains like `dimension_types` to have different join key compositions for different shape families.

## Schema Version

- **v2**: Original schema with `required_items`, `optional_items`
- **v3**: Adds `shape_gating` section for conditional requirements

## Schema Structure

```json
{
  "join_key_schema": "dimension_types.join_key.v3",
  "hash_alg": "md5_utf8_join_pipe",

  "required_items": [
    "dim_type.shape",
    "dim_type.accuracy"
  ],

  "optional_items": [
    "dim_type.prefix",
    "dim_type.suffix"
  ],

  "explicitly_excluded_items": [
    "dim_type.name",
    "dim_type.tick_mark_uid"
  ],

  "shape_gating": {
    "discriminator_key": "dim_type.shape",
    "shape_requirements": {
      "Linear": {
        "additional_required": ["dim_type.witness_line_control"],
        "additional_optional": [],
        "notes": "Linear dimensions must match on witness line control"
      },
      "Radial": {
        "additional_required": ["dim_type.center_marks", "dim_type.center_mark_size"],
        "additional_optional": [],
        "notes": "Radial dimensions must match on center mark configuration"
      },
      "Angular": {
        "additional_required": [],
        "additional_optional": [],
        "notes": "Angular dimensions use common properties only"
      }
    },
    "default_shape_behavior": "common_only"
  },

  "notes": [
    "Schema v3 with shape-gated requirements"
  ]
}
```

## Schema Fields

### Core Fields (Required)

| Field | Type | Description |
|-------|------|-------------|
| `join_key_schema` | string | Schema version identifier |
| `hash_alg` | string | Hash algorithm for join_hash |
| `required_items` | list[str] | Common required keys (all shapes) |
| `optional_items` | list[str] | Common optional keys (all shapes) |
| `explicitly_excluded_items` | list[str] | Keys that must never be used |

### shape_gating Section (Optional)

| Field | Type | Description |
|-------|------|-------------|
| `discriminator_key` | string | Identity key used to determine shape |
| `shape_requirements` | object | Map of shape name to requirements |
| `default_shape_behavior` | string | Behavior for unknown shapes |

### shape_requirements Entry

| Field | Type | Description |
|-------|------|-------------|
| `additional_required` | list[str] | Keys required for this shape |
| `additional_optional` | list[str] | Keys optional for this shape |
| `notes` | string | Documentation (optional) |

### default_shape_behavior Values

| Value | Description |
|-------|-------------|
| `common_only` | Unknown shapes use only common required/optional (default) |
| `block` | Unknown shapes are blocked (join key not generated) |

## Join Matching Algorithm

The join key builder follows this algorithm:

```
1. Extract discriminator value from identity_items
   - Look up discriminator_key (e.g., "dim_type.shape")
   - Get shape_value from kqv map

2. Build effective required/optional lists
   - Start with common required_items and optional_items
   - If shape_value matches a shape_requirements key:
     - Append additional_required to required list
     - Append additional_optional to optional list
   - If shape_value doesn't match any key:
     - If default_shape_behavior == "common_only": use common only
     - If default_shape_behavior == "block": return blocked

3. Build join_key items
   - For each key in effective required list:
     - Add to items (mark missing if not in kqv)
   - For each key in effective optional list:
     - Add to items only if present in kqv

4. Compute join_hash from items

5. Add shape_gating metadata to join_key:
   - discriminator_key
   - shape_value
   - shape_matched (true if shape found in shape_requirements)
   - additional_required_keys (if shape matched)
```

## Example: dimension_types Domain

### Shape Families

| Family | Shapes | Additional Required |
|--------|--------|---------------------|
| LINEAR | Linear, LinearFixed | `witness_line_control` |
| RADIAL | Radial, Diameter, DiameterLinked | `center_marks`, `center_mark_size` |
| ANGULAR | Angular, ArcLength | (none) |
| SPOT | SpotElevation, SpotCoordinate, SpotSlope, SpotElevationFixed | (none) |

### Join Key Composition by Shape

**Linear dimension:**
```
Common: shape, accuracy, tick_mark_sig_hash
+ Shape-specific: witness_line_control
= 4 required items
```

**Radial dimension:**
```
Common: shape, accuracy, tick_mark_sig_hash
+ Shape-specific: center_marks, center_mark_size
= 5 required items
```

**Angular dimension:**
```
Common: shape, accuracy, tick_mark_sig_hash
+ Shape-specific: (none)
= 3 required items
```

## Backward Compatibility

The schema extension is backward compatible:

1. **No shape_gating section**: Builder uses only `required_items` and `optional_items`
2. **shape_gating with common_only default**: Unknown shapes fall back to common requirements
3. **Existing policies continue to work**: No changes required unless shape-gating is desired

## Validation Rules

The policy loader validates:

1. `discriminator_key` must be a non-empty string
2. `shape_requirements` must be an object
3. Each shape requirement must have valid `additional_required` and `additional_optional` arrays
4. `default_shape_behavior` must be "common_only" or "block"
5. Shape-specific keys should NOT appear in common `required_items` (semantic rule, not enforced)

## Output Metadata

When shape_gating is active, the join_key includes metadata:

```json
{
  "schema": "dimension_types.join_key.v3",
  "hash_alg": "md5_utf8_join_pipe",
  "items": [...],
  "join_hash": "abc123...",
  "shape_gating": {
    "discriminator_key": "dim_type.shape",
    "shape_value": "Linear",
    "shape_matched": true,
    "additional_required_keys": ["dim_type.witness_line_control"]
  }
}
```

## Migration Guide

To add shape-gating to an existing domain policy:

1. Identify the discriminator key (e.g., `dim_type.shape`)
2. Identify shape-specific properties that vary by discriminator value
3. Add `shape_gating` section with appropriate `shape_requirements`
4. Move shape-specific keys from common `required_items` to shape-specific `additional_required`
5. Update `join_key_schema` version to indicate the change
6. Test with records of each shape to verify correct join key composition
