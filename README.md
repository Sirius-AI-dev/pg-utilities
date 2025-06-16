# pg-utilities
Useful utilities for PostgreSQL to process json objects, to build templates and to map data

# How to install
Open **pg_utilities.sql** file, copy its content, and paste in PostgreSQL in required database. A new **"ub"** scheme with multiple functions will be created.

List of all functions, with detailed description and examples, is below:

## Table of Contents

*   [Array Utilities](#array-utilities)
    *   [`ub.util_array_float`](#ubutil_array_float)
    *   [`ub.agg_array_float`](#ubagg_array_float)
    *   [`ub.util_array_integer`](#ubutil_array_integer)
    *   [`ub.agg_array_integer`](#ubagg_array_integer)
    *   [`ub.util_array_merge`](#ubutil_array_merge)
    *   [`ub.agg_array_merge`](#ubagg_array_merge)
*   [JSONB Utilities](#jsonb-utilities)
    *   [`ub.util_jsonb_array`](#ubutil_jsonb_array)
    *   [`ub.agg_jsonb_array`](#ubagg_jsonb_array)
    *   [`ub.util_jsonb_concat`](#ubutil_jsonb_concat)
    *   [`ub.agg_jsonb_concat`](#ubagg_jsonb_concat)
    *   [`ub.util_jsonb_merge`](#ubutil_jsonb_merge)
    *   [`ub.agg_jsonb_merge`](#ubagg_jsonb_merge)
    *   [`ub.util_jsonb_merge_null`](#ubutil_jsonb_merge_null)
    *   [`ub.util_jsonb_multi_array`](#ubutil_jsonb_multi_array)
    *   [`ub.util_jsonb_multi_concat`](#ubutil_jsonb_multi_concat)
    *   [`ub.util_jsonb_multi_merge`](#ubutil_jsonb_multi_merge)
    *   [`ub.util_jsonb_nest`](#ubutil_jsonb_nest)
    *   [`ub.util_jsonb_unnest`](#ubutil_jsonb_unnest)
    *   [`ub.util_jsonb_update`](#ubutil_jsonb_update)
    *   [`ub.util_jsonb_differ`](#ubutil_jsonb_differ)
    *   [`ub.util_jsonb_process`](#ubutil_jsonb_process)
*   [Data Modifiers & Templating](#data-modifiers--templating)
    *   [`ub.util_data_modifier`](#ubutil_data_modifier)
    *   [`ub.util_build_template`](#ubutil_build_template)
*   [Security & Scheduling](#security--scheduling)
    *   [`ub.util_jwt`](#ubutil_jwt)
    *   [`ub.util_process_crontab`](#ubutil_process_crontab)
*   [Validation](#validation)
    *   [`ub.util_verificator`](#ubutil_verificator)

---

## Array Utilities

These functions provide advanced operations for PostgreSQL's native `double precision[]` and `integer[]` arrays.

### `ub.util_array_float`

Processes two float arrays. Arrays with different sizes are automatically expanded to the largest one.

**Parameters:**

*   **`lntotaldata double precision[]`**: Initial float array.
*   **`lnupdatedata double precision[]`**: Float array to sum up or merge with.
*   **`lcaggfunc text DEFAULT 'SUM'`**: Aggregate function to apply:
    *   `SUM`: Sum two float arrays, with adjusting their length.
    *   `MERGE`: Concatenate two float arrays.
    *   `MERGE_UNIQUE`: Merge two float arrays with eliminating duplicates.
    *   `MERGE_NNN`: Merge the second array at NNN position (e.g., `MERGE5`).

**Returns:** `double precision[]` - Processed float array.

**Examples:**

```sql
SELECT ub.util_array_float(ARRAY[0.5, 2.1], ARRAY[0.3, 3.2, 5.5], 'SUM');
-- {0.8,5.3,5.5}

SELECT ub.util_array_float(ARRAY[0.5, 2.1], ARRAY[0.3, 2.1, 5.5], 'MERGE');
-- {0.5,2.1,0.3,2.1,5.5}

SELECT ub.util_array_float(ARRAY[0.5, 2.1], ARRAY[0.3, 2.1, 5.5], 'MERGE_UNIQUE');
-- {0.3,2.1,5.5,0.5}

SELECT ub.util_array_float(ARRAY[0.5, 2.1], ARRAY[0.3, 2.1, 5.5], 'MERGE5');
-- {0.5,2.1,0,0,0.3,2.1,5.5}
```

### `ub.agg_array_float`

An aggregate function for operations with float arrays. It uses `ub.util_array_float` internally.

**Operations supported (via `lcaggfunc`):** `SUM`, `MERGE`, `MERGE_UNIQUE`, `MERGE_NNN`.

**Example (conceptual usage):**

```sql
-- Assuming a table 'my_data' with a 'values' column of type double precision[]
-- SELECT ub.agg_array_float(values, 'SUM') FROM my_data;
```

### `ub.util_array_integer`

Processes two integer arrays. Arrays with different sizes are automatically expanded to the largest one.

**Parameters:**

*   **`lntotaldata integer[]`**: Initial integer array.
*   **`lnupdatedata integer[]`**: Integer array to sum up or merge with.
*   **`lcaggfunc text DEFAULT 'SUM'`**: Aggregate function to apply:
    *   `SUM`: Sum two integer arrays, with adjusting their length.
    *   `MERGE`: Concatenate two integer arrays.
    *   `MERGE_UNIQUE`: Merge two integer arrays with eliminating duplicates.
    *   `MERGE_NNN`: Merge the second array at NNN position (e.g., `MERGE5`).

**Returns:** `integer[]` - Processed integer array.

**Examples:**

```sql
SELECT ub.util_array_integer(ARRAY[5, 2], ARRAY[3, 2, 8], 'SUM');
-- {8,4,8}

SELECT ub.util_array_integer(ARRAY[5, 2], ARRAY[3, 2, 8], 'MERGE');
-- {5,2,3,2,8}

SELECT ub.util_array_integer(ARRAY[5, 2], ARRAY[3, 2, 8], 'MERGE_UNIQUE');
-- {2,3,5,8}

SELECT ub.util_array_integer(ARRAY[5, 2], ARRAY[3, 2, 8], 'MERGE5');
-- {5,2,0,0,3,2,8}
```

### `ub.agg_array_integer`

An aggregate function for operations with integer arrays. It uses `ub.util_array_integer` internally.

**Operations supported (via `lcaggfunc`):** `SUM`, `MERGE`, `MERGE_UNIQUE`, `MERGE_NNN`.

**Example (conceptual usage):**

```sql
-- Assuming a table 'my_integer_data' with a 'counts' column of type integer[]
-- SELECT ub.agg_array_integer(counts, 'MERGE') FROM my_integer_data;
```

### `ub.util_array_merge`

Quickly merges the second float array at a specified position (`lnPosID`) of the first float array.

**Parameters:**

*   **`lntotaldata double precision[]`**: Initial float array.
*   **`lnupdatedata double precision[]`**: Float array to merge at `lnPosID`.
*   **`lnposid integer`**: Position where the second array should be inserted.

**Returns:** `double precision[]` - Merged float array.

**Examples:**

```sql
SELECT ub.util_array_merge(ARRAY[0.5, 2.1], ARRAY[0.3, 2.1, 5.5], 5);
-- {0.5,2.1,0,0,0.3,2.1,5.5}
```

### `ub.agg_array_merge`

An aggregate function to merge two float arrays at an arbitrary position. It uses `ub.util_array_merge` internally.

**Example (conceptual usage):**

```sql
-- Assuming a table 'events' with a 'measurements' column (double precision[]) and 'start_pos' (integer)
-- SELECT ub.agg_array_merge(measurements, start_pos) FROM events;
```

---

## JSONB Utilities

These functions provide powerful capabilities for manipulating and querying JSONB data, including merging, transforming, and comparing objects and arrays.

### `ub.util_jsonb_array`

Processes two JSONB arrays based on a specified merge flag.

**Parameters:**

*   **`ljinitialarray jsonb`**: Initial JSONB array.
*   **`ljmergedarray jsonb`**: JSONB array to merge with.
*   **`lcarrayflag text DEFAULT 'add'`**: How to merge arrays:
    *   `replace`: Replaces the `initial` array with the `merged` array.
    *   `expand`: Adds all new elements from the `merged` array to the `initial` array, excluding duplicates (for `number`, `string`, `boolean` types).
    *   `add`: Concatenates all elements from the `merged` array to the `initial` array (duplicates are possible). (Default)
    *   `sub`: Subtracts elements of the `merged` array from the `initial` array.
    *   `intersect`: Calculates common elements in both arrays.

**Returns:** `jsonb` - Processed JSONB array.

**Examples:**

```sql
SELECT ub.util_jsonb_array('[2,3]'::jsonb, '[4,3]'::jsonb, 'expand');
-- [2,3,4]

SELECT ub.util_jsonb_array('[2,3]'::jsonb, '[4,3]'::jsonb, 'replace');
-- [4,3]

SELECT ub.util_jsonb_array('[2,3]'::jsonb, '[4,3]'::jsonb, 'add');
-- [2,3,4,3]

SELECT ub.util_jsonb_array('[2,3,4,5]'::jsonb, '[4,3]'::jsonb, 'sub');
-- [2,5]

SELECT ub.util_jsonb_array('[2,3,4,5]'::jsonb, '[4,3,1]'::jsonb, 'intersect');
-- [3,4]
```

### `ub.agg_jsonb_array`

An aggregate function to process two JSONB arrays. It uses `ub.util_jsonb_array` internally.

**Operations supported (via `lcarrayflag`):** `replace`, `expand`, `add`, `sub`, `intersect`.

**Example (conceptual usage):**

```sql
-- Assuming a table 'log_entries' with a 'tags' column of type jsonb[]
-- SELECT ub.agg_jsonb_array(tags, 'expand') FROM log_entries;
```

### `ub.util_jsonb_concat`

Concatenates two JSONB objects, handling `NULL` values gracefully. If either input is not an object, it defaults to an empty object or returns the valid object.

**Parameters:**

*   **`ljinitialobject jsonb`**: Initial JSONB object.
*   **`ljconcatobject jsonb`**: JSONB object to concatenate.

**Returns:** `jsonb` - Concatenated JSONB object.

**Examples:**

```sql
SELECT ub.util_jsonb_concat('{"a":{"b":{"c": 1}}}'::jsonb, '{"a":{"b":{"d": 5}}}'::jsonb);
-- {"a":{"b":{"d": 5}}}

SELECT ub.util_jsonb_concat('{"a":{"b":{"c": 1}}}'::jsonb, NULL::jsonb);
-- {"a":{"b":{"c": 1}}}

SELECT ub.util_jsonb_concat(NULL::jsonb, '{"a":{"b":{"c": 1}}}'::jsonb);
-- {"a":{"b":{"c": 1}}}
```

### `ub.agg_jsonb_concat`

An aggregate function to concatenate multiple JSONB objects. It uses `ub.util_jsonb_concat` internally.

**Example (conceptual usage):**

```sql
-- Assuming a table 'config_parts' with a 'settings' column of type jsonb
-- SELECT ub.agg_jsonb_concat(settings) FROM config_parts;
```

### `ub.util_jsonb_merge`

Merges JSON objects, arrays, or any JSON type following specific rules:
*   An appropriate key in the initial object is expanded with a non-`NULL` value.
*   If an updated key ends with ".", the appropriate key in the initial object is replaced with the updated value.
*   Arrays are merged using the `lcArrayFlag` parameter.
*   `string`, `number`, `boolean` types are replaced.

**Parameters:**

*   **`ljinitialobject jsonb`**: Initial object or any other JSON type.
*   **`ljupdatedkeys jsonb`**: Object with updated keys and values, or any other JSON type to merge.
*   **`lcarrayflag text DEFAULT 'expand'`**: How to merge arrays:
    *   `expand` (default): Adds new elements, excluding duplicates.
    *   `add`: Concatenates all elements.
    *   `replace`: Replaces the array.

**Returns:** `jsonb` - Expanded/merged object or array.

**Examples:**

```sql
SELECT ub.util_jsonb_merge('{"a": {"b": {"c": 1}}}'::jsonb, '{"a": {"b": {"d": 5}}}'::jsonb, 'expand');
-- {"a": {"b": {"c": 1, "d": 5}}} (objects are expanded)

SELECT ub.util_jsonb_merge('{"a.b": {"c": 5}}'::jsonb, '{"a.b": {"d": 8}}'::jsonb, 'expand');
-- {"a.b": {"c": 5, "d": 8}} (objects are expanded)

SELECT ub.util_jsonb_merge('{"a": {"c": 5, "d": [1,2]}}'::jsonb, '{"a": {"b": 8, "d": [3,4]}}'::jsonb, 'replace');
-- {"a":{"b":8,"c":5,"d":[3,4]}} (arrays are replaced)

SELECT ub.util_jsonb_merge('{"a": {"c": 5, "d": [1,2]}}'::jsonb, '{"a": {"b": 8, "d": [3,4]}}'::jsonb, 'add');
-- {"a":{"b":8,"c":5,"d":[1,2,3,4]}} (arrays are concatenated)
```

### `ub.agg_jsonb_merge`

An aggregate function to merge two multi-level JSONB objects or arrays. It uses `ub.util_jsonb_merge` internally.

**Operations supported (via `lcarrayflag`):** `expand`, `add`, `replace`.

**Example (conceptual usage):**

```sql
-- Assuming a table 'user_profiles' with a 'profile_data' column of type jsonb
-- SELECT ub.agg_jsonb_merge(profile_data, 'expand') FROM user_profiles;
```

### `ub.util_jsonb_merge_null`

Concatenates two JSONB objects and sets any keys present only in the initial object to `NULL` in the result.

**Parameters:**

*   **`ljinitialobject jsonb`**: Initial JSONB object.
*   **`ljconcatobject jsonb`**: JSONB object to concatenate.

**Returns:** `jsonb` - Concatenated JSONB object with old keys set to `NULL` if not present in the new object.

**Examples:**

```sql
SELECT ub.util_jsonb_merge_null('{"a": 1, "b": 2}'::jsonb, '{"b": 3}'::jsonb);
-- {"a": null, "b": 3}
```

### `ub.util_jsonb_multi_array`

Processes multiple JSONB arrays, considering `NULL` values and specified merge rules.

**Parameters:**

*   **`lcarrayflag text`**: How to merge arrays:
    *   `expand`: Adds all new elements, excluding duplicates.
    *   `add`: Concatenates all elements.
    *   `sub`: Subtracts elements.
    *   `intersect`: Calculates common elements.
*   **`VARIADIC ljarray jsonb[]`**: A variadic array of JSONB arrays to process.

**Returns:** `jsonb` - Processed JSONB array.

**Examples:**

```sql
SELECT ub.util_jsonb_multi_array('expand', '[2,3]'::jsonb, '[4,3]'::jsonb, '[5,3,2]'::jsonb);
-- [2,3,4,5]

SELECT ub.util_jsonb_multi_array('add', '[2,3]'::jsonb, '[4,3]'::jsonb, '[5,3,2]'::jsonb);
-- [2,3,4,3,5,3,2]

SELECT ub.util_jsonb_multi_array('intersect', '[2,3]'::jsonb, '[4,3]'::jsonb, '[5,3,2]'::jsonb);
-- [3]
```

### `ub.util_jsonb_multi_concat`

Concatenates multiple JSONB objects, handling `NULL` values.

**Parameters:**

*   **`VARIADIC ljobject jsonb[]`**: A variadic array of JSONB objects to concatenate.

**Returns:** `jsonb` - Concatenated JSONB object.

**Examples:**

```sql
SELECT ub.util_jsonb_multi_concat('{"a": 1}'::jsonb, '{"b": 2}'::jsonb, NULL::jsonb, '{"c": 3}'::jsonb);
-- {"a": 1, "b": 2, "c": 3}
```

### `ub.util_jsonb_multi_merge`

Merges multiple JSON objects, arrays, or any JSON type following specific rules, similar to `ub.util_jsonb_merge` but for multiple inputs.

**Parameters:**

*   **`lcarrayflag text`**: How to merge arrays:
    *   `expand` (default): Adds new elements, excluding duplicates.
    *   `add`: Concatenates all elements.
    *   `replace`: Replaces the array.
*   **`VARIADIC ljdata jsonb[]`**: A variadic array of JSONB data (objects, arrays, or other types) to merge.

**Returns:** `jsonb` - Merged JSONB data.

**Examples:**

```sql
SELECT ub.util_jsonb_multi_merge('expand', '{"a": {"b": {"c": 1}}}'::jsonb, '{"a": {"b": {"d": 5}}}'::jsonb, '{"a": {"b": {"f": 5}}}'::jsonb);
-- {"a":{"b":{"c":1,"d":5,"f":5}}} (objects are expanded)

SELECT ub.util_jsonb_multi_merge('replace', '{"a": {"c": 5, "d": [1,2], "e": [2,3]}}'::jsonb, '{"a": {"b": 8, "d": [3,4]}}'::jsonb, '{"a": {"b": 8, "e": [5,6]}}'::jsonb);
-- {"a":{"b":8,"c":5,"d":[3,4],"e":[5,6]}} (arrays are replaced)

SELECT ub.util_jsonb_multi_merge('add', '{"a": {"c": 5, "d": [1,2]}}'::jsonb, '{"a": {"b": 8, "d": [3,4]}}'::jsonb, '{"a": {"b": 8, "d": [5,6]}}'::jsonb);
-- {"a":{"b":8,"c":5,"d":[1,2,3,4,5,6]}} (arrays are concatenated)
```

### `ub.util_jsonb_nest`

Converts a flattened JSONB object (e.g., `{"key1.key2.key3": value}`) into a nested JSONB object (e.g., `{"key1": {"key2": {"key3": value}}}`).

**Parameters:**

*   **`ljinitial jsonb`**: Initial JSONB object to nest.
*   **`lcarrayflag text DEFAULT 'replace'`**: How to merge arrays within the nesting process:
    *   `replace` (default): Replaces arrays.
    *   `expand`: Adds new elements, excluding duplicates.
    *   `add`: Concatenates all elements.

**Returns:** `jsonb` - Nested JSONB object.

**Examples:**

```sql
SELECT ub.util_jsonb_nest('{"a.b.c": 1, "a.b.d": 5}'::jsonb);
-- {"a":{"b":{"c":1,"d":5}}}

SELECT ub.util_jsonb_nest('{"a.b.c": [1,2], "a.b": {"c": [2,3]}}'::jsonb, 'add');
-- {"a":{"b":{"c":[2,3,1,2]}}}

SELECT ub.util_jsonb_nest('{"a.b.c": 1, "\"a.b.d\"": 5}'::jsonb);
-- {"a":{"b":{"c":1}},"a.b.d":5}
```

### `ub.util_jsonb_unnest`

Converts a nested JSONB object (e.g., `{"key1": {"key2": {"key3": value}}}`) into a flattened JSONB object (e.g., `{"key1.key2.key3": value}`). Supports custom key prefixes and delimiters.

**Parameters:**

*   **`ljinitial jsonb`**: Initial JSONB object to unnest.
*   **`lckeyprefix text DEFAULT NULL`**: Key prefix for the keys in the output object.
*   **`lcdelimiter text DEFAULT '.'`**: Delimiter to use between nested keys.

**Returns:** `jsonb` - Unnested JSONB object.

**Examples:**

```sql
SELECT ub.util_jsonb_unnest('{"a":{"b":{"c": 1, "d": 5}}}'::jsonb);
-- {"a.b.c":1,"a.b.d":5}

SELECT ub.util_jsonb_unnest('{"a":{"b":{"c": 1, "d": 5}}}'::jsonb, 'prefix_', '#');
-- {"prefix_#a#b.c":1,"prefix_#a#b.d":5}
```

### `ub.util_jsonb_update`

Updates a JSONB object with new values at specified paths. It supports various path syntaxes, including dot notation, quoted keys, and JSONPath expressions for arrays.

**Parameters:**

*   **`ljinitialobject jsonb`**: Initial JSONB object to update.
*   **`ljpathvalue jsonb`**: An object where keys represent paths and values are the new data.
    *   **Special Keys**:
        *   `"*"`: Replaces the entire initial object with its value.
        *   `"||"`: Expands the initial object using `ub.util_jsonb_merge` (replace mode).
    *   **Path Syntax**:
        *   `a.b.c`: Standard dot notation.
        *   `"a.b".c`: Quoted key (e.g., a key named "a.b").
        *   `(($.array_key[*] ? (@.id == 2)).nested_array[*] ? (@.sub_id == 1)).final_key`: JSONPath for array elements.
        *   Path ending in `null`: Deletes the element.
        *   Path with `id == 0`: Inserts a new element into an array (requires an 'id' field in the new object to be assigned automatically).

**Returns:** `jsonb` - Updated JSONB object.

**Examples:**

```sql
SELECT ub.util_jsonb_update('{"a":{"b":{"c": 1}, "f": 10}}'::jsonb, '{"a.b":{"d": 5}}'::jsonb);
-- {"a":{"b":{"d":5},"f":10}} (update at specific json path)

SELECT ub.util_jsonb_update('{"a":[{"id": 1, "b": 1}, {"id": 2, "b": [{"id": 1, "f": 3}] }]}'::jsonb, '{"(($.a[*] ? (@.id == 2)).b[*] ? (@.id == 1)).g": 10}'::jsonb);
-- {"a":[{"id":1, "b":1}, {"id":2, "b":[{"f":3,"g":10,"id":1}]}]} (update keys in the object at specific jsonpath)

SELECT ub.util_jsonb_update('{"a":[{"id": 1, "b": 1}, {"id": 2, "b": [{"id": 1, "f": 3}] }]}'::jsonb, '{"(($.a[*] ? (@.id == 2)).b[*] ? (@.id == 1))": {"id": 1, "a": 10} }'::jsonb);
-- {"a":[{"b":1, "id":1}, {"b":[{"a":10,"id":1}], "id":2}]} (replace object at specific jsonpath)

SELECT ub.util_jsonb_update('{"a":[{"id": 1, "b": 1}, {"id": 2, "b": [{"id": 1, "f": 3}] }] }'::jsonb, '{"(($.a[*] ? (@.id == 2)).b[*] ? (@.id == 0 && 17689 > 0))": {"f": 5}}'::jsonb);
-- {"a":[{"b":1,"id":1}, {"b":[{"f":3,"id":1},{"f":5,"id":2}], "id":2}]} (add object at specific jsonpath. Add a random value (e.g. 17689) to keep the whole key name unique)

SELECT ub.util_jsonb_update('{"a":[{"id": 1, "b": 1}, {"id": 2, "b": [{"id": 1, "f": 3}, {"id": 2, "f": 10}]}]}'::jsonb, '{"(($.a[*] ? (@.id == 2)).b[*] ? (@.id == 1))": null}'::jsonb);
-- {"a":[{"b":1,"id":1},{"b":[{"f":10,"id":2}],"id":2}]} (delete object at specific jsonpath)

SELECT ub.util_jsonb_update('{"a":{"b":{"c": 1}}}'::jsonb, '{"*":{"d": 5}}'::jsonb);
-- {"d": 5} (replace with a new object)

SELECT ub.util_jsonb_update('{"a":{"b":{"c": 1}}}'::jsonb, '{"||":{"a": {"b": {"d": 5}}}}'::jsonb);
-- {"a":{"b":{"c": 1, "d": 5}}} (expand the initial object)
```

### `ub.util_jsonb_differ`

Prepares a list of keys in the "updated object" that differ from the same keys in the "initial object".

**Parameters:**

*   **`ljinitialobject jsonb`**: Initial JSONB object.
*   **`ljupdatedobject jsonb`**: Updated JSONB object.
*   **`lcarrayflag text DEFAULT 'order_no_matter'`**: Flag determining how arrays are compared:
    *   `order_no_matter` (default): Order of elements in arrays does not matter for comparison.
    *   `order_matter`: Order of elements in arrays matters for comparison.

**Returns:** `jsonb` - An object containing only the keys and values from `ljupdatedobject` that are different from `ljinitialobject`. Returns `NULL` if no differences.

**Examples:**

```sql
SELECT ub.util_jsonb_differ('{"a":{"b":{"c": 1}}}'::jsonb, '{"a.b.c": 1}'::jsonb);
-- null (no new keys)

SELECT ub.util_jsonb_differ('{"a": [2, 3], "b": 10, "d": 20}'::jsonb, '{"a": [3, 4], "b": 10}'::jsonb);
-- {"a": [3, 4]}

SELECT ub.util_jsonb_differ('{"a": [2, 3], "b": 10}'::jsonb, '{"a": [3, 4], "b": 10}'::jsonb, 'order_no_matter');
-- null (no new keys)

SELECT ub.util_jsonb_differ('{"a": [2, 3], "b": 10}'::jsonb, '{"a": [3, 4], "b": 10}'::jsonb, 'order_matter');
-- {"a": [3, 4]}
```

### `ub.util_jsonb_process`

Processes a JSONB object or array with various specific rules defined by the `lcaction` parameter. This is a multi-purpose function combining several complex JSONB transformations.

**Parameters:**

*   **`ljinitialobject jsonb`**: Initial object or array.
*   **`ljprocessdata jsonb`**: Object or array with process rules (specific to each action).
*   **`lcaction text DEFAULT 'OBJECT'`**: Action to perform:
    *   `JSON_TO_PLAIN_ARRAY`: Convert any JSON with nested arrays into a plain array of objects.
    *   `JSON_DIFFERENCE`: Compare two JSON objects and build an array of objects detailing their differences.
    *   `CHILDREN_FROM_PLAIN_ARRAY`: Build a nested structure from a plain array of objects.

**Returns:** `jsonb` - Processed object or array.

---

#### `JSON_TO_PLAIN_ARRAY` action

Converts any JSON (object or array) with nested arrays into a plain array of objects, each with `prefix`, `path`, `child`, `order`, and `value` keys.

**`ljinitialobject`**: JSON object or array of objects to unnest.
**`ljprocessdata`**: (Internal usage)
    *   `path` (array): Path (text array) to JSON array to unnest.
    *   `prefix` (string): Calculated array prefix, e.g. "a:b".

**Result Structure:**

*   `prefix` (string): Key prefixes to the array, via colon.
*   `path` (array): Path (text array) to JSON array.
*   `child` (array): List of keys with "array" JSON type.
*   `order` (number): Position of the object in the array (NULL for top-level object).
*   `value` (object): Object with non-array keys.

**Example:**

```sql
SELECT ub.util_jsonb_process(
    '{ "foo": [ {"a": [{"b": 1}, {"b": 2}]} ], "bar": "info" }'::jsonb,
    NULL::jsonb,
    'JSON_TO_PLAIN_ARRAY'
);
/*
[
    { "prefix": "", "path": [], "child": ["foo"], "value": { "bar": "info" } },
    { "prefix": "foo", "path": ["foo"], "child": ["a"], "order": 1, "value": {} },
    { "prefix": "foo:a", "path": ["foo", "1", "a"], "child": [], "order": 1, "value": {"b": 1} },
    { "prefix": "foo:a", "path": ["foo", "1", "a"], "child": [], "order": 2, "value": {"b": 2} }
]
*/
```

---

#### `JSON_DIFFERENCE` action

Compares two JSON objects (or arrays of objects) and builds an array of objects detailing the differences.

**`ljinitialobject`**: "Left" JSON object or array of objects (expected values).
**`ljprocessdata`**: "Right" JSON object or array of objects (result values).

**Result Structure:**

*   `path` (array): Path to an object with differences, e.g., `["foo", "2", "bar"]`.
*   `order` (number): Array position of the object with differences.
*   `key` (string): Object key with differences.
*   `expected` (any): Expected value.
*   `result` (any): Result value.
*   `expectedType` (string|null): Type of the expected value (if types differ).
*   `resultType` (string|null): Type of the result value (if types differ).

**Example:**

```sql
SELECT ub.util_jsonb_process(
    '{"a": [2, 3], "b": [{"f": 10}, {"f": 20}], "d": 20}'::jsonb,
    '{"a": [3, 4], "b": [{"f": 20}, {"f": 30}], "d": 20}'::jsonb,
    'JSON_DIFFERENCE'
);
/*
[
    {"key":"a", "path":[], "order":null, "result":[3,4], "expected":[2,3]},
    {"key":"f", "path":["b"], "order":1, "result":20, "expected":10},
    {"key":"f", "path":["b"], "order":2, "result":30, "expected":20}
]
*/
```

---

#### `CHILDREN_FROM_PLAIN_ARRAY` action

Builds a nested structure from a plain array of objects like `{ "key", "value", "parentKey" }`.

**`ljinitialobject`**: Array of objects to nest. Each object should have:
    *   `key` (number|string|object): Unique key of the object.
    *   `value` (object): List of keys with values to store (this object will be the content of the node).
    *   `parentKey` (number|string|object|null): Parent key of the object (`NULL` for root elements).
**`ljprocessdata`**: Settings for the action.
    *   `childKey` (string|null): Key name for the array with children data (defaults to `"children"`).
    *   `parentKey` (string|null): Parent key value to build data (used for recursion).
    *   `preparedKeys` (array|null): List of already prepared parent keys to exclude loops (used for recursion).

**Returns:** `jsonb` - JSON with children data, typically an array of objects.

**Example:**

```sql
SELECT ub.util_jsonb_process(
    '
        [
            { "key": "01", "value": { "info": "foo" }, "parentKey": null },
            { "key": "02", "value": { "info": "bar" }, "parentKey": "01" },
            { "key": "03", "value": { "info": "baz" }, "parentKey": "02" }
        ]
    '::jsonb,
    NULL::jsonb,
    'CHILDREN_FROM_PLAIN_ARRAY'
);
-- [{"info": "foo", "children": [{"info": "bar", "children": [{"info": "baz"}]}]}]
```

---

## Data Modifiers & Templating

These functions provide tools for modifying data types and values, and for building dynamic text templates.

### `ub.util_data_modifier`

Processes a given value with a set of modifier rules, allowing for type conversion, formatting, validation, and string transformations.

**Parameters:**

*   **`ljinput jsonb`**: An object containing:
    *   **`value`**: The initial value to be processed (can be any JSON type).
    *   **`modifier`**: A JSONB array of modifier rules, each a JSONB object with the following potential keys:
        *   `type` (string): Type of modification (`f_...` for format, `v_...` for validate, `s_...` for string, `a_...` for array).
        *   `format` (string): Any valid format for the type (e.g., date formats, `password` for string, `number:<true_val>:<false_val>` for checkbox).
        *   `delimiter` (string): Delimiter to split/aggregate strings from arrays/objects (e.g., `\n` for newline).
        *   `timeZone` (string): 3-letter time zone for timestamp calculations (e.g., `GMT`).
        *   `pretty` (number): `1` to apply `jsonb_pretty()` (for `f_object`, `f_array`).
        *   `strip` (number): `1` to apply `jsonb_strip_nulls()` (for `f_object`, `f_array`).
        *   `regex` (string): Regular expression for string transformations.
        *   `from` (string): Regex pattern to replace (for `s_regex_replace`).
        *   `to` (string): Replacement string (for `s_regex_replace`).
        *   `flag` (string): Regex flags (e.g., `g`) (for `s_regex_replace`).
        *   `btrim` (string): Characters to trim (for `s_btrim`).
        *   `default` (string): Default value for `s_nulls_to_string`.
        *   `validator` (object|null): Validation rules for `v_` types:
            *   `jsonpath` (string): JSONPath expression (e.g., `"$ > 0 && $ < 100"`).
            *   `maxLength` (number): Maximal length of the value.

**Returns:** `jsonb` - A JSONB object:
*   `result` (any): The modified value if successful.
*   `message` (string): Details on invalid input if validation fails (e.g., `invalid_number`, `invalid_jsonpath`, `invalid_max_length`, `invalid_date`, `invalid_datetime`, `invalid_object`, `invalid_array`).
*   `error` (object): Contains `code`, `message`, and `details` if an internal error occurs.

**Examples:**

```sql
-- Example: format a number
SELECT ub.util_data_modifier(jsonb_build_object(
    'value', '25',
    'modifier', '[{"type": "f_number", "format": "FM999,999.00"}]'::jsonb
));
-- { "result": "25.00" }

-- Example: convert and format date from date_2000
SELECT ub.util_data_modifier(jsonb_build_object(
    'value', '7300',
    'modifier', '[{"type": "f_date", "format": "DD Mon YYYY"}]'::jsonb
));
-- { "result": "27 Dec 2019" }

-- Example: split array into elements
SELECT ub.util_data_modifier(jsonb_build_object(
    'value', jsonb_build_array('foo', 'bar'),
    'modifier', '[{"type": "f_string", "delimiter": "\n"}]'::jsonb
));
-- { "result": "foo\nbar" }

-- Example: "p1d" period into seconds
SELECT ub.util_data_modifier(jsonb_build_object(
    'value', 'p1w',
    'modifier', '[{"type": "f_period"}]'::jsonb
));
-- { "result": 604800 }

-- Example: validate text using jsonpath expression
SELECT ub.util_data_modifier(jsonb_build_object(
    'value', 'my text',
    'modifier', '[{"type": "v_string", "validator": {"jsonpath": "$ like_regex \"^a\""}}]'::jsonb
));
-- { "message": "invalid_jsonpath" }
```

Okay, I can definitely help with that! You've provided the text content of the table, which I can now process.

Here's the Markdown version of the table from your `data_modifier` file:

```markdown
| Type             | Description                                                                                     | Parameters                                                                                                                    |
| :--------------- | :---------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------- |
| `f_number`       | convert data to "number" format                                                                 | `"format":` any format for "number" type                                                                                      |
| `f_checkbox`     | convert true / false to any values                                                              | `"format":` "number<:true><:false>" \| "boolean<:true><:false>"                                                              |
| `f_string`       | convert data to "string" format                                                                 | `"format":` "password"<br>`"delimiter"` => build string with delimiters from array \| object                                   |
| `f_object`       | convert data to "object" format                                                                 | `"pretty":` 1 => jsonb_pretty()<br>`"strip":` 1 => strip nulls                                                               |
| `f_array`        | convert data to "array" format                                                                  | `"pretty":` 1 => jsonb_pretty()<br>`"strip":` 1 => strip nulls                                                               |
| `f_date`         | convert date_iso \| date_2000 to "date" format                                                  | `"format":` any format for "date" type<br>`"null_to_1970":` null date=>1970-01-01                                           |
| `f_date_iso`     | convert date from any format to YYYY-MM-DD                                                      | `"format":` any format for "date" type<br>`"null_to_1970":` null date=>1970-01-01                                           |
| `f_timestamp`    | convert data to "timestamp" format                                                              | `"format":` any format for "timestamp" type<br>`"timeZone":` "3-letters timezone"<br>`"null_to_1970":` null date=>1970-01-01 |
| `f_date_2000`    | convert to "date_iso" format, then calculate amount of days after 2000-01-01                    | `"format":` any format for "date" type                                                                                      |
| `f_unix_timestamp`| convert data to UNIX timestamp format (seconds since 1970-01-01)                              | `"format":` any format for "timestamp" type                                                                                 |
| `f_period`       | convert "period" format to amount of seconds                                                    | (No specific parameters listed in the source text)                                                                            |
| `s_lower`<br>`s_upper`<br>`s_initcap` | string transformation                                                           | (No specific parameters listed in the source text)                                                                            |
| `s_btrim`        | btrim operation                                                                                 | `"btrim":` symbols to trim                                                                                                    |
| `s_regexp_match` | regex transformation                                                                            | `"regex":` regex expression                                                                                                   |
| `s_regex_replace`| regex transformation                                                                            | `"from"`, `"to"`, `"flag":`<br>regexp_replace() parameters                                                                   |
| `s_split`        | string to array using regex                                                                     | `"regex":` regex expression                                                                                                   |
| `s_nulls_to_string` | convert all nulls to strings                                                                  | `"default":` <any value> ("" by default)                                                                                     |
| `v_number` \| `v_string` \| `v_date_2000` \| … | convert and validate field type                                                 | `"format":` "<any data format>"<br>`"validator": { "jsonpath": "$ > 0 && $ < 20", "maxLength": 256 }`                        |
```

### `ub.util_build_template`

Converts a template string with placeholders (`{$.<key>}`) and control statements (`{$if:}`, `{$for:}`) into a final text using parameters from a `sourceMapping` object. Supports nested statements.

**Parameters:**

*   **`ljinput jsonb`**: An object containing:
    *   **`template` (string)**: The template string with `{$<statement>}` and `{$.<key>}` insertions.
    *   **`sourceMapping` (object|array)**: Source data to use for the template processing (key-value pairs for placeholders).
    *   **`data` (array|null)**: (Internal use for recursive calls) Array of statements and data within them.
    *   **`firstRow` (number|null)**: (Internal use for recursive calls) Initial row to process `data` (starting with 0).

**Template Syntax & Statements:**

*   **`{$.<key>}`**: Inserts the value of `<key>` from `sourceMapping`. Supports default values: `{$.<key>:<default_value>}`.
*   **`{$if:<jsonpath condition>}`**: Conditional block. If `jsonpath condition` evaluates to true against `sourceMapping`, the block content is processed.
*   **`{$elseif:<jsonpath condition>}`**: Alternative conditional block after an `if`.
*   **`{$else}`**: Default block if no previous `if` or `elseif` matched.
*   **`{$for:<array key>}`**: Loop through elements of an array specified by `array key` (JSONPath). Inside the loop, `sourceMapping` is updated with the current array element, allowing access like `{$.array_name.element_key}`.
*   **`{$end}`**: Ends an `if` or `for` statement block.

**Returns:** `jsonb` - A JSONB object:
*   `result` (string): The processed template text.
*   `lastRow` (number|null): Last processed row (for internal/recursive calls).
*   `error` (object): Contains `code`, `message`, and `details` if an internal error occurs.

**Examples:**

```sql
SELECT ub.util_build_template(jsonb_build_object(
    'template',
    '
        {$if:$.a == 2}
            First
            {$if:$.a == 1}
                Case 1
            {$elseif:$.a == 3}
                Case 3
            {$elseif:$.a == 2}
                Case
                {$if:$.b > 2}
                    check
                {$end}
                2
            {$else}
                Unknown case
            {$end}
        {$elseif:$.a == 1}
            First 2
        {$else}
            Unknown case
        {$end}
        My template
        {$.c + $.b}
        Template text
        {$if:$.c == 2}
            {$for:$.v1[*]}
                Cycle
                {$if:$.a == 2}
                    {$.v1.n}
                {$else}
                    {$.v1.m}
                {$end}
                demo
            {$end}
        {$end}
        Template bottom
        {$if:$.b > 0 && $.c == 2}
            Test
            {$if:$.a == 1}
                Case 1
            {$elseif:$.a == 3}
                Case 3
            {$elseif:$.a == 2}
                Case
                {$if:$.b > 2}
                    check
                {$end}
                2
            {$else}
                Unknown case
            {$end}
            text
        {$end}
    ',
    'sourceMapping',
    '{
        "a":2,
        "b":3,
        "c":2,
        "v1": [
            {"m":"text1","n":"next1"},
            {"m":"text2","n":"next2"}
        ]
    }'::jsonb
));
/*
"
        First
            Case
                check
                2
        My template
        5
        Template text
                Cycle
                    next1
                demo
                Cycle
                    next2
                demo
        Template bottom
        Test
            Case
                check
                2
            text"
*/
```

---

## Security & Scheduling

Functions related to JWT (JSON Web Tokens) and cron-like scheduling.

### `ub.util_jwt`

Provides utilities for JWT (JSON Web Token) processing. **Requires the `pgcrypto` extension.**

**Parameters:**

*   **`ljinput jsonb`**: An object containing:
    *   **`mode` (string)**: Operation mode:
        *   `jwt_sign`: Signs a JWT based on JSON payload, secret key, and algorithm.
        *   `jwt_verify`: Verifies a JWT string and returns its header, payload, and validity status.
        *   `jwt_encode`: Encodes a bytea value into base64url format (used internally).
        *   `jwt_decode`: Decodes a base64url string into bytea (used internally).
        *   `jwt_generate`: Generates a JWT signature for a given value using a secret and algorithm (used internally).
    *   **`jwtValue` (string)**: The JWT string or base64 string, depending on the mode.
    *   **`jwtPayload` (object)**: The JSON object to be used as the JWT payload (for `jwt_sign`).
    *   **`jwtSecret` (string)**: The secret key for signing/verification.
    *   **`jwtAlgorithm` (string)**: The encryption algorithm (`sha256` (default), `sha384`, `sha512`).
    *   **`sessionData` (object)**: (Optional) Any session-related data.

**Returns:** `jsonb` - An object containing:
*   `jwtValue` (string): The resulting JWT string or base64 encoded value.
*   `jwtPayload` (object): The decoded JWT payload (for `jwt_verify`).
*   `jwtHeader` (string): The decoded JWT header (for `jwt_verify`).
*   `jwtValid` (number): `1` if the JWT is valid, `0` otherwise (for `jwt_verify`).

**Examples:**

```sql
SELECT ub.util_jwt('{"mode": "jwt_sign", "jwtPayload": {"a": 1}, "jwtSecret": "1234"}');
-- { "jwtValue": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhIjogMX0.3aKAFdFca4DozVrKxqgcGPZik8erGRtdbTipg8Hk9Ao" }

SELECT ub.util_jwt('{"mode": "jwt_verify", "jwtValue": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhIjogMX0.3aKAFdFca4DozVrKxqgcGPZik8erGRtdbTipg8Hk9Ao", "jwtSecret": "1234"}');
/*
{
    "jwtValid": 1,
    "jwtHeader": {"alg": "HS256", "typ": "JWT" },
    "jwtPayload": { "a": 1}
}
*/
```

### `ub.util_process_crontab`

Calculates the next UNIX timestamp based on a crontab expression.

**Parameters:**

*   **`crontab_expr text`**: The crontab expression string. Positions are:
    1.  Seconds
    2.  Minutes
    3.  Hours
    4.  Day of month
    5.  Month
    6.  Day of week
    Allowed special characters: `,`, `-`, `*`, `/`.

**Returns:** `double precision` - UNIX timestamp (at UTC) of the next event, or `NULL` if the expression is invalid.

**Examples:**

```sql
SELECT ub.util_process_crontab('5 * * * * * '); -- every minute at 5th second
-- 1762090925 (value depends on the current timestamp)

SELECT ub.util_process_crontab('*/5 * * * * * '); -- every 5 seconds
-- 1762092675 (value depends on the current timestamp)

SELECT ub.util_process_crontab('* * 5,10,15 * * *'); -- every day at 5 a.m., 10 a.m. and 3 p.m. UTC
-- 1762092600 (value depends on the current timestamp)

SELECT ub.util_process_crontab('* * 3 * * MON,WED,FRI'); -- at 3 a.m. on Monday, Wednesday and Friday
-- 1762092600 (value depends on the current timestamp)
```

---

## Validation

A universal function for validating various data types and formats.

### `ub.util_verificator`

A universal verificator to check if an input text matches a specified type or format.

**Parameters:**

*   **`lcinput text`**: The input text to verify.
*   **`lcformat text`**: The type or format to validate against:
    *   `JSON`: General JSON format.
    *   `JSONOBJECT`: JSON object format.
    *   `JSONARRAY`: JSON array format.
    *   `JSONPATH`: JSONPath expression format.
    *   `TSQUERY`: Text search query format.
    *   `DATE_<format>`: Date in a specified format (e.g., `DATE_YYYY-MM-DD`).
    *   `DATETIME_<format>`: Datetime in a specified format (e.g., `DATETIME_YYYY-MM-DD HH24:MI:SS`).
    *   `TIME`: Time format.
    *   `BASE64`: Base64 encoded string format.
    *   `EMAIL`: Email address format.
    *   `URL`: URL format.
    *   `EXT`: Checks if a PostgreSQL extension (named `lcinput`) is installed.

**Returns:** `text` - `"TRUE"` if the input is valid for the given format, `"FALSE"` otherwise.

**Examples:**

```sql
SELECT ub.util_verificator('[{2,3]', 'JSON');
-- FALSE

SELECT ub.util_verificator('[2,3]', 'JSONARRAY');
-- TRUE

SELECT ub.util_verificator('12.10.2020', 'DATE_YYYY-MM-DD');
-- FALSE

SELECT ub.util_verificator('https://google.com', 'URL');
-- TRUE

SELECT ub.util_verificator('$.a = "check', 'JSONPATH');
-- FALSE
```
```
