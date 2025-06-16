-- FUNCTION: ub.util_jsonb_process(jsonb, jsonb, text)

-- DROP FUNCTION IF EXISTS ub.util_jsonb_process(jsonb, jsonb, text);

CREATE OR REPLACE FUNCTION ub.util_jsonb_process(
    ljinitialobject jsonb,
    ljprocessdata jsonb,
    lcaction text DEFAULT 'OBJECT'::text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_jsonb_process
@desc Process a json objects / array with rules, described in the third parameter
@desc Aggregate function ub.agg_jsonb_process(ljProcessData, lcAction) is defined too

@param object|array $1  - initial object or array
@param object|array $2 - object or array with process rules
@params text $3 -
    - JSON_TO_PLAIN_ARRAY: convert any json with nested arrays into plain array of objects with "path", "prefix", "value" keys
    - JSON_DIFFERENCE: compare two json objects with building array of objects with differences
    - CHILDREN_FROM_PLAIN_ARRAY: build nested structure from plain array: [ { "key", "value", "parentKey" }] => { "value", "children": [ { "value", "children": [..] } ] }

@return object|array - processed object or array

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#array #object #nest object #unnest object #compare object #expand array #plain array #json aggregate #object aggregate
*/  
    
DECLARE

    -- Exception diagnostics
    lcArrayPrefix           text;                               -- calculated array prefix, e.g. "a:b"
    ljArrayPath             jsonb;                              -- calculated array path, e.g. ["a", "2", "b", "3"]
    lnArrayPosFlag          integer := 1;                       -- 1 = add array position
    lnArrayPos              integer;                            -- array position to process
  
    lcKeyParent             text;                               -- parent key to build array of objects
    lcKeyChild              text;                               -- child key to build array of objects
    ljKeyPrepared           jsonb;                              -- array of prepared parent keys
  
BEGIN

    lcAction := upper(lcAction);
    
    CASE
  
        WHEN lcAction = 'JSON_TO_PLAIN_ARRAY' THEN
        
            /*
            @desc **JSON_TO_PLAIN_ARRAY**
            @desc Convert any json with nested arrays into plain array with "prefix", "path", "child", "order", "value" keys
            
            @param array ljInitialObject - json object or array of objects to unnest
            @param null ljProcessData - for internal usage only
                @key array path - path (text array) to json array to unnest
            @param string lcAction - "JSON_TO_PLAIN_ARRAY" action
            
            @result array - plain array with objects consisting of "prefix", "path", "child", "order", "value keys only
                @key string prefix - key prefixes to the array, via colon
                @key array path - path (text array) to json array to unnest
                @key array child - list of keys with "array" json type
                @key number order - position of the object in the array
                @key object value - object with non-array keys
            #json #object #array #plain structure
            */
    
            IF jsonb_typeof(ljInitialObject) IS DISTINCT FROM 'array' THEN
            
                -- convert object to array for further processing
                ljInitialObject := jsonb_build_array(ljInitialObject);
                ljArrayPath := jsonb_build_array();
                lcArrayPrefix := '';
                lnArrayPosFlag := 0;
                
            ELSE
            
                -- recursive call => calculate array path & prefix
                ljArrayPath := 
                    CASE
                        WHEN jsonb_typeof(ljProcessData->'path') IS DISTINCT FROM 'array'
                        THEN jsonb_build_array()
                        ELSE ljProcessData->'path'
                    END;
                lcArrayPrefix := COALESCE(ljProcessData->>'prefix', '');
            
            END IF;

    
            RETURN COALESCE(
            (
                WITH
                -- unnest input array with assigning array positions
                "unnest_input_array" AS MATERIALIZED (
                    SELECT
                        ROW_NUMBER() OVER() AS array_position,
                        ub.util_jsonb_unnest(object_data) AS object_data
                    FROM jsonb_array_elements(ljInitialObject) object_data
                    WHERE jsonb_typeof(object_data) IS NOT DISTINCT FROM 'object'
                ),
                
                -- unnest each object recursively using "JSON_TO_PLAIN_ARRAY" mode
                "unnest_input_object" AS MATERIALIZED (
                    SELECT
                        jsonb_array_elements(
                            ub.util_jsonb_array(
                            (   -- build an object with all keys except arrays of objects
                                SELECT
                                    jsonb_build_array(
                                        jsonb_build_object(
                                            -- prefix, e.g. foo:a
                                            'prefix', lcArrayPrefix,
                                            -- path, e.g. ["foo", "1", "a"]
                                            'path',   ljArrayPath,
                                            -- position in the parent array (NULL for top-level object)
                                            'order',
                                                CASE
                                                    WHEN lnArrayPosFlag = 1
                                                    THEN "unnest_input_array".array_position
                                                    ELSE NULL::integer
                                                END,
                                            -- select all non "array of objects" keys
                                            'value', COALESCE(
                                                (
                                                    SELECT 
                                                        jsonb_object_agg("key_value".key, "key_value".value)
                                                    FROM jsonb_each("unnest_input_array".object_data) AS key_value(key, value)
                                                    WHERE 
                                                        jsonb_typeof("key_value".value) IS DISTINCT FROM 'array'
                                                        OR jsonb_array_length("key_value".value) = 0
                                                        OR jsonb_typeof("key_value".value->0) IS DISTINCT FROM 'object'
                                                ),
                                                jsonb_build_object()
                                            ),
                                            -- build list of "array of objects" keys
                                            'child', COALESCE(
                                                (
                                                    SELECT 
                                                        jsonb_agg("key_value".key)
                                                    FROM jsonb_each("unnest_input_array".object_data) AS key_value(key, value)
                                                    WHERE 
                                                        jsonb_typeof("key_value".value) IS NOT DISTINCT FROM 'array'
                                                        AND jsonb_array_length("key_value".value) > 0
                                                        AND jsonb_typeof("key_value".value->0) IS NOT DISTINCT FROM 'object'
                                                ),
                                                jsonb_build_array()
                                            )
                                        )
                                  )
                            ),
                            -- unnest each array of objects recursively using "JSON_TO_PLAIN_ARRAY" mode
                            (
                                SELECT
                                    ub.agg_jsonb_array(
                                        ub.util_jsonb_process(
                                            "array_key_value".value, 
                                            jsonb_build_object(
                                                -- add position and array key to the path
                                                'path',  
                                                    ljArrayPath
                                                    ||
                                                    CASE
                                                        WHEN lnArrayPosFlag = 1
                                                        THEN jsonb_build_array("unnest_input_array".array_position::text)
                                                        ELSE jsonb_build_array()
                                                    END
                                                    ||
                                                    jsonb_build_array("array_key_value".key),
                                                -- key prefix, with ":" delimiter
                                                'prefix', concat((nullif(lcArrayPrefix, '') || ':'), "array_key_value".key)
                                            ),
                                            'JSON_TO_PLAIN_ARRAY'
                                        ),
                                        'add'
                                    )
                                FROM jsonb_each("unnest_input_array".object_data) AS array_key_value(key, value)
                                WHERE jsonb_typeof("array_key_value".value) IS NOT DISTINCT FROM 'array'
                                    AND jsonb_array_length("array_key_value".value) > 0
                                    AND jsonb_typeof("array_key_value".value->0) IS NOT DISTINCT FROM 'object'
                            ), 
                            'add'
                        )) AS object_details
                    FROM "unnest_input_array"
                )
                
                -- build response as a plain array
                SELECT
                    jsonb_agg(
                        "unnest_input_object".object_details
                    )
                FROM "unnest_input_object"
                WHERE "unnest_input_object".object_details->>'prefix' IS NOT NULL
           ),
           jsonb_build_array());
        
        
        
        
        WHEN lcAction = 'JSON_DIFFERENCE' THEN

            /*
            @desc **JSON_DIFFERENCE**
            @desc Compare two json objects with building array of objects with differences
            
            @param object|array ljInitialObject - "left" json object or array of objects (expected values)
            @param object|array ljProcessData - "right" json object or array of objects (result values)
            @param string lcAction - "JSON_DIFFERENCE" action
            
            @return object|array - array with details on differences
                @key array path - path to an object with differences, e.g. ["foo", "2", "bar"]
                @key number order - array position of the object with differences
                @key string key - object key with differences
                @key number|string|object|array expected - expected value
                @key number|string|object|array result - result value
                @key string|null expectedType - type of the expected value
                @key string|null resultType - type of the result value
            #json #object #array #compare #difference
            */
            
            RETURN (
                WITH
                -- convert "left" object into plain array using "JSON_TO_PLAIN_ARRAY" mode
                "left_object" AS MATERIALIZED (
                    SELECT
                        object_data->'path'   AS array_path,
                        object_data->>'order' AS array_pos,
                        (jsonb_each(object_data->'value')).key   AS object_key,
                        (jsonb_each(object_data->'value')).value AS object_value
                    FROM jsonb_array_elements(
                        ub.util_jsonb_process(
                            ljInitialObject, NULL::jsonb, 'JSON_TO_PLAIN_ARRAY'
                        )
                    ) object_data
                    WHERE jsonb_typeof(object_data->'value') IS NOT DISTINCT FROM 'object'
                ),

                -- convert "right" object into plain array using "JSON_TO_PLAIN_ARRAY" mode
                "right_object" AS MATERIALIZED (
                    SELECT
                        object_data->'path'   AS array_path,
                        object_data->>'order' AS array_pos,
                        (jsonb_each(object_data->'value')).key   AS object_key,
                        (jsonb_each(object_data->'value')).value AS object_value
                    FROM jsonb_array_elements(
                        ub.util_jsonb_process(
                            ljProcessData, NULL::jsonb, 'JSON_TO_PLAIN_ARRAY'
                        )
                    ) object_data            
                    WHERE jsonb_typeof(object_data->'value') IS NOT DISTINCT FROM 'object'
                ),
                
                -- compare each key in both objects
                "compare_object" AS MATERIALIZED (
                    SELECT
                        COALESCE("left_object".array_path, "right_object".array_path) AS array_path,
                        COALESCE("left_object".array_pos,  "right_object".array_pos)::integer AS array_pos,
                        COALESCE("left_object".object_key, "right_object".object_key) AS object_key,
                        "left_object".object_value  AS left_value,
                        "right_object".object_value AS right_value
                    FROM "left_object"
                    FULL JOIN "right_object" ON
                        "right_object".array_path = "left_object".array_path
                        AND "right_object".array_pos IS NOT DISTINCT FROM "left_object".array_pos
                        AND "right_object".object_key = "left_object".object_key
                    WHERE "left_object".object_value IS DISTINCT FROM "right_object".object_value
                )
                
                -- Build array of differences
                SELECT
                    jsonb_agg(
                        -- compare json values
                        jsonb_build_object(
                            'path',  "compare_object".array_path,
                            'order', "compare_object".array_pos,
                            'key',   "compare_object".object_key,
                            'expected',  "compare_object".left_value,
                            'result',    "compare_object".right_value
                        )
                        ||
                        -- compare difference on json value types too
                        CASE
                            WHEN
                                jsonb_typeof("compare_object".left_value) IS NOT NULL
                                AND jsonb_typeof("compare_object".right_value) IS NOT NULL
                                AND jsonb_typeof("compare_object".left_value) IS DISTINCT FROM jsonb_typeof("compare_object".right_value)
                            THEN jsonb_build_object(
                                    'expectedType', jsonb_typeof("compare_object".left_value),
                                    'resultType',   jsonb_typeof("compare_object".right_value)
                                )
                            ELSE jsonb_build_object()
                        END
                        ORDER BY "compare_object".array_path, "compare_object".array_pos, "compare_object".object_key
                    )
                FROM "compare_object"
            );        
    
    

        WHEN lcAction = 'CHILDREN_FROM_PLAIN_ARRAY' THEN
        
            /*
            @desc **CHILDREN_FROM_PLAIN_ARRAY**
            @desc Build nested structure from plain array of { "key", "value", "parentKey" } objects
            
            @param array ljInitialObject  - array of objects to nest
                @key number|string|object key - unique key of the object
                @key object value - list of keys with values to store
                @key number|string|object|null parentKey - parent key of the object
            @param null ljProcessData - settings
                @key string|null childKey - key name for the array with children data ("children" by default)
                @key string|null parentKey - parent key value to build data
                @key array|null preparedKeys - list of already prepared parent keys to exclude loops
            @param string lcAction - "CHILDREN_FROM_PLAIN_ARRAY" action
            
            @return object|array - json with children data
            #json #object #array #build #child #children
            */
            
            lcKeyParent := ljProcessData->>'parentKey';
            lcKeyChild := COALESCE(ljProcessData->>'childKey', 'children');
            ljKeyPrepared := COALESCE(ljProcessData->'preparedKeys', jsonb_build_array());
            
            RETURN (
                WITH
                -- unnest the array
                "unnest_data" AS MATERIALIZED (
                    SELECT
                        value->>'parentKey' AS parent_key,
                        value AS object_data
                    FROM jsonb_array_elements(ljInitialObject)
                    WHERE jsonb_typeof(ljInitialObject) IS NOT DISTINCT FROM 'array'
                )
                -- recursively build array of objects for specified parent key
                SELECT
                    jsonb_agg(
                        COALESCE("unnest_data".object_data->'value', jsonb_build_object())
                        ||
                        CASE
                            WHEN EXISTS 
                                (
                                    SELECT 1 
                                    FROM "unnest_data" child_data 
                                    WHERE "child_data".parent_key = "unnest_data".object_data->>'key'
                                )
                            THEN jsonb_build_object(
                                lcKeyChild, 
                                ub.util_jsonb_process(
                                    ljInitialObject, 
                                    jsonb_build_object(
                                        'childKey', lcKeyChild, 
                                        'parentKey', "unnest_data".object_data->>'key',
                                        'preparedKeys', ub.util_jsonb_array(
                                            ljKeyPrepared,
                                            jsonb_build_array(lcKeyParent),
                                            'add'
                                        )
                                    ),
                                    'CHILDREN_FROM_PLAIN_ARRAY'
                                )
                            )
                            ELSE jsonb_build_object()
                        END
                    )
                FROM "unnest_data"
                WHERE "unnest_data".parent_key IS NOT DISTINCT FROM lcKeyParent
                    AND NOT ljKeyPrepared ? ("unnest_data".object_data->>'key')
            );
    
    
    
        -- unknown function => return the initial object
        ELSE

            RETURN ljInitialObject;

    END CASE;

END;
/*
@example:
    SELECT ub.util_jsonb_process(
        '{ "foo": [ {"a": [{"b": 1}, {"b": 2}]} ], "bar": "info" }'::jsonb,
        NULL::jsonb,
        'JSON_TO_PLAIN_ARRAY'
    )
    =>
    [
        { "prefix": "", "path": [], "child": ["foo"], "value": { "bar": "info" } },
        { "prefix": "foo", "path": ["foo"], "child": ["a"], "order": 1, "value": {} },
        { "prefix": "foo:a", "path": ["foo", "1", "a"], "child": [], "order": 1, "value": {"b": 1} },
        { "prefix": "foo:a", "path": ["foo", "1", "a"], "child": [], "order": 2, "value": {"b": 2} }
    ]
    
    SELECT ub.util_jsonb_process(
        '{"a": [2, 3], "b": [{"f": 10}, {"f": 20}], "d": 20}'::jsonb, 
        '{"a": [3, 4], "b": [{"f": 20}, {"f": 30}], "d": 20}'::jsonb,
        'JSON_DIFFERENCE'
    )
    =>
    [
        {"key":"a", "path":[], "order":null, "result":[3,4], "expected":[2,3]},
        {"key":"f", "path":["b"], "order":1, "result":20, "expected":10},
        {"key":"f", "path":["b"], "order":2, "result":30, "expected":20}]
    ]
    
    SELECT ub.util_jsonb_process(
        '
            [
                { "key": "01", "value": { "info": "foo" }, "parentKey": null },
                { "key": "02", "value": { "info": "bar" }, "parentKey": "01" },
                { "key": "03", "value": { "info": "baz" }, "parentKey": "02" }
            ]        
        '::jsonb, 
        NULL::jsonb,
        'CHILDREN_FROM_PLAIN_ARRAY')
    => { "info": "foo", "children": [{ "info": "bar", "children": [{ "info": "baz" }] }] }
*/
$BODY$;

ALTER FUNCTION ub.util_jsonb_process(jsonb, jsonb, text)
    OWNER TO postgres;
