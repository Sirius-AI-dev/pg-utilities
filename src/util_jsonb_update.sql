-- FUNCTION: ub.util_jsonb_update(jsonb, jsonb)

-- DROP FUNCTION IF EXISTS ub.util_jsonb_update(jsonb, jsonb);

CREATE OR REPLACE FUNCTION ub.util_jsonb_update(
    ljinitialobject jsonb,
    ljpathvalue jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_jsonb_update
@desc update object with new values at specified paths
@desc basic rules for json keys:
    - [a-zA-Z0-9_]{1,}.* - parent and child key(s)
    - "<key_name>" - process key name within quotes as one key (ignore ".", any symbols are allowed)
    - (<jsonpath>) - process specific object (or its key) in array

@param object $1 - initial object
@param object $2 - object with paths and values
    @key any <path> - new value to plase at the <path>
    
@return object $1 - updated object

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#object #update object #object path
*/

DECLARE

    -- value position in calculated array
    JSON_KEY_TYPE       CONSTANT integer := 1;
    PARENT_KEY_POS      CONSTANT integer := 2;
    CHILD_KEY_POS       CONSTANT integer := 3;
    ARRAY_JSONPATH_POS  CONSTANT integer := 4;
    
    
BEGIN

    -- Nothing to update => return initial object
    IF jsonb_typeof(ljPathValue) IS DISTINCT FROM 'object' THEN

        RETURN
            CASE
                WHEN jsonb_typeof(ljInitialObject) IS DISTINCT FROM 'object'
                THEN jsonb_build_object()
                ELSE ljInitialObject
            END;
        
    -- Replace the initial object
    ELSEIF ljPathValue->>'*' IS NOT NULL THEN
    
        RETURN ljPathValue->'*';
    
    -- Expand the initial object
    ELSEIF ljPathValue->>'||' IS NOT NULL THEN
    
        RETURN ub.util_jsonb_merge(
            ljInitialObject,
            ljPathValue->'||',
            'replace'
        );
        
    END IF;
    
    
    
    -- Build response
    RETURN ub.util_jsonb_concat(
        
        -- initial object
        ljInitialObject,
        
        -- concatenate with updated keys 
        (
            WITH
            -- unnest all keys and calculate parent key, child key and array clause
            "parent_child" AS MATERIALIZED (
                SELECT
    
                    -- calculate parent key, child key and array clause
                    CASE
                        -- [a-zA-Z0-9_]{1,}.* format
                        WHEN "json_path".key ~ '^[a-zA-Z0-9_]' THEN
                            ARRAY[
                                -- type = "basic"
                                'basic',
                                -- parent key: a."b.c" => a
                                (regexp_match("json_path".key, '^[^\.]{1,}'))[1],
                                -- child key: a."b.c" => "b.c"
                                nullif(regexp_replace("json_path".key, '^[^\.]{1,}\.{0,1}', ''), ''),
                                -- array clause
                                NULL::text
                            ]
                                
                        -- ../ format (relative path)
                        WHEN "json_path".key ~ '^\.\.' THEN
                            ARRAY[
                                -- type = "basic"
                                'basic',
                                -- parent key: ../../a.b => ../../a
                                regexp_replace("json_path".key, '\.(?![\.\/]).*', ''),
                                -- child key: ../../a.b => b
                                (regexp_match("json_path".key, '(?<=\.(?=([^\.\/]))).*'))[1],
                                -- array clause
                                NULL::text
                            ]
                                
                        -- "..." format
                        WHEN "json_path".key ~ '^"' THEN
                            ARRAY[
                                -- type = "basic"
                                'basic',
                                -- parent key: "a.b".c => a.b
                                btrim((regexp_match("json_path".key, '^"[^\"]{0,}"'))[1], '"'),
                                -- child key: "a.b".c => c
                                nullif(regexp_replace("json_path".key, '^"[^\"]{0,}"\.{0,1}', ''), ''),
                                -- array clause
                                NULL::text
                            ]
                
                        -- "(...)" format
                        WHEN "json_path".key ~ '^\(' AND "json_path".key ~ '\$\.' THEN
                            
                            CASE
                                WHEN nullif(regexp_replace(
                                    (regexp_match("json_path".key, '(?<=(\$\.))[^\[]{1,}'))[1], 
                                    '((^"[^\"]{0,}"\.{0,1})|(^[^\.]{1,}\.{0,1}))', ''), '') IS NOT NULL 
                                -- array is in inside another object => process the object first
                                THEN 
                                    ARRAY[
                                        -- type = "basic"
                                        'basic',
                                        -- parent key: (($.a."b.c"[*] ? (@.id == 2)).b[*] ? (@.id == 1)).g => a
                                        btrim((regexp_match("json_path".key, '(?<=(\$\.))(("[^\"]{0,}")|([^\.\[]{1,}))'))[1], '"'),
                                        -- child key: remove parent key => (($."b.c"[*] ? (@.id == 2)).b[*] ? (@.id == 1)).g
                                        nullif(regexp_replace("json_path".key, '(?<=(\$\.))(("[^\"]{0,}")|([^\.\[]{1,}))\.{0,1}', ''), ''),
                                        -- array clause
                                        NULL::text
                                    ]
                                -- array is on the top level => start processing the array
                                ELSE
                                    ARRAY[
                                        -- type = "array"
                                        'array',
                                        -- parent key: (($."a.b"[*] ? (@.id == 2)).b[*] ? (@.id == 1)).g => a.b
                                        btrim((regexp_match("json_path".key, '(?<=(\$\.))(("[^\"]{0,}")|([^\.\[]{1,}))'))[1], '"'),
                                        -- child key: (($."a.b"[*] ? (@.id == 2)).b[*] ? (@.id == 1)).g => ($.b[*] ? (@.id == 1)).g
                                        nullif(nullif(regexp_replace(
                                            "json_path".key, 
                                            '\(\$\.[^\[]{1,}\[[^\]]{1,}\][^\(]{1,}\([^\)]{1,}\)\)', '$'), '$'), ''),
                                        -- array clause: (($."a.b"[*] ? (@.id == 2)).b[*] ? (@.id == 1)).g => $.id == 2
                                        concat('$', (regexp_match("json_path".key, '(?<=(\(\@))[^\)]{1,}'))[1])
                                    ]
                            END
                
                        -- empty key
                        WHEN "json_path".key ~ '^\.[^\.]' THEN
                            ARRAY[
                                -- type = "basic"
                                'basic',
                                -- parent key: a."b.c" => a
                                '',
                                -- child key: a."b.c" => "b.c"
                                nullif(regexp_replace("json_path".key, '^\.', ''), ''),
                                -- array clause
                                NULL::text
                            ]
                                
                        -- unknows format => skip the key
                        ELSE NULL::text[]
                
                    END AS key_data,
            
                    -- value to set
                    value AS object_value
                
                FROM jsonb_each(ljPathValue) json_path(key, value)
            ),
            
            
            -- prepare parent key and action name (insert | update | delete)
            "parent_array_list" AS MATERIALIZED (
                SELECT
                
                    -- each unique array key name will have order_id = 1
                    ROW_NUMBER() OVER(PARTITION BY "parent_child".key_data[PARENT_KEY_POS]) AS order_id,
                
                    -- array key name
                    "parent_child".key_data[PARENT_KEY_POS] AS parent_key,
                
                    -- jsonpath expression to find array element
                    "parent_child".key_data[ARRAY_JSONPATH_POS] AS array_jsonpath,
                
                    "parent_child".key_data[CHILD_KEY_POS] AS child_key,
                
                    -- detect the action name
                    CASE
                        -- child key not empty
                        WHEN "parent_child".key_data[CHILD_KEY_POS] IS NOT NULL THEN
                            'child'
                        -- $.id == 0 => add a new element
                        WHEN jsonb_build_object('id', 0) @@ "parent_child".key_data[ARRAY_JSONPATH_POS]::jsonpath THEN
                            'insert'
                        -- value is NULL => delete the element
                        WHEN COALESCE(jsonb_typeof("parent_child".object_value), 'null') = 'null' THEN
                            'delete'
                        -- update the element
                        ELSE 'update'
                    END AS action_name,
                
                    -- object with new element data
                    "parent_child".object_value
                
                FROM "parent_child"
                WHERE "parent_child".key_data[JSON_KEY_TYPE] IS NOT DISTINCT FROM 'array'
            ),
            
            
            -- group all child keys by parent key and jsonpath expression
            "parent_array_child" AS MATERIALIZED (
                SELECT
                    -- array key name
                    "parent_array_list".parent_key,
                    -- jsonpath expression to find array element
                    "parent_array_list".array_jsonpath,
                    -- aggregated object to update the array element
                    jsonb_object_agg(
                        -- remove "$." prefix for object keys
                        regexp_replace("parent_array_list".child_key, '^\$\.', ''),
                        "parent_array_list".object_value
                    ) AS object_value
                FROM "parent_array_list"
                WHERE "parent_array_list".action_name = 'child'
                GROUP BY 1, 2
            ),
            
            
            -- prepare list of array to insert new elements
            "parent_array_insert" AS MATERIALIZED (
                SELECT
                    -- array key name
                    "parent_array_list".parent_key,
                
                    -- row ID
                    ROW_NUMBER() OVER(PARTITION BY "parent_array_list".parent_key) AS row_id,
                
                    -- object with new element data
                    "parent_array_list".object_value
                
                FROM "parent_array_list"
                WHERE "parent_array_list".action_name = 'insert'
            ),
            
            
            -- calculate maximal "id" value for each array
            "parent_array_insert_max" AS MATERIALIZED (
                SELECT
                    -- array key name
                    "parent_array_insert".parent_key,
                
                    CASE
                        WHEN jsonb_typeof(ljInitialObject->("parent_array_insert".parent_key)) IS NOT DISTINCT FROM 'array'
                        -- calculate maximal "id"
                        THEN COALESCE(
                            (
                                SELECT
                                    MAX((array_element->>'id')::bigint)
                                FROM jsonb_array_elements(ljInitialObject->("parent_array_insert".parent_key)) array_element
                            ), 0)
                        ELSE 0
                    END AS maximal_id
                
                FROM "parent_array_insert"
                WHERE "parent_array_insert".row_id = 1
            ),
            
            
            -- group parent keys with "update" and "delete" actions
            "parent_array_update" AS MATERIALIZED (
                SELECT
                    -- array key name
                    "parent_array_list".parent_key,
                
                    -- jsonpath expression to find array element
                    "parent_array_list".array_jsonpath,
                
                    -- merge values for the same array element
                    ub.agg_jsonb_merge(
                        "parent_array_list".object_value,
                        'replace'
                    ) AS object_value
                
                FROM "parent_array_list"
                WHERE "parent_array_list".action_name = 'update'
                GROUP BY 1, 2
            ),
            
            
            -- group parent keys with "update" and "delete" actions
            "parent_array_delete" AS MATERIALIZED (
                SELECT
                    -- array key name
                    "parent_array_list".parent_key,
                
                    -- jsonpath expression to find array element
                    "parent_array_list".array_jsonpath
                
                FROM "parent_array_list"
                WHERE "parent_array_list".action_name = 'delete'
                GROUP BY 1, 2
            ),
            
            
            -- calculate values for all parent keys
            "parent_key_group" AS MATERIALIZED (
            (   -- update all nested objects recursively
                SELECT
                    -- parent key => group by its key name
                    "parent_child".key_data[PARENT_KEY_POS] AS parent_key,
                    -- update all child objects recursively
                    ub.util_jsonb_update(
                        -- current value of the parent key
                        ljInitialObject->("parent_child".key_data[PARENT_KEY_POS]),
                        -- object with updates on the parent key
                        jsonb_object_agg("parent_child".key_data[CHILD_KEY_POS], "parent_child".object_value)
                    ) AS object_value
                FROM "parent_child"
                WHERE "parent_child".key_data[CHILD_KEY_POS] IS NOT NULL
                    AND "parent_child".key_data[JSON_KEY_TYPE] IS NOT DISTINCT FROM 'basic'
                GROUP BY 1
            )
                
            UNION
            (   -- add all objects at leaf level (no child objects)
                SELECT
                    "parent_child".key_data[PARENT_KEY_POS] AS parent_key,
                    "parent_child".object_value
                FROM "parent_child"
                WHERE "parent_child".key_data[CHILD_KEY_POS] IS NULL
                    AND "parent_child".key_data[JSON_KEY_TYPE] IS NOT DISTINCT FROM 'basic'
            )
                
            UNION
            (   -- process array data
                SELECT
                    -- array key name
                    "parent_array_list".parent_key,
                    
                    -- update all elements and add new elements
                    ub.util_jsonb_array(
                    (   -- update all elements in the array matched with jsonpath expressions
                        SELECT
                            jsonb_agg(
                                CASE
                                    -- update the element
                                    WHEN "parent_array_update".array_jsonpath IS NOT NULL THEN
                                        "parent_array_update".object_value
                                    -- apply changes to all child elements
                                    WHEN "parent_array_child".array_jsonpath IS NOT NULL THEN
                                        ub.util_jsonb_update(
                                            -- current value of the element
                                            array_element,
                                            -- object with updates on the element
                                            "parent_array_child".object_value
                                        )
                                    -- not matched 
                                    ELSE array_element
                                END
                            )
                        -- unnest current array
                        FROM jsonb_array_elements(ljInitialObject->("parent_array_list".parent_key)) array_element
                        -- join with list of deleted elements
                        LEFT JOIN "parent_array_delete" ON
                            "parent_array_delete".parent_key = "parent_array_list".parent_key
                            AND array_element @@ "parent_array_delete".array_jsonpath::jsonpath
                        -- join with list of updated elements
                        LEFT JOIN "parent_array_update" ON
                            "parent_array_update".parent_key = "parent_array_list".parent_key
                            AND array_element @@ "parent_array_update".array_jsonpath::jsonpath
                        -- join with list with child elements
                        LEFT JOIN "parent_array_child" ON
                            "parent_array_child".parent_key = "parent_array_list".parent_key
                            AND array_element @@ "parent_array_child".array_jsonpath::jsonpath
                        -- check the array is exists
                        WHERE jsonb_typeof(ljInitialObject->("parent_array_list".parent_key)) IS NOT DISTINCT FROM 'array'
                            -- skip all deleted elements
                            AND "parent_array_delete".array_jsonpath IS NULL
                    ),
                        
                    (   -- add new elements
                        SELECT
                            jsonb_agg(
                                ub.util_jsonb_concat(
                                    -- new object
                                    "parent_array_insert".object_value,
                                    -- unique "id" value
                                    jsonb_build_object(
                                        'id', "parent_array_insert_max".maximal_id + "parent_array_insert".row_id
                                    )
                                )
                                ORDER BY "parent_array_insert".row_id
                            )
                        FROM "parent_array_insert"
                        LEFT JOIN "parent_array_insert_max" ON
                            "parent_array_insert_max".parent_key = "parent_array_insert".parent_key
                        WHERE "parent_array_insert".parent_key = "parent_array_list".parent_key
                    ),
                        
                    'add'
                ) AS object_value
                
                FROM "parent_array_list"
                WHERE "parent_array_list".order_id = 1
            ))
            
            
            -- build new values for all parent keys
            SELECT
                jsonb_object_agg(
                    "parent_key_group".parent_key,
                    "parent_key_group".object_value
                )
            FROM "parent_key_group"
            WHERE "parent_key_group".parent_key IS NOT NULL
        )
    );
    
END;
/*
@example:
    SELECT ub.util_jsonb_update('{"a":{"b":{"c": 1}, "f": 10}}'::jsonb, '{"a.b":{"d": 5}}'::jsonb)
    => {"a":{"b":{"d":5},"f":10}} (update at specific json path)
    
    SELECT ub.util_jsonb_update('{"a":[{"id": 1, "b": 1}, {"id": 2, "b": [{"id": 1, "f": 3}] }]}'::jsonb, '{"(($.a[*] ? (@.id == 2)).b[*] ? (@.id == 1)).g": 10}'::jsonb)
    => {"a":[{"id":1, "b":1}, {"id":2, "b":[{"f":3,"g":10,"id":1}]}]} (update keys in the object at specific jsonpath)
    
    SELECT ub.util_jsonb_update('{"a":[{"id": 1, "b": 1}, {"id": 2, "b": [{"id": 1, "f": 3}] }]}'::jsonb, '{"(($.a[*] ? (@.id == 2)).b[*] ? (@.id == 1))": {"id": 1, "a": 10} }'::jsonb)
    => {"a":[{"b":1, "id":1}, {"b":[{"a":10,"id":1}], "id":2}]} (replace object at specific jsonpath)
    
    SELECT ub.util_jsonb_update('{"a":[{"id": 1, "b": 1}, {"id": 2, "b": [{"id": 1, "f": 3}] }] }'::jsonb, '{"(($.a[*] ? (@.id == 2)).b[*] ? (@.id == 0 && 17689 > 0))": {"f": 5}}'::jsonb)
    => {"a":[{"b":1,"id":1}, {"b":[{"f":3,"id":1},{"f":5,"id":2}], "id":2}]} (add object at specific jsonpath. Add a random value (e.g. 17689) to keep the whole key name unique)
    
    SELECT ub.util_jsonb_update('{"a":[{"id": 1, "b": 1}, {"id": 2, "b": [{"id": 1, "f": 3}, {"id": 2, "f": 10}]}]}'::jsonb, '{"(($.a[*] ? (@.id == 2)).b[*] ? (@.id == 1))": null}'::jsonb)
    => {"a":[{"b":1,"id":1},{"b":[{"f":10,"id":2}],"id":2}]} (delete object at specific jsonpath)
    
    SELECT ub.util_jsonb_update('{"a":{"b":{"c": 1}}}'::jsonb, '{"*":{"d": 5}}'::jsonb)
    => {"d": 5} (replace with a new object)

    SELECT ub.util_jsonb_update('{"a":{"b":{"c": 1}}}'::jsonb, '{"||":{"a": {"b": {"d": 5}}}}'::jsonb)
    => {"a":{"b":{"c": 1, "d": 5}}} (expand the initial object)
*/
$BODY$;

ALTER FUNCTION ub.util_jsonb_update(jsonb, jsonb)
    OWNER TO postgres;
