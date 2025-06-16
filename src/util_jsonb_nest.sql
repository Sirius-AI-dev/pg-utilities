-- FUNCTION: ub.util_jsonb_nest(jsonb, text)

-- DROP FUNCTION IF EXISTS ub.util_jsonb_nest(jsonb, text);

CREATE OR REPLACE FUNCTION ub.util_jsonb_nest(
    ljinitial jsonb,
    lcarrayflag text DEFAULT 'replace'::text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_jsonb_nest
@desc convert object like { "key1.key2.key3": <value> } INTO { "key1": { "key2": { "key3": <value> }} 

@param object $1  - initial object to nest
@param string|null $2 - how to merge arrays
    - replace - replace "initial" array with "merged" array (default)
    - expand - add all new keys from "merged" array to "initial" array (exclude duplicates)
    - add - add all keys from "merged" array to "initial" array (duplicates are possible)
    
@return object $1 - nested object

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#object #nest object
*/  
    
BEGIN
            
    IF jsonb_typeof(ljInitial) IS DISTINCT FROM 'object' THEN
        RETURN jsonb_build_object();
    END IF;

    RETURN COALESCE(
        (
            WITH 
            -- unnest all keys and calculate "parentKey" and "childKey"
            "parent_child" AS MATERIALIZED (
                SELECT
                    -- extract top-level key, considering quotes
                    COALESCE(
                        btrim((regexp_match(key, '^"[^"]{1,}"'))[1], '"'),
                        regexp_replace(key, '\.(?![\.\/]).*', '')
                    ) AS parent_key,
                    -- extract child key, considering quotes
                    (regexp_match(
                        -- remove top key in quotes
                        regexp_replace(key, '^"[^"]{1,}"', ''),
                        -- remove top key before "."
                        '(?<=\.(?=([^\.\/]))).*'
                    ))[1] AS child_key,
                    -- key value
                    value AS object_value
                FROM jsonb_each(ljInitial)
            ),

            -- select parent keys with non-nested child keys
            "data_not_nested" AS MATERIALIZED (
                SELECT
                    "parent_child".parent_key AS object_key,
                    jsonb_object_agg(
                        btrim("parent_child".child_key, '"'),
                        CASE
                            WHEN jsonb_typeof("parent_child".object_value) IS NOT DISTINCT FROM 'object'
                            THEN ub.util_jsonb_nest("parent_child".object_value, lcArrayFlag)
                            ELSE "parent_child".object_value
                        END
                    ) AS object_value
                FROM "parent_child"
                WHERE "parent_child".child_key IS NOT NULL
                    AND NOT regexp_replace("parent_child".child_key, '^"[^"]{1,}"', '') ~ '\.(?![\.\/])'
                GROUP BY 1
            ),

            -- select parent keys with no child keys
            "data_no_child" AS MATERIALIZED (
                SELECT
                    "parent_child".parent_key AS object_key,
                    CASE
                        WHEN jsonb_typeof("parent_child".object_value) IS NOT DISTINCT FROM 'object'
                        THEN ub.util_jsonb_nest("parent_child".object_value, lcArrayFlag)
                        ELSE "parent_child".object_value
                    END AS object_value
                FROM "parent_child"
                WHERE "parent_child".child_key IS NULL       
            ),

            -- select parent keys without child keys, and add non-nested child keys
            "data_merged" AS MATERIALIZED (
                SELECT
                    -- object key
                    COALESCE("data_no_child".object_key, "data_not_nested".object_key) AS object_key,
                    -- object value
                    CASE
                        WHEN "data_no_child".object_key IS NULL THEN 
                            -- one of objects is empty => assign the second object
                            "data_not_nested".object_value
                        WHEN "data_not_nested".object_key IS NULL THEN 
                            -- one of objects is empty => assign the second object
                            "data_no_child".object_value
                        WHEN jsonb_typeof("data_not_nested".object_value) IS NOT DISTINCT FROM 'array' THEN 
                            -- "array" type => merge arrays
                            ub.util_jsonb_array("data_no_child".object_value, "data_not_nested".object_value, lcArrayFlag)
                        WHEN jsonb_typeof("data_not_nested".object_value) IS DISTINCT FROM 'object' THEN 
                            -- one of values is scalar => replace the value
                            "data_not_nested".object_value
                        ELSE
                            -- merge two objects
                            ub.util_jsonb_merge("data_no_child".object_value, "data_not_nested".object_value, lcArrayFlag)
                    END AS object_value
                FROM "data_no_child"
                FULL JOIN "data_not_nested" ON
                    "data_not_nested".object_key = "data_no_child".object_key
                WHERE COALESCE("data_no_child".object_key, "data_not_nested".object_key) IS NOT NULL
            ),

            -- select parent keys with nested child keys
            "data_nested" AS MATERIALIZED (
                SELECT
                    "parent_child".parent_key AS object_key,
                    ub.util_jsonb_nest(
                        jsonb_object_agg(
                            "parent_child".child_key,
                            "parent_child".object_value
                        ),
                        lcArrayFlag
                    ) AS object_value
                FROM "parent_child"
                WHERE "parent_child".child_key IS NOT NULL
                    AND regexp_replace("parent_child".child_key, '^"[^"]{1,}"', '') ~ '\.(?![\.\/])'
                GROUP BY 1
            )

            -- Aggregate all object keys and values
            SELECT
            
                jsonb_object_agg(
                    -- object key
                    COALESCE("data_merged".object_key, "data_nested".object_key),
                    -- object value
                    CASE
                        WHEN "data_nested".object_key IS NULL THEN 
                            -- one of objects is empty => assign the second object
                            "data_merged".object_value
                        WHEN "data_merged".object_key IS NULL THEN 
                            -- one of objects is empty => assign the second object
                            "data_nested".object_value
                        WHEN jsonb_typeof("data_merged".object_value) IS DISTINCT FROM 'object' THEN 
                            -- one of values is not an object => replace the value
                            "data_nested".object_value
                        ELSE
                            -- merge two objects
                            ub.util_jsonb_merge("data_merged".object_value, "data_nested".object_value, lcArrayFlag)
                    END
                )
            
            FROM "data_merged"
            FULL JOIN "data_nested" ON
                "data_nested".object_key = "data_merged".object_key
            WHERE COALESCE("data_merged".object_key, "data_nested".object_key) IS NOT NULL
        ),
        jsonb_build_object()
    );
    
END;
/*
@example:
    SELECT ub.util_jsonb_nest('{"a.b.c": 1, "a.b.d": 5}'::jsonb)
    => {"a":{"b":{"c":1,"d":5}}}
    
    SELECT ub.util_jsonb_nest('{"a.b.c": [1,2], "a.b": {"c": [2,3]}}'::jsonb, 'add')
    => {"a":{"b":{"c":[2,3,1,2]}}}
    
    SELECT ub.util_jsonb_nest('{"a.b.c": 1, "\"a.b.d\"": 5}'::jsonb)
    => {"a":{"b":{"c":1}},"a.b.d":5}
*/
$BODY$;

ALTER FUNCTION ub.util_jsonb_nest(jsonb, text)
    OWNER TO postgres;
