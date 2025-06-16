-- FUNCTION: ub.util_jsonb_differ(jsonb, jsonb, text)

-- DROP FUNCTION IF EXISTS ub.util_jsonb_differ(jsonb, jsonb, text);

CREATE OR REPLACE FUNCTION ub.util_jsonb_differ(
    ljinitialobject jsonb,
    ljupdatedobject jsonb,
    lcarrayflag text DEFAULT 'order_no_matter'::text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_jsonb_differ
@desc prepare list of keys in the "updated object" that are differ from the same keys in the "initial object"

@param object $1 - initial object
@param object $2 - updated object
@param string $3 - flag how to compare arrays
    - order_no_matter = order of elements doesn't matter (default)
    - order_matter = order of elements matters
    
@return object $1 - object with new key values (NULL if no new key values)

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#object #compare object #difference
*/

BEGIN

    IF jsonb_typeof(ljInitialObject) IS DISTINCT FROM 'object' THEN

        -- initial object is null => return the updated object
        RETURN ljUpdatedObject;
        
    ELSEIF jsonb_typeof(ljUpdatedObject) IS DISTINCT FROM 'object' THEN
    
        -- updated object is null => return the second one
        RETURN NULL::jsonb;
        
    END IF;
    
    -- Build response
    RETURN 
        (
            SELECT
                 jsonb_object_agg("updated".key, "updated".value)
            FROM jsonb_each(ljUpdatedObject) AS updated(key, value)
            WHERE 
                -- add if two values are different
                jsonb_path_query_first(ljInitialObject, concat('$.', "updated".key)::jsonpath) IS DISTINCT FROM "updated".value
                AND
                -- null = null
                (jsonb_typeof("updated".value) != 'null' 
                 OR jsonb_path_query_first(ljInitialObject, concat('$.', "updated".key)::jsonpath) IS NOT NULL)
                AND
                -- check if arrays are different, order doesn't matter
                (lcArrayFlag IS NOT DISTINCT FROM 'order_matter'
                 OR jsonb_typeof("updated".value) IS DISTINCT FROM 'array'
                 OR jsonb_typeof(jsonb_path_query_first(ljInitialObject, concat('$.', "updated".key)::jsonpath)) IS DISTINCT FROM 'array'
                 OR NOT "updated".value @> jsonb_path_query_first(ljInitialObject, concat('$.', "updated".key)::jsonpath)
                 OR NOT "updated".value <@ jsonb_path_query_first(ljInitialObject, concat('$.', "updated".key)::jsonpath))
        );
    
END;
/*
@example:
    SELECT ub.util_jsonb_differ('{"a":{"b":{"c": 1}}}'::jsonb, '{"a.b.c": 1}'::jsonb)
    => null (no new keys)
    
    SELECT ub.util_jsonb_differ('{"a": [2, 3], "b": 10, "d": 20}'::jsonb, '{"a": [3, 4], "b": 10}'::jsonb)
    => {"a": [3, 4]}
    
    SELECT ub.util_jsonb_differ('{"a": [2, 3], "b": 10}'::jsonb, '{"a": [3, 4], "b": 10}'::jsonb, 'order_no_matter')
    => null (no new keys)
    
    SELECT ub.util_jsonb_differ('{"a": [2, 3], "b": 10}'::jsonb, '{"a": [3, 4], "b": 10}'::jsonb, 'order_matter')
    => {"a": [3, 2]} 
*/
$BODY$;

ALTER FUNCTION ub.util_jsonb_differ(jsonb, jsonb, text)
    OWNER TO postgres;
