-- FUNCTION: ub.util_jsonb_unnest(jsonb, text, text)

-- DROP FUNCTION IF EXISTS ub.util_jsonb_unnest(jsonb, text, text);

CREATE OR REPLACE FUNCTION ub.util_jsonb_unnest(
    ljinitial jsonb,
    lckeyprefix text DEFAULT NULL::text,
    lcdelimiter text DEFAULT '.'::text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_jsonb_unnest
@desc convert all objects like { "key1": { "key2": { "key3": <value> }} into { "key1.key2.key3": <value> }
@desc any key prefix and delimiter are supported

@param object $1  - initial object to unnest
@param string|null $2 - key prefix for the keys in the object
@return object - unnested object

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#object #unnest object
*/  
    
BEGIN
            
    RETURN COALESCE(
        (
            WITH
            -- unnest object and calculate type for each key
            "unnest_object" AS MATERIALIZED (
                SELECT
                    concat((lcKeyPrefix || lcDelimiter), "top_data".key) AS key, 
                    "top_data".value,
                    jsonb_typeof(nullif("top_data".value, jsonb_build_object())) AS value_type
                FROM jsonb_each(ljInitial) AS top_data(key, value)
            ),

            -- unnest all selected objects and calculate new key name and type for each key
            "value_object" AS MATERIALIZED (
                SELECT
                    concat("unnest_object".key, lcDelimiter, "nested_data".key) AS key, 
                    "nested_data".value,
                    jsonb_typeof("nested_data".value) AS value_type     
                FROM "unnest_object"
                LEFT JOIN jsonb_each("unnest_object".value) AS nested_data(key, value) ON true
                WHERE "unnest_object".value_type IS NOT DISTINCT FROM 'object'
            )

            -- Build response
            SELECT
                -- select all non-objects for the top level
                COALESCE(
                    (
                        SELECT
                            jsonb_object_agg(
                                "unnest_object".key,
                                "unnest_object".value
                            )
                        FROM "unnest_object"
                        WHERE "unnest_object".value_type IS DISTINCT FROM 'object'
                    ),
                    jsonb_build_object()
                )

                ||
                -- select all non-objects for the second level
                COALESCE(
                    (
                        SELECT
                            jsonb_object_agg(
                                "value_object".key,
                                "value_object".value
                            )
                        FROM "value_object"
                        WHERE "value_object".value_type IS DISTINCT FROM 'object'
                    ),
                    jsonb_build_object()
                )

                ||

                -- unnest all objects recursively using ub.util_jsonb_unnest()
                COALESCE(
                    (   
                        SELECT
                            ub.agg_jsonb_concat(
                                ub.util_jsonb_unnest(
                                    "value_object".value,
                                    "value_object".key
                                )
                            )
                        FROM "value_object"
                        WHERE "value_object".value_type IS NOT DISTINCT FROM 'object'
                    ),
                    jsonb_build_object()
                )
        ),
        jsonb_build_object()
    );
    
END;
/*
@example:
    SELECT ub.util_jsonb_unnest('{"a":{"b":{"c": 1, "d": 5}}}'::jsonb)
    => {"a.b.c":1,"a.b.d":5}
    
    SELECT ub.util_jsonb_unnest('{"a":{"b":{"c": 1, "d": 5}}}'::jsonb, 'prefix_', '#')
    => {"prefix_#a#b.c":1,"prefix_#a#b.d":5}
*/
$BODY$;

ALTER FUNCTION ub.util_jsonb_unnest(jsonb, text, text)
    OWNER TO postgres;
