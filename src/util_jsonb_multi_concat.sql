-- FUNCTION: ub.util_jsonb_multi_concat(jsonb[])

-- DROP FUNCTION IF EXISTS ub.util_jsonb_multi_concat(jsonb[]);

CREATE OR REPLACE FUNCTION ub.util_jsonb_multi_concat(
    VARIADIC ljobject jsonb[])
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_jsonb_multi_concat
@desc concatenate multiple objects, considering NULL values

@param object $n - object to concatenate to the result
    
@return object $1 - concatenated object

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#object #concat object #multiple concat
*/

DECLARE

    ljResult    jsonb :=                                -- result object to response
        CASE
            WHEN jsonb_typeof(ljObject[1]) IS DISTINCT FROM 'object'
            THEN jsonb_build_object()
            ELSE ljObject[1]
        END;
        
    lnObject    integer;                                -- object ID
    
    
BEGIN
    
    -- concatenate all objects in the input array
    FOR lnObject IN 2..array_length(ljObject, 1) LOOP
    
        ljResult := ljResult ||
            -- transform NULL to {}
            CASE
                WHEN jsonb_typeof(ljObject[lnObject]) IS DISTINCT FROM 'object'
                THEN jsonb_build_object()
                ELSE ljObject[lnObject]
            END;
    
    END LOOP;
    
    RETURN ljResult;
    
END;
/*
@example:
    SELECT ub.util_jsonb_multi_concat('{"a": 1}', '{"b": 2}', NULL::jsonb, '{"c": 3}')
     => {"a": 1, "b": 2, "c": 3}
*/
$BODY$;

ALTER FUNCTION ub.util_jsonb_multi_concat(jsonb[])
    OWNER TO postgres;
