-- FUNCTION: ub.util_jsonb_concat(jsonb, jsonb)

-- DROP FUNCTION IF EXISTS ub.util_jsonb_concat(jsonb, jsonb);

CREATE OR REPLACE FUNCTION ub.util_jsonb_concat(
    ljinitialobject jsonb,
    ljconcatobject jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_jsonb_concat
@desc concatenate two objects, considering NULL values
@desc Aggregate function ub.agg_jsonb_concat(ljConcatObject) is defined too

@param object $1 - initial object
@param object $2 - concatenated object
    
@return object $1 - concatenated object

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#object #merge object #expand array #replace array #add array
*/

BEGIN

    IF jsonb_typeof(ljInitialObject) IS DISTINCT FROM 'object' THEN

        -- one of objects is null => return the second one
        RETURN
            CASE
                WHEN jsonb_typeof(ljConcatObject) IS DISTINCT FROM 'object'
                THEN jsonb_build_object()
                ELSE ljConcatObject
            END;
        
    ELSEIF jsonb_typeof(ljConcatObject) IS DISTINCT FROM 'object' THEN
    
        -- one of objects is null => return the second one
        RETURN ljInitialObject;
        
    END IF;
    
    -- Build response
    RETURN ljInitialObject || ljConcatObject;
    
END;
/*
@example:
    SELECT ub.util_jsonb_concat('{"a":{"b":{"c": 1}}}'::jsonb, '{"a":{"b":{"d": 5}}}'::jsonb)
    => {"a":{"b":{"d": 5}}}
    
    SELECT ub.util_jsonb_concat('{"a":{"b":{"c": 1}}}'::jsonb, NULL::jsonb)
    => {"a":{"b":{"c": 1}}}
    
    SELECT ub.util_jsonb_concat(NULL::jsonb, '{"a":{"b":{"c": 1}}}'::jsonb)
    => {"a":{"b":{"d": 5}}}
*/
$BODY$;

ALTER FUNCTION ub.util_jsonb_concat(jsonb, jsonb)
    OWNER TO postgres;
