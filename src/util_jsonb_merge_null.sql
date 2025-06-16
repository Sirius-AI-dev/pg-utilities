-- FUNCTION: ub.util_jsonb_merge_null(jsonb, jsonb)

-- DROP FUNCTION IF EXISTS ub.util_jsonb_merge_null(jsonb, jsonb);

CREATE OR REPLACE FUNCTION ub.util_jsonb_merge_null(
    ljinitialobject jsonb,
    ljconcatobject jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_jsonb_merge_null
@desc concatenate two objects and set all old keys to NULL values

@param object $1 - initial object
@param object $2 - concatenated object
    
@return object $1 - concatenated object

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#object #merge object #set null
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
    RETURN ljConcatObject || COALESCE(
        (
            SELECT
                jsonb_object_agg("initial".key, NULL)
            FROM jsonb_each(ljInitialObject) AS initial(key, value)
            WHERE ljConcatObject->>("initial".key) IS NULL
        ),
        jsonb_build_object()
    );
    
END;
/*
@example:
    SELECT ub.util_jsonb_merge_null('{"a": 1, "b": 2},'::jsonb, '{"b": 3}'::jsonb)
    => {"a": null, "b": 3}
*/
$BODY$;

ALTER FUNCTION ub.util_jsonb_merge_null(jsonb, jsonb)
    OWNER TO postgres;
