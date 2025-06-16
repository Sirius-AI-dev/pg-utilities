-- FUNCTION: ub.util_jsonb_merge(jsonb, jsonb, text)

-- DROP FUNCTION IF EXISTS ub.util_jsonb_merge(jsonb, jsonb, text);

CREATE OR REPLACE FUNCTION ub.util_jsonb_merge(
    ljinitialobject jsonb,
    ljupdatedkeys jsonb,
    lcarrayflag text DEFAULT 'expand'::text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_jsonb_merge
@desc merge json object | array | any json type under following rules:
@desc - the appropriate key in the initial object is being expanded with non-null value
@desc - if updated key is ended with ".", the appropriate key in the initial object is being replaced with updated value
@desc - arrays are merged using lcArrayFlag parameter
@desc - "string", "number", "boolean" type are being replaced
@desc Aggregate function ub.agg_jsonb_merge(jsonb, text) is defined too

@param any $1 - initial object | array | any other json type
@param any $2 - object with updated keys & values | array | any other json type
@param string $3 - how to merge arrays: "expand" (default) | "add" | "replace"
    
@return object $1 - expanded object | array | any other json type

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#object #expand object #expand array #replace object #replace array #merge object #merge array 
*/

BEGIN

    -- process object with updated keys
    IF jsonb_typeof(ljUpdatedKeys) IS NOT DISTINCT FROM 'object' THEN
    
        -- Prepare initial object for further processing
        IF jsonb_typeof(ljInitialObject) IS DISTINCT FROM 'object' THEN
        
            ljInitialObject := jsonb_build_object();
            
        END IF;
    
    -- not an object type => merge data of other types
    ELSE
        
        -- UpdatedKeys is null
        IF COALESCE(jsonb_typeof(ljUpdatedKeys), 'null') = 'null' THEN
        
            RETURN
                CASE
                    WHEN COALESCE(jsonb_typeof(ljInitialObject), 'null') = 'null'
                    -- both input parameters are null => return empty object
                    THEN jsonb_build_object()
                    -- return InitialObject as-is
                    ELSE ljInitialObject
                END;
        
        -- Merge to arrays
        ELSEIF jsonb_typeof(ljInitialObject) IS NOT DISTINCT FROM 'array'
            AND jsonb_typeof(ljUpdatedKeys) IS NOT DISTINCT FROM 'array' THEN
            
            RETURN ub.util_jsonb_array(ljInitialObject, ljUpdatedKeys, lcArrayFlag);
        
        -- Non object or array => return ljUpdatedKeys as-is
        ELSE            
        
            RETURN ljUpdatedKeys;
            
        END IF;
    END IF;
    
    
    -- Process updated keys
    RETURN ub.util_jsonb_concat(
        -- initial object
        ljInitialObject,
        -- add updated keys
       (
            SELECT
                jsonb_object_agg(
                    -- key name (remove end "." is present)
                    rtrim("updated".key, '.'),
                    -- expanded value
                    CASE
                        -- "updated" key is "number" | "string" | "boolean" type => just replace with "updated" value
                        -- "updated" key is ended with "." => just replace with "updated" value
                        WHEN jsonb_typeof("updated".value) IN ('number', 'string', 'boolean')
                            OR "updated".key ~ '\.$' THEN
                            "updated".value

                        -- "initial" key is NULL => just replace with "updated" value
                        WHEN COALESCE(jsonb_typeof(ljInitialObject->("updated".key)), 'null') = 'null' THEN
                            "updated".value

                        -- updated value is NULL => keep ljInitialObject->("updated".key)
                        WHEN jsonb_typeof("updated".value) IN ('null') THEN
                            ljInitialObject->("updated".key)
                        
                        -- "array" type => expand the array
                        WHEN jsonb_typeof("updated".value) IS NOT DISTINCT FROM 'array' THEN

                            ub.util_jsonb_array(
                                ljInitialObject->("updated".key),
                                "updated".value,
                                lcArrayFlag
                            )

                        -- "object" typee => run "ub.util_jsonb_merge" recursively
                        WHEN jsonb_typeof("updated".value) IS NOT DISTINCT FROM 'object' THEN

                            ub.util_jsonb_merge(
                                ljInitialObject->("updated".key),
                                "updated".value,
                                lcArrayFlag
                            )

                        ELSE
                            "updated".value
                    END
                )
            FROM jsonb_each(ljUpdatedKeys) AS updated(key, value)
        )
    );

END;
/*
@example:
    SELECT ub.util_jsonb_merge('{"a": {"b": {"c": 1}}}'::jsonb, '{"a": {"b": {"d": 5}}}'::jsonb, 'expand')
    => {"a": {"b": {"c": 1, "d": 5}}} (objects are expanded)
    
    SELECT ub.util_jsonb_merge('{"a.b": {"c": 5}}'::jsonb, '{"a.b": {"d": 8}}'::jsonb, 'expand')
    => {"a.b": {"c": 5, "d": 8}} (objects are expanded)
    
    SELECT ub.util_jsonb_merge('{"a": {"c": 5, "d": [1,2]}}'::jsonb, '{"a": {"b": 8, "d": [3,4]}}'::jsonb, 'replace')
    => {"a":{"b":8,"c":5,"d":[3,4]}} (arrays are replaced)
    
    SELECT ub.util_jsonb_merge('{"a": {"c": 5, "d": [1,2]}}'::jsonb, '{"a": {"b": 8, "d": [3,4]}}'::jsonb, 'add')
    => {"a":{"b":8,"c":5,"d":[1,2,3,4]}} (arrays are concatenated)
*/
$BODY$;

ALTER FUNCTION ub.util_jsonb_merge(jsonb, jsonb, text)
    OWNER TO postgres;
