-- FUNCTION: ub.util_jsonb_multi_merge(text, jsonb[])

-- DROP FUNCTION IF EXISTS ub.util_jsonb_multi_merge(text, jsonb[]);

CREATE OR REPLACE FUNCTION ub.util_jsonb_multi_merge(
    lcarrayflag text,
    VARIADIC ljdata jsonb[])
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_jsonb_multi_merge
@desc merge json objects | arrays | any json type under following rules:
@desc - the appropriate key in the initial object is being expanded with non-null value
@desc - if updated key is ended with ".", the appropriate key in the initial object is being replaced with updated value
@desc - arrays are merged using lcArrayFlag parameter
@desc - "string", "number", "boolean" type are being replaced

@param string $1 - how to merge arrays: "expand" (default) | "add" | "replace"
@param any $n - merged objects|arrays|other json types
    
@return object $1 - merged array

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#object #concat object #multiple concat
*/

DECLARE

    ljResult    jsonb :=                                -- result object to response
        ub.util_jsonb_merge(ljData[1], ljData[2], lcArrayFlag);
        
    lnObject    integer;                                -- object ID to merge
    
    
BEGIN
    
    -- concatenate all data in the input array
    FOR lnObject IN 3..array_length(ljData, 1) LOOP
    
        ljResult := ub.util_jsonb_merge(ljResult, ljData[lnObject], lcArrayFlag);
    
    END LOOP;
    
    RETURN ljResult;
    
END;
/*
@example:

    SELECT ub.util_jsonb_multi_merge('expand', '{"a": {"b": {"c": 1}}}'::jsonb, '{"a": {"b": {"d": 5}}}'::jsonb, '{"a": {"b": {"f": 5}}}'::jsonb)
    => {"a":{"b":{"c":1,"d":5,"f":5}}} (objects are expanded)
    
    SELECT ub.util_jsonb_multi_merge('replace', '{"a": {"c": 5, "d": [1,2], "e": [2,3]}}'::jsonb, '{"a": {"b": 8, "d": [3,4]}}'::jsonb, '{"a": {"b": 8, "e": [5,6]}}'::jsonb)
    => {"a":{"b":8,"c":5,"d":[3,4],"e":[5,6]}} (arrays are replaced)
    
    SELECT ub.util_jsonb_multi_merge('add', '{"a": {"c": 5, "d": [1,2]}}'::jsonb, '{"a": {"b": 8, "d": [3,4]}}'::jsonb, '{"a": {"b": 8, "d": [5,6]}}'::jsonb)
    => {"a":{"b":8,"c":5,"d":[1,2,3,4,5,6]}} (arrays are concatenated)
*/
$BODY$;

ALTER FUNCTION ub.util_jsonb_multi_merge(text, jsonb[])
    OWNER TO postgres;
