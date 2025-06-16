-- FUNCTION: ub.util_jsonb_multi_array(text, jsonb[])

-- DROP FUNCTION IF EXISTS ub.util_jsonb_multi_array(text, jsonb[]);

CREATE OR REPLACE FUNCTION ub.util_jsonb_multi_array(
    lcarrayflag text,
    VARIADIC ljarray jsonb[])
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_jsonb_multi_array
@desc process multiple arrays, considering NULL values and merge rules

@param string $1 - how to merge arrays
    - expand - add all new keys from "merged" array to "initial" array (exclude duplicates) 
    - add - add all keys from "merged" array to "initial" array (duplicates are possible) (default)
    - sub - subtract elements of the merged array from initial array
    - intersect - calculate common keys in both array
@param array $n - arrays to process
    
@return object $1 - processed array

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#array #expand array #multiple arrays #add array #subtract array #intersect array
*/

DECLARE

    ljResult    jsonb :=                                -- merge first two arrays
        ub.util_jsonb_array(ljArray[1], ljArray[2], lcArrayFlag);
        
    lnArrayID   integer;                                -- array ID to merge
    
    
BEGIN
    
    -- concatenate all arrays in the input array
    FOR lnArrayID IN 3..array_length(ljArray, 1) LOOP
    
        ljResult := ub.util_jsonb_array(ljResult, ljArray[lnArrayID], lcArrayFlag);
    
    END LOOP;
    
    RETURN ljResult;
    
END;
/*
@example:
    SELECT ub.util_jsonb_multi_array('expand', '[2,3]'::jsonb, '[4,3]'::jsonb, '[5,3,2]'::jsonb)
    => [2,3,4,5]
    
    SELECT ub.util_jsonb_multi_array('add', '[2,3]'::jsonb, '[4,3]'::jsonb, '[5,3,2]'::jsonb)
    => [2,3,4,3,5,3,2]
    
    SELECT ub.util_jsonb_multi_array('intersect', '[2,3]'::jsonb, '[4,3]'::jsonb, '[5,3,2]'::jsonb)
    => [3]
*/
$BODY$;

ALTER FUNCTION ub.util_jsonb_multi_array(text, jsonb[])
    OWNER TO postgres;
