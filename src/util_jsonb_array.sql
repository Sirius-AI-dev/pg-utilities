-- FUNCTION: ub.util_jsonb_array(jsonb, jsonb, text)

-- DROP FUNCTION IF EXISTS ub.util_jsonb_array(jsonb, jsonb, text);

CREATE OR REPLACE FUNCTION ub.util_jsonb_array(
    ljinitialarray jsonb,
    ljmergedarray jsonb,
    lcarrayflag text DEFAULT 'add'::text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_jsonb_array
@desc process two arrays with replacing | expanding | adding | subtracting | intersecting
@desc Aggregate function ub.agg_jsonb_array(ljMergedArray, lcArrayFlag) is defined too

@param array $1 - initial array
@param array $2 - merged array
@param string|null $3 - how to merge arrays
    - replace - replace "initial" array with "merged" array 
    - expand - add all new keys from "merged" array to "initial" array (exclude duplicates) 
    - add - add all keys from "merged" array to "initial" array (duplicates are possible) (default)
    - sub - subtract elements of the merged array from initial array
    - intersect - calculate common keys in both array
    
@return array $1 - processed array

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#jsonb array #merge jsonb array #expand jsonb array #replace jsonb array #subtract jsonb array #intersect jsonb array
*/

BEGIN
    
    IF (jsonb_typeof(ljInitialArray) IS DISTINCT FROM 'array' AND lcArrayFlag IN ('add', 'expand', 'intersect'))
        OR lcArrayFlag = 'replace' THEN
    
        -- Replace array with the "merged" one
        RETURN
            CASE
                WHEN jsonb_typeof(ljMergedArray) IS DISTINCT FROM 'array'
                    OR lcArrayFlag = 'intersect'
                THEN jsonb_build_array()
                ELSE ljMergedArray
            END;
    
    
    ELSEIF jsonb_typeof(ljMergedArray) IS DISTINCT FROM 'array' THEN
    
        -- Keep "initial" array as-is
        RETURN 
            CASE
                WHEN jsonb_typeof(ljInitialArray) IS DISTINCT FROM 'array'
                    OR lcArrayFlag = 'intersect'
                THEN jsonb_build_array()
                ELSE ljInitialArray
            END;        
    
    
    ELSEIF lcArrayFlag = 'add' THEN
    
        -- concatenate two arrays
        RETURN ljInitialArray || ljMergedArray;
    
    
    ELSEIF lcArrayFlag = 'expand' THEN
        
        -- concatenate two arrays and exclude duplicates for "number", "string", "boolean" types
        RETURN 
            ljInitialArray 
            ||
            COALESCE(
                (
                    WITH
                    -- unnest merged array
                    "merged_data" AS MATERIALIZED (
                        SELECT
                            ROW_NUMBER() OVER() AS row_id,
                            merged_element
                        FROM jsonb_array_elements(ljMergedArray) merged_element
                        WHERE merged_element IS NOT NULL
                    ),
                    -- exclude duplicates in merged array
                    "exclude_duplicates" AS MATERIALIZED (
                        SELECT
                            "merged_data".row_id,
                            ROW_NUMBER() OVER(PARTITION BY "merged_data".merged_element) AS duplicate_id,
                            "merged_data".merged_element
                        FROM "merged_data"
                    )
                    -- add new elements only
                    SELECT
                        jsonb_agg("exclude_duplicates".merged_element ORDER BY "exclude_duplicates".row_id)
                    FROM "exclude_duplicates"
                    WHERE "exclude_duplicates".duplicate_id = 1 
                        AND NOT ljInitialArray @> jsonb_build_array("exclude_duplicates".merged_element)
                ),
                jsonb_build_array()
            );
        
    ELSEIF lcArrayFlag = 'sub'
        AND jsonb_typeof(ljInitialArray) IS NOT DISTINCT FROM 'array' THEN
        
        -- subtract elements of the merged array from initial array
        RETURN 
            
            COALESCE(
                (
                    SELECT
                        jsonb_agg(initial_element)
                    FROM jsonb_array_elements(ljInitialArray) initial_element
                    LEFT JOIN jsonb_array_elements(ljMergedArray) deleted_element ON
                        deleted_element = initial_element
                    WHERE deleted_element IS NULL
                ),
                jsonb_build_array()
            );
        
    
    ELSEIF lcArrayFlag = 'intersect' THEN
    
        -- intersect two arrays
        RETURN COALESCE(
            (
                SELECT
                    jsonb_agg(initial_element)
                FROM jsonb_array_elements(ljInitialArray) initial_element
                INNER JOIN jsonb_array_elements(ljMergedArray) merged_element ON
                    merged_element = initial_element
            ),
            jsonb_build_array()
        );
        
        
    ELSE
    
        -- unknown command => return initial array
        RETURN ljInitialArray;
        
    END IF;
    
END;
/*
@example:
    SELECT ub.util_jsonb_array('[2,3]'::jsonb, '[4,3]'::jsonb, 'expand')
    => [2,3,4]
    
    SELECT ub.util_jsonb_array('[2,3]'::jsonb, '[4,3]'::jsonb, 'replace')
    => [4,3]
    
    SELECT ub.util_jsonb_array('[2,3]'::jsonb, '[4,3]'::jsonb, 'add')
    => [2,3,4,3]
    
    SELECT ub.util_jsonb_array('[2,3,4,5]'::jsonb, '[4,3]'::jsonb, 'sub')
    => [2,5]
    
    SELECT ub.util_jsonb_array('[2,3,4,5]'::jsonb, '[4,3,1]'::jsonb, 'intersect')
    => [3,4]
*/
$BODY$;

ALTER FUNCTION ub.util_jsonb_array(jsonb, jsonb, text)
    OWNER TO postgres;
