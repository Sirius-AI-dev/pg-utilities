-- FUNCTION: ub.util_array_integer(integer[], integer[], text)

-- DROP FUNCTION IF EXISTS ub.util_array_integer(integer[], integer[], text);

CREATE OR REPLACE FUNCTION ub.util_array_integer(
    lntotaldata integer[],
    lnupdatedata integer[],
    lcaggfunc text DEFAULT 'SUM'::text)
    RETURNS integer[]
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_array_integer
@desc Process two integer arrays. Arrays with different sizes are automatically expanded to the largest one
@desc Aggregate function ub.agg_array_integer(lnUpdateData, lcAggFunc) is defined too

@param integer[] $1  - initial integer array
@param integer[] $2 - integer array to sum up or merge with
@params text $3 -
    - SUM: sum two integer arrays, with adjusting their length
    - MERGE: merge two integer arrays
    - MERGE_UNIQUE: merge two integer arrays with eliminating duplicates
    - MERGE_NNN: merge the second array at NNN position

@return integer[] - processed float array

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#integer array #integer array merge #array adjust #array aggregate
*/

BEGIN

    lcAggFunc := upper(lcAggFunc);
    
    CASE

        -- Merge two integer arrays
        WHEN lcAggFunc = 'MERGE' THEN           

            RETURN lnTotalData || lnUpdateData;
  
  
        -- Merge two integer arrays eliminating duplicates  
        WHEN lcAggFunc = 'MERGE_UNIQUE' THEN    

            IF lnTotalData IS NULL THEN
      
                RETURN lnUpdateData;
        
            ELSE
            
                RETURN (
                    SELECT
                        array_agg(COALESCE(basic_id, update_id))
                    FROM unnest(lnTotalData) basic_id
                    FULL JOIN unnest(lnUpdateData) update_id ON
                        (update_id = basic_id)
                );
        
            END IF;
      
        
        -- Merge the second array at position nnn 
        WHEN lcAggFunc ~ 'MERGE[0-9]{1,}' THEN

            RETURN 
                lnTotalData 
                || 
                array_fill(0::integer, ARRAY[GREATEST(substr(lcAggFunc,6)::integer - COALESCE(array_length(lnTotalData, 1), 0) - 1, 0)])
                || 
                lnUpdateData;
      
        
        -- Sum up two integer arrays with adjusting their sizes
        WHEN lcAggFunc = 'SUM' THEN

             -- Initial array is larger than the second one
            IF COALESCE(cardinality(lnTotalData), 0) > COALESCE(cardinality(lnUpdateData), 0) THEN

                RETURN (
                    SELECT
                        array_agg(COALESCE("element_data".value, 0) + COALESCE(lnUpdateData["element_data".order_id], 0))
                    FROM unnest(lnTotalData) WITH ORDINALITY AS element_data(value, order_id)
                );

            -- Initial array is not larger than the second one
            ELSE

                RETURN (
                    SELECT
                        array_agg(COALESCE("element_data".value, 0) + COALESCE(lnTotalData["element_data".order_id], 0))
                    FROM unnest(lnUpdateData) WITH ORDINALITY AS element_data(value, order_id)
                );

            END IF;
        
        
        -- Unknown aggregated function => no processing
        ELSE

            RETURN lnTotalData;

  END CASE;

END;
/*
@example
SELECT ub.util_array_integer(ARRAY[5, 2], ARRAY[3, 2, 8], 'SUM');
{8,4,8}

SELECT ub.util_array_integer(ARRAY[5, 2], ARRAY[3, 2, 8], 'MERGE');
{5,2,3,2,8}

SELECT ub.util_array_integer(ARRAY[5, 2], ARRAY[3, 2, 8], 'MERGE_UNIQUE');
{2,3,5,8}

SELECT ub.util_array_integer(ARRAY[5, 2], ARRAY[3, 2, 8], 'MERGE5');
{5,2,0,0,3,2,8}
*/
$BODY$;

ALTER FUNCTION ub.util_array_integer(integer[], integer[], text)
    OWNER TO postgres;
