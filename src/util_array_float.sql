-- FUNCTION: ub.util_array_float(double precision[], double precision[], text)

-- DROP FUNCTION IF EXISTS ub.util_array_float(double precision[], double precision[], text);

CREATE OR REPLACE FUNCTION ub.util_array_float(
	lntotaldata double precision[],
	lnupdatedata double precision[],
	lcaggfunc text DEFAULT 'SUM'::text)
    RETURNS double precision[]
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_array_float
@desc Process two float arrays. Arrays with different sizes are automatically expanded to the largest one
@desc Aggregate function ub.agg_array_float(lnUpdateData, lcAggFunc) is defined too

@param float[] $1 - initial float array
@param float[] $2 - float array to sum up or merge with
@params text $3 - aggregate function
    - SUM: sum two float arrays, with adjusting their length
    - MERGE: merge two float arrays
    - MERGE_UNIQUE: merge two float arrays with eliminating duplicates
    - MERGE_NNN: merge the second array at NNN position

@return float[] - processed float array

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#float array #float array merge #array adjust #array aggregate
*/

BEGIN

    lcAggFunc := upper(lcAggFunc);
    
    CASE
        
        -- Sum two float arrays with adjusting their lenghts
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
  

        -- Merge two arrays 
        WHEN lcAggFunc = 'MERGE' THEN       

            RETURN lnTotalData || lnUpdateData;
      
        
        -- Merge the second array at position nnn 
        WHEN lcAggFunc ~ 'MERGE[0-9]{1,}' THEN

            RETURN
                -- basic array
                lnTotalData    
                ||
                -- zero values to fill
                array_fill(0::float, ARRAY[GREATEST(substr(lcAggFunc,6)::integer - COALESCE(array_length(lnTotalData, 1), 0) - 1, 0)])
                ||
                -- merged array
                lnUpdateData; 
  
  
        -- Merge two arrays with removing duplicates
        WHEN lcAggFunc = 'MERGE_UNIQUE' THEN

            RETURN 
                (
                    SELECT
                        array_agg(COALESCE(basic_id, update_id))
                    FROM unnest(lnTotalData) basic_id
                    FULL JOIN unnest(lnUpdateData) update_id ON
                        (update_id = basic_id)
                );
  
        -- unknown aggregate function => no processing
        ELSE

            RETURN lnTotalData;

    END CASE;

END;
/*
@example
SELECT ub.util_array_float(ARRAY[0.5, 2.1], ARRAY[0.3, 3.2, 5.5], 'SUM');
{0.8,5.3,5.5}

SELECT ub.util_array_float(ARRAY[0.5, 2.1], ARRAY[0.3, 2.1, 5.5], 'MERGE');
{0.5,2.1,0.3,2.1,5.5}

SELECT ub.util_array_float(ARRAY[0.5, 2.1], ARRAY[0.3, 2.1, 5.5], 'MERGE_UNIQUE');
{0.3,2.1,5.5,0.5}

SELECT ub.util_array_float(ARRAY[0.5, 2.1], ARRAY[0.3, 2.1, 5.5], 'MERGE5');
{0.5,2.1,0,0,0.3,2.1,5.5}
*/
$BODY$;

ALTER FUNCTION ub.util_array_float(double precision[], double precision[], text)
    OWNER TO postgres;
