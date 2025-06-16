-- FUNCTION: ub.util_array_merge(double precision[], double precision[], integer)

-- DROP FUNCTION IF EXISTS ub.util_array_merge(double precision[], double precision[], integer);

CREATE OR REPLACE FUNCTION ub.util_array_merge(
    lntotaldata double precision[],
    lnupdatedata double precision[],
    lnposid integer)
    RETURNS double precision[]
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_array_merge
@desc Quick merge of the second float array at the lnPosID of the first float array
@desc Aggregate function ub.agg_array_float_merge(lnUpdateData, lnPosID) is defined too

@param float[] $1  - initial float array
@param float[] $2 - float array to merge at lnPosID
@params integer $3 - position the second array should be inserted at

@return float[] - merged float array

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#float array #float array merge
*/

BEGIN

    RETURN 
        -- basic array
        lnTotalData
        || 
        -- zero values to fill the basic array till lnPosID
        array_fill(0::float, ARRAY[GREATEST(lnPosID - COALESCE(array_length(lnTotalData, 1), 0) - 1, 0)]) 
        ||
        -- merged array
        lnUpdateData;

END;
/*
@example

SELECT ub.util_array_merge(ARRAY[0.5, 2.1], ARRAY[0.3, 2.1, 5.5], 5);
{0.5,2.1,0,0,0.3,2.1,5.5}
*/
$BODY$;

ALTER FUNCTION ub.util_array_merge(double precision[], double precision[], integer)
    OWNER TO postgres;
