CREATE SCHEMA IF NOT EXISTS ub
    AUTHORIZATION postgres;

GRANT ALL ON SCHEMA ub TO postgres;


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




/*
@function ub.agg_array_float
@desc Aggregate function for operations with float arrays:
    - SUM: sum two float arrays, with adjusting their length
    - MERGE: merge two float arrays
    - MERGE_UNIQUE: merge two float arrays with eliminating duplicates
    - MERGE_NNN: merge the second array at NNN position
*/
CREATE OR REPLACE AGGREGATE ub.agg_array_float(double precision[], text) (
    SFUNC = ub.util_array_float,
    STYPE = float8[] ,
    FINALFUNC_MODIFY = READ_ONLY,
    MFINALFUNC_MODIFY = READ_ONLY
);




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




/*
@function ub.agg_array_integer
@desc Aggregate function for operations with integer arrays:
    - SUM: sum two float arrays, with adjusting their length
    - MERGE: merge two float arrays
    - MERGE_UNIQUE: merge two float arrays with eliminating duplicates
    - MERGE_NNN: merge the second array at NNN position
*/
CREATE OR REPLACE AGGREGATE ub.agg_array_integer(integer[], text) (
    SFUNC = ub.util_array_integer,
    STYPE = int4[] ,
    FINALFUNC_MODIFY = READ_ONLY,
    MFINALFUNC_MODIFY = READ_ONLY
);




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




/*
@function ub.agg_array_merge
@desc Aggregate function to merge two float arrays at arbitrary position
*/
CREATE OR REPLACE AGGREGATE ub.agg_array_merge(double precision[], integer) (
    SFUNC = ub.util_array_merge,
    STYPE = float8[] ,
    FINALFUNC_MODIFY = READ_ONLY,
    MFINALFUNC_MODIFY = READ_ONLY
);





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




/*
@function ub.agg_jsonb_array
@desc Aggregate function to process two jsonb arrays
    - replace - replace "initial" array with "merged" array 
    - expand - add all new keys from "merged" array to "initial" array (exclude duplicates) 
    - add - add all keys from "merged" array to "initial" array (duplicates are possible) (default)
    - sub - subtract elements of the merged array from initial array
    - intersect - calculate common keys in both array
*/
CREATE OR REPLACE AGGREGATE ub.agg_jsonb_array(jsonb, text) (
    SFUNC = ub.util_jsonb_array,
    STYPE = jsonb ,
    FINALFUNC_MODIFY = READ_ONLY,
    MFINALFUNC_MODIFY = READ_ONLY
);




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




/*
@function ub.agg_jsonb_concat
@desc Aggregate function to concatenate two jsonb arrays
*/
CREATE OR REPLACE AGGREGATE ub.agg_jsonb_concat(jsonb) (
    SFUNC = ub.util_jsonb_concat,
    STYPE = jsonb ,
    FINALFUNC_MODIFY = READ_ONLY,
    MFINALFUNC_MODIFY = READ_ONLY
);




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




/*
@function ub.agg_jsonb_merge
@desc Aggregate function to merge two multi-level jsonb arrays
*/
CREATE OR REPLACE AGGREGATE ub.agg_jsonb_merge(jsonb, text) (
    SFUNC = ub.util_jsonb_merge,
    STYPE = jsonb ,
    FINALFUNC_MODIFY = READ_ONLY,
    MFINALFUNC_MODIFY = READ_ONLY
);





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




-- FUNCTION: ub.util_jsonb_nest(jsonb, text)

-- DROP FUNCTION IF EXISTS ub.util_jsonb_nest(jsonb, text);

CREATE OR REPLACE FUNCTION ub.util_jsonb_nest(
    ljinitial jsonb,
    lcarrayflag text DEFAULT 'replace'::text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_jsonb_nest
@desc convert object like { "key1.key2.key3": <value> } INTO { "key1": { "key2": { "key3": <value> }} 

@param object $1  - initial object to nest
@param string|null $2 - how to merge arrays
    - replace - replace "initial" array with "merged" array (default)
    - expand - add all new keys from "merged" array to "initial" array (exclude duplicates)
    - add - add all keys from "merged" array to "initial" array (duplicates are possible)
    
@return object $1 - nested object

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#object #nest object
*/  
    
BEGIN
            
    IF jsonb_typeof(ljInitial) IS DISTINCT FROM 'object' THEN
        RETURN jsonb_build_object();
    END IF;

    RETURN COALESCE(
        (
            WITH 
            -- unnest all keys and calculate "parentKey" and "childKey"
            "parent_child" AS MATERIALIZED (
                SELECT
                    -- extract top-level key, considering quotes
                    COALESCE(
                        btrim((regexp_match(key, '^"[^"]{1,}"'))[1], '"'),
                        regexp_replace(key, '\.(?![\.\/]).*', '')
                    ) AS parent_key,
                    -- extract child key, considering quotes
                    (regexp_match(
                        -- remove top key in quotes
                        regexp_replace(key, '^"[^"]{1,}"', ''),
                        -- remove top key before "."
                        '(?<=\.(?=([^\.\/]))).*'
                    ))[1] AS child_key,
                    -- key value
                    value AS object_value
                FROM jsonb_each(ljInitial)
            ),

            -- select parent keys with non-nested child keys
            "data_not_nested" AS MATERIALIZED (
                SELECT
                    "parent_child".parent_key AS object_key,
                    jsonb_object_agg(
                        btrim("parent_child".child_key, '"'),
                        CASE
                            WHEN jsonb_typeof("parent_child".object_value) IS NOT DISTINCT FROM 'object'
                            THEN ub.util_jsonb_nest("parent_child".object_value, lcArrayFlag)
                            ELSE "parent_child".object_value
                        END
                    ) AS object_value
                FROM "parent_child"
                WHERE "parent_child".child_key IS NOT NULL
                    AND NOT regexp_replace("parent_child".child_key, '^"[^"]{1,}"', '') ~ '\.(?![\.\/])'
                GROUP BY 1
            ),

            -- select parent keys with no child keys
            "data_no_child" AS MATERIALIZED (
                SELECT
                    "parent_child".parent_key AS object_key,
                    CASE
                        WHEN jsonb_typeof("parent_child".object_value) IS NOT DISTINCT FROM 'object'
                        THEN ub.util_jsonb_nest("parent_child".object_value, lcArrayFlag)
                        ELSE "parent_child".object_value
                    END AS object_value
                FROM "parent_child"
                WHERE "parent_child".child_key IS NULL       
            ),

            -- select parent keys without child keys, and add non-nested child keys
            "data_merged" AS MATERIALIZED (
                SELECT
                    -- object key
                    COALESCE("data_no_child".object_key, "data_not_nested".object_key) AS object_key,
                    -- object value
                    CASE
                        WHEN "data_no_child".object_key IS NULL THEN 
                            -- one of objects is empty => assign the second object
                            "data_not_nested".object_value
                        WHEN "data_not_nested".object_key IS NULL THEN 
                            -- one of objects is empty => assign the second object
                            "data_no_child".object_value
                        WHEN jsonb_typeof("data_not_nested".object_value) IS NOT DISTINCT FROM 'array' THEN 
                            -- "array" type => merge arrays
                            ub.util_jsonb_array("data_no_child".object_value, "data_not_nested".object_value, lcArrayFlag)
                        WHEN jsonb_typeof("data_not_nested".object_value) IS DISTINCT FROM 'object' THEN 
                            -- one of values is scalar => replace the value
                            "data_not_nested".object_value
                        ELSE
                            -- merge two objects
                            ub.util_jsonb_merge("data_no_child".object_value, "data_not_nested".object_value, lcArrayFlag)
                    END AS object_value
                FROM "data_no_child"
                FULL JOIN "data_not_nested" ON
                    "data_not_nested".object_key = "data_no_child".object_key
                WHERE COALESCE("data_no_child".object_key, "data_not_nested".object_key) IS NOT NULL
            ),

            -- select parent keys with nested child keys
            "data_nested" AS MATERIALIZED (
                SELECT
                    "parent_child".parent_key AS object_key,
                    ub.util_jsonb_nest(
                        jsonb_object_agg(
                            "parent_child".child_key,
                            "parent_child".object_value
                        ),
                        lcArrayFlag
                    ) AS object_value
                FROM "parent_child"
                WHERE "parent_child".child_key IS NOT NULL
                    AND regexp_replace("parent_child".child_key, '^"[^"]{1,}"', '') ~ '\.(?![\.\/])'
                GROUP BY 1
            )

            -- Aggregate all object keys and values
            SELECT
            
                jsonb_object_agg(
                    -- object key
                    COALESCE("data_merged".object_key, "data_nested".object_key),
                    -- object value
                    CASE
                        WHEN "data_nested".object_key IS NULL THEN 
                            -- one of objects is empty => assign the second object
                            "data_merged".object_value
                        WHEN "data_merged".object_key IS NULL THEN 
                            -- one of objects is empty => assign the second object
                            "data_nested".object_value
                        WHEN jsonb_typeof("data_merged".object_value) IS DISTINCT FROM 'object' THEN 
                            -- one of values is not an object => replace the value
                            "data_nested".object_value
                        ELSE
                            -- merge two objects
                            ub.util_jsonb_merge("data_merged".object_value, "data_nested".object_value, lcArrayFlag)
                    END
                )
            
            FROM "data_merged"
            FULL JOIN "data_nested" ON
                "data_nested".object_key = "data_merged".object_key
            WHERE COALESCE("data_merged".object_key, "data_nested".object_key) IS NOT NULL
        ),
        jsonb_build_object()
    );
    
END;
/*
@example:
    SELECT ub.util_jsonb_nest('{"a.b.c": 1, "a.b.d": 5}'::jsonb)
    => {"a":{"b":{"c":1,"d":5}}}
    
    SELECT ub.util_jsonb_nest('{"a.b.c": [1,2], "a.b": {"c": [2,3]}}'::jsonb, 'add')
    => {"a":{"b":{"c":[2,3,1,2]}}}
    
    SELECT ub.util_jsonb_nest('{"a.b.c": 1, "\"a.b.d\"": 5}'::jsonb)
    => {"a":{"b":{"c":1}},"a.b.d":5}
*/
$BODY$;

ALTER FUNCTION ub.util_jsonb_nest(jsonb, text)
    OWNER TO postgres;




-- FUNCTION: ub.util_jsonb_unnest(jsonb, text, text)

-- DROP FUNCTION IF EXISTS ub.util_jsonb_unnest(jsonb, text, text);

CREATE OR REPLACE FUNCTION ub.util_jsonb_unnest(
    ljinitial jsonb,
    lckeyprefix text DEFAULT NULL::text,
    lcdelimiter text DEFAULT '.'::text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_jsonb_unnest
@desc convert all objects like { "key1": { "key2": { "key3": <value> }} into { "key1.key2.key3": <value> }
@desc any key prefix and delimiter are supported

@param object $1  - initial object to unnest
@param string|null $2 - key prefix for the keys in the object
@return object - unnested object

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#object #unnest object
*/  
    
BEGIN
            
    RETURN COALESCE(
        (
            WITH
            -- unnest object and calculate type for each key
            "unnest_object" AS MATERIALIZED (
                SELECT
                    concat((lcKeyPrefix || lcDelimiter), "top_data".key) AS key, 
                    "top_data".value,
                    jsonb_typeof(nullif("top_data".value, jsonb_build_object())) AS value_type
                FROM jsonb_each(ljInitial) AS top_data(key, value)
            ),

            -- unnest all selected objects and calculate new key name and type for each key
            "value_object" AS MATERIALIZED (
                SELECT
                    concat("unnest_object".key, lcDelimiter, "nested_data".key) AS key, 
                    "nested_data".value,
                    jsonb_typeof("nested_data".value) AS value_type     
                FROM "unnest_object"
                LEFT JOIN jsonb_each("unnest_object".value) AS nested_data(key, value) ON true
                WHERE "unnest_object".value_type IS NOT DISTINCT FROM 'object'
            )

            -- Build response
            SELECT
                -- select all non-objects for the top level
                COALESCE(
                    (
                        SELECT
                            jsonb_object_agg(
                                "unnest_object".key,
                                "unnest_object".value
                            )
                        FROM "unnest_object"
                        WHERE "unnest_object".value_type IS DISTINCT FROM 'object'
                    ),
                    jsonb_build_object()
                )

                ||
                -- select all non-objects for the second level
                COALESCE(
                    (
                        SELECT
                            jsonb_object_agg(
                                "value_object".key,
                                "value_object".value
                            )
                        FROM "value_object"
                        WHERE "value_object".value_type IS DISTINCT FROM 'object'
                    ),
                    jsonb_build_object()
                )

                ||

                -- unnest all objects recursively using ub.util_jsonb_unnest()
                COALESCE(
                    (   
                        SELECT
                            ub.agg_jsonb_concat(
                                ub.util_jsonb_unnest(
                                    "value_object".value,
                                    "value_object".key
                                )
                            )
                        FROM "value_object"
                        WHERE "value_object".value_type IS NOT DISTINCT FROM 'object'
                    ),
                    jsonb_build_object()
                )
        ),
        jsonb_build_object()
    );
    
END;
/*
@example:
    SELECT ub.util_jsonb_unnest('{"a":{"b":{"c": 1, "d": 5}}}'::jsonb)
    => {"a.b.c":1,"a.b.d":5}
    
    SELECT ub.util_jsonb_unnest('{"a":{"b":{"c": 1, "d": 5}}}'::jsonb, 'prefix_', '#')
    => {"prefix_#a#b.c":1,"prefix_#a#b.d":5}
*/
$BODY$;

ALTER FUNCTION ub.util_jsonb_unnest(jsonb, text, text)
    OWNER TO postgres;




-- FUNCTION: ub.util_jsonb_update(jsonb, jsonb)

-- DROP FUNCTION IF EXISTS ub.util_jsonb_update(jsonb, jsonb);

CREATE OR REPLACE FUNCTION ub.util_jsonb_update(
    ljinitialobject jsonb,
    ljpathvalue jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_jsonb_update
@desc update object with new values at specified paths
@desc basic rules for json keys:
    - [a-zA-Z0-9_]{1,}.* - parent and child key(s)
    - "<key_name>" - process key name within quotes as one key (ignore ".", any symbols are allowed)
    - (<jsonpath>) - process specific object (or its key) in array

@param object $1 - initial object
@param object $2 - object with paths and values
    @key any <path> - new value to plase at the <path>
    
@return object $1 - updated object

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#object #update object #object path
*/

DECLARE

    -- value position in calculated array
    JSON_KEY_TYPE       CONSTANT integer := 1;
    PARENT_KEY_POS      CONSTANT integer := 2;
    CHILD_KEY_POS       CONSTANT integer := 3;
    ARRAY_JSONPATH_POS  CONSTANT integer := 4;
    
    
BEGIN

    -- Nothing to update => return initial object
    IF jsonb_typeof(ljPathValue) IS DISTINCT FROM 'object' THEN

        RETURN
            CASE
                WHEN jsonb_typeof(ljInitialObject) IS DISTINCT FROM 'object'
                THEN jsonb_build_object()
                ELSE ljInitialObject
            END;
        
    -- Replace the initial object
    ELSEIF ljPathValue->>'*' IS NOT NULL THEN
    
        RETURN ljPathValue->'*';
    
    -- Expand the initial object
    ELSEIF ljPathValue->>'||' IS NOT NULL THEN
    
        RETURN ub.util_jsonb_merge(
            ljInitialObject,
            ljPathValue->'||',
            'replace'
        );
        
    END IF;
    
    
    
    -- Build response
    RETURN ub.util_jsonb_concat(
        
        -- initial object
        ljInitialObject,
        
        -- concatenate with updated keys 
        (
            WITH
            -- unnest all keys and calculate parent key, child key and array clause
            "parent_child" AS MATERIALIZED (
                SELECT
    
                    -- calculate parent key, child key and array clause
                    CASE
                        -- [a-zA-Z0-9_]{1,}.* format
                        WHEN "json_path".key ~ '^[a-zA-Z0-9_]' THEN
                            ARRAY[
                                -- type = "basic"
                                'basic',
                                -- parent key: a."b.c" => a
                                (regexp_match("json_path".key, '^[^\.]{1,}'))[1],
                                -- child key: a."b.c" => "b.c"
                                nullif(regexp_replace("json_path".key, '^[^\.]{1,}\.{0,1}', ''), ''),
                                -- array clause
                                NULL::text
                            ]
                                
                        -- ../ format (relative path)
                        WHEN "json_path".key ~ '^\.\.' THEN
                            ARRAY[
                                -- type = "basic"
                                'basic',
                                -- parent key: ../../a.b => ../../a
                                regexp_replace("json_path".key, '\.(?![\.\/]).*', ''),
                                -- child key: ../../a.b => b
                                (regexp_match("json_path".key, '(?<=\.(?=([^\.\/]))).*'))[1],
                                -- array clause
                                NULL::text
                            ]
                                
                        -- "..." format
                        WHEN "json_path".key ~ '^"' THEN
                            ARRAY[
                                -- type = "basic"
                                'basic',
                                -- parent key: "a.b".c => a.b
                                btrim((regexp_match("json_path".key, '^"[^\"]{0,}"'))[1], '"'),
                                -- child key: "a.b".c => c
                                nullif(regexp_replace("json_path".key, '^"[^\"]{0,}"\.{0,1}', ''), ''),
                                -- array clause
                                NULL::text
                            ]
                
                        -- "(...)" format
                        WHEN "json_path".key ~ '^\(' AND "json_path".key ~ '\$\.' THEN
                            
                            CASE
                                WHEN nullif(regexp_replace(
                                    (regexp_match("json_path".key, '(?<=(\$\.))[^\[]{1,}'))[1], 
                                    '((^"[^\"]{0,}"\.{0,1})|(^[^\.]{1,}\.{0,1}))', ''), '') IS NOT NULL 
                                -- array is in inside another object => process the object first
                                THEN 
                                    ARRAY[
                                        -- type = "basic"
                                        'basic',
                                        -- parent key: (($.a."b.c"[*] ? (@.id == 2)).b[*] ? (@.id == 1)).g => a
                                        btrim((regexp_match("json_path".key, '(?<=(\$\.))(("[^\"]{0,}")|([^\.\[]{1,}))'))[1], '"'),
                                        -- child key: remove parent key => (($."b.c"[*] ? (@.id == 2)).b[*] ? (@.id == 1)).g
                                        nullif(regexp_replace("json_path".key, '(?<=(\$\.))(("[^\"]{0,}")|([^\.\[]{1,}))\.{0,1}', ''), ''),
                                        -- array clause
                                        NULL::text
                                    ]
                                -- array is on the top level => start processing the array
                                ELSE
                                    ARRAY[
                                        -- type = "array"
                                        'array',
                                        -- parent key: (($."a.b"[*] ? (@.id == 2)).b[*] ? (@.id == 1)).g => a.b
                                        btrim((regexp_match("json_path".key, '(?<=(\$\.))(("[^\"]{0,}")|([^\.\[]{1,}))'))[1], '"'),
                                        -- child key: (($."a.b"[*] ? (@.id == 2)).b[*] ? (@.id == 1)).g => ($.b[*] ? (@.id == 1)).g
                                        nullif(nullif(regexp_replace(
                                            "json_path".key, 
                                            '\(\$\.[^\[]{1,}\[[^\]]{1,}\][^\(]{1,}\([^\)]{1,}\)\)', '$'), '$'), ''),
                                        -- array clause: (($."a.b"[*] ? (@.id == 2)).b[*] ? (@.id == 1)).g => $.id == 2
                                        concat('$', (regexp_match("json_path".key, '(?<=(\(\@))[^\)]{1,}'))[1])
                                    ]
                            END
                
                        -- empty key
                        WHEN "json_path".key ~ '^\.[^\.]' THEN
                            ARRAY[
                                -- type = "basic"
                                'basic',
                                -- parent key: a."b.c" => a
                                '',
                                -- child key: a."b.c" => "b.c"
                                nullif(regexp_replace("json_path".key, '^\.', ''), ''),
                                -- array clause
                                NULL::text
                            ]
                                
                        -- unknows format => skip the key
                        ELSE NULL::text[]
                
                    END AS key_data,
            
                    -- value to set
                    value AS object_value
                
                FROM jsonb_each(ljPathValue) json_path(key, value)
            ),
            
            
            -- prepare parent key and action name (insert | update | delete)
            "parent_array_list" AS MATERIALIZED (
                SELECT
                
                    -- each unique array key name will have order_id = 1
                    ROW_NUMBER() OVER(PARTITION BY "parent_child".key_data[PARENT_KEY_POS]) AS order_id,
                
                    -- array key name
                    "parent_child".key_data[PARENT_KEY_POS] AS parent_key,
                
                    -- jsonpath expression to find array element
                    "parent_child".key_data[ARRAY_JSONPATH_POS] AS array_jsonpath,
                
                    "parent_child".key_data[CHILD_KEY_POS] AS child_key,
                
                    -- detect the action name
                    CASE
                        -- child key not empty
                        WHEN "parent_child".key_data[CHILD_KEY_POS] IS NOT NULL THEN
                            'child'
                        -- $.id == 0 => add a new element
                        WHEN jsonb_build_object('id', 0) @@ "parent_child".key_data[ARRAY_JSONPATH_POS]::jsonpath THEN
                            'insert'
                        -- value is NULL => delete the element
                        WHEN COALESCE(jsonb_typeof("parent_child".object_value), 'null') = 'null' THEN
                            'delete'
                        -- update the element
                        ELSE 'update'
                    END AS action_name,
                
                    -- object with new element data
                    "parent_child".object_value
                
                FROM "parent_child"
                WHERE "parent_child".key_data[JSON_KEY_TYPE] IS NOT DISTINCT FROM 'array'
            ),
            
            
            -- group all child keys by parent key and jsonpath expression
            "parent_array_child" AS MATERIALIZED (
                SELECT
                    -- array key name
                    "parent_array_list".parent_key,
                    -- jsonpath expression to find array element
                    "parent_array_list".array_jsonpath,
                    -- aggregated object to update the array element
                    jsonb_object_agg(
                        -- remove "$." prefix for object keys
                        regexp_replace("parent_array_list".child_key, '^\$\.', ''),
                        "parent_array_list".object_value
                    ) AS object_value
                FROM "parent_array_list"
                WHERE "parent_array_list".action_name = 'child'
                GROUP BY 1, 2
            ),
            
            
            -- prepare list of array to insert new elements
            "parent_array_insert" AS MATERIALIZED (
                SELECT
                    -- array key name
                    "parent_array_list".parent_key,
                
                    -- row ID
                    ROW_NUMBER() OVER(PARTITION BY "parent_array_list".parent_key) AS row_id,
                
                    -- object with new element data
                    "parent_array_list".object_value
                
                FROM "parent_array_list"
                WHERE "parent_array_list".action_name = 'insert'
            ),
            
            
            -- calculate maximal "id" value for each array
            "parent_array_insert_max" AS MATERIALIZED (
                SELECT
                    -- array key name
                    "parent_array_insert".parent_key,
                
                    CASE
                        WHEN jsonb_typeof(ljInitialObject->("parent_array_insert".parent_key)) IS NOT DISTINCT FROM 'array'
                        -- calculate maximal "id"
                        THEN COALESCE(
                            (
                                SELECT
                                    MAX((array_element->>'id')::bigint)
                                FROM jsonb_array_elements(ljInitialObject->("parent_array_insert".parent_key)) array_element
                            ), 0)
                        ELSE 0
                    END AS maximal_id
                
                FROM "parent_array_insert"
                WHERE "parent_array_insert".row_id = 1
            ),
            
            
            -- group parent keys with "update" and "delete" actions
            "parent_array_update" AS MATERIALIZED (
                SELECT
                    -- array key name
                    "parent_array_list".parent_key,
                
                    -- jsonpath expression to find array element
                    "parent_array_list".array_jsonpath,
                
                    -- merge values for the same array element
                    ub.agg_jsonb_merge(
                        "parent_array_list".object_value,
                        'replace'
                    ) AS object_value
                
                FROM "parent_array_list"
                WHERE "parent_array_list".action_name = 'update'
                GROUP BY 1, 2
            ),
            
            
            -- group parent keys with "update" and "delete" actions
            "parent_array_delete" AS MATERIALIZED (
                SELECT
                    -- array key name
                    "parent_array_list".parent_key,
                
                    -- jsonpath expression to find array element
                    "parent_array_list".array_jsonpath
                
                FROM "parent_array_list"
                WHERE "parent_array_list".action_name = 'delete'
                GROUP BY 1, 2
            ),
            
            
            -- calculate values for all parent keys
            "parent_key_group" AS MATERIALIZED (
            (   -- update all nested objects recursively
                SELECT
                    -- parent key => group by its key name
                    "parent_child".key_data[PARENT_KEY_POS] AS parent_key,
                    -- update all child objects recursively
                    ub.util_jsonb_update(
                        -- current value of the parent key
                        ljInitialObject->("parent_child".key_data[PARENT_KEY_POS]),
                        -- object with updates on the parent key
                        jsonb_object_agg("parent_child".key_data[CHILD_KEY_POS], "parent_child".object_value)
                    ) AS object_value
                FROM "parent_child"
                WHERE "parent_child".key_data[CHILD_KEY_POS] IS NOT NULL
                    AND "parent_child".key_data[JSON_KEY_TYPE] IS NOT DISTINCT FROM 'basic'
                GROUP BY 1
            )
                
            UNION
            (   -- add all objects at leaf level (no child objects)
                SELECT
                    "parent_child".key_data[PARENT_KEY_POS] AS parent_key,
                    "parent_child".object_value
                FROM "parent_child"
                WHERE "parent_child".key_data[CHILD_KEY_POS] IS NULL
                    AND "parent_child".key_data[JSON_KEY_TYPE] IS NOT DISTINCT FROM 'basic'
            )
                
            UNION
            (   -- process array data
                SELECT
                    -- array key name
                    "parent_array_list".parent_key,
                    
                    -- update all elements and add new elements
                    ub.util_jsonb_array(
                    (   -- update all elements in the array matched with jsonpath expressions
                        SELECT
                            jsonb_agg(
                                CASE
                                    -- update the element
                                    WHEN "parent_array_update".array_jsonpath IS NOT NULL THEN
                                        "parent_array_update".object_value
                                    -- apply changes to all child elements
                                    WHEN "parent_array_child".array_jsonpath IS NOT NULL THEN
                                        ub.util_jsonb_update(
                                            -- current value of the element
                                            array_element,
                                            -- object with updates on the element
                                            "parent_array_child".object_value
                                        )
                                    -- not matched 
                                    ELSE array_element
                                END
                            )
                        -- unnest current array
                        FROM jsonb_array_elements(ljInitialObject->("parent_array_list".parent_key)) array_element
                        -- join with list of deleted elements
                        LEFT JOIN "parent_array_delete" ON
                            "parent_array_delete".parent_key = "parent_array_list".parent_key
                            AND array_element @@ "parent_array_delete".array_jsonpath::jsonpath
                        -- join with list of updated elements
                        LEFT JOIN "parent_array_update" ON
                            "parent_array_update".parent_key = "parent_array_list".parent_key
                            AND array_element @@ "parent_array_update".array_jsonpath::jsonpath
                        -- join with list with child elements
                        LEFT JOIN "parent_array_child" ON
                            "parent_array_child".parent_key = "parent_array_list".parent_key
                            AND array_element @@ "parent_array_child".array_jsonpath::jsonpath
                        -- check the array is exists
                        WHERE jsonb_typeof(ljInitialObject->("parent_array_list".parent_key)) IS NOT DISTINCT FROM 'array'
                            -- skip all deleted elements
                            AND "parent_array_delete".array_jsonpath IS NULL
                    ),
                        
                    (   -- add new elements
                        SELECT
                            jsonb_agg(
                                ub.util_jsonb_concat(
                                    -- new object
                                    "parent_array_insert".object_value,
                                    -- unique "id" value
                                    jsonb_build_object(
                                        'id', "parent_array_insert_max".maximal_id + "parent_array_insert".row_id
                                    )
                                )
                                ORDER BY "parent_array_insert".row_id
                            )
                        FROM "parent_array_insert"
                        LEFT JOIN "parent_array_insert_max" ON
                            "parent_array_insert_max".parent_key = "parent_array_insert".parent_key
                        WHERE "parent_array_insert".parent_key = "parent_array_list".parent_key
                    ),
                        
                    'add'
                ) AS object_value
                
                FROM "parent_array_list"
                WHERE "parent_array_list".order_id = 1
            ))
            
            
            -- build new values for all parent keys
            SELECT
                jsonb_object_agg(
                    "parent_key_group".parent_key,
                    "parent_key_group".object_value
                )
            FROM "parent_key_group"
            WHERE "parent_key_group".parent_key IS NOT NULL
        )
    );
    
END;
/*
@example:
    SELECT ub.util_jsonb_update('{"a":{"b":{"c": 1}, "f": 10}}'::jsonb, '{"a.b":{"d": 5}}'::jsonb)
    => {"a":{"b":{"d":5},"f":10}} (update at specific json path)
    
    SELECT ub.util_jsonb_update('{"a":[{"id": 1, "b": 1}, {"id": 2, "b": [{"id": 1, "f": 3}] }]}'::jsonb, '{"(($.a[*] ? (@.id == 2)).b[*] ? (@.id == 1)).g": 10}'::jsonb)
    => {"a":[{"id":1, "b":1}, {"id":2, "b":[{"f":3,"g":10,"id":1}]}]} (update keys in the object at specific jsonpath)
    
    SELECT ub.util_jsonb_update('{"a":[{"id": 1, "b": 1}, {"id": 2, "b": [{"id": 1, "f": 3}] }]}'::jsonb, '{"(($.a[*] ? (@.id == 2)).b[*] ? (@.id == 1))": {"id": 1, "a": 10} }'::jsonb)
    => {"a":[{"b":1, "id":1}, {"b":[{"a":10,"id":1}], "id":2}]} (replace object at specific jsonpath)
    
    SELECT ub.util_jsonb_update('{"a":[{"id": 1, "b": 1}, {"id": 2, "b": [{"id": 1, "f": 3}] }] }'::jsonb, '{"(($.a[*] ? (@.id == 2)).b[*] ? (@.id == 0 && 17689 > 0))": {"f": 5}}'::jsonb)
    => {"a":[{"b":1,"id":1}, {"b":[{"f":3,"id":1},{"f":5,"id":2}], "id":2}]} (add object at specific jsonpath. Add a random value (e.g. 17689) to keep the whole key name unique)
    
    SELECT ub.util_jsonb_update('{"a":[{"id": 1, "b": 1}, {"id": 2, "b": [{"id": 1, "f": 3}, {"id": 2, "f": 10}]}]}'::jsonb, '{"(($.a[*] ? (@.id == 2)).b[*] ? (@.id == 1))": null}'::jsonb)
    => {"a":[{"b":1,"id":1},{"b":[{"f":10,"id":2}],"id":2}]} (delete object at specific jsonpath)
    
    SELECT ub.util_jsonb_update('{"a":{"b":{"c": 1}}}'::jsonb, '{"*":{"d": 5}}'::jsonb)
    => {"d": 5} (replace with a new object)

    SELECT ub.util_jsonb_update('{"a":{"b":{"c": 1}}}'::jsonb, '{"||":{"a": {"b": {"d": 5}}}}'::jsonb)
    => {"a":{"b":{"c": 1, "d": 5}}} (expand the initial object)
*/
$BODY$;

ALTER FUNCTION ub.util_jsonb_update(jsonb, jsonb)
    OWNER TO postgres;




-- FUNCTION: ub.util_jsonb_differ(jsonb, jsonb, text)

-- DROP FUNCTION IF EXISTS ub.util_jsonb_differ(jsonb, jsonb, text);

CREATE OR REPLACE FUNCTION ub.util_jsonb_differ(
    ljinitialobject jsonb,
    ljupdatedobject jsonb,
    lcarrayflag text DEFAULT 'order_no_matter'::text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_jsonb_differ
@desc prepare list of keys in the "updated object" that are differ from the same keys in the "initial object"

@param object $1 - initial object
@param object $2 - updated object
@param string $3 - flag how to compare arrays
    - order_no_matter = order of elements doesn't matter (default)
    - order_matter = order of elements matters
    
@return object $1 - object with new key values (NULL if no new key values)

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#object #compare object #difference
*/

BEGIN

    IF jsonb_typeof(ljInitialObject) IS DISTINCT FROM 'object' THEN

        -- initial object is null => return the updated object
        RETURN ljUpdatedObject;
        
    ELSEIF jsonb_typeof(ljUpdatedObject) IS DISTINCT FROM 'object' THEN
    
        -- updated object is null => return the second one
        RETURN NULL::jsonb;
        
    END IF;
    
    -- Build response
    RETURN 
        (
            SELECT
                 jsonb_object_agg("updated".key, "updated".value)
            FROM jsonb_each(ljUpdatedObject) AS updated(key, value)
            WHERE 
                -- add if two values are different
                jsonb_path_query_first(ljInitialObject, concat('$.', "updated".key)::jsonpath) IS DISTINCT FROM "updated".value
                AND
                -- null = null
                (jsonb_typeof("updated".value) != 'null' 
                 OR jsonb_path_query_first(ljInitialObject, concat('$.', "updated".key)::jsonpath) IS NOT NULL)
                AND
                -- check if arrays are different, order doesn't matter
                (lcArrayFlag IS NOT DISTINCT FROM 'order_matter'
                 OR jsonb_typeof("updated".value) IS DISTINCT FROM 'array'
                 OR jsonb_typeof(jsonb_path_query_first(ljInitialObject, concat('$.', "updated".key)::jsonpath)) IS DISTINCT FROM 'array'
                 OR NOT "updated".value @> jsonb_path_query_first(ljInitialObject, concat('$.', "updated".key)::jsonpath)
                 OR NOT "updated".value <@ jsonb_path_query_first(ljInitialObject, concat('$.', "updated".key)::jsonpath))
        );
    
END;
/*
@example:
    SELECT ub.util_jsonb_differ('{"a":{"b":{"c": 1}}}'::jsonb, '{"a.b.c": 1}'::jsonb)
    => null (no new keys)
    
    SELECT ub.util_jsonb_differ('{"a": [2, 3], "b": 10, "d": 20}'::jsonb, '{"a": [3, 4], "b": 10}'::jsonb)
    => {"a": [3, 4]}
    
    SELECT ub.util_jsonb_differ('{"a": [2, 3], "b": 10}'::jsonb, '{"a": [3, 4], "b": 10}'::jsonb, 'order_no_matter')
    => null (no new keys)
    
    SELECT ub.util_jsonb_differ('{"a": [2, 3], "b": 10}'::jsonb, '{"a": [3, 4], "b": 10}'::jsonb, 'order_matter')
    => {"a": [3, 2]} 
*/
$BODY$;

ALTER FUNCTION ub.util_jsonb_differ(jsonb, jsonb, text)
    OWNER TO postgres;




-- FUNCTION: ub.util_jsonb_process(jsonb, jsonb, text)

-- DROP FUNCTION IF EXISTS ub.util_jsonb_process(jsonb, jsonb, text);

CREATE OR REPLACE FUNCTION ub.util_jsonb_process(
    ljinitialobject jsonb,
    ljprocessdata jsonb,
    lcaction text DEFAULT 'OBJECT'::text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_jsonb_process
@desc Process a json objects / array with rules, described in the third parameter
@desc Aggregate function ub.agg_jsonb_process(ljProcessData, lcAction) is defined too

@param object|array $1  - initial object or array
@param object|array $2 - object or array with process rules
@params text $3 -
    - JSON_TO_PLAIN_ARRAY: convert any json with nested arrays into plain array of objects with "path", "prefix", "value" keys
    - JSON_DIFFERENCE: compare two json objects with building array of objects with differences
    - CHILDREN_FROM_PLAIN_ARRAY: build nested structure from plain array: [ { "key", "value", "parentKey" }] => { "value", "children": [ { "value", "children": [..] } ] }

@return object|array - processed object or array

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#array #object #nest object #unnest object #compare object #expand array #plain array #json aggregate #object aggregate
*/  
    
DECLARE

    -- Exception diagnostics
    lcArrayPrefix           text;                               -- calculated array prefix, e.g. "a:b"
    ljArrayPath             jsonb;                              -- calculated array path, e.g. ["a", "2", "b", "3"]
    lnArrayPosFlag          integer := 1;                       -- 1 = add array position
    lnArrayPos              integer;                            -- array position to process
  
    lcKeyParent             text;                               -- parent key to build array of objects
    lcKeyChild              text;                               -- child key to build array of objects
    ljKeyPrepared           jsonb;                              -- array of prepared parent keys
  
BEGIN

    lcAction := upper(lcAction);
    
    CASE
  
        WHEN lcAction = 'JSON_TO_PLAIN_ARRAY' THEN
        
            /*
            @desc **JSON_TO_PLAIN_ARRAY**
            @desc Convert any json with nested arrays into plain array with "prefix", "path", "child", "order", "value" keys
            
            @param array ljInitialObject - json object or array of objects to unnest
            @param null ljProcessData - for internal usage only
                @key array path - path (text array) to json array to unnest
            @param string lcAction - "JSON_TO_PLAIN_ARRAY" action
            
            @result array - plain array with objects consisting of "prefix", "path", "child", "order", "value keys only
                @key string prefix - key prefixes to the array, via colon
                @key array path - path (text array) to json array to unnest
                @key array child - list of keys with "array" json type
                @key number order - position of the object in the array
                @key object value - object with non-array keys
            #json #object #array #plain structure
            */
    
            IF jsonb_typeof(ljInitialObject) IS DISTINCT FROM 'array' THEN
            
                -- convert object to array for further processing
                ljInitialObject := jsonb_build_array(ljInitialObject);
                ljArrayPath := jsonb_build_array();
                lcArrayPrefix := '';
                lnArrayPosFlag := 0;
                
            ELSE
            
                -- recursive call => calculate array path & prefix
                ljArrayPath := 
                    CASE
                        WHEN jsonb_typeof(ljProcessData->'path') IS DISTINCT FROM 'array'
                        THEN jsonb_build_array()
                        ELSE ljProcessData->'path'
                    END;
                lcArrayPrefix := COALESCE(ljProcessData->>'prefix', '');
            
            END IF;

    
            RETURN COALESCE(
            (
                WITH
                -- unnest input array with assigning array positions
                "unnest_input_array" AS MATERIALIZED (
                    SELECT
                        ROW_NUMBER() OVER() AS array_position,
                        ub.util_jsonb_unnest(object_data) AS object_data
                    FROM jsonb_array_elements(ljInitialObject) object_data
                    WHERE jsonb_typeof(object_data) IS NOT DISTINCT FROM 'object'
                ),
                
                -- unnest each object recursively using "JSON_TO_PLAIN_ARRAY" mode
                "unnest_input_object" AS MATERIALIZED (
                    SELECT
                        jsonb_array_elements(
                            ub.util_jsonb_array(
                            (   -- build an object with all keys except arrays of objects
                                SELECT
                                    jsonb_build_array(
                                        jsonb_build_object(
                                            -- prefix, e.g. foo:a
                                            'prefix', lcArrayPrefix,
                                            -- path, e.g. ["foo", "1", "a"]
                                            'path',   ljArrayPath,
                                            -- position in the parent array (NULL for top-level object)
                                            'order',
                                                CASE
                                                    WHEN lnArrayPosFlag = 1
                                                    THEN "unnest_input_array".array_position
                                                    ELSE NULL::integer
                                                END,
                                            -- select all non "array of objects" keys
                                            'value', COALESCE(
                                                (
                                                    SELECT 
                                                        jsonb_object_agg("key_value".key, "key_value".value)
                                                    FROM jsonb_each("unnest_input_array".object_data) AS key_value(key, value)
                                                    WHERE 
                                                        jsonb_typeof("key_value".value) IS DISTINCT FROM 'array'
                                                        OR jsonb_array_length("key_value".value) = 0
                                                        OR jsonb_typeof("key_value".value->0) IS DISTINCT FROM 'object'
                                                ),
                                                jsonb_build_object()
                                            ),
                                            -- build list of "array of objects" keys
                                            'child', COALESCE(
                                                (
                                                    SELECT 
                                                        jsonb_agg("key_value".key)
                                                    FROM jsonb_each("unnest_input_array".object_data) AS key_value(key, value)
                                                    WHERE 
                                                        jsonb_typeof("key_value".value) IS NOT DISTINCT FROM 'array'
                                                        AND jsonb_array_length("key_value".value) > 0
                                                        AND jsonb_typeof("key_value".value->0) IS NOT DISTINCT FROM 'object'
                                                ),
                                                jsonb_build_array()
                                            )
                                        )
                                  )
                            ),
                            -- unnest each array of objects recursively using "JSON_TO_PLAIN_ARRAY" mode
                            (
                                SELECT
                                    ub.agg_jsonb_array(
                                        ub.util_jsonb_process(
                                            "array_key_value".value, 
                                            jsonb_build_object(
                                                -- add position and array key to the path
                                                'path',  
                                                    ljArrayPath
                                                    ||
                                                    CASE
                                                        WHEN lnArrayPosFlag = 1
                                                        THEN jsonb_build_array("unnest_input_array".array_position::text)
                                                        ELSE jsonb_build_array()
                                                    END
                                                    ||
                                                    jsonb_build_array("array_key_value".key),
                                                -- key prefix, with ":" delimiter
                                                'prefix', concat((nullif(lcArrayPrefix, '') || ':'), "array_key_value".key)
                                            ),
                                            'JSON_TO_PLAIN_ARRAY'
                                        ),
                                        'add'
                                    )
                                FROM jsonb_each("unnest_input_array".object_data) AS array_key_value(key, value)
                                WHERE jsonb_typeof("array_key_value".value) IS NOT DISTINCT FROM 'array'
                                    AND jsonb_array_length("array_key_value".value) > 0
                                    AND jsonb_typeof("array_key_value".value->0) IS NOT DISTINCT FROM 'object'
                            ), 
                            'add'
                        )) AS object_details
                    FROM "unnest_input_array"
                )
                
                -- build response as a plain array
                SELECT
                    jsonb_agg(
                        "unnest_input_object".object_details
                    )
                FROM "unnest_input_object"
                WHERE "unnest_input_object".object_details->>'prefix' IS NOT NULL
           ),
           jsonb_build_array());
        
        
        
        
        WHEN lcAction = 'JSON_DIFFERENCE' THEN

            /*
            @desc **JSON_DIFFERENCE**
            @desc Compare two json objects with building array of objects with differences
            
            @param object|array ljInitialObject - "left" json object or array of objects (expected values)
            @param object|array ljProcessData - "right" json object or array of objects (result values)
            @param string lcAction - "JSON_DIFFERENCE" action
            
            @return object|array - array with details on differences
                @key array path - path to an object with differences, e.g. ["foo", "2", "bar"]
                @key number order - array position of the object with differences
                @key string key - object key with differences
                @key number|string|object|array expected - expected value
                @key number|string|object|array result - result value
                @key string|null expectedType - type of the expected value
                @key string|null resultType - type of the result value
            #json #object #array #compare #difference
            */
            
            RETURN (
                WITH
                -- convert "left" object into plain array using "JSON_TO_PLAIN_ARRAY" mode
                "left_object" AS MATERIALIZED (
                    SELECT
                        object_data->'path'   AS array_path,
                        object_data->>'order' AS array_pos,
                        (jsonb_each(object_data->'value')).key   AS object_key,
                        (jsonb_each(object_data->'value')).value AS object_value
                    FROM jsonb_array_elements(
                        ub.util_jsonb_process(
                            ljInitialObject, NULL::jsonb, 'JSON_TO_PLAIN_ARRAY'
                        )
                    ) object_data
                    WHERE jsonb_typeof(object_data->'value') IS NOT DISTINCT FROM 'object'
                ),

                -- convert "right" object into plain array using "JSON_TO_PLAIN_ARRAY" mode
                "right_object" AS MATERIALIZED (
                    SELECT
                        object_data->'path'   AS array_path,
                        object_data->>'order' AS array_pos,
                        (jsonb_each(object_data->'value')).key   AS object_key,
                        (jsonb_each(object_data->'value')).value AS object_value
                    FROM jsonb_array_elements(
                        ub.util_jsonb_process(
                            ljProcessData, NULL::jsonb, 'JSON_TO_PLAIN_ARRAY'
                        )
                    ) object_data            
                    WHERE jsonb_typeof(object_data->'value') IS NOT DISTINCT FROM 'object'
                ),
                
                -- compare each key in both objects
                "compare_object" AS MATERIALIZED (
                    SELECT
                        COALESCE("left_object".array_path, "right_object".array_path) AS array_path,
                        COALESCE("left_object".array_pos,  "right_object".array_pos)::integer AS array_pos,
                        COALESCE("left_object".object_key, "right_object".object_key) AS object_key,
                        "left_object".object_value  AS left_value,
                        "right_object".object_value AS right_value
                    FROM "left_object"
                    FULL JOIN "right_object" ON
                        "right_object".array_path = "left_object".array_path
                        AND "right_object".array_pos IS NOT DISTINCT FROM "left_object".array_pos
                        AND "right_object".object_key = "left_object".object_key
                    WHERE "left_object".object_value IS DISTINCT FROM "right_object".object_value
                )
                
                -- Build array of differences
                SELECT
                    jsonb_agg(
                        -- compare json values
                        jsonb_build_object(
                            'path',  "compare_object".array_path,
                            'order', "compare_object".array_pos,
                            'key',   "compare_object".object_key,
                            'expected',  "compare_object".left_value,
                            'result',    "compare_object".right_value
                        )
                        ||
                        -- compare difference on json value types too
                        CASE
                            WHEN
                                jsonb_typeof("compare_object".left_value) IS NOT NULL
                                AND jsonb_typeof("compare_object".right_value) IS NOT NULL
                                AND jsonb_typeof("compare_object".left_value) IS DISTINCT FROM jsonb_typeof("compare_object".right_value)
                            THEN jsonb_build_object(
                                    'expectedType', jsonb_typeof("compare_object".left_value),
                                    'resultType',   jsonb_typeof("compare_object".right_value)
                                )
                            ELSE jsonb_build_object()
                        END
                        ORDER BY "compare_object".array_path, "compare_object".array_pos, "compare_object".object_key
                    )
                FROM "compare_object"
            );        
    
    

        WHEN lcAction = 'CHILDREN_FROM_PLAIN_ARRAY' THEN
        
            /*
            @desc **CHILDREN_FROM_PLAIN_ARRAY**
            @desc Build nested structure from plain array of { "key", "value", "parentKey" } objects
            
            @param array ljInitialObject  - array of objects to nest
                @key number|string|object key - unique key of the object
                @key object value - list of keys with values to store
                @key number|string|object|null parentKey - parent key of the object
            @param null ljProcessData - settings
                @key string|null childKey - key name for the array with children data ("children" by default)
                @key string|null parentKey - parent key value to build data
                @key array|null preparedKeys - list of already prepared parent keys to exclude loops
            @param string lcAction - "CHILDREN_FROM_PLAIN_ARRAY" action
            
            @return object|array - json with children data
            #json #object #array #build #child #children
            */
            
            lcKeyParent := ljProcessData->>'parentKey';
            lcKeyChild := COALESCE(ljProcessData->>'childKey', 'children');
            ljKeyPrepared := COALESCE(ljProcessData->'preparedKeys', jsonb_build_array());
            
            RETURN (
                WITH
                -- unnest the array
                "unnest_data" AS MATERIALIZED (
                    SELECT
                        value->>'parentKey' AS parent_key,
                        value AS object_data
                    FROM jsonb_array_elements(ljInitialObject)
                    WHERE jsonb_typeof(ljInitialObject) IS NOT DISTINCT FROM 'array'
                )
                -- recursively build array of objects for specified parent key
                SELECT
                    jsonb_agg(
                        COALESCE("unnest_data".object_data->'value', jsonb_build_object())
                        ||
                        CASE
                            WHEN EXISTS 
                                (
                                    SELECT 1 
                                    FROM "unnest_data" child_data 
                                    WHERE "child_data".parent_key = "unnest_data".object_data->>'key'
                                )
                            THEN jsonb_build_object(
                                lcKeyChild, 
                                ub.util_jsonb_process(
                                    ljInitialObject, 
                                    jsonb_build_object(
                                        'childKey', lcKeyChild, 
                                        'parentKey', "unnest_data".object_data->>'key',
                                        'preparedKeys', ub.util_jsonb_array(
                                            ljKeyPrepared,
                                            jsonb_build_array(lcKeyParent),
                                            'add'
                                        )
                                    ),
                                    'CHILDREN_FROM_PLAIN_ARRAY'
                                )
                            )
                            ELSE jsonb_build_object()
                        END
                    )
                FROM "unnest_data"
                WHERE "unnest_data".parent_key IS NOT DISTINCT FROM lcKeyParent
                    AND NOT ljKeyPrepared ? ("unnest_data".object_data->>'key')
            );
    
    
    
        -- unknown function => return the initial object
        ELSE

            RETURN ljInitialObject;

    END CASE;

END;
/*
@example:
    SELECT ub.util_jsonb_process(
        '{ "foo": [ {"a": [{"b": 1}, {"b": 2}]} ], "bar": "info" }'::jsonb,
        NULL::jsonb,
        'JSON_TO_PLAIN_ARRAY'
    )
    =>
    [
        { "prefix": "", "path": [], "child": ["foo"], "value": { "bar": "info" } },
        { "prefix": "foo", "path": ["foo"], "child": ["a"], "order": 1, "value": {} },
        { "prefix": "foo:a", "path": ["foo", "1", "a"], "child": [], "order": 1, "value": {"b": 1} },
        { "prefix": "foo:a", "path": ["foo", "1", "a"], "child": [], "order": 2, "value": {"b": 2} }
    ]
    
    SELECT ub.util_jsonb_process(
        '{"a": [2, 3], "b": [{"f": 10}, {"f": 20}], "d": 20}'::jsonb, 
        '{"a": [3, 4], "b": [{"f": 20}, {"f": 30}], "d": 20}'::jsonb,
        'JSON_DIFFERENCE'
    )
    =>
    [
        {"key":"a", "path":[], "order":null, "result":[3,4], "expected":[2,3]},
        {"key":"f", "path":["b"], "order":1, "result":20, "expected":10},
        {"key":"f", "path":["b"], "order":2, "result":30, "expected":20}]
    ]
    
    SELECT ub.util_jsonb_process(
        '
            [
                { "key": "01", "value": { "info": "foo" }, "parentKey": null },
                { "key": "02", "value": { "info": "bar" }, "parentKey": "01" },
                { "key": "03", "value": { "info": "baz" }, "parentKey": "02" }
            ]        
        '::jsonb, 
        NULL::jsonb,
        'CHILDREN_FROM_PLAIN_ARRAY')
    => { "info": "foo", "children": [{ "info": "bar", "children": [{ "info": "baz" }] }] }
*/
$BODY$;

ALTER FUNCTION ub.util_jsonb_process(jsonb, jsonb, text)
    OWNER TO postgres;
    
    
    
    
-- FUNCTION: ub.util_data_modifier(jsonb)

-- DROP FUNCTION IF EXISTS ub.util_data_modifier(jsonb);

CREATE OR REPLACE FUNCTION ub.util_data_modifier(
    ljinput jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 10
    VOLATILE PARALLEL UNSAFE
AS $BODY$

/*
@function ub.util_data_modifier
@desc Process all modifiers for the calculated value

@param <any> value - calculated value
@param array modifier - array of modifier rules
    @key string type - type to convert the value
    @key string format - any valid format for the type
    @key string delimiter - delimiter to split string or aggregate string from array or object (\n = CHR(10))
    @key string timeZone - 3-letters time zone to calculate timestamp
    @key number pretty - 1 = jsonb_pretty()
    @key number strip - 1 = nullstrip() jsonb
    @key string regex - regular expression
    @key object|null validator - validation rules
        @key string|null jsonpath - jsonpath expression, e.g. "$ > 0 && $ < 100"
        @key number|null maxLength - maximal length of the value

@return <any> result - modified value
@return string message - details on the invalid input value

@version 0.2.1
@author Victor Dobrogorsky <progmer@unibackend.org>
@author Oleg Pravdin <o.pravdin@unibackend.org>
#mapping #mapping rules #modifier
*/

DECLARE

    -- Exception diagnostics
    lcExceptionContext          text;
    lcExceptionDetail           text;
    lcExceptionHint             text;
    lcExceptionMessage          text;
    lcExceptionState            text;

    -- Constants
    INITIAL_DATE                CONSTANT date := '2000-01-01'::date;        -- initial date to calculate date offset
    PASSWORD_ASTERIX_LENGTH     CONSTANT integer := 16;                     -- amount of "*" in password field
    
    -- Input parameters
    ljValue                     jsonb := ljInput->'value';                  -- initial value
    
    -- Local parameters
    ljModifierRule              jsonb;                                      -- modifier rule
    lcModifierType              text;                                       -- modifier type
    
    ljResult                    jsonb;                                      -- "result" object: { "result": <value>, "message": "<invalid>" }
    lcMessage                   text;                                       -- details on the invalid input value
    
    
BEGIN
    
    FOR ljModifierRule IN SELECT jsonb_array_elements(ljInput->'modifier') LOOP
        
        -- modifier type
        lcModifierType := ljModifierRule->>'type';
        
        
        -- calculate new key value
        ljResult := 
            CASE
                -- convert to a new format, without validation
                WHEN lcModifierType ~ '^f_' THEN
                    CASE
                        -- formatted number
                        WHEN lcModifierType = 'f_number' THEN
                            jsonb_build_object('result',
                                CASE
                                    WHEN ljModifierRule->>'format' IS NOT NULL 
                                    THEN to_jsonb(to_char((ljValue #>> '{}')::float, ljModifierRule->>'format'))
                                    ELSE to_jsonb((ljValue #>> '{}')::float)
                                END
                            )
                        
                        -- formatted date
                        WHEN lcModifierType = 'f_date' THEN
                            jsonb_build_object('result',
                                CASE
                                    -- date_2000 => formatted date
                                    WHEN (ljValue #>> '{}') IS NULL
                                        AND ljModifierRule->>'null_to_1970' IS NOT DISTINCT FROM '1' THEN
                                        '1970-01-01'
                                    WHEN ljModifierRule->>'format' IS NOT NULL THEN
                                        to_char(
                                            CASE
                                                WHEN (ljValue #>> '{}') ~ '^[\-]{0,1}[0-9]{1,}$' THEN
                                                    (INITIAL_DATE + (ljValue #>> '{}')::integer)
                                                ELSE (ljValue #>> '{}')::date
                                            END, 
                                            COALESCE(ljModifierRule->>'format', 'YYYY-MM-DD')
                                        )
                                    ELSE (ljValue #>> '{}')
                                END
                            )
                        
                        -- date_2000 format from any date format
                        WHEN lcModifierType = 'f_date_2000' THEN 
                            jsonb_build_object('result',
                                CASE
                                    WHEN (ljValue #>> '{}') ~ '^[\-]{0,1}[0-9]{1,}$'
                                    THEN (ljValue #>> '{}')::integer
                                    ELSE to_date((ljValue #>> '{}'), COALESCE(ljModifierRule->>'format', 'YYYY-MM-DD')) - INITIAL_DATE
                                END
                            )
                        
                        -- string value
                        WHEN lcModifierType = 'f_string' THEN 
                            jsonb_build_object('result',
                                CASE
                                    -- password as ****
                                    WHEN ljModifierRule->>'format' IS NOT DISTINCT FROM 'password' THEN 
                                        repeat('*', PASSWORD_ASTERIX_LENGTH)::text
                                    -- split atring | object | array with specified delimiter
                                    WHEN ljModifierRule->>'delimiter' IS NOT NULL THEN
                                        CASE
                                            -- {"key": "value"} => "<key1>: <value1>", "<key2>: <value2>" via delimiter
                                            WHEN jsonb_typeof(ljValue) = 'object' THEN
                                                (
                                                    SELECT
                                                        string_agg(
                                                            concat("item".key, ': ', "item".value), 
                                                            COALESCE(nullif(ljModifierRule->>'delimiter', '\n'), CHR(10))
                                                        )
                                                    FROM jsonb_each_text(ljValue) AS item(key, value)
                                                    WHERE jsonb_typeof(ljValue) IS NOT DISTINCT FROM 'object'
                                                )
                                            -- ["item 1", "item 2" ] => "item_1", "item_2" via delimiter
                                            WHEN jsonb_typeof(ljValue) = 'array' THEN
                                                (
                                                    SELECT
                                                        string_agg(
                                                            item, 
                                                            COALESCE(nullif(ljModifierRule->>'delimiter', '\n'), CHR(10))
                                                        )
                                                    FROM jsonb_array_elements_text(ljValue) AS item
                                                )
                                            -- split text via delimiter and place via CHR(10)
                                            ELSE 
                                                (
                                                    SELECT
                                                        string_agg(item, CHR(10))
                                                    FROM unnest(string_to_array(ljValue #>> '{}', ljModifierRule->>'delimiter')) item
                                                )
                                        END
                                    ELSE (ljValue #>> '{}')
                                END
                            )

                        -- checkbox value (format: "number<:true_value><:false_value>" or "boolean<:true_value><:false_value>")
                        WHEN lcModifierType = 'f_checkbox' THEN 
                            jsonb_build_object('result',
                                CASE
                                    WHEN ljModifierRule->>'format' IS NOT NULL
                                        AND ljModifierRule->>'format' ~ '\:'
                                    THEN to_jsonb(
                                        (CASE
                                            WHEN ljValue IS NOT DISTINCT FROM to_jsonb(1::integer)
                                                OR ljValue IS NOT DISTINCT FROM to_jsonb(true::boolean)
                                            THEN (regexp_match(ljModifierRule->>'format', '(?<=\:)[^\:]{1,}(?=\:)'))[1]
                                            ELSE (regexp_match(ljModifierRule->>'format', '(?<=\:)[^\:]{1,}$'))[1]
                                         END)::text)
                                    ELSE ljValue
                                END
                            )
                            
                        -- formatted timestamp
                        WHEN lcModifierType = 'f_timestamp' THEN
                            jsonb_build_object('result',
                                CASE
                                    WHEN (ljValue #>> '{}') IS NULL
                                        AND ljModifierRule->>'null_to_1970' IS NOT DISTINCT FROM '1' THEN
                                        '1970-01-01'
                                    WHEN ljModifierRule->>'format' IS NOT NULL THEN 
                                        to_char(
                                            timezone(
                                                COALESCE(ljModifierRule->>'timeZone', 'GMT'),
                                                CASE
                                                    WHEN (ljValue #>> '{}') ~ '^[0-9\.]{1,}$' THEN
                                                        to_timestamp((ljValue #>> '{}')::float)
                                                    ELSE (ljValue #>> '{}')::timestamp
                                                END
                                            ),
                                            COALESCE(ljModifierRule->>'format', 'YYYY-MM-DD HH24:MI:SS')
                                        )
                                    ELSE (ljValue #>> '{}')
                                END
                            )
                        -- UNIX timestamp from any date|timestamp format
                        WHEN lcModifierType = 'f_unix_timestamp' THEN 
                            jsonb_build_object('result',
                                EXTRACT(EPOCH FROM concat(to_timestamp(
                                    btrim(ljValue #>> '{}'), 
                                    COALESCE(ljModifierRule->>'format', 'YYYY-MM-DD HH24:MI:SS'))::timestamp,
                                    ' ', COALESCE(ljModifierRule->>'timeZone', 'GMT'))::timestamptz)::float)
                        
                        -- "object" | "array" value
                        WHEN lcModifierType IN ('f_object', 'f_array') THEN
                            jsonb_build_object('result',
                                CASE
                                    WHEN ljModifierRule->>'pretty' IS NOT DISTINCT FROM '1' THEN
                                        to_jsonb(jsonb_pretty(
                                            CASE 
                                                WHEN ljModifierRule->>'strip' IS NOT DISTINCT FROM '1'
                                                THEN jsonb_strip_nulls(ljValue)
                                                ELSE ljValue
                                            END
                                        ))
                                    WHEN ljModifierRule->>'strip' IS NOT DISTINCT FROM '1' THEN
                                        jsonb_strip_nulls(ljValue)
                                    -- string to object | array
                                    WHEN jsonb_typeof(ljValue) = 'string' THEN
                                        CASE
                                            WHEN lcModifierType = 'f_array'
                                                AND jsonb_typeof((ljValue #>> '{}')::jsonb) IS DISTINCT FROM 'array' 
                                            THEN jsonb_build_array((ljValue #>> '{}')::jsonb)
                                            ELSE (ljValue #>> '{}')::jsonb
                                        END
                                    -- "array" value => convert value to "array" type
                                    WHEN lcModifierType = 'f_array'
                                        AND jsonb_typeof(ljValue) IS DISTINCT FROM 'array' THEN
                                        jsonb_build_array(ljValue)
                                    ELSE ljValue
                                END
                            )
                        
                        -- "period" value, e.g. "100s", "p14d", "p1y"
                        WHEN lcModifierType IN ('f_period') THEN
                            jsonb_build_object('result',
                                regexp_replace((ljValue #>> '{}'), '[^0-9]', '', 'g')::bigint
                                *
                                CASE
                                    WHEN (ljValue #>> '{}') ~ '^[0-9]{1,}$' THEN 1              -- seconds by default
                                    WHEN (ljValue #>> '{}') ~ '^[t]{0,1}[0-9]{1,}s$' THEN 1     -- seconds
                                    WHEN (ljValue #>> '{}') ~ '^[t]{0,1}[0-9]{1,}m$' THEN 60    -- minutes
                                    WHEN (ljValue #>> '{}') ~ '^[t]{0,1}[0-9]{1,}h$' THEN 3600  -- hour
                                    WHEN (ljValue #>> '{}') ~ '^p[0-9]{1,}d' THEN   1 * 86400   -- day
                                    WHEN (ljValue #>> '{}') ~ '^p[0-9]{1,}w' THEN   7 * 86400   -- week
                                    WHEN (ljValue #>> '{}') ~ '^p[0-9]{1,}m' THEN  30 * 86400   -- month
                                    WHEN (ljValue #>> '{}') ~ '^p[0-9]{1,}y' THEN 365 * 86400   -- year
                                    ELSE 0::bigint
                                END
                            )

                        -- unknown format modifier => keep value as-is
                        ELSE jsonb_build_object('result', ljValue)

                    END
                    
                    
                
                -- validate and normalize data, with "invalid" messages
                WHEN lcModifierType ~ '^v_' THEN
                    CASE
                        -- "number" format
                        WHEN lcModifierType = 'v_number' THEN
                            CASE
                                -- check number format
                                WHEN NOT regexp_replace(ljValue #>> '{}', '[ \,]', '', 'g') ~ '^[\-]{0,1}[0-9\.]{1,64}$' THEN
                                    jsonb_build_object('message', 'invalid_number')
                                
                                -- check jsonpath expression
                                WHEN nullif(ljModifierRule->'validator'->>'jsonpath', '') IS NOT NULL
                                    AND NOT to_jsonb(regexp_replace(ljValue #>> '{}', '[ \,]', '', 'g')::float)
                                            @@ (ljModifierRule->'validator'->>'jsonpath')::jsonpath THEN
                                    jsonb_build_object('message', 'invalid_jsonpath')
                                
                                -- correct number format => convert to number
                                ELSE jsonb_build_object(
                                    'result', regexp_replace(ljValue #>> '{}', '[ \,]', '', 'g')::float)
                            END
                        
                        
                        -- "date_2000" format
                        WHEN lcModifierType = 'v_date_2000' THEN 
                            CASE
                                -- check if the value is already converted into "date_2000" format
                                WHEN btrim(ljValue #>> '{}') ~ '^[\-]{0,1}[0-9]{1,8}$' THEN
                                    jsonb_build_object('result', (ljValue #>> '{}')::integer)
                                
                                -- correct date format => convert to "date_2000" value
                                WHEN ub.util_verificator(btrim(ljValue #>> '{}'), concat('DATE', ('_' || (ljModifierRule->>'format')))) = 'TRUE' THEN
                                    jsonb_build_object(
                                        'result', to_date(btrim(ljValue #>> '{}'), COALESCE(ljModifierRule->>'format', 'YYYY-MM-DD'))- INITIAL_DATE
                                    )

                                -- correct date_iso format => convert to "date_2000" value
                                WHEN ub.util_verificator(btrim(ljValue #>> '{}'), 'DATE') = 'TRUE' THEN
                                    jsonb_build_object(
                                        'result', to_date(btrim(ljValue #>> '{}'), 'YYYY-MM-DD') - INITIAL_DATE
                                    )

                                -- incorrect date format
                                ELSE jsonb_build_object('message', 'invalid_date')
                            END

                                
                        -- string format
                        WHEN lcModifierType = 'v_string' THEN 
                            CASE
                                -- check jsonpath expression
                                WHEN nullif(ljModifierRule->'validator'->>'jsonpath', '') IS NOT NULL
                                    AND NOT ljValue @@ (ljModifierRule->'validator'->>'jsonpath')::jsonpath THEN
                                    jsonb_build_object('message', 'invalid_jsonpath')
                                
                                -- check maximal length
                                WHEN nullif(ljModifierRule->'validator'->>'maxLength', '') IS NOT NULL
                                    AND length(ljValue #>> '{}') > (ljModifierRule->'validator'->>'maxLength')::integer THEN
                                    jsonb_build_object('message', 'invalid_max_length')
                                
                                -- no string format => keep value as-is
                                WHEN ljModifierRule->>'format' IS NULL THEN
                                    jsonb_build_object('result', (ljValue #>> '{}'))
                                
                                -- html format => escape dangerous HTML tags: <script>, <applet>, <iframe>, <link>, <embed>
                                -- https://validator.w3.org/feed/docs/warning/SecurityRisk.html
                                WHEN ljModifierRule->>'format' = 'html' THEN
                                    jsonb_build_object('result', regexp_replace(ljValue #>> '{}',
                                        '<(?=script|applet|iframe|link|embed|comment|listing|meta|noscript|object|plaintext|xmp)', 
                                        '\u003c', 'g'))
                                    
                                -- correct string format
                                ELSE jsonb_build_object(
                                    'result', (ljValue #>> '{}'))
                            END
                            
                            
                        -- "unix_timestamp" format
                        WHEN lcModifierType = 'v_unix_timestamp' THEN 
                            CASE
                                -- check if the value is already converted into "unix_timestamp" format
                                WHEN btrim(ljValue #>> '{}') ~ '^[0-9\.]{1,64}$' THEN
                                    jsonb_build_object('result', (ljValue #>> '{}')::float)
                                
                                -- check "timestamp" format
                                WHEN ub.util_verificator(
                                        btrim(ljValue #>> '{}'),
                                        concat('DATETIME', ('_' || (ljModifierRule->>'format')))) = 'FALSE' THEN
                                    jsonb_build_object('message', 'invalid_datetime')
                                
                                -- correct date format => convert to "date_2000" value
                                ELSE jsonb_build_object(
                                    'result', EXTRACT(EPOCH FROM 
                                        concat(to_timestamp(
                                            -- input value
                                            btrim(ljValue #>> '{}'), 
                                            -- datetime format 
                                            COALESCE(ljModifierRule->>'format', 'YYYY-MM-DD HH24:MI:SS'))::timestamp,
                                            -- considering time zone
                                            ' ', COALESCE(ljModifierRule->>'timeZone', 'GMT'))::timestamptz
                                    )::float)
                            END
                        
                        
                        -- "checkbox" (format: "number<:true_value><:false_value>" or "boolean<:true_value><:false_value>")
                        WHEN lcModifierType = 'v_checkbox' THEN 
                            jsonb_build_object('result',
                                CASE
                                    WHEN ljModifierRule->>'format' ~ '^number' 
                                    THEN -- 1 | 0
                                        CASE
                                            WHEN btrim(ljValue #>> '{}') = '1'
                                                OR (ljModifierRule->>'format') ~ concat('(?<=\:)', (ljValue #>> '{}'), '(?=\:)')
                                            THEN to_jsonb(1::integer)
                                            ELSE to_jsonb(0::integer)
                                        END
                                    ELSE -- true | false
                                        CASE
                                            WHEN btrim(ljValue #>> '{}') = 'true'
                                                OR (ljModifierRule->>'format') ~ concat('(?<=\:)', (ljValue #>> '{}'), '(?=\:)')
                                            THEN to_jsonb(true::boolean)
                                            ELSE to_jsonb(false::boolean)
                                        END
                                END
                           )
                        
                        
                        -- object format
                        WHEN lcModifierType = 'v_object' THEN 
                            CASE
                                -- check maximal length
                                WHEN nullif(ljModifierRule->'validator'->>'maxLength', '') IS NOT NULL
                                    AND length(ljValue #>> '{}') > (ljModifierRule->'validator'->>'maxLength')::integer THEN
                                    jsonb_build_object('message', 'invalid_max_length')
                                
                                -- convert delimitered "key:value" list to object
                                WHEN ljModifierRule->>'format' IS NOT NULL THEN
                                    jsonb_build_object('result', COALESCE(
                                        (
                                            SELECT
                                                jsonb_object_agg(
                                                    btrim((regexp_match(item, '[^\:]{1,}'))[1]),
                                                    btrim(regexp_replace(item, '[^\:]{1,}\:', ''))
                                                )
                                            FROM unnest(string_to_array(
                                                ljValue #>> '{}',
                                                COALESCE(nullif(ljModifierRule->>'format', '\n'), CHR(10)))) item
                                            WHERE item ~ '[^\:]{1,}\:'
                                        ),
                                        jsonb_build_object()
                                    ))
                                
                                -- check json object format
                                WHEN ub.util_verificator(nullif(btrim(ljValue #>> '{}'), ''), 'JSONOBJECT') = 'FALSE' THEN
                                    jsonb_build_object('message', 'invalid_object')
                                
                                -- check jsonpath expression
                                WHEN nullif(ljModifierRule->'validator'->>'jsonpath', '') IS NOT NULL
                                    AND NOT nullif(btrim(ljValue #>> '{}'), '')::jsonb 
                                            @@ (ljModifierRule->'validator'->>'jsonpath')::jsonpath THEN
                                    jsonb_build_object('message', 'invalid_jsonpath')
                                
                                -- correct json object => convert to json
                                WHEN nullif(btrim(ljValue #>> '{}'), '') IS NOT NULL THEN
                                    ub.util_data_modifier(jsonb_build_object(
                                        'value', ljValue,
                                        'modifier', jsonb_build_array(jsonb_build_object('type', 'f_object'))
                                    ))
                                
                                -- empty string => convert to {}
                                ELSE jsonb_build_object('result', jsonb_build_object())
                                
                            END
                        
                        
                        -- array format
                        WHEN lcModifierType = 'v_array' THEN 
                            CASE
                                -- check maximal length
                                WHEN nullif(ljModifierRule->'validator'->>'maxLength', '') IS NOT NULL
                                    AND length(ljValue #>> '{}') > (ljModifierRule->'validator'->>'maxLength')::integer THEN
                                    jsonb_build_object('message', 'invalid_max_length')

                                -- convert delimitered "key:value" list to object
                                WHEN ljModifierRule->>'format' IS NOT NULL THEN
                                    jsonb_build_object('result', COALESCE(
                                        (
                                            SELECT
                                                jsonb_agg(btrim(item))
                                            FROM unnest(string_to_array(
                                                ljValue #>> '{}',
                                                COALESCE(nullif(ljModifierRule->>'format', '\n'), CHR(10)))) item
                                        ),
                                        jsonb_build_array()
                                    ))
                                
                                -- check json array format
                                WHEN ub.util_verificator(nullif(btrim(ljValue #>> '{}'), ''), 'JSONARRAY') = 'FALSE' THEN
                                    jsonb_build_object('message', 'invalid_array')
                                
                                -- check jsonpath expression
                                WHEN nullif(ljModifierRule->'validator'->>'jsonpath', '') IS NOT NULL
                                    AND NOT nullif(btrim(ljValue #>> '{}'), '')::jsonb 
                                            @@ (ljModifierRule->'validator'->>'jsonpath')::jsonpath THEN
                                    jsonb_build_object('message', 'invalid_jsonpath')
                                
                                -- correct json array => convert to json
                                WHEN nullif(btrim(ljValue #>> '{}'), '') IS NOT NULL THEN
                                    ub.util_data_modifier(jsonb_build_object(
                                        'value', ljValue,
                                        'modifier', jsonb_build_array(jsonb_build_object('type', 'f_array'))
                                    ))
                                
                                -- empty string => convert to []
                                ELSE jsonb_build_object('result', jsonb_build_array())
                            END
                        
                        
                        -- unknown format modifier => keep value as-is
                        ELSE jsonb_build_object('result', ljValue)

                    END
                
                
                
                -- string transformation
                WHEN lcModifierType ~ '^s_' THEN
                    CASE
                        -- convert string to lowercase
                        WHEN lcModifierType = 's_lower' THEN
                            jsonb_build_object('result', lower(ljValue #>> '{}'))
                        
                        -- convert string to uppercase
                        WHEN lcModifierType = 's_upper' THEN
                            jsonb_build_object('result', upper(ljValue #>> '{}'))
                        
                        -- convert string to initial cap
                        WHEN lcModifierType = 's_initcap' THEN
                            jsonb_build_object('result', initcap(ljValue #>> '{}'))
                        
                        -- trim all required symbols
                        WHEN lcModifierType = 's_btrim' THEN
                            jsonb_build_object('result', btrim(
                                ljValue #>> '{}',
                                COALESCE(ljModifierRule->>'btrim', ' ')
                            ))

                        -- extract any part of the string value using regex
                        WHEN lcModifierType = 's_regexp_match' THEN
                            jsonb_build_object('result', (regexp_match(
                                ljValue #>> '{}',
                                ljModifierRule->>'regex'))[1]::text
                            )
                        
                        -- replace any part(s) of the string value using regex
                        WHEN lcModifierType = 's_regex_replace' THEN
                            jsonb_build_object('result', regexp_replace(
                                ljValue #>> '{}',
                                ljModifierRule->>'from', ljModifierRule->>'to', 
                                COALESCE(ljModifierRule->>'flag', '')
                            ))
                        
                        -- split the string value into array of string values using regex
                        WHEN lcModifierType = 's_split' THEN
                            jsonb_build_object('result', 
                                (
                                    SELECT 
                                        jsonb_agg(split_value)
                                    FROM regexp_split_to_table(
                                        ljValue #>> '{}', 
                                        ljModifierRule->>'regex') split_value                                                   
                                ))
                        
                        -- convert null values into non-null values, e.g. ""
                        WHEN lcModifierType = 's_nulls_to_string' THEN

                            CASE
                                WHEN (ljValue #>> '{}') IS NULL
                                THEN to_jsonb(COALESCE(ljModifierRule->>'default', '')::text)
                                ELSE ljValue
                            END
                        
                        -- unknown string modifier => keep value as-is
                        ELSE jsonb_build_object('result', ljValue)
                        
                    END
                
                
                
                -- array transformation
                WHEN lcModifierType ~ '^a_' THEN
                    CASE
                        -- check if ljValue has "array" type
                        WHEN jsonb_typeof(ljValue) IS DISTINCT FROM 'array' THEN
                            jsonb_build_object('result', ljValue)
                        
                        -- unknown array modifier => keep value as-is
                        ELSE jsonb_build_object('result', ljValue)
                        
                    END  
                
                
                
                -- unknown type
                ELSE jsonb_build_object('result', ljValue)
                                    
            END;
            
        
        -- set a new value
        ljValue := ljResult->'result';
        
        -- exit in case of any invalid message
        IF ljResult->>'message' IS NOT NULL THEN
            EXIT;
        END IF;
            
    END LOOP;
    
    RETURN ljResult;
    
    
EXCEPTION
    WHEN others THEN
        GET STACKED DIAGNOSTICS
            lcExceptionContext = PG_EXCEPTION_CONTEXT,
            lcExceptionDetail  = PG_EXCEPTION_DETAIL,
            lcExceptionHint    = PG_EXCEPTION_HINT,
            lcExceptionMessage = MESSAGE_TEXT,
            lcExceptionState   = RETURNED_SQLSTATE;
 
        RETURN jsonb_build_object(
            'error', jsonb_build_object(
                'code', 500,
                'message', 'Internal error',
                'details', jsonb_build_object(
                    'error_context', lcExceptionContext,
                    'error_detail',  lcExceptionDetail,
                    'error_hint',    lcExceptionHint,
                    'error_message', lcExceptionMessage,
                    'error_state',   lcExceptionState
                )
            )
        );
    
END;
/*
@example format a number

SELECT ub.util_data_modifier(jsonb_build_object(
    'value', '25',
    'modifier', '[{"type": "f_number", "format": "FM999,999.00"}]'::jsonb
))

=> { "result": "25.00" }

@example convert and format date from date_2000

SELECT ub.util_data_modifier(jsonb_build_object(
    'value', '7300',
    'modifier', '[{"type": "f_date", "format": "DD Mon YYYY"}]'::jsonb
))

=> { "result": "27 Dec 2019" }

@example split array into elements

SELECT ub.util_data_modifier(jsonb_build_object(
    'value', jsonb_build_array('foo', 'bar'),
    'modifier', '[{"type": "f_string", "delimiter": "\n"}]'::jsonb
))

=> { "result": "foo\nbar" }

@example "p1d" period into seconds

SELECT ub.util_data_modifier(jsonb_build_object(
    'value', 'p1w',
    'modifier', '[{"type": "f_period"}]'::jsonb
))

=> { "result": 604800 }

@example validate text using jsonpath expression

SELECT ub.util_data_modifier(jsonb_build_object(
    'value', 'my text',
    'modifier', '[{"type": "v_string", "validator": {"jsonpath": "$ like_regex \"^a\""}}]'::jsonb
))

=> { "message": "invalid_jsonpath" }
}
*/
$BODY$;

ALTER FUNCTION ub.util_data_modifier(jsonb)
    OWNER TO unibackend;




-- FUNCTION: ub.util_build_template(jsonb)

-- DROP FUNCTION IF EXISTS ub.util_build_template(jsonb);

CREATE OR REPLACE FUNCTION ub.util_build_template(
    ljinput jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 10
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_build_template
@desc Convert template (string | object | array) with {$.<key>} and {$<statement>} insertions into text using parameters from "sourceMapping" object
@desc Every {$.<key>} can contain an optional default value after ":", e.g. "{$.my_key:10}"
@desc Allowed statements:
    - {$if:<any jsonpath condition>}, e.g. {$if:$.a > 0 && $.b == "test"}
    - {$elseif:<any jsonpath condition>}, e.g. {$elseif:$.a > 0 && $.b == "test"}
    - {$else}
    - {$for:<array key>}, e.g. {$.for:$.a[*]}Name: {$.a.name}{$end}
    - {$end} - end for {if:} | {for:} statements
@desc Nested statements are supported

@param string|object|array template - any string template with {$.<key>} and {$<statement>} insertions
@param object|array sourceMapping - source data to use for the template processing

@param array|null data - array of statements and data inside them (used for a recursive call)
@param number|null firstRow - initial row (starting with 0) to process "data" (used for a recursive call)

@return string result - template with prepared data
@return number|null lastRow - last processed row (used for a recursive call)

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#template #html #statement #condition #control #if #elseif #else #end #for #image
*/

DECLARE

    -- Exception diagnostics
    lcExceptionContext          text;
    lcExceptionDetail           text;
    lcExceptionHint             text;
    lcExceptionMessage          text;
    lcExceptionState            text;
    
    
    -- Constants
    INITIAL_DATE                CONSTANT date := '2000-01-01'::date;        -- initial date to calculate date offset
    
    
    -- Input parameters
    ljSourceData                jsonb :=                                    -- key-values for mapping
        ljInput->'sourceMapping';
        
    ljTemplateData              jsonb :=                                    -- array of statements and their data
        ljInput->'data';
        
    lnRowID                     integer :=                                  -- initial row to process
        COALESCE(ljInput->>'firstRow', '0')::integer;
        
    
    -- Local variables
    lcProcessText               text;                                       -- text / statement to process
    lcStatementData             text;                                       -- statement data
    
    ljElementData               jsonb;                                      -- array element data
    lbIFMatchedFlag             boolean := false;                           -- true = "if" clause has been matched
    lbIFClauseSelect            boolean := true;                            -- true = process template
    lnIgnoreLevel               integer := 0;                               -- ignored embedded levels
    
    ljResponseData              jsonb;                                      -- response after recursive util_build_template() call
    lcResult                    text;                                       -- result text
    
    
BEGIN
    
    -- Initial call
    IF ljTemplateData IS NULL THEN
        
        -- Check if template is an object => process all keys in the object
        IF jsonb_typeof(ljInput->'template') IN ('object', 'array') THEN
            
            RETURN (
                WITH
                -- unnest object / array into plain table
                "unnest_data" AS MATERIALIZED (
                (   -- unnest object
                    SELECT
                        0 AS order_id,
                        "template_data".key,
                        "template_data".value
                    FROM jsonb_each(ljInput->'template') AS template_data(key, value)
                    WHERE jsonb_typeof(ljInput->'template') IS NOT DISTINCT FROM 'object'
                )
                UNION ALL
                (   -- unnest array
                    SELECT
                        "template_data".order_id,
                        NULL::text AS key,
                        "template_data".value
                    FROM jsonb_array_elements(ljInput->'template')
                        WITH ORDINALITY AS template_data(value, order_id)
                    WHERE jsonb_typeof(ljInput->'template') IS NOT DISTINCT FROM 'array'
                )),
                -- process unnested data
                "process_data" AS MATERIALIZED (
                    SELECT
                        "unnest_data".order_id,
                        "unnest_data".key,
                        CASE
                            -- no need to translate number or boolean
                            WHEN jsonb_typeof("unnest_data".value) IN ('number', 'boolean') THEN 
                                "unnest_data".value
                            -- object / array => run ub.util_build_template() recursively
                            WHEN jsonb_typeof("unnest_data".value) IN ('object', 'array') THEN 
                                (ub.util_build_template(jsonb_build_object('template', "unnest_data".value, 'sourceMapping', ljSourceData)))->'result'
                            -- string, with '{$' parameters
                            WHEN ("unnest_data".value #>> '{}') ~ '\{\$' THEN
                                CASE
                                    WHEN ("unnest_data".value #>> '{}') ~ '\{\$[a-z]' 
                                    -- has directives => run ub.util_build_template() recursively
                                    THEN (ub.util_build_template(jsonb_build_object('template', "unnest_data".value, 'sourceMapping', ljSourceData)))->'result'
                                    -- no directives => replace '{$' parameters with real values
                                    ELSE (
                                        SELECT
                                            to_jsonb(
                                                string_agg(
                                                    CASE
                                                        WHEN split_value ~ '^\{\$\.'
                                                        -- translate {$.<key>} into value
                                                        THEN COALESCE(
                                                            -- extract value from input "value" object
                                                            jsonb_path_query_first(ljSourceData, btrim(regexp_replace(split_value, '\:.*', ''), '{}')::jsonpath) #>> '{}',
                                                            -- default value
                                                            (regexp_match(split_value, '(?<=\:)[^\}]{1,}'))[1]
                                                        )
                                                        -- keep value as-is
                                                        ELSE split_value
                                                    END,
                                                    ''
                                                )::text
                                            )
                                        FROM regexp_split_to_table(
                                            ("unnest_data".value #>> '{}'), 
                                            '(?<=((\{\$\.)[^\}]+\}))|(?=((\{\$\.)[^\}]+\}))'
                                        ) split_value
                                    )
                                END
                            -- jsonpath string
                            WHEN ("unnest_data".value #>> '{}') ~ '\$\.'
                                AND ub.util_verificator("unnest_data".value #>> '{}', 'JSONPATH') = 'TRUE' THEN
                                jsonb_path_query_first(ljSourceData, ("unnest_data".value #>> '{}')::jsonpath)
                            -- ordinary string => no need to process data
                            ELSE "unnest_data".value
                        END AS new_value
                    FROM "unnest_data"
                )
                -- build response
                SELECT jsonb_build_object(
                    'result',
                        CASE
                            WHEN jsonb_typeof(ljInput->'template') IS NOT DISTINCT FROM 'array'
                            -- combine response as array
                            THEN (
                                SELECT
                                    jsonb_agg(
                                        "process_data".new_value
                                        ORDER BY "process_data".order_id
                                    )
                                FROM "process_data"
                            )
                            -- combine response as object
                            ELSE (
                                SELECT
                                    jsonb_object_agg(
                                        "process_data".key,
                                        "process_data".new_value
                                    )
                                FROM "process_data"
                            )
                        END
                )
            );
        
        
        -- Text template => split template by statements   
        ELSE
    
            SELECT
                jsonb_agg(statement_data)
            INTO ljTemplateData
            FROM regexp_split_to_table(
                    -- basic template
                    ljInput->>'template',
                    -- split by {$.<statement>} expressions
                    '(?<=((\{\$[^\.])[^\}]+\}))|(?=((\{\$[^\.])[^\}]+\}))'
                ) statement_data;

            -- No data to process => exit
            IF ljTemplateData IS NULL THEN

                RETURN jsonb_build_object(
                    'result', '',
                    'lastRow', 0
                );

            END IF;
        END IF;
    END IF;
    
    
    
    -- Process template data
    LOOP
    
        -- Read statement to process
        lcProcessText := ljTemplateData->>(lnRowID);
        lcStatementData := (regexp_match(lcProcessText, '(?<=(\{\$[a-z]{1,}\:))[^\}]{1,}'))[1];
        
        
        -- "if" statement
        IF lcProcessText ~ '^\{\$if\:' THEN

            -- validate "if" statement
            IF lnRowID IS NOT DISTINCT FROM (ljInput->>'firstRow')::integer THEN

                -- "if" clause is validated => set "matchedFlag" and "clauseSelect" to true
                IF (lcStatementData IS NULL
                     OR ub.util_verificator(lcStatementData, 'JSONPATH') = 'FALSE'
                     OR ljSourceData @@ lcStatementData::jsonpath) THEN

                    lbIFMatchedFlag := true;
                    lbIFClauseSelect := true;

                -- "if" clause is not validated => set "matchedFlag" and "clauseSelect" to false
                ELSE 

                    lbIFMatchedFlag := false;
                    lbIFClauseSelect := false;

                END IF;

            -- process "if" statement recursively => run ub.util_build_template()
            ELSEIF lbIFClauseSelect
                AND lnIgnoreLevel = 0 THEN

                ljResponseData := ub.util_build_template(
                    jsonb_build_object(
                        'data', ljTemplateData,
                        'sourceMapping', ljSourceData,
                        'firstRow', lnRowID
                    )
                );

                lnRowID := (ljResponseData->>'lastRow')::integer;
                lcResult := concat(lcResult, ljResponseData->>'result');
                
            -- ignore "if" statement
            ELSE
            
                lnIgnoreLevel := lnIgnoreLevel + 1;
                lbIFClauseSelect := false;

            END IF;
        
        
        -- "elseif" statement
        ELSEIF lcProcessText ~ '^\{\$elseif\:'
            AND lnIgnoreLevel = 0 THEN

            -- "elseif" clause is validated => set "matchedFlag" and "clauseSelect" to true
            IF NOT lbIFMatchedFlag
                AND (lcStatementData IS NULL
                     OR ub.util_verificator(lcStatementData, 'JSONPATH') = 'FALSE'
                     OR ljSourceData @@ lcStatementData::jsonpath) THEN

                lbIFMatchedFlag := true;
                lbIFClauseSelect := true;

            -- "elseif" clause is not validated => set "matchedFlag" and "clauseSelect" to false
            ELSE 

                lbIFClauseSelect := false;

            END IF;

        -- "else" statement
        ELSEIF lcProcessText ~ '^\{\$else\}'
            AND lnIgnoreLevel = 0 THEN

            -- all previous clauses were false => process "else" statement
            IF NOT lbIFMatchedFlag THEN

                lbIFMatchedFlag := true;
                lbIFClauseSelect := true;

            -- skip "else" statement
            ELSE

                lbIFClauseSelect := false;

            END IF;

        -- "for" statement
        ELSEIF lcProcessText ~ '^\{\$for\:' THEN
        
            -- process "for" statement
            IF lbIFClauseSelect
                AND lnIgnoreLevel = 0 THEN
                
                -- empty or incorrect array data => skip "for" statement
                IF lcStatementData IS NULL
                    OR ub.util_verificator(lcStatementData, 'JSONPATH') = 'FALSE'
                    OR NOT EXISTS(SELECT 1 FROM jsonb_path_query(ljSourceData, lcStatementData::jsonpath)) THEN
                    
                    ljResponseData := ub.util_build_template(
                        jsonb_build_object(
                            'data', ljTemplateData,
                            'sourceMapping', jsonb_build_object(),
                            'firstRow', lnRowID + 1
                        )
                    );
                        
                    lnRowID := (ljResponseData->>'lastRow')::integer;
                    
                ELSE
                    
                    -- Process each element of the array
                    FOR ljElementData IN SELECT jsonb_path_query(ljSourceData, lcStatementData::jsonpath) LOOP
                    
                        ljResponseData := ub.util_build_template(
                            jsonb_build_object(
                                'data', ljTemplateData,
                                'sourceMapping', ub.util_jsonb_update(
                                    ljSourceData,
                                    -- add processing array element
                                    jsonb_build_object((regexp_match(lcStatementData, '(?<=(\$\.))[^\[]{1,}'))[1], ljElementData)
                                ),
                                'firstRow', lnRowID + 1
                            )
                        );
                            
                        lcResult := concat(lcResult, ljResponseData->>'result');

                    END LOOP;

                    lnRowID := (ljResponseData->>'lastRow')::integer;
                    
                END IF;

            -- ignore "for" statement
            ELSE
            
                lnIgnoreLevel := lnIgnoreLevel + 1;

            END IF;
        

        -- "end" statement
        ELSEIF lcProcessText ~ '^\{\$end\}' THEN

            -- statement if over => ignored level should be reduced
            IF lnIgnoreLevel > 0 THEN
            
                lnIgnoreLevel := lnIgnoreLevel - 1;
                
            -- => exit, if not an ignored level
            ELSE
            
                EXIT;
                
            END IF;

        
        -- not a statement => add to response if selectedFlag = true
        ELSEIF lbIFClauseSelect
            AND lnIgnoreLevel = 0 THEN

            lcResult := concat(
                lcResult,
                (
                    SELECT
                        string_agg(
                            CASE
                                -- translate {$.<key>} into value
                                WHEN split_value ~ '^\{\$\.' THEN
                                    COALESCE(
                                        -- extract value from input "value" object
                                        jsonb_path_query_first(ljSourceData, btrim(regexp_replace(split_value, '\:.*', ''), '{}')::jsonpath) #>> '{}',
                                        -- default value
                                        (regexp_match(split_value, '(?<=\:)[^\}]{1,}'))[1]
                                    )
                                -- keep value as-is
                                ELSE split_value
                            END,
                            ''
                        )
                    FROM regexp_split_to_table(
                        lcProcessText, 
                        '(?<=((\{\$\.)[^\}]+\}))|(?=((\{\$\.)[^\}]+\}))'
                    ) split_value
                )
            );

        END IF;    
        
        
        -- Move to the next statement
        lnRowID := lnRowID + 1;
        
        -- All statement are processed => exit
        IF lnRowID > jsonb_array_length(ljTemplateData) THEN
        
            EXIT;
            
        END IF;
    END LOOP;
    
    
    
    RETURN jsonb_build_object(
        'result', lcResult,
        'lastRow', lnRowID
    );
    
    
EXCEPTION
    WHEN others THEN
        GET STACKED DIAGNOSTICS
            lcExceptionContext = PG_EXCEPTION_CONTEXT,
            lcExceptionDetail  = PG_EXCEPTION_DETAIL,
            lcExceptionHint    = PG_EXCEPTION_HINT,
            lcExceptionMessage = MESSAGE_TEXT,
            lcExceptionState   = RETURNED_SQLSTATE;
 
        RETURN jsonb_build_object(
            'error', jsonb_build_object(
                'code', 500,
                'message', 'Internal error',
                'details', jsonb_build_object(
                    'error_context', lcExceptionContext,
                    'error_detail',  lcExceptionDetail,
                    'error_hint',    lcExceptionHint,
                    'error_message', lcExceptionMessage,
                    'error_state',   lcExceptionState
                )
            )
        );
    
END;
/*
@example

-- Enrich jsonb object with data from sourceMapping. 
SELECT ub.util_build_template(jsonb_build_object(
    'template', '{"title": "{$if:$.role_id == \"premium\"}Special offer{$else}Basic offer{$end} {$.price:200}"}'::jsonb,
    'sourceMapping', '{"role_id": "premium", "price": 100}'::jsonb
))
--> {"result": {"title":"Special offer 100"}}


-- Build text template, with if and loop conditions
SELECT ub.util_build_template(jsonb_build_object(
    'template', 
    '
        {$if:$.a == 2}
            First 
            {$if:$.a == 1}
                Case 1
            {$elseif:$.a == 3}
                Case 3
            {$elseif:$.a == 2}
                Case 
                {$if:$.b > 2}
                    check
                {$end}
                2
            {$else}
                Unknown case
            {$end}
        {$elseif:$.a == 1}
            First 2
        {$else}
            Unknown case
        {$end}
        My template
        {$.c + $.b}
        Template text
        {$if:$.c == 2}
            {$for:$.v1[*]}
                Cycle 
                {$if:$.a == 2}
                    {$.v1.n}
                {$else}
                    {$.v1.m}
                {$end}
                demo 
            {$end}
        {$end}
        Template bottom
        {$if:$.b > 0 && $.c == 2}
            Test 
            {$if:$.a == 1}
                Case 1
            {$elseif:$.a == 3}
                Case 3
            {$elseif:$.a == 2}
                Case 
                {$if:$.b > 2}
                    check
                {$end}
                2
            {$else}
                Unknown case
            {$end}
            text
        {$end}
    ',
    'sourceMapping',
    '
        {
            "a":2, 
            "b":3, 
            "c":2,
            "v1": [
                {"m":"text1","n":"next1"},
                {"m":"text2","n":"next2"}
            ]
        }
    '::jsonb
));

=>
"First 
    Case 
        check
        2
    My template
5
Template text
Cycle 
        next1
        demo 
    Cycle 
        next2
        demo 
    Template bottom
Test 
    Case 
        check
        2
    text"
*/
$BODY$;

ALTER FUNCTION ub.util_build_template(jsonb)
    OWNER TO postgres;




-- FUNCTION: ub.util_jwt(jsonb)

-- DROP FUNCTION IF EXISTS ub.util_jwt(jsonb);

CREATE OR REPLACE FUNCTION ub.util_jwt(
    ljinput jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 20
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_jwt
@desc Function for JWT processing (https://jwt.io)
@desc Required extension: pgcrypto (hmac() function)

@param string mode - mode for JWT processing
    - "jwt_sign": sign a JWT based on json payload, secret key and encrypt algorithm
    - "jwt_verify": verify a JWT and return { "header", "payload", "valid": 1|0 } object
    - "jwt_encode": encode a JWT in base64 format
    - "jwt_decode": decode a JWT from base64 format
    - "jwt_generate": generate a JWT based on the token data, secret key and encrypt algorithm
@param string jwtValue - JWT as a string or base64 string
@param object jwtPayload - object with any JWT data
@param string jwtSecret - JWT secret key
@param string jwtAlgorithm - "sha256" (default) | "sha384" | "sha512"
@param object sessionData - session data

@return object -
    @key string jwtValue - JWT as specified at https://jwt.io
    @key object jwtPayload - JWT payload
    @key string jwtHeader - JWT header
    @key number jwtValid - 1 = JWT is valid, 0 = JWT is not valid

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#jwt #jwt payload #user id #jwt secret #jwt algorithm #sha
*/

DECLARE

    ljPayload       jsonb;                              -- JWT payload
    

BEGIN

    CASE ljInput->>'mode'
    
        WHEN 'jwt_encode' THEN      -- jwtValue as bytea => jwtValue as base64
        
            RETURN jsonb_build_object(
                'jwtValue', translate(encode((ljInput->>'jwtValue')::bytea, 'base64'), E'+/=\n', '-_')
            );

        WHEN 'jwt_decode' THEN      -- jwtValue as base64 => jwtValue as bytea, with length adjusting
        
            RETURN jsonb_build_object(
                'jwtValue', 
                    decode(concat(
                        translate(ljInput->>'jwtValue', '-_', '+/'), 
                        repeat('=', 3 - MOD((length(translate(ljInput->>'jwtValue', '-_', '+/')) - 1), 4))
                    ), 'base64')
            );

        WHEN 'jwt_generate' THEN      -- jwtValue, jwtSecret, jwtAlgorithm => jwtValue as base64
        
            RETURN jsonb_build_object(
                'jwtValue', (ub.util_jwt(
                    jsonb_build_object(
                        'mode', 'jwt_encode',
                        'jwtValue', ext.hmac(
                            ljInput->>'jwtValue',
                            ljInput->>'jwtSecret',
                            COALESCE(ljInput->>'jwtAlgorithm', 'sha256')
                    )
                    )
                ))->>'jwtValue'
            );

        WHEN 'jwt_sign' THEN      -- jwtPayload, jwtSecret, jwtAlgorithm => jwtValue as base64
        
            RETURN jsonb_build_object(
                'jwtValue', concat(
                    (ub.util_jwt(
                        jsonb_build_object(
                            'mode', 'jwt_encode',
                            'jwtValue', convert_to(concat(
                                '{"alg":"', replace(COALESCE(ljInput->>'jwtAlgorithm', 'sha256'), 'sha', 'HS'),
                                '","typ":"JWT"}'
                            ), 'utf8')
                        )
                    ))->>'jwtValue',
                    '.',
                    (ub.util_jwt(
                        jsonb_build_object(
                            'mode', 'jwt_encode',
                            'jwtValue', convert_to((ljInput->'jwtPayload')::text, 'utf8')
                        )
                    ))->>'jwtValue',
                    '.',
                    (ub.util_jwt(
                        jsonb_build_object(
                            'mode', 'jwt_generate',
                            'jwtValue', concat(
                                (ub.util_jwt(
                                    jsonb_build_object(
                                        'mode', 'jwt_encode',
                                        'jwtValue', convert_to(concat(
                                            '{"alg":"', replace(COALESCE(ljInput->>'jwtAlgorithm', 'sha256'), 'sha', 'HS'),
                                            '","typ":"JWT"}'
                                        ), 'utf8')
                                    )
                                ))->>'jwtValue',
                                '.',
                                (ub.util_jwt(
                                    jsonb_build_object(
                                        'mode', 'jwt_encode',
                                        'jwtValue', convert_to((ljInput->'jwtPayload')::text, 'utf8')
                                    )
                                ))->>'jwtValue'
                            ),
                            'jwtSecret', ljInput->>'jwtSecret',
                            'jwtAlgorithm', ljInput->>'jwtAlgorithm'
                        )
                    ))->>'jwtValue'
                )
            );

        WHEN 'jwt_verify' THEN      -- jwtValue, jwtSecret, jwtAlgorithm => jwtHeader, JWTPayload, JWTValid
            
            -- Read JWT payload
            ljPayload := convert_from(
                        ((ub.util_jwt(
                            jsonb_build_object(
                                'mode', 'jwt_decode',
                                'jwtValue', split_part(ljInput->>'jwtValue', '.', 2)
                            )
                        ))->>'jwtValue')::bytea,
                        'utf8'
                    )::jsonb;
            
            RETURN jsonb_build_object(
                'jwtHeader',  
                    convert_from(
                        ((ub.util_jwt(
                            jsonb_build_object(
                                'mode', 'jwt_decode',
                                'jwtValue', split_part(ljInput->>'jwtValue', '.', 1)
                            )
                        ))->>'jwtValue')::bytea,
                        'utf8'
                    )::jsonb,
                'jwtPayload', ljPayload,
                'jwtValid',
                    CASE
                        WHEN split_part(ljInput->>'jwtValue', '.', 3) IS NOT DISTINCT FROM
                                (ub.util_jwt(
                                    jsonb_build_object(
                                        'mode', 'jwt_generate',
                                        'jwtValue', concat(
                                            split_part(ljInput->>'jwtValue', '.', 1), '.', 
                                            split_part(ljInput->>'jwtValue', '.', 2)
                                        ),
                                        'jwtSecret', ljInput->>'jwtSecret',
                                        'jwtAlgorithm', ljInput->>'jwtAlgorithm'
                                    )
                                ))->>'jwtValue'
                        THEN 1
                        ELSE 0
                    END
            );
            
        ELSE
        
            RETURN jsonb_build_object();
            
    END CASE;
    
EXCEPTION
 WHEN others THEN 
 
        RETURN jsonb_build_object(
            'jwtValid', 0
        );
     
END;
/*
@example:
    SELECT ub.util_jwt('{"mode": "jwt_sign", "jwtPayload": {"a": 1}, "jwtSecret": "1234"}')
    => { "jwtValue": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhIjogMX0.3aKAFdFca4DozVrKxqgcGPZik8erGRtdbTipg8Hk9Ao" }
    
    SELECT ub.util_jwt('{"mode": "jwt_verify", "jwtValue": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhIjogMX0.3aKAFdFca4DozVrKxqgcGPZik8erGRtdbTipg8Hk9Ao", "jwtSecret": "1234"}')
    => {
            "jwtValid": 1,
            "jwtHeader": {"alg": "HS256", "typ": "JWT" },
            "jwtPayload": { "a": 1}
       }
*/
$BODY$;

ALTER FUNCTION ub.util_jwt(jsonb)
    OWNER TO postgres;




-- FUNCTION: ub.util_process_crontab(text)

-- DROP FUNCTION IF EXISTS ub.util_process_crontab(text);

CREATE OR REPLACE FUNCTION ub.util_process_crontab(
    crontab_expr text)
    RETURNS double precision
    LANGUAGE 'plpgsql'
    COST 20
    VOLATILE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_process_crontab
@desc Calculate next timestamp based on "crontab" parameter (https://en.wikipedia.org/wiki/Cron)
@desc Allowed special characters: "," "-" "*" "/"

@param string $1 - "crontab" expression, with positions: 
    - 1 = seconds
    - 2 = minutes
    - 3 = hours
    - 4 = day of month
    - 5 = month
    - 6 = day of week
@return float|null - UNIX timestamp of the next event after at time zone 'UTC'. null if "crontab" expression is invalid

@version 0.5.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#crontab #period #schedule
*/

DECLARE

    -- Constants
    MONTH_LIST              text[] := ARRAY['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
    DOW_LIST                text[] := ARRAY['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'];
  
    MAX_DAYS_TO_PROCESS     integer := 366; -- One year is a maximal period
    MAX_STEPS_TO_PROCESS    integer := 60;  -- Any period could be from 0 to 60 steps
  
    SECOND_POS              integer := 1;   -- Position 
    MINUTE_POS              integer := 2;   --  of
    HOUR_POS                integer := 3;   --   the period
    DAY_POS                 integer := 4;   --    in the
    MONTH_POS               integer := 5;   --     crontab
    DOW_POS                 integer := 6;   --    expression

    -- Local variables
    ltInitialTime           timestamp;      --  Initial timestamp (at UTC) to calculate time of the next event 
    ldInitialDate           date;           --    converted to date at UTC
    lnInitialSecToday       integer;        --  Initial seconds for "today" period
    lnInitialSecAfter       integer;        --  Initial seconds for "a day after" period
  
    laPeriodValue           text[];         -- Expressions for each period
    laPeriodMask            bigint[];       -- Bitmask of available values for each period
  
    lnNextTime              float;          -- Unix timestamp of the next event (should be calculated)
  
  
BEGIN
  
    WITH
        -- Read list of periods
        "period_list" AS MATERIALIZED (
          SELECT
            ROW_NUMBER() OVER() AS period_id,   -- 1 = seconds, 2 = minutes, 3 = hours, 4 = day of month, 5 = month, 6 = day of week
            upper(period_value) AS period_value   -- e.g. "0-15" "*/5" "*" "5,10,15" "MON,WED,FRI"
          FROM regexp_split_to_table(crontab_expr, ' ') period_value
          WHERE nullif(period_value, '') IS NOT NULL
        ),
    
        -- Translate period to numeric format
        "period_numeric" AS MATERIALIZED (
          SELECT
            "period_list".period_id,

            CASE
              WHEN "period_list".period_id = MONTH_POS AND "period_list".period_value ~ '[A-Z]' THEN    -- Monthes
                (
                  SELECT
                    string_agg(CASE WHEN value ~ '[A-Z]' THEN array_position(MONTH_LIST, value)::text ELSE value END, '')
                  FROM regexp_split_to_table("period_list".period_value, '(?<=[^A-Z]{1,})|(?=[^A-Z]{1,})') value
                  WHERE nullif(value, '') IS NOT NULL
                )
              WHEN "period_list".period_id = DOW_POS AND "period_list".period_value ~ '[A-Z]' THEN    -- Days of week
                (
                  SELECT
                    string_agg(CASE WHEN value ~ '[A-Z]' THEN (array_position(DOW_LIST, value) - 1)::text ELSE value END, '')
                  FROM regexp_split_to_table("period_list".period_value, '(?<=[^A-Z]{1,})|(?=[^A-Z]{1,})') value
                  WHERE nullif(value, '') IS NOT NULL
                )
              ELSE "period_list".period_value
            END AS period_value
          FROM "period_list"
        ),

        -- Build bit masks of possible values
        "period_bitmasks" AS MATERIALIZED (
          SELECT
            "period_numeric".period_id,
            "period_numeric".period_value,

            CASE
              WHEN "period_numeric".period_value ~ '^[0-9]{1,}\-[0-9]{1,}$' THEN    -- "10-20" templates
                (
                  SELECT 
                    bit_or(1::bigint << value::integer) 
                  FROM generate_series(
                      split_part("period_numeric".period_value, '-', 1)::bigint, 
                      split_part("period_numeric".period_value, '-', 2)::bigint
                    ) value
                  WHERE value::integer <= MAX_STEPS_TO_PROCESS
                )
              WHEN "period_numeric".period_value ~ '^\*\/[0-9]{1,}$' THEN           -- "*/5" templates
                (
                  SELECT
                    bit_or(1::bigint << LEAST(value::integer * split_part("period_numeric".period_value, '/', 2)::integer, MAX_STEPS_TO_PROCESS))
                  FROM generate_series(0, CEILING(MAX_STEPS_TO_PROCESS::float / split_part("period_numeric".period_value, '/', 2)::integer)::bigint) value
                )
              WHEN "period_numeric".period_value ~ '^\*$' THEN                      -- *
                (
                  SELECT 
                    bit_or(1::bigint << value::integer)
                  FROM generate_series(0, MAX_STEPS_TO_PROCESS) value
                )
              WHEN "period_numeric".period_value ~ '^[0-9,]{1,}$' THEN              -- "5,8,20" templates
                (
                  SELECT
                    bit_or(1::bigint << value::integer)
                  FROM unnest(string_to_array("period_numeric".period_value, ',')) value
                  WHERE nullif(value, '') IS NOT NULL
                    AND value::integer <= MAX_STEPS_TO_PROCESS
                )
              ELSE 0::bigint
            END AS period_bitmask

          FROM "period_numeric"
        ),

        -- Calculate initial timestamp. We have to increase the first non-asterisk period by 1
        -- For "* * 3-6 * * * " the first non-asterisk period is "hour", so we have to increase the current hour by 1
        "period_non_asterisk" AS MATERIALIZED (
            SELECT
                CASE
                  WHEN "period_numeric".period_id = SECOND_POS THEN date_trunc('second', clock_timestamp(), 'UTC') + interval '1 second'
                  WHEN "period_numeric".period_id = MINUTE_POS THEN date_trunc('minute', clock_timestamp(), 'UTC') + interval '1 minute'
                  WHEN "period_numeric".period_id = HOUR_POS   THEN date_trunc('hour',   clock_timestamp(), 'UTC') + interval '1 hour'
                  WHEN "period_numeric".period_id = MONTH_POS  THEN date_trunc('month',  clock_timestamp(), 'UTC') + interval '1 month'
                  ELSE date_trunc('day', clock_timestamp(), 'UTC') + interval '1 day'
                END AS initial_timestamp
            FROM "period_numeric"
            WHERE NOT "period_numeric".period_value ~ '^\*$'
            ORDER BY "period_numeric".period_id
            LIMIT 1
        )
    
    -- Build expressions for each period, bitmask of available values for each period & initial timestamp
    SELECT
        array_agg("period_bitmasks".period_value   ORDER BY "period_bitmasks".period_id),
        array_agg("period_bitmasks".period_bitmask ORDER BY "period_bitmasks".period_id),
        (SELECT "period_non_asterisk".initial_timestamp AT time zone 'UTC' FROM "period_non_asterisk")
    INTO laPeriodValue, laPeriodMask, ltInitialTime
    FROM "period_bitmasks";
    
  
    -- Check if all bitmasks > 0
    IF array_position(laPeriodMask, 0::bigint) IS NOT NULL THEN
        RETURN NULL::float;
    END IF;
    
    
    -- Initial date
    ldInitialDate := ltInitialTime::date;
    
    -- Initial seconds for today
    SELECT 
        minute_id * 60
    INTO lnInitialSecToday
    FROM generate_series(
        EXTRACT(HOUR FROM ltInitialTime)::bigint * 60 + EXTRACT(MINUTE FROM ltInitialTime)::bigint + 1, 1439) minute_id
    WHERE 
        (laPeriodMask[MINUTE_POS] & (1::bigint << MOD(minute_id, 60)::integer)) != 0
        AND (laPeriodMask[HOUR_POS] & (1::bigint << (minute_id / 60)::integer)) != 0
    LIMIT 1;
    
    -- Initial seconds a day after
    lnInitialSecAfter := 
      (SELECT hour_id * 3600 FROM generate_series(0, 23) hour_id WHERE (laPeriodMask[HOUR_POS] & (1::bigint << hour_id::integer)) != 0 LIMIT 1)
      + (SELECT minute_id * 60 FROM generate_series(0, 59) minute_id WHERE (laPeriodMask[MINUTE_POS] & (1::bigint << minute_id::integer)) != 0 LIMIT 1);
  
  
    WITH
    -- Check if today is matching the clause, and select the nearest seconds
    "selected_today" AS MATERIALIZED (
        SELECT
            ldInitialDate AS run_date,
            COALESCE(
            (
                SELECT
                    EXTRACT(HOUR FROM ltInitialTime)::bigint * 3600
                    + EXTRACT(MINUTE FROM ltInitialTime)::bigint * 60
                    + second_id
                FROM generate_series(EXTRACT(SECOND FROM ltInitialTime)::bigint, 59) second_id
                WHERE
                    (laPeriodMask[MINUTE_POS] & (1::bigint << EXTRACT(MINUTE FROM ltInitialTime)::integer)) != 0
                    AND (laPeriodMask[HOUR_POS] & (1::bigint << EXTRACT(HOUR FROM ltInitialTime)::integer)) != 0
                    AND (laPeriodMask[SECOND_POS] & (1::bigint << second_id::integer)) != 0
                LIMIT 1
            ),          
            (
                SELECT
                    lnInitialSecToday + second_id
                FROM generate_series(0, 59) second_id
                WHERE (laPeriodMask[SECOND_POS] & (1::bigint << second_id::integer)) != 0
                LIMIT 1
            )) AS run_seconds
  
        WHERE 
            ldInitialDate = (clock_timestamp() AT time zone 'UTC')::date
            AND (laPeriodMask[DAY_POS]   & (1::bigint << (EXTRACT (DAY FROM ldInitialDate))::integer)) != 0
            AND (laPeriodMask[MONTH_POS] & (1::bigint << (EXTRACT (MONTH FROM ldInitialDate))::integer)) != 0
            AND (laPeriodMask[DOW_POS]   & (1::bigint << (EXTRACT (DOW FROM ldInitialDate))::integer)) != 0
    ),
    
    -- Select the next matching time
    "selected_date" AS MATERIALIZED (
    (
        SELECT
            "selected_today".run_date,
            "selected_today".run_seconds
        FROM "selected_today"
        WHERE "selected_today".run_seconds IS NOT NULL
    )
    UNION
    (   
        SELECT
            (ldInitialDate + date_id::integer) AS run_date,
            (
              SELECT 
                lnInitialSecAfter + second_id
              FROM generate_series(0, 59) second_id
              WHERE (laPeriodMask[SECOND_POS] & (1::bigint << second_id::integer)) != 0
              LIMIT 1
            ) AS run_seconds
        FROM generate_series(
            CASE WHEN ldInitialDate = (clock_timestamp() AT time zone 'UTC')::date THEN 1 ELSE 0 END, 
            MAX_DAYS_TO_PROCESS) date_id
        WHERE
            NOT EXISTS (SELECT 1 FROM "selected_today" WHERE "selected_today".run_seconds IS NOT NULL)
            AND (laPeriodMask[DAY_POS]   & (1::bigint << (EXTRACT (DAY   FROM (ldInitialDate + date_id::integer)))::integer)) != 0
            AND (laPeriodMask[MONTH_POS] & (1::bigint << (EXTRACT (MONTH FROM (ldInitialDate + date_id::integer)))::integer)) != 0
            AND (laPeriodMask[DOW_POS]   & (1::bigint << (EXTRACT (DOW   FROM (ldInitialDate + date_id::integer)))::integer)) != 0
        LIMIT 1
    ))

    SELECT
        EXTRACT(EPOCH FROM "selected_date".run_date) + "selected_date".run_seconds
    INTO lnNextTime
    FROM "selected_date";
  
  
    RETURN lnNextTime;
  
END;
/*
@example:
    SELECT ub.util_process_crontab('5 * * * * * ')              // every minute at 5th second
    => 1762090925 (value depends on the current timesmamp)
    
    SELECT ub.util_process_crontab('/5 * * * * * ')             // every 5 seconds
    => 1762092675 (value depends on the current timesmamp)
    
    SELECT ub.util_process_crontab(* * 5,10,15 * * *')          // every day at 5 a.m., 10 a.m. and 3 p.m. UTC
    => 1762092600 (value depends on the current timesmamp)
    
    SELECT ub.util_process_crontab('* * 3 * * MON,WED,FRI')     // at 3 a.m. on Monday, Wednesday and Friday
    => 1762092600 (value depends on the current timesmamp)
*/
$BODY$;

ALTER FUNCTION ub.util_process_crontab(text)
    OWNER TO postgres;





-- FUNCTION: ub.util_verificator(text, text)

-- DROP FUNCTION IF EXISTS ub.util_verificator(text, text);

CREATE OR REPLACE FUNCTION ub.util_verificator(
    lcinput text,
    lcformat text)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 1
    IMMUTABLE PARALLEL SAFE 
AS $BODY$

/*
@function ub.util_verificator
@desc Universal verificator of multiple types and formats

@param string $1 - input text to verify
@param string $2 - type or format to validate:
    - JSON = json format
    - JSONOBJECT = json object
    - JSONARRAY = json array
    - JSONPATH = jsonpath format
    - TSQUERY = tsquery format
    - DATE_<format> = date in the specified format
    - DATETIME_<format> = datetime in the specified format
    - TIME = time format
    - BASE64 = base64 format
    - EMAIL = email format
    - URL = url format
    - EXT = check if an extension is installed
    
@return string - "TRUE" = type/format is valid | "FALSE" = invalid type/format

@version 0.2.1
@author Oleg Pravdin <o.pravdin@unibackend.org>
#validate type #validate format #validate #json #time #base64 #clause #email #url
*/
    
    
DECLARE
    lcText        text;
    lbResponse      boolean;
    ljJsonPath      jsonpath;
    ltFTSQuery      tsquery;

    ljResponse      jsonb;
 
 
BEGIN

    -- Normalize format value
    lcFormat := upper(btrim(lcFormat));
  
    CASE
    
        -- check json format
        WHEN lcFormat = 'JSON' THEN        
            ljResponse := lcInput::jsonb;
        
        
        -- check json array format
        WHEN lcFormat = 'JSONOBJECT' THEN
            IF COALESCE(jsonb_typeof(lcInput::jsonb), 'null') NOT IN ('object', 'null') THEN
                RETURN 'FALSE';
            END IF;
        
        
        -- check json array format
        WHEN lcFormat = 'JSONARRAY' THEN   
            IF COALESCE(jsonb_typeof(lcInput::jsonb), 'null') NOT IN ('array', 'null') THEN
                RETURN 'FALSE';
            END IF;
        
        
        -- check jsonpath format
        WHEN lcFormat = 'JSONPATH' THEN    
            ljJsonPath := lcInput::jsonpath;
        
        
        -- check date format
        WHEN lcFormat ~ '^DATE([_]|$)' THEN       
            lcText := to_date(lcInput, COALESCE((regexp_match(lcFormat, '(?<=_).*'))[1], 'YYYY-MM-DD'))::text;
        
        
        -- check date format
        WHEN lcFormat ~ '^DATETIME([_]|$)' THEN       
            lcText := to_timestamp(lcInput, COALESCE((regexp_match(lcFormat, '(?<=_).*'))[1], 'YYYY-MM-DD HH24:MI:SS'))::text;
        
        
        -- check tsquery format
        WHEN lcFormat = 'TSQUERY' THEN
            ltFTSQuery := lcInput::tsquery;
        
        
        -- check time format
        WHEN lcFormat = 'TIME' THEN        
            lcText := (lcInput::time)::text;
        
        
        -- check base64 format
        WHEN lcFormat = 'BASE64' THEN      
            lcText := convert_from(decode(lcInput, 'base64'), 'SQL_ASCII');
        
        
        -- check Email format
        WHEN lcFormat = 'EMAIL' THEN       
            IF NOT lcInput ~ '^[\x21\x23-\x5b\x5d-\x7F]{1,}\@[0-9A-Za-z\.\-]{1,}\.[a-zA-Z]{2,}$' THEN
                RETURN 'FALSE';
            END IF;
        
        
        -- check URL format
        WHEN lcFormat = 'URL' THEN         
            IF NOT lcInput ~ '^(?:http(s)?:\/\/)?[\w.-]+(?:\.[\w\.-]+)+[\w\-\._~:/?#[\]@!\$&''\(\)\*\+,;=.]+$' THEN
                RETURN 'FALSE';
            END IF;
        
        
        -- check the extension is installed
        WHEN lcFormat ~ 'EXT' THEN
        
            IF NOT EXISTS (
                SELECT 1
                FROM pg_extension 
                WHERE lower(extname) = lower(lcInput)
            ) THEN 
            
                RETURN 'FALSE';
                
            END IF;
        
        -- unknown format => return "FALSE"
        ELSE    

            RETURN 'FALSE';
        
    END CASE;
  
    RETURN 'TRUE';
  
  
EXCEPTION
  
    -- Invalid type/format in case of exception
    WHEN OTHERS THEN 
        RETURN 'FALSE';
  
END;
/*
@example:
    SELECT ub.util_verificator('[{2,3]', 'JSON')
    => FALSE
    
    SELECT SELECT ub.util_verificator('[2,3]', 'JSONARRAY')
    => TRUE
    
    SELECT ub.util_verificator('12.10.2020', 'DATE_YYYY-MM-DD')
    => FALSE
    
    SELECT ub.util_verificator('https://google.com', 'URL')
    => TRUE
    
    SELECT ub.util_verificator('$.a = "check', 'JSONPATH')
    => FALSE
*/
$BODY$;

ALTER FUNCTION ub.util_verificator(text, text)
    OWNER TO postgres;
