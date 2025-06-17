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
