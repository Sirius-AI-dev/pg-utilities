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
