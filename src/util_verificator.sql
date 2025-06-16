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
