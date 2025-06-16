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
