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
