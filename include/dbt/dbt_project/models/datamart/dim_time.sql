{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (time_id)"
) }}

WITH base AS (
    SELECT
        "tapInTime" AS event_time
    FROM {{ source('staging', 'stg_transactions') }}
    WHERE "tapInTime" IS NOT NULL

    UNION

    SELECT
        "tapOutTime" AS event_time
    FROM {{ source('staging', 'stg_transactions') }}
    WHERE "tapOutTime" IS NOT NULL
),

distinct_times AS (
    SELECT DISTINCT
        TO_CHAR(CAST(event_time AS TIMESTAMP), 'YYYYMMDDHH24MI')::BIGINT AS time_id,
        DATE_TRUNC('minute', CAST(event_time AS TIMESTAMP)) AS full_datetime
    FROM base
),

final AS (
    SELECT
        time_id,
        full_datetime,
        CAST(full_datetime AS DATE) AS date,
        EXTRACT(YEAR FROM full_datetime) AS year,
        EXTRACT(MONTH FROM full_datetime) AS month,
        EXTRACT(DAY FROM full_datetime) AS day,
        TO_CHAR(full_datetime, 'Day') AS day_of_week,
        EXTRACT(HOUR FROM full_datetime) AS hour,
        CASE 
            WHEN EXTRACT(HOUR FROM full_datetime) BETWEEN 5 AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM full_datetime) BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN EXTRACT(HOUR FROM full_datetime) BETWEEN 18 AND 21 THEN 'Evening'
            ELSE 'Night'
        END AS period
    FROM distinct_times
)

SELECT * FROM final
