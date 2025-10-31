{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    post_hook=[
        "ALTER TABLE {{ this }} ADD CONSTRAINT fk_passenger FOREIGN KEY (payment_card_id) REFERENCES {{ ref('dim_card_holder') }}(payment_card_id)",
        "ALTER TABLE {{ this }} ADD CONSTRAINT fk_corridor FOREIGN KEY (corridor_id) REFERENCES {{ ref('dim_corridor') }}(corridor_id)",
        "ALTER TABLE {{ this }} ADD CONSTRAINT fk_tapin FOREIGN KEY (tap_in_stop_id) REFERENCES {{ ref('dim_stop') }}(stop_id)",
        "ALTER TABLE {{ this }} ADD CONSTRAINT fk_tapout FOREIGN KEY (tap_out_stop_id) REFERENCES {{ ref('dim_stop') }}(stop_id)",
        "ALTER TABLE {{ this }} ADD CONSTRAINT fk_timein FOREIGN KEY (time_id_in) REFERENCES {{ ref('dim_time') }}(time_id)",
        "ALTER TABLE {{ this }} ADD CONSTRAINT fk_timeout FOREIGN KEY (time_id_out) REFERENCES {{ ref('dim_time') }}(time_id)"
    ]
) }}

WITH src AS (
    SELECT DISTINCT
        "transID" AS transaction_id,
        "payCardID" AS payment_card_id,
        "corridorID" AS corridor_id,
        "corridorName" AS corridor_name,
        "tapInStops" AS tap_in_stop_id,
        "tapInStopsName" AS tap_in_stop_name,
        "tapOutStops" AS tap_out_stop_id,
        "tapOutStopsName" AS tap_out_stop_name, 
        TO_CHAR("tapInTime"::timestamp, 'YYYYMMDDHH24MI')::BIGINT AS time_id_in,
        TO_CHAR("tapOutTime"::timestamp, 'YYYYMMDDHH24MI')::BIGINT AS time_id_out,
        "direction",
        "stopStartSeq" AS stop_start_seq,
        "stopEndSeq" AS stop_end_seq,
        EXTRACT(EPOCH FROM ("tapOutTime"::timestamp - "tapInTime"::timestamp)) / 60 AS duration_minutes,
        NULL::FLOAT AS distance_km,
        "payAmount" AS pay_amount,
        "tapInTime"::timestamp AS tap_in_timestamp
    FROM {{ source('staging', 'stg_transactions') }}
    WHERE "payAmount" IS NOT NULL
    {% if is_incremental() %}
        -- Filter berdasarkan timestamp asli untuk performa optimal
        AND "tapInTime"::timestamp > COALESCE(
            (SELECT MAX(TO_TIMESTAMP(time_id_in::text, 'YYYYMMDDHH24MI')) 
             FROM {{ this }} 
             WHERE time_id_in IS NOT NULL),
            '1900-01-01'::timestamp
        )
    {% endif %}
),

corridor_lookup AS (
    SELECT
        corridor_name,
        MAX(corridor_id) AS corridor_id 
    FROM {{ ref('dim_corridor') }}
    GROUP BY corridor_name
),

stop_lookup_in AS (
    SELECT
        stop_name,
        MAX(stop_id) AS stop_id
    FROM {{ ref('dim_stop')}}
    GROUP BY stop_name
),

stop_lookup_out AS (
    SELECT
        stop_name,
        MAX(stop_id) AS stop_id
    FROM {{ ref('dim_stop')}}
    GROUP BY stop_name
),

fact AS (
    SELECT DISTINCT
        s.transaction_id,
        s.payment_card_id,
        COALESCE(s.corridor_id, c.corridor_id) AS corridor_id,
        COALESCE(s.tap_in_stop_id, sl_in.stop_id) AS tap_in_stop_id,
        CASE 
            WHEN s.tap_out_stop_id IS NOT NULL THEN s.tap_out_stop_id
            WHEN s.tap_out_stop_name IS NOT NULL THEN sl_out.stop_id
            ELSE NULL 
        END AS tap_out_stop_id,
        s.time_id_in,
        s.time_id_out,
        s.direction,
        s.stop_start_seq,
        s.stop_end_seq,
        s.duration_minutes,
        s.distance_km,
        s.pay_amount
    FROM src s
    LEFT JOIN corridor_lookup c
        ON s.corridor_name = c.corridor_name
    LEFT JOIN stop_lookup_in sl_in
        ON s.tap_in_stop_name = sl_in.stop_name
    LEFT JOIN stop_lookup_out sl_out
        ON s.tap_out_stop_name = sl_out.stop_name
    WHERE COALESCE(s.corridor_id, c.corridor_id) IS NOT NULL 
        AND COALESCE(s.tap_in_stop_id, sl_in.stop_id) IS NOT NULL
)

-- Langsung SELECT tanpa filter tambahan
SELECT * 
FROM fact