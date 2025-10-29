{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (payment_card_id)"
) }}


WITH source AS (
    SELECT
        "payCardID",
        "payCardBank",
        "payCardName",
        "payCardSex",
        "payCardBirthDate"
    FROM {{ source('staging', 'stg_transactions') }}
),

-- Ubah integer ke tahun lahir (pastikan nilainya seperti 1990, bukan 900101)
transformed AS (
    SELECT DISTINCT
        "payCardID" AS payment_card_id,
        "payCardBank" AS card_bank,
        "payCardName" AS card_name,
        "payCardSex" AS gender,
        "payCardBirthDate"::int AS birth_year,
        EXTRACT(YEAR FROM CURRENT_DATE) - "payCardBirthDate"::int AS age,
        CASE
            WHEN EXTRACT(YEAR FROM CURRENT_DATE) - "payCardBirthDate"::int < 18 THEN 'Under 18'
            WHEN EXTRACT(YEAR FROM CURRENT_DATE) - "payCardBirthDate"::int BETWEEN 18 AND 25 THEN '18-25'
            WHEN EXTRACT(YEAR FROM CURRENT_DATE) - "payCardBirthDate"::int BETWEEN 26 AND 35 THEN '26-35'
            WHEN EXTRACT(YEAR FROM CURRENT_DATE) - "payCardBirthDate"::int BETWEEN 36 AND 45 THEN '36-45'
            WHEN EXTRACT(YEAR FROM CURRENT_DATE) - "payCardBirthDate"::int BETWEEN 46 AND 60 THEN '46-60'
            ELSE '60+'
        END AS age_group
    FROM source
)

SELECT * FROM transformed

