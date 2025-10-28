with source_data as (

    select
        "corridorID",
        nullif(trim("corridorName"), '') as corridor_name
    from {{ source('staging', 'stg_transactions') }}
    where "corridorID" is not null

),

filled_name as (
    -- Gunakan nama koridor yang paling sering muncul untuk setiap ID
    select
        "corridorID" as corridor_id,
        -- Ambil nama terbanyak per corridor_id
        mode() within group (order by corridor_name) as corridor_name
    from source_data
    where corridor_name is not null
    group by "corridorID"
),

-- Tangani corridor_id yang tidak punya nama sama sekali
with_nulls as (
    select
        distinct "corridorID" as corridor_id
    from source_data
    where corridor_name is null
)

select
    coalesce(f.corridor_id, n.corridor_id) as corridor_id,
    coalesce(f.corridor_name, 'Unknown Corridor') as corridor_name,
    current_timestamp as created_at
from filled_name f
full outer join with_nulls n on f.corridor_id = n.corridor_id
