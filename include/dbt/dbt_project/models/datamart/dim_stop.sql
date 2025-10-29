{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (stop_id)"
) }}

with stops_in as (
    select
        "tapInStops" as stop_id,
        "tapInStopsName" as stop_name,
        "tapInStopsLat" as stop_lat,
        "tapInStopsLon" as stop_lon
    from {{ source('staging', 'stg_transactions') }}
    where "tapInStops" is not null
),

stops_out as (
    select
        "tapOutStops" as stop_id,
        "tapOutStopsName" as stop_name,
        "tapOutStopsLat" as stop_lat,
        "tapOutStopsLon" as stop_lon
    from {{ source('staging', 'stg_transactions') }}
    where "tapOutStops" is not null
),

unioned_stops as (
    select * from stops_in
    union all
    select * from stops_out
)

select distinct
    stop_id,
    stop_name,
    stop_lat,
    stop_lon
from unioned_stops
where stop_id is not null
