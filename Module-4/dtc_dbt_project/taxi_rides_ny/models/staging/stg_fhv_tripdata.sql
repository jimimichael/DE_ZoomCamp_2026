-- Read FHV raw data that was loaded by load_fhv_data.py
with source as (
    select * from {{ source('raw', 'raw_fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers
        cast(dispatching_base_num as string) as dispatching_base_num,
        
        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast("dropOff_datetime" as timestamp) as dropoff_datetime,
        
        -- location IDs
        cast("PUlocationID" as integer) as pickup_location_id,
        cast("DOlocationID" as integer) as dropoff_location_id,
        
        -- trip info
        cast("SR_Flag" as integer) as sr_flag,
        cast("Affiliated_base_number" as string) as affiliated_base_num
        
    from source
    -- Filter out records with null dispatching_base_num (data quality requirement)
    where dispatching_base_num is not null
)

select * from renamed


