with source as (
      select * from {{ source('parts_upp', 'part') }}
),
renamed as (
    select
        {{ adapter.quote("P_PARTKEY") }},
        {{ adapter.quote("P_NAME") }},
        {{ adapter.quote("P_MFGR") }},
        {{ adapter.quote("P_BRAND") }},
        {{ adapter.quote("P_TYPE") }},
        {{ adapter.quote("P_SIZE") }},
        {{ adapter.quote("P_CONTAINER") }},
        {{ adapter.quote("P_RETAILPRICE") }},
        {{ adapter.quote("P_COMMENT") }}

    from source
)
select * from renamed
  