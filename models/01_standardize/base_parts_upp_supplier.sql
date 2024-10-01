with source as (
      select * from {{ source('parts_upp', 'supplier') }}
),
renamed as (
    select
        {{ adapter.quote("S_SUPPKEY") }},
        {{ adapter.quote("S_NAME") }},
        {{ adapter.quote("S_ADDRESS") }},
        {{ adapter.quote("S_NATIONKEY") }},
        {{ adapter.quote("S_PHONE") }},
        {{ adapter.quote("S_ACCTBAL") }},
        {{ adapter.quote("S_COMMENT") }}

    from source
)
select * from renamed
  