with source as (
      select * from {{ source('parts_upp', 'partsupp') }}
),
renamed as (
    select
        {{ adapter.quote("PS_PARTKEY") }},
        {{ adapter.quote("PS_SUPPKEY") }},
        {{ adapter.quote("PS_AVAILQTY") }},
        {{ adapter.quote("PS_SUPPLYCOST") }},
        {{ adapter.quote("PS_COMMENT") }}

    from source
)
select * from renamed
  