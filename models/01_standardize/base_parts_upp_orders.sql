with source as (
      select * from {{ source('parts_upp', 'orders') }}
),
renamed as (
    select
        {{ adapter.quote("O_ORDERKEY") }} as order_id,
        {{ adapter.quote("O_CUSTKEY") }},
        {{ adapter.quote("O_ORDERSTATUS") }},
        {{ adapter.quote("O_TOTALPRICE") }},
        {{ adapter.quote("O_ORDERDATE") }},
        {{ adapter.quote("O_ORDERPRIORITY") }},
        {{ adapter.quote("O_CLERK") }},
        {{ adapter.quote("O_SHIPPRIORITY") }},
        {{ adapter.quote("O_COMMENT") }}

    from source
)
select * from renamed
  