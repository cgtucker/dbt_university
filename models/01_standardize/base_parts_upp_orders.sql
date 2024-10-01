with source as (
      select * from {{ source('parts_upp', 'orders') }}
),
renamed as (
    select
        {{ adapter.quote("O_ORDERKEY") }} as o_id,
        {{ adapter.quote("O_CUSTKEY") }} as o_customer_id,
        {{ adapter.quote("O_ORDERSTATUS") }} as o_status,
        {{ adapter.quote("O_TOTALPRICE") }} as o_price_total,
        {{ adapter.quote("O_ORDERDATE") }} as o_date_ordered,
        {{ adapter.quote("O_ORDERPRIORITY") }} as o_priority_order,
        {{ adapter.quote("O_CLERK") }} as o_clerk,
        {{ adapter.quote("O_SHIPPRIORITY") }} as o_priority_ship,
        {{ adapter.quote("O_COMMENT") }} o_comment

    from source
)
renamed_typed_order(
    select
        o.o_id::integer                 as o_id,
        o.o_customer_id::integer        as o_customer_id,
        o.o_status::varchar(1)          as o_status,
        o.o_price_total::numeric(9,2)   as o_price_total,
        o.o_date_ordered::timestamp     as o_date_ordered,
        o.o_priority_order::varchar(15) as o_priority_order,
        o.o_clerk::varchar(15)          as o_clerk,
        o.o_priority_ship::integer      as o_priority_ship,
        o.o_comment::text               as o_comment
    from renamed
) 
select * from renamed_typed_order
  