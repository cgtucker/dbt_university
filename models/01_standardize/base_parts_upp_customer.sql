with source as (
      select * from {{ source('parts_upp', 'customer') }}
),
renamed as (
    select
        {{ adapter.quote("C_CUSTKEY") }} as customer_id,
        {{ adapter.quote("C_NAME") }} as customer_name,
        {{ adapter.quote("C_ADDRESS") }} as customer_address,
        {{ adapter.quote("C_NATIONKEY") }} as customer_nation_id,
        {{ adapter.quote("C_PHONE") }} as customer_phone,
        {{ adapter.quote("C_ACCTBAL") }} as customer_account_balance,
        {{ adapter.quote("C_MKTSEGMENT") }} as customer_market_segment,
        {{ adapter.quote("C_COMMENT") }} as comment

    from source
)
renamed_typed_records as (
    select
        c.customer_id::integer                                  as customer_id,
        c.customer_name::text                                   as customer_name,
        c.customer_address::text                                as customer_address,
        c.customer_nation_id::integer                           as customer_nation_id,
        c.customer_phone::text                                  as customer_phone,
        c.customer_account_balance::numeric(7,2)                as customer_account_balance,
        c.customer_market_segment::text                         as customer_market_segment,
        c.customer_comment::text                                as customer_comment
    from renamed
)

select * from renamed_typed_records
  