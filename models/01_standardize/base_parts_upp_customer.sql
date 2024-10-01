with source as (
      select * from {{ source('parts_upp', 'c') }}
),
renamed as (
    select
        {{ adapter.quote("C_CUSTKEY") }} as c_id,
        {{ adapter.quote("C_NAME") }} as c_name,
        {{ adapter.quote("C_ADDRESS") }} as c_address,
        {{ adapter.quote("C_NATIONKEY") }} as c_nation_id,
        {{ adapter.quote("C_PHONE") }} as c_phone,
        {{ adapter.quote("C_ACCTBAL") }} as c_account_balance,
        {{ adapter.quote("C_MKTSEGMENT") }} as c_market_segment,
        {{ adapter.quote("C_COMMENT") }} as comment

    from source
)
renamed_typed_records as (
    select
        c.c_id::integer                                  as c_id,
        c.c_name::text                                   as c_name,
        c.c_address::text                                as c_address,
        c.c_nation_id::integer                           as c_nation_id,
        c.c_phone::text                                  as c_phone,
        c.c_account_balance::numeric(7,2)                as c_account_balance,
        c.c_market_segment::text                         as c_market_segment,
        c.c_comment::text                                as c_comment
    from renamed
)

select * from renamed_typed_records
  