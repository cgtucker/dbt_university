with source as (
      select * from {{ source('parts_upp', 'supplier') }}
),
renamed as (
    select
        {{ adapter.quote("S_SUPPKEY") }} as s_id,
        {{ adapter.quote("S_NAME") }} as s_name,
        {{ adapter.quote("S_ADDRESS") }} as s_address,
        {{ adapter.quote("S_NATIONKEY") }} as s_nation_id,
        {{ adapter.quote("S_PHONE") }} as s_phone,
        {{ adapter.quote("S_ACCTBAL") }} as s_balance_account,
        {{ adapter.quote("S_COMMENT") }} as s_comment

    from source
)
renamed_typed_supplier(
    select
        s.s_id::integer                     as s_id,
        s.s_name::text                      as s_name,
        s.s_address::text                   as s_address,
        s.s_nation_id::integer              as s_nation_id,
        s.s_phone::text                     as s_phone,
        s.s_balance_account::numeric(6,2)   as s_balance_account,
        s.s_comment::text                   as s_comment
    from renamed
)
select * from renamed_typed_supplier
  