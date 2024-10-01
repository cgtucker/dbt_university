with source as (
      select * from {{ source('parts_upp', 'part') }}
),
renamed as (
    select
        {{ adapter.quote("P_PARTKEY") }} as p_id,
        {{ adapter.quote("P_NAME") }} as p_name,
        {{ adapter.quote("P_MFGR") }} as p_manufacturer,
        {{ adapter.quote("P_BRAND") }} as p_brand,
        {{ adapter.quote("P_TYPE") }} as p_type,
        {{ adapter.quote("P_SIZE") }} as p_size,
        {{ adapter.quote("P_CONTAINER") }} p_container,
        {{ adapter.quote("P_RETAILPRICE") }} p_price_retail,
        {{ adapter.quote("P_COMMENT") }} p_comment

    from source
)
renamed_typed_part(
    select
        p.p_id::integer as p_id,
        p.p_name::text as p_name,
        p.p_manufacturer::varchar(14) as p_manufacturer,
        p.p_brand::varchar(8) as p_brand,
        p.p_type::text as p_type,
        p.p_size::integer as p_size,
        p.p_container::text as p_container, 
        p.p_price_retail::numeric(6,2) as p_price_retail,
        p.p_comment::text as p_comment
    from renamed
)
select * from renamed_typed_part
  