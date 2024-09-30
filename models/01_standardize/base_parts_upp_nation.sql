with source as (
      select * from {{ source('parts_upp', 'nation') }}
),
renamed as (
    select
        {{ adapter.quote("N_NATIONKEY") }} as nation_id,
        {{ adapter.quote("N_NAME") }} as nation_name,
        {{ adapter.quote("N_REGIONKEY") }} as nation_region_id,
        {{ adapter.quote("N_COMMENT") }} as nation_comment

    from source
)
renamed_typed_nation(
    select
        n.nation_id::integer as nation_id,
        n.nation_name::varchar(36) as nation_name,
        n.nation_region_id::integer as nation_region_id,
        n.nation_comment::text as nation_comment
    from renamed
)
select * from renamed_typed_nation
  