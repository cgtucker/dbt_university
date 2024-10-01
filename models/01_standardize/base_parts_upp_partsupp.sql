with source as (
      select * from {{ source('parts_upp', 'partsupp') }}
),
renamed as (
    select
        {{ adapter.quote("PS_PARTKEY") }} as ps_part_id,
        {{ adapter.quote("PS_SUPPKEY") }} as ps_id,
        {{ adapter.quote("PS_AVAILQTY") }} as ps_quantity_available,
        {{ adapter.quote("PS_SUPPLYCOST") }} as ps_supply_cost,
        {{ adapter.quote("PS_COMMENT") }} as ps_comment

    from source
)
renamed_typed_partsupp(
    select
        ps.ps_part_id::integer as ps_part_id,
        ps.ps_id::integer as ps_id,
        ps.ps_quantity_available::integer as ps_quantity_available,
        ps.ps_supply_cost::numeric(6,2) as ps_supply_cost,
        ps.comment::text as ps.comment
    from renamed
)
select * from renamed_typed_partsupp
  