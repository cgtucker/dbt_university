with source as (
      select * from {{ source('parts_upp', 'nation') }}
),
renamed as (
    select
        {{ adapter.quote("N_NATIONKEY") }} as n_id,
        {{ adapter.quote("N_NAME") }} as n_name,
        {{ adapter.quote("N_REGIONKEY") }} as n_region_id,
        {{ adapter.quote("N_COMMENT") }} as n_comment

    from source
)
renamed_typed_nation(
    select
        n.n_id::integer                as n_id,
        n.n_name::varchar(36)          as n_name,
        n.n_region_id::integer         as n_region_id,
        n.n_comment::text              as n_comment
    from renamed
)
select * from renamed_typed_nation
  