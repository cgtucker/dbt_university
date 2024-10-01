with source as (
      select * from {{ source('parts_upp', 'region') }}
),
renamed as (
    select
        {{ adapter.quote("R_REGIONKEY") }} as r_id,
        {{ adapter.quote("R_NAME") }} as r_name,
        {{ adapter.quote("R_COMMENT") }} as r_comment

    from source
)
renamed_typed_region(
    select 
        r.r_id::integer as r_id,
        r.r_name::text as r_name,
        r.r_comment::text as r_comment
    from renamed
)
select * from renamed_typed_region
  