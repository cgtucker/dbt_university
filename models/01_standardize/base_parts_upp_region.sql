with source as (
      select * from {{ source('parts_upp', 'region') }}
),
renamed as (
    select
        {{ adapter.quote("R_REGIONKEY") }},
        {{ adapter.quote("R_NAME") }},
        {{ adapter.quote("R_COMMENT") }}

    from source
)
select * from renamed
  