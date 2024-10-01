with source as (
      select * from {{ source('parts_upp', 'lineitem') }}
),
renamed as (
    select
        {{ adapter.quote("L_ORDERKEY") }} as li_order_id,
        {{ adapter.quote("L_PARTKEY") }} as li_part_id,
        {{ adapter.quote("L_SUPPKEY") }} as li_supplier_id,
        {{ adapter.quote("L_LINENUMBER") }} as li_line_number,
        {{ adapter.quote("L_QUANTITY") }} as li_quantity,
        {{ adapter.quote("L_EXTENDEDPRICE") }} as li_price_extended,
        {{ adapter.quote("L_DISCOUNT") }} as li_percent_discount,
        {{ adapter.quote("L_TAX") }} as li_percent_tax,
        {{ adapter.quote("L_RETURNFLAG") }} as li_return_flag_status,
        {{ adapter.quote("L_LINESTATUS") }} as li_is_fulfilled,
        {{ adapter.quote("L_SHIPDATE") }} as li_date_shipped,
        {{ adapter.quote("L_COMMITDATE") }} as li_date_commit,
        {{ adapter.quote("L_RECEIPTDATE") }} as li_date_receipt,
        {{ adapter.quote("L_SHIPINSTRUCT") }} as li_shipping_instructions,
        {{ adapter.quote("L_SHIPMODE") }} as li_shipping_by,
        {{ adapter.quote("L_COMMENT") }} as li_comment

    from source
)
renamed_typed_lineitem as (
    select
        l.li_order_id::integer                                 as li_order_id,
        l.li_part_id::integer                                  as li_part_id,
        l.li_supplier_id::integer                              as li_supplier_id,
        l.li_line_number::integer                              as li_line_number,
        l.li_quantity::numeric(5,2)                            as li_quantity,
        l.li_price_extended::numeric(8,2)                      as li_price_extended,
        l.li_percent_discount::numeric(2,2)                    as li_percent_discount,
        l.li_percent_tax::numeric(2,2)                         as li_percent_tax,
        l.li_return_flag_status::char(1)                       as li_return_flag_status,
        l.li_is_fulfilled::char(1)                             as li_is_fulfilled,
        l.li_date_shipped::timestamp                           as li_date_shipped,
        l.li_date_commit::timestamp                            as li_date_commit,
        l.li_date_receipt::timestamp                           as li_date_receipt,
        l.li_shipping_instructions::text                       as li_shipping_instructions,
        l.li_shipping_by::varchar(7)                           as li_shipping_by,
        l.li_comment::text                                     as li_comment
    from renamed
)
select * from renamed_typed_lineitem
  