with source as (
      select * from {{ source('parts_upp', 'lineitem') }}
),
renamed as (
    select
        {{ adapter.quote("L_ORDERKEY") }} as order_id,
        {{ adapter.quote("L_PARTKEY") }} as part_id,
        {{ adapter.quote("L_SUPPKEY") }} as supplier_id,
        {{ adapter.quote("L_LINENUMBER") }} as line_number,
        {{ adapter.quote("L_QUANTITY") }} as quantity,
        {{ adapter.quote("L_EXTENDEDPRICE") }} as extended_price,
        {{ adapter.quote("L_DISCOUNT") }} as percent_discount,
        {{ adapter.quote("L_TAX") }} as percent_tax,
        {{ adapter.quote("L_RETURNFLAG") }} as return_flag_status,
        {{ adapter.quote("L_LINESTATUS") }} as is_fulfilled,
        {{ adapter.quote("L_SHIPDATE") }} as ship_date,
        {{ adapter.quote("L_COMMITDATE") }} as commit_date,
        {{ adapter.quote("L_RECEIPTDATE") }} as receipt_date,
        {{ adapter.quote("L_SHIPINSTRUCT") }} as shipping_instructions,
        {{ adapter.quote("L_SHIPMODE") }} as shipping_by,
        {{ adapter.quote("L_COMMENT") }} as comment

    from source
)
renamed_typed_lineitem as (
    select
        l.order_id::integer as order_id,
        l.part_id::integer as part_id,
        l.supplier_id::integer as supplier_id,
        l.line_number::integer as line_number,
        l.quantity::numeric(5,2) as quantity,
        l.extended_price::numeric(8,2) as extended_price,
        l.percent_discount::numeric(2,2) as percent_discount,
        l.percent_tax::numeric(2,2) as percent_tax,
        l.return_flag_status::char(1) as return_flag_status,
        l.is_fulfilled::char(1) as is_fulfilled,
        l.ship_date::timestamp as ship_date,
        l.commit_date::timestamp as commit_date,
        l.receipt_date::timestamp as receipt_date,
        l.shipping_instructions::text as shipping_instructions,
        l.shipping_by::varchar(7) as shipping_by,
        l.comment::text as comment
    from renamed
)
select * from renamed_typed_lineitem
  