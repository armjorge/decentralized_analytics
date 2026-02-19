with invoices_key as ( 
    select *
    from {{ref('invoices_key')}}
), 

sales_and_deliveries as (
    select *
    from {{ref('sales_delivery_invoices')}}
),

invoices_deliveries as ( 
    select * 
    from invoices_key ik
    left join sales_and_deliveries  sd
    on ik.referencia = sd.orden_venta
)
select * from invoices_deliveries