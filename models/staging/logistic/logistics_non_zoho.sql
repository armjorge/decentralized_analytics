
with source_items as (
    select
        orden_venta
        ,instituto
        ,clave
        ,piezas_entregadas
        ,importe_entregado
        ,proveedor3pl
        ,codigo_entrega
        ,orden
        ,factura
    from 
        {{ref('logistics_lined')}}
     
    ),

not_centralized_deliveries as (
    select 
        *
    from 
        source_items
    where 
        orden not in ( select orden from {{ref('no_centrailized_orders')}} )
),

non_zoho_order as (

    select *
    from 
        not_centralized_deliveries
    where 
        orden_venta not in ( select distinct orden_venta from {{ref('zoho_lined_items')}} )

)

select *
from non_zoho_order
order by piezas_entregadas ASC
