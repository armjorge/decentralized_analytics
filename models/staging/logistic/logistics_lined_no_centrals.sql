
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
    where
        piezas_entregadas > 0
     
    ),

not_centralized_deliveries as (
    select 
        *
    from 
        source_items
    where 
        orden not in ( select orden from {{ref('centralized_orders')}} )
)

select * from not_centralized_deliveries
