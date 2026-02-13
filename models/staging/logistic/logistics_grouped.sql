
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
        orden not in ( select orden from {{ref('no_centrailized_orders')}} )
),

grouped_items as( 
    SELECT 
         orden_venta
        ,LISTAGG(DISTINCT instituto, ', ') as instituto
        ,LISTAGG(DISTINCT orden, ', ') as orden
        ,sum(importe_entregado) as importe_entregado
        ,LISTAGG(DISTINCT proveedor3pl, ', ') as proveedor3pl
        ,LISTAGG(DISTINCT codigo_entrega, ', ') as codigo_entrega
        ,LISTAGG(DISTINCT clave || ': '|| piezas_entregadas, ', ') as items_entregados
        ,LISTAGG(DISTINCT factura) as facturas_entregadas
    FROM 
        not_centralized_deliveries
    GROUP BY
        orden_venta
    )
    
select * from grouped_items