
with grouped_items as( 
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
        {{ref('logistics_lined_no_centrals')}}
    GROUP BY
        orden_venta
    )
    
select * from grouped_items