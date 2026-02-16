
with grouped_sales as ( 
    select
    orden_venta,
    fecha_emision,
    institucion_homologada,
    orden,
    punto_entrega,
    importe,
    items,
    orden_estatus,
    file_name    
    from {{ref('zoho_grouped')}}
),

grouped_deliveries as (
    select
    orden_venta as orden_venta_logistic,
    instituto, 
    importe_entregado,
    proveedor3pl,
    codigo_entrega,
    orden,
    facturas_entregadas    
    from {{ref('logistics_not_centrals_grouped')}}
),

sales_deliveries as (
select
     sales.orden_venta
    ,deliveries.orden_venta_logistic
    ,sales.importe
    ,deliveries.importe_entregado
from 
    grouped_sales sales
    LEFT JOIN grouped_deliveries deliveries
    ON   sales.orden_venta = deliveries.orden_venta_logistic
)


select * from sales_deliveries



