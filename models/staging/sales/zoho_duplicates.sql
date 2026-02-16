
WITH base_duplicates_orders_cte as (

    select 
    orden_venta
    ,file_name
    ,ROW_NUMBER() OVER (PARTITION BY orden_venta ORDER BY file_name) as ordenes_duplicadas
    from {{ref('zoho_lined_items')}}
    group by orden_venta, file_name
    order by orden_venta
)

, list_duplicates_orders_cte as(
    select 
    orden_venta
    from base_duplicates_orders_cte
    where ordenes_duplicadas > 1

)


, duplicates_details AS (

SELECT 
    orden_venta,
    MIN(fecha_emision) AS fecha_emision,
    LISTAGG(DISTINCT institucion_homologada, ', ') AS institucion_homologada,
    LISTAGG(DISTINCT orden, ', ') AS orden,
    LISTAGG(DISTINCT punto_entrega, ', ') AS punto_entrega,
    SUM(importe) AS importe,
    LISTAGG('Clave: ' || clave || ' Piezas: ' || piezas || ' Precio: ' || precio, ', ') AS items,
    LISTAGG(DISTINCT orden_estatus, ', ') AS orden_estatus,
    LISTAGG(DISTINCT file_name, ', ') AS file_name,
    ROW_NUMBER() OVER (PARTITION BY orden_venta ORDER BY file_name) as ordenes_duplicadas
FROM 
    {{ref('zoho_lined_items')}}
WHERE orden_venta in (select distinct orden_venta from list_duplicates_orders_cte)
GROUP BY 
    orden_venta, file_name
ORDER BY 
    orden_venta
)


select *
from duplicates_details