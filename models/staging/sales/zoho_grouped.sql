with zoho_grouped as (
SELECT 
    orden_venta,
    MIN(fecha_emision) AS fecha_emision,
    LISTAGG(DISTINCT institucion_homologada, ', ') AS institucion_homologada,
    LISTAGG(DISTINCT orden, ', ') AS orden,
    LISTAGG(DISTINCT punto_entrega, ', ') AS punto_entrega,
    SUM(importe) AS importe,
    LISTAGG('Clave: ' || clave || ' Piezas: ' || piezas || ' Precio: ' || precio, ', ') AS items,
    LISTAGG(DISTINCT orden_estatus, ', ') AS orden_estatus,
    LISTAGG(DISTINCT file_name, ', ') AS file_name
FROM 
    {{ref('zoho_lined_items')}}
GROUP BY 
    orden_venta, file_name
ORDER BY 
    orden_venta
) 
SELECT *
FROM zoho_grouped
-- retiramos duplicados
WHERE (orden_venta, file_name) NOT IN (
    ('SO24-00165', '2024-06-01 - 2024-11-30 ZN.xlsx'),
    ('SO24-00166', '2024-06-01 - 2024-11-30.xlsx'),
    ('SO24-00167', '2024-06-01 - 2024-11-30.xlsx')
)

