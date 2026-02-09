SELECT 
    Orden_venta,
    MIN(Fecha_emision) AS Fecha_emision,
    LISTAGG(DISTINCT Institucion_homologada, ', ') AS Institucion_homologada,
    LISTAGG(DISTINCT Orden, ', ') AS Orden,
    LISTAGG(DISTINCT Punto_entrega, ', ') AS Punto_entrega,
    SUM(Importe) AS Importe,
    LISTAGG('Clave: ' || Clave || ' Piezas: ' || Piezas || ' Precio: ' || Precio, ', ') AS Items,
    LISTAGG(DISTINCT Estatus, ', ') AS status,
    LISTAGG(DISTINCT file_name, ', ') AS file_name
FROM 
    {{ref('zoho_lined_items')}}
GROUP BY 
    Orden_venta, file_name