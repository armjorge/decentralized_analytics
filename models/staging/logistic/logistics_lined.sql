WITH logistics_lined as ( 
SELECT 
    $1::string as codigo_entrega, 
    $2::string as orden, 
    $3::string as orden_venta, 
    $4::string as factura, 
    $5::string as instituto, 
    $6::string as clave, 
    $7::numeric as piezas_entregadas, 
    $8::date as fecha_entrega, 
    $9::string as comentarios_entrega, 
    $10::string as proveedor3pl
FROM
    @{{source('eseotres_landing', 'BASE_CSV')}}/SUMA_PTYCSA.csv
    (FILE_FORMAT => eseotres.landing_data.pipe_csv)

)

SELECT *
FROM logistics_lined