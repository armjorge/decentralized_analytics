WITH logistics_lined as ( 
SELECT 
    $1::string as codigo_entrega, 
    trim($2::string) as orden, 
    trim($3::string) as orden_venta_raw, 
    $4::string as factura, 
    trim(REGEXP_REPLACE($5::string, '[^a-zA-Z0-9 ]', '')) as instituto, 
    $6::string as clave, 
    $7::numeric as piezas_entregadas, 
    $8::date as fecha_entrega, 
    $9::string as comentarios_entrega, 
    $10::string as proveedor3pl
FROM
    @{{source('eseotres_landing', 'BASE_CSV')}}/SUMA_PTYCSA.csv
    (FILE_FORMAT => eseotres.landing_data.pipe_csv)

),

-- Step 1: Handle all modifications to 'orden_venta'
calc_orden_venta AS (
    SELECT 
        codigo_entrega,
        orden,
        factura,
        instituto,
        clave,
        piezas_entregadas,
        fecha_entrega,
        comentarios_entrega,
        proveedor3pl,
        -- LOGIC FOR ORDEN_VENTA
        CASE 
            -- 1. Override based on codigo_entrega (Logic 5)
            WHEN codigo_entrega = 'P-eseo-00054-2412' THEN 'SO24-00404'
            WHEN codigo_entrega = 'P-eseo-00018-2308' THEN 'SO23-0954'
            WHEN codigo_entrega = 'P-eseo-00032-2307' THEN 'SO23-0951'
            WHEN codigo_entrega = 'P-eseo-00001-2307' THEN 'SO23-0948'
            WHEN codigo_entrega IN ('P-eseo-00107-2308', 'P-eseo-00108-2308', 'P-eseo-00003-2309') THEN 'SO23-0956'
            WHEN codigo_entrega IN ('P-eseo-00150-2309', 'P-eseo-00149-2309') THEN 'SO23-0919'
            WHEN codigo_entrega = 'P-eseo-00147-2309' THEN 'SO23-0819'
            
            -- 2. Fix specific typo (Logic 6)
            -- Note: We check the version without spaces to be safe, as Logic 1 happens globally
            WHEN REPLACE(orden_venta_raw, ' ', '') = 'O23-1002' THEN 'SO23-1002'
            
            -- 3. Default: Just remove spaces (Logic 1)
            ELSE REPLACE(orden_venta_raw, ' ', '')
        END AS orden_venta
    FROM logistics_lined
),

cleaned_institucion as (
SELECT 
    codigo_entrega,
    orden,
    orden_venta,
    factura,
    -- LOGIC FOR INSTITUTO
    CASE 
        -- Logic 2
        WHEN orden_venta IN ('SO24-00053','SO24-00054', 'SO24-00055') 
            THEN 'Hospital Regional de Alta Especialidad Ciudad Salud'
        -- Logic 3
        WHEN orden_venta = 'SO24-00059' 
            THEN 'HOSPITAL DE ESPECIALIDADES PEDIATRICAS'
        -- Logic 4 (Standardize ISSSTE)
        WHEN instituto = 'INSTITUTO DE SEGURIDAD Y SERVICIOS SOCIALES DE LOS TRABAJADORES DEL ESTADO' 
            THEN 'ISSSTE'
        -- Default: Keep original
        ELSE instituto
    END AS instituto,
    clave,
    piezas_entregadas,
    fecha_entrega,
    comentarios_entrega,
    proveedor3pl
FROM calc_orden_venta
)

SELECT 
    *,
    piezas_entregadas * 
    CASE clave
        WHEN '010.000.4224.00' THEN 207.00
        WHEN '010.000.4154.00' THEN 207.00
        WHEN '010.000.0246.00' THEN 69.00
        WHEN '010.000.4242.00' THEN 111.00
        WHEN '010.000.4411.00' THEN 83.00
        WHEN '010.000.5319.01' THEN 327.00
        ELSE 0 -- Price isn't known
    END AS importe_entregado
FROM cleaned_institucion

