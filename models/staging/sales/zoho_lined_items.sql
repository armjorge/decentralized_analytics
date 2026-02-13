WITH landing_lined_items as ( 

    SELECT
        $1::string AS institucion_homologada,
        trim(REGEXP_REPLACE($12::STRING, '[^a-zA-Z0-9\\-]', '')) AS orden_venta,
        $3::string AS orden,
        $2::string AS contrato,
        $4::string AS clave,
        $7::string AS producto,
        $10::NUMERIC as piezas,
        $11::NUMERIC AS precio,
        $14::NUMERIC AS importe,        
        $5::DATE AS fecha_emision,
        $6::string AS punto_entrega,
        $8::STRING AS direccion,
        $9::DATE as fecha_max_entrega,
        $13::STRING AS orden_estatus,
        --$15 Notas
        --$16 uso CFDI
        --$17 Forma de pago
        --$18 Quantity Invoiced
        --$19 ShippingState
        $20::STRING as File_name
    FROM
        @{{source('eseotres_landing', 'BASE_CSV')}}/consolidado.csv
        (FILE_FORMAT => eseotres.landing_data.pipe_csv)
),

polished_items as (
    -- STEP 2: CLEAN (Applying business rules and standardization)
    SELECT
        -- Polishing Institucion_homologada
        CASE 
            WHEN (Institucion_homologada IN ('HJM', 'CINSABI')) 
                 OR (
                     Punto_entrega IN (
                        'Hospital Regional de Alta Especialidad de Oaxaca',
                        'INSTITUTO NACIONAL DE ENFERMEDADES RESPIRATORIAS ISMAEL COSÍO VILLEGAS', 
                        'Instituto Nacional de Ciencias Médicas y Nutrición Salvador Zubirán'
                     ) 
                     AND (Institucion_homologada IS NULL OR Institucion_homologada = '')
                 ) THEN 'CCINSHAE'
            WHEN Institucion_homologada = 'Privado' THEN 'PRIVADO'
            WHEN Punto_entrega IN ('IMSS', 'Instituto Mexicano del Seguro Social') THEN 'IMSS'
            WHEN Punto_entrega = 'INSABI' OR Institucion_homologada = 'IMSSBIENESTAR' THEN 'INSABI'
            WHEN Punto_entrega = 'INSTITUTO DE SEGURIDAD Y SERVICIOS SOCIALES DE LOS TRABAJADORES DEL ESTADO' 
                 AND (Institucion_homologada IS NULL OR Institucion_homologada = '') THEN 'ISSSTE'
            ELSE Institucion_homologada 
        END AS Institucion_homologada,

        Contrato,
        Orden,

        -- Standardizing Clave format
        CASE Clave
            WHEN '010 000 4224 00 00' THEN '010.000.4224.00'
            WHEN '010 000 4154 00 00' THEN '010.000.4154.00'
            WHEN '010 000 0246 00 00' THEN '010.000.0246.00' 
            WHEN '010 000 4242 00 00' THEN '010.000.4242.00'
            WHEN '010 000 4411 00 00' THEN '010.000.4411.00'
            WHEN '010 000 5319 01 00' THEN '010.000.5319.01'
            ELSE Clave
        END AS Clave,

        Fecha_emision,

        -- Standardizing Punto_entrega
        CASE 
            WHEN Punto_entrega = 'Secretaría de Salud // Hospital de la mujer' THEN 'Hospital de la Mujer'
            ELSE Punto_entrega 
        END AS Punto_entrega,
        Orden_venta,
        orden_estatus,
        Producto,
        Direccion,
        Fecha_max_entrega,
        Piezas,
        Precio,
        Importe,
        File_name
    FROM landing_lined_items
)

-- FINAL OUTPUT
SELECT * FROM polished_items
WHERE institucion_homologada = 'CCINSHAE'