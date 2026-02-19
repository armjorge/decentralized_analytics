with focus_invoices as ( 
    select
        fecha_factura,
        uuid,
        rfc_receptor,
        receptor,
        total,
        neto,
        referencia,
        factura
    from {{source('eseotres_raw', 'uuids_institutos')}}
)

select *
from focus_invoices

--dbt run-operation generate_base_model --args '{"source_name": "eseotres_raw", "table_name": "uuids_institutos"}