with invoices_paid as (
      select
        fecha_pago,
        factura,
        uuid,
        deposito

    from {{source('eseotres_raw', 'uuids_pagados')}}
)

select *
from invoices_paid