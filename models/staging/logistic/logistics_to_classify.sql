with orders_to_clasify as (
    select *
    from {{ref('logistics_with_zoho')}}
    where piezas_venta is null
)

select *
from orders_to_clasify
order by instituto


