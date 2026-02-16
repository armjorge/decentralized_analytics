with logistics_no_centrals as (
    select * 
    from {{ref('logistics_lined_no_centrals')}}
)

, zoho_lined as ( 
    select 
    orden_venta
    ,clave
    ,piezas
    ,importe
    from {{ref('zoho_lined_items')}}
)

select
    lognc.orden_venta
    ,lognc.instituto
    ,lognc.clave
    ,lognc.piezas_entregadas
    ,lognc.importe_entregado
    ,lognc.proveedor3pl
    ,zl.piezas as piezas_venta
    ,zl.importe as importe_venta
from logistics_lined_no_centrals lognc
left join zoho_lined zl 
    on lognc.orden_venta = zl.orden_venta
