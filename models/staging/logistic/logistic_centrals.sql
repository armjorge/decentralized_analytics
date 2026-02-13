with source_items as ( 
    select 
     orden
    ,instituto
    from 
        {{ref('logistics_lined')}}
    ),

not_centralized_deliveries as (
    select 
        *
    from 
        source_items
    where 
        orden not in ( select orden from {{ref('no_centrailized_orders')}} )
), 

lined_items as (
    select  
        orden
        ,length(orden) as char_orden
        ,instituto
    from
        not_centralized_deliveries
    where 
        instituto in ('ISSSTE','INSABI', 'IMSS BIENESTAR', 'IMSS')
    order by 
        instituto, char_orden desc
)

select * from lined_items