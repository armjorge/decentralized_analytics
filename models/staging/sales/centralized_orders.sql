-- Camunda 
with insabi_orders as (
    SELECT
        orden
        ,instituto
    FROM
            {{source('eseotres_raw', 'camunda')}}
),

imss_orders as (
    SELECT
        orden
        ,instituto
    FROM
            {{source('eseotres_raw', 'SAI')}}
)

select 
    *
    from imss_orders
UNION ALL   
select 
    *
    from insabi_orders
order by instituto, orden