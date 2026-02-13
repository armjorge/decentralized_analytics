-- Camunda 
with insabi_orders as (
    SELECT
        $1::string as orden
        ,'INSABI' as instituto
    FROM
            @{{source('eseotres_landing', 'BASE_CSV')}}/camunda.csv
            (FILE_FORMAT => eseotres.landing_data.pipe_csv)
),

imss_orders as (
    SELECT
        $3::string as orden
        ,'IMSS' as instituto
    FROM
            @{{source('eseotres_landing', 'BASE_CSV')}}/SAIR_orders.csv
            (FILE_FORMAT => eseotres.landing_data.pipe_csv)
)

select 
    *
    from imss_orders
UNION ALL
select 
    *
    from insabi_orders
order by instituto, orden