with territory as
(
    select * from {{ ref('stg_territory') }}
),

orders as 
(
    select * from {{ ref('stg_orders') }}
),

territory_orders as 
(
    select
        territory_id,
        sum(order_quantity) as units_sold

    from orders
    group by 1
),


dim_territory as 
(
    select
        t.territory_id,
        t.region,
        t.country,
        t.continent,
        coalesce(todr.units_sold, 0) as units_sold

    from
        territory t 
        left join territory_orders todr using(territory_id)
)

select * from dim_territory