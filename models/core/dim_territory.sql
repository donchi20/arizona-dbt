with territory as
(
    select * from {{ ref('stg_territory') }}
),

orders as 
(
    select * from {{ ref('stg_orders') }}
),

products as 
(
    select * from {{ ref('stg_products') }}
),

territory_orders as 
(
    select
        territory_id,
        sum(order_quantity) as units_sold,
        sum(p.product_price * order_quantity) as revenue

    from orders o
    left join products p using(product_id)
    group by 1
),


dim_territory as 
(
    select
        row_number() over(order by territory_id asc) as territory_pk,
        t.territory_id,
        t.region,
        t.country,
        t.continent,
        coalesce(todr.units_sold, 0) as units_sold,
        todr.revenue as revenue

    from
        territory t 
        left join territory_orders todr using(territory_id)
)

select * from dim_territory