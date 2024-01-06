with orders as
(
    select * from {{ ref('stg_orders') }}
),

customers as 
(
    select * from {{ ref('dim_customers') }}
),

products as 
(
    select * from {{ ref('dim_products') }}
),

territory as 
(
    select * from {{ ref('dim_territory') }}
),

fact_orders as
(
    select
        order_date,
        order_number,
        product_id,
        p.product_pk as product_fk,
        customer_id,
        c.customer_pk as customer_fk,
        territory_id,
        t.territory_pk as territory_fk,
        order_quantity,
        p.product_cost as cost,
        p.product_price as price,
        p.product_price * order_quantity as revenue
    
    from
        orders o 
        left join products p using(product_id)
        left join customers c using(customer_id)
        left join territory t using(territory_id)
)

select * from fact_orders