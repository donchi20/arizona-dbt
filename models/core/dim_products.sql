with
    products as 
    (
        select * from {{ ref('stg_products') }}
    ),

    orders as
    (
        select * from {{ ref('stg_orders') }}
    ),

    product_orders as 
    (
        select 
            product_id,
            sum(order_quantity) as units_sold

        from  orders
        group by 1
    ),

    dim_products as 
    (
        select 
            row_number() over(order by product_id asc) as product_pk,
            p.product_id,
            p.product_name,
            p.model_name,
            p.product_cost,
            p.product_price,
            coalesce(s.units_sold,0) as units_sold,
            (s.units_sold * p.product_price) as product_revenue

        from
            products as p
            left join product_orders as s using (product_id)
    )

select * from dim_products