with
    products as 
    (
        select * from {{ ref('stg_products') }}
    ),

    sales as
    (
        select * from {{ ref('stg_sales') }}
    ),

    product_sales as 
    (
        select 
            product_id,
            sum(order_quantity) as units_sold
        from sales
        group by 1
    ),

    final as 
    (
        select 
            row_number() over(order by product_id asc) as product_pk,
            p.product_id,
            p.product_name,
            p.model_name,
            p.product_cost,
            p.product_price,
            coalesce(s.units_sold,0) as units_sold,
            (s.units_sold * p.product_price) as revenue

        from
            products as p
            left join product_sales as s using (product_id)
    )

select * from final