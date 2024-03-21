with
    products as 
    (
        select * from {{ ref('stg_products') }}
    ),

    subcategory as
    (
        select * from {{ ref('stg_product_subcategories') }}
    ),

    category as
    (
        select * from {{ ref('stg_product_categories') }}
    ),

    orders as
    (
        select * from {{ ref('stg_orders') }}
    ),

    product_orders as 
    (
        select 
            product_id,
            sum(order_quantity) as units_sold,
            max(order_date) as last_order_date

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
            sc.subcategory,
            c.category,
            p.product_cost,
            p.product_price,
            coalesce(s.units_sold,0) as units_sold,
            last_order_date,
            (s.units_sold * p.product_price) as product_revenue

        from
            products as p
            left join subcategory as sc using (subcategory_id)
            left join category as c using (category_id)
            left join product_orders as s using (product_id)
    )

select * from dim_products