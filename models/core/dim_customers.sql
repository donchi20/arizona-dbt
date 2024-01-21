with
    customers as 
    (
        select * from {{ ref('stg_customers') }}
    ),

    orders as
    (
        select * from {{ ref('stg_orders') }}
    ),
    products as
    (
        select * from {{ ref('stg_products') }}
    ),

    customer_orders as 
    (
        select 
            customer_id,
            min(order_date) as first_order_date,
            max(order_date) as most_recent_order_date,
            count(order_number) as number_of_orders,
            sum(order_quantity) as units_purchased,
            sum(order_quantity * product_price) as customer_spend

        from  orders
        left join products using (product_id)
        group by 1
    ),

    dim_customers as 
    (
        select 
            row_number() over(order by customer_id asc) as customer_pk,
            c.customer_id,
            c.first_name,
            c.last_name,
            c.email,
            c.gender,
            c.birth_date,
            c.marital_status,
            c.home_owner,
            c.total_children,
            c.education_level,
            c.occupation,
            c.annual_income,
            co.first_order_date,
            co.most_recent_order_date,
            coalesce(co.number_of_orders, 0) as number_of_orders,
            coalesce(co.units_purchased, 0) as units_purchased,
            coalesce(co.customer_spend, 0) as customer_spend

        from
            customers c
            left join customer_orders co using (customer_id)
    )

select * from dim_customers