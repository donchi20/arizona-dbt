with
    customers as 
    (
        select * from {{ ref('stg_customers') }}
    ),

    orders as
    (
        select * from {{ ref('stg_orders') }}
    ),

    customer_orders as 
    (
        select 
            customer_id,
            min(order_date) as first_order_date,
            max(order_date) as most_recent_order_date,
            count(order_number) as number_of_orders,
            sum(order_quantity) as units_purchased

        from  orders
        group by 1
    ),

    dim_customers as 
    (
        select 
            row_number() over(order by customer_id asc) as customer_pk,
            customers.customer_id,
            customers.first_name,
            customers.last_name,
            customers.email,
            customers.gender,
            customers.occupation,
            customers.annual_income,
            customer_orders.first_order_date,
            customer_orders.most_recent_order_date,
            coalesce(customer_orders.number_of_orders,0) as number_of_orders,
            coalesce(customer_orders.units_purchased,0) as units_purchased
        from
            customers
            left join customer_orders using (customer_id)
    )

select * from dim_customers