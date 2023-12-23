with
    customers as 
    (
        select * from {{ ref('stg_customers') }}
    ),

    sales as
    (
        select * from {{ ref('stg_sales') }}
    ),

    customer_sales as 
    (
        select 
            customer_id,
            min(order_date) as first_order_date,
            max(order_date) as most_recent_order_date,
            count(order_number) as number_of_orders,
            sum(order_quantity) as total_order_quantity
        from sales
        group by 1
    ),

    final as 
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
            customer_sales.first_order_date,
            customer_sales.most_recent_order_date,
            coalesce(customer_sales.number_of_orders,0) as number_of_orders,
            coalesce(customer_sales.total_order_quantity,0) as total_order_quantity
        from
            customers
            left join customer_sales using (customer_id)
    )

select * from final