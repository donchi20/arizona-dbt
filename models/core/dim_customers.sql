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
            min(order_date),
            max(order_date)
        from
            sales

    )
select * from customer_sales

    