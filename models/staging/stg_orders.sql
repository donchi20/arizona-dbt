select 
    cast(order_date as date),
    order_number,
    product_key as product_id,
    customer_key as customer_id,
    territory_key as territory_id,
    order_quantity
from 
    {{ source('arizona', 'orders') }}