select 
    orderdate as order_date,
    ordernumber as order_number,
    productkey as product_id,
    cast(customerkey as text) as customer_id,
    territorykey as territory_id,
    orderquantity as order_quantity
from 
    {{ source('adventureworks', 'orders') }}