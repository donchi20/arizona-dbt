select 
    orderdate as order_date,
    ordernumber as order_no,
    productkey as product_id,
    customerkey as customer_id,
    territorykey as territory_id,
    orderquantity as order_qty
from 
    source.sales.sales