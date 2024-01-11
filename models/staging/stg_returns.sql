select 
    returndate as return_date,
    territorykey as territory_id,
    productkey as product_id,
    returnquantity as return_quantity

from {{ source('arizona', 'returns') }}