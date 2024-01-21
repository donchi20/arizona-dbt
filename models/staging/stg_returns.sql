select 
    return_date,
    territory_key as territory_id,
    product_key as product_id,
    return_quantity as return_quantity

from {{ source('arizona', 'returns') }}