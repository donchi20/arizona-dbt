select 
    sales_territory_key as territory_id,
    region,
    country,
    continent
from
    {{ source('arizona', 'territory') }}
