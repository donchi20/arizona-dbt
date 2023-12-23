select 
    salesterritorykey as territory_id,
    region,
    country,
    continent

from
    {{ source('adventureworks', 'territory') }}
