with stg_products as
(
    select 
    row_number() over(partition by product_key order by product_key) as rn, -- using columns to remove duplicates
    coalesce(product_key, -1) as product_id, -- replace product key with -1
    product_name,
    model_name,
    product_subcategory_key as subcategory_id,
    product_cost,
    product_price

from
    {{ source('arizona', 'products') }}
)

select * from stg_products
where rn = 1
