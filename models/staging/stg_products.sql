with stg_products as
(
    select 
    row_number() over(partition by productkey order by productkey) as rn, -- using columns to remove duplicates
    coalesce(productkey, -1) as product_id, -- replace product key with -1
    productname as product_name,
    modelname as model_name,
    productcost as product_cost,
    productprice as product_price

from
    {{ source('arizona', 'products') }}
)

select * from stg_products
where rn = 1

-- create a dup_products table to store dups