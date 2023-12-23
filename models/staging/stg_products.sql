with stg_products as
(
    select 
    row_number() over(partition by productkey order by productkey) as rn,
    coalesce(productkey, -1) as product_id,
    productname as product_name,
    modelname as model_name,
    productcost as product_cost,
    productprice as product_price

from
    {{ source('adventureworks', 'products') }}
)

select * from stg_products
where rn = 1
-- check for data integruty issues. create a dup_products table to store dups