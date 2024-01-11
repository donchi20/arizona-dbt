select 
    productcategorykey as category_id,
    categoryname as category

from {{ source('arizona', 'product_categories') }}