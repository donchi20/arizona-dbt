select 
    product_category_key as category_id,
    category_name as category

from {{ source('arizona', 'product_categories') }}