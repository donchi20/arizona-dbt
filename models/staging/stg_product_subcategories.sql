select
    product_subcategory_key as subcategory_id,
    subcategory_name as subcategory,
    product_category_key as category_id

from {{ source('arizona', 'product_subcategories') }}