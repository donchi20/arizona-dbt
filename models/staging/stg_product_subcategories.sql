select
    productsubcategorykey as subcategory_id,
    subcategoryname as subcategory,
    productcategorykey as category_id

from {{ source('arizona', 'product_subcategories') }}