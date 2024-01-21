with returns as
(
    select * from {{ ref('stg_returns') }}
),

products as 
(
    select * from {{ ref('dim_products') }}
),

territory as 
(
    select * from {{ ref('dim_territory') }}
),

fact_returns as 
(
    select 
        r.return_date,
        t.territory_pk as territory_fk,
        p.product_pk as product_fk,
        r.return_quantity,
        p.product_price * r.return_quantity as return_amount
    
    from returns as r
    left join products as p using (product_id)
    left join territory as t using (territory_id)

)

select * from fact_returns