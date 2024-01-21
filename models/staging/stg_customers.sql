with stg_customers as
(
    select 
    row_number() over(partition by customer_key order by customer_key) as rn, -- using column to remove duplicates
    coalesce(customer_key, -1) as customer_id, -- replace null customer keys with not available
    first_name,
    last_name,
    birth_date,
    marital_status,
    gender,
    email_address as email,
    annual_income,
    total_children,
    education_level,
    occupation,
    home_owner 
from 
    {{ source('arizona', 'customers') }}
)

select * from stg_customers
where rn = 1

-- create a dup_customers table to store dups