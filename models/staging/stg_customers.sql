with stg_customers as
(
    select 
    row_number() over(partition by customerkey order by customerkey) as rn, -- using column to remove duplicates
    coalesce(customerkey, 'not available') as customer_id, -- replace null customer keys with not available
    firstname as first_name,
    lastname as last_name,
    maritalstatus as marital_status,
    gender,
    emailaddress as email,
    annualincome as annual_income,
    occupation 
from 
    {{ source('adventureworks', 'customers') }}
)

select * from stg_customers
where rn = 1

-- create a dup_customers table to store dups