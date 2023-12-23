with stg_customers as
(
    select 
    row_number() over(partition by customerkey order by customerkey) as rn,
    coalesce(customerkey, 'not available') as customer_id,
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
-- check for data integruty issues. create a dup_customers table to store dups