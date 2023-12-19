select 
    customerkey as customer_id,
    firstname as first_name,
    lastname as last_name,
    maritalstatus as marital_status,
    gender,
    emailaddress as email,
    annualincome as annual_income,
    occupation 
from 
    source.sales.customers