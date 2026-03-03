select
    rendering_npi,
    provider_specialty,
    provider_state,
    sum(total_medicare_payment) as total_medicare_payment,
    sum(total_services) as total_services,
    count(distinct hcpcs_code) as distinct_procedures
from main_marts.fct_provider_payments
group by 1,2,3
order by total_medicare_payment desc
