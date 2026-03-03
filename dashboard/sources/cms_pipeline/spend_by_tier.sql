select
    spend_tier,
    count(*) as drug_count,
    sum(total_spending_2023) as total_spending,
    avg(avg_spend_per_beneficiary_2023) as avg_spend_per_beneficiary
from main_marts.fct_drug_spend_by_specialty
group by spend_tier
order by total_spending desc
