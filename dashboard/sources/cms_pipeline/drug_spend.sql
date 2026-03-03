select
    brand_name,
    generic_name,
    manufacturer_name,
    total_spending_2023,
    total_claims_2023,
    total_beneficiaries_2023,
    avg_spend_per_beneficiary_2023,
    spend_tier,
    is_outlier_2023
from main_marts.fct_drug_spend_by_specialty
order by total_spending_2023 desc
