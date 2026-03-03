---
title: CMS Medicare Claims Pipeline
---

# CMS Medicare Claims Pipeline

Analytics on public CMS Medicare data — Part D drug spending and provider utilization for 2023.

## Drug Spending Overview
```sql spend_summary
select
    spend_tier,
    drug_count,
    total_spending,
    avg_spend_per_beneficiary
from cms_pipeline.spend_by_tier
order by total_spending desc
```

<BarChart 
    data={spend_summary} 
    x=spend_tier 
    y=total_spending 
    title="Total Medicare Drug Spending by Tier (2023)"
/>

## Top 20 Drugs by Total Spending
```sql top_drugs
select
    brand_name,
    generic_name,
    total_spending_2023,
    total_beneficiaries_2023,
    avg_spend_per_beneficiary_2023,
    spend_tier
from cms_pipeline.drug_spend
limit 20
```

<DataTable data={top_drugs} rows=20/>

## Top Provider States by Medicare Payment
```sql top_states
select
    provider_state,
    sum(total_medicare_payment) as total_payment,
    sum(total_services) as total_services,
    count(distinct rendering_npi) as provider_count
from cms_pipeline.provider_payments
group by provider_state
order by total_payment desc
limit 15
```

<BarChart 
    data={top_states} 
    x=provider_state 
    y=total_payment 
    title="Total Medicare Payment by State (2023)"
/>
