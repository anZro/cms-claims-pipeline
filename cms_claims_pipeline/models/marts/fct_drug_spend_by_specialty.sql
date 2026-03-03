with drug_spending as (
    select * from {{ ref('int_drug_spending') }}
    where is_aggregate_row = false  -- exclude Overall rollup rows
),

final as (
    select
        -- drug identifiers
        brand_name,
        generic_name,
        manufacturer_name,

        -- spending metrics
        total_spending_2023,
        total_claims_2023,
        total_beneficiaries_2023,
        avg_spend_per_claim_2023,
        avg_spend_per_beneficiary_2023,
        avg_spend_per_dosage_unit_2023,

        -- trend signals
        chg_avg_spend_per_unit_22_23,
        cagr_avg_spend_per_unit_19_23,

        -- flags
        is_outlier_2023,

        -- spend tier derived column
        case
            when total_spending_2023 >= 1000000000 then 'Billion+'
            when total_spending_2023 >= 100000000  then '100M-1B'
            when total_spending_2023 >= 10000000   then '10M-100M'
            when total_spending_2023 >= 1000000    then '1M-10M'
            else 'Under 1M'
        end as spend_tier,

        -- cost efficiency ratio using safe_divide macro
        {{ safe_divide('total_spending_2023', 'total_beneficiaries_2023') }}
            as spend_per_beneficiary_derived

    from drug_spending
)

select * from final
