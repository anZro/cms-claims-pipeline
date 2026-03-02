with source as (
    select * from {{ ref('stg_cms_partd') }}
),

cleaned as (
    select
        -- identifiers
        brand_name,
        generic_name,
        manufacturer_name,

        -- filter to non-aggregate rows only
        -- 'Overall' rows summarize across manufacturers, we want drug-level detail
        case
            when manufacturer_name = 'Overall' then true
            else false
        end as is_aggregate_row,

        -- spending metrics — cast and guard against nulls
        coalesce(cast(total_spending_2023 as decimal(18,2)), 0)          as total_spending_2023,
        coalesce(cast(total_claims_2023 as decimal(18,2)), 0)            as total_claims_2023,
        coalesce(cast(total_beneficiaries_2023 as bigint), 0)            as total_beneficiaries_2023,
        coalesce(cast(avg_spend_per_claim_2023 as decimal(18,2)), 0)     as avg_spend_per_claim_2023,
        coalesce(cast(avg_spend_per_beneficiary_2023 as decimal(18,2)), 0) as avg_spend_per_beneficiary_2023,
        coalesce(cast(avg_spend_per_dosage_unit_2023 as decimal(18,2)), 0) as avg_spend_per_dosage_unit_2023,

        -- year over year signals
        cast(chg_avg_spend_per_unit_22_23 as decimal(18,6))   as chg_avg_spend_per_unit_22_23,
        cast(cagr_avg_spend_per_unit_19_23 as decimal(18,6))  as cagr_avg_spend_per_unit_19_23,

        -- outlier flag as boolean
        case
            when outlier_flag_2023 = 1 then true
            else false
        end as is_outlier_2023,

        _loaded_at

    from source
)

select * from cleaned
