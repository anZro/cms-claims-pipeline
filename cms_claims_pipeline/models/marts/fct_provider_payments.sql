with payments as (
    select * from {{ ref('int_provider_payments') }}
),

providers as (
    select * from {{ ref('dim_providers') }}
),

final as (
    select
        -- keys
        p.rendering_npi,
        p.hcpcs_code,
        p.hcpcs_description,
        p.hcpcs_drug_indicator,
        p.place_of_service,

        -- provider context from dim
        d.provider_specialty,
        d.provider_state,
        d.provider_city,
        d.is_medicare_participating,

        -- utilization
        p.total_beneficiaries,
        p.total_services,
        p.total_beneficiary_day_services,

        -- payments
        p.avg_submitted_charge,
        p.avg_medicare_allowed_amt,
        p.avg_medicare_payment_amt,
        p.avg_medicare_standardized_amt,
        p.medicare_payment_ratio,

        -- derived metrics
        round(p.avg_medicare_payment_amt * p.total_services, 2) as total_medicare_payment,
        round(p.avg_submitted_charge * p.total_services, 2)     as total_submitted_charges

    from payments p
    left join providers d
        on p.rendering_npi = d.rendering_npi
)

select * from final
