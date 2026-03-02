with source as (
    select * from {{ ref('stg_cms_provider_util') }}
),

specialty_codes as (
    select * from {{ ref('specialty_codes') }}
),

cleaned as (
    select
        -- provider identifiers
        cast(rendering_npi as varchar)      as rendering_npi,
        provider_last_org_name,
        provider_first_name,
        provider_credentials,
        provider_entity_code,
        provider_specialty,

        -- location
        provider_city,
        provider_state,
        provider_zip,
        provider_country,

        -- procedure
        hcpcs_code,
        hcpcs_description,
        hcpcs_drug_indicator,
        place_of_service,

        -- utilization — guard nulls
        coalesce(cast(total_beneficiaries as bigint), 0)            as total_beneficiaries,
        coalesce(cast(total_services as decimal(18,2)), 0)          as total_services,
        coalesce(cast(total_beneficiary_day_services as bigint), 0) as total_beneficiary_day_services,

        -- payments — cast and guard
        coalesce(cast(avg_submitted_charge as decimal(18,2)), 0)          as avg_submitted_charge,
        coalesce(cast(avg_medicare_allowed_amt as decimal(18,2)), 0)      as avg_medicare_allowed_amt,
        coalesce(cast(avg_medicare_payment_amt as decimal(18,2)), 0)      as avg_medicare_payment_amt,
        coalesce(cast(avg_medicare_standardized_amt as decimal(18,2)), 0) as avg_medicare_standardized_amt,

        -- derived
        round(
            cast(avg_medicare_payment_amt as decimal(18,2)) /
            nullif(cast(avg_submitted_charge as decimal(18,2)), 0),
        4) as medicare_payment_ratio,

        -- participation flag as boolean
        case
            when medicare_participating_ind = 'Y' then true
            else false
        end as is_medicare_participating,

        _loaded_at

    from source
),

enriched as (
    select
        c.*,
        coalesce(s.specialty_description, c.provider_specialty) as specialty_description,
        coalesce(s.specialty_category, 'Other')                 as specialty_category
    from cleaned c
    left join specialty_codes s
        on c.provider_specialty = s.specialty_code
)

select * from enriched