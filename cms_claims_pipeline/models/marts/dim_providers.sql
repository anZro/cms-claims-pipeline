with source as (
    select * from {{ ref('int_provider_payments') }}
),

-- one row per provider -- deduplicate on NPI
-- take the most common specialty and most recent location data
deduped as (
    select
        rendering_npi,
        provider_last_org_name,
        provider_first_name,
        provider_credentials,
        provider_entity_code,
        provider_specialty,
        provider_city,
        provider_state,
        provider_zip,
        provider_country,
        is_medicare_participating,
        row_number() over (
            partition by rendering_npi
            order by total_services desc
        ) as row_num

    from source
),

final as (
    select
        rendering_npi,
        provider_last_org_name,
        provider_first_name,
        provider_credentials,
        provider_entity_code,
        provider_specialty,
        provider_city,
        provider_state,
        provider_zip,
        provider_country,
        is_medicare_participating
    from deduped
    where row_num = 1
)

select * from final
