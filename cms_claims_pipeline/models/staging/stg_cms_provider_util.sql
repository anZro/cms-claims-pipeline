with source as (
    select * from {{ source('cms_raw', 'cms_provider_raw') }}
),

renamed as (
    select
        -- provider identifiers
        "Rndrng_NPI"                    as rendering_npi,
        "Rndrng_Prvdr_Last_Org_Name"    as provider_last_org_name,
        "Rndrng_Prvdr_First_Name"       as provider_first_name,
        "Rndrng_Prvdr_Crdntls"          as provider_credentials,
        "Rndrng_Prvdr_Ent_Cd"           as provider_entity_code,
        "Rndrng_Prvdr_Type"             as provider_specialty,

        -- provider location
        "Rndrng_Prvdr_City"             as provider_city,
        "Rndrng_Prvdr_State_Abrvtn"     as provider_state,
        "Rndrng_Prvdr_State_FIPS"       as provider_state_fips,
        "Rndrng_Prvdr_Zip5"             as provider_zip,
        "Rndrng_Prvdr_Cntry"            as provider_country,
        "Rndrng_Prvdr_RUCA"             as provider_ruca_code,
        "Rndrng_Prvdr_RUCA_Desc"        as provider_ruca_desc,

        -- participation
        "Rndrng_Prvdr_Mdcr_Prtcptg_Ind" as medicare_participating_ind,

        -- procedure
        "HCPCS_Cd"                      as hcpcs_code,
        "HCPCS_Desc"                    as hcpcs_description,
        "HCPCS_Drug_Ind"                as hcpcs_drug_indicator,
        "Place_Of_Srvc"                 as place_of_service,

        -- utilization
        "Tot_Benes"                     as total_beneficiaries,
        "Tot_Srvcs"                     as total_services,
        "Tot_Bene_Day_Srvcs"            as total_beneficiary_day_services,

        -- payments
        "Avg_Sbmtd_Chrg"                as avg_submitted_charge,
        "Avg_Mdcr_Alowd_Amt"            as avg_medicare_allowed_amt,
        "Avg_Mdcr_Pymt_Amt"             as avg_medicare_payment_amt,
        "Avg_Mdcr_Stdzd_Amt"            as avg_medicare_standardized_amt,

        -- metadata
        current_timestamp               as _loaded_at

    from source
)

select * from renamed
