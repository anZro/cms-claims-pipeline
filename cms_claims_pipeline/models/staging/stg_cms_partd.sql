with source as (
    select * from {{ source('cms_raw', 'cms_partd_raw') }}
),

renamed as (
    select
        -- identifiers
        "Brnd_Name"     as brand_name,
        "Gnrc_Name"     as generic_name,
        "Mftr_Name"     as manufacturer_name,
        "Tot_Mftr"      as total_manufacturers,

        -- 2023 metrics (most recent year)
        "Tot_Spndng_2023"               as total_spending_2023,
        "Tot_Dsg_Unts_2023"             as total_dosage_units_2023,
        "Tot_Clms_2023"                 as total_claims_2023,
        "Tot_Benes_2023"                as total_beneficiaries_2023,
        "Avg_Spnd_Per_Dsg_Unt_Wghtd_2023" as avg_spend_per_dosage_unit_2023,
        "Avg_Spnd_Per_Clm_2023"         as avg_spend_per_claim_2023,
        "Avg_Spnd_Per_Bene_2023"        as avg_spend_per_beneficiary_2023,
        "Outlier_Flag_2023"             as outlier_flag_2023,

        -- year-over-year
        "Chg_Avg_Spnd_Per_Dsg_Unt_22_23" as chg_avg_spend_per_unit_22_23,
        "CAGR_Avg_Spnd_Per_Dsg_Unt_19_23" as cagr_avg_spend_per_unit_19_23,

        -- metadata
        current_timestamp as _loaded_at

    from source
)

select * from renamed

