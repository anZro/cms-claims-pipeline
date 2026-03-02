{% snapshot snp_provider_payments %}

{{
    config(
        target_schema='snapshots',
        unique_key='rendering_npi || hcpcs_code',
        strategy='check',
        check_cols=[
            'avg_medicare_payment_amt',
            'avg_submitted_charge',
            'avg_medicare_standardized_amt',
            'total_services',
            'total_beneficiaries'
        ]
    )
}}

select
    rendering_npi,
    hcpcs_code,
    hcpcs_description,
    provider_specialty,
    provider_state,
    avg_submitted_charge,
    avg_medicare_allowed_amt,
    avg_medicare_payment_amt,
    avg_medicare_standardized_amt,
    total_services,
    total_beneficiaries,
    current_timestamp as snapshot_extracted_at

from {{ ref('int_provider_payments') }}

{% endsnapshot %}
