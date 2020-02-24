{{ config(
    alias="ad_groups"
) }}

with ad_groups_source as (

    select * from {{ source('google_ads', 'ad_groups') }}
),
ad_groups_renamed as (

    select 
        id as ad_group_id,
        campaign_id,
        name,
        adwords_customer_id as customer_id,
        received_at as updated_at
    from ad_groups_source
)

select * from ad_groups_renamed

