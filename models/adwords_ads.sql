{{ config(
    alias="ads"
) }}

with ads_source as (

    select * from {{ source('google_ads', 'ads') }}

),

ads_renamed as (

    select 

        id as ad_id,
        ad_group_id,
        adwords_customer_id as customer_id,
        received_at as updated_at,
        status,
        final_urls,
        original_id as original_ad_id,
        type,
        -- Note:  these are probably not correct, but only Segment source we have has no real values for this.
        {{ dbt_utils.split_part('final_urls', "'?'", 1) }} as base_url,
        {{ dbt_utils.get_url_host('final_urls') }} as url_host,
        '/' || {{ dbt_utils.get_url_path('final_urls') }} as url_path,
        {{ dbt_utils.get_url_parameter('final_urls', 'utm_source') }} as utm_source,
        {{ dbt_utils.get_url_parameter('final_urls', 'utm_medium') }} as utm_medium,
        {{ dbt_utils.get_url_parameter('final_urls', 'utm_campaign') }} as utm_campaign,
        {{ dbt_utils.get_url_parameter('final_urls', 'utm_content') }} as utm_content,
        {{ dbt_utils.get_url_parameter('final_urls', 'utm_term') }} as utm_term

    from ads_source

)

select * from ads_renamed
