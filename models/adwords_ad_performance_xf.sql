{{ config(
    alias="adwords_ad_performance"
) }}

with ads1 as (

  select * from {{ref('adwords_ads')}}

), insights1 as (

  select * from {{ref('adwords_ad_performance_base')}}

), campaigns1 as (

  select * from {{ref('adwords_campaigns')}}

), adsets1 as (

  select * from {{ref('adwords_ad_groups')}}

), final1 as (

    select
    
        i.*,
        adsets.ad_group_id,
        campaigns.campaign_id,
        
        ads.original_ad_id,
        ads.utm_source,
        ads.utm_medium,
        ads.utm_campaign,
        ads.utm_content,
        ads.utm_term,

        campaigns.name as campaign_name,
        adsets.name as adset_name

    from insights1 as i
    left join ads1 as ads on i.ad_id = ads.ad_id
    left join adsets1 as adsets on adsets.ad_group_id = ads.ad_group_id
    left join campaigns1 as campaigns on campaigns.campaign_id = adsets.campaign_id

)

select * from final1
