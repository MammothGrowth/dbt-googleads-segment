{{ config(
    alias="adwords_campaign_performance"
) }}

with insights1 as (

  select * from {{ref('adwords_campaign_performance_base')}}

), campaigns1 as (

  select * from {{ref('adwords_campaigns')}}

), final1 as (

    select

        i.*,
        campaigns.name as campaign_name

    from insights1 as i
    left join campaigns1 as campaigns on campaigns.campaign_id = i.campaign_id

)

select * from final1
