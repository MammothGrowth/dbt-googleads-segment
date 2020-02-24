{{ config(
    alias="ad_performance_base",
    materialized="ephemeral"
) }}


with performance_base as (

    select * from {{ source('google_ads', 'ad_performance_reports') }}

),

aggregated as (

    select

        id,
        date_stop::date as date_day,
        ad_id,
        adwords_customer_id as customer_id,
        received_at as updated_at,
        sum(clicks) as clicks,
        sum(impressions) as impressions,
        sum(cast((cost::float/1000000::float) as numeric(38,6))) as spend

    from performance_base

    {{ dbt_utils.group_by(5) }}

),

ranked as (

    select
    
        *,
        rank() over (partition by id
            order by updated_at desc) as latest
            
    from aggregated

),

final as (

    select *
    from ranked
    where latest = 1

)

select * from final

