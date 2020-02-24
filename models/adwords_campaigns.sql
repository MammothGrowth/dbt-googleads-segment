{{ config(
    alias="campaigns"
) }}

with campaigns_source as (

    select * from {{ source('google_ads', 'campaigns') }}

),

campaigns_renamed as (

    select 

        id as campaign_id,
        name,
        adwords_customer_id as customer_id,
        received_at as updated_at,
        serving_status,
        status,
        start_date,
        end_date
        
    from campaigns_source

)

select * from campaigns_renamed

