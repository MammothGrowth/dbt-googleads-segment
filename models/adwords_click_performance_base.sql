{{ config(
    alias="click_report"
) }}

with gclid_base as (

    select

        id,
        gcl_id as gclid,
        date_stop::date as date_day,
        campaign_id,
        ad_group_id,
        creative_id,
        criteria_parameters,
        ad_format,
        click_type,
        device,
        page,
        slot,
        user_list_id,
        row_number() over (partition by gclid order by date_day) as row_num
    from {{ source('google_ads', 'click_performance_reports') }}
)

select * from gclid_base
where row_num = 1
and gclid is not null

