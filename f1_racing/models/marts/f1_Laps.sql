{{
    config(
        materialized='incremental'
    )
}}

select * from {{ref('stg_laps')}} as staging
{% if is_incremental() %}
left join {{this}} as existing 
on staging.sessionkey = existing.sessionkey 
{% endif %}